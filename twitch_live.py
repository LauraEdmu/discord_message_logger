from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp

log = logging.getLogger(__name__)

EVENTSUB_WS_URL = "wss://eventsub.wss.twitch.tv/ws"
HELIX_EVENTSUB_URL = "https://api.twitch.tv/helix/eventsub/subscriptions"
HELIX_STREAMS_URL = "https://api.twitch.tv/helix/streams"
TWITCH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"


@dataclass(slots=True, frozen=True)
class TwitchLiveEvent:
    stream_id: str
    broadcaster_user_id: str
    broadcaster_login: str
    broadcaster_name: str
    started_at: str
    source: str  # "eventsub" or "startup-check"
    title: str | None = None
    game_name: str | None = None


LiveCallback = Callable[[TwitchLiveEvent], Awaitable[None]]


class TwitchLiveWatcher:
    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        broadcaster_user_id: str,
        token_file: str | Path = "twitch_tokens.json",
        state_file: str | Path = "twitch_live_state.json",
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.broadcaster_user_id = broadcaster_user_id

        self.token_file = Path(token_file)
        self.state_file = Path(state_file)

        self.access_token = ""
        self.refresh_token = ""

        self._stop_event = asyncio.Event()
        self._seen_message_ids: set[str] = set()

    async def stop(self) -> None:
        self._stop_event.set()

    async def run(self, on_live: LiveCallback) -> None:
        self._load_tokens()

        async with aiohttp.ClientSession() as session:
            await self._refresh_token(session)

            # Safety net: if the Discord bot starts/restarts while you're already live,
            # there may be no new stream.online event to receive.
            await self._check_currently_live(session, on_live)

            ws_url = EVENTSUB_WS_URL

            while not self._stop_event.is_set():
                try:
                    async with session.ws_connect(ws_url, heartbeat=30) as ws:
                        log.info("Connected to Twitch EventSub WebSocket")

                        # Reset after a successful connection unless Twitch gives us
                        # another reconnect URL.
                        ws_url = EVENTSUB_WS_URL

                        async for ws_msg in ws:
                            if self._stop_event.is_set():
                                return

                            if ws_msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(ws_msg.data)

                                reconnect_url = await self._handle_ws_message(
                                    session,
                                    data,
                                    on_live,
                                )

                                if reconnect_url:
                                    ws_url = reconnect_url
                                    break

                            elif ws_msg.type in {
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.ERROR,
                            }:
                                break

                except asyncio.CancelledError:
                    raise

                except Exception:
                    log.exception("Twitch EventSub loop crashed")

                await self._sleep_or_stop(5)

    async def _handle_ws_message(
        self,
        session: aiohttp.ClientSession,
        message: dict[str, Any],
        on_live: LiveCallback,
    ) -> str | None:
        metadata = message.get("metadata", {})
        payload = message.get("payload", {})

        message_type = metadata.get("message_type")
        message_id = metadata.get("message_id")

        if message_id:
            if message_id in self._seen_message_ids:
                return None

            self._seen_message_ids.add(message_id)

            if len(self._seen_message_ids) > 1000:
                self._seen_message_ids.clear()

        if message_type == "session_welcome":
            session_id = payload["session"]["id"]
            await self._subscribe_stream_online(session, session_id)
            return None

        if message_type == "session_keepalive":
            return None

        if message_type == "session_reconnect":
            return payload["session"]["reconnect_url"]

        if message_type == "notification":
            subscription_type = metadata.get("subscription_type")

            if subscription_type != "stream.online":
                return None

            event = payload["event"]

            live_event = TwitchLiveEvent(
                stream_id=event["id"],
                broadcaster_user_id=event["broadcaster_user_id"],
                broadcaster_login=event["broadcaster_user_login"],
                broadcaster_name=event["broadcaster_user_name"],
                started_at=event["started_at"],
                title=event.get("title"),
                game_name=event.get("game_name"),
                source="eventsub",
            )

            await self._maybe_emit_live(live_event, on_live)
            return None

        if message_type == "revocation":
            log.warning("Twitch revoked an EventSub subscription: %s", message)
            return None

        log.debug("Unhandled Twitch EventSub message: %s", message)
        return None

    async def _subscribe_stream_online(
        self,
        session: aiohttp.ClientSession,
        eventsub_session_id: str,
    ) -> None:
        payload = {
            "type": "stream.online",
            "version": "1",
            "condition": {
                "broadcaster_user_id": self.broadcaster_user_id,
            },
            "transport": {
                "method": "websocket",
                "session_id": eventsub_session_id,
            },
        }

        await self._api_json(session, "POST", HELIX_EVENTSUB_URL, json_body=payload)
        log.info("Subscribed to Twitch stream.online")

    async def _check_currently_live(
        self,
        session: aiohttp.ClientSession,
        on_live: LiveCallback,
    ) -> None:
        data = await self._api_json(
            session,
            "GET",
            HELIX_STREAMS_URL,
            params={"user_id": self.broadcaster_user_id},
        )

        streams = data.get("data", [])
        if not streams:
            return

        stream = streams[0]

        live_event = TwitchLiveEvent(
            stream_id=stream["id"],
            broadcaster_user_id=self.broadcaster_user_id,
            broadcaster_login=stream["user_login"],
            broadcaster_name=stream["user_name"],
            started_at=stream["started_at"],
            source="startup-check",
        )

        await self._maybe_emit_live(live_event, on_live)

    async def _maybe_emit_live(
        self,
        event: TwitchLiveEvent,
        on_live: LiveCallback,
    ) -> None:
        state = self._load_state()

        if state.get("last_stream_id") == event.stream_id:
            log.info("Skipping duplicate Twitch live event: %s", event.stream_id)
            return

        await on_live(event)

        self._save_state(
            {
                "last_stream_id": event.stream_id,
                "last_started_at": event.started_at,
                "last_source": event.source,
            }
        )

    async def _api_json(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        *,
        params: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        for attempt in range(2):
            async with session.request(
                method,
                url,
                headers=self._headers(),
                params=params,
                json=json_body,
            ) as resp:
                text = await resp.text()

                if resp.status == 401 and attempt == 0:
                    await self._refresh_token(session)
                    continue

                # Duplicate EventSub subscription. Fine after reconnect/restart.
                if resp.status == 409:
                    log.info("Twitch EventSub subscription already exists: %s", text)
                    return json.loads(text) if text else {}

                if resp.status >= 400:
                    raise RuntimeError(f"Twitch API failed: {resp.status} {text}")

                return json.loads(text) if text else {}

        raise RuntimeError("Twitch API failed after token refresh")

    async def _refresh_token(self, session: aiohttp.ClientSession) -> None:
        async with session.post(
            TWITCH_TOKEN_URL,
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
            },
        ) as resp:
            text = await resp.text()

            if resp.status >= 400:
                raise RuntimeError(f"Twitch token refresh failed: {resp.status} {text}")

            data = json.loads(text)

        self.access_token = data["access_token"]
        self.refresh_token = data.get("refresh_token", self.refresh_token)
        self._save_tokens()

    def _headers(self) -> dict[str, str]:
        return {
            "Client-Id": self.client_id,
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def _load_tokens(self) -> None:
        data = json.loads(self.token_file.read_text(encoding="utf-8"))
        self.access_token = data["access_token"]
        self.refresh_token = data["refresh_token"]

    def _save_tokens(self) -> None:
        self.token_file.write_text(
            json.dumps(
                {
                    "access_token": self.access_token,
                    "refresh_token": self.refresh_token,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    def _load_state(self) -> dict[str, Any]:
        if not self.state_file.exists():
            return {}

        return json.loads(self.state_file.read_text(encoding="utf-8"))

    def _save_state(self, state: dict[str, Any]) -> None:
        self.state_file.write_text(
            json.dumps(state, indent=2),
            encoding="utf-8",
        )

    async def _sleep_or_stop(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except TimeoutError:
            pass