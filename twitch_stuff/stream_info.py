from typing import Any, cast
from yt_dlp import YoutubeDL

def get_twitch_stream_title(url: str) -> tuple[str | None, int | None]:
    opts = cast(Any, {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
    })

    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception:
        return None, None

    if not isinstance(info, dict):
        return None, None

    title = info.get("description")
    started_at = info.get("release_timestamp")

    if not isinstance(title, str):
        return None, None

    return title.strip() or None, started_at