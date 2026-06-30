#!/usr/bin/env python3
from pathlib import Path

import discord
from discord import Poll, app_commands
import logging
import os
from datetime import timedelta
from datetime import datetime as datetime_dt
import pdb
from rich.traceback import install
from dotenv import load_dotenv
import asyncpg
from enum import StrEnum
import io
import asyncio
import random
import json
import re
import datetime
import aiofiles
from twitch_live import TwitchLiveEvent, TwitchLiveWatcher

BATCH_SIZE = 100
FLUSH_INTERVAL = 1.0

class LogLevel(StrEnum): # for bot event logging criticality
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


# required pip installs: pip install discord rich asyncpg dotenv httpx asyncio


load_dotenv()

install(show_locals=True) # Enable rich traceback

# Globals

DEV_ID = 262687596642041856

default_duration = 15
TIMEOUT_DURATION = timedelta(minutes=default_duration) # for banned word timeout
GAME_TIMEOUT_DURATION = timedelta(minutes=5) 
IS_INT_NUMBERS = re.compile(r'^\s*\d+\s*$')
BANNED_USERS_PATH = os.path.join("banned_stuff", "banned_users.json")
BANNED_SUBSTRINGS_PATH = os.path.join("banned_stuff", "banned_substrings.json")
BANNED_WORDS_PATH = os.path.join("banned_stuff", "banned_words.json")
BANNED_REGEX_PATH = os.path.join("banned_stuff", "banned_regex.json")
MESSAGE_RATE_PATH = os.path.join("banned_stuff", "message_rate.json")
MESSAGE_RATE_WINDOW_SECONDS = 10
MESSAGE_RATE_MAX_STORED = 50
MESSAGE_RATE_LIMIT = 5 # if a user sends more than this many messages in the MESSAGE_RATE_WINDOW_SECONDS time window, they will be timed out for spamming
SPAM_TIMEOUT_DURATION = timedelta(minutes=15)
MODERATOR_NOTICES_CHANNEL_ID = 1510737589978791936
MODERATOR_ROLE_ID = 1508158074014273668

YOUTUBE_SI_REGEX = re.compile(
    r"youtu\.be/(?P<videoid>[A-Za-z0-9_-]{11})"
    r"\?"
    r"(?=[^#\s]*\bsi=(?P<si>[A-Za-z0-9_-]{16}))"
    r"(?:(?=[^#\s]*\bt=(?P<timestamp>\d+)))?"
    r"[^#\s]*"
)

PRONOUNS = [
    "i", "me", "myself", "mine", "my",
    "we", "us", "ourselves", "ours", "our",
    "you", "yourself", "yourselves", "yours", "your",
    "thou", "thee", "thyself", "thine", "thy",
    "he", "him", "himself", "his",
    "she", "her", "herself", "hers",
    "it", "itself", "its",
    "they", "them", "themself", "themselves", "theirs", "their",
    "one", "oneself", "one's",
    "who", "whom", "whose", "what", "which",
    "each other", "each other's",
    "one another", "one another's",
    "there",
]

PRONOUN_RE = re.compile(
    r"(?<!\w)(?:"
    + "|".join(re.escape(p).replace(r"\ ", r"\s+") for p in sorted(PRONOUNS, key=len, reverse=True))
    + r")(?!\w)",
    re.IGNORECASE,
)

needed_intents = discord.Intents.default()
needed_intents.message_content = True
needed_intents.members = True
# client = discord.Client(intents=needed_intents)
# tree = app_commands.CommandTree(client)

PER_GUILD_DIR = Path("per_guild_info")
guild_infos: dict[int, Path] = {}

class MyClient(discord.Client):
    db_pool: asyncpg.Pool
    startup_done: bool = False
    start_time: datetime_dt
    web_server_started: bool = False

    message_buffer: list[tuple[int, int, int, int | None, str]]
    message_buffer_lock: asyncio.Lock

    user_buffer: dict[int, tuple[int, str, str | None, bool]]
    user_buffer_lock: asyncio.Lock

    flush_task: asyncio.Task | None = None

    twitch_live: TwitchLiveWatcher
    twitch_live_task: asyncio.Task | None = None

    async def setup_hook(self):
        self.db_pool = await asyncpg.create_pool(
            os.getenv("psql_url"),
            min_size=1,
            max_size=10
        )
        await create_db_tables(self.db_pool)

        self.message_buffer = []
        self.message_buffer_lock = asyncio.Lock()

        self.user_buffer = {}
        self.user_buffer_lock = asyncio.Lock()

        self.flush_task = asyncio.create_task(message_flush_loop())

        self.twitch_live = TwitchLiveWatcher(
            client_id=os.environ["TWITCH_CLIENT_ID"],
            client_secret=os.environ["TWITCH_CLIENT_SECRET"],
            broadcaster_user_id=os.environ["TWITCH_BROADCASTER_ID"],
            token_file=os.getenv("TWITCH_TOKEN_FILE", "twitch_tokens.json"),
            state_file=os.getenv("TWITCH_LIVE_STATE_FILE", "twitch_live_state.json"),
        )

        self.twitch_live_task = asyncio.create_task(
            self.twitch_live.run(self.on_twitch_live)
        )
    
    async def on_twitch_live(self, event: TwitchLiveEvent) -> None:
        channel = self.get_channel(DISCORD_LIVE_CHANNEL_ID)

        if channel is None:
            channel = await self.fetch_channel(DISCORD_LIVE_CHANNEL_ID)

        if not isinstance(channel, discord.abc.Messageable):
            logger.error(
                f"Channel with ID {DISCORD_LIVE_CHANNEL_ID} is not messageable."
            )
            return

        sent_message = await channel.send(
            f"@everyone\n"
            f"🔴 **{event.broadcaster_name} is live!**\n"
            f"**Title:** {event.title}\n"
            f"**Game:** {event.game_name}\n"
            f"https://twitch.tv/{event.broadcaster_login}"
        )

        try:
            await sent_message.publish()
        except discord.Forbidden:
            logger.error("Failed to publish Twitch live announcement due to permissions.")
        except discord.HTTPException as e:
            logger.error(f"Failed to publish Twitch live announcement: {e}")

    async def close(self):
        try:
            await flush_buffers()

            if hasattr(self, "twitch_live"):
                await self.twitch_live.stop()

            if self.twitch_live_task:
                self.twitch_live_task.cancel()
                try:
                    await self.twitch_live_task
                except asyncio.CancelledError:
                    pass

        finally:
            await super().close()

client = MyClient(intents=needed_intents)
tree = app_commands.CommandTree(client)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

log_path = os.path.join("main_logs", "bot.log")
os.makedirs(os.path.dirname(log_path), exist_ok=True)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
file_handler = logging.FileHandler(log_path)
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

async def flush_buffers():
    async with client.user_buffer_lock:
        user_rows = list(client.user_buffer.values())
        client.user_buffer.clear()

    async with client.message_buffer_lock:
        if not client.message_buffer and not user_rows:
            return

        message_rows = client.message_buffer[:]
        client.message_buffer.clear()

    try:
        async with client.db_pool.acquire() as conn:
            async with conn.transaction():
                if user_rows:
                    await conn.executemany("""
                        INSERT INTO users (id, username, global_name, bot)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (id) DO UPDATE
                        SET username = EXCLUDED.username,
                            global_name = EXCLUDED.global_name,
                            bot = EXCLUDED.bot
                    """, user_rows)

                if message_rows:
                    await conn.executemany("""
                        INSERT INTO messages (id, user_id, channel_id, guild_id, content)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (id) DO NOTHING
                    """, message_rows)

    except Exception as e:
        logger.error(f"Error flushing buffers: {e}", exc_info=True)

        async with client.user_buffer_lock:
            for row in user_rows:
                client.user_buffer[row[0]] = row

        async with client.message_buffer_lock:
            client.message_buffer = message_rows + client.message_buffer

async def message_flush_loop():
    await client.wait_until_ready()

    while not client.is_closed():
        try:
            await asyncio.sleep(FLUSH_INTERVAL)
            await flush_buffers()
        except Exception as e:
            logger.error(f"Error in message flush loop: {e}", exc_info=True)

# log severities are: debug, info, warning, error, critical

async def create_db_tables(pool: asyncpg.Pool):
    async with pool.acquire() as conn:
        async with conn.transaction():
            # --- tables ---
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS guilds (
                    id BIGINT PRIMARY KEY,
                    name TEXT NOT NULL,
                    owner_id BIGINT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT PRIMARY KEY,
                    username TEXT NOT NULL,
                    global_name TEXT,
                    bot BOOLEAN NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id BIGINT PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    channel_id BIGINT NOT NULL,
                    guild_id BIGINT REFERENCES guilds(id) ON DELETE CASCADE,
                    content TEXT NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS bot_events (
                    id SERIAL PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    criticality TEXT NOT NULL
                        CHECK (criticality IN ('debug', 'info', 'warning', 'error', 'critical')),
                    description TEXT,
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            # --- indexes ---
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_messages_guild_id
                ON messages(guild_id)
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_messages_timestamp
                ON messages(timestamp)
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_messages_user_id
                ON messages(user_id)
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_messages_channel_id
                ON messages(channel_id)
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_bot_events_timestamp
                ON bot_events(timestamp)
            """)

@client.event
async def on_ready():
    if client.startup_done:
        return
    client.startup_done = True

    client.start_time = datetime_dt.now()

    await tree.sync()
    

    # load short jokes csv
    with open("shortjokes.csv", newline="", encoding="utf-8") as csvfile:
        import csv
        global jokes
        reader = csv.DictReader(csvfile) 
        jokes = [row["Joke"] for row in reader if row["Joke"]]

    await client.change_presence(
        status=discord.Status.online,
        activity=discord.Game("Status!")
    )

    # load banned_users.json (contains a set of user IDs that are banned from using the bot, which is checked in the on_message event and join event) into a global set variable
    os.makedirs(os.path.dirname(BANNED_USERS_PATH), exist_ok=True)
    if os.path.exists(BANNED_USERS_PATH):
        with open(BANNED_USERS_PATH, "r") as f:
            global banned_users
            banned_users = set(json.load(f))
    else:
        banned_users = set()
    
    # load banned_substrings.json (contains a list of substrings that are not allowed in messages, which is checked in the on_message event) into a global list variable
    global banned_substrings_regex
    global banned_substrings
    os.makedirs(os.path.dirname(BANNED_SUBSTRINGS_PATH), exist_ok=True)
    if os.path.exists(BANNED_SUBSTRINGS_PATH):
        with open(BANNED_SUBSTRINGS_PATH, "r", encoding="utf-8") as f:
            banned_substrings = json.load(f)
    else:
        banned_substrings = []
      
    banned_substrings = [
        sub for sub in banned_substrings
        if isinstance(sub, str) and sub
    ]

    banned_substrings_regex = (
        re.compile("|".join(re.escape(sub) for sub in banned_substrings), re.IGNORECASE)
        if banned_substrings
        else None
    )

    global banned_words_regex
    global banned_words
    os.makedirs(os.path.dirname(BANNED_WORDS_PATH), exist_ok=True)
    if os.path.exists(BANNED_WORDS_PATH):
        with open(BANNED_WORDS_PATH, "r", encoding="utf-8") as f:
            banned_words = json.load(f)
    else:
        banned_words = []

    banned_words = [
        word for word in banned_words
        if isinstance(word, str) and word
    ]

    banned_words_regex = (
        re.compile(
            r"\b(?:"
            + "|".join(re.escape(word) for word in banned_words)
            + r")\b",
            re.IGNORECASE,
        )
        if banned_words
        else None
    )

    global banned_regex_regex
    global banned_regex
    os.makedirs(os.path.dirname(BANNED_REGEX_PATH), exist_ok=True)
    if os.path.exists(BANNED_REGEX_PATH):
        with open(BANNED_REGEX_PATH, "r", encoding="utf-8") as f:
            banned_regex = json.load(f)
    else:
        banned_regex = []

    banned_regex = [
        regex for regex in banned_regex
        if isinstance(regex, str) and regex
    ]

    banned_regex_regex = (
        re.compile("|".join(banned_regex), re.IGNORECASE)
        if banned_regex
        else None
    )

    PER_GUILD_DIR.mkdir(exist_ok=True)

    for guild in client.guilds:
        guild_path = PER_GUILD_DIR / str(guild.id)
        guild_path.mkdir(exist_ok=True)

        guild_infos[guild.id] = guild_path

    try:
        async with client.db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO bot_events (event_type, criticality, description)
                VALUES ($1, $2, $3)
            """, "startup", LogLevel.INFO.value, "Bot has started up successfully.")
    except Exception as e:
        logger.error(f"Error logging bot startup event: {e}", exc_info=True)

    try:
        async with client.db_pool.acquire() as conn:
            for guild in client.guilds:
                await conn.execute("""
                    INSERT INTO guilds (id, name, owner_id)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (id) DO UPDATE
                    SET name = EXCLUDED.name,
                        owner_id = EXCLUDED.owner_id
                """, guild.id, guild.name, guild.owner_id)
    except Exception as e:
        logger.error(f"Error logging guild info: {e}", exc_info=True)

    try:
        async with client.db_pool.acquire() as conn:
            user_rows: dict[int, tuple[int, str, str | None, bool]] = {}

            for guild in client.guilds:
                for member in guild.members:
                    user_rows[member.id] = (
                        member.id,
                        member.name,
                        member.global_name,
                        member.bot,
                    )

            if user_rows:
                await conn.executemany("""
                    INSERT INTO users (id, username, global_name, bot)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (id) DO UPDATE
                    SET username = EXCLUDED.username,
                        global_name = EXCLUDED.global_name,
                        bot = EXCLUDED.bot
                """, list(user_rows.values()))
    except Exception as e:
        logger.error(f"Error logging startup user info: {e}", exc_info=True)
    
    

    logger.debug("Got to end of on_ready")

@client.event
async def on_guild_join(guild: discord.Guild): # log new guild info to db when bot is added to a new server
    guild_path = PER_GUILD_DIR / str(guild.id)
    guild_path.mkdir(exist_ok=True)
    guild_infos[guild.id] = guild_path
    
    try:
        async with client.db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO guilds (id, name, owner_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (id) DO UPDATE
                SET name = EXCLUDED.name,
                    owner_id = EXCLUDED.owner_id
            """, guild.id, guild.name, guild.owner_id)
    except Exception as e:
        logger.error(f"Error logging joined guild info: {e}", exc_info=True)

async def dm_member_with_retry(
    member: discord.Member,
    content: str,
    delays: tuple[float, ...] = (2, 5, 10),
) -> bool:
    """
    Try to DM a member after short delays.

    Returns True if sent, False if all attempts failed.
    """
    for attempt, delay in enumerate(delays, start=1):
        await asyncio.sleep(delay)

        try:
            await member.send(content)
            return True

        except discord.Forbidden:
            logger.debug(
                "Could not DM %s on attempt %s/%s",
                member,
                attempt,
                len(delays),
            )

        except discord.HTTPException as e:
            logger.debug(
                "HTTP error while DMing %s on attempt %s/%s: %s",
                member,
                attempt,
                len(delays),
                e,
            )

    logger.info("Could not DM %s after joining.", member)
    return False

@client.event
async def on_member_join(member: discord.Member): # log new user info to db when a new user joins a server the bot is in
    if member.id in banned_users:
        # ban the user from the server if they are in the banned_users set
        try:
            await member.ban(reason="User is banned",delete_message_seconds=3600)
            logger.info(f"Banned user {member.name} on join due to being in banned_users list.")
        except discord.Forbidden as e:
            logger.error(f'Failed to ban user {member.name} on join. E: {e}')
        except Exception as e:
            logger.critical(e)
        return
    
    user_row = (
        member.id,
        member.name,
        getattr(member, "global_name", None),
        member.bot,
    )

    try:
        async with client.db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO users (id, username, global_name, bot)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (id) DO UPDATE
                SET username = EXCLUDED.username,
                    global_name = EXCLUDED.global_name,
                    bot = EXCLUDED.bot
            """, *user_row)
    except Exception as e:
        logger.error(f"Error logging new member info: {e}", exc_info=True)
    

    guild = member.guild
    roles_location = Path(guild_infos[guild.id]) / "roles.json"
    if roles_location.exists():
        async with aiofiles.open(roles_location, "r", encoding="utf-8") as f:
            roles_data = await f.read()
            roles_dict = json.loads(roles_data)

        role_id = roles_dict.get("member", None)
        role = guild.get_role(role_id) if role_id else None
    logger.debug(f"Member {member.name} joined")
    
    if role:
        try:
            await member.add_roles(role)
            logger.info(f'Assigned {role.name} role to {member.name}')
            print(f'Assigned {role.name} role to {member.name}')
        except discord.Forbidden as e:
            logger.error(f'Failed to assign {role.name} role to {member.name}. E: {e}')
            print(f'Failed to assign {role.name} role to {member.name}')
    
    await dm_member_with_retry(member, fr"Welcome to the server, {member.display_name}!")

def rebuild_banned_substrings_regex() -> None:
    global banned_substrings_regex

    cleaned = [
        sub for sub in banned_substrings
        if isinstance(sub, str) and sub
    ]

    banned_substrings_regex = (
        re.compile("|".join(re.escape(sub) for sub in cleaned), re.IGNORECASE)
        if cleaned
        else None
    )

@tree.command(name="bansubstring")
async def ban_substring(interaction: discord.Interaction, substring: str):
    global banned_substrings

    if interaction.guild is None:
        await interaction.response.send_message(
            "This command can only be used in a server.",
            ephemeral=True,
        )
        return

    if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            "Not permitted to ban substrings.",
            ephemeral=True,
        )
        return

    substring = substring.strip()
    if not substring:
        await interaction.response.send_message(
            "Cannot ban an empty substring.",
            ephemeral=True,
        )
        return

    banned_substrings_folded = {sub.casefold() for sub in banned_substrings}

    if substring.casefold() in banned_substrings_folded:
        await interaction.response.send_message(
            f'"{substring}" is already in the banned substrings list.',
            ephemeral=True,
        )
        return

    banned_substrings.append(substring)

    with open(BANNED_SUBSTRINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(banned_substrings, f, indent=2)

    rebuild_banned_substrings_regex()

    logger.info(f'Added "{substring}" to banned substrings list via command.')
    await interaction.response.send_message(
        f'"{substring}" has been added to the banned substrings list.',
        ephemeral=True,
    )

@tree.command(name="unbansubstring")
async def unban_substring(interaction: discord.Interaction, substring: str):
    global banned_substrings

    if interaction.guild is None:
        await interaction.response.send_message(
            "This command can only be used in a server.",
            ephemeral=True,
        )
        return

    if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            "Not permitted to unban substrings.",
            ephemeral=True,
        )
        return

    substring = substring.strip()

    match = next(
        (sub for sub in banned_substrings if sub.casefold() == substring.casefold()),
        None,
    )

    if match is None:
        await interaction.response.send_message(
            f'"{substring}" is not in the banned substrings list.',
            ephemeral=True,
        )
        return

    banned_substrings.remove(match)

    with open(BANNED_SUBSTRINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(banned_substrings, f, indent=2)

    rebuild_banned_substrings_regex()

    logger.info(f'Removed "{match}" from banned substrings list via command.')
    await interaction.response.send_message(
        f'"{match}" has been removed from the banned substrings list.',
        ephemeral=True,
    )

def rebuild_banned_words_regex() -> None:
    global banned_words_regex

    cleaned = [
        word for word in banned_words
        if isinstance(word, str) and word
    ]

    banned_words_regex = (
        re.compile(
            r"\b(?:"
            + "|".join(re.escape(word) for word in cleaned)
            + r")\b",
            re.IGNORECASE,
        )
        if banned_words
        else None
    )

@tree.command(name="banword")
async def ban_word(interaction: discord.Interaction, word: str):
    global banned_words

    if interaction.guild is None:
        await interaction.response.send_message(
            "This command can only be used in a server.",
            ephemeral=True,
        )
        return

    if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            "Not permitted to ban words.",
            ephemeral=True,
        )
        return

    word = word.strip()
    if not word:
        await interaction.response.send_message(
            "Cannot ban an empty word.",
            ephemeral=True,
        )
        return

    banned_words_folded = {w.casefold() for w in banned_words}

    if word.casefold() in banned_words_folded:
        await interaction.response.send_message(
            f'"{word}" is already in the banned words list.',
            ephemeral=True,
        )
        return

    banned_words.append(word)

    with open(BANNED_WORDS_PATH, "w", encoding="utf-8") as f:
        json.dump(banned_words, f, indent=2)

    rebuild_banned_words_regex()

    logger.info(f'Added "{word}" to banned words list via command.')
    await interaction.response.send_message(
        f'"{word}" has been added to the banned words list.',
        ephemeral=True,
    )

@tree.command(name="unbanword")
async def unban_word(interaction: discord.Interaction, word: str):
    global banned_words

    if interaction.guild is None:
        await interaction.response.send_message(
            "This command can only be used in a server.",
            ephemeral=True,
        )
        return

    if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            "Not permitted to unban words.",
            ephemeral=True,
        )
        return

    word = word.strip()

    match = next(
        (w for w in banned_words if w.casefold() == word.casefold()),
        None,
    )

    if match is None:
        await interaction.response.send_message(
            f'"{word}" is not in the banned words list.',
            ephemeral=True,
        )
        return

    banned_words.remove(match)

    with open(BANNED_WORDS_PATH, "w", encoding="utf-8") as f:
        json.dump(banned_words, f, indent=2)

    rebuild_banned_words_regex()

    logger.info(f'Removed "{match}" from banned words list via command.')
    await interaction.response.send_message(
        f'"{match}" has been removed from the banned words list.',
        ephemeral=True,
    )

@tree.command(name="ban")
async def ban_user(interaction: discord.Interaction, user: discord.User):
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command can only be used in a server.",
            ephemeral=True,
        )
        return
    
    if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            "Not permitted to ban users.",
            ephemeral=True,
        )
        return


    banned_users.add(user.id)
    with open(BANNED_USERS_PATH, "w") as f:
        json.dump(list(banned_users), f)
    
    try:
        await interaction.guild.ban(user, reason="User is banned", delete_message_seconds=3600)
        logger.info(f"Banned user {user.name} via command and added to banned_users list.")
        await interaction.response.send_message(f"User {user.name} has been banned.", ephemeral=True)
    except discord.Forbidden as e:
        logger.error(f'Failed to ban user {user.name} via command. E: {e}')
        await interaction.response.send_message(f"Failed to ban user {user.name} due to permissions.", ephemeral=True)
    except Exception as e:
        logger.critical(e)
        await interaction.response.send_message(f"An error occurred while trying to ban user {user.name}.", ephemeral=True)

@tree.command(name="unban")
async def unban_user(interaction: discord.Interaction, user: discord.User):
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command can only be used in a server.",
            ephemeral=True,
        )
        return
    
    if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            "Not permitted to unban users.",
            ephemeral=True,
        )
        return
    
    if user.id in banned_users:
        banned_users.remove(user.id)
        with open(BANNED_USERS_PATH, "w") as f:
            json.dump(list(banned_users), f)
 
    try:
        await interaction.guild.unban(user, reason="User is unbanned")
        logger.info(f"Unbanned user {user.name} via command and removed from banned_users list.")
        await interaction.response.send_message(f"User {user.name} has been unbanned.", ephemeral=True)
    except discord.Forbidden as e:
        logger.error(f'Failed to unban user {user.name} via command. E: {e}')
        await interaction.response.send_message(f"Failed to unban user {user.name} due to permissions.", ephemeral=True)
    except Exception as e:
        logger.critical(e)
        await interaction.response.send_message(f"An error occurred while trying to unban user {user.name}.", ephemeral=True)
    

def clean_youtube_si_link(message: str) -> str | None:
    youtube_si = YOUTUBE_SI_REGEX.search(message)

    if not youtube_si:
        return None

    clean_url = f"https://youtu.be/{youtube_si.group('videoid')}"

    if youtube_si.group("timestamp"):
        clean_url += f"?t={youtube_si.group('timestamp')}"

    return clean_url

async def spam_check(message: discord.Message) -> bool:
    if not isinstance(message.author, discord.Member):
        return False
    
    if not isinstance(message.channel, (discord.TextChannel, discord.Thread, discord.VoiceChannel)):
        return False

    if os.path.exists(MESSAGE_RATE_PATH):
        with open(MESSAGE_RATE_PATH, "r", encoding="utf-8") as f:
            message_rate = json.load(f)
    else:
        message_rate = {}

    user_id_str = str(message.author.id)
    now_timestamp = datetime_dt.now().timestamp()

    recent_messages = message_rate.get(user_id_str, [])

    recent_messages = [
        [msg_id, ts]
        for msg_id, ts in recent_messages
        if now_timestamp - ts < MESSAGE_RATE_WINDOW_SECONDS
    ]

    recent_messages.append([message.id, now_timestamp])

    # Keep only the most recent N messages for this user.
    recent_messages = recent_messages[-MESSAGE_RATE_MAX_STORED:]

    message_rate[user_id_str] = recent_messages

    with open(MESSAGE_RATE_PATH, "w", encoding="utf-8") as f:
        json.dump(message_rate, f, indent=2)

    if len(recent_messages) > MESSAGE_RATE_LIMIT:
        try:
            mod_channel = client.get_channel(MODERATOR_NOTICES_CHANNEL_ID)
            mod_role = message.guild.get_role(MODERATOR_ROLE_ID) if message.guild else None
            mod_mention = mod_role.mention if mod_role else "Moderators"
            if isinstance(mod_channel, discord.TextChannel):
                forwarded = await message.forward(mod_channel)
                await forwarded.reply(
                    f"{mod_mention}, User {message.author.mention} ({message.author.id}) is sending messages at a high rate and may be spamming. The above is the triggering message. I timed them out, but you may decide to ban them. Original channel: {message.channel.mention}"
                )
        except discord.Forbidden as e:
            logger.error(f"Failed to forward potential spam message from {message.author.name} to moderator channel due to permissions. E: {e}")

        try:
            await message.author.timeout(
                SPAM_TIMEOUT_DURATION,
                reason="Spamming messages",
            )
            logger.info(
                f"Timed out user {message.author.name} for spamming messages."
            )

            await message.author.send(
                f"{message.author.mention} You have been timed out for spamming messages. Please slow down to avoid further penalties."
            )

        except discord.Forbidden as e:
            logger.error(
                f"Failed to timeout user {message.author.name} for spamming. E: {e}"
            )
            await message.author.send(
                f"{message.author.mention} You have been detected spamming messages, but I was unable to timeout you due to permissions. Please slow down to avoid further penalties."
            )
        except Exception as e:
            logger.critical(e)

        return True

    return False

async def delete_spammed_messages(message: discord.Message) -> None:
    if not isinstance(message.author, discord.Member):
        return

    if not isinstance(
        message.channel,
        (discord.TextChannel, discord.Thread, discord.VoiceChannel),
    ):
        return

    if not os.path.exists(MESSAGE_RATE_PATH):
        return

    with open(MESSAGE_RATE_PATH, "r", encoding="utf-8") as f:
        message_rate = json.load(f)

    user_id_str = str(message.author.id)
    recent_messages = message_rate.get(user_id_str, [])

    now_timestamp = datetime_dt.now().timestamp()
    delete_window = MESSAGE_RATE_WINDOW_SECONDS * 2

    for msg_id, ts in recent_messages:
        if now_timestamp - ts >= delete_window:
            continue

        try:
            msg = await message.channel.fetch_message(msg_id)
            await msg.delete()

            logger.info(
                f"Deleted spam message with ID {msg_id} from user {message.author.name}."
            )

        except discord.NotFound:
            logger.warning(
                f"Could not find message with ID {msg_id} to delete for user {message.author.name}. "
                "It may have already been deleted, or it may be in another channel."
            )
        except discord.Forbidden as e:
            logger.error(
                f"Failed to delete spam message with ID {msg_id} from user {message.author.name} due to permissions. E: {e}"
            )
        except Exception as e:
            logger.critical(e)

async def send_suggested_poll(message: discord.Message):
    if not isinstance(message.author, discord.Member):
        return
    if not isinstance(message.channel, (discord.TextChannel, discord.Thread)):
        return
    
    poll = Poll(message.content.strip(), datetime.timedelta(hours=1))
    poll.add_answer(text="Yes")
    poll.add_answer(text="No")

    await message.reply("Here's a suggested poll based on your message content:", poll=poll)

from fun_stuff.brittishification import brittishify

@client.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    
    if message.author.id in banned_users:
        # delete the message and ban the user from the server if they are in the banned_users set
        try:
            await message.delete()
            logger.info(f"Deleted message from {message.author.name} due to being in banned_users list.")
            if message.guild is not None:
                await message.guild.ban(message.author, reason="User is banned", delete_message_seconds=3600)
                logger.info(f"Banned user {message.author.name} due to being in banned_users list.")
        except discord.Forbidden as e:
            logger.error(f'Failed to delete message from {message.author.name}. E: {e}')
        except Exception as e:
            logger.critical(e)
        return
    
    if (
        isinstance(message.author, discord.Member)
        and (
            (banned_substrings_regex and banned_substrings_regex.search(message.content))
            or
            (banned_words_regex and banned_words_regex.search(message.content))
            or
            (banned_regex_regex and banned_regex_regex.search(message.content))
        )
    ):
        try:
            await message.delete()
            logger.info(f"Deleted message from {message.author.name} due to containing a banned substring, word or pattern.")
            if message.guild is not None:
                await message.author.timeout(TIMEOUT_DURATION, reason="Message contained a banned substring, word or pattern")
                logger.info(f"Timed out user {message.author.name} for sending a message with a banned substring, word or pattern.")
            return # don't process the message further since it has been deleted and the user has been timed out
        except discord.Forbidden as e:
            logger.error(f'Failed to delete message from {message.author.name} or timeout user. E: {e}')
        except Exception as e:
            logger.critical(e)
        finally:
            if os.path.exists(os.path.join("banned_stuff", "warnings_sent.json")):
                with open(os.path.join("banned_stuff", "warnings_sent.json"), "r") as f:
                    warnings_sent = json.load(f)
                warnings_sent[str(message.author.id)] = warnings_sent.get(str(message.author.id), 0) + 1
                warning_ordinal = ordinal(warnings_sent[str(message.author.id)])
                await message.author.send(f"Your message contained a banned substring, word or pattern and has been removed. This is your {warning_ordinal} warning. Please adhere to the server rules to avoid further consequences.")
                with open(os.path.join("banned_stuff", "warnings_sent.json"), "w") as f:
                    json.dump(warnings_sent, f, indent=2)
            else:
                warnings_sent = {str(message.author.id): 1}
                warning_ordinal = ordinal(warnings_sent[str(message.author.id)])
                await message.author.send(f"Your message contained a banned substring, word or pattern and has been removed. This is your {warning_ordinal} warning. Please adhere to the server rules to avoid further consequences.")
                with open(os.path.join("banned_stuff", "warnings_sent.json"), "w") as f:
                    json.dump(warnings_sent, f, indent=2)
    
    if await spam_check(message):
        return # if the user is spamming messages, we timeout them and don't process the message further

    try:
        if message.content.strip()[-1] == "?":
            await send_suggested_poll(message)
    except IndexError:
        pass

    if cleaned := clean_youtube_si_link(message.content):
        try:
            await message.reply(f"{message.author.mention} Cleaned YouTube link with no SI tracking: {cleaned}")
            logger.info(f"Cleaned a YouTube SI link from {message.author.name}'s message.")
        except discord.Forbidden as e:
            logger.error(f'Failed to send cleaned YouTube link message in response to {message.author.name}. E: {e}')
        except Exception as e:
            logger.critical(e)

    ### Brittishification channel meme
    if message.channel.id == 1514557077912289280:
        try:
            britt_message, changed = brittishify(message.content)
            if changed:
                await message.reply(f"{message.author.mention} Here's your message Brittishified: {britt_message}")
                logger.info(f"Brittishified a message from {message.author.name}.")
        except discord.Forbidden as e:
            logger.error(f'Failed to send Brittishified message in response to {message.author.name}. E: {e}')
        except Exception as e:
            logger.critical(e)

    ### Pronoun channel meme
    content = message.content.replace("’", "'")
    if message.channel.id == 1510074902772711604 and PRONOUN_RE.search(content):
        pronouns = PRONOUN_RE.findall(content)

        # Dedupe while preserving order
        pronouns = list(dict.fromkeys(p.lower() for p in pronouns))

        p_string = ", ".join(f"`{p}`" for p in pronouns)

        await message.reply(
            f"This is a pronoun-free zone! You can't use {p_string} here."
        )
        try:
            await message.add_reaction("❌")
        except discord.Forbidden as e:
            logger.error(f'Failed to add reaction to pronoun message from {message.author.name}. E: {e}')
        except Exception as e:
            logger.critical(e)
        return

    ### No letter 'E' channel meme
    if message.channel.id == 1510301194575151285 and 'e' in message.content.lower() and message.author.id != 330283683246505996:
        await message.reply(
            "The letter 'E' is not allowed in this channel! Please remove it from your message."
        )
        try:
            await message.add_reaction("❌")
        except discord.Forbidden as e:
            logger.error(f'Failed to add reaction to message with letter E from {message.author.name}. E: {e}')
        except Exception as e:
            logger.critical(e)
        return
    if message.channel.id == 1510301194575151285 and message.author.id == 330283683246505996 and (set(re.sub(r'[^a-z]', '', message.content.lower())) - set('elfangorax')):
        await message.reply(
            f"For you {message.author.mention} you can only use the letters in 'Elfangorax' in this channel. Please remove any other letters from your message."
        )
        try:
            await message.add_reaction("❌")
        except discord.Forbidden as e:
            logger.error(f'Failed to add reaction to message with invalid letters from {message.author.name}. E: {e}')
        except Exception as e:
            logger.critical(e)
        return
    
    # you can only say letters in your own name
    if message.channel.id == 1512556314792952059 and set(re.sub(r'[^a-z]', '', message.content.lower())) - set(re.sub(r'[^a-z]', '', message.author.display_name.lower())):
        await message.reply(
            f"For you {message.author.mention} you can only use the letters that are in {message.author.display_name} in this channel. Please remove any other letters from your message."
        )
        try:
            await message.add_reaction("❌")
        except discord.Forbidden as e:
            logger.error(f'Failed to add reaction to message with invalid letters from {message.author.display_name}. E: {e}')
        except Exception as e:
            logger.critical(e)
        return

    ### Counting game
    if message.channel.id == 1508224353488212050 and IS_INT_NUMBERS.match(message.content):
        counting_path = os.path.join("games", "counting.json")
        os.makedirs("games", exist_ok=True)
        try:
            with open(counting_path, "r") as f:
                counting_data = json.load(f)
        except FileNotFoundError:
            counting_data = {}
        
        count_total, last_counter_id = counting_data.get("count_total", 0), counting_data.get("last_counter_id", None)
        submitted_number = int(re.sub(r'\D', '', message.content)) # just to validate that the content is an int, since the regex allows for whitespace around it
        if last_counter_id == message.author.id:
            await message.add_reaction("🟡")
            await message.channel.send(f"{message.author.mention} You can't count twice in a row! The count was {count_total} and is still {count_total}.")
        else:
            if submitted_number == count_total + 1:
                count_total += 1
                counting_data["count_total"] = count_total
                counting_data["last_counter_id"] = message.author.id
                with open(counting_path, "w") as f:
                    json.dump(counting_data, f)
                await message.add_reaction("✅")
            else:
                await message.add_reaction("❌")
                await message.channel.send(f"{message.author.mention} Your number must be exactly 1 higher than the previous number! The count was {count_total} and is now 0.")
                counting_data["count_total"] = 0
                counting_data["last_counter_id"] = None
                with open(counting_path, "w") as f:
                    json.dump(counting_data, f)




    user_row = (
        message.author.id,
        message.author.name,
        getattr(message.author, "global_name", None),
        message.author.bot,
    )

    message_row = (
        message.id,
        message.author.id,
        message.channel.id,
        message.guild.id if message.guild else None,
        message.content,
    )

    try:
        should_flush = False

        async with client.user_buffer_lock:
            client.user_buffer[message.author.id] = user_row

        async with client.message_buffer_lock:
            client.message_buffer.append(message_row)
            if len(client.message_buffer) >= BATCH_SIZE:
                should_flush = True

        if should_flush:
            await flush_buffers()

    except Exception as e:
        logger.error(f"Error buffering message/user: {e}", exc_info=True)

@client.event
async def on_voice_state_update(
    member: discord.Member,
    before: discord.VoiceState,
    after: discord.VoiceState
):
    if member.bot:
        return

    target_user_id = 288219032178393092
    # target_user_id = 262687596642041856

    # Only fire when the user was not in voice before, and is now in voice
    if member.id != target_user_id:
        return

    if before.channel is not None or after.channel is None:
        return

    if os.getenv("pushover_notify_on_exit") != "True":
        return

    import httpx

    try:
        async with httpx.AsyncClient(timeout=10) as http:
            await http.post(
                "https://api.pushover.net/1/messages.json",
                data={
                    "token": os.getenv("pushover_app_token"),
                    "user": os.getenv("pushover_user_key"),
                    "message": (
                        f"{member.name} joined voice channel "
                        f"{after.channel.name} at {datetime_dt.now().isoformat()}."
                    ),
                    "priority": 1,
                    "ttl": 3600,
                },
            )
    except Exception as e:
        logger.error(f"Error sending pushover notification: {e}", exc_info=True)

SENDABLE_CHANNEL_TYPES = (
    discord.TextChannel,
    discord.Thread,
)

def ordinal(n: int) -> str:
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"

@tree.command(name="russianroulette", description="1/6 chance to get a timeout")
async def russian_roulette(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command can only be used in a server.",
            ephemeral=True,
        )
        return
    
    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message(
            "Could not verify your server permissions.",
            ephemeral=True,
        )
        return

    if not isinstance(interaction.channel, SENDABLE_CHANNEL_TYPES):
        await interaction.response.send_message(
            "This channel type does not support sending messages like that.",
            ephemeral=True,
        )
        return
    
    # check if channel id is 1508163397571842098, which is the channel ID for the #bot-stuff channel in the server, and if not, return an error message saying that this command can only be used in the #bot-stuff channel
    if interaction.channel.id != 1508163397571842098:
        await interaction.response.send_message(
            "This command can only be used in the #bot-stuff channel.",
            ephemeral=True,
        )
        return
    
    russian_roulette_stats_path = os.path.join("games", "russian_stats.json")
    os.makedirs("games", exist_ok=True)

    user_id = str(interaction.user.id)
    try:
        with open(russian_roulette_stats_path, "r") as f:
            stats = json.load(f)
    except FileNotFoundError:
        stats = {}
    
    failures, wins = stats.get(user_id, [0, 0])

    if random.randint(1, 6) == 1:
        try:
            failures += 1
            fail_count = ordinal(failures)

            mins_timed_out = int(GAME_TIMEOUT_DURATION.total_seconds() // 60)
            await interaction.user.timeout(GAME_TIMEOUT_DURATION, reason="Russian Roulette")
            await interaction.response.send_message(
                f"You got the unlucky outcome and have been timed out for {mins_timed_out} minutes! This is your {fail_count} failure.")
        except discord.Forbidden as e:
            logger.error(f'Failed to timeout user {interaction.user.name} in russian roulette. E: {e}')
            await interaction.response.send_message(
                "You got the unlucky outcome but I don't have permission to time you out.")
        except Exception as e:
            logger.critical(e)
            await interaction.response.send_message(
                "An error occurred while trying to time you out for losing at Russian Roulette.")
    else:
        wins += 1
        win_count = ordinal(wins)

        await interaction.response.send_message(
            f"Congratulations, you won at Russian Roulette and are safe! This is your {win_count} win.")
    
    # save updated stats back to json file
    stats[user_id] = [failures, wins]
    with open(russian_roulette_stats_path, "w") as f:
        json.dump(stats, f)

@tree.command(name="echo")
async def echomode(
    interaction: discord.Interaction,
    msg: str,
    file: discord.Attachment | None = None,
    user: discord.User | None = None,
):
    channel = interaction.channel

    if interaction.guild is None or channel is None:
        await interaction.response.send_message(
            "This command can only be used in a server.",
            ephemeral=True
        )
        return

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message(
            "Could not verify your server permissions.",
            ephemeral=True
        )
        return

    if not isinstance(channel, SENDABLE_CHANNEL_TYPES):
        await interaction.response.send_message(
            "This channel type does not support sending messages like that.",
            ephemeral=True
        )
        return

    if not channel.permissions_for(interaction.user).manage_messages:
        await interaction.response.send_message(
            "Permission not granted lmao",
            ephemeral=True
        )
        return

    try:
        discord_file: discord.File | None = None

        if file is not None:
            file_bytes = await file.read()
            discord_file = discord.File(
                io.BytesIO(file_bytes),
                filename=file.filename
            )

        if user is not None:
            if discord_file is not None:
                await user.send(content=msg, file=discord_file)
            else:
                await user.send(content=msg)
        else:
            if discord_file is not None:
                await channel.send(content=msg, file=discord_file)
            else:
                await channel.send(content=msg)

        await interaction.response.send_message("Done!", ephemeral=True)

    except discord.Forbidden:
        await interaction.response.send_message(
            "I couldn't send that due to permissions.",
            ephemeral=True
        )
    except Exception as e:
        logger.error(f"Error in echo command: {e}", exc_info=True)
        await interaction.response.send_message(
            "Something went wrong while sending that.",
            ephemeral=True
        )

from pathlib import Path

from fun_stuff.quizing import QuizHandler

BASE_DIR = Path(__file__).parent

quiz_path = BASE_DIR / "quiz_stuff" / "questions.jsonl"
quiz_path.parent.mkdir(parents=True, exist_ok=True)

quiz_handler = QuizHandler(str(quiz_path))


def get_quiz_scope_id(interaction: discord.Interaction) -> str:
    """
    Use guild ID for server-wide quizzes.
    Fall back to user ID for DMs.
    """
    return str(interaction.guild.id if interaction.guild else interaction.user.id)


def ensure_questions_loaded() -> bool:
    """
    Try to reload questions if none are currently loaded.
    Returns True if questions are available.
    """
    if not quiz_handler.questions:
        quiz_handler.reload_questions()

    return bool(quiz_handler.questions)


@tree.command(name="question")
async def ask_question(interaction: discord.Interaction):
    id_for_quiz = get_quiz_scope_id(interaction)

    if not ensure_questions_loaded():
        await interaction.response.send_message(
            "No quiz questions are available at the moment.",
            ephemeral=True,
        )
        logger.debug("No quiz questions are available when /question command was invoked.")
        return

    question = quiz_handler.get_current_question(id_for_quiz)

    if question:
        await interaction.response.send_message(
            f"**Quiz Question:** {question['question']}"
        )
    else:
        await interaction.response.send_message(
            "No quiz question is available.",
            ephemeral=True,
        )
        logger.debug("No quiz question is available when /question command was invoked.")

def make_progress_bar(current_index: int, total: int, width: int = 10) -> str:
    """
    current_index is the next question index.
    So:
    - 0 means 0 questions answered
    - 1 means 1 question answered
    - -1 means quiz complete
    """
    if total <= 0:
        return "Progress: No questions."

    if current_index == -1:
        answered = total
    else:
        answered = max(0, min(current_index, total))

    progress = answered / total

    filled = round(progress * width)
    empty = width - filled

    bar = "█" * filled + "░" * empty

    return f"Progress: `[{bar}]` {answered} of {total}"

@tree.command(name="answer")
async def answer_question(interaction: discord.Interaction, answer: str):
    id_for_quiz = get_quiz_scope_id(interaction)

    if not ensure_questions_loaded():
        await interaction.response.send_message(
            "No quiz questions are available at the moment.",
            ephemeral=True,
        )
        logger.debug("No quiz questions are available when /answer command was invoked.")
        return

    question_index = quiz_handler.get_question_index(id_for_quiz)
    correct, real_answer = quiz_handler.check_answer(answer, question_index)

    if real_answer == "":
        await interaction.response.send_message(
            "No quiz question is available.",
            ephemeral=True,
        )
        logger.debug("No quiz question is available when /answer command was invoked.")
        return

    if correct:
        response_text = f"Correct, {interaction.user.mention}! The answer is {real_answer}."
    else:
        response_text = f"Incorrect, {interaction.user.mention}. The correct answer was: {real_answer}"

    quiz_handler.advance_question_index(id_for_quiz)

    question_index, total_questions = quiz_handler.check_progress(id_for_quiz)
    progress_text = make_progress_bar(question_index, total_questions)

    await interaction.response.send_message(f"{response_text}\n{progress_text}")

    if question_index == -1:
        await interaction.followup.send(
            f"{interaction.user.mention}, you've completed the quiz!"
        )

@tree.command(name="reload_questions")
async def reload_questions(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command can only be used in a server.",
            ephemeral=True,
        )
        return

    if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            "Not permitted to reload quiz questions.",
            ephemeral=True,
        )
        return

    quiz_handler.reload_questions()

    if quiz_handler.questions:
        await interaction.response.send_message(
            f"Reloaded quiz questions. {len(quiz_handler.questions)} questions are now available."
        )
    else:
        await interaction.response.send_message(
            "Reloaded quiz questions, but no questions are currently available.",
            ephemeral=True,
        )

@tree.command(name="reset_quiz")
async def reset_quiz(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command can only be used in a server.",
            ephemeral=True,
        )
        return

    if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            "Not permitted to reset quiz progress.",
            ephemeral=True,
        )
        return

    id_for_quiz = get_quiz_scope_id(interaction)
    quiz_handler.reset_progress(id_for_quiz)

    await interaction.response.send_message(
        "Quiz progress has been reset."
    )

from twitch_stuff.stream_info import get_twitch_stream_title

DISCORD_LIVE_CHANNEL_ID = int(os.environ["DISCORD_LIVE_CHANNEL_ID"])
IFTTT_SHARED_SECRET = os.environ["IFTTT_SHARED_SECRET"]
TWITCH_USERNAME = os.environ["TWITCH_USERNAME"]


# async def handle_ifttt_live(request: web.Request):
#     secret = request.headers.get("X-Webhook-Secret")

#     if secret != IFTTT_SHARED_SECRET:
#         logger.warning("Received IFTTT webhook with invalid secret.")
#         return web.Response(status=403, text="Forbidden")

#     data = await request.json()

#     category = data.get("category", "Unknown category")
#     url = data.get("url", f"https://twitch.tv/{TWITCH_USERNAME}")
#     if category == "Unknown category":
#         logger.warning("Received IFTTT webhook with missing category.")

#     channel = client.get_channel(DISCORD_LIVE_CHANNEL_ID)
#     if channel is None:
#         channel = await client.fetch_channel(DISCORD_LIVE_CHANNEL_ID)
    
#     if not isinstance(channel, SENDABLE_CHANNEL_TYPES):
#         logger.error(f"Channel with ID {DISCORD_LIVE_CHANNEL_ID} is not a text channel or thread.")
#         return web.Response(status=500, text="Channel configuration error")

#     # get stream title with yt-dlp --skip-download --print description "https://www.twitch.tv/username"
#     stream_title, started_at = get_twitch_stream_title(url)

#     if stream_title is None:
#         stream_title = "Unknown title"
#         logger.warning("Received IFTTT webhook but could not retrieve stream title.")
#     if started_at is None:
#         started_text = "Unknown start time"
#         logger.warning("Received IFTTT webhook but could not retrieve stream start time.")
#     else:
#         started_text = f"<t:{started_at}:f>"

#     sent_message = await channel.send(
#         f"@everyone\n"
#         f"🔴 **{TWITCH_USERNAME} is live!**\n"
#         f"Playing: {category}\n"
#         f"Title: {stream_title}\n"
#         f"Started at: {started_text}\n"
#         f"{url}"
#     )

#     try:
#         await sent_message.publish()
#     except discord.Forbidden:
#         logger.error(f"Failed to publish announcement message in {channel.name} due to permissions.")
#     except discord.HTTPException as e:
#         logger.error(f"Failed to publish announcement message in {channel.name}. HTTPException: {e}")

#     return web.Response(text="OK")

from twitch_stuff.live_check import twitch_is_live

@tree.command(name="newcategory")
async def new_category(interaction: discord.Interaction, category: str):
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command can only be used in a server.",
            ephemeral=True,
        )
        return

    if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            "Not permitted to change the category.",
            ephemeral=True,
        )
        return

    if not twitch_is_live(TWITCH_USERNAME):
        await interaction.response.send_message(
            f"{TWITCH_USERNAME} is not currently live on Twitch. Cannot change category.",
            ephemeral=True,
        )
        return

    twitch_category = category.strip()
    twitch_title, _ = get_twitch_stream_title(f"https://twitch.tv/{TWITCH_USERNAME}")
    if twitch_title is None:
        twitch_title = "Unknown title"

    logger.info(f'Changed Twitch category to "{twitch_category}" via command.')
    await interaction.response.send_message(
        f'@everyone\nTwitch category has been changed to: "{twitch_category}".\nStream title is now: "{twitch_title}".',
        ephemeral=False,
    )


if __name__ == '__main__':
    logger.debug("Starting bot")

    crashed = False
    try:    
        client.run(os.getenv("discord_api_key",""))
    except Exception as e:
        logger.error(f"Error running bot: {e}", exc_info=True)
        crashed = True
        if os.getenv("pdb_postmortem") == "True":
            pdb.post_mortem()
    finally:
        if os.getenv("pushover_notify_on_exit") == "True":
            # send pushover notification on bot crash/exit
            import httpx
            try:
                httpx.post("https://api.pushover.net/1/messages.json", data={
                    "token": os.getenv("pushover_app_token"),
                    "user": os.getenv("pushover_user_key"),
                    "message": f"Bot has {'crashed' if crashed else 'stopped'} at {datetime_dt.now().isoformat()}. Check logs for details.",
                    "priority": 1 if crashed else 0,
                    "ttl": 3600 if not crashed else 604800 # 1 hour for normal stop, 1 week for crash
                }, timeout=10)
            except Exception as notify_e:
                logger.error(f"Error sending pushover notification: {notify_e}", exc_info=True)
