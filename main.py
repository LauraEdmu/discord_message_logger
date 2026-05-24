#!/usr/bin/env python3
import discord
from discord import app_commands
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

default_duration = 60
TIMEOUT_DURATION = timedelta(minutes=default_duration) # for banned word timeout

needed_intents = discord.Intents.default()
needed_intents.message_content = True
needed_intents.members = True
# client = discord.Client(intents=needed_intents)
# tree = app_commands.CommandTree(client)

class MyClient(discord.Client):
    db_pool: asyncpg.Pool
    startup_done: bool = False
    start_time: datetime_dt

    message_buffer: list[tuple[int, int, int, int | None, str]]
    message_buffer_lock: asyncio.Lock

    user_buffer: dict[int, tuple[int, str, str | None, bool]]
    user_buffer_lock: asyncio.Lock

    flush_task: asyncio.Task | None = None

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

    async def close(self):
        try:
            await flush_buffers()
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
    if os.path.exists("banned_users.json"):
        with open("banned_users.json", "r") as f:
            global banned_users
            banned_users = set(json.load(f))
    else:
        banned_users = set()

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
            """, user_row)
    except Exception as e:
        logger.error(f"Error logging new member info: {e}", exc_info=True)
    
    try:
        await member.send(fr"Welcome to the server, {member.display_name}!")

        logger.debug(f"Send onboarding message to {member.display_name}")
    except discord.Forbidden as e:
        logger.error(f'Failed to message {member.name} on join. E: {e}')
    except Exception as e:
        logger.critical(e)

    guild = member.guild
    role = guild.get_role(1508144612370419835) # role ID for "Apprentices" role, which is given to new members on join
    logger.debug(f"Member {member.name} joined")
    
    if role:
        try:
            await member.add_roles(role)
            logger.info(f'Assigned {role.name} role to {member.name}')
            print(f'Assigned {role.name} role to {member.name}')
        except discord.Forbidden as e:
            logger.error(f'Failed to assign {role.name} role to {member.name}. E: {e}')
            print(f'Failed to assign {role.name} role to {member.name}')

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
    with open("banned_users.json", "w") as f:
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
        with open("banned_users.json", "w") as f:
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
    


@client.event
async def on_message(message: discord.Message):
    if message.author == client.user:
        return

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

    if random.randint(1, 6) == 1:
        try:
            await interaction.user.timeout(TIMEOUT_DURATION, reason="Russian Roulette")
            await interaction.response.send_message(
                f"You got the unlucky outcome and have been timed out for {TIMEOUT_DURATION} minutes!",
                ephemeral=True,
            )
        except discord.Forbidden as e:
            logger.error(f'Failed to timeout user {interaction.user.name} in russian roulette. E: {e}')
            await interaction.response.send_message(
                "You got the unlucky outcome but I don't have permission to time you out.",
                ephemeral=True,
            )
        except Exception as e:
            logger.critical(e)
            await interaction.response.send_message(
                "An error occurred while trying to time you out for losing at Russian Roulette.",
                ephemeral=True,
            )
    else:
        await interaction.response.send_message(
            "Congratulations, you won at Russian Roulette and are safe!",
            ephemeral=True,
        )

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
