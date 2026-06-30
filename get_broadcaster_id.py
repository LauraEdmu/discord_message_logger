import json
import os
import urllib.parse
import urllib.request
from urllib.error import HTTPError

from dotenv import load_dotenv

load_dotenv()

client_id = os.environ["TWITCH_CLIENT_ID"]
token_file = os.getenv("TWITCH_TOKEN_FILE", "twitch_tokens.json")

login = os.getenv("TWITCH_USERNAME") or input("Twitch login: ").strip()

with open(token_file, "r", encoding="utf-8") as f:
    access_token = json.load(f)["access_token"]

url = "https://api.twitch.tv/helix/users?" + urllib.parse.urlencode({
    "login": login,
})

req = urllib.request.Request(
    url,
    headers={
        "Client-Id": client_id,
        "Authorization": f"Bearer {access_token}",
    },
)

try:
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode("utf-8"))
except HTTPError as e:
    print(e.read().decode("utf-8"))
    raise

users = data.get("data", [])

if not users:
    raise RuntimeError(f"No Twitch user found for login: {login!r}")

user = users[0]

print(f'Twitch login: {user["login"]}')
print(f'Display name: {user["display_name"]}')
print()
print(f'TWITCH_BROADCASTER_ID={user["id"]}')