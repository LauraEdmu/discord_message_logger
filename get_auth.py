# make_twitch_tokens.py

import json
import os
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer

from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.environ["TWITCH_CLIENT_ID"]
CLIENT_SECRET = os.environ["TWITCH_CLIENT_SECRET"]

REDIRECT_URI = "http://localhost:17564"
TOKEN_FILE = os.getenv("TWITCH_TOKEN_FILE", "twitch_tokens.json")

auth_code = None


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code

        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)

        auth_code = qs.get("code", [None])[0]

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"You can close this tab now.")

    def log_message(self, format, *args):
        return


params = {
    "client_id": CLIENT_ID,
    "redirect_uri": REDIRECT_URI,
    "response_type": "code",
    "scope": "",
}

auth_url = "https://id.twitch.tv/oauth2/authorize?" + urllib.parse.urlencode(params)

print("Opening Twitch auth page...")
webbrowser.open(auth_url)

server = HTTPServer(("localhost", 17564), Handler)
server.handle_request()

if auth_code is None:
    raise RuntimeError("No auth code received")

data = urllib.parse.urlencode({
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "code": auth_code,
    "grant_type": "authorization_code",
    "redirect_uri": REDIRECT_URI,
}).encode()

req = urllib.request.Request(
    "https://id.twitch.tv/oauth2/token",
    data=data,
    method="POST",
)

with urllib.request.urlopen(req) as resp:
    token_data = json.loads(resp.read().decode())

with open(TOKEN_FILE, "w", encoding="utf-8") as f:
    json.dump(
        {
            "access_token": token_data["access_token"],
            "refresh_token": token_data["refresh_token"],
        },
        f,
        indent=2,
    )

print(f"Wrote {TOKEN_FILE}")