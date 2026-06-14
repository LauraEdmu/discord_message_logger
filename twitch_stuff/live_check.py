
import subprocess


def twitch_is_live(username: str) -> bool:
    username = username.strip().removeprefix("@")
    url = f"https://www.twitch.tv/{username}"

    result = subprocess.run(
        [
            "yt-dlp",
            "--skip-download",
            "--no-warnings",
            "--print",
            "%(live_status)s",
            url,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=20,
    )

    stdout = result.stdout.strip()
    stderr = result.stderr.strip().lower()

    if result.returncode == 0:
        return stdout == "is_live"

    # Twitch offline channels often make yt-dlp exit non-zero instead of cleanly
    # printing "not_live", so treat the known offline case as False.
    if "not currently live" in stderr or "offline" in stderr:
        return False

    raise RuntimeError(f"yt-dlp failed: {result.stderr.strip()}")


if __name__ == "__main__":
    user = input("Twitch username: ")
    print("LIVE" if twitch_is_live(user) else "offline")
