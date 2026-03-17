"""
Post about new Criterion Closet Picks guests to X/Twitter and Threads.

Usage:
    # Dry-run: detect new guests via git diff and show composed posts
    python scripts/post_new_guests.py --dry-run

    # Dry-run for a specific guest
    python scripts/post_new_guests.py --dry-run --guest-slug shinichiro-watanabe

    # Post to all configured platforms
    python scripts/post_new_guests.py --guest-slug shinichiro-watanabe

    # Post with custom text (from conversational editing)
    python scripts/post_new_guests.py --guest-slug shinichiro-watanabe --text "Custom post text here"

    # Platform-specific
    python scripts/post_new_guests.py --guest-slug shinichiro-watanabe --twitter-only
    python scripts/post_new_guests.py --guest-slug shinichiro-watanabe --threads-only
"""

import argparse
import json
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import GUESTS_FILE, PICKS_FILE, get_env, load_json, log

SITE_URL = "closetpicks.westenb.org"
X_CHAR_LIMIT = 280
THREADS_CHAR_LIMIT = 500


# ---------------------------------------------------------------------------
# Credential checks
# ---------------------------------------------------------------------------

def has_twitter_creds() -> bool:
    return all(get_env(k, required=False) for k in (
        "TWITTER_API_KEY", "TWITTER_API_SECRET",
        "TWITTER_ACCESS_TOKEN", "TWITTER_ACCESS_SECRET",
    ))


def has_threads_creds() -> bool:
    return all(get_env(k, required=False) for k in (
        "THREADS_ACCESS_TOKEN", "THREADS_USER_ID",
    ))


# ---------------------------------------------------------------------------
# Guest detection
# ---------------------------------------------------------------------------

def get_new_guests() -> list[dict]:
    """Compare current guests.json to HEAD~1 to find newly added guests."""
    current = load_json(GUESTS_FILE)
    current_slugs = {g["slug"] for g in current}

    try:
        result = subprocess.run(
            ["git", "show", "HEAD~1:data/guests.json"],
            capture_output=True, text=True, check=True,
        )
        previous = json.loads(result.stdout)
        previous_slugs = {g["slug"] for g in previous}
    except (subprocess.CalledProcessError, json.JSONDecodeError):
        log("Could not read previous guests.json from git — treating all as new")
        return current

    new_slugs = current_slugs - previous_slugs
    return [g for g in current if g["slug"] in new_slugs]


def get_guest_by_slug(slug: str) -> dict | None:
    """Load a single guest by slug."""
    guests = load_json(GUESTS_FILE)
    for g in guests:
        if g["slug"] == slug:
            return g
    return None


# ---------------------------------------------------------------------------
# Pick loading
# ---------------------------------------------------------------------------

def get_guest_picks(slug: str) -> list[dict]:
    """Load picks for a guest."""
    picks = load_json(PICKS_FILE)
    return [p for p in picks if p.get("guest_slug") == slug]


# ---------------------------------------------------------------------------
# Post composition
# ---------------------------------------------------------------------------

def compose_post(guest: dict, picks: list[dict], char_limit: int) -> str:
    """Compose a post about a guest, truncating the film list to fit char_limit."""
    name = guest.get("name", "Unknown")
    profession = guest.get("profession", "")
    slug = guest["slug"]
    pick_count = guest.get("pick_count", len(picks))

    # Build header and footer
    if profession:
        header = f"New on Closet Picks: {name} ({profession}) picked {pick_count} films.\n\n"
    else:
        header = f"New on Closet Picks: {name} picked {pick_count} films.\n\n"

    footer = f"\n\nAll picks + quotes: {SITE_URL}/guests/{slug}/"

    # Calculate space for film list
    available = char_limit - len(header) - len(footer)

    # Build film list, truncating if needed
    film_titles = []
    for p in picks:
        title = p.get("film_title", p.get("title", "Unknown"))
        film_titles.append(title)

    lines = []
    remaining = 0
    for i, title in enumerate(film_titles):
        line = f"- {title}"
        # Check if adding this line would still leave room
        # If there are more films after this one, we need space for the suffix too
        test_lines = lines + [line]
        films_after = len(film_titles) - i - 1
        if films_after > 0:
            suffix = f"...and {films_after} more"
            test_text = "\n".join(test_lines + [suffix])
        else:
            test_text = "\n".join(test_lines)
        if len(test_text) > available:
            remaining = len(film_titles) - i
            break
        lines.append(line)

    if remaining > 0:
        lines.append(f"...and {remaining} more")

    film_list = "\n".join(lines)
    return header + film_list + footer


# ---------------------------------------------------------------------------
# Posting: X/Twitter
# ---------------------------------------------------------------------------

def post_to_twitter(text: str) -> str | None:
    """Post a tweet via tweepy. Returns tweet URL or None on failure."""
    try:
        import tweepy
    except ImportError:
        log("tweepy not installed — run: pip install tweepy")
        return None

    client = tweepy.Client(
        consumer_key=get_env("TWITTER_API_KEY"),
        consumer_secret=get_env("TWITTER_API_SECRET"),
        access_token=get_env("TWITTER_ACCESS_TOKEN"),
        access_token_secret=get_env("TWITTER_ACCESS_SECRET"),
    )

    response = client.create_tweet(text=text)
    tweet_id = response.data["id"]
    # Get username for URL
    me = client.get_me()
    username = me.data.username
    url = f"https://x.com/{username}/status/{tweet_id}"
    log(f"Posted to X: {url}")
    return url


# ---------------------------------------------------------------------------
# Posting: Threads
# ---------------------------------------------------------------------------

def maybe_refresh_threads_token() -> None:
    """Refresh the Threads long-lived token if it's >50 days old."""
    created_str = get_env("THREADS_TOKEN_CREATED", required=False)
    if not created_str:
        return

    try:
        created = date.fromisoformat(created_str.strip())
    except ValueError:
        log(f"Invalid THREADS_TOKEN_CREATED date: {created_str}")
        return

    age_days = (date.today() - created).days
    if age_days <= 50:
        log(f"Threads token is {age_days} days old — no refresh needed")
        return

    log(f"Threads token is {age_days} days old — refreshing...")
    token = get_env("THREADS_ACCESS_TOKEN")

    import requests
    resp = requests.get(
        "https://graph.threads.net/refresh_access_token",
        params={"grant_type": "th_refresh_token", "access_token": token},
    )
    resp.raise_for_status()
    new_token = resp.json()["access_token"]

    # Update .env file in place
    env_path = Path(__file__).resolve().parent.parent / ".env"
    env_text = env_path.read_text()
    today = date.today().isoformat()

    import re
    env_text = re.sub(
        r"^THREADS_ACCESS_TOKEN=.*$",
        f"THREADS_ACCESS_TOKEN={new_token}",
        env_text, flags=re.MULTILINE,
    )
    env_text = re.sub(
        r"^THREADS_TOKEN_CREATED=.*$",
        f"THREADS_TOKEN_CREATED={today}",
        env_text, flags=re.MULTILINE,
    )
    env_path.write_text(env_text)
    log(f"Threads token refreshed — new expiry ~{today} + 60 days")


def post_to_threads(text: str) -> str | None:
    """Post to Threads via Graph API (two-step: create container, publish)."""
    import requests

    maybe_refresh_threads_token()

    token = get_env("THREADS_ACCESS_TOKEN")
    user_id = get_env("THREADS_USER_ID")

    # Step 1: Create media container
    resp = requests.post(
        f"https://graph.threads.net/v1.0/{user_id}/threads",
        params={
            "media_type": "TEXT",
            "text": text,
            "access_token": token,
        },
    )
    resp.raise_for_status()
    container_id = resp.json()["id"]

    # Step 2: Publish
    resp = requests.post(
        f"https://graph.threads.net/v1.0/{user_id}/threads_publish",
        params={
            "creation_id": container_id,
            "access_token": token,
        },
    )
    resp.raise_for_status()
    post_id = resp.json()["id"]

    url = f"https://www.threads.net/@user/post/{post_id}"
    log(f"Posted to Threads (ID: {post_id})")
    return url


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Post about new Closet Picks guests")
    parser.add_argument("--dry-run", action="store_true", help="Show posts without sending")
    parser.add_argument("--guest-slug", help="Post about a specific guest (skip git detection)")
    parser.add_argument("--text", help="Use custom post text (bypass template)")
    parser.add_argument("--twitter-only", action="store_true", help="Post to X/Twitter only")
    parser.add_argument("--threads-only", action="store_true", help="Post to Threads only")
    args = parser.parse_args()

    # Determine which guests to post about
    if args.guest_slug:
        guest = get_guest_by_slug(args.guest_slug)
        if not guest:
            log(f"Guest not found: {args.guest_slug}")
            sys.exit(1)
        guests = [guest]
    else:
        guests = get_new_guests()

    if not guests:
        log("No new guests found.")
        return

    log(f"Found {len(guests)} guest(s) to post about")

    for guest in guests:
        picks = get_guest_picks(guest["slug"])
        name = guest.get("name", guest["slug"])

        print(f"\n{'='*60}")
        print(f"Guest: {name} ({len(picks)} picks)")
        print(f"{'='*60}")

        if args.text:
            x_text = args.text
            threads_text = args.text
        else:
            x_text = compose_post(guest, picks, X_CHAR_LIMIT)
            threads_text = compose_post(guest, picks, THREADS_CHAR_LIMIT)

        # Show X post
        if not args.threads_only:
            print(f"\n--- X/Twitter ({len(x_text)}/{X_CHAR_LIMIT} chars) ---")
            print(x_text)

        # Show Threads post
        if not args.twitter_only:
            print(f"\n--- Threads ({len(threads_text)}/{THREADS_CHAR_LIMIT} chars) ---")
            print(threads_text)

        if args.dry_run:
            print("\n[DRY RUN — not posting]")
            continue

        # Post to X
        if not args.threads_only:
            if has_twitter_creds():
                post_to_twitter(x_text)
            else:
                log("X/Twitter: skipped (no credentials)")

        # Post to Threads
        if not args.twitter_only:
            if has_threads_creds():
                post_to_threads(threads_text)
            else:
                log("Threads: skipped (no credentials)")


if __name__ == "__main__":
    main()
