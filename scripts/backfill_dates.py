#!/usr/bin/env python3
"""
Backfill episode_date for guests using the YouTube Data API v3.

Fetches snippet.publishedAt for all youtube_video_ids missing an episode_date,
batching up to 50 IDs per request. Skips guests/visits that already have dates.

Requires YOUTUBE_API_KEY in .env.
"""

import os
import sys

import requests

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import GUESTS_FILE, load_json, save_json, log


YOUTUBE_API_URL = "https://www.googleapis.com/youtube/v3/videos"
BATCH_SIZE = 50


def fetch_upload_dates(video_ids: list[str], api_key: str) -> dict[str, str]:
    """Fetch upload dates for video IDs via YouTube Data API v3.

    Returns dict of {video_id: "YYYY-MM-DD"}.
    """
    dates = {}
    for i in range(0, len(video_ids), BATCH_SIZE):
        batch = video_ids[i : i + BATCH_SIZE]
        log(f"  Fetching batch {i // BATCH_SIZE + 1} ({len(batch)} videos)...")
        resp = requests.get(
            YOUTUBE_API_URL,
            params={
                "part": "snippet",
                "id": ",".join(batch),
                "key": api_key,
                "fields": "items(id,snippet/publishedAt)",
            },
        )
        resp.raise_for_status()
        for item in resp.json().get("items", []):
            published = item["snippet"]["publishedAt"]  # e.g. 2024-01-15T18:00:00Z
            dates[item["id"]] = published[:10]  # YYYY-MM-DD
    return dates


def main():
    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        log("ERROR: YOUTUBE_API_KEY not set in environment. Add it to .env.")
        sys.exit(1)

    guests = load_json(GUESTS_FILE)
    log(f"Loaded {len(guests)} guests")

    # Collect video IDs that need dates
    needs_date = {}  # video_id -> list of (guest_index, visit_index_or_None)
    for gi, guest in enumerate(guests):
        if guest.get("visits"):
            for vi, visit in enumerate(guest["visits"]):
                vid = visit.get("youtube_video_id")
                if vid and not visit.get("episode_date"):
                    needs_date.setdefault(vid, []).append((gi, vi))
        else:
            vid = guest.get("youtube_video_id")
            if vid and not guest.get("episode_date"):
                needs_date.setdefault(vid, []).append((gi, None))

    if not needs_date:
        log("All guests already have episode_date. Nothing to do.")
        return

    log(f"Fetching dates for {len(needs_date)} videos...")
    dates = fetch_upload_dates(list(needs_date.keys()), api_key)

    # Apply dates
    filled = 0
    failures = []
    for vid, locations in needs_date.items():
        date = dates.get(vid)
        if not date:
            failures.append(vid)
            continue
        for gi, vi in locations:
            if vi is not None:
                guests[gi]["visits"][vi]["episode_date"] = date
            else:
                guests[gi]["episode_date"] = date
            filled += 1

    save_json(GUESTS_FILE, guests)

    # Summary
    all_dates = []
    for guest in guests:
        if guest.get("visits"):
            for visit in guest["visits"]:
                if visit.get("episode_date"):
                    all_dates.append(visit["episode_date"])
        elif guest.get("episode_date"):
            all_dates.append(guest["episode_date"])

    log(f"Done! Filled {filled} episode dates.")
    if all_dates:
        all_dates.sort()
        log(f"Date range: {all_dates[0]} to {all_dates[-1]}")
    if failures:
        log(f"Failed to fetch {len(failures)} videos: {failures}")
    else:
        log("No failures.")


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    main()
