#!/usr/bin/env python3
"""
Match YouTube Closet Picks playlist videos to guests.
Uses yt-dlp for playlist metadata and youtube-transcript-api for transcripts.

Output: updated guests.json + data/transcripts/{video_id}.json
"""

import argparse
import json
import re
import subprocess
import sys
import time

from tqdm import tqdm

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import (
    GUESTS_FILE,
    TRANSCRIPTS_DIR,
    PILOT_GUESTS,
    load_json,
    save_json,
    log,
    fuzzy_match_name,
    fuzzy_match_score,
    slugify,
)

# Criterion Collection Closet Picks playlists
PLAYLIST_URL = "https://www.youtube.com/playlist?list=PL7D89754A5DAD1E8E"
SEARCH_URL = "https://www.youtube.com/@CriterionCollection/search?query=closet+picks"


def _fetch_yt_dlp_videos(url: str) -> list[dict]:
    """Fetch video metadata from a YouTube URL via yt-dlp."""
    cmd = [
        "yt-dlp",
        "--flat-playlist",
        "--dump-json",
        "--no-warnings",
        url,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if result.returncode != 0:
            log(f"  yt-dlp error: {result.stderr[:300]}")
            return []
    except subprocess.TimeoutExpired:
        log("  yt-dlp timed out")
        return []
    except FileNotFoundError:
        log("  yt-dlp not found. Install with: pip install yt-dlp")
        return []

    videos = []
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        try:
            data = json.loads(line)
            # Skip playlist/tab entries
            if data.get("_type") == "url" and data.get("ie_key") == "YoutubeTab":
                continue
            video = {
                "video_id": data.get("id", ""),
                "title": data.get("title", ""),
                "upload_date": data.get("upload_date", ""),
                "duration": data.get("duration"),
                "url": data.get("url", f"https://www.youtube.com/watch?v={data.get('id', '')}"),
            }
            if video["video_id"]:
                videos.append(video)
        except json.JSONDecodeError:
            continue

    return videos


def get_playlist_videos(playlist_url: str = PLAYLIST_URL) -> list[dict]:
    """
    Fetch video metadata from the official playlist + channel search.
    Merges and deduplicates by video_id.
    """
    seen_ids = set()
    all_videos = []

    # Primary: official playlist
    log("Fetching official playlist via yt-dlp...")
    playlist_videos = _fetch_yt_dlp_videos(playlist_url)
    log(f"  Official playlist: {len(playlist_videos)} videos")
    for v in playlist_videos:
        if v["video_id"] not in seen_ids:
            all_videos.append(v)
            seen_ids.add(v["video_id"])

    # Secondary: channel search (catches videos not in the playlist)
    log("Fetching channel search results via yt-dlp...")
    search_videos = _fetch_yt_dlp_videos(SEARCH_URL)
    log(f"  Channel search: {len(search_videos)} videos")
    for v in search_videos:
        if v["video_id"] not in seen_ids:
            all_videos.append(v)
            seen_ids.add(v["video_id"])

    log(f"Total unique videos: {len(all_videos)}")
    return all_videos


def parse_guest_name_from_video_title(title: str) -> str:
    """
    Extract guest name from YouTube video title.
    Common patterns:
      - "Barry Jenkins's Closet Picks"
      - "Barry Jenkins Picks His Criterion Closet Favorites"
      - "Criterion Closet Picks: Barry Jenkins"
      - "Barry Jenkins | Closet Picks"
      - "Bong Joon Ho's DVD Picks"
    """
    title = title.strip()

    # Pattern 1 (most specific): "Name's Closet/DVD Picks" (possessive with smart or regular apostrophe)
    m = re.match(
        r"^(.+?)(?:'s|'s|\u2019s)\s+(?:Criterion\s+)?(?:Closet\s+|DVD\s+)?(?:Picks?|Favorites?)",
        title,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()

    # Pattern 2: "Name Picks His/Her/Their Criterion..."
    m = re.match(
        r"^(.+?)\s+Picks?\s+(?:His|Her|Their)\s+(?:Criterion\s+)?(?:Closet\s+)?(?:Favorites?|Picks?)",
        title,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()

    # Pattern 3: "Name | Closet Picks" or "Name - Closet Picks"
    m = re.match(r"^(.+?)\s*[|\-\u2013\u2014]\s*(?:Criterion\s+)?Closet", title, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # Pattern 4: "Criterion Closet Picks: Name" or "Closet Picks: Name"
    m = re.search(r"(?:Criterion\s+)?Closet\s+Picks?[:\s]+(.+)", title, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    return title


def _normalize_name(name: str) -> str:
    """Normalize a name for matching: lowercase, remove hyphens, extra spaces."""
    name = name.lower().strip()
    name = name.replace("-", " ").replace("'s", "").replace("\u2019s", "")
    name = re.sub(r"\s+", " ", name)
    return name


def _is_closet_picks_video(title: str) -> bool:
    """Check if a video title is a Closet Picks episode (not a related video)."""
    title_lower = title.lower()
    # Must contain "closet picks" or "dvd picks" or "closet favorites"
    return any(phrase in title_lower for phrase in [
        "closet picks", "closet favorites", "dvd picks",
    ])


def match_videos_to_guests(
    videos: list[dict],
    guests: list[dict],
    pilot_only: bool = False,
) -> list[tuple[dict, dict]]:
    """
    Match YouTube videos to guest entries using fuzzy name matching.
    Only considers videos that are actual Closet Picks episodes.
    Returns list of (video, guest) tuples (one per guest, best match).
    """
    if pilot_only:
        target_names = set(PILOT_GUESTS)
    else:
        target_names = {g["name"] for g in guests}

    # First pass: collect all potential matches for each guest
    guest_candidates: dict[str, list[tuple[dict, int]]] = {g["slug"]: [] for g in guests}

    for video in videos:
        # Only consider actual Closet Picks videos
        if not _is_closet_picks_video(video["title"]):
            continue

        parsed_name = parse_guest_name_from_video_title(video["title"])
        parsed_norm = _normalize_name(parsed_name)

        for guest in guests:
            if pilot_only and guest["name"] not in target_names:
                continue

            guest_norm = _normalize_name(guest["name"])

            # Try multiple matching strategies
            score = fuzzy_match_score(parsed_norm, guest_norm)

            # Also try matching against parts of compound names
            # e.g., "Cate Blanchett and Todd Field" should match "Cate Blanchett"
            if " and " in parsed_name.lower():
                parts = parsed_name.lower().split(" and ")
                for part in parts:
                    part_score = fuzzy_match_score(part.strip(), guest_norm)
                    score = max(score, part_score)

            if score >= 70:
                guest_candidates[guest["slug"]].append((video, score))

    # Second pass: pick the best video per guest
    matches = []
    matched_guest_slugs = set()

    for guest in guests:
        if pilot_only and guest["name"] not in target_names:
            continue

        candidates = guest_candidates[guest["slug"]]
        if not candidates:
            log(f"  No video match for {guest['name']}")
            continue

        # Sort by score descending, take the best
        candidates.sort(key=lambda x: x[1], reverse=True)
        best_video, best_score = candidates[0]
        log(f"  Matched: {guest['name']} -> '{best_video['title']}' (score: {best_score})")
        matches.append((best_video, guest))
        matched_guest_slugs.add(guest["slug"])

    return matches


def fetch_transcript(video_id: str) -> list[dict] | None:
    """
    Fetch transcript for a YouTube video using youtube-transcript-api.
    Returns list of {text, start, duration} or None if unavailable.
    """
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        from youtube_transcript_api._errors import (
            TranscriptsDisabled,
            NoTranscriptFound,
        )
    except ImportError:
        log("youtube-transcript-api not installed")
        return None

    try:
        ytt_api = YouTubeTranscriptApi()
        transcript = ytt_api.fetch(video_id, languages=["en"])
        # Convert to list of dicts
        segments = []
        for entry in transcript:
            segments.append({
                "text": entry.text if hasattr(entry, 'text') else str(entry.get('text', '')),
                "start": entry.start if hasattr(entry, 'start') else float(entry.get('start', 0)),
                "duration": entry.duration if hasattr(entry, 'duration') else float(entry.get('duration', 0)),
            })
        return segments
    except Exception as e:
        log(f"  Transcript error for {video_id}: {type(e).__name__}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Match YouTube videos to guests")
    parser.add_argument("--pilot", action="store_true", help="Only process pilot guests")
    parser.add_argument("--playlist-url", default=PLAYLIST_URL, help="YouTube playlist URL")
    parser.add_argument("--limit", type=int, default=0, help="Limit videos to process")
    args = parser.parse_args()

    # Load guests
    guests = load_json(GUESTS_FILE)
    if not guests:
        log("ERROR: No guests found. Run scrape_letterboxd.py first.")
        sys.exit(1)
    log(f"Loaded {len(guests)} guests")

    # Get playlist videos
    videos = get_playlist_videos(args.playlist_url)
    if not videos:
        log("ERROR: No videos found in playlist")
        sys.exit(1)

    # Match videos to guests
    log("Matching videos to guests...")
    matches = match_videos_to_guests(videos, guests, pilot_only=args.pilot)
    log(f"Matched {len(matches)} videos to guests")

    if args.limit:
        matches = matches[:args.limit]

    # Fetch transcripts and update guests
    success_count = 0
    for video, guest in tqdm(matches, desc="Fetching transcripts"):
        video_id = video["video_id"]
        transcript_path = TRANSCRIPTS_DIR / f"{video_id}.json"

        # Check if transcript already exists
        if transcript_path.exists():
            log(f"  Transcript already exists: {video_id}")
            transcript = load_json(transcript_path)
        else:
            log(f"  Fetching transcript: {video_id} ({guest['name']})")
            transcript = fetch_transcript(video_id)
            time.sleep(0.5)

            if transcript:
                save_json(transcript_path, {
                    "video_id": video_id,
                    "guest_name": guest["name"],
                    "segments": transcript,
                })
                log(f"  Saved transcript: {len(transcript)} segments")
            else:
                log(f"  No transcript available for {video_id}")

        # Update guest entry
        guest["youtube_video_id"] = video_id
        guest["youtube_video_url"] = f"https://www.youtube.com/watch?v={video_id}"

        # Parse upload date
        upload_date = video.get("upload_date", "")
        if upload_date and len(upload_date) == 8:
            guest["episode_date"] = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}"

        if transcript:
            success_count += 1

    # Save updated guests
    save_json(GUESTS_FILE, guests)
    log(f"Updated {GUESTS_FILE}")
    log(f"Transcripts fetched: {success_count}/{len(matches)}")

    # Report
    for video, guest in matches:
        has_transcript = (TRANSCRIPTS_DIR / f"{video['video_id']}.json").exists()
        status = "OK" if has_transcript else "NO TRANSCRIPT"
        log(f"  {guest['name']}: {video['video_id']} [{status}]")


if __name__ == "__main__":
    main()
