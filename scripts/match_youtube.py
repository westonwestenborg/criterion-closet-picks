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
    # Skip guests that already have a video ID (set by Criterion page extraction)
    guests_needing_video = []
    for guest in guests:
        if guest.get("youtube_video_id") or guest.get("vimeo_video_id"):
            log(f"  Skipping {guest['name']}: already has video ID")
            continue
        guests_needing_video.append(guest)

    log(f"  {len(guests) - len(guests_needing_video)} guests already have video IDs, matching {len(guests_needing_video)}")

    guest_candidates: dict[str, list[tuple[dict, int]]] = {g["slug"]: [] for g in guests_needing_video}

    for video in videos:
        # Only consider actual Closet Picks videos
        if not _is_closet_picks_video(video["title"]):
            continue

        parsed_name = parse_guest_name_from_video_title(video["title"])
        parsed_norm = _normalize_name(parsed_name)

        for guest in guests_needing_video:
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
    matched_video_ids = set()

    for guest in guests_needing_video:
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
        matched_video_ids.add(best_video["video_id"])

    return matches, matched_video_ids


def match_second_visit_videos(
    videos: list[dict],
    guests: list[dict],
    matched_video_ids: set[str],
) -> list[tuple[dict, dict, int]]:
    """
    Match second-visit YouTube videos for multi-visit guests.
    Returns list of (video, guest, visit_index) tuples.

    Strategy A: Criterion page URL -> extract video ID directly
    Strategy B: Fuzzy title match against unmatched playlist videos
    """
    # Collect already-consumed video IDs (from primary matching + existing guests)
    consumed_ids = set(matched_video_ids)
    for g in guests:
        if g.get("youtube_video_id"):
            consumed_ids.add(g["youtube_video_id"])
        for v in g.get("visits", []):
            vid = v.get("youtube_video_id")
            if vid:
                consumed_ids.add(vid)

    # Identify multi-visit guests with missing video IDs on some visits
    candidates = []
    for guest in guests:
        visits = guest.get("visits", [])
        if len(visits) < 2:
            continue
        for i, visit in enumerate(visits):
            if not visit.get("youtube_video_id") and not visit.get("vimeo_video_id"):
                candidates.append((guest, i))

    if not candidates:
        log("No multi-visit guests need second video matching")
        return []

    log(f"Matching second-visit videos for {len(candidates)} visit(s)...")
    results = []

    # Strategy A: Use Criterion page URL to extract video ID
    for guest, visit_idx in candidates[:]:
        visit = guest["visits"][visit_idx]
        criterion_url = visit.get("criterion_page_url")
        if not criterion_url:
            continue

        # Try importing from scrape_criterion_picks
        try:
            from scripts.scrape_criterion_picks import (
                create_scraper,
                extract_video_ids as extract_video_ids_from_page,
            )
        except ImportError:
            break

        log(f"  Strategy A: {guest['name']} visit {visit_idx + 1} — fetching {criterion_url}")
        try:
            scraper = create_scraper()
            resp = scraper.get(criterion_url, timeout=30)
            if resp.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, "lxml")
                page_video_ids = extract_video_ids_from_page(soup)
                yt_id = page_video_ids.get("youtube_video_id")
                vim_id = page_video_ids.get("vimeo_video_id")

                if yt_id and yt_id not in consumed_ids:
                    log(f"    Found YouTube: {yt_id}")
                    # Find the matching video in playlist for upload_date
                    matched_video = next((v for v in videos if v["video_id"] == yt_id), None)
                    if not matched_video:
                        matched_video = {"video_id": yt_id, "title": f"{guest['name']} visit {visit_idx + 1}", "upload_date": ""}
                    results.append((matched_video, guest, visit_idx))
                    consumed_ids.add(yt_id)
                    candidates.remove((guest, visit_idx))
                elif vim_id and vim_id not in consumed_ids:
                    log(f"    Found Vimeo: {vim_id}")
                    visit["vimeo_video_id"] = vim_id
                    consumed_ids.add(vim_id)
                    candidates.remove((guest, visit_idx))
                else:
                    log(f"    No new video found (or already consumed)")
            time.sleep(1.5)
        except Exception as e:
            log(f"    Error fetching Criterion page: {e}")

    # Strategy B: Fuzzy title match against unmatched playlist videos
    unmatched_videos = [v for v in videos if v["video_id"] not in consumed_ids and _is_closet_picks_video(v["title"])]

    for guest, visit_idx in candidates:
        guest_norm = _normalize_name(guest["name"])
        video_matches = []

        for video in unmatched_videos:
            parsed_name = parse_guest_name_from_video_title(video["title"])
            parsed_norm = _normalize_name(parsed_name)
            score = fuzzy_match_score(parsed_norm, guest_norm)

            # Also try compound name parts
            if " and " in parsed_name.lower():
                parts = parsed_name.lower().split(" and ")
                for part in parts:
                    part_score = fuzzy_match_score(part.strip(), guest_norm)
                    score = max(score, part_score)

            if score >= 70:
                video_matches.append((video, score))

        if not video_matches:
            log(f"  Strategy B: No match for {guest['name']} visit {visit_idx + 1}")
            continue

        # Sort by score, then by upload_date (newest first for visit 2)
        video_matches.sort(key=lambda x: (x[1], x[0].get("upload_date", "")), reverse=True)
        best_video, best_score = video_matches[0]

        log(f"  Strategy B: {guest['name']} visit {visit_idx + 1} -> '{best_video['title']}' (score: {best_score})")
        results.append((best_video, guest, visit_idx))
        consumed_ids.add(best_video["video_id"])
        # Remove from unmatched so it can't be double-assigned
        unmatched_videos = [v for v in unmatched_videos if v["video_id"] != best_video["video_id"]]

    return results


def fetch_transcript(video_id: str) -> list[dict] | None:
    """
    Fetch transcript for a YouTube video using youtube-transcript-api.
    Tries English first, then auto-generated English variants, then any available language.
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

    ytt_api = YouTubeTranscriptApi()

    def _to_segments(transcript) -> list[dict]:
        segments = []
        for entry in transcript:
            segments.append({
                "text": entry.text if hasattr(entry, 'text') else str(entry.get('text', '')),
                "start": entry.start if hasattr(entry, 'start') else float(entry.get('start', 0)),
                "duration": entry.duration if hasattr(entry, 'duration') else float(entry.get('duration', 0)),
            })
        return segments

    # Try English first
    try:
        transcript = ytt_api.fetch(video_id, languages=["en"])
        log(f"  Transcript language: en")
        return _to_segments(transcript)
    except (NoTranscriptFound, Exception):
        pass

    # Try auto-generated English variants
    try:
        transcript = ytt_api.fetch(video_id, languages=["en-US", "en-GB"])
        log(f"  Transcript language: en-US/en-GB")
        return _to_segments(transcript)
    except (NoTranscriptFound, Exception):
        pass

    # Accept any available language
    try:
        transcript = ytt_api.fetch(video_id)
        # Try to detect what language we got
        lang = "unknown"
        if hasattr(transcript, 'language'):
            lang = transcript.language
        elif hasattr(transcript, '_language'):
            lang = transcript._language
        log(f"  Transcript language: {lang} (non-English fallback)")
        return _to_segments(transcript)
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
    matches, matched_video_ids = match_videos_to_guests(videos, guests, pilot_only=args.pilot)
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

        # Propagate video info to visit 1 if visits array exists
        if guest.get("visits") and len(guest["visits"]) > 0:
            guest["visits"][0]["episode_date"] = guest["episode_date"]
            guest["visits"][0]["youtube_video_id"] = video_id
            guest["visits"][0]["youtube_video_url"] = guest.get("youtube_video_url")

        if transcript:
            success_count += 1

    # Save updated guests (intermediate save before multi-visit matching)
    save_json(GUESTS_FILE, guests)
    log(f"Updated {GUESTS_FILE}")
    log(f"Transcripts fetched: {success_count}/{len(matches)}")

    # Report primary matches
    for video, guest in matches:
        has_transcript = (TRANSCRIPTS_DIR / f"{video['video_id']}.json").exists()
        status = "OK" if has_transcript else "NO TRANSCRIPT"
        log(f"  {guest['name']}: {video['video_id']} [{status}]")

    # Multi-visit second video matching
    if not args.pilot:
        log("\nMatching second-visit videos...")
        visit_matches = match_second_visit_videos(videos, guests, matched_video_ids)
        log(f"Matched {len(visit_matches)} second-visit videos")

        for video, guest, visit_idx in visit_matches:
            video_id = video["video_id"]
            visit = guest["visits"][visit_idx]

            # Set video ID on the visit
            visit["youtube_video_id"] = video_id
            visit["youtube_video_url"] = f"https://www.youtube.com/watch?v={video_id}"

            # Set episode_date from upload_date on this visit
            upload_date = video.get("upload_date", "")
            if upload_date and len(upload_date) == 8:
                visit["episode_date"] = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}"

            # Fetch transcript
            transcript_path = TRANSCRIPTS_DIR / f"{video_id}.json"
            if transcript_path.exists():
                log(f"  Transcript already exists: {video_id}")
            else:
                log(f"  Fetching transcript: {video_id} ({guest['name']} visit {visit_idx + 1})")
                transcript = fetch_transcript(video_id)
                time.sleep(0.5)

                if transcript:
                    save_json(transcript_path, {
                        "video_id": video_id,
                        "guest_name": guest["name"],
                        "visit": visit_idx + 1,
                        "segments": transcript,
                    })
                    log(f"  Saved transcript: {len(transcript)} segments")
                else:
                    log(f"  No transcript available for {video_id}")

        # Ensure visit video assignments are in chronological order.
        # Primary matching tends to find the newer video first, which gets
        # assigned to visits[0]. But visits[0] is the older visit. Swap if needed.
        # Collect video IDs that need date lookups
        need_dates: dict[str, None] = {}  # video_id -> None (ordered set)
        multi_visit_guests = []
        for guest in guests:
            gv = guest.get("visits", [])
            if len(gv) < 2:
                continue
            v0_id = gv[0].get("youtube_video_id")
            v1_id = gv[1].get("youtube_video_id")
            if not v0_id or not v1_id:
                continue
            multi_visit_guests.append(guest)
            if not gv[0].get("episode_date"):
                need_dates[v0_id] = None
            if not gv[1].get("episode_date"):
                need_dates[v1_id] = None

        # Fetch upload dates for videos missing episode_date
        video_dates: dict[str, str] = {}
        if need_dates:
            log(f"Fetching upload dates for {len(need_dates)} multi-visit videos...")
            for vid in need_dates:
                try:
                    r = subprocess.run(
                        ["yt-dlp", "--print", "%(upload_date)s", "--no-warnings",
                         f"https://www.youtube.com/watch?v={vid}"],
                        capture_output=True, text=True, timeout=30,
                    )
                    date_str = r.stdout.strip()
                    if date_str and len(date_str) == 8:
                        video_dates[vid] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                        log(f"  {vid}: {video_dates[vid]}")
                except Exception as e:
                    log(f"  {vid}: error fetching date: {e}")

        for guest in multi_visit_guests:
            gv = guest["visits"]
            v0_date = gv[0].get("episode_date") or video_dates.get(gv[0]["youtube_video_id"], "")
            v1_date = gv[1].get("episode_date") or video_dates.get(gv[1]["youtube_video_id"], "")

            if v0_date and v1_date and v0_date > v1_date:
                log(f"  Swapping visit video order for {guest['name']}: "
                    f"v1={v0_date} > v2={v1_date}")
                swap_keys = [
                    "youtube_video_id", "youtube_video_url",
                    "vimeo_video_id", "episode_date",
                ]
                for k in swap_keys:
                    gv[0][k], gv[1][k] = gv[1].get(k), gv[0].get(k)
                # Populate episode_dates from fetched dates after swap
                for visit in gv:
                    if not visit.get("episode_date"):
                        visit["episode_date"] = video_dates.get(
                            visit.get("youtube_video_id", ""), None)
                # Update top-level to match visit 1
                guest["youtube_video_id"] = gv[0].get("youtube_video_id")
                guest["youtube_video_url"] = gv[0].get("youtube_video_url")
                guest["episode_date"] = gv[0].get("episode_date")

        # Save with multi-visit updates
        save_json(GUESTS_FILE, guests)
        log(f"Saved multi-visit updates to {GUESTS_FILE}")


if __name__ == "__main__":
    main()
