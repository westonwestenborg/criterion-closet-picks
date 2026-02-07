#!/usr/bin/env python3
"""
Process a single Criterion Closet Picks video end-to-end.
Useful for adding new episodes or reprocessing individual guests.

Steps:
  1. Look up guest in guests.json (or create entry from YouTube video)
  2. Fetch/update transcript
  3. Extract quotes via Gemini
  4. Enrich film data via TMDB
  5. Enrich guest data via TMDB

Usage:
  python scripts/process_video.py --guest-slug barry-jenkins
  python scripts/process_video.py --video-id R7HLpe65fHY
  python scripts/process_video.py --guest-slug barry-jenkins --skip-enrich
"""

import argparse
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    GUESTS_FILE,
    PICKS_RAW_FILE,
    PICKS_FILE,
    TRANSCRIPTS_DIR,
    load_json,
    save_json,
    log,
    slugify,
    get_env,
)


def find_guest(guests: list[dict], slug: str = None, video_id: str = None) -> dict | None:
    """Find a guest by slug or video ID (YouTube or Vimeo)."""
    for g in guests:
        if slug and g["slug"] == slug:
            return g
        if video_id and g.get("youtube_video_id") == video_id:
            return g
        if video_id and g.get("vimeo_video_id") == video_id:
            return g
    return None


def step_fetch_transcript(guest: dict) -> bool:
    """Fetch transcript for the guest's video (YouTube or Vimeo)."""
    video_id = guest.get("youtube_video_id")
    video_source = "youtube"

    if not video_id:
        vimeo_id = guest.get("vimeo_video_id")
        if vimeo_id:
            log(f"Guest {guest['name']} has Vimeo video ({vimeo_id}) — no transcript API available for Vimeo")
            return False
        log(f"No video ID for {guest['name']}, cannot fetch transcript")
        return False

    transcript_path = TRANSCRIPTS_DIR / f"{video_id}.json"
    if transcript_path.exists():
        log(f"Transcript already exists: {transcript_path}")
        return True

    log(f"Fetching transcript for {video_id}...")
    from scripts.match_youtube import fetch_transcript

    segments = fetch_transcript(video_id)
    if not segments:
        log(f"No transcript available for {video_id}")
        return False

    save_json(transcript_path, {
        "video_id": video_id,
        "guest_name": guest["name"],
        "segments": segments,
    })
    log(f"Saved transcript: {len(segments)} segments")
    return True


def step_extract_quotes(guest: dict, force: bool = False) -> bool:
    """Extract quotes for this guest's picks."""
    from scripts.extract_quotes import (
        get_gemini_model,
        extract_quotes_for_guest,
    )

    slug = guest["slug"]
    video_id = guest.get("youtube_video_id") or guest.get("vimeo_video_id")
    video_source = "youtube" if guest.get("youtube_video_id") else "vimeo"

    if not video_id:
        log(f"No video ID for {guest['name']}")
        return False

    transcript_path = TRANSCRIPTS_DIR / f"{video_id}.json"
    if not transcript_path.exists():
        log(f"No transcript for {guest['name']}")
        return False

    # Load picks for this guest
    picks_raw = load_json(PICKS_RAW_FILE)
    guest_picks = [p for p in picks_raw if p["guest_slug"] == slug]
    if not guest_picks:
        log(f"No raw picks for {guest['name']}")
        return False

    # Load existing enriched picks
    existing_picks = load_json(PICKS_FILE)
    existing_index = {(p["guest_slug"], p.get("film_title", "")): p for p in existing_picks}

    # Check if already processed
    has_quotes = any(
        p.get("quote") for p in existing_picks if p["guest_slug"] == slug
    )
    if has_quotes and not force:
        log(f"Quotes already extracted for {guest['name']} (use --force to re-extract)")
        return True

    # Initialize Gemini
    model = get_gemini_model()
    log(f"Extracting quotes for {guest['name']} ({len(guest_picks)} picks)...")

    # Load transcript
    transcript_data = load_json(transcript_path)
    segments = transcript_data.get("segments", [])
    if not segments:
        log(f"Empty transcript for {guest['name']}")
        return False

    quotes = extract_quotes_for_guest(model, guest, guest_picks, segments)
    if not quotes:
        log(f"No quotes extracted for {guest['name']}")
        return False

    log(f"Extracted {len(quotes)} quotes")

    # Merge quotes into picks
    quotes_by_title = {q["film_title"].lower(): q for q in quotes}
    for pick in guest_picks:
        title = pick["film_title"]
        quote_match = quotes_by_title.get(title.lower())
        if quote_match:
            pick["quote"] = quote_match["quote"]
            pick["start_timestamp"] = quote_match["start_timestamp"]
            pick["extraction_confidence"] = quote_match["confidence"]
            if video_id and quote_match["start_timestamp"]:
                if video_source == "vimeo":
                    pick["vimeo_timestamp_url"] = (
                        f"https://vimeo.com/{video_id}#t={quote_match['start_timestamp']}s"
                    )
                else:
                    pick["youtube_timestamp_url"] = (
                        f"https://www.youtube.com/watch?v={video_id}&t={quote_match['start_timestamp']}"
                    )
        existing_index[(slug, title)] = pick

    # Save all picks
    all_picks = list(existing_index.values())
    save_json(PICKS_FILE, all_picks)
    log(f"Saved {len(all_picks)} picks")
    return True


def step_enrich_films(guest: dict) -> bool:
    """Enrich films in this guest's picks via TMDB."""
    from scripts.enrich_tmdb import TMDBClient, enrich_film

    slug = guest["slug"]
    picks = load_json(PICKS_FILE)
    catalog = load_json(CATALOG_FILE)

    # Find film spines from this guest's picks
    guest_spines = set()
    for p in picks:
        if p["guest_slug"] == slug and p.get("catalog_spine"):
            guest_spines.add(p["catalog_spine"])

    if not guest_spines:
        log(f"No catalog spines for {guest['name']}'s picks")
        return True  # Not an error, just no films to enrich

    films_to_enrich = [c for c in catalog if c["spine_number"] in guest_spines]
    # Filter to only unenriched
    films_to_enrich = [f for f in films_to_enrich if not (f.get("tmdb_id") and f.get("poster_url"))]

    if not films_to_enrich:
        log(f"All {len(guest_spines)} films already enriched")
        return True

    log(f"Enriching {len(films_to_enrich)} films via TMDB...")
    client = TMDBClient()
    genres = client.get_genres()

    count = 0
    for film in films_to_enrich:
        before = (film.get("tmdb_id"), film.get("poster_url"))
        enrich_film(client, film, genres)
        after = (film.get("tmdb_id"), film.get("poster_url"))
        if before != after:
            count += 1

    save_json(CATALOG_FILE, catalog)
    log(f"Enriched {count} films")
    return True


def step_enrich_guest(guest: dict, guests: list[dict]) -> bool:
    """Enrich guest with TMDB person data."""
    if guest.get("profession") and guest.get("photo_url"):
        log(f"Guest {guest['name']} already enriched")
        return True

    from scripts.enrich_tmdb import TMDBClient, enrich_guest

    log(f"Enriching guest {guest['name']} via TMDB...")
    client = TMDBClient()
    enrich_guest(client, guest)

    save_json(GUESTS_FILE, guests)
    log(f"  Profession: {guest.get('profession', '?')}, Photo: {'yes' if guest.get('photo_url') else 'no'}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Process a single video end-to-end")
    parser.add_argument("--guest-slug", type=str, help="Guest slug to process")
    parser.add_argument("--video-id", type=str, help="YouTube video ID to process")
    parser.add_argument("--force", action="store_true", help="Force re-extraction of quotes")
    parser.add_argument("--skip-enrich", action="store_true", help="Skip TMDB enrichment")
    parser.add_argument("--skip-quotes", action="store_true", help="Skip quote extraction")
    args = parser.parse_args()

    if not args.guest_slug and not args.video_id:
        parser.error("Must provide --guest-slug or --video-id")

    # Load guests
    guests = load_json(GUESTS_FILE)
    guest = find_guest(guests, slug=args.guest_slug, video_id=args.video_id)

    if not guest:
        identifier = args.guest_slug or args.video_id
        log(f"ERROR: Guest not found: {identifier}")
        log("Available guests:")
        for g in guests:
            log(f"  {g['slug']} ({g['name']})")
        sys.exit(1)

    log(f"Processing: {guest['name']} (slug: {guest['slug']})")

    # Step 1: Fetch transcript
    log("\n--- Step 1: Fetch Transcript ---")
    if not step_fetch_transcript(guest):
        log("WARNING: No transcript available, quote extraction will be skipped")

    # Step 2: Extract quotes
    if not args.skip_quotes:
        log("\n--- Step 2: Extract Quotes ---")
        step_extract_quotes(guest, force=args.force)
    else:
        log("\n--- Step 2: Extract Quotes (SKIPPED) ---")

    # Step 3: Enrich data
    if not args.skip_enrich:
        log("\n--- Step 3: Enrich Films ---")
        step_enrich_films(guest)

        log("\n--- Step 4: Enrich Guest ---")
        step_enrich_guest(guest, guests)
    else:
        log("\n--- Steps 3-4: TMDB Enrichment (SKIPPED) ---")

    log(f"\nDone processing {guest['name']}")


if __name__ == "__main__":
    main()
