#!/usr/bin/env python3
"""
Parallel quote extraction wrapper.

Splits guests into N shards and runs extract_quotes.py on each shard concurrently.
Each shard uses its own Gemini model instance to maximize throughput within rate limits.

Usage:
  python scripts/extract_quotes_parallel.py --workers 4          # Resume from checkpoint
  python scripts/extract_quotes_parallel.py --workers 4 --force  # Re-extract all
"""

import argparse
import json
import math
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    GUESTS_FILE,
    PICKS_RAW_FILE,
    PICKS_FILE,
    TRANSCRIPTS_DIR,
    CHECKPOINT_FILE,
    load_json,
    save_json,
    log,
    get_env,
)


def get_guests_to_process(force: bool) -> list[dict]:
    """Build list of guests that need processing."""
    guests = load_json(GUESTS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)
    checkpoint = load_json(CHECKPOINT_FILE) or {}

    picks_by_guest = {}
    for pick in picks_raw:
        slug = pick["guest_slug"]
        if slug not in picks_by_guest:
            picks_by_guest[slug] = []
        picks_by_guest[slug].append(pick)

    processable = []
    for guest in guests:
        slug = guest["slug"]
        video_id = guest.get("youtube_video_id") or guest.get("vimeo_video_id")
        guest_picks = picks_by_guest.get(slug, [])

        if not video_id or not guest_picks:
            continue

        transcript_path = TRANSCRIPTS_DIR / f"{video_id}.json"
        if not transcript_path.exists():
            continue

        if not force and slug in checkpoint:
            continue

        processable.append(guest)

    return processable


_checkpoint_lock = threading.Lock()


def _update_checkpoint(slug: str, data: dict):
    """Thread-safe checkpoint update: read-modify-write under lock."""
    with _checkpoint_lock:
        checkpoint = load_json(CHECKPOINT_FILE) or {}
        checkpoint[slug] = data
        save_json(CHECKPOINT_FILE, checkpoint)


def process_shard(shard_id: int, guest_slugs: list[str], force: bool) -> dict:
    """Process a shard of guests. Each shard gets its own Gemini model."""
    import google.generativeai as genai

    api_key = get_env("GEMINI_API_KEY")
    genai.configure(api_key=api_key)

    model = genai.GenerativeModel(
        "gemini-3-flash-preview",
        generation_config={
            "temperature": 0.1,
            "response_mime_type": "application/json",
            "max_output_tokens": 65536,
        },
    )

    from scripts.extract_quotes import (
        extract_quotes_for_guest,
        format_transcript,
    )

    guests = load_json(GUESTS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)

    guest_map = {g["slug"]: g for g in guests}
    picks_by_guest = {}
    for pick in picks_raw:
        slug = pick["guest_slug"]
        if slug not in picks_by_guest:
            picks_by_guest[slug] = []
        picks_by_guest[slug].append(pick)

    results = {}
    processed = 0
    errors = 0

    for slug in guest_slugs:
        guest = guest_map.get(slug)
        if not guest:
            continue

        video_id = guest.get("youtube_video_id") or guest.get("vimeo_video_id")
        guest_picks = picks_by_guest.get(slug, [])
        transcript_path = TRANSCRIPTS_DIR / f"{video_id}.json"

        if not transcript_path.exists():
            continue

        transcript_data = load_json(transcript_path)
        if isinstance(transcript_data, list):
            segments = transcript_data
        else:
            segments = transcript_data.get("segments", [])

        if not segments:
            errors += 1
            continue

        log(f"  [W{shard_id}] {guest['name']} ({len(guest_picks)} picks)")

        try:
            quotes = extract_quotes_for_guest(model, guest, guest_picks, segments)
        except Exception as e:
            log(f"  [W{shard_id}] Error: {guest['name']}: {e}")
            errors += 1
            time.sleep(2)
            continue

        if not quotes:
            errors += 1
            continue

        # Build quote map
        quotes_by_title = {q["film_title"].lower(): q for q in quotes}

        for pick in guest_picks:
            title = pick["film_title"]
            quote_match = quotes_by_title.get(title.lower())
            if quote_match:
                pick["quote"] = quote_match["quote"]
                pick["start_timestamp"] = quote_match["start_timestamp"]
                pick["extraction_confidence"] = quote_match["confidence"]
                pick["visit_index"] = 1
                video_source = "youtube" if guest.get("youtube_video_id") else "vimeo"
                if video_id and quote_match["start_timestamp"]:
                    if video_source == "vimeo":
                        pick["vimeo_timestamp_url"] = (
                            f"https://vimeo.com/{video_id}#t={quote_match['start_timestamp']}s"
                        )
                    else:
                        pick["youtube_timestamp_url"] = (
                            f"https://www.youtube.com/watch?v={video_id}&t={quote_match['start_timestamp']}"
                        )
            results[(slug, pick["film_title"])] = pick

        # Update checkpoint (thread-safe)
        _update_checkpoint(slug, {
            "processed_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "quotes_count": len(quotes),
            "picks_count": len(guest_picks),
        })

        processed += 1

    return {"shard": shard_id, "processed": processed, "errors": errors, "picks": results}


def main():
    parser = argparse.ArgumentParser(description="Parallel quote extraction")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    parser.add_argument("--force", action="store_true", help="Re-extract all guests")
    args = parser.parse_args()

    processable = get_guests_to_process(args.force)
    log(f"Guests to process: {len(processable)}")

    if not processable:
        log("Nothing to process")
        return

    # Split into shards
    slugs = [g["slug"] for g in processable]
    shard_size = math.ceil(len(slugs) / args.workers)
    shards = [slugs[i:i + shard_size] for i in range(0, len(slugs), shard_size)]
    log(f"Split into {len(shards)} shards of ~{shard_size} guests each")

    # Load existing picks for merging
    existing_picks = load_json(PICKS_FILE)
    existing_pick_index = {}
    for p in existing_picks:
        key = (p["guest_slug"], p.get("film_title", ""))
        existing_pick_index[key] = p

    # Run shards in parallel
    all_results = {}
    total_processed = 0
    total_errors = 0

    with ThreadPoolExecutor(max_workers=len(shards)) as executor:
        futures = {
            executor.submit(process_shard, i, shard, args.force): i
            for i, shard in enumerate(shards)
        }
        for future in as_completed(futures):
            shard_id = futures[future]
            try:
                result = future.result()
                total_processed += result["processed"]
                total_errors += result["errors"]
                all_results.update(result["picks"])
                log(f"Shard {shard_id}: {result['processed']} processed, {result['errors']} errors")
            except Exception as e:
                log(f"Shard {shard_id} failed: {e}")

    # Merge results into existing picks
    for key, pick in all_results.items():
        existing_pick_index[key] = pick

    all_picks = list(existing_pick_index.values())

    # Post-process: clean quotes
    try:
        from scripts.clean_quotes import clean_quote, build_title_map
        catalog = load_json(CATALOG_FILE)
        title_map = build_title_map(catalog)
        cleaned = 0
        for pick in all_picks:
            if pick.get("quote"):
                original = pick["quote"]
                pick["quote"] = clean_quote(original, title_map)
                if pick["quote"] != original:
                    cleaned += 1
        log(f"Cleaned {cleaned} quotes")
    except ImportError:
        log("clean_quotes not available, skipping post-processing")

    save_json(PICKS_FILE, all_picks)
    log(f"\nSaved {len(all_picks)} picks to {PICKS_FILE}")
    log(f"Total: {total_processed} processed, {total_errors} errors")

    # Confidence breakdown
    high = sum(1 for p in all_picks if p.get("extraction_confidence") == "high")
    medium = sum(1 for p in all_picks if p.get("extraction_confidence") == "medium")
    low = sum(1 for p in all_picks if p.get("extraction_confidence") == "low")
    none_conf = sum(1 for p in all_picks if p.get("extraction_confidence") in ("none", None))
    log(f"Confidence: high={high}, medium={medium}, low={low}, none={none_conf}")


if __name__ == "__main__":
    main()
