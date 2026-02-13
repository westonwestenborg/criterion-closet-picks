#!/usr/bin/env python3
"""
Extract quotes from transcripts using Gemini 2.0 Flash.
For each guest with both picks and a transcript, sends the transcript + known picks
to Gemini and extracts verbatim quotes with timestamps.

Output: data/picks.json
"""

import argparse
import json
import re
import sys
import time

from tqdm import tqdm

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    GUESTS_FILE,
    PICKS_RAW_FILE,
    PICKS_FILE,
    TRANSCRIPTS_DIR,
    CHECKPOINT_FILE,
    PILOT_GUESTS,
    load_json,
    save_json,
    log,
    get_env,
    slugify,
)


EXTRACTION_PROMPT = """You are extracting film commentary from a Criterion Closet Picks video transcript.

CONTEXT: In these videos, guests visit the Criterion Collection's closet and
physically pick up DVDs/Blu-rays while talking about why they love each film.
Guests walk through shelves grabbing films, so they often refer to films
indirectly ("this one", "oh my god", picking it up without naming it) rather
than saying the full title. Auto-generated captions frequently misspell film
titles and proper names.

GUEST: {guest_name}

KNOWN PICKS (from curated Letterboxd data - these are the films they took home):
{picks_list}

TRANSCRIPT (with timestamps in seconds):
{transcript}

YOUR TASK: For each film in the known picks list, find the segment(s) of the
transcript where the guest discusses that film. Return a JSON array with one
object per film:

{{
  "film_title": "exact title from the known picks list",
  "start_timestamp": 142,
  "quote": "cleaned verbatim quote spanning their discussion of this film",
  "confidence": "high|medium|low|none"
}}

GUIDELINES:
- Films are generally discussed in the order they're physically picked up,
  roughly matching transcript order
- For the quote: combine consecutive transcript segments about the same film
  into one flowing quote. Fix obvious auto-caption errors (e.g., "rack catcher"
  -> "Ratcatcher", "Lynn" -> "Lynne Ramsay") but preserve the speaker's actual
  words and speech patterns
- Some films may have very brief mentions ("I'll take this too") - include
  these with a short quote
- Some films in the picks list may not be discussed at all in the transcript
  (guest grabbed it silently, or it was a box set addition) - set confidence
  to "none" and quote to empty string
- The guest may discuss films they DON'T take home - ignore these, only
  extract quotes for films in the known picks list
- confidence levels:
  - "high": clear discussion, film identifiable from context
  - "medium": probable match but some ambiguity
  - "low": uncertain, could be about a different film
  - "none": no discussion found in transcript
- start_timestamp should be the beginning of their discussion of that film
  (in seconds, as an integer)
- Cap each quote at 500 characters maximum

Return ONLY the JSON array, no other text."""


def get_gemini_model():
    """Initialize Gemini model."""
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
    return model


def format_transcript(segments: list[dict]) -> str:
    """Format transcript segments into a readable string with timestamps."""
    lines = []
    for seg in segments:
        start = int(seg.get("start", 0))
        text = seg.get("text", "").strip()
        if text:
            lines.append(f"[{start}s] {text}")
    return "\n".join(lines)


def format_picks_list(picks: list[dict]) -> str:
    """Format picks into a numbered list for the prompt."""
    lines = []
    for i, pick in enumerate(picks, 1):
        title = pick.get("film_title", "Unknown")
        year = pick.get("film_year", "")
        year_str = f" ({year})" if year else ""
        lines.append(f"{i}. {title}{year_str}")
    return "\n".join(lines)


BATCH_SIZE = 20  # Max picks per API call to avoid output truncation


def _extract_single_batch(
    model,
    guest_name: str,
    picks: list[dict],
    transcript: str,
) -> list[dict]:
    """Extract quotes for a single batch of picks."""
    picks_list = format_picks_list(picks)
    prompt = EXTRACTION_PROMPT.format(
        guest_name=guest_name,
        picks_list=picks_list,
        transcript=transcript,
    )

    try:
        response = model.generate_content(prompt)
        response_text = response.text.strip()

        # Handle markdown code blocks if present
        if response_text.startswith("```"):
            response_text = re.sub(r"^```(?:json)?\s*", "", response_text)
            response_text = re.sub(r"\s*```$", "", response_text)

        quotes = json.loads(response_text)

        if not isinstance(quotes, list):
            log(f"  WARNING: Gemini returned non-list response")
            return []

        # Validate and clean quotes
        cleaned = []
        for q in quotes:
            if not isinstance(q, dict):
                continue
            cleaned.append({
                "film_title": q.get("film_title", ""),
                "start_timestamp": int(q.get("start_timestamp", 0) or 0),
                "quote": (q.get("quote", "") or "")[:500],
                "confidence": q.get("confidence", "none"),
            })

        return cleaned

    except json.JSONDecodeError as e:
        log(f"  JSON parse error: {e}")
        log(f"  Response: {response_text[:300]}")
        return []
    except Exception as e:
        log(f"  Gemini error: {type(e).__name__}: {e}")
        return []


def extract_quotes_for_guest(
    model,
    guest: dict,
    picks: list[dict],
    transcript_segments: list[dict],
) -> list[dict]:
    """
    Send transcript + picks to Gemini and extract quotes.
    Batches large pick lists to avoid output truncation.
    Returns list of quote objects.
    """
    guest_name = guest["name"]
    transcript = format_transcript(transcript_segments)

    # Truncate transcript if too long (Gemini has ~1M token context)
    if len(transcript_segments) > 1000:
        transcript = format_transcript(transcript_segments[:1000])
        log(f"  Truncated transcript to 1000 segments")

    # Batch large pick lists to avoid output token truncation
    if len(picks) <= BATCH_SIZE:
        return _extract_single_batch(model, guest_name, picks, transcript)

    all_quotes = []
    num_batches = (len(picks) + BATCH_SIZE - 1) // BATCH_SIZE
    log(f"  Splitting {len(picks)} picks into {num_batches} batches")

    for i in range(0, len(picks), BATCH_SIZE):
        batch = picks[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        log(f"  Batch {batch_num}/{num_batches}: {len(batch)} picks")
        batch_quotes = _extract_single_batch(model, guest_name, batch, transcript)
        all_quotes.extend(batch_quotes)
        if batch_num < num_batches:
            time.sleep(6)  # Rate limit between batches

    return all_quotes


AUDIO_EXTRACTION_PROMPT = """You are extracting film commentary from a Criterion Closet Picks video.
Listen to the audio and transcribe what the guest says. If the guest speaks a
non-English language, translate their words to English and prefix quotes with
"[Translated] ".

CONTEXT: In these videos, guests visit the Criterion Collection's closet and
physically pick up DVDs/Blu-rays while talking about why they love each film.

GUEST: {guest_name}

KNOWN PICKS (from curated data - these are the films they took home):
{picks_list}

YOUR TASK: Listen to the audio carefully and for each film in the known picks
list, find what the guest says about that film. Return a JSON array with one
object per film:

{{
  "film_title": "exact title from the known picks list",
  "start_timestamp": 142,
  "quote": "cleaned verbatim quote about this film",
  "confidence": "high|medium|low|none"
}}

GUIDELINES:
- If the guest speaks a non-English language, prefix translated quotes with "[Translated] "
- For the quote: combine discussion segments about the same film into one flowing quote
- Fix obvious misheard words but preserve the speaker's actual words and speech patterns
- Some films may have very brief mentions ("I'll take this too") - include these
- Some films may not be discussed at all - set confidence to "none" and quote to ""
- confidence levels:
  - "high": clear discussion, film identifiable from context
  - "medium": probable match but some ambiguity
  - "low": uncertain
  - "none": no discussion found
- start_timestamp should be the beginning of their discussion (seconds, integer)
- Cap each quote at 500 characters maximum

Return ONLY the JSON array, no other text."""


def extract_quotes_from_audio(
    model,
    guest: dict,
    picks: list[dict],
    video_id: str,
) -> list[dict]:
    """
    Extract quotes from a video by downloading audio and sending to Gemini.
    Used for non-English guests who lack text transcripts.
    Downloads audio via yt-dlp, uploads to Gemini, and extracts quotes.
    """
    import subprocess
    import tempfile
    import google.generativeai as genai

    guest_name = guest["name"]
    picks_list = format_picks_list(picks)

    # Download audio to temp file
    with tempfile.TemporaryDirectory() as tmpdir:
        audio_path = f"{tmpdir}/{video_id}.mp3"
        cmd = [
            "yt-dlp",
            "-x", "--audio-format", "mp3",
            "--audio-quality", "5",
            "-o", audio_path,
            f"https://www.youtube.com/watch?v={video_id}",
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if result.returncode != 0:
                log(f"  yt-dlp audio download failed: {result.stderr[:200]}")
                return []
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            log(f"  yt-dlp audio error: {e}")
            return []

        import pathlib
        if not pathlib.Path(audio_path).exists():
            # yt-dlp may append different extension
            import glob
            candidates = glob.glob(f"{tmpdir}/{video_id}.*")
            if candidates:
                audio_path = candidates[0]
            else:
                log(f"  Audio file not found after download")
                return []

        log(f"  Downloaded audio: {audio_path}")

        # Upload to Gemini
        try:
            audio_file = genai.upload_file(audio_path)
            log(f"  Uploaded audio to Gemini")
        except Exception as e:
            log(f"  Gemini upload error: {e}")
            return []

    # Generate prompt
    prompt = AUDIO_EXTRACTION_PROMPT.format(
        guest_name=guest_name,
        picks_list=picks_list,
    )

    try:
        response = model.generate_content([prompt, audio_file])
        response_text = response.text.strip()

        # Handle markdown code blocks
        if response_text.startswith("```"):
            response_text = re.sub(r"^```(?:json)?\s*", "", response_text)
            response_text = re.sub(r"\s*```$", "", response_text)

        quotes = json.loads(response_text)
        if not isinstance(quotes, list):
            log(f"  WARNING: Gemini returned non-list for audio extraction")
            return []

        cleaned = []
        for q in quotes:
            if not isinstance(q, dict):
                continue
            cleaned.append({
                "film_title": q.get("film_title", ""),
                "start_timestamp": int(q.get("start_timestamp", 0) or 0),
                "quote": (q.get("quote", "") or "")[:500],
                "confidence": q.get("confidence", "none"),
            })
        return cleaned

    except json.JSONDecodeError as e:
        log(f"  Audio JSON parse error: {e}")
        return []
    except Exception as e:
        log(f"  Audio Gemini error: {type(e).__name__}: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Extract quotes via Gemini")
    parser.add_argument("--pilot", action="store_true", help="Only process pilot guests")
    parser.add_argument("--limit", type=int, default=0, help="Limit guests to process")
    parser.add_argument("--guest-slug", type=str, help="Process only this guest")
    parser.add_argument("--force", action="store_true", help="Re-extract even if already done")
    args = parser.parse_args()

    # Load data
    guests = load_json(GUESTS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)
    existing_picks = load_json(PICKS_FILE)

    if not guests:
        log("ERROR: No guests. Run scrape_letterboxd.py first.")
        sys.exit(1)
    if not picks_raw:
        log("ERROR: No picks. Run scrape_letterboxd.py first.")
        sys.exit(1)

    # Load checkpoint
    checkpoint = load_json(CHECKPOINT_FILE) or {}

    # Initialize Gemini
    model = get_gemini_model()
    log("Gemini model initialized")

    # Filter guests
    if args.pilot:
        target_slugs = {slugify(n) for n in PILOT_GUESTS}
        guests = [g for g in guests if g["slug"] in target_slugs]
    if args.guest_slug:
        guests = [g for g in guests if g["slug"] == args.guest_slug]

    # Build picks index by guest slug
    picks_by_guest = {}
    for pick in picks_raw:
        slug = pick["guest_slug"]
        if slug not in picks_by_guest:
            picks_by_guest[slug] = []
        picks_by_guest[slug].append(pick)

    # Build existing picks index for merging
    existing_pick_index = {}
    for p in existing_picks:
        key = (p["guest_slug"], p.get("film_title", ""))
        existing_pick_index[key] = p

    processed = 0
    skipped = 0
    errors = 0

    guests_to_process = []
    audio_candidates = []
    for guest in guests:
        slug = guest["slug"]
        video_id = guest.get("youtube_video_id") or guest.get("vimeo_video_id")
        video_source = "youtube" if guest.get("youtube_video_id") else "vimeo"
        guest_picks = picks_by_guest.get(slug, [])

        if not video_id:
            log(f"  {guest['name']}: No video ID, skipping")
            skipped += 1
            continue

        if not guest_picks:
            log(f"  {guest['name']}: No picks, skipping")
            skipped += 1
            continue

        transcript_path = TRANSCRIPTS_DIR / f"{video_id}.json"
        if not transcript_path.exists():
            # No text transcript — candidate for audio fallback
            if video_source == "youtube":
                if not args.force and f"{slug}_audio" in checkpoint:
                    log(f"  {guest['name']}: Audio already processed (use --force)")
                    skipped += 1
                else:
                    # For multi-visit guests, only send visit-1 picks to audio fallback
                    # (visit-2 picks will be handled by the multi-visit second pass)
                    audio_picks = guest_picks
                    if len(guest.get("visits", [])) >= 2:
                        audio_picks = [p for p in guest_picks if p.get("visit_index", 1) == 1]
                        log(f"  {guest['name']}: No transcript for visit 1, queued for audio ({len(audio_picks)}/{len(guest_picks)} picks)")
                    else:
                        log(f"  {guest['name']}: No transcript, queued for audio fallback")
                    if audio_picks:
                        audio_candidates.append((guest, audio_picks, video_id))
            else:
                log(f"  {guest['name']}: No transcript (Vimeo, no audio fallback)")
                skipped += 1
            continue

        # Check checkpoint
        if not args.force and slug in checkpoint:
            log(f"  {guest['name']}: Already processed (use --force to re-extract)")
            skipped += 1
            continue

        guests_to_process.append((guest, guest_picks, transcript_path))

    if args.limit:
        guests_to_process = guests_to_process[:args.limit]

    log(f"Processing {len(guests_to_process)} guests, skipping {skipped}")

    for guest, guest_picks, transcript_path in tqdm(guests_to_process, desc="Extracting quotes"):
        slug = guest["slug"]
        video_id = guest.get("youtube_video_id") or guest.get("vimeo_video_id")
        video_source = "youtube" if guest.get("youtube_video_id") else "vimeo"
        log(f"  Processing {guest['name']} ({len(guest_picks)} picks)")

        # Load transcript
        transcript_data = load_json(transcript_path)
        if isinstance(transcript_data, list):
            segments = transcript_data
        else:
            segments = transcript_data.get("segments", [])

        if not segments:
            log(f"  Empty transcript for {guest['name']}")
            errors += 1
            continue

        # Extract quotes
        quotes = extract_quotes_for_guest(model, guest, guest_picks, segments)

        if not quotes:
            log(f"  No quotes extracted for {guest['name']}")
            errors += 1
            continue

        log(f"  Extracted {len(quotes)} quotes")

        # Merge quotes into picks
        # Match by film_title
        quotes_by_title = {q["film_title"].lower(): q for q in quotes}

        for pick in guest_picks:
            title = pick["film_title"]
            quote_match = quotes_by_title.get(title.lower())

            if quote_match:
                pick["quote"] = quote_match["quote"]
                pick["start_timestamp"] = quote_match["start_timestamp"]
                pick["extraction_confidence"] = quote_match["confidence"]
                # Tag with visit_index 1 (primary pass = first visit)
                pick["visit_index"] = 1
                if video_id and quote_match["start_timestamp"]:
                    if video_source == "vimeo":
                        pick["vimeo_timestamp_url"] = (
                            f"https://vimeo.com/{video_id}#t={quote_match['start_timestamp']}s"
                        )
                    else:
                        pick["youtube_timestamp_url"] = (
                            f"https://www.youtube.com/watch?v={video_id}&t={quote_match['start_timestamp']}"
                        )

            # Update existing picks index
            key = (slug, title)
            existing_pick_index[key] = pick

        # Update checkpoint
        checkpoint[slug] = {
            "processed_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "quotes_count": len(quotes),
            "picks_count": len(guest_picks),
        }
        save_json(CHECKPOINT_FILE, checkpoint)

        processed += 1
        time.sleep(6)  # Rate limit: ~10 RPM for Gemini

    # --- Audio fallback for non-English guests ---
    if audio_candidates:
        log(f"\nAudio fallback: {len(audio_candidates)} guest(s) without text transcripts")
        for guest, guest_picks, video_id in audio_candidates:
            slug = guest["slug"]
            log(f"  Audio extraction: {guest['name']} ({len(guest_picks)} picks)")

            quotes = extract_quotes_from_audio(model, guest, guest_picks, video_id)

            if not quotes:
                log(f"  No quotes from audio for {guest['name']}")
                errors += 1
                checkpoint[f"{slug}_audio"] = {
                    "processed_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "quotes_count": 0,
                    "picks_count": len(guest_picks),
                    "method": "audio",
                }
                save_json(CHECKPOINT_FILE, checkpoint)
                time.sleep(6)
                continue

            log(f"  Extracted {len(quotes)} quotes from audio")
            quotes_by_title = {q["film_title"].lower(): q for q in quotes}

            for pick in guest_picks:
                title = pick["film_title"]
                quote_match = quotes_by_title.get(title.lower())
                if quote_match:
                    pick["quote"] = quote_match["quote"]
                    pick["start_timestamp"] = quote_match["start_timestamp"]
                    pick["extraction_confidence"] = quote_match["confidence"]
                    pick["visit_index"] = 1
                    if video_id and quote_match["start_timestamp"]:
                        pick["youtube_timestamp_url"] = (
                            f"https://www.youtube.com/watch?v={video_id}&t={quote_match['start_timestamp']}"
                        )
                key = (slug, title)
                existing_pick_index[key] = pick

            checkpoint[f"{slug}_audio"] = {
                "processed_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "quotes_count": len(quotes),
                "picks_count": len(guest_picks),
                "method": "audio",
            }
            save_json(CHECKPOINT_FILE, checkpoint)
            processed += 1
            time.sleep(6)

    # --- Multi-visit second pass ---
    # For multi-visit guests, check if visit 2 has a transcript we can use
    # to fill in picks that still have confidence "none"
    multi_visit_processed = 0
    for guest in guests:
        slug = guest["slug"]
        visits = guest.get("visits", [])
        if len(visits) < 2:
            continue

        # Check if this guest has "none" confidence picks that might benefit
        guest_picks_in_index = [
            p for key, p in existing_pick_index.items()
            if key[0] == slug and p.get("extraction_confidence") in ("none", None)
        ]
        if not guest_picks_in_index:
            continue

        # Try each visit's transcript (skip visit 0 which was already processed above)
        for visit_idx in range(1, len(visits)):
            visit = visits[visit_idx]
            visit_video_id = visit.get("youtube_video_id") or visit.get("vimeo_video_id")
            if not visit_video_id:
                continue

            visit_transcript_path = TRANSCRIPTS_DIR / f"{visit_video_id}.json"
            if not visit_transcript_path.exists():
                continue

            # Check checkpoint for visit-specific processing
            visit_checkpoint_key = f"{slug}_visit{visit_idx + 1}"
            if not args.force and visit_checkpoint_key in checkpoint:
                continue

            visit_transcript_data = load_json(visit_transcript_path)
            visit_segments = visit_transcript_data.get("segments", [])
            if not visit_segments:
                continue

            # Get the raw picks for this guest (for the prompt)
            guest_raw_picks = picks_by_guest.get(slug, [])
            # Only send picks that have no quote yet
            none_picks = [p for p in guest_raw_picks if p.get("extraction_confidence") in ("none", None) or not p.get("quote")]
            if not none_picks:
                continue

            log(f"  Multi-visit pass: {guest['name']} visit {visit_idx + 1} — {len(none_picks)} picks without quotes")
            quotes = extract_quotes_for_guest(model, guest, none_picks, visit_segments)

            if quotes:
                visit_video_source = "youtube" if visit.get("youtube_video_id") else "vimeo"
                quotes_by_title = {q["film_title"].lower(): q for q in quotes}
                new_quotes_found = 0

                for pick in none_picks:
                    title = pick["film_title"]
                    quote_match = quotes_by_title.get(title.lower())
                    if quote_match and quote_match.get("quote") and quote_match["confidence"] != "none":
                        pick["quote"] = quote_match["quote"]
                        pick["start_timestamp"] = quote_match["start_timestamp"]
                        pick["extraction_confidence"] = quote_match["confidence"]
                        # Tag with visit_index (1-based: visit_idx 1 = visit 2)
                        pick["visit_index"] = visit_idx + 1
                        if visit_video_id and quote_match["start_timestamp"]:
                            if visit_video_source == "vimeo":
                                pick["vimeo_timestamp_url"] = (
                                    f"https://vimeo.com/{visit_video_id}#t={quote_match['start_timestamp']}s"
                                )
                            else:
                                pick["youtube_timestamp_url"] = (
                                    f"https://www.youtube.com/watch?v={visit_video_id}&t={quote_match['start_timestamp']}"
                                )
                        existing_pick_index[(slug, title)] = pick
                        new_quotes_found += 1

                log(f"    Found {new_quotes_found} new quotes from visit {visit_idx + 1}")
                multi_visit_processed += 1

            checkpoint[visit_checkpoint_key] = {
                "processed_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "quotes_count": len(quotes) if quotes else 0,
                "picks_count": len(none_picks),
            }
            save_json(CHECKPOINT_FILE, checkpoint)
            time.sleep(6)

    if multi_visit_processed:
        log(f"Multi-visit pass: processed {multi_visit_processed} additional transcripts")

    # Save all picks
    all_picks = list(existing_pick_index.values())

    # Post-process: clean up quotes
    from scripts.clean_quotes import clean_quote, build_title_map
    catalog = load_json(CATALOG_FILE)
    title_map = build_title_map(catalog)
    cleaned_count = 0
    for pick in all_picks:
        if pick.get("quote"):
            original = pick["quote"]
            pick["quote"] = clean_quote(original, title_map)
            if pick["quote"] != original:
                cleaned_count += 1
    log(f"Cleaned {cleaned_count} quotes")

    save_json(PICKS_FILE, all_picks)
    log(f"Saved {len(all_picks)} picks to {PICKS_FILE}")

    # Summary
    log(f"Processed: {processed}, Skipped: {skipped}, Errors: {errors}")

    # Confidence breakdown
    high = sum(1 for p in all_picks if p.get("extraction_confidence") == "high")
    medium = sum(1 for p in all_picks if p.get("extraction_confidence") == "medium")
    low = sum(1 for p in all_picks if p.get("extraction_confidence") == "low")
    none = sum(1 for p in all_picks if p.get("extraction_confidence") in ("none", None))
    total = len(all_picks)
    log(f"Confidence: high={high}, medium={medium}, low={low}, none={none} (total={total})")
    if total > 0:
        high_pct = (high / total) * 100
        log(f"High confidence rate: {high_pct:.1f}%")


if __name__ == "__main__":
    main()
