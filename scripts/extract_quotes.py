#!/usr/bin/env python3
"""
Extract quotes from transcripts using Gemini Flash.
For each guest with both picks and a transcript, sends the transcript + known picks
to Gemini and extracts verbatim quotes with timestamps.

Use --workers N to parallelize the transcript pass (32 recommended for full runs;
throughput is bound by per-call latency, not rate limits). The audio-fallback and
multi-visit passes always run serially after it.

Output: data/picks.json
"""

import argparse
import difflib
import json
import re
import sys
from pathlib import Path
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext

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

KNOWN PICKS (from curated data - these are the films they took home):
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


class GeminiModel:
    """
    Thin adapter over the google-genai client.

    The new SDK has no model handle -- the model name is an argument to each
    call -- while this module threads one around. Binding the name and config
    here keeps that shape, so callers still say model.generate_content(...).
    """

    def __init__(self, client, model_name: str, config):
        self._client = client
        self._model_name = model_name
        self._config = config

    def generate_content(self, contents):
        return self._client.models.generate_content(
            model=self._model_name,
            contents=contents,
            config=self._config,
        )

    def upload_file(self, path: str):
        return self._client.files.upload(file=path)


def get_gemini_model():
    """Initialize Gemini model."""
    from google import genai
    from google.genai import types

    api_key = get_env("GEMINI_API_KEY")
    client = genai.Client(api_key=api_key)

    config = types.GenerateContentConfig(
        temperature=0.1,
        response_mime_type="application/json",
        max_output_tokens=65536,
    )
    return GeminiModel(client, "gemini-3-flash-preview", config)


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
        lines.append(f"{i}. {title}")
    return "\n".join(lines)


BATCH_SIZE = 20  # Max picks per API call to avoid output truncation

def pick_has_quote(pick: dict) -> bool:
    """A pick counts as quoted only if it has text AND a confidence we trust."""
    return bool((pick.get("quote") or "").strip()) and pick.get(
        "extraction_confidence"
    ) not in (None, "", "none")


def _normalize_quote(text: str) -> str:
    return " ".join((text or "").lower().split())


def duplicates_existing_quote(
    candidate: str, existing: list[str], threshold: float = 0.75
) -> bool:
    """
    True when candidate is essentially a quote the guest already has elsewhere.

    Uses a similarity ratio rather than a prefix key: the real case that made
    this necessary put Breathless's words under Rashomon at 0.99 similarity,
    but the two texts were 200 and 203 characters and diverged partway, so any
    fixed-length prefix key missed it.

    The threshold errs toward blocking. When the model cannot find a real quote
    for a pick it tends to re-serve a neighbouring one lightly reworded -- Barry
    Jenkins's line about Weekend came back for The Apu Trilogy at 0.77, "movie"
    swapped for "series", still ending on getting Andrew Haigh to sign it. A
    pick left unquoted is the status quo; a pick given another film's words is a
    new error.
    """
    cand = _normalize_quote(candidate)
    if not cand:
        return False
    for other in existing:
        prior = _normalize_quote(other)
        if not prior:
            continue
        # autojunk must stay off. Above 200 characters difflib treats popular
        # elements as junk, which makes ratio() asymmetric: the pair that put A
        # Man Escaped's words on Salvatore Giuliano scored 0.92 one way round
        # and 0.36 the other, so the guard's verdict depended on argument order.
        if (
            difflib.SequenceMatcher(None, cand, prior, autojunk=False).ratio()
            >= threshold
        ):
            return True
    return False


def pick_index_key(pick: dict) -> tuple:
    """Stable key for merging enriched quote data without collapsing duplicate titles."""
    return (
        pick.get("guest_slug"),
        pick.get("visit_index"),
        pick.get("film_id") or pick.get("film_slug"),
    )


def parse_json_array_response(response_text: str) -> list:
    """Parse a Gemini response that should contain a JSON array."""
    response_text = response_text.strip()

    if response_text.startswith("```"):
        response_text = re.sub(r"^```(?:json)?\s*", "", response_text)
        response_text = re.sub(r"\s*```$", "", response_text)

    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        start = response_text.find("[")
        if start == -1:
            raise
        decoder = json.JSONDecoder()
        parsed, _ = decoder.raw_decode(response_text[start:])
        return parsed


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
        quotes = parse_json_array_response(response_text)

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

    guest_name = guest["name"]
    picks_list = format_picks_list(picks)

    # Download audio to temp file
    with tempfile.TemporaryDirectory() as tmpdir:
        audio_path = f"{tmpdir}/{video_id}.mp3"
        # Prefer the venv's yt-dlp over whatever is on PATH. YouTube changes
        # often enough that yt-dlp warns once a release is 90 days old and then
        # starts failing; the venv is what the project pins and updates, while
        # PATH here resolved to a Homebrew copy seven months behind, which is
        # why Bill Hader's audio fallback returned no quotes.
        ytdlp = Path(sys.executable).parent / "yt-dlp"
        cmd = [
            str(ytdlp) if ytdlp.exists() else "yt-dlp",
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
            audio_file = model.upload_file(audio_path)
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
        quotes = parse_json_array_response(response_text)
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


def _process_transcript_guest(
    model,
    guest: dict,
    guest_picks: list[dict],
    transcript_path,
    existing_pick_index: dict,
    checkpoint: dict,
    lock: threading.Lock | None = None,
    visit_index: int = 1,
    fill_missing_only: bool = False,
) -> bool:
    """
    Extract quotes for one guest from a text transcript and merge the results
    into existing_pick_index + checkpoint. Returns True on success.
    Pass a lock to make the shared-state updates thread-safe.

    With fill_missing_only, a pick that already has a quote is left exactly as
    it was. A whole-guest re-extraction is not safe once a guest has good
    quotes: on Barry Jenkins it recovered nothing, moved his Breathless quote
    onto Rashomon, and degraded another, so filling the gaps has to be additive.
    """
    slug = guest["slug"]
    video_id = guest.get("youtube_video_id") or guest.get("vimeo_video_id")
    video_source = "youtube" if guest.get("youtube_video_id") else "vimeo"

    transcript_data = load_json(transcript_path)
    if isinstance(transcript_data, list):
        segments = transcript_data
    else:
        segments = transcript_data.get("segments", [])

    if not segments:
        log(f"  Empty transcript for {guest['name']}")
        return False

    quotes = extract_quotes_for_guest(model, guest, guest_picks, segments)

    if not quotes:
        log(f"  No quotes extracted for {guest['name']}")
        return False

    log(f"  Extracted {len(quotes)} quotes for {guest['name']}")

    # Merge quotes into picks, matching by film_title
    quotes_by_title = {q["film_title"].lower(): q for q in quotes}

    # guest_picks comes from picks_raw.json, where every quote is empty -- the
    # quotes we already have live in existing_pick_index, loaded from
    # picks.json. So both checks below must consult the prior record, not the
    # raw pick, or fill_missing_only silently overwrites everything.
    prior_by_key = {
        key: existing_pick_index.get(key)
        for key in (pick_index_key(p) for p in guest_picks)
    }
    # Quotes the guest already has, so a re-run cannot copy one onto a second
    # pick -- the failure that put Breathless's words under Rashomon.
    existing_quotes = [
        prior.get("quote", "")
        for prior in prior_by_key.values()
        if prior is not None and pick_has_quote(prior)
    ]
    # Timestamps already spoken for. Two picks sharing a start_timestamp are two
    # slices of one utterance, which is how The Apu Trilogy -- a film the
    # transcript never even names -- came back holding Barry Jenkins's line
    # about Weekend, both at t=7.
    existing_timestamps = {
        prior.get("start_timestamp")
        for prior in prior_by_key.values()
        if prior is not None
        and pick_has_quote(prior)
        and prior.get("start_timestamp") is not None
    }

    with lock if lock is not None else nullcontext():
        for pick in guest_picks:
            title = pick["film_title"]
            quote_match = quotes_by_title.get(title.lower())
            key = pick_index_key(pick)
            prior = prior_by_key.get(key)

            if fill_missing_only and prior is not None and pick_has_quote(prior):
                # Keep the whole prior record: quote, timestamp and link alike.
                existing_pick_index[key] = prior
                continue

            if quote_match and fill_missing_only:
                # The model returns a row per pick whether or not it found
                # anything; an empty or "none"-confidence row is not a fill, and
                # writing it resets a good start_timestamp to 0.
                if not (quote_match.get("quote") or "").strip() or quote_match.get(
                    "confidence"
                ) in (None, "", "none"):
                    quote_match = None

            if quote_match and fill_missing_only:
                ts = quote_match.get("start_timestamp")
                if ts is not None and ts in existing_timestamps:
                    log(
                        f"    Skipping {title}: same timestamp ({ts}s) as a quote"
                        f" already on another {guest['name']} pick"
                    )
                    quote_match = None
                elif duplicates_existing_quote(quote_match["quote"], existing_quotes):
                    log(
                        f"    Skipping {title}: text duplicates a quote already on"
                        f" another {guest['name']} pick"
                    )
                    quote_match = None

            if fill_missing_only and not quote_match and prior is not None:
                # Nothing to add, so leave the record exactly as it is. Falling
                # through would replace the enriched picks.json entry with the
                # bare picks_raw one and drop fields the later pipeline steps
                # added (box-set URLs, pick_order, timestamp links).
                existing_pick_index[key] = prior
                continue

            if quote_match:
                pick["quote"] = quote_match["quote"]
                pick["start_timestamp"] = quote_match["start_timestamp"]
                pick["extraction_confidence"] = quote_match["confidence"]
                # Primary pass is the first visit unless --visit targeted another.
                pick["visit_index"] = visit_index
                # `is not None`, not truthiness: a pick discussed at 0:00 has a
                # timestamp of 0, which is falsy and would silently lose its link.
                if video_id and quote_match["start_timestamp"] is not None:
                    if video_source == "vimeo":
                        pick["vimeo_timestamp_url"] = (
                            f"https://vimeo.com/{video_id}#t={quote_match['start_timestamp']}s"
                        )
                    else:
                        pick["youtube_timestamp_url"] = (
                            f"https://www.youtube.com/watch?v={video_id}&t={quote_match['start_timestamp']}"
                        )

            if fill_missing_only and prior is not None:
                # pick comes from picks_raw and lacks fields the pipeline adds
                # later -- film_slug, box-set URLs, pick_order. Writing it whole
                # nulls them, so layer the new quote over the record we have.
                merged = dict(prior)
                merged.update({k: v for k, v in pick.items() if v is not None})
                existing_pick_index[key] = merged
            else:
                existing_pick_index[pick_index_key(pick)] = pick

        checkpoint[slug] = {
            "processed_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "quotes_count": len(quotes),
            "picks_count": len(guest_picks),
        }
        save_json(CHECKPOINT_FILE, checkpoint)

    return True


def main():
    parser = argparse.ArgumentParser(description="Extract quotes via Gemini")
    parser.add_argument("--pilot", action="store_true", help="Only process pilot guests")
    parser.add_argument("--limit", type=int, default=0, help="Limit guests to process")
    parser.add_argument("--guest-slug", type=str, help="Process only this guest")
    parser.add_argument("--force", action="store_true", help="Re-extract even if already done")
    parser.add_argument(
        "--fill-missing",
        action="store_true",
        help="Only write quotes to picks that have none; never touch an existing quote. "
             "Implies --force, since the guests needing this are already checkpointed.",
    )
    parser.add_argument("--visit", type=int, default=None,
                        help="Extract only this visit number (1-indexed)")
    parser.add_argument("--workers", type=int, default=1,
                        help="Parallel workers for the transcript pass (default 1 = serial; 32 recommended for full runs)")
    args = parser.parse_args()

    # Load data
    guests = load_json(GUESTS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)
    existing_picks = load_json(PICKS_FILE)

    if not guests:
        log("ERROR: No guests. Run scrape_criterion_picks.py first.")
        sys.exit(1)
    if not picks_raw:
        log("ERROR: No picks. Run scrape_criterion_picks.py first.")
        sys.exit(1)

    # Load checkpoint
    checkpoint = load_json(CHECKPOINT_FILE) or {}

    # Initialize Gemini
    model = get_gemini_model()
    log("Gemini model initialized")

    # Filter guests
    # The guests with gaps are all marked done, so this would otherwise no-op.
    if args.fill_missing:
        args.force = True
        if not args.guest_slug:
            # Only guests that actually have a gap; the rest would be 350+
            # pointless model calls that can only leave their picks unchanged.
            gap_slugs = {
                p["guest_slug"]
                for p in existing_picks
                if not pick_has_quote(p)
            }
            guests = [g for g in guests if g["slug"] in gap_slugs]
            log(f"--fill-missing: {len(guests)} guests have at least one unquoted pick")

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

    # Filter to specific visit if requested
    if args.visit is not None:
        for slug in picks_by_guest:
            picks_by_guest[slug] = [
                p for p in picks_by_guest[slug]
                if p.get("visit_index", 1) == args.visit
            ]

    # Build existing picks index for merging
    existing_pick_index = {}
    for p in existing_picks:
        existing_pick_index[pick_index_key(p)] = p

    processed = 0
    skipped = 0
    errors = 0

    guests_to_process = []
    audio_candidates = []
    for guest in guests:
        slug = guest["slug"]
        source_visit = guest
        # --visit N filters the picks to visit N, so the transcript has to come
        # from THAT visit's video. Reading the guest's primary (visit-1) video
        # here would score visit-1 speech against visit-N films.
        if args.visit is not None:
            match = [v for v in guest.get("visits", []) if v.get("visit_index") == args.visit]
            if match:
                source_visit = match[0]
        video_id = source_visit.get("youtube_video_id") or source_visit.get("vimeo_video_id")
        video_source = "youtube" if source_visit.get("youtube_video_id") else "vimeo"
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

    if args.workers <= 1:
        for guest, guest_picks, transcript_path in tqdm(guests_to_process, desc="Extracting quotes"):
            log(f"  Processing {guest['name']} ({len(guest_picks)} picks)")
            if _process_transcript_guest(
                model, guest, guest_picks, transcript_path, existing_pick_index, checkpoint,
                visit_index=args.visit if args.visit is not None else 1,
                fill_missing_only=args.fill_missing,
            ):
                processed += 1
                time.sleep(6)  # Rate limit: ~10 RPM for Gemini
            else:
                errors += 1
    else:
        # Parallel transcript pass: each worker thread gets its own Gemini model;
        # shared state (pick index + checkpoint) is guarded by a lock.
        log(f"Running transcript pass with {args.workers} parallel workers")
        thread_local = threading.local()
        state_lock = threading.Lock()

        def process_one(item) -> bool:
            guest, guest_picks, transcript_path = item
            if not hasattr(thread_local, "model"):
                thread_local.model = get_gemini_model()
            log(f"  Processing {guest['name']} ({len(guest_picks)} picks)")
            try:
                return _process_transcript_guest(
                    thread_local.model, guest, guest_picks, transcript_path,
                    existing_pick_index, checkpoint, lock=state_lock,
                    visit_index=args.visit if args.visit is not None else 1,
                    fill_missing_only=args.fill_missing,
                )
            except Exception as e:
                log(f"  Error: {guest['name']}: {e}")
                return False

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            for ok in executor.map(process_one, guests_to_process):
                if ok:
                    processed += 1
                else:
                    errors += 1

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
                # Same rule as the transcript path: under --fill-missing this may
                # only add. guest_picks comes from picks_raw and carries no
                # quotes, so the live record has to be consulted instead.
                audio_prior = existing_pick_index.get(pick_index_key(pick))
                if args.fill_missing and audio_prior is not None and pick_has_quote(
                    audio_prior
                ):
                    existing_pick_index[pick_index_key(audio_prior)] = audio_prior
                    continue
                if args.fill_missing and not quote_match and audio_prior is not None:
                    existing_pick_index[pick_index_key(audio_prior)] = audio_prior
                    continue
                if quote_match:
                    pick["quote"] = quote_match["quote"]
                    pick["start_timestamp"] = quote_match["start_timestamp"]
                    pick["extraction_confidence"] = quote_match["confidence"]
                    pick["visit_index"] = 1
                    # `is not None`, not truthiness: a pick discussed at 0:00 has a
                    # timestamp of 0, which is falsy and would silently lose its link.
                    if video_id and quote_match["start_timestamp"] is not None:
                        pick["youtube_timestamp_url"] = (
                            f"https://www.youtube.com/watch?v={video_id}&t={quote_match['start_timestamp']}"
                        )
                existing_pick_index[pick_index_key(pick)] = pick

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
    # Skip when --visit is set (targeting a specific visit only)
    multi_visit_processed = 0
    if args.visit is not None:
        log(f"Skipping multi-visit second pass (--visit {args.visit} set)")
    else:
        for guest in guests:
            slug = guest["slug"]
            visits = guest.get("visits", [])
            if len(visits) < 2:
                continue

            # Check if this guest has "none" confidence picks that might benefit
            guest_picks_in_index = [
                p for key, p in existing_pick_index.items()
                if p.get("guest_slug") == slug and p.get("extraction_confidence") in ("none", None)
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
                # Only send picks that have no quote yet. picks_raw carries no
                # quotes at all, so testing the raw pick selects every pick the
                # guest has: that is how a visit-2 pass rewrote seven of Bill
                # Hader's visit-1 quotes. The live state is in existing_pick_index.
                none_picks = []
                for raw_pick in guest_raw_picks:
                    current = existing_pick_index.get(pick_index_key(raw_pick))
                    if current is None or not pick_has_quote(current):
                        none_picks.append(raw_pick)
                if not none_picks:
                    continue

                log(f"  Multi-visit pass: {guest['name']} visit {visit_idx + 1} — {len(none_picks)} picks without quotes")
                quotes = extract_quotes_for_guest(model, guest, none_picks, visit_segments)

                if quotes:
                    visit_video_source = "youtube" if visit.get("youtube_video_id") else "vimeo"
                    quotes_by_title = {q["film_title"].lower(): q for q in quotes}
                    new_quotes_found = 0

                    # Same duplicate rules as the transcript path. Without them
                    # this pass gave A Man Escaped the words already sitting on
                    # Salvatore Giuliano, at 0.93 similarity.
                    guest_existing = [
                        p2.get("quote", "")
                        for p2 in existing_pick_index.values()
                        if p2.get("guest_slug") == slug and pick_has_quote(p2)
                    ]
                    guest_timestamps = {
                        p2.get("start_timestamp")
                        for p2 in existing_pick_index.values()
                        if p2.get("guest_slug") == slug
                        and pick_has_quote(p2)
                        and p2.get("start_timestamp") is not None
                    }

                    for pick in none_picks:
                        title = pick["film_title"]
                        quote_match = quotes_by_title.get(title.lower())
                        if quote_match:
                            ts = quote_match.get("start_timestamp")
                            if ts is not None and ts in guest_timestamps:
                                log(
                                    f"    Skipping {title}: same timestamp ({ts}s)"
                                    f" as an existing {guest['name']} quote"
                                )
                                quote_match = None
                            elif duplicates_existing_quote(
                                quote_match.get("quote", ""), guest_existing
                            ):
                                log(
                                    f"    Skipping {title}: text duplicates an"
                                    f" existing {guest['name']} quote"
                                )
                                quote_match = None
                        if quote_match and quote_match.get("quote") and quote_match["confidence"] != "none":
                            # visit_index is part of pick_index_key, so retagging
                            # a pick without dropping its old entry inserts a
                            # second record instead of moving the one that exists.
                            old_key = pick_index_key(pick)
                            pick["quote"] = quote_match["quote"]
                            pick["start_timestamp"] = quote_match["start_timestamp"]
                            pick["extraction_confidence"] = quote_match["confidence"]
                            # Tag with visit_index (1-based: visit_idx 1 = visit 2)
                            pick["visit_index"] = visit_idx + 1
                            if pick_index_key(pick) != old_key:
                                existing_pick_index.pop(old_key, None)
                            guest_existing.append(pick["quote"])
                            if pick.get("start_timestamp") is not None:
                                guest_timestamps.add(pick["start_timestamp"])
                            # `is not None`, not truthiness: a pick discussed at 0:00 has a
                            # timestamp of 0, which is falsy and would silently lose its link.
                            if visit_video_id and quote_match["start_timestamp"] is not None:
                                if visit_video_source == "vimeo":
                                    pick["vimeo_timestamp_url"] = (
                                        f"https://vimeo.com/{visit_video_id}#t={quote_match['start_timestamp']}s"
                                    )
                                else:
                                    pick["youtube_timestamp_url"] = (
                                        f"https://www.youtube.com/watch?v={visit_video_id}&t={quote_match['start_timestamp']}"
                                    )
                            existing_pick_index[pick_index_key(pick)] = pick
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
            pick["quote"] = clean_quote(original, title_map, pick.get("film_title"))
            if pick["quote"] != original:
                cleaned_count += 1
    log(f"Cleaned {cleaned_count} quotes")

    # Re-apply quote corrections last, so a re-extraction cannot silently revert
    # a documented fix. Mirrors apply_pick_overrides in the scraper.
    from scripts.utils import apply_quote_overrides
    overridden = apply_quote_overrides(all_picks)
    if overridden:
        log(f"Quote overrides applied: {overridden}")

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
