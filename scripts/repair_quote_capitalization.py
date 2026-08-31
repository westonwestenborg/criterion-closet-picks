#!/usr/bin/env python3
"""
Repair quotes where an ordinary word was capitalized because it is also a
Criterion title.

fix_film_titles in clean_quotes.py used to substitute any catalog title it found
in a quote, so the 125 titles that are also ordinary English words (House,
Mother, Performance, Festival...) rewrote the common noun: "watching my Mother
on the stage", "Ingrid Bergman's Performance in this is...". clean_quotes.py now
skips ambiguous titles unless the guest is naming their own pick, but that only
prevents new damage -- re-cleaning cannot undo a capital already in picks.json.

Two lists are involved, and they are deliberately different sizes.
ambiguous_film_titles.json holds all 125 collisions and is what clean_quotes.py
declines to substitute -- over-inclusion there is free. quote_repair_words.json
is the subset safe to *rewrite*, and over-inclusion there causes damage, so it
holds only words with a genuine everyday meaning. Bound, Shaft, Naked, Stalker,
Polyester and Mirror were dropped from it after review: guests name those films
often enough, and the transcripts lowercase them, so the checks below cleared
real title references (Gershon on Bound, Ryder on Shaft, Baker on Naked).

The guest's own transcript is the authority. For each suspect occurrence this
checks how the word is written mid-sentence in that guest's transcript and only
lowercases ours when every mid-sentence occurrence in the source is lowercase.
Three further guards keep a correct capital: the word is the guest's own pick,
an adjacent word is capitalized so this is a proper-noun phrase ("Istanbul Film
Festival"), or the occurrence sits inside a longer catalog title ("Where Is the
Friend's House?").

Occurrences are left untouched when the transcript capitalizes the word, does
not contain it, or is not on disk (transcripts are gitignored and regenerable).

Re-runnable: idempotent. Running twice is a no-op.

Usage:
  python scripts/repair_quote_capitalization.py --dry-run
  python scripts/repair_quote_capitalization.py
  python scripts/repair_quote_capitalization.py --guest-slug SLUG
"""

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    DATA_DIR,
    GUESTS_FILE,
    PICKS_FILE,
    load_json,
    save_json,
    log,
)

TRANSCRIPT_DIR = DATA_DIR / "transcripts"
REPAIR_WORDS_FILE = DATA_DIR / "quote_repair_words.json"

# A capital is expected after these, so an occurrence there is a sentence start
# rather than a title substitution.
SENTENCE_END = '.!?:"“”'


def _norm(text: str) -> str:
    """Lowercase and flatten smart apostrophes so titles and quotes compare."""
    return text.lower().replace("\u2019", "'").replace("\u2018", "'")


def transcript_text(slug: str, guests: dict[str, dict], cache: dict) -> str | None:
    """Concatenated transcript text for a guest, or None if not on disk."""
    if slug in cache:
        return cache[slug]

    text = None
    video_id = (guests.get(slug) or {}).get("youtube_video_id")
    if video_id:
        path = TRANSCRIPT_DIR / f"{video_id}.json"
        if path.exists():
            data = json.loads(path.read_text())
            # Older files are a bare segment list; newer ones wrap it in a dict.
            segments = data if isinstance(data, list) else data.get("segments", [])
            text = " ".join(
                s.get("text", "") for s in segments if isinstance(s, dict)
            )
    cache[slug] = text
    return text


def _mid_sentence_spans(text: str, word: str) -> list[re.Match]:
    """Occurrences of word in text that are not at a sentence start."""
    out = []
    for m in re.finditer(r"\b" + re.escape(word) + r"\b", text, re.IGNORECASE):
        before = text[: m.start()].rstrip()
        if before and before[-1] in SENTENCE_END:
            continue
        out.append(m)
    return out


def source_says_lowercase(word: str, transcript: str) -> bool:
    """
    True when every mid-sentence occurrence of word in the transcript is
    lowercase. A single capitalized occurrence means the guest may well be
    naming the film (or a proper noun that contains the word), so we leave it.
    """
    occurrences = [
        transcript[m.start() : m.end()] for m in _mid_sentence_spans(transcript, word)
    ]
    if not occurrences:
        return False
    return all(o[0].islower() for o in occurrences)


def _has_capitalized_neighbour(text: str, m: re.Match) -> bool:
    """
    True when the adjacent word is capitalized, which means this occurrence sits
    inside a proper-noun phrase -- "Naked Island", "Istanbul Film Festival" --
    where the capital belongs even though the word is an ordinary one.
    """
    before = re.search(r"(\S+)\s+$", text[: m.start()])
    if before and before.group(1)[:1].isupper():
        # A possessive is the ordinary way to introduce a common noun -- "Ingrid
        # Bergman's performance" -- so it is not evidence of a proper-noun phrase.
        if not re.search(r"[’']s$", before.group(1)):
            return True
    after = re.match(r"\s+(\S+)", text[m.end() :])
    if after and after.group(1)[:1].isupper():
        return True
    return False


def build_longer_title_index(
    catalog: list[dict], words: dict[str, str]
) -> dict[str, list[str]]:
    """
    For each repair word, the lowercased catalog titles that contain it and are
    longer than it. An occurrence sitting inside one of these is part of a film
    title -- "Where Is the Friend's House?", "The Naked Island" -- so the capital
    belongs even though the bare word is an ordinary one.
    """
    index: dict[str, list[str]] = {w: [] for w in words}
    titles: set[str] = set()
    for entry in catalog:
        title = entry.get("title") or ""
        if not title:
            continue
        titles.add(_norm(title))
        # Guests say the film, not the set it ships in: the catalog has "The
        # Koker Trilogy: Where Is the Friend's House?" while the quote says only
        # the half after the colon.
        if ":" in title:
            tail = title.split(":", 1)[1].strip()
            if tail:
                titles.add(_norm(tail))
    for word in words:
        needle = re.compile(r"\b" + re.escape(word) + r"\b")
        index[word] = [
            t for t in titles if len(t) > len(word) and needle.search(t)
        ]
    return index


def _inside_longer_title(quote: str, m: re.Match, longer_titles: list[str]) -> bool:
    """True when this occurrence falls inside a longer catalog title in the quote."""
    low = _norm(quote)
    for title in longer_titles:
        start = low.find(title)
        while start != -1:
            if start <= m.start() and m.end() <= start + len(title):
                return True
            start = low.find(title, start + 1)
    return False


def repair_quote(
    quote: str,
    own_title: str,
    transcript: str | None,
    ambiguous: dict[str, str],
    longer_titles: dict[str, list[str]] | None = None,
) -> tuple[str, Counter]:
    """Lowercase transcript-confirmed false capitals. Returns (quote, counts)."""
    changed: Counter = Counter()
    if not quote or transcript is None:
        return quote, changed

    own_low = (own_title or "").lower()
    for low, canonical in ambiguous.items():
        if low in own_low:
            continue  # the guest is naming their own pick; the capital is right
        if canonical not in quote:
            continue
        if not source_says_lowercase(low, transcript):
            continue
        # Rebuild right-to-left so earlier match offsets stay valid.
        for m in reversed(_mid_sentence_spans(quote, canonical)):
            if quote[m.start() : m.end()] != canonical:
                continue  # already lowercase, or cased differently
            if _has_capitalized_neighbour(quote, m):
                continue  # part of a proper-noun phrase
            if longer_titles and _inside_longer_title(quote, m, longer_titles.get(low, [])):
                continue  # part of a longer film title
            quote = quote[: m.start()] + low + quote[m.end() :]
            changed[canonical] += 1
    return quote, changed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Preview, do not save")
    parser.add_argument("--guest-slug", help="Repair only this guest")
    parser.add_argument(
        "--show", type=int, default=25, help="How many changes to print (default 25)"
    )
    args = parser.parse_args()

    ambiguous = {t.lower(): t for t in load_json(REPAIR_WORDS_FILE)}
    guests = {g["slug"]: g for g in load_json(GUESTS_FILE)}
    picks = load_json(PICKS_FILE)
    longer_titles = build_longer_title_index(load_json(CATALOG_FILE), ambiguous)
    log(f"Loaded {len(picks)} picks, {len(ambiguous)} repair words")

    cache: dict[str, str | None] = {}
    per_word: Counter = Counter()
    touched = 0
    shown = 0

    for pick in picks:
        if args.guest_slug and pick.get("guest_slug") != args.guest_slug:
            continue
        original = pick.get("quote") or ""
        if not original:
            continue

        transcript = transcript_text(pick["guest_slug"], guests, cache)
        repaired, changed = repair_quote(
            original, pick.get("film_title", ""), transcript, ambiguous, longer_titles
        )
        if not changed:
            continue

        touched += 1
        per_word.update(changed)
        if shown < args.show:
            shown += 1
            log(f"\n{pick['guest_slug']}: {pick.get('film_title', '?')}  {dict(changed)}")
            log(f"  BEFORE: {original[:180]}")
            log(f"  AFTER:  {repaired[:180]}")
        pick["quote"] = repaired

    log(f"\nPicks changed: {touched}")
    log(f"Occurrences lowercased: {sum(per_word.values())}")
    for word, n in per_word.most_common():
        log(f"   {n:4}  {word}")

    if args.dry_run:
        log("\nDry run -- no files written.")
        return

    save_json(PICKS_FILE, picks)
    log(f"\nSaved {PICKS_FILE.name}")


if __name__ == "__main__":
    main()
