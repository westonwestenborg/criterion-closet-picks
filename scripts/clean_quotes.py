#!/usr/bin/env python3
"""
Post-process extracted quotes to clean up auto-caption artifacts.
- Remove filler words (um, uh, repeated words)
- Fix capitalization (sentence starts, "I", known film titles)
- Normalize whitespace and trailing punctuation

Can be run standalone or called from extract_quotes.py.

Usage:
  python scripts/clean_quotes.py                  # Clean all quotes in picks.json
  python scripts/clean_quotes.py --dry-run        # Preview changes without saving
  python scripts/clean_quotes.py --guest-slug X   # Clean only one guest
"""

import argparse
import json
import re
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import PICKS_FILE, CATALOG_FILE, load_json, save_json, log


# Filler patterns to remove (with word boundaries)
FILLER_PATTERNS = [
    r"\buh\b",
    r"\bum\b",
    r"\bhmm\b",
    r"\bahh?\b",
]

# Repeated word patterns: "like like", "the the", etc.
REPEATED_WORD_RE = re.compile(r"\b(\w+)\s+\1\b", re.IGNORECASE)


def build_title_map(catalog: list[dict]) -> dict[str, str]:
    """
    Build a map of lowercase film titles to their correct capitalization.
    Includes titles from the catalog and common known corrections.
    """
    title_map = {}
    for entry in catalog:
        title = entry.get("title", "")
        if title:
            title_map[title.lower()] = title
            # Also map cleaned title (without parenthetical annotations)
            cleaned = re.sub(r"\s*\([^)]*\)\s*$", "", title).strip()
            if cleaned:
                title_map[cleaned.lower()] = cleaned

    # Common transcription corrections
    known_corrections = {
        "rack catcher": "Ratcatcher",
        "rat catcher": "Ratcatcher",
        "decalog": "Dekalog",
        "decalogue": "Dekalog",
        "rashaman": "Rashomon",
        "roshomon": "Rashomon",
        "film school": "film school",  # keep lowercase
    }
    title_map.update({k.lower(): v for k, v in known_corrections.items()})

    return title_map


def remove_fillers(text: str) -> str:
    """Remove filler words (uh, um, etc.) from text."""
    for pattern in FILLER_PATTERNS:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)
    # Clean up resulting double spaces
    text = re.sub(r"\s{2,}", " ", text).strip()
    return text


def deduplicate_words(text: str) -> str:
    """Remove immediately repeated words: 'like like like' -> 'like'."""
    prev = None
    while prev != text:
        prev = text
        text = REPEATED_WORD_RE.sub(r"\1", text)
    return text


def fix_capitalization(text: str) -> str:
    """Fix basic capitalization: start of text, after sentence-ending punctuation, standalone 'I'."""
    if not text:
        return text

    # Capitalize first character
    text = text[0].upper() + text[1:]

    # Capitalize after sentence-ending punctuation
    text = re.sub(
        r"([.!?])\s+([a-z])",
        lambda m: m.group(1) + " " + m.group(2).upper(),
        text,
    )

    # Capitalize standalone "i" (but not inside words)
    text = re.sub(r"\bi\b", "I", text)
    # Fix "I'm", "I've", "I'll", "I'd" that may have been lowercased
    text = re.sub(r"\bi'([mvedlst])", lambda m: "I'" + m.group(1), text)

    return text


def fix_film_titles(text: str, title_map: dict[str, str]) -> str:
    """
    Fix known film title capitalization in quotes.
    Uses word boundaries to avoid false substring matches (e.g. "Birth" inside "birthday").
    """
    for lower_title, correct_title in title_map.items():
        if len(lower_title) < 5:
            continue  # Skip very short titles to avoid false matches
        # Use word boundaries to avoid matching inside other words
        pattern = re.compile(r"\b" + re.escape(lower_title) + r"\b", re.IGNORECASE)
        text = pattern.sub(correct_title, text)
    return text


def normalize_whitespace(text: str) -> str:
    """Normalize whitespace and clean up punctuation artifacts."""
    # Collapse multiple spaces
    text = re.sub(r"\s{2,}", " ", text)
    # Remove space before punctuation
    text = re.sub(r"\s+([.,!?;:])", r"\1", text)
    # Ensure space after punctuation (but not inside numbers like 3.5)
    text = re.sub(r"([.,!?;:])([A-Za-z])", r"\1 \2", text)
    return text.strip()


def add_trailing_ellipsis(text: str) -> str:
    """Add ellipsis if the quote ends mid-sentence (no terminal punctuation)."""
    if not text:
        return text
    if text[-1] not in ".!?\"'":
        # Looks like it was cut off mid-sentence
        text = text.rstrip(",;: ") + "..."
    return text


def clean_quote(text: str, title_map: dict[str, str]) -> str:
    """Apply all cleaning steps to a single quote."""
    if not text or not text.strip():
        return ""

    text = remove_fillers(text)
    text = deduplicate_words(text)
    text = fix_film_titles(text, title_map)
    text = fix_capitalization(text)
    text = normalize_whitespace(text)
    text = add_trailing_ellipsis(text)

    # Enforce 500-char limit
    if len(text) > 500:
        text = text[:497] + "..."

    return text


def main():
    parser = argparse.ArgumentParser(description="Clean up extracted quotes")
    parser.add_argument("--dry-run", action="store_true", help="Preview without saving")
    parser.add_argument("--guest-slug", type=str, help="Only clean this guest")
    args = parser.parse_args()

    picks = load_json(PICKS_FILE)
    catalog = load_json(CATALOG_FILE)

    if not picks:
        log("No picks to clean")
        return

    title_map = build_title_map(catalog)
    log(f"Built title map with {len(title_map)} entries")

    cleaned_count = 0
    changed_count = 0

    for pick in picks:
        if args.guest_slug and pick.get("guest_slug") != args.guest_slug:
            continue

        original = pick.get("quote", "")
        if not original:
            continue

        cleaned = clean_quote(original, title_map)
        cleaned_count += 1

        if cleaned != original:
            changed_count += 1
            if args.dry_run:
                log(f"\n{pick['guest_slug']}: {pick.get('film_title', '?')}")
                log(f"  BEFORE: {original[:200]}")
                log(f"  AFTER:  {cleaned[:200]}")
            pick["quote"] = cleaned

    log(f"Processed {cleaned_count} quotes, changed {changed_count}")

    if not args.dry_run:
        save_json(PICKS_FILE, picks)
        log(f"Saved cleaned quotes to {PICKS_FILE}")
    else:
        log("(dry run — no changes saved)")


if __name__ == "__main__":
    main()
