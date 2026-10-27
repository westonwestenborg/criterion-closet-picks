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
from scripts.utils import DATA_DIR, PICKS_FILE, CATALOG_FILE, load_json, save_json, log

AMBIGUOUS_TITLES_FILE = DATA_DIR / "ambiguous_film_titles.json"


_AMBIGUOUS_CACHE: set[str] | None = None


def load_ambiguous_titles() -> set[str]:
    """
    Catalog titles that are also ordinary English words (House, Mother,
    Performance, Festival...) or everyday phrases (The Most Beautiful, Real
    Life, The Kid). fix_film_titles matches a title anywhere in a
    quote, so for these it is as likely to hit the common noun as the film --
    which is how quotes ended up reading "my Mother on the stage" and "at that
    friend's House". Substituting them is only safe when the guest is naming
    their own pick, so they are skipped otherwise.
    """
    global _AMBIGUOUS_CACHE
    if _AMBIGUOUS_CACHE is None:
        try:
            _AMBIGUOUS_CACHE = {t.lower() for t in load_json(AMBIGUOUS_TITLES_FILE)}
        except FileNotFoundError:
            _AMBIGUOUS_CACHE = set()
    return _AMBIGUOUS_CACHE


# Filler patterns to remove (with word boundaries)
FILLER_PATTERNS = [
    r"\buh\b",
    r"\bum\b",
    r"\bhmm\b",
    r"\bahh?\b",
]
# The same words as one alternation, for fillers set off by commas.
FILLER_WORDS = r"(?:uh|um|hmm|ahh?)"

# Repeated word patterns: "like like", "the the", etc.
REPEATED_WORD_RE = re.compile(r"\b(\w+)\s+\1\b", re.IGNORECASE)

# Words whose doubling is grammar, not a stutter: "I had had enough",
# "I think that that's the one". Collapsing these drops a word the guest said.
KEEP_REPEATED = {"had", "that"}


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
    # A filler set off by commas takes its commas with it: "amazing, uh,
    # scenery" -> "amazing scenery", "Uh, great movie" -> "great movie".
    text = re.sub(rf",\s*\b{FILLER_WORDS}\b\s*,", " ", text, flags=re.IGNORECASE)
    text = re.sub(rf",\s*\b{FILLER_WORDS}\b(?=\s*[.!?…])", "", text, flags=re.IGNORECASE)
    # Speech that opens with a filler keeps its capital: '“Uh, you have' ->
    # '“You have'.
    text = re.sub(
        rf"([“\"‘])\s*\b{FILLER_WORDS}\b\s*,\s*(\w)",
        lambda m: m.group(1) + m.group(2).upper(),
        text,
        flags=re.IGNORECASE,
    )
    # Anywhere else, the comma after a filler goes too: "the myth. Uh, I love"
    # -> "the myth. I love", "And uh, I saw" -> "And I saw".
    text = re.sub(rf"\b{FILLER_WORDS}\b\s*,\s*", "", text, flags=re.IGNORECASE)
    for pattern in FILLER_PATTERNS:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)
    # Anything the rules above missed still must not leave ",," behind, or a
    # comma straight after a full stop, or one at the very start.
    text = re.sub(r",(\s*,)+", ",", text)
    text = re.sub(r"([.!?…])\s*,", r"\1", text)
    text = re.sub(r"^(?:[\s,;]|[.!?](?!\.))+", "", text)
    # Same for speech that opened with a filler: 'He said, “Uh, you have to'.
    text = re.sub(
        r"(^|\s)([“\"‘])\s*,\s*(\w)",
        lambda m: m.group(1) + m.group(2) + m.group(3).upper(),
        text,
    )
    # Clean up resulting double spaces
    text = re.sub(r"\s{2,}", " ", text).strip()
    return text


_DOUBLED_TITLE_WORDS: dict[int, frozenset[str]] = {}


def doubled_title_words(title_map: dict[str, str]) -> frozenset[str]:
    """Words a catalog title doubles ("yi" for Yi Yi, "jo" for Jo Jo Dancer)."""
    key = id(title_map)
    if key not in _DOUBLED_TITLE_WORDS:
        _DOUBLED_TITLE_WORDS[key] = frozenset(
            m.group(1).lower() for t in title_map for m in REPEATED_WORD_RE.finditer(t)
        )
    return _DOUBLED_TITLE_WORDS[key]


def deduplicate_words(text: str, keep: frozenset[str] = frozenset()) -> str:
    """
    Remove immediately repeated words: 'like like like' -> 'like'.

    Words in keep are left doubled. clean_quote passes the words catalog titles
    double, since collapsing them turned 18 guests' "Yi Yi" into "Yi".
    """
    def collapse(m: re.Match) -> str:
        word = m.group(1).lower()
        return m.group(0) if word in KEEP_REPEATED or word in keep else m.group(1)

    prev = None
    while prev != text:
        prev = text
        text = REPEATED_WORD_RE.sub(collapse, text)
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


# Compiled title patterns, cached across calls. The title_map holds ~1800
# entries — far above CPython's internal re cache (512) — so compiling per call
# thrashed that cache and made cleaning the full pick set take minutes.
_TITLE_PATTERN_CACHE: dict[str, re.Pattern] = {}


def _title_pattern(lower_title: str) -> re.Pattern:
    pattern = _TITLE_PATTERN_CACHE.get(lower_title)
    if pattern is None:
        pattern = re.compile(r"\b" + re.escape(lower_title) + r"\b", re.IGNORECASE)
        _TITLE_PATTERN_CACHE[lower_title] = pattern
    return pattern


def fix_film_titles(
    text: str, title_map: dict[str, str], own_title: str | None = None
) -> str:
    """
    Fix known film title capitalization in quotes.
    Uses word boundaries to avoid false substring matches (e.g. "Birth" inside "birthday").

    A title listed in ambiguous_film_titles.json is only substituted when it is
    the guest's own pick (own_title), because otherwise the match is far more
    likely to be the ordinary English word than a reference to the film.
    """
    ambiguous = load_ambiguous_titles()
    own_low = (own_title or "").lower()
    low = text.lower()
    for lower_title, correct_title in title_map.items():
        if len(lower_title) < 5:
            continue  # Skip very short titles to avoid false matches
        if lower_title in ambiguous and lower_title not in own_low:
            continue
        # A \b...\b match requires the literal substring to be present, so skip
        # titles that can't possibly match before paying for the regex.
        if lower_title not in low:
            continue
        # Use word boundaries to avoid matching inside other words
        new_text = _title_pattern(lower_title).sub(correct_title, text)
        if new_text != text:
            text = new_text
            low = text.lower()  # keep in sync; a correction may change letters
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
    # "…" and curly closing quotes end a sentence too. A "..." after one is
    # this function's own earlier mistake, from before it knew that.
    text = re.sub(r"([…”’])\.\.\.$", r"\1", text)
    if text[-1] not in ".!?\"'…”’":
        # Looks like it was cut off mid-sentence
        text = text.rstrip(",;: ") + "..."
    return text


def clean_quote(
    text: str, title_map: dict[str, str], own_title: str | None = None
) -> str:
    """Apply all cleaning steps to a single quote."""
    if not text or not text.strip():
        return ""

    text = remove_fillers(text)
    text = deduplicate_words(text, doubled_title_words(title_map))
    text = fix_film_titles(text, title_map, own_title)
    text = fix_capitalization(text)
    text = normalize_whitespace(text)
    text = add_trailing_ellipsis(text)

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

        cleaned = clean_quote(original, title_map, pick.get("film_title"))
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
