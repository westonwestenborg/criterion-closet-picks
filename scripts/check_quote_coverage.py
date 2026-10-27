#!/usr/bin/env python3
"""
Report transcript words that no quote covers, so a dropped line gets read.

Extraction sometimes stops a quote early or skips a passage: in October 2026
Gemini 4 Argon left out Kim Cattrall's story about her mother on Heaven Can
Wait, and nothing flagged it. This lines every quote up against its visit's
transcript and prints two things for a person to read:

- uncovered stretches: runs of transcript words that are in no quote at all,
  including before the first quote and after the last
- inside gaps: transcript words a quote skips between two of its own parts

Most stretches are chatter that belongs to no pick ("I feel like I'm in this
corner too much"). The ones that are about a film need a fix: re-extract the
guest, or restore the passage in data/quote_overrides.json (fix-guest
Workflow 10). A run of two quotes about different films sharing words is
Workflow 10's job, not this script's.

Usage:
  python scripts/check_quote_coverage.py --guest-slug SLUG
  python scripts/check_quote_coverage.py                  # every guest
  python scripts/check_quote_coverage.py --min-words 25   # longer stretches only
"""

import argparse
import re
import sys
import unicodedata
from difflib import SequenceMatcher

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import GUESTS_FILE, PICKS_FILE, TRANSCRIPTS_DIR, load_json, log

WORD_RE = re.compile(r"[a-z0-9']+")
FILLERS = {"uh", "um", "hmm", "ah", "ahh"}
MIN_MATCH = 3  # matched runs shorter than this are coincidence ("and the")
MIN_GAP = 5  # skipped runs shorter than this are caption fixes and stutters
MAX_JUMP = 150  # a jump this far starts a separate mention (or a stray match)
MIN_CLUSTER = 8  # a separate mention needs this many matched words to count


def words(text: str) -> list[str]:
    text = unicodedata.normalize("NFKD", (text or "").lower().replace("’", "'"))
    text = "".join(c for c in text if not unicodedata.combining(c))
    return [w for w in WORD_RE.findall(text) if w not in FILLERS]


def quote_span(transcript: list[str], quote: list[str]) -> tuple[set[int], list[tuple[int, int]]]:
    """
    Transcript word positions a quote covers, plus the gaps it skips inside.

    Takes the largest cluster of matched runs, so a phrase that also turns up
    elsewhere in the video ("I love this movie") does not stretch the span.
    """
    # autojunk stays off: above 200 items difflib treats common words as junk.
    blocks = [
        b for b in SequenceMatcher(None, transcript, quote, autojunk=False).get_matching_blocks()
        if b.size >= MIN_MATCH
    ]
    if not blocks:
        return set(), []
    clusters, current = [], [blocks[0]]
    for b in blocks[1:]:
        if b.a - (current[-1].a + current[-1].size) > MAX_JUMP:
            clusters.append(current)
            current = [b]
        else:
            current.append(b)
    clusters.append(current)
    # A quote can join two mentions minutes apart (Amy Irving names Crossing
    # Delancey at 0:10 and again at 4:00), so every cluster with enough matched
    # words counts. A stray shared phrase stays below MIN_CLUSTER.
    best = max(clusters, key=lambda c: sum(b.size for b in c))
    kept = [c for c in clusters if c is best or sum(b.size for b in c) >= MIN_CLUSTER]
    covered = {i for c in kept for b in c for i in range(b.a, b.a + b.size)}
    gaps = [
        (x.a + x.size, y.a)
        for c in kept
        for x, y in zip(c, c[1:])
        if y.a - (x.a + x.size) >= MIN_GAP
    ]
    return covered, gaps


def dropped_run(transcript: list[str], prior_quote: str, new_quotes: list[str]) -> str:
    """
    The longest run of transcript words an existing quote holds that no new
    quote holds, or "" when there is none of at least MIN_GAP words.

    extract_quotes.py uses this to refuse a re-extraction that would lose
    words. Words that moved to another pick are held by its new quote, and
    words not in this transcript (a quote from the wrong video) align to
    nothing, so neither counts as lost.
    """
    held, _ = quote_span(transcript, words(prior_quote))
    if not held:
        return ""
    covered = set()
    for quote in new_quotes:
        covered |= quote_span(transcript, words(quote))[0]
    lost = sorted(held - covered)
    best, run = [], []
    for i in lost:
        run = run + [i] if run and i == run[-1] + 1 else [i]
        if len(run) > len(best):
            best = run
    return " ".join(transcript[i] for i in best) if len(best) >= MIN_GAP else ""


def held_by_transcript(transcript: list[str], quote: str) -> bool:
    """
    True when nearly all of a quote's words occur, in order, in this transcript.

    Works for a two-word quote ("Tony Richardson.") where quote_span's
    three-word runs cannot, which is why the never-blank-a-pick rule in
    extract_quotes.py uses it.
    """
    q = words(quote)
    if not q:
        return False
    matched = sum(b.size for b in SequenceMatcher(None, transcript, q, autojunk=False).get_matching_blocks())
    return matched >= 0.8 * len(q)


def visit_video(guest: dict, visit_index: int) -> str | None:
    for visit in guest.get("visits") or []:
        if visit.get("visit_index") == visit_index:
            return visit.get("youtube_video_id") or visit.get("vimeo_video_id")
    if visit_index == 1:
        return guest.get("youtube_video_id") or guest.get("vimeo_video_id")
    return None


def flatten(segments: list[dict]) -> tuple[list[str], list[int]]:
    """Transcript words, and the start second of the segment each one is in."""
    transcript, starts = [], []
    for seg in segments:
        for w in words(seg.get("text", "")):
            transcript.append(w)
            starts.append(int(seg.get("start", 0)))
    return transcript, starts


def find_gaps(transcript: list[str], picks: list[dict], min_words: int) -> list[dict]:
    """
    Stretches of transcript words that no quote holds, as dicts with "kind"
    ("inside" a quote's span, or "uncovered"), "start" and "end" word indices,
    and the neighbouring picks. fill_quote_gaps.py works from these.
    """
    owner: dict[int, dict] = {}
    inner_gaps = []
    for pick in picks:
        covered, gaps = quote_span(transcript, words(pick.get("quote", "")))
        for i in covered:
            owner.setdefault(i, pick)
        inner_gaps += [(pick, start, end) for start, end in gaps]

    found = []
    # Words a quote skips count only if no other quote holds them: del Toro
    # talks about Notorious in the middle of Black Narcissus, and those words
    # are Notorious's, not missing.
    for pick, start, end in inner_gaps:
        if sum(1 for i in range(start, end) if i not in owner) >= MIN_GAP:
            found.append({"kind": "inside", "start": start, "end": end, "pick": pick})
    if not owner:
        return found

    # The whole video, not just between the first and last quote: Amy Irving's
    # lost line about bringing pickles for Crossing Delancey came right before
    # her first quote. The intro this also catches is one line to read.
    i = 0
    while i < len(transcript):
        if i in owner:
            i += 1
            continue
        j = i
        while j < len(transcript) and j not in owner:
            j += 1
        if j - i >= min_words:
            found.append({"kind": "uncovered", "start": i, "end": j,
                          "before": owner.get(i - 1), "after": owner.get(j)})
        i = j
    return found


def check_visit(segments: list[dict], picks: list[dict], min_words: int) -> list[str]:
    """Lines to print for one visit; empty when every stretch is covered."""
    transcript, starts = flatten(segments)
    lines = []
    for gap in find_gaps(transcript, picks, min_words):
        text = " ".join(transcript[gap["start"]:gap["end"]])
        at = starts[gap["start"]]
        if gap["kind"] == "inside":
            lines.append(f"    inside {gap['pick']['film_title']} at {at}s: {text[:200]}")
        else:
            before = gap["before"]["film_title"] if gap["before"] else "the start"
            after = gap["after"]["film_title"] if gap["after"] else "the end"
            lines.append(
                f"    uncovered {at}-{starts[gap['end'] - 1]}s, {gap['end'] - gap['start']} words,"
                f" between {before} and {after}: {text[:300]}"
            )
    return lines


def coverage_report(guest: dict, picks: list[dict], min_words: int = 15) -> list[str]:
    """Report lines for one guest, across every visit with a transcript on disk."""
    by_visit: dict[int, list[dict]] = {}
    for p in picks:
        if p["guest_slug"] != guest["slug"]:
            continue
        # Translated quotes count too: a guest whose transcript is Criterion's
        # subtitles (read_subtitles.py) has quotes that line up with it.
        if (p.get("quote") or "").strip():
            by_visit.setdefault(p.get("visit_index") or 1, []).append(p)
    out = []
    for visit_index, visit_picks in sorted(by_visit.items()):
        video_id = visit_video(guest, visit_index)
        path = TRANSCRIPTS_DIR / f"{video_id}.json" if video_id else None
        if not path or not path.exists():
            continue
        data = load_json(path)
        segments = data["segments"] if isinstance(data, dict) else data
        lines = check_visit(segments, visit_picks, min_words)
        if lines:
            out.append(f"{guest['slug']} (visit {visit_index}):")
            out.extend(lines)
    return out


def main():
    parser = argparse.ArgumentParser(description="Report transcript words no quote covers")
    parser.add_argument("--guest-slug", help="Check only this guest")
    parser.add_argument("--min-words", type=int, default=15,
                        help="Shortest uncovered stretch to report (default 15)")
    args = parser.parse_args()

    guests = load_json(GUESTS_FILE)
    picks = load_json(PICKS_FILE)
    if args.guest_slug:
        guests = [g for g in guests if g["slug"] == args.guest_slug]
        if not guests:
            log(f"ERROR: no guest with slug {args.guest_slug}")
            sys.exit(1)

    reported = 0
    for guest in guests:
        lines = coverage_report(guest, picks, args.min_words)
        if lines:
            reported += 1
            print("\n".join(lines))
    log(f"{reported} guest(s) with words no quote covers")


if __name__ == "__main__":
    main()
