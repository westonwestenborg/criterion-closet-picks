#!/usr/bin/env python3
"""
Second pass: put back transcript passages that no quote holds.

The extraction pass sometimes stops a quote early or skips a passage: in
October 2026 Gemini 4 Argon dropped Kim Cattrall's story about her mother from
Heaven Can Wait. Asking a model again to "be complete" mostly repeats its first
answer (3 of 39 dropped passages came back in a test), so this pass splits the
job. Code finds every stretch of transcript no quote holds
(check_quote_coverage.find_gaps); models only decide which pick a stretch is
about, or none.

Two models from different providers answer each stretch on their own: Gemini 4
Argon, and Claude Sonnet 5.5 through OpenRouter. They make different mistakes
(in the October bake-off only Gemini found Barry Lyndon, and only the others
refused to quote Bong Joon Ho's garbled captions), so agreement is the signal:
- both name the same pick: the stretch goes into that pick's quote, at its
  place in the transcript
- both say none: it is chatter or an intro, and stays out
- they name two different picks: Gemini watches that moment of the video
  (YouTube only), and its answer decides if it matches one of theirs
- one says none: the stretch is reported for a person, because the video shows
  what the guest is holding, not whether they are talking about it
- otherwise the stretch is reported for a person to read

The pass only adds words, only words that are in the transcript, and only for
stretches of MIN_WORDS or more; shorter ones stay in the coverage report.

Usage:
  python scripts/fill_quote_gaps.py --guest-slug SLUG           # report, change nothing
  python scripts/fill_quote_gaps.py --guest-slug SLUG --apply   # write picks.json

Needs GEMINI_API_KEY and OPENROUTER_API_KEY.
"""

import argparse
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from difflib import SequenceMatcher

import requests

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.check_quote_coverage import MIN_MATCH, find_gaps, flatten, visit_video, words
from scripts.utils import (
    CATALOG_FILE,
    GUESTS_FILE,
    PICKS_FILE,
    TRANSCRIPTS_DIR,
    apply_quote_overrides,
    get_env,
    load_json,
    log,
    save_json,
)

SECOND_MODEL = "anthropic/claude-sonnet-5.5"
MIN_WORDS = 20  # shorter stretches are mostly asides ("there we go"); the coverage report still lists them
CONTEXT = 30  # words either side of a stretch a model's clean text may borrow
VERBATIM = 0.9  # share of a model's clean-text words that must be the stretch's own, in order

PROMPT = """You are checking quotes taken from a Criterion Closet Picks video, where a guest
walks through the Criterion closet picking up films and talking about them.

GUEST: {guest}

PICKS (the films the guest took home), with the quote each has so far:
{picks}

TRANSCRIPT (timestamps in seconds):
{transcript}

This stretch of the transcript, from {start}s to {end}s, is in none of the quotes:
"{stretch}"

Does this stretch say something about one of the picks: what the guest thinks
of it, a memory of it, or a fact about it, its makers or its cast? Answer with
that pick's number from the list, or 0 if not. Answer 0 for an intro or
greeting, a thank-you, talk about the closet, the shelves, the bag or choosing
("this is a bit overwhelming", "let's see what I don't have in here", "I hope I
don't die before I can watch all of these"), general remarks about cinema or
the guest's career, another person speaking, or a film that is not a pick, even
when the guest is holding a pick at the time. If it is about a pick, also give
the part that is about it as clean text: the same words in the same order, with
punctuation and capitals, fixing caption errors in names and titles only.

Return ONLY this JSON object:
{{"pick": 0, "text": "", "reason": "one sentence"}}"""

VIDEO_PROMPT = """In this clip from a Criterion Closet Picks video, which film is the guest
holding, pointing at, or talking about while saying: "{stretch}"

Answer with that film's number from this list, or 0 if it is none of them:
{titles}

Return ONLY this JSON object: {{"pick": 0, "reason": "one sentence"}}"""


def parse_object(text: str) -> dict:
    """A JSON object from a model reply, tolerating code fences and a wrapping list."""
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", (text or "").strip())
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data, _ = json.JSONDecoder().raw_decode(text[text.find("{"):])
    if isinstance(data, list):
        data = data[0] if data else {}
    return data if isinstance(data, dict) else {}


def ask_gemini(model, prompt: str) -> dict:
    return parse_object(model.generate_content(prompt).text)


def ask_second(prompt: str) -> dict:
    r = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={"Authorization": f"Bearer {get_env('OPENROUTER_API_KEY')}"},
        json={"model": SECOND_MODEL, "messages": [{"role": "user", "content": prompt}], "max_tokens": 4096},
        timeout=300,
    )
    r.raise_for_status()
    return parse_object(r.json()["choices"][0]["message"].get("content") or "")


def ask_video(model, video_id: str, start: int, end: int, stretch: str, titles: list[str]) -> dict:
    from google.genai import types

    clip = types.Part(
        file_data=types.FileData(file_uri=f"https://www.youtube.com/watch?v={video_id}"),
        video_metadata=types.VideoMetadata(start_offset=f"{max(0, start - 5)}s", end_offset=f"{end + 5}s"),
    )
    listing = "\n".join(f"{n}. {t}" for n, t in enumerate(titles, 1))
    return parse_object(model.generate_content([clip, VIDEO_PROMPT.format(stretch=stretch, titles=listing)]).text)


def as_pick(answer: dict, count: int) -> int:
    try:
        n = int(answer.get("pick", 0))
    except (TypeError, ValueError):
        return 0
    return n if 0 <= n <= count else 0


def locate(text: str, transcript: list[str], start: int, end: int) -> tuple[str, int, int] | None:
    """
    The part of the clean text that is transcript[start:end], with its word
    range, or None when the text is not the transcript's own words.

    A stretch often mixes a pick's lines with chatter or another film, and the
    models rightly return only the pick's part (Agnes Varda on Taste of Cherry
    runs on into La promesse). They also pad with a few words the quote already
    has (Sara Dosa's Varda sentence), so the text is matched against the stretch
    plus CONTEXT words either side, then cut back to the stretch.
    """
    offsets = word_offsets(text)
    lo = max(0, start - CONTEXT)
    window = transcript[lo:end + CONTEXT]
    blocks = [
        b for b in SequenceMatcher(None, window, [w for w, _ in offsets], autojunk=False).get_matching_blocks()
        if b.size
    ]
    if not offsets or sum(b.size for b in blocks) < VERBATIM * len(offsets):
        return None
    inside = [(lo + b.a + i, b.b + i) for b in blocks for i in range(b.size) if start <= lo + b.a + i < end]
    if len(inside) < MIN_MATCH:
        return None
    (first_t, first_w), (last_t, last_w) = inside[0], inside[-1]
    tail = re.match(r"[\w'’]+[^\w\s]*", text[offsets[last_w][1]:])
    cut = text[offsets[first_w][1]:offsets[last_w][1] + (tail.end() if tail else 0)]
    return cut, first_t, last_t + 1


def word_offsets(text: str) -> list[tuple[str, int]]:
    """Each normalized word of text (as words() yields them) with its character offset."""
    return [(w, m.start()) for m in re.finditer(r"[\w'’]+", text) for w in words(m.group())]


def splice(quote: str, transcript: list[str], start: int, end: int, text: str) -> tuple[str, bool]:
    """
    Insert text, the clean version of transcript[start:end], into quote at its
    place in transcript order. Returns the new quote and whether the text went
    in front of everything the quote already held.
    """
    offsets = word_offsets(quote)
    blocks = [
        b for b in SequenceMatcher(None, transcript, [w for w, _ in offsets], autojunk=False).get_matching_blocks()
        if b.size >= MIN_MATCH
    ]
    after = next((b for b in blocks if b.a >= end), None)
    if after is None:
        return f"{quote.rstrip()} {text.strip()}", False
    pos = offsets[after.b][1]
    head, tail = quote[:pos].rstrip(), quote[pos:].lstrip()
    return f"{head} {text.strip()} {tail}".strip(), after is blocks[0]


def fill_visit(guest: dict, visit_index: int, picks: list[dict], model, use_video: bool = True) -> list[dict]:
    """
    Decide every gap in one visit and splice the agreed ones into the picks
    (in place). Returns one decision dict per gap.
    """
    from scripts.extract_quotes import format_transcript

    video_id = visit_video(guest, visit_index)
    path = TRANSCRIPTS_DIR / f"{video_id}.json" if video_id else None
    if not path or not path.exists():
        return []
    data = load_json(path)
    segments = data["segments"] if isinstance(data, dict) else data
    transcript, starts = flatten(segments)
    quoted = [p for p in picks if (p.get("quote") or "").strip()]
    gaps = [g for g in find_gaps(transcript, quoted, MIN_WORDS) if g["end"] - g["start"] >= MIN_WORDS]
    if not gaps:
        return []

    listing = "\n".join(
        f"{n}. {p['film_title']}: \"{(p.get('quote') or '(no quote yet)')[:200]}\"" for n, p in enumerate(picks, 1)
    )
    titles = [p["film_title"] for p in picks]
    source = next((v for v in guest.get("visits") or [] if v.get("visit_index") == visit_index), guest)
    youtube_id = source.get("youtube_video_id")  # the video tiebreak reads YouTube only

    def decide(gap: dict) -> dict:
        stretch_words = transcript[gap["start"]:gap["end"]]
        start, end = starts[gap["start"]], starts[gap["end"] - 1]
        raw = " ".join(s.get("text", "") for s in segments if start <= int(s.get("start", 0)) <= end)
        prompt = PROMPT.format(guest=guest["name"], picks=listing, transcript=format_transcript(segments),
                               start=start, end=end, stretch=raw)
        out = {"start": start, "end": end, "stretch": " ".join(stretch_words), "kind": gap["kind"]}
        try:
            a = ask_gemini(model, prompt)
        except Exception as e:  # noqa: BLE001
            a = {"error": f"{type(e).__name__}: {e}"[:200]}
        try:
            b = ask_second(prompt)
        except Exception as e:  # noqa: BLE001
            b = {"error": f"{type(e).__name__}: {e}"[:200]}
        pa, pb = as_pick(a, len(picks)), as_pick(b, len(picks))
        out.update(gemini=pa, second=pb, gemini_reason=a.get("reason") or a.get("error"),
                   second_reason=b.get("reason") or b.get("error"))
        if "error" in a or "error" in b:
            out["verdict"] = "unresolved"
            return out
        if pa == pb:
            chosen = pa
        elif 0 in (pa, pb):
            # One model hears chatter. The video cannot settle that: it shows
            # what the guest holds, not whether they are talking about it
            # (it put Schlondorff's greeting to "the spirits of the Vault" on
            # Eclipse). Leave it for a person.
            chosen = -1
        elif use_video and youtube_id:
            try:
                v = as_pick(ask_video(model, youtube_id, start, end, raw, titles), len(picks))
            except Exception:  # noqa: BLE001
                v = -1
            out["video"] = v
            chosen = v if v in (pa, pb) else -1
        else:
            chosen = -1
        if chosen == -1:
            out["verdict"] = "unresolved"
        elif chosen == 0:
            out["verdict"] = "none"
        else:
            # The clean text of a model that named this pick, cut to the stretch's own words.
            texts = [x.get("text", "") for x, p in ((a, pa), (b, pb)) if p == chosen]
            found = next((f for f in (locate(t, transcript, gap["start"], gap["end"]) for t in texts) if f), None)
            if found:
                text, lo, hi = found
                out.update(pick=chosen, text=text, verdict="add", span=(lo, hi), start=starts[lo])
            else:
                out.update(pick=chosen, verdict="unresolved", texts=texts)
        return out

    with ThreadPoolExecutor(8) as ex:
        decisions = list(ex.map(decide, gaps))

    # Splice in transcript order so offsets stay true; later gaps sit after earlier ones.
    for gap, d in sorted(zip(gaps, decisions), key=lambda x: x[0]["start"]):
        if d["verdict"] != "add":
            continue
        pick = picks[d["pick"] - 1]
        quote, in_front = splice(pick.get("quote") or "", transcript, d["span"][0], d["span"][1], d["text"])
        pick["quote"] = quote
        d["film_id"] = pick["film_id"]
        if in_front or pick.get("start_timestamp") is None:
            pick["start_timestamp"] = d["start"]
            if pick.get("youtube_timestamp_url"):
                pick["youtube_timestamp_url"] = re.sub(r"t=\d+", f"t={d['start']}", pick["youtube_timestamp_url"])
        if pick.get("extraction_confidence") in (None, "", "none"):
            pick["extraction_confidence"] = "medium"
    return decisions


def main():
    parser = argparse.ArgumentParser(description="Put back transcript passages no quote holds")
    parser.add_argument("--guest-slug", required=True, help="Guest to fill")
    parser.add_argument("--apply", action="store_true", help="Write the additions to picks.json")
    parser.add_argument("--no-video", action="store_true", help="Skip the video tiebreak")
    args = parser.parse_args()

    from scripts.clean_quotes import build_title_map, clean_quote
    from scripts.extract_quotes import get_gemini_model

    guest = next((g for g in load_json(GUESTS_FILE) if g["slug"] == args.guest_slug), None)
    if not guest:
        log(f"ERROR: no guest with slug {args.guest_slug}")
        sys.exit(1)
    all_picks = load_json(PICKS_FILE)
    model = get_gemini_model()
    title_map = build_title_map(load_json(CATALOG_FILE))

    visits = sorted({p.get("visit_index") or 1 for p in all_picks if p["guest_slug"] == guest["slug"]})
    added = 0
    for visit_index in visits:
        picks = [p for p in all_picks if p["guest_slug"] == guest["slug"] and (p.get("visit_index") or 1) == visit_index]
        before = {id(p): p.get("quote") for p in picks}
        for d in fill_visit(guest, visit_index, picks, model, use_video=not args.no_video):
            who = picks[d["pick"] - 1]["film_title"] if d.get("pick") else "-"
            print(f"  [{d['verdict']:10}] {d['start']}-{d['end']}s gemini={d['gemini']} second={d['second']}"
                  f"{' video=' + str(d['video']) if 'video' in d else ''} -> {who}: {d['stretch'][:120]}")
        for p in picks:
            if p.get("quote") != before[id(p)]:
                p["quote"] = clean_quote(p["quote"], title_map, p.get("film_title"))
                added += 1
    log(f"{added} quote(s) gained words")
    if args.apply and added:
        apply_quote_overrides(all_picks)
        save_json(PICKS_FILE, all_picks)
        log(f"Saved {PICKS_FILE}")
    elif added:
        log("Dry run: nothing written (use --apply)")


if __name__ == "__main__":
    main()
