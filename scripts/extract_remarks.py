#!/usr/bin/env python3
"""
Keep what a guest says that is not about any one pick.

Quotes hold a guest's words about each film, so the rest of a visit fell out
of the site: William Friedkin calling the closet "the history of world
cinema", Lee Daniels on finding he is "influenced by so many filmmakers",
Claude Lelouch on hearing all his dialogue in cafes. This reads every
stretch of transcript no quote holds (check_quote_coverage.find_gaps) and asks
Gemini 4 Argon what it is:
- "remark": the guest's own words worth keeping, a reflection, a story, a
  closing thought; saved to the guest's "remarks" in guests.json and shown on
  the guest page under "Also said"
- "pick": about one of the picks; reported, for fill_quote_gaps.py or a
  person to place, never saved here
- "logistics": greetings, thanks, talk about the shelves or the bag; dropped

A remark's text must be the transcript's own words (fill_quote_gaps.locate).

Usage:
  python scripts/extract_remarks.py --guest-slug SLUG            # report, change nothing
  python scripts/extract_remarks.py --guest-slug SLUG --apply    # write guests.json
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.check_quote_coverage import find_gaps, flatten, visit_video
from scripts.fill_quote_gaps import locate, parse_object
from scripts.utils import GUESTS_FILE, PICKS_FILE, TRANSCRIPTS_DIR, load_json, log, save_json

MIN_WORDS = 15  # shorter stretches are asides

PROMPT = """You are sorting what a guest said in a Criterion Closet Picks video, where a
guest walks through the Criterion closet picking films and talking about them.

GUEST: {guest}

PICKS (the films the guest took home):
{picks}

This stretch of the video, from {start}s, is in none of the quotes about the picks.
It comes after {before} and before {after}:
"{stretch}"

What is it?
- "pick": the guest talking about one of the picks: what they think of it, a
  memory of it, a fact about it. Give the pick's number.
- "remark": the guest's own words worth keeping that are not about one pick: a
  reflection on cinema, film, their work or their life, a story, a dedication,
  a closing thought, or an introduction that says something about them.
- "logistics": a bare greeting, thanks, talk about the closet, the shelves, the
  bag or choosing ("let's see what I don't have in here"), or someone else
  speaking.

For a remark, also give its text: the guest's words from the stretch, the same
words in the same order, with punctuation and capitals, fixing caption errors
in names only. Leave out logistics at its start or end.

Return ONLY this JSON object:
{{"kind": "pick|remark|logistics", "pick": 0, "text": "", "reason": "one sentence"}}"""


def remarks_for_visit(guest: dict, visit_index: int, picks: list[dict], model) -> tuple[list[dict], list[dict]]:
    """(remarks to save, every decision) for one visit."""
    video_id = visit_video(guest, visit_index)
    path = TRANSCRIPTS_DIR / f"{video_id}.json" if video_id else None
    if not path or not path.exists():
        return [], []
    data = load_json(path)
    segments = data["segments"] if isinstance(data, dict) else data
    transcript, starts = flatten(segments)
    quoted = [p for p in picks if (p.get("quote") or "").strip()]
    gaps = [g for g in find_gaps(transcript, quoted, MIN_WORDS)
            if g["kind"] == "uncovered" and g["end"] - g["start"] >= MIN_WORDS]
    listing = "\n".join(f"{n}. {p['film_title']}" for n, p in enumerate(picks, 1))
    translated = isinstance(data, dict) and data.get("spoken_language") not in (None, "", "en")
    source = next((v for v in guest.get("visits") or [] if v.get("visit_index") == visit_index), guest)

    def decide(gap: dict) -> dict:
        start, end = starts[gap["start"]], starts[gap["end"] - 1]
        raw = " ".join(s.get("text", "") for s in segments if start <= int(s.get("start", 0)) <= end)
        answer = parse_object(model.generate_content(PROMPT.format(
            guest=guest["name"], picks=listing, start=start, stretch=raw,
            before=gap["before"]["film_title"] if gap["before"] else "the start of the video",
            after=gap["after"]["film_title"] if gap["after"] else "the end of the video",
        )).text)
        out = {"start": start, "kind": answer.get("kind"), "reason": answer.get("reason"),
               "stretch": " ".join(transcript[gap["start"]:gap["end"]])}
        if out["kind"] == "remark":
            found = locate(answer.get("text") or "", transcript, gap["start"], gap["end"])
            if found:
                text, lo, _ = found
                out.update(text=text, start=starts[lo])
            else:
                out["kind"] = "unverified"  # the text was not the transcript's own words
        elif out["kind"] == "pick":
            out["pick"] = answer.get("pick")
        return out

    with ThreadPoolExecutor(8) as ex:
        decisions = list(ex.map(decide, gaps))

    remarks = []
    for d in decisions:
        if d["kind"] != "remark":
            continue
        remark = {"visit_index": visit_index, "start_timestamp": int(d["start"]),
                  "text": f"[Translated] {d['text']}" if translated else d["text"]}
        if source.get("youtube_video_id"):
            remark["youtube_timestamp_url"] = (
                f"https://www.youtube.com/watch?v={source['youtube_video_id']}&t={int(d['start'])}")
        elif source.get("vimeo_video_id"):
            remark["vimeo_timestamp_url"] = f"https://vimeo.com/{source['vimeo_video_id']}#t={int(d['start'])}s"
        remarks.append(remark)
    return remarks, decisions


def main():
    parser = argparse.ArgumentParser(description="Keep what a guest says that is not about any one pick")
    parser.add_argument("--guest-slug", required=True)
    parser.add_argument("--apply", action="store_true", help="Write the remarks to guests.json")
    args = parser.parse_args()

    from scripts.clean_quotes import clean_quote
    from scripts.extract_quotes import get_gemini_model

    guests = load_json(GUESTS_FILE)
    guest = next((g for g in guests if g["slug"] == args.guest_slug), None)
    if not guest:
        log(f"ERROR: no guest with slug {args.guest_slug}")
        sys.exit(1)
    picks = [p for p in load_json(PICKS_FILE) if p["guest_slug"] == guest["slug"]]
    model = get_gemini_model()

    remarks = []
    for visit_index in sorted({p.get("visit_index") or 1 for p in picks}):
        found, decisions = remarks_for_visit(
            guest, visit_index, [p for p in picks if (p.get("visit_index") or 1) == visit_index], model)
        for d in decisions:
            print(f"  [{d['kind']:10}] {d['start']}s {d.get('text') or d['stretch'][:110]}")
        remarks += found
    for r in remarks:
        r["text"] = clean_quote(r["text"], {})
    log(f"{len(remarks)} remark(s) for {guest['name']}")
    if args.apply:
        guest["remarks"] = remarks
        save_json(GUESTS_FILE, guests)
        log(f"Saved {GUESTS_FILE}")


if __name__ == "__main__":
    main()
