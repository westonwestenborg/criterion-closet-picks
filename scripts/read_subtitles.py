#!/usr/bin/env python3
"""
Read Criterion's burned-in English subtitles off a guest's YouTube video.

When a guest speaks another language, YouTube usually has only an auto-caption
track in that language, and Criterion puts its own English translation into
the picture itself. Before October 2026, fetch_transcript saved the
original-language captions and the quotes went out untranslated; for Bong Joon
Ho, whose Korean and English were auto-captioned as Italian, they were
nonsense. Gemini 4 Argon reads the on-screen subtitles from the YouTube URL,
with times, which gives Criterion's human translation.

The result is saved as the episode's transcript, data/transcripts/{id}.json,
with "source": "burned_in_subtitles" and the spoken language, so
extract_quotes.py takes its normal text path and prefixes the quotes
"[Translated] ". An original-language caption file already there is kept as
{id}.{language}.json, not deleted.

Usage:
  python scripts/read_subtitles.py --guest-slug SLUG
  python scripts/read_subtitles.py --guest-slug SLUG --dry-run   # print, save nothing
"""

import argparse
import json
import re
import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import GUESTS_FILE, TRANSCRIPTS_DIR, get_env, load_json, log, save_json

MODEL = "gemini-4-argon"  # the extraction model, see get_gemini_model in extract_quotes.py

PROMPT = """This is a Criterion Closet Picks video: a guest walks through the Criterion
closet and talks about films. The guest may speak a language other than English,
and Criterion has burned English subtitles into the picture.

Transcribe every English subtitle that appears on screen, exactly as written, in
order, each with the time it first appears as minutes:seconds from the start of
the video ("0:05", "4:01"). Some guests also speak English, which is not
subtitled: transcribe what they say in English too, word for word, in the same
list and order. Do not translate anything yourself, and do not add words that
are neither in the subtitles nor spoken in English. If the video has no
burned-in English subtitles, return an empty list.

Also give the main language the guest speaks, as a two-letter code.

Return ONLY this JSON object:
{"spoken_language": "fr", "subtitles": [{"time": "0:05", "text": "..."}]}"""

ENGLISH = set("the and i to a of it is that you this in was my so like but for what with he me just have be on".split())


def english_share(text: str) -> float:
    """Share of common English words; under 0.2 means the text is not English."""
    found = re.findall(r"[a-z']+", text.lower())
    return sum(w in ENGLISH for w in found) / len(found) if found else 0.0


# Common words of the other languages guests have spoken in the closet. An
# untranslated French, Italian or Spanish quote is full of them; an English one
# naming "Le silence de la mer" is not. In October 2026 this caught 59 of the 60
# untranslated quotes then live and flagged no English quote, where English
# word share alone could not tell the two apart.
FOREIGN = set(
    "le la les et est je j'ai c'est il elle pour que qui un une des du au aux sur dans pas très mon"
    " notre nous vous avec aussi voilà di che non è per con del della sono ma anche questo io los"
    " las el y es una por muy pero como lo para mi su".split()
)


def foreign_share(text: str) -> float:
    found = re.findall(r"[^\W\d_]+(?:'[^\W\d_]+)?", text.lower())
    return sum(w in FOREIGN for w in found) / len(found) if found else 0.0


def non_latin_share(text: str) -> float:
    """Share of letters outside the Latin script (Persian, Korean, Japanese...)."""
    import unicodedata

    letters = [c for c in text if c.isalpha()]
    return sum("LATIN" not in unicodedata.name(c, "") for c in letters) / len(letters) if letters else 0.0


def looks_untranslated(text: str) -> bool:
    """True for a quote that is still in the language the guest spoke."""
    return non_latin_share(text) >= 0.3 or foreign_share(text) >= 0.15


def seconds(value) -> float:
    """A subtitle time as seconds: "4:01" or "1:02:03" or a plain number."""
    if isinstance(value, str) and ":" in value:
        total = 0.0
        for part in value.split(":"):
            total = total * 60 + float(part or 0)
        return total
    return float(value or 0)


def fix_run_together(starts: list[float]) -> list[float]:
    """
    Undo times written as minutes and seconds run together, 401 for 4:01.

    Asked for plain seconds, Argon did this on Claude Lelouch and Lucrecia
    Martel: their 259- and 197-second videos came back ending at 401 and 310.
    In real seconds, a few minutes of subtitles always has some time whose
    last two digits are 60-99; if none does, the times are run together.
    """
    if not starts or max(starts) < 100 or any(int(t) % 100 >= 60 for t in starts):
        return starts
    return [(int(t) // 100) * 60 + (t - int(t) // 100 * 100) for t in starts]


def to_segments(subs: list[dict]) -> list[dict]:
    """Subtitles ({time or start, text}) as transcript segments ({text, start, duration})."""
    subs = [s for s in subs if (s.get("text") or "").strip()]
    starts = fix_run_together([seconds(s.get("time", s.get("start"))) for s in subs])
    order = sorted(range(len(subs)), key=lambda i: starts[i])
    segments = []
    for n, i in enumerate(order):
        start = starts[i]
        end = starts[order[n + 1]] if n + 1 < len(order) else start + 3
        segments.append({"text": subs[i]["text"].strip(), "start": round(start, 2),
                         "duration": round(max(end - start, 0.5), 2)})
    return segments


def read_subtitles(video_id: str) -> tuple[list[dict], str]:
    """Segments ({text, start, duration}) and the spoken-language code; ([], "") when none."""
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=get_env("GEMINI_API_KEY"))
    video = types.Part(file_data=types.FileData(file_uri=f"https://www.youtube.com/watch?v={video_id}"))
    config = types.GenerateContentConfig(
        temperature=0.1,
        response_mime_type="application/json",
        max_output_tokens=32768,
        # Subtitles are large text; low resolution reads them and costs about
        # a third of the default (roughly 100 tokens a second).
        media_resolution="MEDIA_RESOLUTION_LOW",
    )
    for attempt in range(3):
        try:
            reply = client.models.generate_content(model=MODEL, contents=[video, PROMPT], config=config)
            break
        except Exception as e:  # noqa: BLE001 -- dropped connections were common in October 2026
            if attempt == 2:
                raise
            log(f"  {type(e).__name__}, retrying")
            time.sleep(10 * (attempt + 1))
    data = json.loads(reply.text)
    if isinstance(data, list):
        data = data[0] if data else {}
    return to_segments(data.get("subtitles") or []), (data.get("spoken_language") or "").lower()[:2]


def transcript_is_english(path) -> bool:
    data = load_json(path)
    segments = data["segments"] if isinstance(data, dict) else data
    return english_share(" ".join(s.get("text", "") for s in segments)) >= 0.2


def save_subtitle_transcript(guest: dict, video_id: str) -> bool:
    """
    Read the video's subtitles into data/transcripts/{video_id}.json. False,
    and nothing written, when it has no English subtitles. A transcript that
    is already there and is not subtitles is kept as {video_id}.{language}.json.
    """
    segments, language = read_subtitles(video_id)
    text = " ".join(s["text"] for s in segments)
    log(f"  {guest['name']}: {len(segments)} subtitles, spoken language {language or '?'},"
        f" English share {english_share(text):.2f}, last at {segments[-1]['start'] if segments else 0}s")
    if not segments or english_share(text) < 0.2:
        log("  No English subtitles found")
        return False
    path = TRANSCRIPTS_DIR / f"{video_id}.json"
    if path.exists():
        old = load_json(path)
        if not (isinstance(old, dict) and old.get("source") == "burned_in_subtitles"):
            keep = TRANSCRIPTS_DIR / f"{video_id}.{language or 'orig'}.json"
            path.rename(keep)
            log(f"  Kept the original captions as {keep.name}")
    save_json(path, {"video_id": video_id, "guest_name": guest["name"], "source": "burned_in_subtitles",
                     "spoken_language": language, "segments": segments})
    log(f"  Saved {path.name}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Read burned-in English subtitles into a transcript")
    parser.add_argument("--guest-slug", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    guest = next((g for g in load_json(GUESTS_FILE) if g["slug"] == args.guest_slug), None)
    if not guest or not guest.get("youtube_video_id"):
        log(f"ERROR: {args.guest_slug} has no YouTube video")
        sys.exit(1)
    video_id = guest["youtube_video_id"]

    if args.dry_run:
        segments, language = read_subtitles(video_id)
        log(f"{guest['name']}: {len(segments)} subtitles, spoken language {language or '?'}")
        for s in segments[:8]:
            print(f"  {s['start']:7.1f}  {s['text']}")
        return
    if not save_subtitle_transcript(guest, video_id):
        sys.exit(2)


if __name__ == "__main__":
    main()
