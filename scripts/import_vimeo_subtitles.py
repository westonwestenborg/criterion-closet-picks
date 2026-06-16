#!/usr/bin/env python3
"""Convert downloaded Vimeo VTT subtitles into local transcript JSON."""

from __future__ import annotations

import argparse
import html
import re
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.utils import GUESTS_FILE, TRANSCRIPTS_DIR, load_json, save_json


TIMING_RE = re.compile(
    r"(?P<start>\d{2}:\d{2}:\d{2}\.\d{3})\s+-->\s+"
    r"(?P<end>\d{2}:\d{2}:\d{2}\.\d{3})(?:\s+.*)?$"
)
TAG_RE = re.compile(r"<[^>]+>")


def parse_timestamp(value: str) -> float:
    hours, minutes, seconds = value.split(":")
    return int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def clean_cue_text(lines: list[str]) -> str:
    text = " ".join(line.strip() for line in lines if line.strip())
    text = TAG_RE.sub("", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def parse_vtt(text: str) -> list[dict[str, Any]]:
    """Parse the common WEBVTT cue shape emitted by yt-dlp."""
    segments: list[dict[str, Any]] = []
    cue_lines: list[str] = []
    start: float | None = None
    end: float | None = None

    def flush() -> None:
        nonlocal cue_lines, start, end
        if start is not None and end is not None:
            cue_text = clean_cue_text(cue_lines)
            if cue_text:
                segments.append(
                    {
                        "text": cue_text,
                        "start": round(start, 3),
                        "duration": round(max(end - start, 0.0), 3),
                    }
                )
        cue_lines = []
        start = None
        end = None

    for raw_line in text.splitlines():
        line = raw_line.strip("\ufeff").strip()
        if not line:
            flush()
            continue
        if line == "WEBVTT" or line.startswith(("NOTE", "STYLE", "REGION")):
            continue

        match = TIMING_RE.match(line)
        if match:
            start = parse_timestamp(match.group("start"))
            end = parse_timestamp(match.group("end"))
            cue_lines = []
            continue

        if start is None:
            # Cue identifiers appear before timing lines; ignore them.
            continue
        cue_lines.append(line)

    flush()
    return segments


def find_guest(guests: list[dict[str, Any]], guest_slug: str) -> dict[str, Any]:
    for guest in guests:
        if guest.get("slug") == guest_slug:
            return guest
    raise ValueError(f"Guest not found: {guest_slug}")


def build_transcript(
    *,
    guest: dict[str, Any],
    vtt_text: str,
) -> dict[str, Any]:
    vimeo_id = guest.get("vimeo_video_id")
    if not vimeo_id:
        raise ValueError(f"Guest has no vimeo_video_id: {guest.get('slug')}")

    segments = parse_vtt(vtt_text)
    if not segments:
        raise ValueError("No subtitle segments parsed from VTT")

    return {
        "video_id": vimeo_id,
        "guest_name": guest.get("name", ""),
        "source": "vimeo_auto_subtitles",
        "segments": segments,
    }


def import_vimeo_subtitle(
    *,
    guest_slug: str,
    vtt_path: Path,
    output_dir: Path = TRANSCRIPTS_DIR,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    guests = load_json(GUESTS_FILE)
    guest = find_guest(guests, guest_slug)
    transcript = build_transcript(guest=guest, vtt_text=vtt_path.read_text(encoding="utf-8"))

    output_path = output_dir / f"{transcript['video_id']}.json"
    if output_path.exists() and not force:
        raise FileExistsError(f"Transcript already exists: {output_path}")

    if not dry_run:
        save_json(output_path, transcript)

    return {
        "guest_slug": guest_slug,
        "guest_name": transcript["guest_name"],
        "video_id": transcript["video_id"],
        "segments": len(transcript["segments"]),
        "output_path": str(output_path),
        "dry_run": dry_run,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Import downloaded Vimeo subtitles")
    parser.add_argument("--guest-slug", required=True)
    parser.add_argument("--vtt-path", required=True, type=Path)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    result = import_vimeo_subtitle(
        guest_slug=args.guest_slug,
        vtt_path=args.vtt_path,
        force=args.force,
        dry_run=args.dry_run,
    )
    status = "Would write" if args.dry_run else "Wrote"
    print(
        f"{status} {result['segments']} segments for {result['guest_name']} "
        f"to {result['output_path']}"
    )


if __name__ == "__main__":
    main()
