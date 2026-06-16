#!/usr/bin/env python3
"""Backfill deterministic guest media fields from local verification."""

from __future__ import annotations

import argparse
import copy
import sys
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.utils import GUESTS_FILE, VALIDATION_DIR, load_json, save_json


SCHEMA_VERSION = 1
DEFAULT_REPORT_JSON = VALIDATION_DIR / "guest_media_backfill.json"
DEFAULT_REPORT_MARKDOWN = VALIDATION_DIR / "guest_media_backfill.md"

VERIFIED_VIMEO_EPISODE_DATES = {
    "jack-garfein": {
        "episode_date": "2022-11-11",
        "vimeo_video_id": "769988118",
        "source": "yt-dlp Vimeo metadata",
    },
    "lee-daniels": {
        "episode_date": "2024-09-10",
        "vimeo_video_id": "1008075360",
        "source": "yt-dlp Vimeo metadata",
    },
    "neal-brennan": {
        "episode_date": "2024-06-17",
        "vimeo_video_id": "960366172",
        "source": "yt-dlp Vimeo metadata",
    },
    "park-chan-wook": {
        "episode_date": "2023-08-10",
        "vimeo_video_id": "853427660",
        "source": "yt-dlp Vimeo metadata",
    },
    "the-quay-brothers": {
        "episode_date": "2022-11-11",
        "vimeo_video_id": "769988668",
        "source": "yt-dlp Vimeo metadata",
    },
    "wim-wenders": {
        "episode_date": "2024-03-12",
        "vimeo_video_id": "922549736",
        "source": "yt-dlp Vimeo metadata",
    },
}

VERIFIED_PROFESSIONS = {
    "alfonso-cuaron-and-pawel-pawlikowski": "director",
    "andre-gregory-and-wallace-shawn": "actor",
    "aubrey-plaza-and-jeff-baena": "actor",
    "ben-gibbards": "musician",
    "charlotte-rampling-and-andrew-haigh": "actor",
    "five-comics": "other",
    "jean-pierre-and-luc-dardenne": "director",
    "stellan-skarsgard": "actor",
    "the-quay-brothers": "director",
    "the-wolfpack": "other",
}

VERIFIED_PHOTO_URLS = {
    "franklin-leonard": {
        "photo_url": "https://upload.wikimedia.org/wikipedia/commons/thumb/8/8b/Franklin_Leonard_for_Mercer.jpg/500px-Franklin_Leonard_for_Mercer.jpg",
        "source_url": "https://en.wikipedia.org/wiki/Franklin_Leonard",
        "source": "Wikipedia page image",
    },
    "karina-longworth": {
        "photo_url": "https://images.squarespace-cdn.com/content/v1/532b4541e4b078ca803911d1/1535045643029-ACS7JG21CL52RZML0FQM/090916LONGWORTH278+%281%29.jpg",
        "source_url": "https://www.vidiocy.com/",
        "source": "Official Vidiocy site",
    },
}


def add_action(
    actions: list[dict[str, Any]],
    changes: Counter[str],
    action_type: str,
    slug: str,
    guest: dict[str, Any],
    evidence: dict[str, Any],
) -> None:
    changes[f"data/guests.json:{action_type}"] += 1
    actions.append(
        {
            "type": action_type,
            "file": "data/guests.json",
            "record_key": slug,
            "title": str(guest.get("name") or slug),
            "evidence": evidence,
        }
    )


def add_review(
    review: list[dict[str, Any]],
    review_type: str,
    slug: str,
    guest: dict[str, Any] | None,
    evidence: dict[str, Any],
    suggested_action: str,
) -> None:
    review.append(
        {
            "type": review_type,
            "file": "data/guests.json",
            "record_key": slug,
            "title": str((guest or {}).get("name") or slug),
            "evidence": evidence,
            "suggested_action": suggested_action,
        }
    )


def backfill_guest_media(
    guests: list[dict[str, Any]],
    vimeo_dates: dict[str, dict[str, str]] | None = None,
    professions: dict[str, str] | None = None,
    photo_urls: dict[str, dict[str, str]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    vimeo_dates = VERIFIED_VIMEO_EPISODE_DATES if vimeo_dates is None else vimeo_dates
    professions = VERIFIED_PROFESSIONS if professions is None else professions
    photo_urls = VERIFIED_PHOTO_URLS if photo_urls is None else photo_urls
    repaired = copy.deepcopy(guests)
    by_slug = {str(guest.get("slug") or ""): guest for guest in repaired}
    changes: Counter[str] = Counter()
    actions: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []

    for slug in sorted(by_slug):
        guest = by_slug[slug]
        if guest.get("episode_date"):
            continue
        visits = guest.get("visits") or []
        dated_visits = [visit for visit in visits if visit.get("episode_date")]
        if len(visits) == 1 and len(dated_visits) == 1:
            episode_date = dated_visits[0]["episode_date"]
            guest["episode_date"] = episode_date
            add_action(
                actions,
                changes,
                "episode_date_copied_from_single_visit",
                slug,
                guest,
                {
                    "episode_date": episode_date,
                    "visit_index": dated_visits[0].get("visit_index"),
                },
            )

    for slug, data in sorted(vimeo_dates.items()):
        guest = by_slug.get(slug)
        if not guest:
            add_review(
                review,
                "verified_vimeo_date_guest_missing",
                slug,
                None,
                data,
                "Create or rename the guest before applying this verified Vimeo date.",
            )
            continue

        expected_vimeo_id = data.get("vimeo_video_id")
        existing_vimeo_id = guest.get("vimeo_video_id")
        if expected_vimeo_id and existing_vimeo_id and existing_vimeo_id != expected_vimeo_id:
            add_review(
                review,
                "verified_vimeo_date_video_conflict",
                slug,
                guest,
                {
                    "existing_vimeo_video_id": existing_vimeo_id,
                    "verified_vimeo_video_id": expected_vimeo_id,
                    "episode_date": data["episode_date"],
                },
                "Review the conflicting Vimeo IDs before applying the date.",
            )
            continue

        existing_date = guest.get("episode_date")
        if existing_date == data["episode_date"]:
            continue
        if existing_date:
            add_review(
                review,
                "verified_vimeo_date_conflict",
                slug,
                guest,
                {
                    "existing_episode_date": existing_date,
                    "verified_episode_date": data["episode_date"],
                    "vimeo_video_id": expected_vimeo_id,
                },
                "Review the conflicting episode dates before overwriting canonical data.",
            )
            continue

        guest["episode_date"] = data["episode_date"]
        add_action(
            actions,
            changes,
            "episode_date_backfilled_from_vimeo_metadata",
            slug,
            guest,
            data,
        )

    for slug, profession in sorted(professions.items()):
        guest = by_slug.get(slug)
        if not guest:
            add_review(
                review,
                "verified_profession_guest_missing",
                slug,
                None,
                {"profession": profession},
                "Create or rename the guest before applying this profession.",
            )
            continue
        if guest.get("profession") == profession:
            continue
        if guest.get("profession"):
            add_review(
                review,
                "verified_profession_conflict",
                slug,
                guest,
                {
                    "existing_profession": guest.get("profession"),
                    "verified_profession": profession,
                },
                "Review the conflicting profession before overwriting canonical data.",
            )
            continue

        guest["profession"] = profession
        add_action(
            actions,
            changes,
            "profession_backfilled_from_verified_map",
            slug,
            guest,
            {"profession": profession},
        )

    for slug, data in sorted(photo_urls.items()):
        guest = by_slug.get(slug)
        if not guest:
            add_review(
                review,
                "verified_photo_guest_missing",
                slug,
                None,
                data,
                "Create or rename the guest before applying this photo URL.",
            )
            continue

        photo_url = data.get("photo_url")
        if not photo_url:
            add_review(
                review,
                "verified_photo_url_missing",
                slug,
                guest,
                data,
                "Add a verified photo_url before applying this photo backfill.",
            )
            continue
        if guest.get("photo_url") == photo_url:
            continue
        if guest.get("photo_url"):
            add_review(
                review,
                "verified_photo_conflict",
                slug,
                guest,
                {
                    "existing_photo_url": guest.get("photo_url"),
                    "verified_photo_url": photo_url,
                    "source_url": data.get("source_url"),
                },
                "Review the conflicting photo URLs before overwriting canonical data.",
            )
            continue

        guest["photo_url"] = photo_url
        add_action(
            actions,
            changes,
            "photo_url_backfilled_from_verified_map",
            slug,
            guest,
            data,
        )

    actions = sorted(actions, key=lambda item: (item["type"], item["record_key"]))
    review = sorted(review, key=lambda item: (item["type"], item["record_key"]))
    report = {
        "schema_version": SCHEMA_VERSION,
        "summary": {
            "changes": dict(sorted(changes.items())),
            "total_changes": sum(changes.values()),
            "actions": len(actions),
            "review_items": len(review),
            "review_by_type": dict(sorted(Counter(item["type"] for item in review).items())),
        },
        "actions": actions,
        "review_items": review,
    }
    return repaired, report


def render_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# Guest Media Backfill",
        "",
        "Deterministic local guest media repairs. Canonical JSON is updated only for unambiguous missing values.",
        "",
        "## Summary",
        "",
        f"- Total changes: {summary['total_changes']}",
        f"- Actions: {summary['actions']}",
        f"- Review items: {summary['review_items']}",
        "",
        "## Changes by Type",
        "",
    ]
    if summary["changes"]:
        for key, count in summary["changes"].items():
            lines.append(f"- `{key}`: {count}")
    else:
        lines.append("- None")

    if report["actions"]:
        lines.extend(["", "## Actions", "", "| Type | Guest | Evidence |", "|---|---|---|"])
        for item in report["actions"]:
            evidence = ", ".join(f"{key}={value}" for key, value in item["evidence"].items())
            lines.append(
                "| {type} | `{guest}` | {evidence} |".format(
                    type=item["type"],
                    guest=str(item["record_key"]).replace("|", "\\|"),
                    evidence=evidence.replace("|", "\\|"),
                )
            )

    if report["review_items"]:
        lines.extend(["", "## Review Items", ""])
        for key, count in summary["review_by_type"].items():
            lines.append(f"- `{key}`: {count}")

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill deterministic guest media fields")
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing data")
    parser.add_argument("--report-json", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--report-markdown", type=Path, default=DEFAULT_REPORT_MARKDOWN)
    args = parser.parse_args()

    guests = load_json(GUESTS_FILE)
    repaired_guests, report = backfill_guest_media(guests)
    markdown = render_markdown(report)

    if not args.dry_run:
        save_json(GUESTS_FILE, repaired_guests)
        save_json(args.report_json, report)
        args.report_markdown.parent.mkdir(parents=True, exist_ok=True)
        args.report_markdown.write_text(markdown, encoding="utf-8")

    print("GUEST MEDIA BACKFILL")
    print("====================")
    print(f"Total changes: {report['summary']['total_changes']}")
    print(f"Actions: {report['summary']['actions']}")
    print(f"Review items: {report['summary']['review_items']}")
    for key, count in report["summary"]["changes"].items():
        print(f"{key}: {count}")
    if args.dry_run:
        print("\nDry run: no files written.")
    else:
        print(f"\nReport JSON: {args.report_json}")
        print(f"Report Markdown: {args.report_markdown}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
