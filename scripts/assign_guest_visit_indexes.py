#!/usr/bin/env python3
"""Assign deterministic visit_index values for video-backed multi-visit guests."""

from __future__ import annotations

import argparse
import copy
import sys
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.utils import GUESTS_FILE, VALIDATION_DIR, load_json, save_json


SCHEMA_VERSION = 2
DEFAULT_REPORT_JSON = VALIDATION_DIR / "guest_visit_index_assignment.json"
DEFAULT_REPORT_MARKDOWN = VALIDATION_DIR / "guest_visit_index_assignment.md"


def guest_has_video(guest: dict[str, Any]) -> bool:
    if guest.get("youtube_video_id") or guest.get("vimeo_video_id"):
        return True
    return any(
        visit.get("youtube_video_id") or visit.get("vimeo_video_id")
        for visit in guest.get("visits") or []
    )


def assign_guest_visit_indexes(
    guests: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    repaired = copy.deepcopy(guests)
    changes: Counter[str] = Counter()
    actions: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []

    for guest in repaired:
        slug = str(guest.get("slug") or "")
        visits = guest.get("visits") or []
        if not visits or not guest_has_video(guest):
            continue

        existing_indexes = [visit.get("visit_index") for visit in visits]
        missing_positions = [
            position for position, index in enumerate(existing_indexes) if not index
        ]
        if not missing_positions:
            continue

        existing_numbered_indexes = [
            index for index in existing_indexes if index not in {None, "", 0}
        ]
        invalid_indexes = [
            index for index in existing_numbered_indexes if not isinstance(index, int)
        ]
        duplicate_indexes = [
            index
            for index, count in Counter(existing_numbered_indexes).items()
            if count > 1
        ]
        expected_indexes = set(range(1, len(visits) + 1))
        out_of_range_indexes = [
            index
            for index in existing_numbered_indexes
            if isinstance(index, int) and index not in expected_indexes
        ]
        missing_index_values = sorted(expected_indexes - set(existing_numbered_indexes))

        if (
            invalid_indexes
            or duplicate_indexes
            or out_of_range_indexes
            or len(missing_index_values) != len(missing_positions)
        ):
            review.append(
                {
                    "type": "ambiguous_missing_visit_index",
                    "file": "data/guests.json",
                    "record_key": slug,
                    "title": str(guest.get("name") or slug),
                    "evidence": {
                        "visit_indexes": existing_indexes,
                        "invalid_indexes": invalid_indexes,
                        "duplicate_indexes": duplicate_indexes,
                        "out_of_range_indexes": out_of_range_indexes,
                        "candidate_missing_indexes": missing_index_values,
                    },
                    "suggested_action": "Review visit ordering before automated assignment.",
                }
            )
            continue

        assigned: list[dict[str, Any]] = []
        for position, index in zip(missing_positions, missing_index_values, strict=True):
            visit = visits[position]
            visit["visit_index"] = index
            changes["data/guests.json:visit_index_assigned"] += 1
            assigned.append(
                {
                    "visit_index": index,
                    "youtube_video_id": visit.get("youtube_video_id"),
                    "vimeo_video_id": visit.get("vimeo_video_id"),
                    "episode_date": visit.get("episode_date"),
                    "criterion_page_url": visit.get("criterion_page_url"),
                }
            )

        actions.append(
            {
                "type": "guest_visit_indexes_assigned",
                "file": "data/guests.json",
                "record_key": slug,
                "title": str(guest.get("name") or slug),
                "evidence": {"assigned_visits": assigned},
            }
        )

    actions = sorted(actions, key=lambda item: item["record_key"])
    review = sorted(review, key=lambda item: item["record_key"])
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
        "# Guest Visit Index Assignment",
        "",
        "Assigned missing `visit_index` values for video-backed guests when local visit order made the gap unambiguous.",
        "",
        "## Summary",
        "",
        f"- Total changes: {summary['total_changes']}",
        f"- Guests updated: {summary['actions']}",
        f"- Review items: {summary['review_items']}",
        "",
        "## Changes",
        "",
    ]
    for key, count in summary["changes"].items():
        lines.append(f"- `{key}`: {count}")

    if report["actions"]:
        lines.extend(
            [
                "",
                "## Updated Guests",
                "",
                "| Guest | Assigned visit indexes |",
                "|---|---|",
            ]
        )
        for item in report["actions"]:
            indexes = ", ".join(
                str(visit["visit_index"])
                for visit in item["evidence"]["assigned_visits"]
            )
            lines.append(
                "| {guest} | {indexes} |".format(
                    guest=str(item["title"]).replace("|", "\\|"),
                    indexes=indexes,
                )
            )

    if report["review_items"]:
        lines.extend(["", "## Review Items", ""])
        for key, count in summary["review_by_type"].items():
            lines.append(f"- `{key}`: {count}")

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Assign missing guest visit_index values")
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing data")
    parser.add_argument("--report-json", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--report-markdown", type=Path, default=DEFAULT_REPORT_MARKDOWN)
    args = parser.parse_args()

    guests = load_json(GUESTS_FILE)
    repaired_guests, report = assign_guest_visit_indexes(guests)
    markdown = render_markdown(report)

    if not args.dry_run:
        save_json(GUESTS_FILE, repaired_guests)
        save_json(args.report_json, report)
        args.report_markdown.parent.mkdir(parents=True, exist_ok=True)
        args.report_markdown.write_text(markdown, encoding="utf-8")

    print("GUEST VISIT INDEX ASSIGNMENT")
    print("============================")
    print(f"Total changes: {report['summary']['total_changes']}")
    print(f"Guests updated: {report['summary']['actions']}")
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
