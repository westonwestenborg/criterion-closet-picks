#!/usr/bin/env python3
"""Backfill verified Criterion-hosted Vimeo IDs for existing guests."""

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
DEFAULT_REPORT_JSON = VALIDATION_DIR / "verified_vimeo_backfill.json"
DEFAULT_REPORT_MARKDOWN = VALIDATION_DIR / "verified_vimeo_backfill.md"

VERIFIED_VIMEO_IDS = {
    "lee-daniels": {
        "vimeo_video_id": "1008075360",
        "source_url": "https://www.criterion.com/shop/collection/682-lee-daniels-s-closet-picks",
    },
    "park-chan-wook": {
        "vimeo_video_id": "853427660",
        "source_url": "https://www.criterion.com/shop/collection/575-park-chan-wook-s-closet-picks",
    },
    "the-quay-brothers": {
        "vimeo_video_id": "769988668",
        "source_url": "https://www.criterion.com/shop/collection/597-the-quay-brothers-closet-picks",
    },
    "jack-garfein": {
        "vimeo_video_id": "769988118",
        "source_url": "https://www.criterion.com/shop/collection/604-jack-garfein-s-closet-picks",
    },
    "neal-brennan": {
        "vimeo_video_id": "960366172",
        "source_url": "https://www.criterion.com/shop/collection/628-neal-brennan-s-closet-picks",
    },
    "wim-wenders": {
        "vimeo_video_id": "922549736",
        "source_url": "https://www.criterion.com/shop/collection/634-wim-wenders-closet-picks",
    },
}


def backfill_verified_vimeo_ids(
    guests: list[dict[str, Any]],
    verified: dict[str, dict[str, str]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    verified = verified or VERIFIED_VIMEO_IDS
    repaired = copy.deepcopy(guests)
    by_slug = {str(guest.get("slug") or ""): guest for guest in repaired}
    changes: Counter[str] = Counter()
    actions: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []

    for slug, data in sorted(verified.items()):
        guest = by_slug.get(slug)
        if not guest:
            review.append(
                {
                    "type": "verified_vimeo_guest_missing",
                    "file": "data/guests.json",
                    "record_key": slug,
                    "title": slug,
                    "evidence": data,
                    "suggested_action": "Create or rename the guest before applying this verified Vimeo ID.",
                }
            )
            continue

        vimeo_id = data["vimeo_video_id"]
        existing = guest.get("vimeo_video_id")
        if existing == vimeo_id:
            continue
        if existing:
            review.append(
                {
                    "type": "verified_vimeo_conflict",
                    "file": "data/guests.json",
                    "record_key": slug,
                    "title": str(guest.get("name") or slug),
                    "evidence": {
                        "existing_vimeo_video_id": existing,
                        "verified_vimeo_video_id": vimeo_id,
                        "source_url": data["source_url"],
                    },
                    "suggested_action": "Review the conflicting Vimeo IDs before overwriting canonical data.",
                }
            )
            continue

        guest["vimeo_video_id"] = vimeo_id
        changes["data/guests.json:vimeo_video_id_backfilled"] += 1
        actions.append(
            {
                "type": "verified_vimeo_id_backfilled",
                "file": "data/guests.json",
                "record_key": slug,
                "title": str(guest.get("name") or slug),
                "evidence": {
                    "vimeo_video_id": vimeo_id,
                    "source_url": data["source_url"],
                },
            }
        )

    actions = sorted(actions, key=lambda item: item["record_key"])
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
        "# Verified Vimeo Backfill",
        "",
        "Backfilled Vimeo video IDs verified from Criterion collection pages.",
        "",
        "## Summary",
        "",
        f"- Total changes: {summary['total_changes']}",
        f"- Guests updated: {summary['actions']}",
        f"- Review items: {summary['review_items']}",
        "",
        "## Updated Guests",
        "",
        "| Guest | Vimeo ID | Source |",
        "|---|---|---|",
    ]
    for item in report["actions"]:
        evidence = item["evidence"]
        source_url = evidence["source_url"]
        lines.append(
            "| {guest} | `{vimeo}` | {source} |".format(
                guest=str(item["title"]).replace("|", "\\|"),
                vimeo=evidence["vimeo_video_id"],
                source=f"[Criterion]({source_url})",
            )
        )

    if report["review_items"]:
        lines.extend(["", "## Review Items", ""])
        for key, count in summary["review_by_type"].items():
            lines.append(f"- `{key}`: {count}")

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill verified Criterion Vimeo IDs")
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing data")
    parser.add_argument("--report-json", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--report-markdown", type=Path, default=DEFAULT_REPORT_MARKDOWN)
    args = parser.parse_args()

    guests = load_json(GUESTS_FILE)
    repaired_guests, report = backfill_verified_vimeo_ids(guests)
    markdown = render_markdown(report)

    if not args.dry_run:
        save_json(GUESTS_FILE, repaired_guests)
        save_json(args.report_json, report)
        args.report_markdown.parent.mkdir(parents=True, exist_ok=True)
        args.report_markdown.write_text(markdown, encoding="utf-8")

    print("VERIFIED VIMEO BACKFILL")
    print("=======================")
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
