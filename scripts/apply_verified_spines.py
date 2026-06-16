#!/usr/bin/env python3
"""
Apply externally verified Criterion spine numbers to the local catalog.

The verification input is a deterministic JSON file checked into data/validation.
This script does not fetch Criterion pages; it only applies records whose status
is verified_spine and reports no_spine_visible records separately.
"""

from __future__ import annotations

import argparse
import copy
import sys
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.utils import CATALOG_FILE, VALIDATION_DIR, load_json, save_json


BACKFILL_SCHEMA_VERSION = 1
DEFAULT_VERIFICATION_FILE = VALIDATION_DIR / "verified_spine_backfill.json"
DEFAULT_REPORT_JSON = VALIDATION_DIR / "verified_spine_backfill_report.json"
DEFAULT_REPORT_MARKDOWN = VALIDATION_DIR / "verified_spine_backfill_report.md"


def coerce_spine(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def load_verification_records(path: Path) -> list[dict[str, Any]]:
    raw = load_json(path)
    if isinstance(raw, dict):
        records = raw.get("records", [])
    else:
        records = raw
    if not isinstance(records, list):
        return []
    return [record for record in records if isinstance(record, dict)]


def catalog_by_film_id(catalog: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(film["film_id"]): film
        for film in catalog
        if film.get("film_id")
    }


def report_item(
    action: str,
    film_id: str,
    title: str,
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "action": action,
        "film_id": film_id,
        "title": title,
        "evidence": evidence or {},
    }


def sort_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(items, key=lambda item: (item["action"], item["film_id"]))


def apply_verified_spines(
    catalog: list[dict[str, Any]],
    verification_records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    repaired_catalog = copy.deepcopy(catalog)
    by_id = catalog_by_film_id(repaired_catalog)
    changes: list[dict[str, Any]] = []
    no_spine_visible: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()

    for record in sorted(verification_records, key=lambda item: str(item.get("film_id") or "")):
        film_id = str(record.get("film_id") or "")
        status = str(record.get("status") or "")
        verified_url = str(record.get("criterion_url") or "")
        film = by_id.get(film_id)

        if not film_id:
            counts["invalid_records"] += 1
            review.append(
                report_item(
                    "invalid_record",
                    film_id,
                    str(record.get("title") or "Missing film_id"),
                    {"record": record},
                )
            )
            continue

        if status == "no_spine_visible":
            counts["no_spine_visible"] += 1
            no_spine_visible.append(
                report_item(
                    "no_spine_visible",
                    film_id,
                    str((film or record).get("title") or film_id),
                    {
                        "criterion_url": verified_url,
                        "note": record.get("note"),
                    },
                )
            )
            continue

        if status != "verified_spine":
            counts["invalid_records"] += 1
            review.append(
                report_item(
                    "unknown_status",
                    film_id,
                    str(record.get("title") or film_id),
                    {"status": status, "criterion_url": verified_url},
                )
            )
            continue

        spine = coerce_spine(record.get("spine_number"))
        if spine is None:
            counts["invalid_records"] += 1
            review.append(
                report_item(
                    "missing_verified_spine",
                    film_id,
                    str(record.get("title") or film_id),
                    {
                        "spine_number": record.get("spine_number"),
                        "criterion_url": verified_url,
                    },
                )
            )
            continue

        if not film:
            counts["review_items"] += 1
            review.append(
                report_item(
                    "missing_catalog_record",
                    film_id,
                    str(record.get("title") or film_id),
                    {"spine_number": spine, "criterion_url": verified_url},
                )
            )
            continue

        catalog_url = str(film.get("criterion_url") or "")
        if verified_url and catalog_url and verified_url != catalog_url:
            counts["review_items"] += 1
            review.append(
                report_item(
                    "criterion_url_mismatch",
                    film_id,
                    str(film.get("title") or film_id),
                    {
                        "catalog_criterion_url": catalog_url,
                        "verified_criterion_url": verified_url,
                        "spine_number": spine,
                    },
                )
            )
            continue

        previous_spine = film.get("spine_number")
        if previous_spine == spine:
            counts["unchanged"] += 1
            continue

        film["spine_number"] = spine
        counts["spines_updated"] += 1
        changes.append(
            report_item(
                "spine_updated",
                film_id,
                str(film.get("title") or film_id),
                {
                    "previous_spine_number": previous_spine,
                    "spine_number": spine,
                    "criterion_url": verified_url or catalog_url,
                    "evidence": record.get("evidence"),
                },
            )
        )

    report = {
        "schema_version": BACKFILL_SCHEMA_VERSION,
        "summary": {
            "records": len(verification_records),
            "spines_updated": counts["spines_updated"],
            "unchanged": counts["unchanged"],
            "no_spine_visible": counts["no_spine_visible"],
            "review_items": counts["review_items"],
            "invalid_records": counts["invalid_records"],
        },
        "changes": sort_items(changes),
        "no_spine_visible": sort_items(no_spine_visible),
        "review_items": sort_items(review),
    }
    return repaired_catalog, report


def render_markdown(report: dict[str, Any], max_items: int = 200) -> str:
    summary = report["summary"]
    lines = [
        "# Verified Spine Backfill Report",
        "",
        "Applies only checked-in records marked `verified_spine`; records marked `no_spine_visible` are reported but not written to the catalog.",
        "",
        "## Summary",
        "",
        f"- Verification records: {summary['records']}",
        f"- Catalog spines updated: {summary['spines_updated']}",
        f"- Already correct: {summary['unchanged']}",
        f"- No public spine visible: {summary['no_spine_visible']}",
        f"- Review items: {summary['review_items']}",
        f"- Invalid records: {summary['invalid_records']}",
        "",
        "## Updated Spines",
        "",
        "| Film ID | Title | Previous | New | Criterion URL |",
        "|---|---|---:|---:|---|",
    ]
    for item in report["changes"][:max_items]:
        evidence = item["evidence"]
        lines.append(
            "| {film_id} | {title} | {previous} | {new} | {url} |".format(
                film_id=item["film_id"],
                title=str(item["title"]).replace("|", "\\|"),
                previous=evidence.get("previous_spine_number"),
                new=evidence.get("spine_number"),
                url=evidence.get("criterion_url") or "",
            )
        )

    lines.extend(
        [
            "",
            "## No Public Spine Visible",
            "",
            "| Film ID | Title | Criterion URL |",
            "|---|---|---|",
        ]
    )
    for item in report["no_spine_visible"][:max_items]:
        evidence = item["evidence"]
        lines.append(
            "| {film_id} | {title} | {url} |".format(
                film_id=item["film_id"],
                title=str(item["title"]).replace("|", "\\|"),
                url=evidence.get("criterion_url") or "",
            )
        )

    if report["review_items"]:
        lines.extend(
            [
                "",
                "## Review Items",
                "",
                "| Action | Film ID | Title | Evidence |",
                "|---|---|---|---|",
            ]
        )
        for item in report["review_items"][:max_items]:
            lines.append(
                "| {action} | {film_id} | {title} | `{evidence}` |".format(
                    action=item["action"],
                    film_id=item["film_id"],
                    title=str(item["title"]).replace("|", "\\|"),
                    evidence=item["evidence"],
                )
            )

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply verified Criterion spine backfills")
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing data")
    parser.add_argument("--verification-file", type=Path, default=DEFAULT_VERIFICATION_FILE)
    parser.add_argument("--report-json", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--report-markdown", type=Path, default=DEFAULT_REPORT_MARKDOWN)
    parser.add_argument("--max-markdown-items", type=int, default=200)
    args = parser.parse_args()

    catalog = load_json(CATALOG_FILE)
    records = load_verification_records(args.verification_file)
    repaired_catalog, report = apply_verified_spines(catalog, records)
    markdown = render_markdown(report, max_items=args.max_markdown_items)

    if not args.dry_run:
        save_json(CATALOG_FILE, repaired_catalog)
        save_json(args.report_json, report)
        args.report_markdown.parent.mkdir(parents=True, exist_ok=True)
        args.report_markdown.write_text(markdown, encoding="utf-8")

    print("VERIFIED SPINE BACKFILL")
    print("=======================")
    print(f"Verification records: {report['summary']['records']}")
    print(f"Catalog spines updated: {report['summary']['spines_updated']}")
    print(f"No public spine visible: {report['summary']['no_spine_visible']}")
    print(f"Review items: {report['summary']['review_items']}")
    print(f"Invalid records: {report['summary']['invalid_records']}")
    if args.dry_run:
        print("\nDry run: no files written.")
    else:
        print(f"\nReport JSON: {args.report_json}")
        print(f"Report Markdown: {args.report_markdown}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
