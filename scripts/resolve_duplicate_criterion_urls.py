#!/usr/bin/env python3
"""
Resolve known duplicate Criterion URL catalog identities.

This is intentionally narrow: it handles the three duplicate URL families left
after the spine remediation pass and writes a reviewable report.
"""

from __future__ import annotations

import argparse
import copy
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.audit_data_quality import pick_key
from scripts.utils import (
    CATALOG_FILE,
    GUESTS_FILE,
    PICKS_FILE,
    PICKS_RAW_FILE,
    VALIDATION_DIR,
    load_json,
    save_json,
)


RESOLVE_SCHEMA_VERSION = 1
DEFAULT_REPORT_JSON = VALIDATION_DIR / "duplicate_criterion_url_resolution.json"
DEFAULT_REPORT_MARKDOWN = VALIDATION_DIR / "duplicate_criterion_url_resolution.md"

CATALOG_REMOVE_IDS = {
    "7581-gregg-araki-s-teen-apocalypse-trilogy",
    "something-wild-1961",
    "the-killer",
    "the-killing-with-killers-kiss",
}

CATALOG_UPDATES = {
    "the-killing": {
        "criterion_url": "https://www.criterion.com/films/27751-the-killing",
        "spine_number": 575,
        "title": "The Killing",
    },
}

DIRECT_PICK_MERGES = {
    "7581-gregg-araki-s-teen-apocalypse-trilogy": "gregg-arakis-teen-apocalypse-trilogy",
    "gregg-arakis-teen-apocalypse-trilogy": "gregg-arakis-teen-apocalypse-trilogy",
    "something-wild-1961": "something-wild",
    "the-killing-with-killers-kiss": "the-killing",
}

THE_KILLING_URL = "https://www.criterion.com/films/27751-the-killing"
THE_KILLERS_URLS = {
    "https://www.criterion.com/films/725-the-killers",
    "https://www.criterion.com/boxsets/334-the-killers",
}


def catalog_by_film_id(catalog: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(film["film_id"]): film
        for film in catalog
        if film.get("film_id")
    }


def canonical_pick_film_id(pick: dict[str, Any]) -> str | None:
    film_id = str(pick.get("film_id") or pick.get("film_slug") or "")
    if film_id == "the-killer":
        criterion_url = str(pick.get("criterion_film_url") or "")
        if criterion_url == THE_KILLING_URL:
            return "the-killing"
        if criterion_url in THE_KILLERS_URLS:
            return "the-killers"
        return None
    return DIRECT_PICK_MERGES.get(film_id)


def has_value(value: Any) -> bool:
    return value is not None and value != ""


def update_pick_from_catalog(
    pick: dict[str, Any],
    canonical_id: str,
    catalog: dict[str, dict[str, Any]],
    file_name: str,
    changes: Counter[str],
) -> None:
    previous_id = pick.get("film_id")
    if previous_id != canonical_id:
        pick["film_id"] = canonical_id
        changes[f"{file_name}:film_id_updated"] += 1

    if pick.get("film_slug") == previous_id:
        pick["film_slug"] = canonical_id
        changes[f"{file_name}:film_slug_updated"] += 1

    catalog_entry = catalog.get(canonical_id)
    if not catalog_entry:
        return

    title = catalog_entry.get("title")
    if title and pick.get("film_title") != title:
        pick["film_title"] = title
        changes[f"{file_name}:film_title_from_catalog"] += 1
    if title and pick.get("catalog_title") != title:
        pick["catalog_title"] = title
        changes[f"{file_name}:catalog_title_from_catalog"] += 1

    spine = catalog_entry.get("spine_number")
    if pick.get("catalog_spine") != spine:
        pick["catalog_spine"] = spine
        changes[f"{file_name}:catalog_spine_from_catalog"] += 1

    catalog_url = catalog_entry.get("criterion_url")
    if has_value(catalog_url) and (
        pick.get("criterion_film_url") in {"", None}
        or pick.get("criterion_film_url") == THE_KILLING_URL
        or previous_id in DIRECT_PICK_MERGES
    ):
        if pick.get("criterion_film_url") != catalog_url:
            pick["criterion_film_url"] = catalog_url
            changes[f"{file_name}:criterion_url_from_catalog"] += 1

    if catalog_url and pick.get("criterion_film_url") == catalog_url:
        if pick.get("match_method") != "criterion_url":
            pick["match_method"] = "criterion_url"
            changes[f"{file_name}:match_method_criterion_url"] += 1


def update_pick_rows(
    rows: list[dict[str, Any]],
    catalog: dict[str, dict[str, Any]],
    file_name: str,
    changes: Counter[str],
) -> None:
    for pick in rows:
        canonical_id = canonical_pick_film_id(pick)
        if not canonical_id:
            continue
        update_pick_from_catalog(pick, canonical_id, catalog, file_name, changes)


def pick_order_distance(pick: dict[str, Any], enriched_by_key: dict[str, dict[str, Any]]) -> int:
    enriched = enriched_by_key.get(pick_key(pick))
    if not enriched:
        return 10_000
    try:
        return abs(int(pick.get("pick_order")) - int(enriched.get("pick_order")))
    except (TypeError, ValueError):
        return 10_000


def raw_dedupe_score(
    pick: dict[str, Any],
    enriched_by_key: dict[str, dict[str, Any]],
    affected_ids: set[str],
) -> tuple[int, int, int, str]:
    key = pick_key(pick)
    film_id = str(pick.get("film_id") or "")
    has_enriched_counterpart = 0 if key in enriched_by_key else 1
    distance = pick_order_distance(pick, enriched_by_key)
    try:
        order = int(pick.get("pick_order"))
    except (TypeError, ValueError):
        order = 10_000
    affected = 0 if film_id in affected_ids else 1
    return (has_enriched_counterpart, distance, order, affected)


def dedupe_raw_rows(
    picks_raw: list[dict[str, Any]],
    enriched_picks: list[dict[str, Any]],
    changes: Counter[str],
    review: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    affected_ids = {
        "gregg-arakis-teen-apocalypse-trilogy",
        "something-wild",
        "the-killing",
        "the-killers",
    }
    enriched_by_key = {pick_key(pick): pick for pick in enriched_picks}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for pick in picks_raw:
        grouped[pick_key(pick)].append(pick)

    deduped: list[dict[str, Any]] = []
    for key, group in grouped.items():
        film_id = str(group[0].get("film_id") or "")
        if len(group) == 1 or film_id not in affected_ids:
            deduped.extend(group)
            continue

        ordered = sorted(
            group,
            key=lambda pick: raw_dedupe_score(pick, enriched_by_key, affected_ids),
        )
        keep = ordered[0]
        deduped.append(keep)
        changes["data/picks_raw.json:duplicate_rows_removed"] += len(ordered) - 1
        review.append(
            {
                "type": "deduped_raw_pick_key",
                "file": "data/picks_raw.json",
                "record_key": key,
                "kept_pick_order": keep.get("pick_order"),
                "removed_pick_orders": [item.get("pick_order") for item in ordered[1:]],
            }
        )

    return deduped


def resolve_duplicate_criterion_urls(
    catalog: list[dict[str, Any]],
    picks: list[dict[str, Any]],
    picks_raw: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    repaired_catalog = copy.deepcopy(catalog)
    repaired_picks = copy.deepcopy(picks)
    repaired_picks_raw = copy.deepcopy(picks_raw)
    changes: Counter[str] = Counter()
    review: list[dict[str, Any]] = []

    kept_catalog: list[dict[str, Any]] = []
    for film in repaired_catalog:
        film_id = str(film.get("film_id") or "")
        if film_id in CATALOG_REMOVE_IDS:
            changes["data/criterion_catalog.json:duplicate_rows_removed"] += 1
            continue
        updates = CATALOG_UPDATES.get(film_id, {})
        for field, value in updates.items():
            if film.get(field) != value:
                film[field] = value
                changes[f"data/criterion_catalog.json:{field}_updated"] += 1
        kept_catalog.append(film)

    repaired_catalog = kept_catalog
    catalog_index = catalog_by_film_id(repaired_catalog)
    update_pick_rows(repaired_picks, catalog_index, "data/picks.json", changes)
    update_pick_rows(repaired_picks_raw, catalog_index, "data/picks_raw.json", changes)
    repaired_picks_raw = dedupe_raw_rows(repaired_picks_raw, repaired_picks, changes, review)

    report = {
        "schema_version": RESOLVE_SCHEMA_VERSION,
        "summary": {
            "changes": dict(sorted(changes.items())),
            "total_changes": sum(changes.values()),
            "review_items": len(review),
        },
        "review_items": sorted(review, key=lambda item: item["record_key"]),
    }
    return repaired_catalog, repaired_picks, repaired_picks_raw, report


def render_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# Duplicate Criterion URL Resolution",
        "",
        "Resolved the three known duplicate Criterion URL families by merging pick references into canonical catalog rows.",
        "",
        "## Summary",
        "",
        f"- Total changes: {summary['total_changes']}",
        f"- Review items: {summary['review_items']}",
        "",
        "## Changes",
        "",
    ]
    for key, count in summary["changes"].items():
        lines.append(f"- `{key}`: {count}")

    if report["review_items"]:
        lines.extend(
            [
                "",
                "## Raw Pick Dedupes",
                "",
                "| Record | Kept order | Removed orders |",
                "|---|---:|---|",
            ]
        )
        for item in report["review_items"]:
            lines.append(
                "| {record} | {kept} | {removed} |".format(
                    record=str(item["record_key"]).replace("|", "\\|"),
                    kept=item["kept_pick_order"],
                    removed=", ".join(str(order) for order in item["removed_pick_orders"]),
                )
            )

    return "\n".join(lines) + "\n"


def displayable_pick_counts(
    picks: list[dict[str, Any]],
    picks_raw: list[dict[str, Any]],
) -> dict[str, int]:
    picks_by_guest: dict[str, list[dict[str, Any]]] = defaultdict(list)
    raw_by_guest: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for pick in picks:
        picks_by_guest[str(pick.get("guest_slug") or "")].append(pick)
    for pick in picks_raw:
        raw_by_guest[str(pick.get("guest_slug") or "")].append(pick)

    guest_slugs = set(picks_by_guest) | set(raw_by_guest)
    counts: dict[str, int] = {}
    for slug in guest_slugs:
        processed_slugs: set[str] = set()
        displayable = 0
        for pick in picks_by_guest.get(slug, []):
            film_key = str(pick.get("film_slug") or pick.get("film_id") or "")
            if film_key:
                processed_slugs.add(film_key)
            if pick.get("source") == "criterion" or (pick.get("quote") or "").strip():
                displayable += 1

        for pick in raw_by_guest.get(slug, []):
            film_key = str(pick.get("film_id") or "")
            if film_key in processed_slugs:
                continue
            if pick.get("source") == "criterion":
                displayable += 1
        counts[slug] = displayable
    return counts


def update_guest_pick_counts(
    guests: list[dict[str, Any]],
    picks: list[dict[str, Any]],
    picks_raw: list[dict[str, Any]],
    changes: Counter[str],
) -> list[dict[str, Any]]:
    repaired_guests = copy.deepcopy(guests)
    counts = displayable_pick_counts(picks, picks_raw)
    for guest in repaired_guests:
        slug = str(guest.get("slug") or "")
        if slug not in counts:
            continue
        if guest.get("pick_count") != counts[slug]:
            guest["pick_count"] = counts[slug]
            changes["data/guests.json:pick_count_updated"] += 1
    return repaired_guests


def main() -> int:
    parser = argparse.ArgumentParser(description="Resolve known duplicate Criterion URL records")
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing data")
    parser.add_argument("--report-json", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--report-markdown", type=Path, default=DEFAULT_REPORT_MARKDOWN)
    args = parser.parse_args()

    catalog = load_json(CATALOG_FILE)
    guests = load_json(GUESTS_FILE)
    picks = load_json(PICKS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)
    repaired_catalog, repaired_picks, repaired_picks_raw, report = resolve_duplicate_criterion_urls(
        catalog,
        picks,
        picks_raw,
    )
    changes = Counter(report["summary"]["changes"])
    repaired_guests = update_guest_pick_counts(
        guests,
        repaired_picks,
        repaired_picks_raw,
        changes,
    )
    report["summary"]["changes"] = dict(sorted(changes.items()))
    report["summary"]["total_changes"] = sum(changes.values())
    markdown = render_markdown(report)

    if not args.dry_run:
        save_json(GUESTS_FILE, repaired_guests)
        save_json(CATALOG_FILE, repaired_catalog)
        save_json(PICKS_FILE, repaired_picks)
        save_json(PICKS_RAW_FILE, repaired_picks_raw)
        save_json(args.report_json, report)
        args.report_markdown.parent.mkdir(parents=True, exist_ok=True)
        args.report_markdown.write_text(markdown, encoding="utf-8")

    print("DUPLICATE CRITERION URL RESOLUTION")
    print("==================================")
    print(f"Total changes: {report['summary']['total_changes']}")
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
