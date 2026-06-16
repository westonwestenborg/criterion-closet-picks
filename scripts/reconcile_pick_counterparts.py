#!/usr/bin/env python3
"""
Resolve raw/enriched pick counterpart drift.

The audit treats (guest_slug, visit_index, film_id) as the stable pick key.
This helper only changes records when local data gives a deterministic answer:

- stale raw-only rows that collide with an enriched pick at the same pick_order
  are removed when the enriched pick already has its own raw counterpart;
- a stale raw-only row can be replaced by an enriched raw shell when it is the
  only raw record for that pick_order;
- raw-only rows without an order collision are promoted into picks.json because
  the frontend already displays them as raw fallbacks.
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


RECONCILE_SCHEMA_VERSION = 2
DEFAULT_REPORT_JSON = VALIDATION_DIR / "pick_counterpart_reconciliation.json"
DEFAULT_REPORT_MARKDOWN = VALIDATION_DIR / "pick_counterpart_reconciliation.md"

PICK_FIELD_ORDER = [
    "guest_slug",
    "guest_name",
    "film_title",
    "film_year",
    "film_id",
    "catalog_spine",
    "catalog_title",
    "match_method",
    "letterboxd_url",
    "criterion_film_url",
    "source",
    "is_box_set",
    "box_set_name",
    "quote",
    "start_timestamp",
    "youtube_timestamp_url",
    "extraction_confidence",
    "box_set_film_count",
    "box_set_film_titles",
    "box_set_criterion_url",
    "film_slug",
    "visit_index",
    "pick_order",
]

RAW_FIELD_ORDER = [
    "guest_slug",
    "guest_name",
    "film_title",
    "film_year",
    "film_id",
    "catalog_spine",
    "catalog_title",
    "match_method",
    "letterboxd_url",
    "criterion_film_url",
    "source",
    "visit_index",
    "is_box_set",
    "box_set_name",
    "box_set_film_count",
    "box_set_film_titles",
    "box_set_criterion_url",
    "quote",
    "start_timestamp",
    "youtube_timestamp_url",
    "extraction_confidence",
    "pick_order",
]


def ordered_copy(source: dict[str, Any], field_order: list[str]) -> dict[str, Any]:
    copied: dict[str, Any] = {}
    for field in field_order:
        if field in source:
            copied[field] = copy.deepcopy(source[field])
    for field, value in source.items():
        if field not in copied:
            copied[field] = copy.deepcopy(value)
    return copied


def has_text(value: Any) -> bool:
    return isinstance(value, str) and value.strip() != ""


def catalog_by_film_id(catalog: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(film["film_id"]): film
        for film in catalog
        if film.get("film_id")
    }


def unique_pick_index(
    rows: list[dict[str, Any]],
    file_name: str,
    review: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], set[str]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[pick_key(row)].append(row)

    unique: dict[str, dict[str, Any]] = {}
    duplicate_keys: set[str] = set()
    for key, matches in sorted(grouped.items()):
        if len(matches) == 1:
            unique[key] = matches[0]
            continue
        duplicate_keys.add(key)
        review.append(
            {
                "type": "ambiguous_duplicate_pick_key",
                "severity": "P1",
                "category": "pick_consistency",
                "file": file_name,
                "record_key": key,
                "title": str(matches[0].get("film_title") or key),
                "evidence": {
                    "count": len(matches),
                    "pick_orders": [item.get("pick_order") for item in matches],
                },
                "suggested_action": "Dedupe this key before automated counterpart reconciliation.",
            }
        )
    return unique, duplicate_keys


def order_key(pick: dict[str, Any]) -> tuple[str, Any, Any]:
    return (
        str(pick.get("guest_slug") or ""),
        pick.get("visit_index"),
        pick.get("pick_order"),
    )


def rows_by_order(rows: list[dict[str, Any]]) -> dict[tuple[str, Any, Any], list[dict[str, Any]]]:
    grouped: dict[tuple[str, Any, Any], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("pick_order") is None:
            continue
        grouped[order_key(row)].append(row)
    return grouped


def missing_visit_index(value: Any) -> bool:
    return value in {None, ""}


def visit_signature(pick: dict[str, Any]) -> tuple[str, str, Any]:
    return (
        str(pick.get("guest_slug") or ""),
        str(pick.get("film_id") or pick.get("film_slug") or ""),
        pick.get("pick_order"),
    )


def film_signature(pick: dict[str, Any]) -> tuple[str, str]:
    return (
        str(pick.get("guest_slug") or ""),
        str(pick.get("film_id") or pick.get("film_slug") or ""),
    )


def backfill_raw_visit_indexes(
    picks: list[dict[str, Any]],
    picks_raw: list[dict[str, Any]],
    changes: Counter[str],
    actions: list[dict[str, Any]],
    review: list[dict[str, Any]],
) -> tuple[set[str], set[str]]:
    """Copy raw visit_index from unique enriched matches before key reconciliation."""
    picks_by_signature: dict[tuple[str, str, Any], list[dict[str, Any]]] = defaultdict(list)
    picks_by_film: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for pick in picks:
        if missing_visit_index(pick.get("visit_index")) or pick.get("pick_order") is None:
            continue
        picks_by_signature[visit_signature(pick)].append(pick)
        picks_by_film[film_signature(pick)].append(pick)

    blocked_raw_keys: set[str] = set()
    blocked_pick_keys: set[str] = set()
    for raw in picks_raw:
        if not missing_visit_index(raw.get("visit_index")) or raw.get("pick_order") is None:
            continue

        signature = visit_signature(raw)
        candidates = picks_by_signature.get(signature, [])
        match_basis = "guest_film_pick_order"
        if not candidates:
            candidates = picks_by_film.get(film_signature(raw), [])
            match_basis = "unique_guest_film"
        if not candidates:
            continue

        raw_key = pick_key(raw)
        unique_visit_indexes = sorted(
            {
                candidate.get("visit_index")
                for candidate in candidates
                if not missing_visit_index(candidate.get("visit_index"))
            }
        )
        if len(candidates) != 1 or len(unique_visit_indexes) != 1:
            blocked_raw_keys.add(raw_key)
            blocked_pick_keys.update(pick_key(candidate) for candidate in candidates)
            review.append(
                {
                    "type": "ambiguous_raw_visit_index",
                    "severity": "P2",
                    "category": "pick_consistency",
                    "file": "data/picks_raw.json",
                    "record_key": raw_key,
                    "title": str(raw.get("film_title") or raw_key),
                    "evidence": {
                        "signature": {
                            "guest_slug": signature[0],
                            "film_id": signature[1],
                            "pick_order": signature[2],
                        },
                        "candidate_keys": [pick_key(candidate) for candidate in candidates],
                        "candidate_visit_indexes": unique_visit_indexes,
                        "match_basis": match_basis,
                    },
                    "suggested_action": "Resolve the ambiguous enriched visit before copying visit_index to raw data.",
                }
            )
            continue

        raw["visit_index"] = unique_visit_indexes[0]
        changes["data/picks_raw.json:visit_index_from_enriched_pick"] += 1
        actions.append(
            action_item(
                "raw_visit_index_backfilled",
                "data/picks_raw.json",
                raw_key,
                str(raw.get("film_title") or raw_key),
                {
                    "new_key": pick_key(raw),
                    "visit_index": raw.get("visit_index"),
                    "signature": {
                        "guest_slug": signature[0],
                        "film_id": signature[1],
                        "pick_order": signature[2],
                    },
                    "match_basis": match_basis,
                },
            )
        )
    return blocked_raw_keys, blocked_pick_keys


def local_box_set_film_count(
    pick: dict[str, Any],
    catalog_index: dict[str, dict[str, Any]],
    local_rows: list[dict[str, Any]],
) -> int | None:
    value = pick.get("box_set_film_count")
    if isinstance(value, int) and value > 0:
        return value

    film_id = str(pick.get("film_id") or pick.get("film_slug") or "")
    catalog_entry = catalog_index.get(film_id)
    value = (catalog_entry or {}).get("box_set_film_count")
    if isinstance(value, int) and value > 0:
        return value

    box_set_name = str(pick.get("box_set_name") or "")
    criterion_url = str(
        pick.get("box_set_criterion_url") or pick.get("criterion_film_url") or ""
    )
    for row in local_rows:
        row_count = row.get("box_set_film_count")
        if not isinstance(row_count, int) or row_count <= 0:
            continue
        if film_id and film_id == str(row.get("film_id") or row.get("film_slug") or ""):
            return row_count
        if box_set_name and box_set_name == str(row.get("box_set_name") or ""):
            return row_count
        row_url = str(row.get("box_set_criterion_url") or row.get("criterion_film_url") or "")
        if criterion_url and criterion_url == row_url:
            return row_count
    return None


def promote_raw_pick(
    raw: dict[str, Any],
    catalog_index: dict[str, dict[str, Any]],
    local_rows: list[dict[str, Any]],
    changes: Counter[str],
) -> dict[str, Any]:
    promoted = ordered_copy(raw, PICK_FIELD_ORDER)
    promoted["quote"] = promoted.get("quote") or ""
    promoted["start_timestamp"] = promoted.get("start_timestamp")
    promoted["youtube_timestamp_url"] = promoted.get("youtube_timestamp_url") or ""
    promoted["extraction_confidence"] = promoted.get("extraction_confidence") or "none"

    film_id = str(promoted.get("film_id") or promoted.get("film_slug") or "")
    catalog_entry = catalog_index.get(film_id, {})
    criterion_url = str(promoted.get("criterion_film_url") or "")
    is_box_set = bool(
        promoted.get("is_box_set")
        or catalog_entry.get("is_box_set")
        or "/boxsets/" in criterion_url
    )

    if is_box_set:
        count = local_box_set_film_count(promoted, catalog_index, local_rows)
        if count:
            promoted["is_box_set"] = True
            promoted["box_set_film_count"] = count
            promoted["box_set_name"] = promoted.get("box_set_name") or catalog_entry.get("title")
            promoted["box_set_criterion_url"] = (
                promoted.get("box_set_criterion_url")
                or promoted.get("criterion_film_url")
                or catalog_entry.get("criterion_url")
            )
            changes["data/picks.json:box_set_aggregate_fields_inferred"] += 1
        elif (
            promoted.get("source") == "criterion"
            and not has_text(promoted.get("quote"))
            and promoted.get("extraction_confidence") in {None, "", "none"}
        ):
            # Frontend raw fallbacks do not carry is_box_set. Keeping this false
            # preserves visibility when local data lacks aggregate box-set counts.
            promoted["is_box_set"] = False
            promoted.pop("box_set_film_count", None)
            changes["data/picks.json:box_set_flag_normalized_for_display"] += 1

    return promoted


def raw_shell_from_enriched(pick: dict[str, Any]) -> dict[str, Any]:
    raw = ordered_copy(pick, RAW_FIELD_ORDER)
    raw["quote"] = ""
    raw["start_timestamp"] = None
    raw["youtube_timestamp_url"] = ""
    raw["extraction_confidence"] = "none"
    return raw


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

    counts: dict[str, int] = {}
    for slug in sorted(set(picks_by_guest) | set(raw_by_guest)):
        processed_slugs: set[str] = set()
        displayable = 0
        for pick in picks_by_guest.get(slug, []):
            film_key = str(pick.get("film_slug") or pick.get("film_id") or "")
            if film_key:
                processed_slugs.add(film_key)
            if pick.get("source") == "criterion" or has_text(pick.get("quote")):
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


def action_item(
    action_type: str,
    file: str,
    record_key: str,
    title: str,
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "type": action_type,
        "file": file,
        "record_key": record_key,
        "title": title,
        "evidence": evidence or {},
    }


def reconcile_pick_counterparts(
    guests: list[dict[str, Any]],
    catalog: list[dict[str, Any]],
    picks: list[dict[str, Any]],
    picks_raw: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    repaired_picks = copy.deepcopy(picks)
    repaired_raw = copy.deepcopy(picks_raw)
    changes: Counter[str] = Counter()
    review: list[dict[str, Any]] = []
    actions: list[dict[str, Any]] = []

    blocked_raw_keys, blocked_pick_keys = backfill_raw_visit_indexes(
        repaired_picks,
        repaired_raw,
        changes,
        actions,
        review,
    )

    picks_by_key, duplicate_pick_keys = unique_pick_index(
        repaired_picks,
        "data/picks.json",
        review,
    )
    raw_by_key, duplicate_raw_keys = unique_pick_index(
        repaired_raw,
        "data/picks_raw.json",
        review,
    )
    picks_by_order = rows_by_order(repaired_picks)
    catalog_index = catalog_by_film_id(catalog)
    local_rows = [*repaired_picks, *repaired_raw]

    raw_keys_to_remove: set[str] = set()
    raw_replacements: dict[str, dict[str, Any]] = {}
    promotions: list[dict[str, Any]] = []

    for key, raw in sorted(raw_by_key.items()):
        if key in blocked_raw_keys:
            continue
        if key in picks_by_key:
            continue
        if key in duplicate_pick_keys or key in duplicate_raw_keys:
            continue

        order_matches = picks_by_order.get(order_key(raw), [])
        if order_matches:
            if len(order_matches) != 1:
                review.append(
                    {
                        "type": "ambiguous_pick_order_collision",
                        "severity": "P2",
                        "category": "pick_consistency",
                        "file": "data/picks_raw.json",
                        "record_key": key,
                        "title": str(raw.get("film_title") or key),
                        "evidence": {
                            "order_key": order_key(raw),
                            "candidate_keys": [pick_key(item) for item in order_matches],
                        },
                        "suggested_action": "Resolve the duplicate pick_order before counterpart reconciliation.",
                    }
                )
                continue

            enriched = order_matches[0]
            enriched_key = pick_key(enriched)
            if enriched_key in raw_by_key:
                raw_keys_to_remove.add(key)
                changes["data/picks_raw.json:stale_raw_counterpart_removed"] += 1
                actions.append(
                    action_item(
                        "stale_raw_counterpart_removed",
                        "data/picks_raw.json",
                        key,
                        str(raw.get("film_title") or key),
                        {
                            "same_order_enriched_key": enriched_key,
                            "same_order_enriched_title": enriched.get("film_title"),
                        },
                    )
                )
            else:
                raw_replacements[key] = raw_shell_from_enriched(enriched)
                changes["data/picks_raw.json:stale_raw_counterpart_replaced"] += 1
                actions.append(
                    action_item(
                        "stale_raw_counterpart_replaced",
                        "data/picks_raw.json",
                        key,
                        str(raw.get("film_title") or key),
                        {
                            "replacement_key": enriched_key,
                            "replacement_title": enriched.get("film_title"),
                        },
                    )
                )
            continue

        promoted = promote_raw_pick(raw, catalog_index, local_rows, changes)
        promotions.append(promoted)
        changes["data/picks.json:raw_counterpart_promoted"] += 1
        actions.append(
            action_item(
                "raw_counterpart_promoted",
                "data/picks.json",
                key,
                str(raw.get("film_title") or key),
                {"guest_slug": raw.get("guest_slug"), "film_id": raw.get("film_id")},
            )
        )

    next_raw: list[dict[str, Any]] = []
    for raw in repaired_raw:
        key = pick_key(raw)
        if key in raw_keys_to_remove:
            continue
        next_raw.append(raw_replacements.get(key, raw))
    repaired_raw = next_raw
    repaired_picks.extend(promotions)

    raw_keys_after = {pick_key(raw) for raw in repaired_raw}
    for key, pick in sorted(picks_by_key.items()):
        if key in blocked_pick_keys:
            continue
        if key in raw_keys_after or key in duplicate_raw_keys:
            continue
        shell = raw_shell_from_enriched(pick)
        repaired_raw.append(shell)
        raw_keys_after.add(key)
        changes["data/picks_raw.json:enriched_counterpart_shell_added"] += 1
        actions.append(
            action_item(
                "enriched_counterpart_shell_added",
                "data/picks_raw.json",
                key,
                str(pick.get("film_title") or key),
                {"guest_slug": pick.get("guest_slug"), "film_id": pick.get("film_id")},
            )
        )

    repaired_guests = update_guest_pick_counts(guests, repaired_picks, repaired_raw, changes)

    actions = sorted(actions, key=lambda item: (item["type"], item["file"], item["record_key"]))
    review = sorted(
        review,
        key=lambda item: (
            item["severity"],
            item["category"],
            item["type"],
            item["file"],
            item["record_key"],
        ),
    )
    report = {
        "schema_version": RECONCILE_SCHEMA_VERSION,
        "summary": {
            "changes": dict(sorted(changes.items())),
            "total_changes": sum(changes.values()),
            "actions": len(actions),
            "actions_by_type": dict(sorted(Counter(item["type"] for item in actions).items())),
            "review_items": len(review),
            "review_by_type": dict(sorted(Counter(item["type"] for item in review).items())),
        },
        "actions": actions,
        "review_items": review,
    }
    return repaired_guests, repaired_picks, repaired_raw, report


def render_markdown(report: dict[str, Any], max_items: int = 200) -> str:
    summary = report["summary"]
    lines = [
        "# Pick Counterpart Reconciliation",
        "",
        "Resolved deterministic drift between `picks.json` and `picks_raw.json`.",
        "",
        "## Summary",
        "",
        f"- Total changes: {summary['total_changes']}",
        f"- Actions: {summary['actions']}",
        f"- Review items: {summary['review_items']}",
        "",
        "## Changes",
        "",
    ]
    for key, count in summary["changes"].items():
        lines.append(f"- `{key}`: {count}")

    lines.extend(["", "## Actions by Type", ""])
    for key, count in summary["actions_by_type"].items():
        lines.append(f"- `{key}`: {count}")

    actions = report["actions"][:max_items]
    lines.extend(
        [
            "",
            f"## Actions (first {len(actions)})",
            "",
            "| Type | File | Record | Title |",
            "|---|---|---|---|",
        ]
    )
    for item in actions:
        lines.append(
            "| {type} | `{file}` | `{record}` | {title} |".format(
                type=item["type"],
                file=item["file"],
                record=str(item["record_key"]).replace("|", "\\|"),
                title=str(item["title"]).replace("|", "\\|"),
            )
        )

    if report["review_items"]:
        lines.extend(["", "## Review Items", ""])
        for key, count in summary["review_by_type"].items():
            lines.append(f"- `{key}`: {count}")

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile pick raw/enriched counterparts")
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing data")
    parser.add_argument("--report-json", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--report-markdown", type=Path, default=DEFAULT_REPORT_MARKDOWN)
    parser.add_argument("--max-markdown-items", type=int, default=200)
    args = parser.parse_args()

    guests = load_json(GUESTS_FILE)
    catalog = load_json(CATALOG_FILE)
    picks = load_json(PICKS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)
    repaired_guests, repaired_picks, repaired_raw, report = reconcile_pick_counterparts(
        guests,
        catalog,
        picks,
        picks_raw,
    )
    markdown = render_markdown(report, max_items=args.max_markdown_items)

    if not args.dry_run:
        save_json(GUESTS_FILE, repaired_guests)
        save_json(PICKS_FILE, repaired_picks)
        save_json(PICKS_RAW_FILE, repaired_raw)
        save_json(args.report_json, report)
        args.report_markdown.parent.mkdir(parents=True, exist_ok=True)
        args.report_markdown.write_text(markdown, encoding="utf-8")

    print("PICK COUNTERPART RECONCILIATION")
    print("===============================")
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
