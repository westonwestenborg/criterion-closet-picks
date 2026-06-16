#!/usr/bin/env python3
"""
Deterministic local repairs for data-quality audit findings.

This script only applies fixes backed by current local JSON evidence. Ambiguous
or externally unverifiable records are written to a review report instead of
being guessed.
"""

from __future__ import annotations

import argparse
import copy
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.audit_data_quality import (
    DEFAULT_EXCEPTIONS_FILE,
    load_known_exceptions,
    normalize_text,
    pick_key,
)
from scripts.utils import (
    CATALOG_FILE,
    PICKS_FILE,
    PICKS_RAW_FILE,
    VALIDATION_DIR,
    load_json,
    save_json,
)


REPAIR_SCHEMA_VERSION = 1
DEFAULT_REVIEW_JSON = VALIDATION_DIR / "data_quality_repair_review.json"
DEFAULT_REVIEW_MARKDOWN = VALIDATION_DIR / "data_quality_repair_review.md"

CRITERION_ID_RE = re.compile(
    r"^https://www\.criterion\.com/(?P<kind>films|boxsets)/(?P<id>\d+)(?:[-/]|$)"
)
SMART_QUOTE_TRANSLATION = str.maketrans(
    {
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
    }
)
WORLD_CINEMA_PROJECT_MEMBER_RE = re.compile(
    r"\(world cinema project(?: no\.?\s*(?P<volume>\d+))?\)$"
)
WORLD_CINEMA_PROJECT_NAME_RE = re.compile(
    r"\bworld cinema project(?: no\.?\s*(?P<volume>\d+))?\b"
)


def criterion_identity(url: str | None) -> tuple[str, str] | None:
    if not url:
        return None
    match = CRITERION_ID_RE.match(url)
    if not match:
        return None
    return match.group("kind"), match.group("id")


def has_value(value: Any) -> bool:
    return value is not None and value != ""


def normalize_box_set_title(value: Any) -> str:
    text = str(value or "").translate(SMART_QUOTE_TRANSLATION).strip().lower()
    return re.sub(r"\s+", " ", text)


def world_cinema_project_volume(value: Any) -> int | None:
    match = WORLD_CINEMA_PROJECT_NAME_RE.search(normalize_box_set_title(value))
    if not match:
        return None
    return int(match.group("volume") or 1)


def build_world_cinema_project_member_counts(
    catalog: list[dict[str, Any]],
) -> dict[int, int]:
    counts: Counter[int] = Counter()
    for film in catalog:
        title = normalize_box_set_title(film.get("title"))
        match = WORLD_CINEMA_PROJECT_MEMBER_RE.search(title)
        if not match or "box set" in title:
            continue
        counts[int(match.group("volume") or 1)] += 1
    return dict(counts)


def review_item(
    issue_type: str,
    severity: str,
    category: str,
    file: str,
    record_key: str,
    title: str,
    evidence: dict[str, Any] | None = None,
    suggested_action: str = "Review before changing canonical data.",
) -> dict[str, Any]:
    return {
        "type": issue_type,
        "severity": severity,
        "category": category,
        "file": file,
        "record_key": record_key,
        "title": title,
        "evidence": evidence or {},
        "suggested_action": suggested_action,
    }


def sort_review_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            item["severity"],
            item["category"],
            item["type"],
            item["file"],
            item["record_key"],
        ),
    )


def accepted_film_ids_for_issue(
    exceptions: dict[str, str], issue_type: str
) -> set[str]:
    prefix = f"film:{issue_type}:"
    return {
        key.removeprefix(prefix)
        for key in exceptions
        if key.startswith(prefix)
    }


def build_catalog_by_film_id(catalog: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(film["film_id"]): film for film in catalog if film.get("film_id")}


def build_spine_candidates(
    catalog: list[dict[str, Any]],
    picks: list[dict[str, Any]],
    picks_raw: list[dict[str, Any]],
) -> dict[tuple[str, str], dict[int, list[dict[str, Any]]]]:
    candidates: dict[tuple[str, str], dict[int, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )

    for film in catalog:
        identity = criterion_identity(film.get("criterion_url"))
        spine = film.get("spine_number")
        if identity and has_value(spine):
            candidates[identity][int(spine)].append(
                {
                    "file": "data/criterion_catalog.json",
                    "record_key": film.get("film_id"),
                    "title": film.get("title"),
                }
            )

    for file_name, rows in [
        ("data/picks.json", picks),
        ("data/picks_raw.json", picks_raw),
    ]:
        for pick in rows:
            identity = criterion_identity(pick.get("criterion_film_url"))
            spine = pick.get("catalog_spine")
            if identity and has_value(spine):
                candidates[identity][int(spine)].append(
                    {
                        "file": file_name,
                        "record_key": pick_key(pick),
                        "title": pick.get("film_title"),
                    }
                )

    return candidates


def repair_catalog_spines(
    catalog: list[dict[str, Any]],
    candidates: dict[tuple[str, str], dict[int, list[dict[str, Any]]]],
    changes: Counter[str],
    review: list[dict[str, Any]],
    accepted_regular_no_spine_ids: set[str] | None = None,
) -> None:
    accepted_regular_no_spine_ids = accepted_regular_no_spine_ids or set()
    for film in catalog:
        if has_value(film.get("spine_number")):
            continue
        film_id = str(film.get("film_id") or "")
        identity = criterion_identity(film.get("criterion_url"))
        if not identity:
            continue

        kind, criterion_id = identity
        spine_candidates = candidates.get(identity, {})
        if kind == "boxsets":
            spine_candidates = {
                spine: [
                    source
                    for source in sources
                    if source.get("file") == "data/criterion_catalog.json"
                ]
                for spine, sources in spine_candidates.items()
            }
            spine_candidates = {
                spine: sources for spine, sources in spine_candidates.items() if sources
            }
        spine_values = sorted(spine_candidates)
        if len(spine_values) == 1:
            film["spine_number"] = spine_values[0]
            changes["catalog_spines_backfilled"] += 1
            continue

        if len(spine_values) > 1:
            review.append(
                review_item(
                    "ambiguous_catalog_spine",
                    "P1",
                    "identity",
                    "data/criterion_catalog.json",
                    str(film.get("film_id") or ""),
                    str(film.get("title") or film.get("film_id") or ""),
                    {
                        "criterion_url": film.get("criterion_url"),
                        "criterion_kind": kind,
                        "criterion_id": criterion_id,
                        "candidate_spines": spine_values,
                        "candidate_sources": spine_candidates,
                    },
                    "Choose the correct spine manually; local records disagree.",
                )
            )
        elif kind == "films":
            if film_id in accepted_regular_no_spine_ids:
                continue
            review.append(
                review_item(
                    "unresolved_regular_catalog_spine",
                    "P1",
                    "identity",
                    "data/criterion_catalog.json",
                    str(film.get("film_id") or ""),
                    str(film.get("title") or film.get("film_id") or ""),
                    {
                        "criterion_url": film.get("criterion_url"),
                        "criterion_id": criterion_id,
                    },
                    "Verify the Criterion spine externally before updating canonical data.",
                )
            )


def propagate_catalog_fields(
    rows: list[dict[str, Any]],
    catalog_by_film_id: dict[str, dict[str, Any]],
    file_name: str,
    changes: Counter[str],
    review: list[dict[str, Any]],
) -> None:
    for pick in rows:
        film_id = str(pick.get("film_id") or pick.get("film_slug") or "")
        catalog_entry = catalog_by_film_id.get(film_id)
        if not catalog_entry:
            review.append(
                review_item(
                    "pick_missing_catalog_entry",
                    "P0",
                    "pick_consistency",
                    file_name,
                    pick_key(pick),
                    str(pick.get("film_title") or film_id or "Untitled"),
                    {"film_id": film_id, "guest_slug": pick.get("guest_slug")},
                    "Create or correct the catalog film_id before automated repair.",
                )
            )
            continue

        catalog_spine = catalog_entry.get("spine_number")
        if has_value(catalog_spine) and pick.get("catalog_spine") != catalog_spine:
            pick["catalog_spine"] = catalog_spine
            changes[f"{file_name}:catalog_spine_from_catalog"] += 1
        elif has_value(pick.get("catalog_spine")) and not has_value(catalog_spine):
            review.append(
                review_item(
                    "pick_spine_without_catalog_spine",
                    "P1",
                    "identity",
                    file_name,
                    pick_key(pick),
                    str(pick.get("film_title") or film_id),
                    {
                        "film_id": film_id,
                        "pick_catalog_spine": pick.get("catalog_spine"),
                        "criterion_film_url": pick.get("criterion_film_url"),
                    },
                    "Backfill or verify the catalog spine before trusting the pick-level value.",
                )
            )

        catalog_title = catalog_entry.get("title")
        if catalog_title and pick.get("catalog_title") != catalog_title:
            pick["catalog_title"] = catalog_title
            changes[f"{file_name}:catalog_title_from_catalog"] += 1

        catalog_url = catalog_entry.get("criterion_url")
        if catalog_url and not pick.get("criterion_film_url"):
            pick["criterion_film_url"] = catalog_url
            changes[f"{file_name}:criterion_url_from_catalog"] += 1


def unique_pick_index(
    rows: list[dict[str, Any]],
    file_name: str,
    review: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for pick in rows:
        grouped[pick_key(pick)].append(pick)

    unique: dict[str, dict[str, Any]] = {}
    for key, matches in sorted(grouped.items()):
        if len(matches) == 1:
            unique[key] = matches[0]
            continue
        review.append(
            review_item(
                "ambiguous_duplicate_pick_key",
                "P1",
                "pick_consistency",
                file_name,
                key,
                str(matches[0].get("film_title") or key),
                {
                    "count": len(matches),
                    "guest_slug": matches[0].get("guest_slug"),
                    "film_ids": [item.get("film_id") for item in matches],
                },
                "Dedupe this guest/visit/film key before automated reconciliation.",
            )
        )
    return unique


def reconcile_raw_enriched_picks(
    picks: list[dict[str, Any]],
    picks_raw: list[dict[str, Any]],
    changes: Counter[str],
    review: list[dict[str, Any]],
) -> None:
    picks_by_key = unique_pick_index(picks, "data/picks.json", review)
    raw_by_key = unique_pick_index(picks_raw, "data/picks_raw.json", review)

    for key, pick in sorted(picks_by_key.items()):
        raw = raw_by_key.get(key)
        if not raw:
            review.append(
                review_item(
                    "enriched_pick_missing_raw_counterpart",
                    "P2",
                    "pick_consistency",
                    "data/picks.json",
                    key,
                    str(pick.get("film_title") or key),
                    {"guest_slug": pick.get("guest_slug"), "film_id": pick.get("film_id")},
                    "Confirm whether this enriched pick should still have raw source data.",
                )
            )
            continue

        for field in ["catalog_title", "criterion_film_url", "source"]:
            raw_value = raw.get(field)
            if field == "criterion_film_url":
                raw_value = raw_value or ""
                should_copy = True
            else:
                should_copy = has_value(raw_value)
            if should_copy and pick.get(field) != raw_value:
                pick[field] = raw_value
                changes[f"data/picks.json:{field}_from_raw"] += 1

    for key, raw in sorted(raw_by_key.items()):
        if key in picks_by_key:
            continue
        review.append(
            review_item(
                "raw_pick_missing_enriched_counterpart",
                "P2",
                "pick_consistency",
                "data/picks_raw.json",
                key,
                str(raw.get("film_title") or key),
                {"guest_slug": raw.get("guest_slug"), "film_id": raw.get("film_id")},
                "Confirm whether this raw pick should appear in picks.json or remain fallback-only.",
            )
        )


def infer_match_method(
    pick: dict[str, Any], catalog_entry: dict[str, Any] | None
) -> str | None:
    if not catalog_entry:
        return None

    pick_identity = criterion_identity(pick.get("criterion_film_url"))
    catalog_identity = criterion_identity(catalog_entry.get("criterion_url"))
    if pick.get("criterion_film_url") and pick.get("criterion_film_url") == catalog_entry.get(
        "criterion_url"
    ):
        return "criterion_url"
    if pick_identity and catalog_identity and pick_identity == catalog_identity:
        return "criterion_url"
    if normalize_text(pick.get("film_title")) == normalize_text(catalog_entry.get("title")):
        return "exact"
    return None


def backfill_match_methods(
    rows: list[dict[str, Any]],
    catalog_by_film_id: dict[str, dict[str, Any]],
    file_name: str,
    changes: Counter[str],
    review: list[dict[str, Any]],
) -> None:
    for pick in rows:
        if pick.get("match_method"):
            continue
        film_id = str(pick.get("film_id") or pick.get("film_slug") or "")
        method = infer_match_method(pick, catalog_by_film_id.get(film_id))
        if method:
            pick["match_method"] = method
            changes[f"{file_name}:match_method_{method}"] += 1
        else:
            review.append(
                review_item(
                    "unresolved_match_method",
                    "P2",
                    "pick_consistency",
                    file_name,
                    pick_key(pick),
                    str(pick.get("film_title") or film_id or "Untitled"),
                    {
                        "film_id": film_id,
                        "criterion_film_url": pick.get("criterion_film_url"),
                    },
                    "Review the match path before assigning match_method.",
                )
            )


def backfill_pick_order(
    rows: list[dict[str, Any]],
    file_name: str,
    changes: Counter[str],
) -> None:
    groups: dict[tuple[str, Any], list[dict[str, Any]]] = defaultdict(list)
    for pick in rows:
        groups[(str(pick.get("guest_slug") or ""), pick.get("visit_index"))].append(pick)

    for group_rows in groups.values():
        for index, pick in enumerate(group_rows, start=1):
            if pick.get("pick_order") != index:
                pick["pick_order"] = index
                changes[f"{file_name}:pick_order_from_file_order"] += 1


def repair_box_set_member_titles(
    rows: list[dict[str, Any]],
    catalog_by_film_id: dict[str, dict[str, Any]],
    world_cinema_project_counts: dict[int, int],
    file_name: str,
    changes: Counter[str],
) -> None:
    for pick in rows:
        titles = pick.get("box_set_film_titles")
        if isinstance(titles, list) and titles:
            film_id = str(pick.get("film_id") or pick.get("film_slug") or "")
            catalog_entry = catalog_by_film_id.get(film_id, {})
            self_title_keys = {
                normalize_box_set_title(value)
                for value in [
                    pick.get("film_title"),
                    pick.get("box_set_name"),
                    catalog_entry.get("title"),
                ]
                if value
            }

            cleaned_titles = [
                title
                for title in titles
                if normalize_box_set_title(title) not in self_title_keys
            ]
            removed = len(titles) - len(cleaned_titles)
            if removed:
                changes[f"{file_name}:box_set_self_titles_removed"] += removed
                if cleaned_titles:
                    pick["box_set_film_titles"] = cleaned_titles
                    if pick.get("box_set_film_count") != len(cleaned_titles):
                        pick["box_set_film_count"] = len(cleaned_titles)
                        changes[f"{file_name}:box_set_count_from_member_titles"] += 1
                else:
                    pick.pop("box_set_film_titles", None)

        volume = world_cinema_project_volume(pick.get("box_set_name") or pick.get("film_title"))
        local_count = world_cinema_project_counts.get(volume or 0)
        if local_count and pick.get("box_set_film_count") != local_count:
            pick["box_set_film_count"] = local_count
            changes[f"{file_name}:box_set_count_from_catalog_members"] += 1


def build_repair_report(
    changes: Counter[str], review: list[dict[str, Any]]
) -> dict[str, Any]:
    review = sort_review_items(review)
    return {
        "schema_version": REPAIR_SCHEMA_VERSION,
        "summary": {
            "changes": dict(sorted(changes.items())),
            "total_changes": sum(changes.values()),
            "review_items": len(review),
            "review_by_type": dict(sorted(Counter(item["type"] for item in review).items())),
            "review_by_severity": dict(
                sorted(Counter(item["severity"] for item in review).items())
            ),
        },
        "review_items": review,
    }


def repair_box_set_title_fields(
    catalog: list[dict[str, Any]],
    picks: list[dict[str, Any]],
    picks_raw: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    repaired_catalog = copy.deepcopy(catalog)
    repaired_picks = copy.deepcopy(picks)
    repaired_picks_raw = copy.deepcopy(picks_raw)
    changes: Counter[str] = Counter()
    review: list[dict[str, Any]] = []

    catalog_by_film_id = build_catalog_by_film_id(repaired_catalog)
    world_cinema_project_counts = build_world_cinema_project_member_counts(repaired_catalog)
    repair_box_set_member_titles(
        repaired_picks,
        catalog_by_film_id,
        world_cinema_project_counts,
        "data/picks.json",
        changes,
    )
    repair_box_set_member_titles(
        repaired_picks_raw,
        catalog_by_film_id,
        world_cinema_project_counts,
        "data/picks_raw.json",
        changes,
    )

    return repaired_catalog, repaired_picks, repaired_picks_raw, build_repair_report(
        changes, review
    )


def repair_data(
    catalog: list[dict[str, Any]],
    picks: list[dict[str, Any]],
    picks_raw: list[dict[str, Any]],
    accepted_regular_no_spine_ids: set[str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    repaired_catalog = copy.deepcopy(catalog)
    repaired_picks = copy.deepcopy(picks)
    repaired_picks_raw = copy.deepcopy(picks_raw)

    changes: Counter[str] = Counter()
    review: list[dict[str, Any]] = []

    candidates = build_spine_candidates(repaired_catalog, repaired_picks, repaired_picks_raw)
    repair_catalog_spines(
        repaired_catalog,
        candidates,
        changes,
        review,
        accepted_regular_no_spine_ids=accepted_regular_no_spine_ids,
    )

    catalog_by_film_id = build_catalog_by_film_id(repaired_catalog)
    propagate_catalog_fields(
        repaired_picks,
        catalog_by_film_id,
        "data/picks.json",
        changes,
        review,
    )
    propagate_catalog_fields(
        repaired_picks_raw,
        catalog_by_film_id,
        "data/picks_raw.json",
        changes,
        review,
    )
    reconcile_raw_enriched_picks(repaired_picks, repaired_picks_raw, changes, review)
    backfill_match_methods(
        repaired_picks,
        catalog_by_film_id,
        "data/picks.json",
        changes,
        review,
    )
    backfill_match_methods(
        repaired_picks_raw,
        catalog_by_film_id,
        "data/picks_raw.json",
        changes,
        review,
    )
    backfill_pick_order(repaired_picks, "data/picks.json", changes)
    backfill_pick_order(repaired_picks_raw, "data/picks_raw.json", changes)
    world_cinema_project_counts = build_world_cinema_project_member_counts(repaired_catalog)
    repair_box_set_member_titles(
        repaired_picks,
        catalog_by_film_id,
        world_cinema_project_counts,
        "data/picks.json",
        changes,
    )
    repair_box_set_member_titles(
        repaired_picks_raw,
        catalog_by_film_id,
        world_cinema_project_counts,
        "data/picks_raw.json",
        changes,
    )

    return repaired_catalog, repaired_picks, repaired_picks_raw, build_repair_report(
        changes, review
    )


def render_review_markdown(report: dict[str, Any], max_items: int = 200) -> str:
    summary = report["summary"]
    lines = [
        "# Data Quality Repair Review",
        "",
        "Deterministic local repairs were applied only where current JSON evidence was unambiguous.",
        "",
        "## Summary",
        "",
        f"- Total field changes: {summary['total_changes']}",
        f"- Records needing review: {summary['review_items']}",
        "",
        "## Changes",
        "",
    ]
    for key, count in summary["changes"].items():
        lines.append(f"- `{key}`: {count}")

    lines.extend(["", "## Review Items by Type", ""])
    for key, count in summary["review_by_type"].items():
        lines.append(f"- `{key}`: {count}")

    review_items = report["review_items"]
    lines.extend(
        [
            "",
            f"## Top Review Items (first {min(max_items, len(review_items))})",
            "",
            "| Severity | Category | Type | File | Record | Title | Suggested action |",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for item in review_items[:max_items]:
        lines.append(
            "| {severity} | {category} | `{type}` | `{file}` | `{record}` | {title} | {action} |".format(
                severity=item["severity"],
                category=item["category"],
                type=item["type"],
                file=item["file"],
                record=str(item["record_key"]).replace("|", "\\|"),
                title=str(item["title"]).replace("|", "\\|"),
                action=str(item["suggested_action"]).replace("|", "\\|"),
            )
        )

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply deterministic local data-quality repairs")
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing data")
    parser.add_argument(
        "--box-set-member-titles-only",
        action="store_true",
        help="Only repair self-referential box-set member title fields and related local counts",
    )
    parser.add_argument("--exceptions-file", type=Path, default=DEFAULT_EXCEPTIONS_FILE)
    parser.add_argument("--review-json", type=Path, default=DEFAULT_REVIEW_JSON)
    parser.add_argument("--review-markdown", type=Path, default=DEFAULT_REVIEW_MARKDOWN)
    parser.add_argument("--max-markdown-items", type=int, default=200)
    args = parser.parse_args()

    catalog = load_json(CATALOG_FILE)
    picks = load_json(PICKS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)
    exceptions = load_known_exceptions(args.exceptions_file)
    accepted_regular_no_spine_ids = accepted_film_ids_for_issue(
        exceptions,
        "catalog_regular_film_missing_spine",
    )

    if args.box_set_member_titles_only:
        repaired_catalog, repaired_picks, repaired_picks_raw, report = repair_box_set_title_fields(
            catalog,
            picks,
            picks_raw,
        )
    else:
        repaired_catalog, repaired_picks, repaired_picks_raw, report = repair_data(
            catalog,
            picks,
            picks_raw,
            accepted_regular_no_spine_ids=accepted_regular_no_spine_ids,
        )
    markdown = render_review_markdown(report, max_items=args.max_markdown_items)

    if not args.dry_run:
        save_json(CATALOG_FILE, repaired_catalog)
        save_json(PICKS_FILE, repaired_picks)
        save_json(PICKS_RAW_FILE, repaired_picks_raw)
        save_json(args.review_json, report)
        args.review_markdown.parent.mkdir(parents=True, exist_ok=True)
        args.review_markdown.write_text(markdown, encoding="utf-8")

    print("DATA QUALITY REPAIR")
    print("===================")
    print(f"Field changes: {report['summary']['total_changes']}")
    print(f"Review items: {report['summary']['review_items']}")
    for key, count in report["summary"]["changes"].items():
        print(f"{key}: {count}")
    if args.dry_run:
        print("\nDry run: no files written.")
    else:
        print(f"\nReview JSON: {args.review_json}")
        print(f"Review Markdown: {args.review_markdown}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
