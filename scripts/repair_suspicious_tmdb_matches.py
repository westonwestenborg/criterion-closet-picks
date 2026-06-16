#!/usr/bin/env python3
"""Repair locally verified suspicious TMDB/catalog identity matches."""

from __future__ import annotations

import argparse
import copy
import sys
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.utils import (
    CATALOG_FILE,
    PICKS_FILE,
    PICKS_RAW_FILE,
    VALIDATION_DIR,
    load_json,
    save_json,
)


SCHEMA_VERSION = 1
DEFAULT_REPORT_JSON = VALIDATION_DIR / "suspicious_tmdb_match_repair.json"
DEFAULT_REPORT_MARKDOWN = VALIDATION_DIR / "suspicious_tmdb_match_repair.md"

TRAFFIC_URL = "https://www.criterion.com/films/381-traffic"
THE_INNOCENT_URL = "https://www.criterion.com/films/33649-the-innocent"
THE_INNOCENTS_URL = "https://www.criterion.com/films/28569-the-innocents"
STRAW_DOGS_URL = "https://www.criterion.com/films/730-straw-dogs"
STRAY_DOG_URL = "https://www.criterion.com/films/788-stray-dog"

MOVIE_FIELDS = {
    "traffic": {
        "film_id": "traffic",
        "title": "Traffic",
        "director": "Steven Soderbergh",
        "year": 2000,
        "country": "United States",
        "spine_number": 151,
        "criterion_url": TRAFFIC_URL,
        "tmdb_id": 1900,
        "imdb_id": None,
        "genres": [],
        "poster_source": "criterion",
    },
    "the-innocents": {
        "film_id": "the-innocents",
        "title": "The Innocents",
        "director": "Jack Clayton",
        "year": 1961,
        "country": "United States",
        "spine_number": 727,
        "criterion_url": THE_INNOCENTS_URL,
        "tmdb_id": 16372,
        "imdb_id": None,
        "genres": [],
        "poster_url": "https://s3.amazonaws.com/criterion-production/films/cb71c87fc3d8226a70d5773a2ec9e48e/pY4N3dYTsGXunHjifYu4MbfAA5FKib_original.jpg",
        "poster_source": "criterion",
    },
    "the-innocent": {
        "film_id": "the-innocent",
        "title": "The Innocent",
        "director": "Louis Garrel",
        "year": 2022,
        "country": "France",
        "spine_number": None,
        "criterion_url": THE_INNOCENT_URL,
        "tmdb_id": 919573,
        "imdb_id": None,
        "genres": [],
        "poster_url": "https://s3.amazonaws.com/criterion-production/films/303e9f38f9bfbd8553006af0f8a02201/A1BOQLhJEJfs2iOBh6B18fMXpuKanW_original.jpg",
        "poster_source": "criterion",
    },
    "stray-dog": {
        "film_id": "stray-dog",
        "title": "Stray Dog",
        "director": "Akira Kurosawa",
        "year": 1949,
        "country": "Japan",
        "spine_number": 233,
        "criterion_url": STRAY_DOG_URL,
        "tmdb_id": 30368,
        "imdb_id": None,
        "genres": [],
        "poster_url": "https://s3.amazonaws.com/criterion-production/films/7b8b89e66c85b8bc543d76173f49717f/KOW5xpKfZGHlt86Ep17hYF1ORXvSce_original.jpg",
        "poster_source": "criterion",
    },
    "straw-dogs": {
        "film_id": "straw-dogs",
        "title": "Straw Dogs",
        "director": "Sam Peckinpah",
        "year": 1971,
        "country": "United States",
        "spine_number": 182,
        "criterion_url": STRAW_DOGS_URL,
        "tmdb_id": 994,
        "imdb_id": None,
        "genres": [],
        "poster_url": "https://s3.amazonaws.com/criterion-production/films/beca22ceb97188c09bd8ef6090c6bca7/XQmMDjG2VLEmmHY6Y6TslMJBOi1w2p_original.jpg",
        "poster_source": "criterion",
    },
}

CHE_FIELDS = {
    "tmdb_id": None,
    "imdb_id": None,
    "genres": ["Drama", "History", "War"],
}

STALE_TMDB_FIELDS = {"credits", "tmdb_type", "tmdb_url", "letterboxd_url"}
STRAW_DOGS_GUESTS = {
    "ari-aster",
    "jason-schwartzman",
    "kathryn-bigelow",
    "ron-shelton",
}


def add_action(
    actions: list[dict[str, Any]],
    changes: Counter[str],
    action_type: str,
    file_name: str,
    record_key: str,
    title: str,
    evidence: dict[str, Any],
) -> None:
    changes[f"{file_name}:{action_type}"] += 1
    actions.append(
        {
            "type": action_type,
            "file": file_name,
            "record_key": record_key,
            "title": title,
            "evidence": evidence,
        }
    )


def update_fields(
    row: dict[str, Any],
    fields: dict[str, Any],
    file_name: str,
    record_key: str,
    actions: list[dict[str, Any]],
    changes: Counter[str],
) -> None:
    changed: dict[str, dict[str, Any]] = {}
    for key, value in fields.items():
        if row.get(key) != value:
            changed[key] = {"old": row.get(key), "new": value}
            row[key] = value
    for field in sorted(STALE_TMDB_FIELDS):
        if field in row:
            changed[field] = {"old": row.get(field), "new": None}
            row.pop(field, None)
    if changed:
        add_action(
            actions,
            changes,
            "catalog_identity_corrected",
            file_name,
            record_key,
            str(row.get("title") or record_key),
            changed,
        )


def ensure_catalog_row(
    catalog: list[dict[str, Any]],
    template: dict[str, Any],
    insert_after: str,
    actions: list[dict[str, Any]],
    changes: Counter[str],
) -> None:
    if any(row.get("film_id") == template["film_id"] for row in catalog):
        return
    insert_index = len(catalog)
    for index, row in enumerate(catalog):
        if row.get("film_id") == insert_after:
            insert_index = index + 1
            break
    catalog.insert(insert_index, copy.deepcopy(template))
    add_action(
        actions,
        changes,
        "catalog_row_created",
        "data/criterion_catalog.json",
        template["film_id"],
        template["title"],
        {"insert_after": insert_after, "criterion_url": template.get("criterion_url")},
    )


def repair_catalog(catalog: list[dict[str, Any]], actions: list[dict[str, Any]], changes: Counter[str]) -> list[dict[str, Any]]:
    repaired = copy.deepcopy(catalog)
    for row in repaired:
        film_id = str(row.get("film_id") or "")
        if film_id == "trafic" and row.get("criterion_url") == TRAFFIC_URL:
            update_fields(row, MOVIE_FIELDS["traffic"], "data/criterion_catalog.json", "trafic", actions, changes)
        elif film_id == "the-innocents":
            update_fields(row, MOVIE_FIELDS["the-innocents"], "data/criterion_catalog.json", film_id, actions, changes)
        elif film_id == "stray-dog":
            update_fields(row, MOVIE_FIELDS["stray-dog"], "data/criterion_catalog.json", film_id, actions, changes)
        elif film_id == "che" and row.get("tmdb_id"):
            update_fields(row, CHE_FIELDS, "data/criterion_catalog.json", film_id, actions, changes)

    ensure_catalog_row(repaired, MOVIE_FIELDS["the-innocent"], "the-innocents", actions, changes)
    ensure_catalog_row(repaired, MOVIE_FIELDS["straw-dogs"], "stray-dog", actions, changes)
    return repaired


def pick_target(pick: dict[str, Any]) -> dict[str, Any] | None:
    film_id = str(pick.get("film_id") or "")
    url = str(pick.get("criterion_film_url") or "")
    guest_slug = str(pick.get("guest_slug") or "")

    if film_id == "trafic" and url == TRAFFIC_URL:
        return MOVIE_FIELDS["traffic"]
    if film_id == "the-innocents" and url == THE_INNOCENT_URL:
        return MOVIE_FIELDS["the-innocent"]
    if film_id == "the-innocents" and url == THE_INNOCENTS_URL:
        return MOVIE_FIELDS["the-innocents"]
    if film_id == "stray-dog" and guest_slug in STRAW_DOGS_GUESTS:
        return MOVIE_FIELDS["straw-dogs"]
    if film_id == "stray-dog":
        return MOVIE_FIELDS["stray-dog"]
    return None


def update_pick(
    pick: dict[str, Any],
    target: dict[str, Any],
    file_name: str,
    actions: list[dict[str, Any]],
    changes: Counter[str],
) -> None:
    changed: dict[str, dict[str, Any]] = {}
    pick_fields = {
        "film_id": target["film_id"],
        "film_title": target["title"],
        "catalog_title": target["title"],
        "catalog_spine": target["spine_number"],
        "criterion_film_url": target["criterion_url"],
    }
    if pick.get("film_slug") == pick.get("film_id"):
        pick_fields["film_slug"] = target["film_id"]
    for key, value in pick_fields.items():
        if pick.get(key) != value:
            changed[key] = {"old": pick.get(key), "new": value}
            pick[key] = value
    if target.get("criterion_url") and pick.get("match_method") != "criterion_url":
        changed["match_method"] = {"old": pick.get("match_method"), "new": "criterion_url"}
        pick["match_method"] = "criterion_url"
    if changed:
        record_key = "|".join(
            [
                str(pick.get("guest_slug") or ""),
                str(pick.get("visit_index") or ""),
                str(pick.get("film_id") or ""),
            ]
        )
        add_action(
            actions,
            changes,
            "pick_identity_corrected",
            file_name,
            record_key,
            str(pick.get("film_title") or record_key),
            changed,
        )


def repair_pick_rows(
    rows: list[dict[str, Any]],
    file_name: str,
    actions: list[dict[str, Any]],
    changes: Counter[str],
) -> list[dict[str, Any]]:
    repaired = copy.deepcopy(rows)
    for pick in repaired:
        target = pick_target(pick)
        if target:
            update_pick(pick, target, file_name, actions, changes)
    return repaired


def repair_suspicious_tmdb_matches(
    catalog: list[dict[str, Any]],
    picks: list[dict[str, Any]],
    picks_raw: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    changes: Counter[str] = Counter()
    actions: list[dict[str, Any]] = []

    repaired_catalog = repair_catalog(catalog, actions, changes)
    repaired_picks = repair_pick_rows(picks, "data/picks.json", actions, changes)
    repaired_raw = repair_pick_rows(picks_raw, "data/picks_raw.json", actions, changes)

    actions = sorted(actions, key=lambda item: (item["file"], item["record_key"], item["type"]))
    report = {
        "schema_version": SCHEMA_VERSION,
        "summary": {
            "changes": dict(sorted(changes.items())),
            "total_changes": sum(changes.values()),
            "actions": len(actions),
        },
        "actions": actions,
    }
    return repaired_catalog, repaired_picks, repaired_raw, report


def render_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# Suspicious TMDB Match Repair",
        "",
        "Deterministic repairs for locally verified catalog identity splits and stale TMDB matches.",
        "",
        "## Summary",
        "",
        f"- Total changes: {summary['total_changes']}",
        f"- Actions: {summary['actions']}",
        "",
        "## Changes by Type",
        "",
    ]
    if summary["changes"]:
        for key, count in summary["changes"].items():
            lines.append(f"- `{key}`: {count}")
    else:
        lines.append("- None")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair suspicious TMDB/catalog identity matches")
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing data")
    parser.add_argument("--report-json", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--report-markdown", type=Path, default=DEFAULT_REPORT_MARKDOWN)
    args = parser.parse_args()

    catalog = load_json(CATALOG_FILE)
    picks = load_json(PICKS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)
    repaired_catalog, repaired_picks, repaired_raw, report = repair_suspicious_tmdb_matches(
        catalog,
        picks,
        picks_raw,
    )
    markdown = render_markdown(report)

    if not args.dry_run:
        save_json(CATALOG_FILE, repaired_catalog)
        save_json(PICKS_FILE, repaired_picks)
        save_json(PICKS_RAW_FILE, repaired_raw)
        save_json(args.report_json, report)
        args.report_markdown.parent.mkdir(parents=True, exist_ok=True)
        args.report_markdown.write_text(markdown, encoding="utf-8")

    print("SUSPICIOUS TMDB MATCH REPAIR")
    print("============================")
    print(f"Total changes: {report['summary']['total_changes']}")
    print(f"Actions: {report['summary']['actions']}")
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
