#!/usr/bin/env python3
"""Backfill audited film metadata from verified TMDB matches."""

from __future__ import annotations

import argparse
import copy
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.enrich_tmdb import TMDBClient, TMDB_IMAGE_BASE
from scripts.utils import CATALOG_FILE, DATA_DIR, VALIDATION_DIR, load_json, save_json


SCHEMA_VERSION = 1
DEFAULT_AUDIT_JSON = VALIDATION_DIR / "data_quality_audit.json"
DEFAULT_REPORT_JSON = VALIDATION_DIR / "film_metadata_backfill.json"
DEFAULT_REPORT_MARKDOWN = VALIDATION_DIR / "film_metadata_backfill.md"

TRUST_EXISTING_TMDB_IDS = {
    "1984",
    "blind-chance",
    "cold-war",
    "dont-look-back",
    "killer-of-sheep-1978",
    "king-lear-1987",
    "mirror",
    "scarface-1932",
    "scorsese-shorts",
    "the-umbrellas-of-cherbourg-jacques-demy-box",
    "the-wind-will-carry-us-1999",
    "martha-graham-dance-on-film",
}

VERIFIED_TV_TMDB_IDS = {
    "tanner-88": 1804,
    "the-underground-railroad": 80039,
}

VERIFIED_TV_DIRECTORS = {
    "tanner-88": "Robert Altman",
    "the-underground-railroad": "Barry Jenkins",
}

VERIFIED_FIELD_BACKFILLS = {
    "les-blank-always-for-pleasure": {
        "director": "Les Blank",
    },
}

OVERWRITE_FIELDS_AFTER_TMDB_CORRECTION = {
    "credits",
    "director",
    "genres",
    "imdb_id",
    "poster_url",
    "year",
}


def has_value(value: Any) -> bool:
    return value not in (None, "", [])


def audited_film_fields(issues: list[dict[str, Any]]) -> dict[str, set[str]]:
    fields_by_film: dict[str, set[str]] = defaultdict(set)
    for issue in issues:
        if issue.get("status") != "open" or issue.get("category") != "film_metadata":
            continue
        film_id = str(issue.get("record_key") or "")
        field = issue.get("evidence", {}).get("field")
        if film_id and field:
            fields_by_film[film_id].add(str(field))
    return dict(sorted(fields_by_film.items()))


def movie_metadata(client: TMDBClient, tmdb_id: int) -> dict[str, Any] | None:
    details = client._get(f"/movie/{tmdb_id}")
    if not details:
        return None

    external_ids = client.get_movie_external_ids(tmdb_id) or {}
    credits_data = client.get_movie_credits(tmdb_id) or {}
    crew = credits_data.get("crew", [])
    cast = credits_data.get("cast", [])

    def crew_by_job(*jobs: str) -> list[dict[str, Any]]:
        return [
            {"name": item["name"], "tmdb_id": item["id"]}
            for item in crew
            if item.get("job") in jobs and item.get("name") and item.get("id")
        ]

    release_date = details.get("release_date") or ""
    year = int(release_date[:4]) if len(release_date) >= 4 and release_date[:4].isdigit() else None
    poster_path = details.get("poster_path")
    directors = crew_by_job("Director")

    return {
        "tmdb_id": tmdb_id,
        "poster_url": f"{TMDB_IMAGE_BASE}/w185{poster_path}" if poster_path else None,
        "genres": [genre["name"] for genre in details.get("genres", []) if genre.get("name")],
        "imdb_id": external_ids.get("imdb_id"),
        "year": year,
        "director": directors[0]["name"] if directors else None,
        "credits": {
            "directors": directors,
            "writers": crew_by_job("Writer", "Screenplay"),
            "cinematographers": crew_by_job("Director of Photography"),
            "editors": crew_by_job("Editor"),
            "cast": [
                {"name": item["name"], "tmdb_id": item["id"], "character": item.get("character", "")}
                for item in cast[:8]
                if item.get("name") and item.get("id")
            ],
        },
    }


def tv_metadata(client: TMDBClient, tmdb_id: int) -> dict[str, Any] | None:
    details = client.get_tv_details(tmdb_id)
    if not details:
        return None

    external_ids = client.get_tv_external_ids(tmdb_id) or {}
    credits_data = client.get_tv_credits(tmdb_id) or {}
    cast = credits_data.get("cast", [])
    creators = [
        {"name": item["name"], "tmdb_id": item["id"]}
        for item in details.get("created_by", [])
        if item.get("name") and item.get("id")
    ]
    first_air_date = details.get("first_air_date") or ""
    year = int(first_air_date[:4]) if len(first_air_date) >= 4 and first_air_date[:4].isdigit() else None
    poster_path = details.get("poster_path")

    return {
        "tmdb_id": tmdb_id,
        "tmdb_type": "tv",
        "poster_url": f"{TMDB_IMAGE_BASE}/w185{poster_path}" if poster_path else None,
        "genres": [genre["name"] for genre in details.get("genres", []) if genre.get("name")],
        "imdb_id": external_ids.get("imdb_id"),
        "year": year,
        "director": creators[0]["name"] if creators else None,
        "credits": {
            "directors": creators,
            "writers": creators,
            "cinematographers": [],
            "editors": [],
            "cast": [
                {"name": item["name"], "tmdb_id": item["id"], "character": item.get("roles", [{}])[0].get("character", "")}
                for item in cast[:8]
                if item.get("name") and item.get("id")
            ],
        },
    }


def add_action(
    actions: list[dict[str, Any]],
    changes: Counter[str],
    action_type: str,
    film: dict[str, Any],
    evidence: dict[str, Any],
) -> None:
    changes[f"data/criterion_catalog.json:{action_type}"] += 1
    actions.append(
        {
            "type": action_type,
            "file": "data/criterion_catalog.json",
            "record_key": str(film.get("film_id") or ""),
            "title": str(film.get("title") or film.get("film_id") or ""),
            "evidence": evidence,
        }
    )


def add_review(
    review: list[dict[str, Any]],
    review_type: str,
    film: dict[str, Any] | None,
    film_id: str,
    evidence: dict[str, Any],
    suggested_action: str,
) -> None:
    review.append(
        {
            "type": review_type,
            "file": "data/criterion_catalog.json",
            "record_key": film_id,
            "title": str((film or {}).get("title") or film_id),
            "evidence": evidence,
            "suggested_action": suggested_action,
        }
    )


def apply_metadata(
    film: dict[str, Any],
    metadata: dict[str, Any],
    fields: set[str],
    overwrite: bool,
) -> dict[str, Any]:
    changed: dict[str, Any] = {}
    for field in sorted(fields):
        value = metadata.get(field)
        if not has_value(value):
            continue
        if overwrite or not has_value(film.get(field)):
            if film.get(field) != value:
                film[field] = value
                changed[field] = value
    return changed


def backfill_film_metadata(
    catalog: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    client: Any,
    tmdb_overrides: dict[str, int],
    trusted_existing_ids: set[str] | None = None,
    field_backfills: dict[str, dict[str, Any]] | None = None,
    verified_tv_ids: dict[str, int] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    trusted_existing_ids = TRUST_EXISTING_TMDB_IDS if trusted_existing_ids is None else trusted_existing_ids
    field_backfills = VERIFIED_FIELD_BACKFILLS if field_backfills is None else field_backfills
    verified_tv_ids = VERIFIED_TV_TMDB_IDS if verified_tv_ids is None else verified_tv_ids
    repaired = copy.deepcopy(catalog)
    by_film_id = {str(film.get("film_id") or ""): film for film in repaired}
    target_fields = audited_film_fields(issues)
    changes: Counter[str] = Counter()
    actions: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []
    metadata_cache: dict[tuple[str, int], dict[str, Any] | None] = {}

    for film_id, fields in target_fields.items():
        film = by_film_id.get(film_id)
        if not film:
            add_review(
                review,
                "film_metadata_target_missing_catalog_entry",
                None,
                film_id,
                {"fields": sorted(fields)},
                "Create the catalog entry before backfilling film metadata.",
            )
            continue

        direct_fields = field_backfills.get(film_id, {})
        direct_changed = apply_metadata(film, direct_fields, fields, overwrite=False)
        for field, value in direct_changed.items():
            add_action(
                actions,
                changes,
                f"{field}_backfilled",
                film,
                {"source": "verified_field_backfill", field: value},
            )
        fields = {field for field in fields if not has_value(film.get(field))}
        if not fields:
            continue

        tv_id = verified_tv_ids.get(film_id)
        override_id = tmdb_overrides.get(film_id)
        is_tv = tv_id is not None or film.get("tmdb_type") == "tv"
        is_override = override_id is not None or tv_id is not None
        tmdb_id = tv_id or override_id or film.get("tmdb_id")
        if not tmdb_id:
            add_review(
                review,
                "film_metadata_missing_verified_tmdb_id",
                film,
                film_id,
                {"fields": sorted(fields)},
                "Find a verified TMDB match before backfilling this film.",
            )
            continue

        if not is_override and film_id not in trusted_existing_ids:
            add_review(
                review,
                "film_metadata_not_auto_repaired",
                film,
                film_id,
                {"tmdb_id": tmdb_id, "fields": sorted(fields)},
                "Review the current TMDB match before using it to fill missing metadata.",
            )
            continue

        tmdb_id = int(tmdb_id)
        cache_key = ("tv" if is_tv else "movie", tmdb_id)
        if cache_key not in metadata_cache:
            metadata_cache[cache_key] = tv_metadata(client, tmdb_id) if is_tv else movie_metadata(client, tmdb_id)
        metadata = metadata_cache[cache_key]
        if not metadata:
            add_review(
                review,
                "film_metadata_tmdb_fetch_failed",
                film,
                film_id,
                {"tmdb_id": tmdb_id, "fields": sorted(fields)},
                "Retry TMDB metadata lookup or review the TMDB ID manually.",
            )
            continue
        if is_tv and VERIFIED_TV_DIRECTORS.get(film_id):
            metadata = {**metadata, "director": VERIFIED_TV_DIRECTORS[film_id]}

        if is_override and film.get("tmdb_id") != tmdb_id:
            old_tmdb_id = film.get("tmdb_id")
            film["tmdb_id"] = tmdb_id
            add_action(
                actions,
                changes,
                "tmdb_id_corrected",
                film,
                {"old_tmdb_id": old_tmdb_id, "new_tmdb_id": tmdb_id},
            )
        if is_tv and film.get("tmdb_type") != "tv":
            old_tmdb_type = film.get("tmdb_type")
            film["tmdb_type"] = "tv"
            add_action(
                actions,
                changes,
                "tmdb_type_corrected",
                film,
                {"old_tmdb_type": old_tmdb_type, "new_tmdb_type": "tv"},
            )

        fields_to_apply = set(fields)
        if is_override:
            fields_to_apply |= OVERWRITE_FIELDS_AFTER_TMDB_CORRECTION
        if is_tv:
            fields_to_apply.add("tmdb_type")
        changed = apply_metadata(film, metadata, fields_to_apply, overwrite=is_override)
        for field, value in changed.items():
            add_action(
                actions,
                changes,
                f"{field}_backfilled",
                film,
                {"tmdb_id": tmdb_id, field: value},
            )

        missing_after = [
            field
            for field in sorted(fields)
            if not has_value(film.get(field))
        ]
        for field in missing_after:
            add_review(
                review,
                "film_metadata_field_unavailable",
                film,
                film_id,
                {"tmdb_id": tmdb_id, "field": field},
                "Review this field manually or accept it as unavailable.",
            )

    actions = sorted(actions, key=lambda item: (item["record_key"], item["type"]))
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
        "# Film Metadata Backfill",
        "",
        "Backfilled audited film metadata from verified TMDB matches.",
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

    if report["review_items"]:
        lines.extend(["", "## Review Items", ""])
        for key, count in summary["review_by_type"].items():
            lines.append(f"- `{key}`: {count}")

    return "\n".join(lines) + "\n"


def load_tmdb_overrides(path: Path = DATA_DIR / "tmdb_corrections.json") -> dict[str, int]:
    if not path.exists():
        return {}
    corrections = load_json(path)
    return {
        str(film_id): int(data["tmdb_id"])
        for film_id, data in corrections.items()
        if data.get("tmdb_id")
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill audited film metadata")
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing data")
    parser.add_argument("--audit-json", type=Path, default=DEFAULT_AUDIT_JSON)
    parser.add_argument("--report-json", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--report-markdown", type=Path, default=DEFAULT_REPORT_MARKDOWN)
    args = parser.parse_args()

    catalog = load_json(CATALOG_FILE)
    audit = load_json(args.audit_json)
    issues = audit.get("issues", []) if isinstance(audit, dict) else []
    repaired_catalog, report = backfill_film_metadata(
        catalog,
        issues,
        TMDBClient(),
        load_tmdb_overrides(),
    )
    markdown = render_markdown(report)

    if not args.dry_run:
        save_json(CATALOG_FILE, repaired_catalog)
        save_json(args.report_json, report)
        args.report_markdown.parent.mkdir(parents=True, exist_ok=True)
        args.report_markdown.write_text(markdown, encoding="utf-8")

    print("FILM METADATA BACKFILL")
    print("======================")
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
