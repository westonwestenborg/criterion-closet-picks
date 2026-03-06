#!/usr/bin/env python3
"""
Audit TMDB data quality in the catalog.

Checks for director mismatches, duplicate TMDB IDs, missing TMDB IDs,
and missing posters. Generates a report for manual review.

Usage:
  python scripts/audit_tmdb.py --dry-run   # Check catalog data only (no scraping)
  python scripts/audit_tmdb.py --full       # Full audit with Criterion page scraping
"""

import argparse
import sys
from collections import Counter

from thefuzz import fuzz
from tqdm import tqdm

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    PICKS_FILE,
    PICKS_RAW_FILE,
    VALIDATION_DIR,
    load_json,
    save_json,
    log,
)
from scripts.enrich_tmdb import (
    get_metadata_from_criterion_url,
    build_criterion_url_lookup,
)


def find_duplicate_tmdb_ids(catalog: list[dict]) -> list[dict]:
    """
    Find non-box-set films sharing the same tmdb_id.
    Returns list of {"tmdb_id": int, "films": [{"film_id", "title", "spine_number"}]}.
    """
    tmdb_to_films: dict[int, list[dict]] = {}

    for film in catalog:
        tmdb_id = film.get("tmdb_id")
        if not tmdb_id or film.get("is_box_set"):
            continue
        tmdb_to_films.setdefault(tmdb_id, []).append({
            "film_id": film.get("film_id", ""),
            "title": film.get("title", ""),
            "spine_number": film.get("spine_number", ""),
        })

    return [
        {"tmdb_id": tmdb_id, "films": films}
        for tmdb_id, films in sorted(tmdb_to_films.items())
        if len(films) > 1
    ]


def find_missing_tmdb(catalog: list[dict], picked_film_ids: set[str]) -> list[dict]:
    """
    Find non-box-set films in picks that have no tmdb_id.
    Returns list of {"film_id", "title", "criterion_url"}.
    """
    results = []
    for film in catalog:
        if film.get("is_box_set"):
            continue
        if film.get("tmdb_id"):
            continue
        film_id = film.get("film_id", "")
        if film_id not in picked_film_ids:
            continue
        results.append({
            "film_id": film_id,
            "title": film.get("title", ""),
            "criterion_url": film.get("criterion_url") or None,
        })
    return results


def find_missing_posters(catalog: list[dict], picked_film_ids: set[str]) -> list[dict]:
    """
    Find picked films without a poster_url.
    Returns list of {"film_id", "title", "has_criterion_url"}.
    """
    results = []
    for film in catalog:
        film_id = film.get("film_id", "")
        if film_id not in picked_film_ids:
            continue
        if film.get("poster_url"):
            continue
        results.append({
            "film_id": film_id,
            "title": film.get("title", ""),
            "has_criterion_url": bool(film.get("criterion_url")),
        })
    return results


def find_director_mismatches(
    catalog: list[dict], criterion_url_lookup: dict[str, str]
) -> list[dict]:
    """
    Compare Criterion director vs TMDB director for films with both.
    Uses fuzzy matching — flags pairs with ratio < 75.
    Returns list of {"film_id", "title", "criterion_director", "tmdb_director", "score"}.
    """
    candidates = []
    for film in catalog:
        if not film.get("tmdb_id") or film.get("is_box_set"):
            continue
        criterion_url = film.get("criterion_url")
        if not criterion_url:
            criterion_url = criterion_url_lookup.get(film.get("film_id", ""))
        if not criterion_url:
            continue

        # Get TMDB director
        tmdb_director = film.get("director") or ""
        if not tmdb_director:
            credits = film.get("credits") or {}
            directors = credits.get("directors") or []
            if directors:
                tmdb_director = directors[0].get("name", "")
        if not tmdb_director:
            continue

        candidates.append((film, criterion_url, tmdb_director))

    mismatches = []
    for film, criterion_url, tmdb_director in tqdm(
        candidates, desc="Checking directors", unit="film"
    ):
        metadata = get_metadata_from_criterion_url(criterion_url)
        if not metadata:
            continue
        criterion_director = metadata.get("director") or ""
        if not criterion_director:
            continue

        score = fuzz.ratio(criterion_director.lower(), tmdb_director.lower())
        if score < 75:
            mismatches.append({
                "film_id": film.get("film_id", ""),
                "title": film.get("title", ""),
                "criterion_director": criterion_director,
                "tmdb_director": tmdb_director,
                "score": score,
            })

    return mismatches


def main():
    parser = argparse.ArgumentParser(description="Audit TMDB data quality in catalog")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Check catalog data only, no Criterion scraping (default)",
    )
    mode.add_argument(
        "--full",
        action="store_true",
        help="Full audit including Criterion page scraping for director validation",
    )
    args = parser.parse_args()

    # Load data
    catalog = load_json(CATALOG_FILE)
    picks = load_json(PICKS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)

    log(f"Loaded {len(catalog)} catalog entries, {len(picks)} picks, {len(picks_raw)} raw picks")

    # Build picked film IDs (union of film_id and film_slug fields)
    picked_film_ids: set[str] = set()
    for p in picks:
        fid = p.get("film_id")
        if fid:
            picked_film_ids.add(fid)
        slug = p.get("film_slug")
        if slug:
            picked_film_ids.add(slug)

    criterion_url_lookup = build_criterion_url_lookup(picks_raw)

    log(f"Picked films: {len(picked_film_ids)}, Criterion URL lookup: {len(criterion_url_lookup)}")

    # --- Run checks ---

    duplicates = find_duplicate_tmdb_ids(catalog)
    missing_tmdb = find_missing_tmdb(catalog, picked_film_ids)
    missing_posters = find_missing_posters(catalog, picked_film_ids)
    director_mismatches = []

    if args.full:
        director_mismatches = find_director_mismatches(catalog, criterion_url_lookup)

    # --- Console report ---

    print("\nTMDB AUDIT REPORT")
    print("=================\n")

    print(f"Duplicate TMDB IDs: {len(duplicates)}")
    for dup in duplicates:
        film_ids = ", ".join(f["film_id"] for f in dup["films"])
        print(f"  tmdb_id {dup['tmdb_id']}: {film_ids}")

    print(f"\nMissing TMDB ID (picked films): {len(missing_tmdb)}")
    for entry in missing_tmdb:
        has_url = "yes" if entry["criterion_url"] else "no"
        print(f'  {entry["film_id"]}: "{entry["title"]}" (has criterion URL: {has_url})')

    print(f"\nMissing Poster (picked films): {len(missing_posters)}")
    for entry in missing_posters:
        print(f'  {entry["film_id"]}: "{entry["title"]}"')

    if args.full:
        print(f"\nDirector Mismatches: {len(director_mismatches)}")
        for entry in director_mismatches:
            print(
                f'  {entry["film_id"]}: "{entry["title"]}" '
                f'-- Criterion: "{entry["criterion_director"]}" '
                f'vs TMDB: "{entry["tmdb_director"]}" (score: {entry["score"]})'
            )

    # --- Save report ---

    report = {
        "duplicate_tmdb_ids": duplicates,
        "missing_tmdb_id": missing_tmdb,
        "missing_posters": missing_posters,
        "director_mismatches": director_mismatches,
        "summary": {
            "duplicate_tmdb_ids": len(duplicates),
            "missing_tmdb_id": len(missing_tmdb),
            "missing_posters": len(missing_posters),
            "director_mismatches": len(director_mismatches),
            "full_audit": args.full,
        },
    }

    report_path = VALIDATION_DIR / "tmdb_audit.json"
    save_json(report_path, report)
    log(f"\nReport saved to {report_path}")


if __name__ == "__main__":
    main()
