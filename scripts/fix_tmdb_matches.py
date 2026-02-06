#!/usr/bin/env python3
"""
One-time script to find and fix wrong TMDB matches in the catalog.

Compares TMDB-enriched year against the year from the Criterion film page.
Films with a mismatch > 2 years get their TMDB data cleared so that
enrich_tmdb.py can re-enrich them (now with Criterion year disambiguation).

Usage:
  python scripts/fix_tmdb_matches.py --dry-run   # Preview mismatches
  python scripts/fix_tmdb_matches.py              # Fix them
"""

import argparse
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    PICKS_RAW_FILE,
    load_json,
    save_json,
    log,
)
from scripts.enrich_tmdb import (
    get_year_from_criterion_url,
    build_criterion_url_lookup,
)


TMDB_FIELDS_TO_CLEAR = [
    "tmdb_id",
    "year",
    "director",
    "poster_url",
    "imdb_id",
    "genres",
    "credits",
]


def find_mismatches(catalog: list[dict], criterion_url_lookup: dict) -> list[dict]:
    """
    Find catalog entries where the TMDB year doesn't match the Criterion page year.
    Returns list of {film, criterion_year, tmdb_year, criterion_url}.
    """
    mismatches = []

    # Only check films that have TMDB data and a criterion URL
    candidates = []
    for film in catalog:
        if not film.get("tmdb_id"):
            continue
        criterion_url = film.get("criterion_url")
        if not criterion_url:
            criterion_url = criterion_url_lookup.get(film.get("film_id", ""))
        if not criterion_url:
            continue
        candidates.append((film, criterion_url))

    log(f"Checking {len(candidates)} films with both TMDB data and criterion URLs...")

    for film, criterion_url in candidates:
        criterion_year = get_year_from_criterion_url(criterion_url)
        if not criterion_year:
            continue

        tmdb_year = film.get("year")
        if not tmdb_year:
            continue

        if abs(criterion_year - tmdb_year) > 2:
            mismatches.append({
                "film": film,
                "criterion_year": criterion_year,
                "tmdb_year": tmdb_year,
                "criterion_url": criterion_url,
            })

    return mismatches


def clear_tmdb_data(film: dict) -> None:
    """Clear TMDB-derived fields from a film entry."""
    for field in TMDB_FIELDS_TO_CLEAR:
        if field in ("genres",):
            film[field] = []
        elif field in ("credits",):
            film[field] = None
        else:
            film[field] = None


def main():
    parser = argparse.ArgumentParser(description="Find and fix wrong TMDB matches")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only show mismatches, don't fix them",
    )
    args = parser.parse_args()

    catalog = load_json(CATALOG_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)
    criterion_url_lookup = build_criterion_url_lookup(picks_raw)

    log(f"Loaded {len(catalog)} catalog entries, {len(criterion_url_lookup)} criterion URLs")

    mismatches = find_mismatches(catalog, criterion_url_lookup)

    if not mismatches:
        log("No TMDB mismatches found!")
        return

    log(f"\nFound {len(mismatches)} mismatches:")
    for mm in mismatches:
        film = mm["film"]
        log(
            f"  {film['title']} (spine {film.get('spine_number')}): "
            f"TMDB year={mm['tmdb_year']}, Criterion year={mm['criterion_year']}, "
            f"director={film.get('director', '?')}"
        )

    if args.dry_run:
        log("\nDry run — no changes made. Run without --dry-run to fix.")
        return

    # Clear TMDB data for mismatched films
    for mm in mismatches:
        film = mm["film"]
        log(f"  Clearing TMDB data for: {film['title']}")
        clear_tmdb_data(film)
        # Set the correct year from Criterion
        film["year"] = mm["criterion_year"]

    save_json(CATALOG_FILE, catalog)
    log(f"\nCleared TMDB data for {len(mismatches)} films")
    log("Run 'python scripts/enrich_tmdb.py' to re-enrich with correct disambiguation")


if __name__ == "__main__":
    main()
