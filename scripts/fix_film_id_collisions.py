#!/usr/bin/env python3
"""
One-time migration: Fix film_id collisions caused by make_film_id() generating
identical slugs for different films with the same title but different years.

Five collision pairs exist where Digital Bits had no year data, so both entries
got the same film_id (title-only slug). TMDB enrichment then overwrote one
entry's metadata onto both, cross-contaminating the catalog.

Corrections are hard-coded from verified Criterion.com URLs and spine numbers.

Re-runnable: safe to run multiple times (idempotent).

Usage:
  python scripts/fix_film_id_collisions.py --dry-run   # Preview changes
  python scripts/fix_film_id_collisions.py              # Apply fixes
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    PICKS_FILE,
    PICKS_RAW_FILE,
    load_json,
    save_json,
    log,
)

# ---------------------------------------------------------------------------
# Verified corrections: spine -> correct metadata
# ---------------------------------------------------------------------------
# Sources: Criterion.com film pages, Digital Bits spine list, picks_raw criterion_film_urls

SPINE_CORRECTIONS = {
    # Roma
    848: {
        "film_id": "roma-1972",
        "title": "Roma",
        "year": 1972,
        "director": "Federico Fellini",
        "criterion_url": "https://www.criterion.com/films/28039-roma",
    },
    1014: {
        "film_id": "roma-2018",
        "title": "Roma",
        "year": 2018,
        "director": "Alfonso Cuarón",
        "criterion_url": "https://www.criterion.com/films/30124-roma",
    },
    # Weekend
    622: {
        "film_id": "weekend-1967",
        "title": "Weekend",
        "year": 1967,
        "director": "Jean-Luc Godard",
        "criterion_url": "https://www.criterion.com/films/28441-weekend",
    },
    635: {
        "film_id": "weekend-2011",
        "title": "Weekend",
        "year": 2011,
        "director": "Andrew Haigh",
        "criterion_url": "https://www.criterion.com/films/27783-weekend",
    },
    # Something Wild
    563: {
        "film_id": "something-wild-1961",
        "title": "Something Wild",
        "year": 1961,
        "director": "Jack Garfein",
        "criterion_url": "https://www.criterion.com/films/28777-something-wild",
    },
    850: {
        "film_id": "something-wild-1986",
        "title": "Something Wild",
        "year": 1986,
        "director": "Jonathan Demme",
        "criterion_url": "https://www.criterion.com/films/27603-something-wild",
    },
    # Bergman Island
    477: {
        "film_id": "bergman-island-2006",
        "title": "Bergman Island",
        "year": 2006,
        "director": "Marie Nyreröd",
        "criterion_url": "https://www.criterion.com/films/556-bergman-island",
    },
    1170: {
        "film_id": "bergman-island-2021",
        "title": "Bergman Island",
        "year": 2021,
        "director": "Mia Hansen-Løve",
        "criterion_url": "https://www.criterion.com/films/33380-bergman-island",
    },
    # Nightmare Alley (no picks, just catalog dedup)
    1078: {
        "film_id": "nightmare-alley-1947",
        "title": "Nightmare Alley",
        "year": 1947,
        "director": "Edmund Goulding",
        "criterion_url": "",
    },
    1286: {
        "film_id": "nightmare-alley-2021",
        "title": "Nightmare Alley",
        "year": 2021,
        "director": "Guillermo del Toro",
        "criterion_url": "",
    },
}

# Criterion film URL -> correct spine and film_id
# Used to fix picks_raw and picks entries
URL_TO_CORRECTION = {
    "https://www.criterion.com/films/28039-roma": (848, "roma-1972"),
    "https://www.criterion.com/films/30124-roma": (1014, "roma-2018"),
    "https://www.criterion.com/films/27783-weekend": (635, "weekend-2011"),
    "https://www.criterion.com/films/28441-weekend": (622, "weekend-1967"),
    "https://www.criterion.com/films/27603-something-wild": (850, "something-wild-1986"),
    "https://www.criterion.com/films/28777-something-wild": (563, "something-wild-1961"),
    "https://www.criterion.com/films/33380-bergman-island": (1170, "bergman-island-2021"),
    "https://www.criterion.com/films/556-bergman-island": (477, "bergman-island-2006"),
}

# Old colliding film_ids that need fixing
COLLIDING_IDS = {"roma", "weekend", "something-wild", "bergman-island", "nightmare-alley"}

# TMDB fields to clear so enrich_tmdb.py re-fetches correct data
TMDB_FIELDS = ["tmdb_id", "imdb_id", "poster_url", "genres"]


def fix_catalog(catalog: list[dict]) -> int:
    """Fix catalog entries for colliding spines. Returns count of entries changed."""
    changed = 0
    for entry in catalog:
        spine = entry.get("spine_number")
        if spine not in SPINE_CORRECTIONS:
            continue

        correction = SPINE_CORRECTIONS[spine]
        needs_update = False

        # Check if entry already has correct film_id
        if entry.get("film_id") != correction["film_id"]:
            needs_update = True

        if not needs_update:
            # Already fixed
            continue

        old_id = entry.get("film_id", "")
        log(f"  Catalog spine {spine}: {old_id} -> {correction['film_id']}")
        log(f"    year: {entry.get('year')} -> {correction['year']}")
        log(f"    director: {entry.get('director')} -> {correction['director']}")
        log(f"    criterion_url: {entry.get('criterion_url', '')} -> {correction['criterion_url']}")

        entry["film_id"] = correction["film_id"]
        entry["title"] = correction["title"]
        entry["year"] = correction["year"]
        entry["director"] = correction["director"]
        if correction["criterion_url"]:
            entry["criterion_url"] = correction["criterion_url"]

        # Clear TMDB fields so enrichment re-fetches correct data
        for field in TMDB_FIELDS:
            if field in entry:
                log(f"    Clearing {field}: {entry[field]}")
                del entry[field]

        changed += 1

    return changed


def fix_picks_raw(picks_raw: list[dict]) -> int:
    """Fix film_id and catalog_spine in picks_raw using criterion_film_url. Returns count changed."""
    changed = 0
    for pick in picks_raw:
        if pick.get("film_id") not in COLLIDING_IDS:
            continue

        criterion_url = pick.get("criterion_film_url", "")
        if not criterion_url:
            log(f"  WARNING: picks_raw entry has colliding film_id={pick['film_id']} "
                f"but no criterion_film_url (guest={pick['guest_slug']})")
            continue

        correction = URL_TO_CORRECTION.get(criterion_url)
        if not correction:
            log(f"  WARNING: Unknown criterion_film_url {criterion_url} "
                f"for film_id={pick['film_id']} (guest={pick['guest_slug']})")
            continue

        correct_spine, correct_film_id = correction
        old_id = pick["film_id"]
        old_spine = pick.get("catalog_spine")

        if old_id == correct_film_id and old_spine == correct_spine:
            continue  # Already correct

        log(f"  picks_raw [{pick['guest_slug']}]: "
            f"film_id {old_id}->{correct_film_id}, "
            f"spine {old_spine}->{correct_spine}")

        pick["film_id"] = correct_film_id
        pick["catalog_spine"] = correct_spine
        changed += 1

    return changed


def fix_picks(picks: list[dict], picks_raw: list[dict]) -> int:
    """
    Fix film_slug/film_id and catalog_spine in picks.json.

    Match each colliding pick to its picks_raw counterpart by (guest_slug, film_title)
    to find the authoritative criterion_film_url, then apply the correction.

    Note: picks_raw may already have updated film_ids from fix_picks_raw(), so we
    match on film_title (stable) rather than film_id (already changed).
    """
    # Build lookup: (guest_slug, film_title_lower) -> criterion_film_url from picks_raw
    raw_url_by_title: dict[tuple[str, str], str] = {}
    for rp in picks_raw:
        cfu = rp.get("criterion_film_url", "")
        if not cfu or cfu not in URL_TO_CORRECTION:
            continue
        title = rp.get("film_title", "").lower().strip()
        if title:
            raw_url_by_title[(rp["guest_slug"], title)] = cfu

    changed = 0
    for pick in picks:
        # Check both film_slug and film_id fields
        current_id = pick.get("film_slug") or pick.get("film_id", "")
        if current_id not in COLLIDING_IDS:
            # Also check if already partially fixed (e.g. roma-2018)
            continue

        guest_slug = pick["guest_slug"]
        current_spine = pick.get("catalog_spine")

        # Strategy 1: Match by (guest_slug, film_title) -> criterion_film_url -> correction
        title = pick.get("film_title", "").lower().strip()
        raw_url = raw_url_by_title.get((guest_slug, title))

        if raw_url and raw_url in URL_TO_CORRECTION:
            correct_spine, correct_film_id = URL_TO_CORRECTION[raw_url]
        else:
            # No matching raw entry -- should not happen for colliding picks
            log(f"  WARNING: Cannot resolve pick for guest={guest_slug}, "
                f"film_title={pick.get('film_title')}, film_id={current_id}")
            continue

        old_slug = pick.get("film_slug")
        old_id = pick.get("film_id", "")

        if old_slug == correct_film_id and pick.get("catalog_spine") == correct_spine:
            continue

        log(f"  picks [{guest_slug}]: "
            f"film_slug {old_slug}->{correct_film_id}, "
            f"spine {current_spine}->{correct_spine}")

        if "film_slug" in pick:
            pick["film_slug"] = correct_film_id
        if "film_id" in pick:
            pick["film_id"] = correct_film_id
        pick["catalog_spine"] = correct_spine
        changed += 1

    return changed


def main():
    parser = argparse.ArgumentParser(
        description="Fix film_id collisions in catalog and picks data"
    )
    parser.add_argument("--dry-run", action="store_true", help="Show changes without writing files")
    args = parser.parse_args()

    catalog = load_json(CATALOG_FILE)
    picks = load_json(PICKS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)

    log(f"Loaded: {len(catalog)} catalog entries, {len(picks)} picks, {len(picks_raw)} raw picks")

    # Check for existing collisions
    from collections import Counter
    id_counts = Counter(f["film_id"] for f in catalog)
    dupes = {k: v for k, v in id_counts.items() if v > 1}
    if dupes:
        log(f"Found {len(dupes)} duplicate film_ids: {dupes}")
    else:
        log("No duplicate film_ids found -- entries may already be fixed")

    # 1. Fix catalog
    log("\n--- Fixing catalog ---")
    n_catalog = fix_catalog(catalog)
    log(f"Catalog: {n_catalog} entries updated")

    # 2. Fix picks_raw
    log("\n--- Fixing picks_raw ---")
    n_raw = fix_picks_raw(picks_raw)
    log(f"picks_raw: {n_raw} entries updated")

    # 3. Fix picks
    log("\n--- Fixing picks ---")
    n_picks = fix_picks(picks, picks_raw)
    log(f"picks: {n_picks} entries updated")

    # Verify no remaining collisions
    id_counts_after = Counter(f["film_id"] for f in catalog)
    dupes_after = {k: v for k, v in id_counts_after.items() if v > 1}
    if dupes_after:
        log(f"\nWARNING: Still have duplicate film_ids after fix: {dupes_after}")
    else:
        log(f"\nAll film_id collisions resolved")

    # Summary
    log(f"\nTotal changes: {n_catalog} catalog + {n_raw} raw picks + {n_picks} picks")

    if args.dry_run:
        log("\nDry run -- no files written.")
    else:
        save_json(CATALOG_FILE, catalog)
        save_json(PICKS_RAW_FILE, picks_raw)
        save_json(PICKS_FILE, picks)
        log(f"\nSaved all files")


if __name__ == "__main__":
    main()
