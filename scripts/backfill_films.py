#!/usr/bin/env python3
"""
Backfill missing films into criterion_catalog.json and propagate Criterion URLs.

Two tasks:
1. Films referenced in picks.json that have no catalog entry get synthetic entries
   created from the best available data (picks.json + picks_raw.json).
2. criterion_film_url values from picks_raw.json are propagated to matching
   catalog entries (which currently all have empty criterion_url).

Output: data/criterion_catalog.json (updated in place)
"""

import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    PICKS_FILE,
    PICKS_RAW_FILE,
    load_json,
    save_json,
    log,
)


def build_criterion_url_map(picks_raw: list[dict]) -> dict[str, str]:
    """Build film_id -> criterion_film_url from picks_raw entries."""
    url_map: dict[str, str] = {}
    for p in picks_raw:
        fid = p.get("film_id")
        url = p.get("criterion_film_url")
        if fid and url and fid not in url_map:
            url_map[fid] = url
    return url_map


def build_film_info(picks: list[dict], picks_raw: list[dict]) -> dict[str, dict]:
    """Build film_id -> best available metadata from picks and picks_raw."""
    info: dict[str, dict] = {}

    # First pass: picks_raw (may have more fields like criterion_film_url)
    for p in picks_raw:
        fid = p.get("film_id")
        if not fid or fid in info:
            continue
        info[fid] = {
            "film_title": p.get("film_title") or p.get("catalog_title"),
            "film_year": p.get("film_year"),
            "catalog_spine": p.get("catalog_spine"),
            "catalog_title": p.get("catalog_title"),
            "criterion_film_url": p.get("criterion_film_url", ""),
        }

    # Second pass: picks.json (overwrite only if we get better data)
    for p in picks:
        fid = p.get("film_id")
        if not fid:
            continue
        if fid not in info:
            info[fid] = {
                "film_title": p.get("film_title") or p.get("catalog_title"),
                "film_year": p.get("film_year"),
                "catalog_spine": p.get("catalog_spine"),
                "catalog_title": p.get("catalog_title"),
                "criterion_film_url": "",
            }
        else:
            # Fill in blanks from picks if picks_raw had None
            existing = info[fid]
            if not existing["film_title"]:
                existing["film_title"] = p.get("film_title") or p.get("catalog_title")
            if not existing["film_year"]:
                existing["film_year"] = p.get("film_year")
            if not existing["catalog_spine"]:
                existing["catalog_spine"] = p.get("catalog_spine")

    return info


def make_synthetic_entry(film_id: str, meta: dict) -> dict:
    """Create a catalog entry for a film not in the original catalog."""
    title = meta.get("film_title") or meta.get("catalog_title") or film_id.replace("-", " ").title()
    return {
        "spine_number": meta.get("catalog_spine"),
        "title": title,
        "director": "",
        "year": meta.get("film_year"),
        "country": "",
        "criterion_url": meta.get("criterion_film_url", ""),
        "film_id": film_id,
        "imdb_id": None,
        "tmdb_id": None,
        "genres": [],
        "poster_url": None,
        "credits": None,
    }


def main() -> None:
    catalog = load_json(CATALOG_FILE)
    picks = load_json(PICKS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)

    catalog_by_id = {c["film_id"]: c for c in catalog}
    log(f"Loaded {len(catalog)} catalog entries, {len(picks)} picks, {len(picks_raw)} picks_raw")

    # --- Task 1: Backfill missing films ---
    pick_film_ids = set(p["film_id"] for p in picks)
    missing_ids = pick_film_ids - set(catalog_by_id.keys())
    log(f"Films in picks but not catalog: {len(missing_ids)}")

    film_info = build_film_info(picks, picks_raw)
    added = 0
    for fid in sorted(missing_ids):
        meta = film_info.get(fid, {})
        entry = make_synthetic_entry(fid, meta)
        catalog.append(entry)
        catalog_by_id[fid] = entry
        added += 1

    log(f"Added {added} synthetic catalog entries")

    # --- Task 2: Propagate Criterion URLs ---
    url_map = build_criterion_url_map(picks_raw)
    propagated = 0
    for entry in catalog:
        fid = entry["film_id"]
        if not entry.get("criterion_url") and fid in url_map:
            entry["criterion_url"] = url_map[fid]
            propagated += 1

    log(f"Propagated criterion_url to {propagated} catalog entries")

    # --- Save ---
    save_json(CATALOG_FILE, catalog)
    log(f"Saved {len(catalog)} catalog entries to {CATALOG_FILE}")


if __name__ == "__main__":
    main()
