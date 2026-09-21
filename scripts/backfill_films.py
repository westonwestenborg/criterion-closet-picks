#!/usr/bin/env python3
"""
Backfill missing films into criterion_catalog.json and propagate Criterion URLs.

Four tasks:
1. Films referenced in picks.json that have no catalog entry get synthetic entries
   created from the best available data (picks.json + picks_raw.json).
2. criterion_film_url values from picks_raw.json are propagated to matching
   catalog entries (which currently all have empty criterion_url).
3. Catalog entries whose URL points at /boxsets/ are flagged is_box_set.
4. Picks left unmatched by the scrape because task 1 had not run yet get their
   catalog_title / catalog_spine / match_method filled in.

Output: data/criterion_catalog.json, and data/picks*.json when task 4 changes one
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
from scripts.schema import CatalogFilm, Pick


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
                    "catalog_spine": p.get("catalog_spine"),
                "catalog_title": p.get("catalog_title"),
                "criterion_film_url": "",
            }
        else:
            # Fill in blanks from picks if picks_raw had None
            existing = info[fid]
            if not existing["film_title"]:
                existing["film_title"] = p.get("film_title") or p.get("catalog_title")
            if not existing["catalog_spine"]:
                existing["catalog_spine"] = p.get("catalog_spine")

    return info


def make_synthetic_entry(film_id: str, meta: dict) -> dict:
    """Create a catalog entry for a film not in the original catalog."""
    title = meta.get("film_title") or meta.get("catalog_title") or film_id.replace("-", " ").title()
    url = meta.get("criterion_film_url", "")
    entry = {
        "spine_number": meta.get("catalog_spine"),
        "title": title,
        "director": "",
        # Left empty on purpose: enrich_tmdb.py fills year from the film's own
        # Criterion page, which is authoritative. Picks never carried a reliable
        # year of their own.
        "year": None,
        "country": "",
        "criterion_url": url,
        "film_id": film_id,
        "imdb_id": None,
        "tmdb_id": None,
        "genres": [],
        "poster_url": None,
        "credits": None,
    }
    if "/boxsets/" in url:
        entry["is_box_set"] = True
    return entry


def main() -> None:
    catalog: list[CatalogFilm] = load_json(CATALOG_FILE)
    picks: list[Pick] = load_json(PICKS_FILE)
    picks_raw: list[Pick] = load_json(PICKS_RAW_FILE)

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

    # --- Task 3: Flag box set entries ---
    flagged = 0
    for entry in catalog:
        url = entry.get("criterion_url", "") or ""
        if "/boxsets/" in url and not entry.get("is_box_set"):
            entry["is_box_set"] = True
            flagged += 1
    if flagged:
        log(f"Flagged {flagged} catalog entries as is_box_set")

    # --- Task 4: Re-resolve picks whose film was missing from the catalog ---
    # scrape_criterion_picks.py resolves each pick against the catalog and leaves
    # catalog_title / catalog_spine / match_method null when nothing matches. A
    # film Criterion had not shelved yet -- Body Heat, spine 1308 -- therefore
    # scraped as unmatched, and task 1 above then built its catalog entry out of
    # that very pick. The URL match the scrape tried now succeeds, so run it
    # again here rather than leave the pick reading as unmatched forever.
    # Only a pick whose own criterion_film_url equals the entry's counts: film_id
    # alone would let a synthetic entry vouch for a link nothing established.
    resolved = 0
    for rows in (picks, picks_raw):
        for pick in rows:
            if pick.get("match_method"):
                continue
            entry = catalog_by_id.get(pick.get("film_id"))
            if entry is None:
                continue
            url = pick.get("criterion_film_url") or ""
            if not url or url != entry.get("criterion_url"):
                continue
            pick["catalog_title"] = entry.get("title")
            pick["catalog_spine"] = entry.get("spine_number")
            pick["match_method"] = "criterion_url"
            resolved += 1

    log(f"Re-resolved match markers on {resolved} picks")

    # --- Save ---
    save_json(CATALOG_FILE, catalog)
    log(f"Saved {len(catalog)} catalog entries to {CATALOG_FILE}")
    if resolved:
        save_json(PICKS_FILE, picks)
        save_json(PICKS_RAW_FILE, picks_raw)
        log(f"Saved picks to {PICKS_FILE} and {PICKS_RAW_FILE}")


if __name__ == "__main__":
    main()
