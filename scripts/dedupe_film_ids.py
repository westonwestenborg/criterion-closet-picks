#!/usr/bin/env python3
"""
Dedupe catalog entries with numeric-prefixed film_ids that shadow a clean base
entry, repoint picks/picks_raw to the base id, and normalize pick film_title
fields to the canonical catalog title.

Issue 1 — Catalog duplicates
  Some films have two catalog entries because Criterion serves the same film
  under two URL formats (e.g. /films/28561-all-that-jazz and /films/all-that-jazz),
  yielding film_ids "28561-all-that-jazz" and "all-that-jazz". This script
  detects entries whose film_id matches ^\\d+-(.+) where the captured group is
  also a film_id, merges any non-empty fields from the prefixed entry into the
  base entry, deletes the prefixed entry, and repoints picks/picks_raw.

  Films that legitimately start with digits (e.g. 3-women, 12-angry-men) are
  untouched because there is no matching base entry.

Issue 2 — Inconsistent title casing in picks
  Different sources (Letterboxd, Criterion, Gemini extraction) capitalize
  titles differently. After dedup, every pick's film_title is rewritten to the
  canonical title from criterion_catalog.json (looked up by film_id). Picks
  whose existing title looks meaningfully different from the canonical (not
  just casing/punctuation) are listed in a warning so the underlying mapping
  can be investigated separately.

Re-runnable: idempotent. Running twice is a no-op.
"""

import argparse
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    GUESTS_FILE,
    PICKS_FILE,
    PICKS_RAW_FILE,
    load_json,
    save_json,
    log,
)
from scripts.migrate_source_visit import update_pick_counts


PREFIX_RE = re.compile(r"^(\d+)-(.+)")


def find_dupes(catalog: list[dict]) -> dict[str, str]:
    """Return mapping of prefixed_film_id -> base_film_id for catalog dupes."""
    ids = {entry["film_id"] for entry in catalog if "film_id" in entry}
    mapping: dict[str, str] = {}
    for entry in catalog:
        film_id = entry.get("film_id", "")
        m = PREFIX_RE.match(film_id)
        if not m:
            continue
        base = m.group(2)
        if base in ids:
            mapping[film_id] = base
    return mapping


def merge_into_base(base: dict, prefixed: dict) -> list[str]:
    """Copy non-empty fields from prefixed into base where base is missing them.

    Returns list of fields that were filled in from the prefixed entry.
    """
    filled = []
    for key, value in prefixed.items():
        if key == "film_id":
            continue
        if value in (None, "", [], {}):
            continue
        existing = base.get(key)
        if existing in (None, "", [], {}):
            base[key] = value
            filled.append(key)
    return filled


def fix_catalog(catalog: list[dict], mapping: dict[str, str]) -> tuple[list[dict], int]:
    """Merge prefixed entries into base entries and drop the prefixed ones."""
    by_id = {entry["film_id"]: entry for entry in catalog}
    merged = 0
    for prefixed_id, base_id in mapping.items():
        prefixed_entry = by_id.get(prefixed_id)
        base_entry = by_id.get(base_id)
        if not prefixed_entry or not base_entry:
            continue
        filled = merge_into_base(base_entry, prefixed_entry)
        if filled:
            log(f"  merge {prefixed_id} -> {base_id}: filled {filled}")
        else:
            log(f"  drop  {prefixed_id} -> {base_id} (no new fields)")
        merged += 1

    new_catalog = [e for e in catalog if e.get("film_id") not in mapping]
    return new_catalog, merged


def repoint_picks(picks: list[dict], mapping: dict[str, str], base_by_id: dict[str, dict]) -> int:
    """Update film_id and catalog_spine on picks pointing to a prefixed dupe."""
    changed = 0
    for pick in picks:
        film_id = pick.get("film_id")
        if film_id in mapping:
            base_id = mapping[film_id]
            pick["film_id"] = base_id
            base = base_by_id.get(base_id)
            if base and base.get("spine_number") is not None:
                pick["catalog_spine"] = base["spine_number"]
            if base and base.get("title"):
                pick["catalog_title"] = base["title"]
            changed += 1
    return changed


def dedupe_picks_raw(picks_raw: list[dict]) -> int:
    """Drop exact-duplicate raw picks introduced by repointing.

    Two raw entries are duplicates when they share the same guest_slug,
    visit_index, criterion_film_url, and film_id. Keeps the first occurrence.
    """
    seen: set[tuple] = set()
    out: list[dict] = []
    for pick in picks_raw:
        key = (
            pick.get("guest_slug"),
            pick.get("visit_index"),
            pick.get("criterion_film_url"),
            pick.get("film_id"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(pick)
    removed = len(picks_raw) - len(out)
    picks_raw[:] = out
    return removed


def _is_minor_variation(a: str, b: str) -> bool:
    """True when two titles differ only by casing, punctuation, or diacritics."""
    def normalize(s: str) -> str:
        import unicodedata
        s = unicodedata.normalize("NFKD", s)
        s = s.encode("ascii", "ignore").decode("ascii")
        s = re.sub(r"[^a-z0-9]+", "", s.lower())
        return s
    return normalize(a) == normalize(b)


def normalize_titles(picks: list[dict], base_by_id: dict[str, dict], label: str) -> tuple[int, list[tuple]]:
    """Rewrite film_title to the canonical catalog title. Returns (changed, suspect)."""
    changed = 0
    suspect: list[tuple] = []
    for pick in picks:
        film_id = pick.get("film_id")
        if not film_id:
            continue
        canonical = base_by_id.get(film_id, {}).get("title")
        if not canonical:
            continue
        current = pick.get("film_title", "")
        if current == canonical:
            continue
        if not _is_minor_variation(current, canonical):
            suspect.append((label, pick.get("guest_slug"), film_id, current, canonical, pick.get("criterion_film_url")))
        pick["film_title"] = canonical
        changed += 1
    return changed, suspect


def main():
    parser = argparse.ArgumentParser(
        description="Dedupe prefixed-vs-base catalog film_ids and normalize pick titles"
    )
    parser.add_argument("--dry-run", action="store_true", help="Do not write files")
    args = parser.parse_args()

    catalog = load_json(CATALOG_FILE)
    guests = load_json(GUESTS_FILE)
    picks = load_json(PICKS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)

    log(f"Loaded: {len(catalog)} catalog, {len(guests)} guests, "
        f"{len(picks)} picks, {len(picks_raw)} raw picks")

    mapping = find_dupes(catalog)
    log(f"\nFound {len(mapping)} prefixed catalog entries shadowing a base entry")

    if mapping:
        log("\n--- Merging catalog ---")
        catalog, n_catalog = fix_catalog(catalog, mapping)

        base_by_id = {e["film_id"]: e for e in catalog}

        log("\n--- Repointing picks.json ---")
        n_picks = repoint_picks(picks, mapping, base_by_id)
        log(f"Repointed {n_picks} picks")

        log("\n--- Repointing picks_raw.json ---")
        n_raw = repoint_picks(picks_raw, mapping, base_by_id)
        log(f"Repointed {n_raw} raw picks")

        log("\n--- Deduping picks_raw.json ---")
        n_raw_dropped = dedupe_picks_raw(picks_raw)
        log(f"Dropped {n_raw_dropped} duplicate raw picks")
    else:
        log("No catalog dupes to merge.")
        base_by_id = {e["film_id"]: e for e in catalog}
        n_catalog = n_picks = n_raw = n_raw_dropped = 0

    log("\n--- Normalizing pick titles to canonical ---")
    n_titles_picks, suspect_picks = normalize_titles(picks, base_by_id, "picks")
    n_titles_raw, suspect_raw = normalize_titles(picks_raw, base_by_id, "picks_raw")
    log(f"Updated {n_titles_picks} picks titles, {n_titles_raw} raw titles")

    suspect = suspect_picks + suspect_raw
    if suspect:
        log(f"\nWARNING: {len(suspect)} picks have a title that is NOT a minor variation")
        log("of the canonical title. These may indicate wrong film_id mappings:")
        seen_keys: set = set()
        for label, guest, film_id, current, canonical, url in suspect:
            key = (film_id, current, canonical)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            log(f"  [{label}] {film_id} canonical={canonical!r} got={current!r} url={url}")
        log(f"  ({len(suspect)} total occurrences across {len(seen_keys)} distinct cases)")

    log("\n--- Recalculating guest pick_counts ---")
    n_guests = update_pick_counts(guests, picks, picks_raw)
    log(f"Updated pick_count on {n_guests} guests")

    id_counts = Counter(f["film_id"] for f in catalog)
    remaining_dupes = {k: v for k, v in id_counts.items() if v > 1}
    if remaining_dupes:
        log(f"\nWARNING: catalog still has duplicate film_ids: {remaining_dupes}")

    log("\n--- Summary ---")
    log(f"  catalog dupes merged   : {n_catalog}")
    log(f"  picks repointed        : {n_picks}")
    log(f"  raw picks repointed    : {n_raw}")
    log(f"  raw duplicates dropped : {n_raw_dropped}")
    log(f"  pick titles normalized : {n_titles_picks}")
    log(f"  raw  titles normalized : {n_titles_raw}")
    log(f"  guest pick_counts upd. : {n_guests}")

    if args.dry_run:
        log("\nDry run -- no files written.")
        return

    save_json(CATALOG_FILE, catalog)
    save_json(GUESTS_FILE, guests)
    save_json(PICKS_FILE, picks)
    save_json(PICKS_RAW_FILE, picks_raw)
    log("\nSaved catalog, guests, picks, picks_raw.")


if __name__ == "__main__":
    main()
