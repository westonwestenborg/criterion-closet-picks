#!/usr/bin/env python3
"""
One-time migration: backfill `source` and `visit_index` fields on existing data.

Source backfill:
  - picks_raw.json: criterion_film_url present -> "criterion", else "letterboxd"
  - picks.json: look up matching entry in picks_raw by (guest_slug, film_id/film_title)

Visit index backfill:
  - Single-visit guests: all picks get visit_index = 1
  - Multi-visit guests (picks.json): match by video ID in timestamp URL
  - Multi-visit guests (picks_raw.json): cross-reference with picks.json,
    then attribute remaining picks to the visit that lacks coverage

Re-runnable: clears existing visit_index before recomputing.

Usage:
  python scripts/migrate_source_visit.py [--dry-run]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.utils import GUESTS_FILE, PICKS_FILE, PICKS_RAW_FILE, load_json, save_json, log


def backfill_source_raw(picks_raw: list[dict]) -> int:
    """Backfill source field on picks_raw entries. Returns count changed."""
    changed = 0
    for p in picks_raw:
        if p.get("source"):
            continue
        if p.get("criterion_film_url"):
            p["source"] = "criterion"
        else:
            p["source"] = "letterboxd"
        changed += 1
    return changed


def backfill_source_picks(picks: list[dict], picks_raw: list[dict]) -> int:
    """Backfill source field on picks.json entries from picks_raw. Returns count changed."""
    # Build lookup: (guest_slug, film_title) -> source from picks_raw
    raw_source = {}
    for rp in picks_raw:
        key = (rp["guest_slug"], rp.get("film_title", "").lower())
        raw_source[key] = rp.get("source", "letterboxd")
        # Also index by film_id for fallback
        film_id = rp.get("film_id", "")
        if film_id:
            raw_source[(rp["guest_slug"], film_id)] = rp.get("source", "letterboxd")

    changed = 0
    for p in picks:
        # Try matching by (guest_slug, film_title)
        title_key = (p["guest_slug"], p.get("film_title", "").lower())
        source = raw_source.get(title_key)
        if not source:
            # Try by film_id/film_slug
            film_key = p.get("film_slug") or p.get("film_id", "")
            source = raw_source.get((p["guest_slug"], film_key))
        new_source = source or "letterboxd"
        if p.get("source") != new_source:
            p["source"] = new_source
            changed += 1
    return changed


def _extract_video_id_from_url(url: str) -> str | None:
    """Extract YouTube video ID from a timestamp URL."""
    if not url:
        return None
    if "v=" in url:
        for part in url.split("?")[-1].split("&"):
            if part.startswith("v="):
                return part[2:]
    return None


def _extract_vimeo_id_from_url(url: str) -> str | None:
    """Extract Vimeo video ID from a timestamp URL."""
    if not url:
        return None
    for part in url.split("/"):
        clean = part.split("#")[0]
        if clean.isdigit():
            return clean
    return None


def backfill_visit_index_picks(picks: list[dict], guests: list[dict]) -> int:
    """
    Backfill visit_index on picks.json entries.
    For multi-visit guests, match by video ID in youtube_timestamp_url.
    For picks without video URLs, attribute based on which visit other picks
    from the same guest map to.
    """
    guest_by_slug = {g["slug"]: g for g in guests}

    # Build video_id -> visit_index mapping per guest
    video_to_visit = {}  # (guest_slug, video_id) -> visit_index
    multi_visit_slugs = set()
    for g in guests:
        visits = g.get("visits", [])
        if len(visits) < 2:
            continue
        multi_visit_slugs.add(g["slug"])
        for i, v in enumerate(visits):
            vid = v.get("youtube_video_id")
            if vid:
                video_to_visit[(g["slug"], vid)] = i + 1
            vim = v.get("vimeo_video_id")
            if vim:
                video_to_visit[(g["slug"], vim)] = i + 1

    # Clear existing visit_index so we recompute cleanly
    for p in picks:
        p.pop("visit_index", None)

    # First pass: attribute picks that have video URLs
    changed = 0
    deferred = []  # (index, slug) for multi-visit picks without video URLs
    for i, p in enumerate(picks):
        slug = p["guest_slug"]

        if slug not in multi_visit_slugs:
            p["visit_index"] = 1
            changed += 1
            continue

        # Multi-visit: extract video ID from timestamp URL
        yt_url = p.get("youtube_timestamp_url", "")
        vim_url = p.get("vimeo_timestamp_url", "")

        vid = _extract_video_id_from_url(yt_url)
        visit_idx = video_to_visit.get((slug, vid)) if vid else None

        if not visit_idx:
            vim_id = _extract_vimeo_id_from_url(vim_url)
            visit_idx = video_to_visit.get((slug, vim_id)) if vim_id else None

        if visit_idx:
            p["visit_index"] = visit_idx
            changed += 1
        else:
            deferred.append((i, slug))

    # Second pass: picks without video URLs were processed against the
    # primary (visit 1) transcript by extract_quotes.py. They didn't get
    # a quote, but they're still from the visit 1 pick list. Default to 1.
    for idx, slug in deferred:
        picks[idx]["visit_index"] = 1
        changed += 1

    return changed


def override_visit_from_criterion(picks: list[dict], picks_raw: list[dict], guests: list[dict]) -> int:
    """
    For criterion-sourced picks in picks.json, override visit_index with the
    value from the matching criterion-sourced picks_raw entry (set by the scraper
    from collection page URLs). Criterion pages are the authority on which visit
    a film belongs to, even if the quote came from a different visit's video.

    Only applies to guests who have SEPARATE criterion pages per visit. If a guest
    has only one criterion page covering all visits, video-based attribution is
    more reliable.
    """
    # Identify guests with multiple distinct criterion page URLs across visits
    guests_with_multi_criterion = set()
    for g in guests:
        visits = g.get("visits", [])
        criterion_urls = {v.get("criterion_page_url") for v in visits if v.get("criterion_page_url")}
        if len(criterion_urls) >= 2:
            guests_with_multi_criterion.add(g["slug"])

    # Build lookup: (guest_slug, film_id) -> visit_index from criterion-sourced raw picks
    criterion_visit = {}
    for rp in picks_raw:
        if rp.get("source") != "criterion":
            continue
        if rp["guest_slug"] not in guests_with_multi_criterion:
            continue
        vi = rp.get("visit_index")
        if not vi:
            continue
        film_id = rp.get("film_id", "")
        if film_id:
            criterion_visit[(rp["guest_slug"], film_id)] = vi
        title = rp.get("film_title", "").lower()
        if title:
            criterion_visit[(rp["guest_slug"], title)] = vi

    changed = 0
    for p in picks:
        if p.get("source") != "criterion":
            continue
        slug = p["guest_slug"]
        if slug not in guests_with_multi_criterion:
            continue
        film_key = p.get("film_slug") or p.get("film_id", "")
        title = p.get("film_title", "").lower()
        raw_vi = criterion_visit.get((slug, film_key)) or criterion_visit.get((slug, title))
        if raw_vi and p.get("visit_index") != raw_vi:
            p["visit_index"] = raw_vi
            changed += 1
    return changed


def update_pick_counts(guests: list[dict], picks: list[dict], picks_raw: list[dict]) -> int:
    """
    Update pick_count on guests to reflect displayable picks.
    Display rule: source === 'criterion' OR has a non-empty quote.
    Mirrors getDisplayablePicksForGuest() logic in data.ts.
    """
    # Build picks.json lookup per guest
    picks_by_guest: dict[str, list[dict]] = {}
    for p in picks:
        picks_by_guest.setdefault(p["guest_slug"], []).append(p)

    # Build raw picks lookup per guest
    raw_by_guest: dict[str, list[dict]] = {}
    for p in picks_raw:
        raw_by_guest.setdefault(p["guest_slug"], []).append(p)

    changed = 0
    for g in guests:
        slug = g["slug"]
        guest_picks = picks_by_guest.get(slug, [])
        guest_raw = raw_by_guest.get(slug, [])

        # Processed picks that are displayable
        processed_slugs = set()
        displayable = 0
        for p in guest_picks:
            film_key = p.get("film_slug") or p.get("film_id", "")
            processed_slugs.add(film_key)
            # Display if criterion-sourced or has a quote
            if p.get("source") == "criterion":
                displayable += 1
            elif p.get("quote", "").strip():
                displayable += 1

        # Raw criterion picks not already in processed
        for rp in guest_raw:
            film_key = rp.get("film_id", "")
            if film_key in processed_slugs:
                continue
            if rp.get("source") == "criterion":
                displayable += 1

        if g.get("pick_count") != displayable:
            g["pick_count"] = displayable
            changed += 1

    return changed


def backfill_visit_index_raw(picks_raw: list[dict], picks: list[dict], guests: list[dict]) -> int:
    """
    Backfill visit_index on picks_raw entries.
    Strategy for multi-visit guests:
      1. Cross-reference with picks.json: if a raw pick's film matches
         a picks.json entry, copy that visit_index.
      2. For remaining: attribute to the visit NOT covered by picks.json,
         since merged Letterboxd lists have picks from both visits mixed together.
    """
    guest_by_slug = {g["slug"]: g for g in guests}
    multi_visit_slugs = {g["slug"] for g in guests if len(g.get("visits", [])) >= 2}

    # Build picks.json lookup: (guest_slug, film_id) -> visit_index
    picks_visit = {}
    for p in picks:
        film_key = p.get("film_slug") or p.get("film_id", "")
        if film_key and p.get("visit_index"):
            picks_visit[(p["guest_slug"], film_key)] = p["visit_index"]
        # Also by title
        title = p.get("film_title", "").lower()
        if title and p.get("visit_index"):
            picks_visit[(p["guest_slug"], title)] = p["visit_index"]

    # Clear existing visit_index, but preserve criterion-sourced picks
    # whose visit_index was set by the scraper from collection page URLs
    for p in picks_raw:
        if p.get("source") == "criterion" and p.get("visit_index"):
            continue  # Scraper set this from the actual Criterion page URL
        p.pop("visit_index", None)

    # First pass: attribute what we can
    changed = 0
    deferred = []  # indices of multi-visit picks we couldn't attribute
    for i, p in enumerate(picks_raw):
        if p.get("visit_index"):
            changed += 1
            continue  # Already set (criterion-sourced, preserved above)

        slug = p["guest_slug"]

        if slug not in multi_visit_slugs:
            p["visit_index"] = 1
            changed += 1
            continue

        # Try cross-reference with picks.json
        film_id = p.get("film_id", "")
        title = p.get("film_title", "").lower()
        visit_idx = picks_visit.get((slug, film_id)) or picks_visit.get((slug, title))

        if visit_idx:
            p["visit_index"] = visit_idx
            changed += 1
        else:
            deferred.append(i)

    # Second pass: for unattributed raw picks of multi-visit guests,
    # attribute to the visit NOT covered by picks.json entries.
    if deferred:
        # Collect attributed visits per guest from picks.json
        picks_visits_per_guest = {}  # slug -> set of visit indices
        for p in picks:
            slug = p["guest_slug"]
            if slug in multi_visit_slugs and p.get("visit_index"):
                if slug not in picks_visits_per_guest:
                    picks_visits_per_guest[slug] = set()
                picks_visits_per_guest[slug].add(p["visit_index"])

        for idx in deferred:
            p = picks_raw[idx]
            slug = p["guest_slug"]
            guest = guest_by_slug[slug]
            num_visits = len(guest.get("visits", []))
            known = picks_visits_per_guest.get(slug, set())

            # If picks.json only has entries from one visit, remaining raw picks
            # are likely from the other visit
            if len(known) == 1 and num_visits == 2:
                other_visit = 1 if 2 in known else 2
                p["visit_index"] = other_visit
            else:
                # Can't determine -- default to 1
                p["visit_index"] = 1
            changed += 1

    return changed


def main():
    parser = argparse.ArgumentParser(description="Backfill source and visit_index on existing data")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without writing files")
    args = parser.parse_args()

    guests = load_json(GUESTS_FILE)
    picks = load_json(PICKS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)

    log(f"Loaded: {len(guests)} guests, {len(picks)} picks, {len(picks_raw)} raw picks")

    # 1. Backfill source on picks_raw
    n = backfill_source_raw(picks_raw)
    log(f"Source backfill (picks_raw): {n} entries updated")

    # 2. Backfill source on picks.json
    n = backfill_source_picks(picks, picks_raw)
    log(f"Source backfill (picks): {n} entries updated")

    # 3. Backfill visit_index on picks.json FIRST (raw uses it as reference)
    n = backfill_visit_index_picks(picks, guests)
    log(f"Visit index backfill (picks): {n} entries updated")

    # 4. Override visit_index on criterion-sourced picks.json entries
    #    with the authoritative visit_index from criterion-sourced picks_raw
    #    (set by scraper from collection page URLs)
    n = override_visit_from_criterion(picks, picks_raw, guests)
    log(f"Criterion visit override (picks): {n} entries updated")

    # 5. Backfill visit_index on picks_raw (cross-references picks.json)
    n = backfill_visit_index_raw(picks_raw, picks, guests)
    log(f"Visit index backfill (picks_raw): {n} entries updated")

    # 6. Update pick_count to reflect displayable picks
    #    Display rule: source === 'criterion' OR has a quote
    n = update_pick_counts(guests, picks, picks_raw)
    log(f"Pick count updates: {n} guests updated")

    # Stats
    raw_criterion = sum(1 for p in picks_raw if p.get("source") == "criterion")
    raw_letterboxd = sum(1 for p in picks_raw if p.get("source") == "letterboxd")
    log(f"\npicks_raw source distribution: criterion={raw_criterion}, letterboxd={raw_letterboxd}")

    picks_criterion = sum(1 for p in picks if p.get("source") == "criterion")
    picks_letterboxd = sum(1 for p in picks if p.get("source") == "letterboxd")
    log(f"picks source distribution: criterion={picks_criterion}, letterboxd={picks_letterboxd}")

    # Multi-visit stats
    log(f"\nMulti-visit guest attribution:")
    multi_visit_guests = [g for g in guests if len(g.get("visits", [])) >= 2]
    for g in multi_visit_guests:
        slug = g["slug"]
        guest_picks = [p for p in picks if p["guest_slug"] == slug]
        guest_raw = [p for p in picks_raw if p["guest_slug"] == slug]
        by_visit_picks = {}
        for p in guest_picks:
            vi = p.get("visit_index", 1)
            by_visit_picks[vi] = by_visit_picks.get(vi, 0) + 1
        by_visit_raw = {}
        for p in guest_raw:
            vi = p.get("visit_index", 1)
            by_visit_raw[vi] = by_visit_raw.get(vi, 0) + 1
        log(f"  {g['name']:30s} picks={dict(sorted(by_visit_picks.items()))}  raw={dict(sorted(by_visit_raw.items()))}")

    if args.dry_run:
        log("\nDry run -- no files written.")
    else:
        save_json(PICKS_RAW_FILE, picks_raw)
        save_json(PICKS_FILE, picks)
        save_json(GUESTS_FILE, guests)
        log(f"\nSaved {len(picks_raw)} raw picks, {len(picks)} picks, and {len(guests)} guests")


if __name__ == "__main__":
    main()
