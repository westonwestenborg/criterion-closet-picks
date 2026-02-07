#!/usr/bin/env python3
"""
One-time data cleanup for box set handling.

Fixes:
1. Canonicalizes box set names (smart quotes, duplicates)
2. Adds is_box_set=True to catalog entries
3. Fixes stale Criterion URLs (BRD Trilogy, Apu, Cassavetes)
4. Propagates box_set_criterion_url from picks_raw.json
5. Converts "box set as unit" picks to proper aggregates with quotes preserved

Usage:
  python scripts/fix_box_sets.py --dry-run   # Preview changes
  python scripts/fix_box_sets.py              # Apply changes
"""

import argparse
import re
import sys
import unicodedata
from collections import defaultdict

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    PICKS_FILE,
    PICKS_RAW_FILE,
    load_json,
    save_json,
    log,
)


# ---------------------------------------------------------------------------
# Name canonicalization map: variant -> canonical name
# Built from picks_raw.json criterion_film_url -> film_title mapping
# plus manual overrides for known inconsistencies
# ---------------------------------------------------------------------------

MANUAL_NAME_MAP = {
    # group_box_sets.py used short names; picks_raw uses canonical Criterion names
    "Five Films by Cassavetes": "John Cassavetes: Five Films",
    "Jacques Demy box": "The Essential Jacques Demy",
    "Apu Trilogy": "The Apu Trilogy",
    "Jacques Tati box": "The Complete Jacques Tati",
    "Wim Wenders box": "Wim Wenders: The Road Trilogy",
    "Dietrich box": "Dietrich & von Sternberg in Hollywood",
    "Monte Hellman set": "The Shooting/Ride in the Whirlwind",
    "Melvin Van Peebles: Four Films box": "Melvin Van Peebles: Essential Films",
    "Marseille Trilogy": "The Marseille Trilogy",
    "World Cinema Project": "Martin Scorsese's World Cinema Project No. 1",
    "Three Colors": "Three Colors",
    "Fanny and Alexander Box Set": "Fanny and Alexander",
    # Smart quote variants (NFKD normalization handles most, but be explicit)
    "Ingmar Bergman\u2019s Cinema": "Ingmar Bergman's Cinema",
}

# Stale URL fixes: film_id -> correct criterion_url
STALE_URL_FIXES = {
    # BRD Trilogy currently points to Before Trilogy URL
    "the-brd-trilogy": "https://www.criterion.com/boxsets/138-the-brd-trilogy",
}


def normalize_smart_quotes(text: str) -> str:
    """Replace smart quotes with straight quotes."""
    return (
        text.replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
    )


def build_canonical_name_map(picks_raw: list[dict]) -> dict[str, str]:
    """
    Build a map from variant box set names to canonical names.
    Canonical = the title used in picks_raw.json for a given criterion_film_url.
    """
    # First, build URL -> canonical title from picks_raw (most reliable source)
    url_to_title: dict[str, str] = {}
    for p in picks_raw:
        url = p.get("criterion_film_url", "")
        if "/boxsets/" in url and p.get("film_title"):
            # Use the first title we encounter for each URL
            if url not in url_to_title:
                url_to_title[url] = p["film_title"]

    # Build the final name map
    name_map: dict[str, str] = {}

    # Add manual overrides
    name_map.update(MANUAL_NAME_MAP)

    # Add smart-quote normalized versions
    for canonical_title in url_to_title.values():
        normalized = normalize_smart_quotes(canonical_title)
        if normalized != canonical_title:
            name_map[canonical_title] = normalized

    return name_map


def build_url_map(picks_raw: list[dict]) -> dict[str, str]:
    """
    Build box_set_name -> criterion URL map from picks_raw.json.
    Uses the canonical name (after applying name map).
    """
    url_map: dict[str, str] = {}
    for p in picks_raw:
        url = p.get("criterion_film_url", "")
        title = p.get("film_title", "")
        if "/boxsets/" in url and title:
            # Normalize title
            title = normalize_smart_quotes(title)
            if title not in url_map:
                url_map[title] = url
    return url_map


def build_film_id_to_url(picks_raw: list[dict]) -> dict[str, str]:
    """Map film_id -> criterion_film_url from picks_raw for box set entries."""
    result: dict[str, str] = {}
    for p in picks_raw:
        fid = p.get("film_id", "")
        url = p.get("criterion_film_url", "")
        if "/boxsets/" in url and fid:
            if fid not in result:
                result[fid] = url
    return result


def _infer_film_count_from_name(name: str) -> int | None:
    """Try to infer film count from box set name patterns."""
    lower = name.lower()

    # Number words
    word_to_num = {"two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
                   "seven": 7, "eight": 8, "nine": 9, "ten": 10}

    # "Three Films by..." or "3 Films by..." anywhere in name
    m = re.search(r"(\d+)\s+films?\b", lower)
    if m:
        return int(m.group(1))
    for word, num in word_to_num.items():
        if re.search(rf"\b{word}\s+films?\b", lower):
            return num

    # "Trilogy" -> 3, "Double Feature" -> 2
    if "trilogy" in lower:
        return 3
    if "double feature" in lower:
        return 2

    # "Six Moral Tales" etc. - number word + noun
    for word, num in word_to_num.items():
        if lower.startswith(word + " "):
            return num
    m = re.match(r"(\d+)\s+", lower)
    if m:
        return int(m.group(1))

    # "X / Y" or "X/Y" (two-film sets like "La Jetée/Sans Soleil")
    if "/" in name and "eclipse" not in lower:
        parts = [p.strip() for p in name.split("/") if p.strip()]
        if 2 <= len(parts) <= 3:
            return len(parts)

    return None


def main():
    parser = argparse.ArgumentParser(description="Fix box set data")
    parser.add_argument("--dry-run", action="store_true", help="Preview without saving")
    args = parser.parse_args()

    catalog = load_json(CATALOG_FILE)
    picks = load_json(PICKS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)

    if not picks:
        log("No picks to process")
        return

    name_map = build_canonical_name_map(picks_raw)
    url_map = build_url_map(picks_raw)
    film_id_to_url = build_film_id_to_url(picks_raw)

    # Also build name -> URL using the name_map for reverse lookups
    # (variant name -> canonical name -> URL)
    canonical_url_map: dict[str, str] = {}
    canonical_url_map.update(url_map)
    for variant, canonical in name_map.items():
        if canonical in url_map:
            canonical_url_map[variant] = url_map[canonical]

    # ---- Step 1: Name canonicalization in picks ----
    log("=== Step 1: Name canonicalization ===")
    name_changes = 0
    for pick in picks:
        # Normalize smart quotes first
        for field in ("box_set_name", "film_title"):
            val = pick.get(field, "")
            if val:
                normalized = normalize_smart_quotes(val)
                if normalized != val:
                    pick[field] = normalized

        # Apply name map
        bsn = pick.get("box_set_name", "")
        if bsn and bsn in name_map:
            new_name = name_map[bsn]
            if pick.get("film_title") == bsn:
                pick["film_title"] = new_name
            pick["box_set_name"] = new_name
            name_changes += 1

        # Also canonicalize film_title for unit picks where it matches a variant
        ft = pick.get("film_title", "")
        if ft and ft in name_map and ft == pick.get("box_set_name", ""):
            # Already handled above
            pass
        elif ft and ft in name_map:
            pick["film_title"] = name_map[ft]

    log(f"  Renamed {name_changes} box_set_name entries")

    # ---- Step 2: Add is_box_set to catalog ----
    log("=== Step 2: Mark is_box_set in catalog ===")
    catalog_by_id = {c["film_id"]: c for c in catalog}
    marked_count = 0

    # Method 1: criterion_url contains /boxsets/
    for entry in catalog:
        url = entry.get("criterion_url", "") or ""
        if "/boxsets/" in url:
            if not entry.get("is_box_set"):
                entry["is_box_set"] = True
                marked_count += 1

    # Method 2: film_id appears as a box set in picks_raw
    for fid, url in film_id_to_url.items():
        if fid in catalog_by_id and not catalog_by_id[fid].get("is_box_set"):
            catalog_by_id[fid]["is_box_set"] = True
            # Also set the criterion_url if missing
            if not catalog_by_id[fid].get("criterion_url"):
                catalog_by_id[fid]["criterion_url"] = url
            marked_count += 1

    log(f"  Marked {marked_count} catalog entries as is_box_set")

    # ---- Step 3: Fix stale URLs ----
    log("=== Step 3: Fix stale URLs ===")
    url_fixes = 0
    for fid, correct_url in STALE_URL_FIXES.items():
        if fid in catalog_by_id:
            old_url = catalog_by_id[fid].get("criterion_url", "")
            if old_url != correct_url:
                log(f"  {fid}: {old_url} -> {correct_url}")
                catalog_by_id[fid]["criterion_url"] = correct_url
                url_fixes += 1
                # Also fix in picks
                for pick in picks:
                    if pick.get("film_id") == fid:
                        pick["criterion_film_url"] = correct_url

    # Fix catalog entries whose URL doesn't match picks_raw
    # (skip entries already handled by STALE_URL_FIXES)
    already_fixed = set(STALE_URL_FIXES.keys())
    for entry in catalog:
        fid = entry.get("film_id", "")
        if fid in already_fixed:
            continue
        if fid in film_id_to_url:
            raw_url = film_id_to_url[fid]
            current_url = entry.get("criterion_url", "")
            if current_url and current_url != raw_url and "/boxsets/" in raw_url:
                log(f"  {fid}: {current_url} -> {raw_url}")
                entry["criterion_url"] = raw_url
                url_fixes += 1

    log(f"  Fixed {url_fixes} stale URLs")

    # ---- Step 4: Propagate box_set_criterion_url ----
    log("=== Step 4: Propagate box_set_criterion_url ===")
    url_propagated = 0
    for pick in picks:
        if not pick.get("is_box_set"):
            continue
        if pick.get("box_set_criterion_url"):
            continue

        bsn = pick.get("box_set_name", "")
        fid = pick.get("film_id", "")

        # Try URL from name map
        url = canonical_url_map.get(bsn)

        # Try URL from film_id
        if not url and fid:
            url = film_id_to_url.get(fid)

        # Try URL from catalog entry
        if not url and fid and fid in catalog_by_id:
            cat_url = catalog_by_id[fid].get("criterion_url", "")
            if "/boxsets/" in cat_url:
                url = cat_url

        if url:
            pick["box_set_criterion_url"] = url
            url_propagated += 1

    log(f"  Propagated {url_propagated} box_set_criterion_url values")

    # ---- Step 5: Convert unit picks to proper aggregates ----
    log("=== Step 5: Convert unit picks to aggregates ===")
    converted = 0
    quotes_preserved = 0

    # Count how many films are in each box set from TWO sources:
    # 1. picks_raw: individual film entries with a box_set_name
    # 2. catalog: parenthetical annotations like "Aparajito (Apu Trilogy)"
    box_set_film_counts: dict[str, set[str]] = defaultdict(set)
    box_set_film_titles: dict[str, set[str]] = defaultdict(set)

    # Source 1: picks_raw sub-film entries
    for p in picks_raw:
        bsn = p.get("box_set_name", "")
        if bsn:
            bsn = normalize_smart_quotes(bsn)
            if bsn in name_map:
                bsn = name_map[bsn]
            ft = p.get("film_title", "")
            fid = p.get("film_id", "")
            # Don't count the box set itself as a film
            if ft != bsn:
                box_set_film_counts[bsn].add(fid or ft)
                box_set_film_titles[bsn].add(ft)

    # Source 2: catalog parenthetical annotations
    for entry in catalog:
        title = entry.get("title", "")
        fid = entry.get("film_id", "")
        m = re.search(r"\(([^)]+)\)$", title)
        if m:
            parent_name = m.group(1)
            parent_name = normalize_smart_quotes(parent_name)
            if parent_name in name_map:
                parent_name = name_map[parent_name]
            # Extract the base title (without the parenthetical)
            base_title = title[: m.start()].strip()
            box_set_film_counts[parent_name].add(fid)
            box_set_film_titles[parent_name].add(base_title)

    for pick in picks:
        ft = pick.get("film_title", "")
        bsn = pick.get("box_set_name", "")

        # Unit pick: film_title matches box_set_name (the guest picked the whole set)
        if not (ft and bsn and ft == bsn):
            continue
        # Skip if already an aggregate
        if pick.get("box_set_film_count"):
            continue

        pick["is_box_set"] = True

        # Set film count from combined data
        film_ids = box_set_film_counts.get(bsn, set())
        titles = box_set_film_titles.get(bsn, set())
        if film_ids:
            pick["box_set_film_count"] = len(film_ids)
            pick["box_set_film_titles"] = sorted(titles) if titles else sorted(film_ids)
        else:
            # Infer count from name for common patterns
            count = _infer_film_count_from_name(bsn)
            if count:
                pick["box_set_film_count"] = count
            else:
                # Mark as aggregate even without exact count (frontend uses truthiness)
                pick["box_set_film_count"] = -1  # unknown count

        # Preserve the quote (key difference from old group_box_sets.py)
        if pick.get("quote"):
            quotes_preserved += 1

        converted += 1

    log(f"  Converted {converted} unit picks to aggregates")
    log(f"  Preserved {quotes_preserved} quotes on aggregates")

    # ---- Summary ----
    total_box_set_picks = sum(1 for p in picks if p.get("is_box_set"))
    total_aggregates = sum(1 for p in picks if p.get("box_set_film_count"))
    total_with_url = sum(1 for p in picks if p.get("box_set_criterion_url"))

    log(f"\n=== Summary ===")
    log(f"  Total box set picks: {total_box_set_picks}")
    log(f"  Aggregates (has box_set_film_count): {total_aggregates}")
    log(f"  Picks with box_set_criterion_url: {total_with_url}")

    if not args.dry_run:
        save_json(PICKS_FILE, picks)
        save_json(CATALOG_FILE, catalog)
        log(f"\nSaved {PICKS_FILE} and {CATALOG_FILE}")
    else:
        log("\n(dry run - no files modified)")


if __name__ == "__main__":
    main()
