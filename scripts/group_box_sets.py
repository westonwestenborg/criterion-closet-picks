#!/usr/bin/env python3
"""
Group box set films into single entries per guest.

Detection methods:
1. Picks already tagged with is_box_set/box_set_name from scrapers
2. Catalog parenthetical annotations (e.g., "Aparajito (Apu Trilogy)")
3. Known large box sets defined by film title lists

When a guest picked a box set as a unit (film_title == box_set_name):
- Converted to an aggregate entry with quote preserved
- box_set_film_count set from catalog/raw data or inferred from name

When a guest picked individual films from a box set:
- Films with high/medium confidence quotes stay as separate entries
- Remaining films are collapsed into one aggregate entry

Usage:
  python scripts/group_box_sets.py                  # Group box sets in picks.json
  python scripts/group_box_sets.py --dry-run        # Preview changes
"""

import argparse
import re
import sys
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

# Known large box sets: title -> list of film titles
# Supplemental to catalog annotations for sets with many films
KNOWN_BOX_SETS = {
    "Ingmar Bergman's Cinema": [
        "Crisis", "A Ship to India", "Port of Call", "Thirst", "To Joy",
        "Summer Interlude", "Waiting Women", "Summer with Monika",
        "Sawdust and Tinsel", "A Lesson in Love", "Dreams",
        "Smiles of a Summer Night", "The Seventh Seal", "Wild Strawberries",
        "Brink of Life", "The Magician", "The Virgin Spring", "The Devil's Eye",
        "Through a Glass Darkly", "Winter Light", "The Silence", "All These Women",
        "Persona", "Hour of the Wolf", "Shame", "The Rite",
        "The Passion of Anna", "Fårö Document 1979", "The Touch",
        "Cries and Whispers", "Scenes from a Marriage", "The Magic Flute",
        "The Serpent's Egg", "Autumn Sonata", "Fårö Document",
        "From the Life of the Marionettes", "Fanny and Alexander",
        "After the Rehearsal", "Saraband",
    ],
    "Essential Fellini": [
        "Variety Lights", "The White Sheik", "I Vitelloni", "La Strada",
        "Il Bidone", "The Swindle", "Nights of Cabiria", "La Dolce Vita",
        "8½", "Juliet of the Spirits", "Fellini Satyricon", "Satyricon",
        "The Clowns", "Roma", "Amarcord", "And the Ship Sails On",
        "Intervista",
    ],
    "John Cassavetes: Five Films": [
        "Shadows", "Faces", "A Woman Under the Influence",
        "The Killing of a Chinese Bookie", "Opening Night",
    ],
    "Godzilla: The Showa-Era Films, 1954\u20131975": [
        "Godzilla", "Godzilla Raids Again", "King Kong vs. Godzilla",
        "Mothra vs. Godzilla", "Ghidorah, the Three-Headed Monster",
        "Invasion of Astro-Monster", "Ebirah, Horror of the Deep",
        "Son of Godzilla", "Destroy All Monsters", "All Monsters Attack",
        "Godzilla vs. Hedorah", "Godzilla vs. Gigan", "Godzilla vs. Megalon",
        "Godzilla vs. Mechagodzilla", "Terror of Mechagodzilla",
    ],
}


def normalize_smart_quotes(text: str) -> str:
    """Replace smart quotes with straight quotes."""
    if not text:
        return text
    return (
        text.replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
    )


def infer_film_count_from_name(name: str) -> int | None:
    """Try to infer film count from box set name patterns."""
    lower = name.lower()
    word_to_num = {"two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
                   "seven": 7, "eight": 8, "nine": 9, "ten": 10}

    m = re.search(r"(\d+)\s+films?\b", lower)
    if m:
        return int(m.group(1))
    for word, num in word_to_num.items():
        if re.search(rf"\b{word}\s+films?\b", lower):
            return num

    if "trilogy" in lower:
        return 3
    if "double feature" in lower:
        return 2

    for word, num in word_to_num.items():
        if lower.startswith(word + " "):
            return num
    m = re.match(r"(\d+)\s+", lower)
    if m:
        return int(m.group(1))

    if "/" in name and "eclipse" not in lower:
        parts = [p.strip() for p in name.split("/") if p.strip()]
        if 2 <= len(parts) <= 3:
            return len(parts)

    return None


def build_url_map(picks_raw: list[dict]) -> dict[str, str]:
    """Build box_set_name -> criterion URL map from picks_raw.json."""
    url_map: dict[str, str] = {}
    for p in picks_raw:
        url = p.get("criterion_film_url", "")
        title = normalize_smart_quotes(p.get("film_title", ""))
        if "/boxsets/" in url and title:
            if title not in url_map:
                url_map[title] = url
    return url_map


def extract_box_set_name(catalog_title: str) -> str | None:
    """Extract box set name from parenthetical annotation in catalog title."""
    m = re.search(r"\(([^)]+)\)$", catalog_title)
    if m:
        name = m.group(1)
        box_keywords = ["trilogy", "box", "set", "double feature", "cinema project", "films"]
        if any(kw in name.lower() for kw in box_keywords):
            return normalize_smart_quotes(name)
    return None


def build_catalog_box_set_map(catalog: list[dict]) -> dict[str, str]:
    """Map film_id -> box_set_name from catalog parenthetical annotations."""
    film_to_box_set = {}
    for entry in catalog:
        title = entry.get("title", "")
        film_id = entry.get("film_id", "")
        name = extract_box_set_name(title)
        if name and film_id:
            film_to_box_set[film_id] = name
    return film_to_box_set


def build_known_box_set_map() -> dict[str, str]:
    """Map lowercase film title -> box_set_name from known box set definitions."""
    title_to_box_set = {}
    for box_set_name, titles in KNOWN_BOX_SETS.items():
        for title in titles:
            title_to_box_set[title.lower()] = box_set_name
    return title_to_box_set


def detect_box_set_for_pick(
    pick: dict,
    catalog_map: dict[str, str],
    known_title_map: dict[str, str],
) -> str | None:
    """Determine if a pick belongs to a box set. Returns box set name or None."""
    # Already tagged by scrapers
    if pick.get("is_box_set") and pick.get("box_set_name"):
        return normalize_smart_quotes(pick["box_set_name"])

    # Catalog annotation
    film_id = pick.get("film_id", "")
    if film_id in catalog_map:
        return catalog_map[film_id]

    # Known box set by title
    film_title = pick.get("film_title", "").lower()
    if film_title in known_title_map:
        return known_title_map[film_title]

    return None


def group_picks_for_guest(
    guest_picks: list[dict],
    catalog_map: dict[str, str],
    known_title_map: dict[str, str],
    url_map: dict[str, str],
    catalog_by_id: dict[str, dict],
) -> list[dict]:
    """
    Group a guest's picks, collapsing box set films into single entries.
    Films with high/medium confidence quotes stay as separate entries.
    Unit picks (film_title == box_set_name) become aggregates with quotes preserved.
    """
    box_set_groups: dict[str, list[dict]] = defaultdict(list)
    standalone_picks: list[dict] = []

    for pick in guest_picks:
        box_set_name = detect_box_set_for_pick(pick, catalog_map, known_title_map)
        ft = normalize_smart_quotes(pick.get("film_title", ""))

        # Unit pick: guest picked the whole box set
        if box_set_name and ft == box_set_name:
            pick["is_box_set"] = True
            pick["box_set_name"] = box_set_name
            # Already an aggregate or convert to one
            if not pick.get("box_set_film_count"):
                count = infer_film_count_from_name(box_set_name)
                pick["box_set_film_count"] = count or -1
            # Propagate URL
            if not pick.get("box_set_criterion_url"):
                url = url_map.get(box_set_name)
                if not url:
                    fid = pick.get("film_id", "")
                    if fid in catalog_by_id:
                        cat_url = catalog_by_id[fid].get("criterion_url", "")
                        if "/boxsets/" in cat_url:
                            url = cat_url
                if url:
                    pick["box_set_criterion_url"] = url
            # Quote is preserved (not cleared)
            standalone_picks.append(pick)
            continue

        has_quote = bool(pick.get("quote", "").strip())
        has_confidence = pick.get("extraction_confidence") in ("high", "medium")

        if box_set_name and not (has_quote and has_confidence):
            box_set_groups[box_set_name].append(pick)
        else:
            if box_set_name:
                pick["is_box_set"] = True
                pick["box_set_name"] = box_set_name
            standalone_picks.append(pick)

    # Create aggregate entries for grouped films
    for box_set_name, grouped_picks in box_set_groups.items():
        if len(grouped_picks) < 2:
            standalone_picks.extend(grouped_picks)
            continue

        film_titles = [p.get("film_title", "") for p in grouped_picks]

        template = grouped_picks[0].copy()
        template["is_box_set"] = True
        template["box_set_name"] = box_set_name
        template["box_set_film_count"] = len(grouped_picks)
        template["box_set_film_titles"] = film_titles
        template["film_title"] = box_set_name
        template["quote"] = ""
        template["extraction_confidence"] = "none"
        template["youtube_timestamp_url"] = ""

        url = url_map.get(box_set_name)
        if url:
            template["box_set_criterion_url"] = url

        standalone_picks.append(template)

    # Merge duplicate box set entries (unit pick + collapsed aggregate for same set)
    seen_box_sets: dict[str, int] = {}
    merged_picks: list[dict] = []
    for pick in standalone_picks:
        bs_name = pick.get("box_set_name")
        if bs_name and pick.get("box_set_film_count"):
            if bs_name in seen_box_sets:
                # Merge into the existing entry
                existing = merged_picks[seen_box_sets[bs_name]]
                # Keep the quote from whichever has one
                if not existing.get("quote") and pick.get("quote"):
                    existing["quote"] = pick["quote"]
                    existing["start_timestamp"] = pick.get("start_timestamp", 0)
                    existing["extraction_confidence"] = pick.get("extraction_confidence", "none")
                    for url_key in ("youtube_timestamp_url", "vimeo_timestamp_url"):
                        if pick.get(url_key):
                            existing[url_key] = pick[url_key]
                # Take the higher film count (collapsed count is more accurate than -1)
                if (pick.get("box_set_film_count") or 0) > (existing.get("box_set_film_count") or 0):
                    existing["box_set_film_count"] = pick["box_set_film_count"]
                # Combine film titles
                if pick.get("box_set_film_titles") and not existing.get("box_set_film_titles"):
                    existing["box_set_film_titles"] = pick["box_set_film_titles"]
                # Take URL if missing
                if not existing.get("box_set_criterion_url") and pick.get("box_set_criterion_url"):
                    existing["box_set_criterion_url"] = pick["box_set_criterion_url"]
                continue
            seen_box_sets[bs_name] = len(merged_picks)
        merged_picks.append(pick)

    return merged_picks


def main():
    parser = argparse.ArgumentParser(description="Group box set films")
    parser.add_argument("--dry-run", action="store_true", help="Preview without saving")
    args = parser.parse_args()

    catalog = load_json(CATALOG_FILE)
    picks = load_json(PICKS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)

    if not picks:
        log("No picks to process")
        return

    catalog_map = build_catalog_box_set_map(catalog)
    catalog_by_id = {c["film_id"]: c for c in catalog}
    known_title_map = build_known_box_set_map()
    url_map = build_url_map(picks_raw)
    log(f"Catalog-annotated: {len(catalog_map)} films, Known box sets: {len(known_title_map)} films")
    log(f"URL map: {len(url_map)} box set URLs from picks_raw")

    # Group by guest
    picks_by_guest = defaultdict(list)
    for pick in picks:
        picks_by_guest[pick["guest_slug"]].append(pick)

    all_grouped = []
    total_collapsed = 0

    for guest_slug, guest_picks in picks_by_guest.items():
        before_count = len(guest_picks)
        grouped = group_picks_for_guest(guest_picks, catalog_map, known_title_map, url_map, catalog_by_id)
        after_count = len(grouped)
        collapsed = before_count - after_count

        if collapsed > 0:
            total_collapsed += collapsed
            box_sets_found = [
                f"{p['box_set_name']} ({p.get('box_set_film_count', '?')} films)"
                for p in grouped
                if p.get("is_box_set") and p.get("box_set_film_count")
            ]
            log(f"  {guest_slug}: {before_count} -> {after_count} picks ({collapsed} collapsed)")
            for bs in box_sets_found:
                log(f"    {bs}")

        all_grouped.extend(grouped)

    log(f"\nTotal: {len(picks)} -> {len(all_grouped)} picks ({total_collapsed} collapsed)")

    if not args.dry_run:
        save_json(PICKS_FILE, all_grouped)
        log(f"Saved to {PICKS_FILE}")
    else:
        log("(dry run)")


if __name__ == "__main__":
    main()
