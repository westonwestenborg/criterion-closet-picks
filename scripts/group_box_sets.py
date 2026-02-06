#!/usr/bin/env python3
"""
Group box set films into single entries per guest.

Detection methods:
1. Catalog parenthetical annotations (e.g., "Aparajito (Apu Trilogy)")
2. Known large box sets defined by film title lists
3. Consecutive-run heuristic: 5+ consecutive films in a guest's list that
   share the same catalog-annotated box set name

When a guest has multiple films from the same box set:
- Creates ONE box set entry with is_box_set=True
- Films the guest specifically discussed (has a high/medium confidence quote)
  stay as separate entries too, tagged with box_set_name
- The box set entry aggregates the undiscussed films

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
    load_json,
    save_json,
    log,
)

# Minimum number of films to trigger consecutive-run box set detection
RUN_THRESHOLD = 5

# Known large box sets: title -> list of film titles
# These are detected by matching film titles in a guest's picks
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
    "Five Films by Cassavetes": [
        "Shadows", "Faces", "A Woman Under the Influence",
        "The Killing of a Chinese Bookie", "Opening Night",
    ],
}

# Known Criterion box set URLs
KNOWN_BOX_SET_URLS = {
    "Ingmar Bergman's Cinema": "https://www.criterion.com/boxsets/2575-ingmar-bergman-s-cinema",
    "Essential Fellini": "https://www.criterion.com/boxsets/2839-essential-fellini",
    "Apu Trilogy": "https://www.criterion.com/boxsets/1702-the-apu-trilogy",
    "Jacques Demy box": "https://www.criterion.com/boxsets/1477-the-essential-jacques-demy",
    "The BRD Trilogy": "https://www.criterion.com/boxsets/389-the-brd-trilogy",
    "A Film Trilogy by Ingmar Bergman": "https://www.criterion.com/boxsets/393-a-film-trilogy-by-ingmar-bergman",
    "Roberto Rossellini's War Trilogy": "https://www.criterion.com/boxsets/1265-roberto-rossellini-s-war-trilogy",
    "The Three Colors Trilogy": "https://www.criterion.com/boxsets/1538-three-colors",
    "The Before Trilogy": "https://www.criterion.com/boxsets/2164-the-before-trilogy",
    "Wim Wenders box": "https://www.criterion.com/boxsets/1872-wim-wenders-road-trilogy",
    "Five Films by Cassavetes": "https://www.criterion.com/boxsets/298-five-films-by-john-cassavetes",
    "World Cinema Project": "https://www.criterion.com/boxsets/1519-martin-scorsese-s-world-cinema-project",
    "Fanny and Alexander Box Set": "https://www.criterion.com/films/28561-fanny-and-alexander",
    "Melvin Van Peebles: Four Films box": "https://www.criterion.com/boxsets/3206-melvin-van-peebles-four-films",
    "Monte Hellman set": "https://www.criterion.com/boxsets/1470-monte-hellman-two-by-two",
    "Dietrich box": "https://www.criterion.com/boxsets/2320-dietrich-von-sternberg-in-hollywood",
    "Marseille Trilogy": "https://www.criterion.com/boxsets/2182-the-marseille-trilogy",
}


def extract_box_set_name(catalog_title: str) -> str | None:
    """Extract box set name from parenthetical annotation in catalog title."""
    m = re.search(r"\(([^)]+)\)$", catalog_title)
    if m:
        name = m.group(1)
        box_keywords = ["trilogy", "box", "set", "double feature", "cinema project", "films"]
        if any(kw in name.lower() for kw in box_keywords):
            return name
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
    # Method 1: catalog annotation
    film_id = pick.get("film_id", "")
    if film_id in catalog_map:
        return catalog_map[film_id]

    # Method 2: known box set by title
    film_title = pick.get("film_title", "").lower()
    if film_title in known_title_map:
        return known_title_map[film_title]

    return None


def group_picks_for_guest(
    guest_picks: list[dict],
    catalog_map: dict[str, str],
    known_title_map: dict[str, str],
) -> list[dict]:
    """
    Group a guest's picks, collapsing box set films into single entries.
    Films with high/medium confidence quotes stay as separate entries.
    """
    # Tag each pick with its box set (if any)
    box_set_groups = defaultdict(list)
    standalone_picks = []

    for pick in guest_picks:
        box_set_name = detect_box_set_for_pick(pick, catalog_map, known_title_map)

        has_quote = bool(pick.get("quote", "").strip())
        has_confidence = pick.get("extraction_confidence") in ("high", "medium")

        if box_set_name and not (has_quote and has_confidence):
            # Part of a box set, not individually discussed -> group
            box_set_groups[box_set_name].append(pick)
        else:
            # Keep as standalone
            if box_set_name:
                pick["is_box_set"] = True
                pick["box_set_name"] = box_set_name
            standalone_picks.append(pick)

    # Only create box set summary entries if we actually grouped films
    # (if threshold not met, put them back as standalone)
    for box_set_name, grouped_picks in box_set_groups.items():
        if len(grouped_picks) < 2:
            # Not enough to justify grouping, keep as standalone
            standalone_picks.extend(grouped_picks)
            continue

        film_titles = [p.get("film_title", "") for p in grouped_picks]

        # Use the first pick as template for the box set entry
        template = grouped_picks[0].copy()
        template["is_box_set"] = True
        template["box_set_name"] = box_set_name
        template["box_set_film_count"] = len(grouped_picks)
        template["box_set_film_titles"] = film_titles
        template["film_title"] = box_set_name
        template["quote"] = ""
        template["extraction_confidence"] = "none"
        template["youtube_timestamp_url"] = ""

        # Criterion URL for this box set
        box_url = KNOWN_BOX_SET_URLS.get(box_set_name)
        if box_url:
            template["box_set_criterion_url"] = box_url

        standalone_picks.append(template)

    return standalone_picks


def main():
    parser = argparse.ArgumentParser(description="Group box set films")
    parser.add_argument("--dry-run", action="store_true", help="Preview without saving")
    args = parser.parse_args()

    catalog = load_json(CATALOG_FILE)
    picks = load_json(PICKS_FILE)

    if not picks:
        log("No picks to process")
        return

    catalog_map = build_catalog_box_set_map(catalog)
    known_title_map = build_known_box_set_map()
    log(f"Catalog-annotated: {len(catalog_map)} films, Known box sets: {len(known_title_map)} films")

    # Group by guest
    picks_by_guest = defaultdict(list)
    for pick in picks:
        picks_by_guest[pick["guest_slug"]].append(pick)

    all_grouped = []
    total_collapsed = 0

    for guest_slug, guest_picks in picks_by_guest.items():
        before_count = len(guest_picks)
        grouped = group_picks_for_guest(guest_picks, catalog_map, known_title_map)
        after_count = len(grouped)
        collapsed = before_count - after_count

        if collapsed > 0:
            total_collapsed += collapsed
            box_sets_found = [
                f"{p['box_set_name']} ({p['box_set_film_count']} films)"
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
