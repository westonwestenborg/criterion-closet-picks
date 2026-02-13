#!/usr/bin/env python3
"""
Normalize guest data: merge duplicates, fix wrong videos, clean names,
tag non-person entities, and build visits arrays.

Idempotent — safe to re-run. Modifies guests.json, picks.json, picks_raw.json in place.

Run: python scripts/normalize_guests.py [--dry-run]
"""

import argparse
import copy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.utils import GUESTS_FILE, PICKS_FILE, PICKS_RAW_FILE, load_json, save_json, log


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Repeat visits: merge secondary into primary, build visits array
REPEAT_VISIT_MERGES = {
    "guillermo-del-toro": "guillermo-del-toros-second",
    "yorgos-lanthimos": "yorgos-lanthimos-second",
    "ari-aster": "ari-asters-second",
    "edgar-wright": "edgar-wrights-second",
    "benny-safdie": "benny-safdies-second",
    "michael-cera": "michael-ceras-second",
    "griffin-dunne": "griffin-dunnes-second",
    "isabelle-huppert": "isabelle-hupperts-second",
    "barry-jenkins": "barry-jenkins-second",
    "bill-hader": "bill-haders-second",
    "wim-wenders": "wim-wenders-second",
}

# Name-variant duplicates: same video, different name format
NAME_VARIANT_MERGES = {
    "mary-steenburgen-and-ted-danson": "ted-danson-mary-steenburgen",
    "john-david-washington-malcolm-washington": "john-david-washington-and-malcolm-washington",
    "roger-and-james-deakins": "roger-james-deakins",
    "katya-zamolodchikova": "katya-zamolodchikovas-closet-picks",
}

# Solo-into-pair merges: solo entry was wrongly extracted from pair list
SOLO_INTO_PAIR_MERGES = {
    "seth-rogen-evan-goldberg": "seth-rogen",
    "juliette-binoche-ralph-fiennes": "juliette-binoche",
    "laura-albert-jeff-feuerzeig": "laura-albert",
    "marianne-jean-baptiste-mike-leigh": "mike-leigh",
    "lily-gladstone-erica-tremblay": "lily-gladstone",
}

# Two solo entries that shared a video -> create new pair entry
NEW_PAIR = {
    "slug": "john-early-jacqueline-novak",
    "name": "John Early and Jacqueline Novak",
    "from_slugs": ["john-early", "jacqueline-novak"],
    "shared_video": "k9c6EUVVWjU",
}

# Wrong video assignments: null out video fields
WRONG_VIDEO_FIXES = {
    "matt-johnson": "U2plMSuOgrI",       # Rian Johnson's video
    "michael-mohan": "Ewx-oog2kmQ",       # Michael Shannon's video
    "lee-daniels": "dqNtp1bAI8o",          # Daniel Roher's video
}

# Slug fixes: rename slug (and update picks) to fix matching issues
SLUG_FIXES = {
    "seth-myers": "seth-meyers",  # Typo: enables match to "Seth Meyers's Closet Picks"
    "waych-shopvince-staples": "vince-staples",  # Garbled overlay text
    "watch-shop": "jeremy-pope",  # Garbled overlay text
}

# Known video IDs: set youtube_video_id for guests where we know the correct video
# These are orphan transcripts or research-confirmed videos that fuzzy matching missed
KNOWN_VIDEO_IDS = {
    # Orphan transcripts already on disk
    "francois-girard": {"youtube_video_id": "frNkePt5Eac"},
    "janelle-monae": {"youtube_video_id": "iBpffnfc3AI"},
    "roger-and-james-deakins": {"youtube_video_id": "juhiRVORJ8o"},
    "lila-aviles": {"youtube_video_id": "0PMTo4SK__s"},
    "hlynur-palmason": {"youtube_video_id": "gowVPl5YriI"},
    # Research-confirmed unmatched playlist videos
    "tracy-letts": {"youtube_video_id": "EQ7jDVdL5Ko"},
    "seth-meyers": {"youtube_video_id": "-pfZlhfY4JM"},
    "franklin-leonard": {"youtube_video_id": "5YfyrBaAWpE"},
}

# Known Criterion page URLs for guests missing them
KNOWN_CRITERION_URLS = {
    "francois-girard": "https://www.criterion.com/shop/collection/838-francois-girard-s-closet-picks",
    "janelle-monae": "https://www.criterion.com/shop/collection/733-janelle-monae-s-closet-picks",
    "roger-and-james-deakins": "https://www.criterion.com/shop/collection/566-roger-james-deakins-s-closet-picks",
    "tracy-letts": None,  # Unknown
    "seth-meyers": None,  # Unknown
    "franklin-leonard": None,  # Unknown
    "robert-eggers": "https://www.criterion.com/shop/collection/769-robert-eggers-s-closet-picks",
    "todd-haynes": "https://www.criterion.com/shop/collection/591-todd-haynes-s-closet-picks",
    "hans-zimmer": "https://www.criterion.com/shop/collection/793-hans-zimmer-s-closet-picks",
    "ryusuke-hamaguchi": "https://www.criterion.com/shop/collection/652-ryusuke-hamaguchi-s-closet-picks",
    "gael-garca-bernal": "https://www.criterion.com/shop/collection/616-gael-garcia-bernal-s-closet-picks",
    "karina-longworth": "https://www.criterion.com/shop/collection/455-karina-longworth-s-closet-picks",
    "anton-corbijn": "https://www.criterion.com/shop/collection/571-anton-corbijn-s-closet-picks",
    "joel-potrykus": "https://www.criterion.com/shop/collection/806-joel-potrykus-s-closet-picks",
    "coralie-fargeat": None,  # Unknown
    "isabella-rossellini": None,  # Unknown
    "pablo-larran": "https://www.criterion.com/shop/collection/766-pablo-larrain-s-closet-picks",
    "pauline-chalamet": "https://www.criterion.com/shop/collection/536-pauline-chalamets-closet-picks",
    "india-donaldson-and-lily-collias": None,  # Unknown
    "tracy-letts": "https://www.criterion.com/shop/collection/879-tracy-letts-s-closet-picks",
    "franklin-leonard": "https://www.criterion.com/shop/collection/802-franklin-leonard-s-closet-picks",
    "lee-daniels": "https://www.criterion.com/shop/collection/682-lee-daniels-s-closet-picks",
}

# Name cleanup
NAME_FIXES = {
    "David and Nathan Zellner's Criterion Picks": "David and Nathan Zellner",
    "Katya Zamolodchikova's Closet Picks": "Katya Zamolodchikova",
    "Seth Myers": "Seth Meyers",  # Fix corresponding name
    "Waych & shopVince Staples": "Vince Staples",  # Garbled overlay text
    "Watch& & shop": "Jeremy Pope",  # Garbled overlay text
    "Claire Denis'": "Claire Denis",  # Trailing apostrophe from URL extraction
    "Five Comics'": "Five Comics",  # Trailing apostrophe from URL extraction
}

# Import multi-visit URLs from shared config
from scripts.utils import VISIT_CRITERION_URLS

# Non-person tagging
GUEST_TYPE_TAGS = {
    "that-one-time-five-comics-raided-the-criterion-closet": "group",
    "m3gan": "character",
    "letterboxd-visits-the-criterion-mobile-closet-at-sxsw": "event",
    "letterboxd": "event",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def guest_by_slug(guests: list[dict], slug: str) -> dict | None:
    for g in guests:
        if g["slug"] == slug:
            return g
    return None


def remove_guest(guests: list[dict], slug: str) -> dict | None:
    for i, g in enumerate(guests):
        if g["slug"] == slug:
            return guests.pop(i)
    return None


def build_visit(guest: dict) -> dict:
    """Extract a visit record from a guest entry."""
    return {
        "youtube_video_id": guest.get("youtube_video_id"),
        "youtube_video_url": guest.get("youtube_video_url"),
        "vimeo_video_id": guest.get("vimeo_video_id"),
        "episode_date": guest.get("episode_date"),
        "letterboxd_list_url": guest.get("letterboxd_list_url"),
        "criterion_page_url": guest.get("criterion_page_url"),
    }


def merge_guest_fields(primary: dict, secondary: dict):
    """Copy non-null fields from secondary into primary (don't overwrite existing)."""
    for key in ["profession", "photo_url", "episode_date"]:
        if not primary.get(key) and secondary.get(key):
            primary[key] = secondary[key]


def update_picks_guest_slug(picks: list[dict], old_slug: str, new_slug: str) -> int:
    """Update guest_slug from old to new. Returns count changed."""
    count = 0
    for p in picks:
        if p.get("guest_slug") == old_slug:
            p["guest_slug"] = new_slug
            count += 1
    return count


def dedup_picks(picks: list[dict]) -> list[dict]:
    """Deduplicate picks by (guest_slug, film_slug/film_id), keeping highest confidence."""
    confidence_rank = {"high": 3, "medium": 2, "low": 1, "none": 0}
    seen: dict[tuple, int] = {}
    result = []

    for i, p in enumerate(picks):
        film_key = p.get("film_slug") or p.get("film_id", "")
        key = (p["guest_slug"], film_key)

        conf = confidence_rank.get(p.get("extraction_confidence", "none"), 0)

        if key in seen:
            existing_idx = seen[key]
            existing_conf = confidence_rank.get(
                result[existing_idx].get("extraction_confidence", "none"), 0
            )
            if conf > existing_conf:
                result[existing_idx] = p
        else:
            seen[key] = len(result)
            result.append(p)

    return result


def dedup_picks_raw(picks_raw: list[dict]) -> list[dict]:
    """Deduplicate raw picks by (guest_slug, film_id). Prefer criterion-sourced entries."""
    seen: dict[tuple, int] = {}
    result = []
    for p in picks_raw:
        key = (p["guest_slug"], p.get("film_id", ""))
        if key not in seen:
            seen[key] = len(result)
            result.append(p)
        else:
            # Prefer criterion-sourced entry over letterboxd
            existing_idx = seen[key]
            if p.get("source") == "criterion" and result[existing_idx].get("source") != "criterion":
                result[existing_idx] = p
    return result


def recompute_pick_counts(guests: list[dict], picks: list[dict], picks_raw: list[dict]):
    """Recompute pick_count for every guest from actual picks data."""
    counts: dict[str, int] = {}
    for p in picks:
        slug = p["guest_slug"]
        counts[slug] = counts.get(slug, 0) + 1

    # Also count from raw picks for guests not in processed picks
    raw_counts: dict[str, int] = {}
    for p in picks_raw:
        slug = p["guest_slug"]
        raw_counts[slug] = raw_counts.get(slug, 0) + 1

    for g in guests:
        slug = g["slug"]
        g["pick_count"] = counts.get(slug, raw_counts.get(slug, 0))


# ---------------------------------------------------------------------------
# Main normalization
# ---------------------------------------------------------------------------

def normalize(dry_run: bool = False):
    guests = load_json(GUESTS_FILE)
    picks = load_json(PICKS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)

    stats = {
        "repeat_merges": 0,
        "name_variant_merges": 0,
        "solo_pair_merges": 0,
        "new_pairs": 0,
        "wrong_video_fixes": 0,
        "name_fixes": 0,
        "guest_type_tags": 0,
        "picks_reassigned": 0,
        "raw_picks_reassigned": 0,
    }

    # --- 0. Slug fixes (do first so everything else uses correct slugs) ---
    for old_slug, new_slug in SLUG_FIXES.items():
        g = guest_by_slug(guests, old_slug)
        if g:
            log(f"  Slug fix: '{old_slug}' -> '{new_slug}'")
            g["slug"] = new_slug
            n = update_picks_guest_slug(picks, old_slug, new_slug)
            stats["picks_reassigned"] += n
            n = update_picks_guest_slug(picks_raw, old_slug, new_slug)
            stats["raw_picks_reassigned"] += n

    # --- 1. Name cleanup (do first so merges work on clean names) ---
    for g in guests:
        if g["name"] in NAME_FIXES:
            old_name = g["name"]
            g["name"] = NAME_FIXES[old_name]
            log(f"  Name fix: '{old_name}' -> '{g['name']}'")
            stats["name_fixes"] += 1

    # --- 2. Build visits arrays for multi-visit guests ---
    for slug, urls in VISIT_CRITERION_URLS.items():
        g = guest_by_slug(guests, slug)
        if not g:
            log(f"  Skip visits build: '{slug}' not found")
            continue

        if len(urls) < 2:
            # Single-visit guest, just ensure criterion_page_url is set
            if not g.get("criterion_page_url"):
                g["criterion_page_url"] = urls[0]
            continue

        # Preserve existing visit data (e.g., video IDs set by match_youtube)
        existing_visits = g.get("visits", [])

        # Build visit 1 from the guest's current data
        visit1 = build_visit(g)
        visit1["criterion_page_url"] = urls[0]
        # Preserve visit 1 data if already populated
        if existing_visits and existing_visits[0].get("episode_date"):
            visit1["episode_date"] = existing_visits[0]["episode_date"]

        # Build visit 2 — preserve existing if match_youtube already set video IDs
        if len(existing_visits) > 1 and (existing_visits[1].get("youtube_video_id") or existing_visits[1].get("vimeo_video_id")):
            visit2 = existing_visits[1].copy()
            visit2["criterion_page_url"] = urls[1]
        else:
            visit2 = {
                "youtube_video_id": None,
                "youtube_video_url": None,
                "vimeo_video_id": None,
                "episode_date": None,
                "criterion_page_url": urls[1],
            }

        secondary_slug = REPEAT_VISIT_MERGES.get(slug)
        if secondary_slug:
            secondary = guest_by_slug(guests, secondary_slug)
            if secondary:
                log(f"  Repeat merge: '{secondary['name']}' -> '{g['name']}'")
                visit2 = build_visit(secondary)
                visit2["criterion_page_url"] = urls[1]
                merge_guest_fields(g, secondary)
                n = update_picks_guest_slug(picks, secondary_slug, slug)
                stats["picks_reassigned"] += n
                n = update_picks_guest_slug(picks_raw, secondary_slug, slug)
                stats["raw_picks_reassigned"] += n
                remove_guest(guests, secondary_slug)

        # Detect and clear duplicate video IDs across visits
        v1_vid = visit1.get("youtube_video_id")
        v2_vid = visit2.get("youtube_video_id")
        if v1_vid and v1_vid == v2_vid:
            log(f"  Dedup video: '{g['name']}' visit 2 had same video as visit 1, clearing")
            visit2["youtube_video_id"] = None
            visit2["youtube_video_url"] = None

        g["visits"] = [visit1, visit2]
        g["criterion_page_url"] = urls[0]
        log(f"  Built visits for '{g['name']}': {len(urls)} visit(s)")
        stats["repeat_merges"] += 1

    # --- 3. Name-variant merges ---
    for primary_slug, secondary_slug in NAME_VARIANT_MERGES.items():
        primary = guest_by_slug(guests, primary_slug)
        secondary = guest_by_slug(guests, secondary_slug)

        if not primary:
            log(f"  Skip name-variant merge: primary '{primary_slug}' not found")
            continue
        if not secondary:
            log(f"  Skip name-variant merge: secondary '{secondary_slug}' not found (already merged?)")
            continue

        log(f"  Name-variant merge: '{secondary['name']}' -> '{primary['name']}'")
        merge_guest_fields(primary, secondary)

        n = update_picks_guest_slug(picks, secondary_slug, primary_slug)
        stats["picks_reassigned"] += n
        n = update_picks_guest_slug(picks_raw, secondary_slug, primary_slug)
        stats["raw_picks_reassigned"] += n

        remove_guest(guests, secondary_slug)
        stats["name_variant_merges"] += 1

    # --- 4. Solo-into-pair merges ---
    for pair_slug, solo_slug in SOLO_INTO_PAIR_MERGES.items():
        pair = guest_by_slug(guests, pair_slug)
        solo = guest_by_slug(guests, solo_slug)

        if not pair:
            log(f"  Skip solo-pair merge: pair '{pair_slug}' not found")
            continue
        if not solo:
            log(f"  Skip solo-pair merge: solo '{solo_slug}' not found (already merged?)")
            continue

        log(f"  Solo-pair merge: '{solo['name']}' -> '{pair['name']}'")
        merge_guest_fields(pair, solo)

        n = update_picks_guest_slug(picks, solo_slug, pair_slug)
        stats["picks_reassigned"] += n
        n = update_picks_guest_slug(picks_raw, solo_slug, pair_slug)
        stats["raw_picks_reassigned"] += n

        remove_guest(guests, solo_slug)
        stats["solo_pair_merges"] += 1

    # --- 5. New pair entry (john-early + jacqueline-novak) ---
    existing_pair = guest_by_slug(guests, NEW_PAIR["slug"])
    if not existing_pair:
        source_guests = []
        for src_slug in NEW_PAIR["from_slugs"]:
            g = guest_by_slug(guests, src_slug)
            if g:
                source_guests.append(g)
            else:
                log(f"  Skip new pair: source '{src_slug}' not found")

        if len(source_guests) == len(NEW_PAIR["from_slugs"]):
            # Create new pair entry from first source as template
            new_guest = copy.deepcopy(source_guests[0])
            new_guest["name"] = NEW_PAIR["name"]
            new_guest["slug"] = NEW_PAIR["slug"]
            new_guest["youtube_video_id"] = NEW_PAIR["shared_video"]
            new_guest["youtube_video_url"] = f"https://www.youtube.com/watch?v={NEW_PAIR['shared_video']}"

            # Merge fields from all sources
            for src in source_guests[1:]:
                merge_guest_fields(new_guest, src)

            guests.append(new_guest)

            # Reassign picks from both sources
            for src_slug in NEW_PAIR["from_slugs"]:
                n = update_picks_guest_slug(picks, src_slug, NEW_PAIR["slug"])
                stats["picks_reassigned"] += n
                n = update_picks_guest_slug(picks_raw, src_slug, NEW_PAIR["slug"])
                stats["raw_picks_reassigned"] += n
                remove_guest(guests, src_slug)

            log(f"  New pair: '{NEW_PAIR['name']}' from {NEW_PAIR['from_slugs']}")
            stats["new_pairs"] += 1
    else:
        log(f"  Skip new pair: '{NEW_PAIR['slug']}' already exists")

    # --- 6. Wrong video fixes ---
    for slug, wrong_video in WRONG_VIDEO_FIXES.items():
        g = guest_by_slug(guests, slug)
        if not g:
            log(f"  Skip wrong video fix: '{slug}' not found")
            continue

        if g.get("youtube_video_id") == wrong_video:
            log(f"  Wrong video fix: '{g['name']}' — nulling video {wrong_video}")
            g["youtube_video_id"] = None
            g["youtube_video_url"] = None
            g["vimeo_video_id"] = None
            stats["wrong_video_fixes"] += 1
        elif g.get("youtube_video_id") is None:
            log(f"  Skip wrong video fix: '{g['name']}' already has no video")
        else:
            log(f"  Skip wrong video fix: '{g['name']}' has different video '{g.get('youtube_video_id')}'")

    # --- 6b. Known video ID fixes ---
    for slug, video_info in KNOWN_VIDEO_IDS.items():
        g = guest_by_slug(guests, slug)
        if not g:
            log(f"  Skip known video fix: '{slug}' not found")
            continue
        if g.get("youtube_video_id"):
            continue  # Already has a video, don't overwrite

        yt_id = video_info.get("youtube_video_id")
        if yt_id:
            g["youtube_video_id"] = yt_id
            g["youtube_video_url"] = f"https://www.youtube.com/watch?v={yt_id}"
            log(f"  Known video fix: '{g['name']}' -> {yt_id}")

    # --- 6c. Known Criterion page URL fixes ---
    for slug, url in KNOWN_CRITERION_URLS.items():
        if not url:
            continue
        g = guest_by_slug(guests, slug)
        if not g:
            log(f"  Skip known criterion URL: '{slug}' not found")
            continue
        if g.get("criterion_page_url"):
            continue  # Already has a URL, don't overwrite
        g["criterion_page_url"] = url
        log(f"  Known criterion URL: '{g['name']}' -> {url}")

    # --- 7. Non-person tagging ---
    for slug, guest_type in GUEST_TYPE_TAGS.items():
        g = guest_by_slug(guests, slug)
        if g:
            if g.get("guest_type") != guest_type:
                g["guest_type"] = guest_type
                log(f"  Guest type: '{g['name']}' -> {guest_type}")
                stats["guest_type_tags"] += 1
        else:
            log(f"  Skip guest type tag: '{slug}' not found")

    # --- 8. Fix Criterion page URLs per visit ---
    for slug, urls in VISIT_CRITERION_URLS.items():
        g = guest_by_slug(guests, slug)
        if not g:
            log(f"  Skip criterion URL fix: '{slug}' not found")
            continue

        changed = False

        # Set top-level criterion_page_url to visit 1 (oldest)
        if g.get("criterion_page_url") != urls[0]:
            g["criterion_page_url"] = urls[0]
            changed = True

        # Update visits array if present
        if g.get("visits"):
            for i, visit in enumerate(g["visits"]):
                url = urls[i] if i < len(urls) else None
                if visit.get("criterion_page_url") != url:
                    visit["criterion_page_url"] = url
                    changed = True

        if changed:
            log(f"  Criterion URL fix: '{g['name']}' — {len(urls)} page(s)")

    # --- 9. Dedup picks ---
    before_picks = len(picks)
    picks = dedup_picks(picks)
    after_picks = len(picks)
    if before_picks != after_picks:
        log(f"  Deduped picks: {before_picks} -> {after_picks}")

    before_raw = len(picks_raw)
    picks_raw = dedup_picks_raw(picks_raw)
    after_raw = len(picks_raw)
    if before_raw != after_raw:
        log(f"  Deduped raw picks: {before_raw} -> {after_raw}")

    # --- 9. Recompute pick counts ---
    recompute_pick_counts(guests, picks, picks_raw)

    # --- Summary ---
    log(f"\nNormalization summary:")
    log(f"  Repeat-visit merges: {stats['repeat_merges']}")
    log(f"  Name-variant merges: {stats['name_variant_merges']}")
    log(f"  Solo-pair merges: {stats['solo_pair_merges']}")
    log(f"  New pair entries: {stats['new_pairs']}")
    log(f"  Wrong video fixes: {stats['wrong_video_fixes']}")
    log(f"  Name fixes: {stats['name_fixes']}")
    log(f"  Guest type tags: {stats['guest_type_tags']}")
    log(f"  Picks reassigned: {stats['picks_reassigned']}")
    log(f"  Raw picks reassigned: {stats['raw_picks_reassigned']}")
    log(f"  Final: {len(guests)} guests, {len(picks)} picks, {len(picks_raw)} raw picks")

    if dry_run:
        log("\nDry run — no files written.")
    else:
        save_json(GUESTS_FILE, guests)
        save_json(PICKS_FILE, picks)
        save_json(PICKS_RAW_FILE, picks_raw)
        log(f"\nSaved to {GUESTS_FILE}, {PICKS_FILE}, {PICKS_RAW_FILE}")


def main():
    parser = argparse.ArgumentParser(description="Normalize guest data")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without writing files")
    args = parser.parse_args()
    normalize(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
