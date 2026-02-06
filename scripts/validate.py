#!/usr/bin/env python3
"""
Validate pipeline data integrity and generate coverage reports.
Checks all data files for completeness, consistency, and quality.

Output: data/validation/report.json + console summary
"""

import argparse
import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    GUESTS_FILE,
    PICKS_RAW_FILE,
    PICKS_FILE,
    TRANSCRIPTS_DIR,
    VALIDATION_DIR,
    PILOT_GUESTS,
    load_json,
    save_json,
    log,
    slugify,
)


def validate_catalog(catalog: list[dict]) -> dict:
    """Validate criterion_catalog.json."""
    issues = []
    stats = {
        "total": len(catalog),
        "with_tmdb_id": 0,
        "with_poster": 0,
        "with_imdb_id": 0,
        "with_genres": 0,
        "with_year": 0,
        "with_director": 0,
        "duplicate_spines": [],
        "missing_title": 0,
    }

    seen_spines = {}
    for film in catalog:
        spine = film.get("spine_number")
        title = film.get("title", "")

        # Check for missing required fields
        if not spine:
            issues.append({"type": "missing_spine", "film": title})
        if not title:
            stats["missing_title"] += 1
            issues.append({"type": "missing_title", "spine": spine})

        # Check for duplicate spines
        if spine in seen_spines:
            stats["duplicate_spines"].append(spine)
            issues.append({
                "type": "duplicate_spine",
                "spine": spine,
                "titles": [seen_spines[spine], title],
            })
        seen_spines[spine] = title

        # Count enrichment coverage
        if film.get("tmdb_id"):
            stats["with_tmdb_id"] += 1
        if film.get("poster_url"):
            stats["with_poster"] += 1
        if film.get("imdb_id"):
            stats["with_imdb_id"] += 1
        if film.get("genres"):
            stats["with_genres"] += 1
        if film.get("year"):
            stats["with_year"] += 1
        if film.get("director"):
            stats["with_director"] += 1

    return {"stats": stats, "issues": issues}


def validate_guests(guests: list[dict]) -> dict:
    """Validate guests.json."""
    issues = []
    stats = {
        "total": len(guests),
        "with_youtube_video": 0,
        "with_transcript": 0,
        "with_profession": 0,
        "with_photo": 0,
        "with_letterboxd_url": 0,
        "with_episode_date": 0,
    }

    for guest in guests:
        name = guest.get("name", "")
        slug = guest.get("slug", "")

        if not name:
            issues.append({"type": "missing_name", "slug": slug})
        if not slug:
            issues.append({"type": "missing_slug", "name": name})

        if guest.get("youtube_video_id"):
            stats["with_youtube_video"] += 1
            # Check if transcript exists
            transcript_path = TRANSCRIPTS_DIR / f"{guest['youtube_video_id']}.json"
            if transcript_path.exists():
                stats["with_transcript"] += 1
            else:
                issues.append({
                    "type": "missing_transcript",
                    "guest": name,
                    "video_id": guest["youtube_video_id"],
                })
        else:
            issues.append({"type": "no_youtube_video", "guest": name})

        if guest.get("profession"):
            stats["with_profession"] += 1
        if guest.get("photo_url"):
            stats["with_photo"] += 1
        if guest.get("letterboxd_list_url"):
            stats["with_letterboxd_url"] += 1
        if guest.get("episode_date"):
            stats["with_episode_date"] += 1

    return {"stats": stats, "issues": issues}


def validate_picks(picks: list[dict], guests: list[dict], catalog: list[dict]) -> dict:
    """Validate picks.json."""
    issues = []
    stats = {
        "total": len(picks),
        "with_quote": 0,
        "with_timestamp": 0,
        "with_youtube_url": 0,
        "with_catalog_spine": 0,
        "confidence_high": 0,
        "confidence_medium": 0,
        "confidence_low": 0,
        "confidence_none": 0,
        "guests_represented": 0,
        "unique_films": 0,
    }

    guest_slugs = {g["slug"] for g in guests}
    catalog_spines = {c["spine_number"] for c in catalog}
    seen_guest_slugs = set()
    seen_films = set()

    for pick in picks:
        guest_slug = pick.get("guest_slug", "")
        film_title = pick.get("film_title", "")

        # Track coverage
        seen_guest_slugs.add(guest_slug)
        film_key = f"{film_title}|{pick.get('film_year', '')}"
        seen_films.add(film_key)

        # Check guest reference
        if guest_slug and guest_slug not in guest_slugs:
            issues.append({
                "type": "unknown_guest_slug",
                "slug": guest_slug,
                "film": film_title,
            })

        # Check catalog reference
        catalog_spine = pick.get("catalog_spine")
        if catalog_spine:
            stats["with_catalog_spine"] += 1
            if catalog_spine not in catalog_spines:
                issues.append({
                    "type": "invalid_catalog_spine",
                    "spine": catalog_spine,
                    "film": film_title,
                })

        # Quote extraction stats
        quote = pick.get("quote", "")
        if quote:
            stats["with_quote"] += 1
        if pick.get("start_timestamp"):
            stats["with_timestamp"] += 1
        if pick.get("youtube_timestamp_url"):
            stats["with_youtube_url"] += 1

        # Confidence breakdown
        confidence = pick.get("extraction_confidence", "none")
        if confidence == "high":
            stats["confidence_high"] += 1
        elif confidence == "medium":
            stats["confidence_medium"] += 1
        elif confidence == "low":
            stats["confidence_low"] += 1
        else:
            stats["confidence_none"] += 1

    stats["guests_represented"] = len(seen_guest_slugs)
    stats["unique_films"] = len(seen_films)

    return {"stats": stats, "issues": issues}


def validate_picks_raw(picks_raw: list[dict]) -> dict:
    """Validate picks_raw.json."""
    stats = {
        "total": len(picks_raw),
        "with_catalog_spine": 0,
        "match_methods": {},
        "guests": {},
    }
    issues = []

    for pick in picks_raw:
        guest_slug = pick.get("guest_slug", "")
        stats["guests"][guest_slug] = stats["guests"].get(guest_slug, 0) + 1

        if pick.get("catalog_spine"):
            stats["with_catalog_spine"] += 1

        method = pick.get("match_method", "none")
        stats["match_methods"][method] = stats["match_methods"].get(method, 0) + 1

    stats["unique_guests"] = len(stats["guests"])
    match_rate = stats["with_catalog_spine"] / stats["total"] * 100 if stats["total"] else 0
    stats["catalog_match_rate_pct"] = round(match_rate, 1)

    return {"stats": stats, "issues": issues}


def generate_per_guest_report(
    guests: list[dict],
    picks: list[dict],
    picks_raw: list[dict],
    pilot_only: bool = False,
) -> list[dict]:
    """Generate per-guest breakdown."""
    if pilot_only:
        pilot_slugs = {slugify(n) for n in PILOT_GUESTS}
        guests = [g for g in guests if g["slug"] in pilot_slugs]

    # Index picks by guest
    picks_by_guest = {}
    for p in picks:
        slug = p["guest_slug"]
        if slug not in picks_by_guest:
            picks_by_guest[slug] = []
        picks_by_guest[slug].append(p)

    raw_by_guest = {}
    for p in picks_raw:
        slug = p["guest_slug"]
        if slug not in raw_by_guest:
            raw_by_guest[slug] = []
        raw_by_guest[slug].append(p)

    report = []
    for guest in guests:
        slug = guest["slug"]
        guest_picks = picks_by_guest.get(slug, [])
        guest_raw = raw_by_guest.get(slug, [])

        high = sum(1 for p in guest_picks if p.get("extraction_confidence") == "high")
        medium = sum(1 for p in guest_picks if p.get("extraction_confidence") == "medium")
        low = sum(1 for p in guest_picks if p.get("extraction_confidence") == "low")
        none = sum(1 for p in guest_picks if p.get("extraction_confidence") in ("none", None))
        with_quote = sum(1 for p in guest_picks if p.get("quote"))
        matched = sum(1 for p in guest_raw if p.get("catalog_spine"))

        has_video = bool(guest.get("youtube_video_id"))
        video_id = guest.get("youtube_video_id")
        has_transcript = bool(
            video_id and (TRANSCRIPTS_DIR / f"{video_id}.json").exists()
        )

        total = len(guest_picks)
        high_rate = round(high / total * 100, 1) if total else 0

        report.append({
            "name": guest["name"],
            "slug": slug,
            "profession": guest.get("profession"),
            "has_video": has_video,
            "has_transcript": has_transcript,
            "raw_picks": len(guest_raw),
            "catalog_matched": matched,
            "catalog_match_rate_pct": round(matched / len(guest_raw) * 100, 1) if guest_raw else 0,
            "enriched_picks": total,
            "with_quote": with_quote,
            "confidence_high": high,
            "confidence_medium": medium,
            "confidence_low": low,
            "confidence_none": none,
            "high_confidence_rate_pct": high_rate,
        })

    return report


def print_report(
    catalog_result: dict,
    guests_result: dict,
    picks_result: dict,
    raw_result: dict,
    per_guest: list[dict],
) -> None:
    """Print a formatted console report."""
    print("\n" + "=" * 70)
    print("  CRITERION CLOSET PICKS - DATA VALIDATION REPORT")
    print("=" * 70)

    # Catalog
    cs = catalog_result["stats"]
    print(f"\n--- Catalog ---")
    print(f"  Total films: {cs['total']}")
    print(f"  With TMDB ID: {cs['with_tmdb_id']} ({cs['with_tmdb_id']/cs['total']*100:.1f}%)" if cs["total"] else "")
    print(f"  With poster:  {cs['with_poster']}")
    print(f"  With IMDB ID: {cs['with_imdb_id']}")
    print(f"  With genres:  {cs['with_genres']}")
    print(f"  With year:    {cs['with_year']}")
    if catalog_result["issues"]:
        print(f"  Issues: {len(catalog_result['issues'])}")

    # Guests
    gs = guests_result["stats"]
    print(f"\n--- Guests ---")
    print(f"  Total: {gs['total']}")
    print(f"  With YouTube video: {gs['with_youtube_video']}")
    print(f"  With transcript:    {gs['with_transcript']}")
    print(f"  With profession:    {gs['with_profession']}")
    print(f"  With photo:         {gs['with_photo']}")
    if guests_result["issues"]:
        print(f"  Issues: {len(guests_result['issues'])}")
        for issue in guests_result["issues"]:
            print(f"    - {issue['type']}: {issue.get('guest', issue.get('name', ''))}")

    # Raw picks
    rs = raw_result["stats"]
    print(f"\n--- Raw Picks ---")
    print(f"  Total: {rs['total']}")
    print(f"  Unique guests: {rs['unique_guests']}")
    print(f"  Catalog matched: {rs['with_catalog_spine']} ({rs['catalog_match_rate_pct']}%)")
    print(f"  Match methods: {rs['match_methods']}")

    # Enriched picks
    ps = picks_result["stats"]
    print(f"\n--- Enriched Picks ---")
    print(f"  Total: {ps['total']}")
    print(f"  Guests represented: {ps['guests_represented']}")
    print(f"  Unique films: {ps['unique_films']}")
    print(f"  With quote:        {ps['with_quote']}")
    print(f"  With timestamp:    {ps['with_timestamp']}")
    print(f"  With YouTube URL:  {ps['with_youtube_url']}")
    print(f"  With catalog spine: {ps['with_catalog_spine']}")

    total = ps["total"]
    if total:
        print(f"\n  Confidence breakdown:")
        print(f"    High:   {ps['confidence_high']:>4} ({ps['confidence_high']/total*100:.1f}%)")
        print(f"    Medium: {ps['confidence_medium']:>4} ({ps['confidence_medium']/total*100:.1f}%)")
        print(f"    Low:    {ps['confidence_low']:>4} ({ps['confidence_low']/total*100:.1f}%)")
        print(f"    None:   {ps['confidence_none']:>4} ({ps['confidence_none']/total*100:.1f}%)")

    # Per-guest breakdown
    print(f"\n--- Per-Guest Breakdown ---")
    print(f"  {'Guest':<25} {'Video':>5} {'Trans':>5} {'Raw':>4} {'Match':>5} {'Quotes':>6} {'High%':>6}")
    print(f"  {'-'*25} {'-'*5} {'-'*5} {'-'*4} {'-'*5} {'-'*6} {'-'*6}")
    for g in per_guest:
        video_mark = "Y" if g["has_video"] else "N"
        trans_mark = "Y" if g["has_transcript"] else "N"
        print(
            f"  {g['name']:<25} {video_mark:>5} {trans_mark:>5} "
            f"{g['raw_picks']:>4} {g['catalog_match_rate_pct']:>4.0f}% "
            f"{g['with_quote']:>6} {g['high_confidence_rate_pct']:>5.1f}%"
        )

    # Pilot success criteria
    print(f"\n--- Pilot Success Criteria ---")
    processed = sum(1 for g in per_guest if g["has_video"] and g["has_transcript"] and g["with_quote"] > 0)
    total_guests = len(per_guest)
    print(f"  Videos processed end-to-end: {processed}/{total_guests} (target: 8/10)")

    all_high = sum(g["confidence_high"] for g in per_guest)
    all_total = sum(g["enriched_picks"] for g in per_guest)
    # For high confidence rate, exclude picks with no transcript (none confidence from missing data)
    has_data = [g for g in per_guest if g["has_transcript"]]
    data_high = sum(g["confidence_high"] for g in has_data)
    data_total = sum(g["enriched_picks"] for g in has_data)
    overall_high_pct = data_high / data_total * 100 if data_total else 0
    print(f"  High confidence rate (guests with transcripts): {overall_high_pct:.1f}% (target: >70%)")

    # Film matching rate
    total_raw = sum(g["raw_picks"] for g in per_guest)
    total_matched = sum(g["catalog_matched"] for g in per_guest)
    match_pct = total_matched / total_raw * 100 if total_raw else 0
    print(f"  Film matching rate: {match_pct:.1f}% (target: >90%)")

    # Pass/fail
    pass_video = processed >= 8
    pass_confidence = overall_high_pct >= 70
    pass_matching = match_pct >= 90

    print(f"\n  {'PASS' if pass_video else 'FAIL'}: Video processing (>= 8/10)")
    print(f"  {'PASS' if pass_confidence else 'FAIL'}: Quote confidence (>= 70%)")
    print(f"  {'PASS' if pass_matching else 'FAIL'}: Film matching (>= 90%)")

    all_pass = pass_video and pass_confidence and pass_matching
    print(f"\n  Overall: {'ALL CRITERIA MET' if all_pass else 'SOME CRITERIA NOT MET'}")
    print("=" * 70 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Validate pipeline data")
    parser.add_argument("--pilot", action="store_true", help="Only validate pilot guests")
    args = parser.parse_args()

    log("Loading data files...")

    catalog = load_json(CATALOG_FILE)
    guests = load_json(GUESTS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)
    picks = load_json(PICKS_FILE)

    if not catalog:
        log("WARNING: No catalog data found")
    if not guests:
        log("WARNING: No guest data found")
    if not picks:
        log("WARNING: No picks data found")

    # Filter to pilot if requested
    if args.pilot:
        pilot_slugs = {slugify(n) for n in PILOT_GUESTS}
        guests = [g for g in guests if g["slug"] in pilot_slugs]
        picks_raw = [p for p in picks_raw if p.get("guest_slug") in pilot_slugs]
        picks = [p for p in picks if p.get("guest_slug") in pilot_slugs]
        log(f"Filtered to {len(guests)} pilot guests")

    log("Running validations...")

    catalog_result = validate_catalog(catalog)
    guests_result = validate_guests(guests)
    picks_result = validate_picks(picks, guests, catalog)
    raw_result = validate_picks_raw(picks_raw)
    per_guest = generate_per_guest_report(guests, picks, picks_raw, pilot_only=args.pilot)

    # Save full report
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "catalog": catalog_result,
        "guests": guests_result,
        "picks": picks_result,
        "picks_raw": raw_result,
        "per_guest": per_guest,
    }
    report_path = VALIDATION_DIR / "report.json"
    save_json(report_path, report)
    log(f"Saved validation report to {report_path}")

    # Print formatted report
    print_report(catalog_result, guests_result, picks_result, raw_result, per_guest)


if __name__ == "__main__":
    main()
