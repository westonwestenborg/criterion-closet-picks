#!/usr/bin/env python3
"""
Data validation tests for Criterion Closet Picks.
Verifies data integrity and checks that known bug fixes remain in effect.

Run: python scripts/test_data.py
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    GUESTS_FILE,
    PICKS_FILE,
    PICKS_RAW_FILE,
    load_json,
)

# Load data once for all tests
catalog = load_json(CATALOG_FILE)
guests = load_json(GUESTS_FILE)
picks = load_json(PICKS_FILE)
picks_raw = load_json(PICKS_RAW_FILE)

# Build lookup indexes
catalog_by_spine = {f["spine_number"]: f for f in catalog}
catalog_by_film_id = {f["film_id"]: f for f in catalog}
guest_by_slug = {g["slug"]: g for g in guests}


class TestFilmCoverage(unittest.TestCase):
    """All picked films should have catalog entries."""

    def test_all_picked_films_in_catalog(self):
        """Every film referenced in picks.json with a catalog_spine should exist in the catalog."""
        missing = []
        for pick in picks:
            spine = pick.get("catalog_spine")
            if spine and spine not in catalog_by_spine:
                missing.append(
                    f"{pick.get('film_title', '?')} (spine {spine}, guest {pick.get('guest_slug', '?')})"
                )
        self.assertEqual(missing, [], f"Picks reference missing catalog spines:\n" + "\n".join(missing))


class TestNoZeroDecade(unittest.TestCase):
    """No film should have year=0."""

    def test_no_zero_year(self):
        """Films can have year=null/None, but never year=0."""
        zero_year = [
            f"{f.get('title', '?')} (spine {f.get('spine_number', '?')})"
            for f in catalog
            if f.get("year") == 0
        ]
        self.assertEqual(zero_year, [], f"Films with year=0:\n" + "\n".join(zero_year))


class TestCriterionUrls(unittest.TestCase):
    """Criterion URLs should be canonical, not search URL fallbacks."""

    def test_no_search_url_fallbacks(self):
        """Any non-empty criterion_url should NOT contain /shop/browse."""
        search_urls = [
            f"{f.get('title', '?')} (spine {f.get('spine_number', '?')}): {f.get('criterion_url', '')}"
            for f in catalog
            if f.get("criterion_url") and "/shop/browse" in f["criterion_url"]
        ]
        self.assertEqual(
            search_urls, [], f"Films with search URL fallbacks:\n" + "\n".join(search_urls)
        )


class TestBoxSetStructure(unittest.TestCase):
    """Box set data should be well-formed."""

    def test_aggregates_have_name(self):
        """Picks with box_set_film_count should also have box_set_name."""
        missing_name = []
        for pick in picks:
            if pick.get("box_set_film_count") and not pick.get("box_set_name"):
                missing_name.append(
                    f"{pick.get('film_title', '?')} (guest {pick.get('guest_slug', '?')})"
                )
        self.assertEqual(
            missing_name, [],
            f"Box set picks with film_count but no name:\n" + "\n".join(missing_name),
        )

    def test_box_set_film_titles_exist(self):
        """Box set film titles listed in box_set_film_titles should be findable in the catalog."""
        catalog_titles_lower = {f["title"].lower() for f in catalog}
        missing = []
        for pick in picks:
            for title in pick.get("box_set_film_titles", []):
                if title.lower() not in catalog_titles_lower:
                    missing.append(
                        f"'{title}' from box set '{pick.get('box_set_name', '?')}' "
                        f"(guest {pick.get('guest_slug', '?')})"
                    )
        # Advisory only -- box set sub-films may not all be separate catalog entries
        if missing:
            print(f"\nAdvisory: {len(missing)} box set film titles not found in catalog (may be expected)")


class TestGuestCoverage(unittest.TestCase):
    """Every guest should have displayable content."""

    def test_all_guests_have_picks(self):
        """Every guest in guests.json should have at least one pick in picks.json or picks_raw.json."""
        picks_guest_slugs = {p["guest_slug"] for p in picks}
        raw_guest_slugs = {p["guest_slug"] for p in picks_raw}
        all_pick_slugs = picks_guest_slugs | raw_guest_slugs

        no_picks = [
            f"{g['name']} ({g['slug']})"
            for g in guests
            if g["slug"] not in all_pick_slugs
        ]
        self.assertEqual(
            no_picks, [],
            f"Guests with no picks in either picks.json or picks_raw.json:\n" + "\n".join(no_picks),
        )


class TestYearValidity(unittest.TestCase):
    """Film years should be reasonable."""

    def test_years_are_valid(self):
        """Non-null years should be > 1800 and <= 2026."""
        invalid = [
            f"{f.get('title', '?')} (spine {f.get('spine_number', '?')}): year={f.get('year')}"
            for f in catalog
            if f.get("year") is not None and (f["year"] <= 1800 or f["year"] > 2026)
        ]
        self.assertEqual(
            invalid, [],
            f"Films with invalid years:\n" + "\n".join(invalid),
        )


class TestFilmPageLinks(unittest.TestCase):
    """Film links should resolve to canonical Criterion URLs."""

    def test_criterion_urls_are_canonical(self):
        """Non-empty criterion_url values should start with https://www.criterion.com/films/ or /boxsets/."""
        bad_urls = []
        for f in catalog:
            url = f.get("criterion_url", "")
            if not url:
                continue
            if not (
                url.startswith("https://www.criterion.com/films/")
                or url.startswith("https://www.criterion.com/boxsets/")
            ):
                bad_urls.append(
                    f"{f.get('title', '?')} (spine {f.get('spine_number', '?')}): {url}"
                )
        self.assertEqual(
            bad_urls, [],
            f"Films with non-canonical criterion_url:\n" + "\n".join(bad_urls),
        )


class TestNoDuplicateVideoIds(unittest.TestCase):
    """No two guests should share the same YouTube video ID."""

    def test_no_duplicate_video_ids(self):
        """Each youtube_video_id should appear at most once across guests."""
        video_map: dict[str, list[str]] = {}
        for g in guests:
            vid = g.get("youtube_video_id")
            if vid:
                video_map.setdefault(vid, []).append(g["slug"])
        dupes = {vid: slugs for vid, slugs in video_map.items() if len(slugs) > 1}
        self.assertEqual(
            dupes, {},
            f"Duplicate youtube_video_id assignments:\n"
            + "\n".join(f"  {vid}: {slugs}" for vid, slugs in dupes.items()),
        )


class TestNoNameArtifacts(unittest.TestCase):
    """Guest names should not contain scraping artifacts."""

    def test_no_name_artifacts(self):
        """No person guest name should contain 'Closet', 'Criterion', or 'Picks'."""
        bad = [
            f"{g['name']} ({g['slug']})"
            for g in guests
            if g.get("guest_type", "person") == "person"
            and any(word in g["name"] for word in ["Closet", "Criterion", "Picks"])
        ]
        self.assertEqual(
            bad, [],
            f"Guests with name artifacts:\n" + "\n".join(bad),
        )


class TestNoRepeatVisitSuffixes(unittest.TestCase):
    """Guest names should not contain visit markers."""

    def test_no_repeat_visit_suffixes(self):
        """No guest name should contain '(2nd Visit)', '(3rd Visit)', etc."""
        import re
        bad = [
            f"{g['name']} ({g['slug']})"
            for g in guests
            if re.search(r"\(\d+\w+\s+Visit\)", g["name"])
        ]
        self.assertEqual(
            bad, [],
            f"Guests with repeat visit suffixes:\n" + "\n".join(bad),
        )


class TestPickCountAccuracy(unittest.TestCase):
    """Guest pick counts should match actual picks in data files."""

    def test_pick_count_accuracy(self):
        """pick_count should match the number of picks for each guest."""
        picks_count: dict[str, int] = {}
        for p in picks:
            slug = p["guest_slug"]
            picks_count[slug] = picks_count.get(slug, 0) + 1

        raw_count: dict[str, int] = {}
        for p in picks_raw:
            slug = p["guest_slug"]
            raw_count[slug] = raw_count.get(slug, 0) + 1

        mismatches = []
        for g in guests:
            slug = g["slug"]
            actual = picks_count.get(slug, raw_count.get(slug, 0))
            declared = g.get("pick_count", 0)
            if actual != declared:
                mismatches.append(
                    f"{g['name']} ({slug}): declared={declared}, actual={actual}"
                )
        self.assertEqual(
            mismatches, [],
            f"pick_count mismatches:\n" + "\n".join(mismatches),
        )


if __name__ == "__main__":
    unittest.main()
