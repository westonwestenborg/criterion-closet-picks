#!/usr/bin/env python3
"""Fixture tests for scripts.scrape_criterion_picks."""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import scripts.scrape_criterion_picks as scrape_mod
from scripts.browser_utils import FetchResult
from scripts.scrape_criterion_picks import (
    CollectionUnavailable,
    CriterionBlocked,
    _resolve_visit_index,
    match_films_to_catalog,
    parse_guest_name_from_link_text,
    restore_unscraped_visit_indexes,
    scrape_all_collections,
    scrape_collection_page,
    scrape_index,
)
from scripts.utils import collection_id, collection_ids, same_collection

COLLECTION_URL = "https://www.criterion.com/shop/collection/989-a-guest-s-closet-picks"

# The alphabetical head of /shop/browse, which is what an unpublished collection
# redirects to. Shaped like a real listing so only the final URL distinguishes it.
BROWSE_HTML = """
<html><body><h1>Shop All Films</h1>
  <a href="/films/1333-2-or-3-things-i-know-about-her">2 or 3 Things I Know About Her</a>
  <a href="/films/431-3-10-to-yuma">3:10 to Yuma</a>
</body></html>
"""

COLLECTION_HTML = """
<html><body><h1>A Guest&rsquo;s Closet Picks</h1>
  <a href="/films/612-purple-noon">Purple Noon</a>
</body></html>
"""

# What Cloudflare serves in place of the page while it holds a browser.
CHALLENGE_HTML = """
<html><head><title>Just a moment...</title></head><body></body></html>
"""


class FakeScraper:
    """Stands in for CriterionBrowser, returning a canned FetchResult."""

    def __init__(self, html, final_url, status_code=200, raises=None):
        self.html = html
        self.final_url = final_url
        self.status_code = status_code
        self.raises = raises
        self.fetched = []

    def fetch(self, url, timeout=30):
        self.fetched.append(url)
        if self.raises:
            raise self.raises
        return FetchResult(status_code=self.status_code, text=self.html, url=self.final_url)


class ScrapeCollectionPageTest(unittest.TestCase):
    def test_redirect_to_browse_raises_instead_of_returning_catalog_films(self):
        scraper = FakeScraper(BROWSE_HTML, "https://www.criterion.com/shop/browse")

        with self.assertRaises(CollectionUnavailable):
            scrape_collection_page(scraper, COLLECTION_URL)

    def test_live_collection_page_is_scraped_normally(self):
        scraper = FakeScraper(COLLECTION_HTML, COLLECTION_URL)

        films, _video_ids = scrape_collection_page(scraper, COLLECTION_URL)

        self.assertEqual([f["title"] for f in films], ["Purple Noon"])

    def test_fetch_exception_raises_instead_of_reporting_an_empty_collection(self):
        # An empty return would be checkpointed as done, skipping a real
        # collection permanently after one transient timeout.
        scraper = FakeScraper(
            COLLECTION_HTML, COLLECTION_URL, raises=TimeoutError("Timeout 30000ms exceeded")
        )

        with self.assertRaises(CollectionUnavailable):
            scrape_collection_page(scraper, COLLECTION_URL)

    def test_non_200_raises_instead_of_reporting_an_empty_collection(self):
        scraper = FakeScraper(COLLECTION_HTML, COLLECTION_URL, status_code=503)

        with self.assertRaises(CollectionUnavailable):
            scrape_collection_page(scraper, COLLECTION_URL)

    def test_cloudflare_challenge_raises_blocked_not_just_unavailable(self):
        # Before CriterionBlocked, a challenge was logged as "collection not
        # live", so a run blocked on every page read as a quiet week.
        scraper = FakeScraper(CHALLENGE_HTML, COLLECTION_URL, status_code=403)

        with self.assertRaises(CriterionBlocked):
            scrape_collection_page(scraper, COLLECTION_URL)

    def test_blocked_index_raises_instead_of_returning_no_collections(self):
        scraper = FakeScraper(CHALLENGE_HTML, "https://www.criterion.com/closet-picks", status_code=403)

        with self.assertRaises(CriterionBlocked):
            scrape_index(scraper)


class FetchResultBlockedTest(unittest.TestCase):
    def test_403_is_blocked(self):
        self.assertTrue(FetchResult(403, "", COLLECTION_URL).blocked)

    def test_challenge_title_is_blocked_whatever_the_status(self):
        self.assertTrue(FetchResult(200, CHALLENGE_HTML, COLLECTION_URL).blocked)

    def test_real_pages_are_not_blocked(self):
        self.assertFalse(FetchResult(200, COLLECTION_HTML, COLLECTION_URL).blocked)
        self.assertFalse(FetchResult(503, COLLECTION_HTML, COLLECTION_URL).blocked)


# A box set and one of its member films. Criterion's collection page names the set
# "Three Colors", which fuzzy-matches the member title at 86 and the set's own
# catalog title at only 67 -- so title matching alone sends the pick to Red.
THREE_COLORS_CATALOG = [
    {
        "film_id": "the-three-colors-trilogy",
        "title": "The Three Colors Trilogy",
        "spine_number": 587,
        "criterion_url": "https://www.criterion.com/boxsets/844-three-colors",
        "is_box_set": True,
    },
    {
        "film_id": "three-colors-red",
        "title": "Three Colors: Red",
        "spine_number": 590,
        "criterion_url": "https://www.criterion.com/films/27733-three-colors-red",
    },
]


class MatchFilmsToCatalogTest(unittest.TestCase):
    def test_box_set_resolves_by_url_not_by_higher_scoring_member_title(self):
        films = [
            {
                "title": "Three Colors",
                "criterion_film_url": "https://www.criterion.com/boxsets/844-three-colors",
                "criterion_film_id": "844",
                "is_box_set": True,
            }
        ]

        (film,) = match_films_to_catalog(films, THREE_COLORS_CATALOG)

        self.assertEqual(film["film_id"], "the-three-colors-trilogy")
        self.assertEqual(film["match_method"], "criterion_url")

    def test_member_film_still_resolves_to_the_member_entry(self):
        films = [
            {
                "title": "Three Colors: Red",
                "criterion_film_url": "https://www.criterion.com/films/27733-three-colors-red",
                "criterion_film_id": "27733",
                "is_box_set": False,
            }
        ]

        (film,) = match_films_to_catalog(films, THREE_COLORS_CATALOG)

        self.assertEqual(film["film_id"], "three-colors-red")

    def test_unknown_url_falls_back_to_title_matching(self):
        films = [
            {
                "title": "Three Colors: Red",
                "criterion_film_url": "https://www.criterion.com/films/99999-not-in-catalog",
                "criterion_film_id": "99999",
                "is_box_set": False,
            }
        ]

        (film,) = match_films_to_catalog(films, THREE_COLORS_CATALOG)

        self.assertEqual(film["film_id"], "three-colors-red")
        self.assertEqual(film["match_method"], "exact")


BASE = "https://www.criterion.com/shop/collection/"


class CollectionIdentityTest(unittest.TestCase):
    """
    Criterion serves a collection off the numeric id and ignores the slug, so
    it can rename one and keep serving both spellings. Collection 911 really
    was renamed to ...-s-mobile-closet-picks while the old URL kept resolving.
    """

    def test_id_comes_from_the_number_not_the_slug(self):
        self.assertEqual(collection_id(BASE + "911-guillermo-del-toro-s-closet-picks"), "911")
        self.assertEqual(
            collection_id(BASE + "911-guillermo-del-toro-s-mobile-closet-picks"), "911"
        )
        self.assertEqual(collection_id(BASE + "1001-wes-anderson-s-mobile-closet-picks"), "1001")

    def test_non_collection_urls_have_no_id(self):
        self.assertIsNone(collection_id(None))
        self.assertIsNone(collection_id(""))
        self.assertIsNone(collection_id("https://www.criterion.com/films/234-the-39-steps"))

    def test_renamed_urls_are_the_same_collection(self):
        self.assertTrue(
            same_collection(BASE + "911-a-s-closet-picks", BASE + "911-a-s-mobile-closet-picks")
        )
        self.assertFalse(same_collection(BASE + "911-a", BASE + "645-a"))

    def test_two_missing_urls_are_not_the_same_collection(self):
        # Otherwise every guest with no Criterion page would match every other.
        self.assertFalse(same_collection(None, None))

    def test_collection_ids_skips_urls_without_one(self):
        self.assertEqual(collection_ids([BASE + "645-x", BASE + "911-y", "junk", None]), {"645", "911"})


class ResolveVisitIndexTest(unittest.TestCase):
    """
    Regression cover for the collapse this replaced: a renamed collection URL
    matched no stored visit, fell through to visit 1, and merged a two-visit
    guest's picks into one visit.
    """

    GUEST = {
        "slug": "guillermo-del-toro",
        "visits": [
            {"visit_index": 1, "criterion_page_url": BASE + "645-guillermo-del-toro-s-closet-picks"},
            {"visit_index": 2, "criterion_page_url": BASE + "911-guillermo-del-toro-s-closet-picks"},
        ],
    }

    def test_matches_the_visit_it_belongs_to(self):
        self.assertEqual(_resolve_visit_index(self.GUEST, "guillermo-del-toro", "645"), 1)
        self.assertEqual(_resolve_visit_index(self.GUEST, "guillermo-del-toro", "911"), 2)

    def test_renamed_collection_still_resolves_to_its_own_visit(self):
        # The stored URL says 911-...-s-closet-picks and the live index says
        # 911-...-s-mobile-closet-picks. Same id, so still visit 2.
        live = BASE + "911-guillermo-del-toro-s-mobile-closet-picks"
        self.assertEqual(
            _resolve_visit_index(self.GUEST, "guillermo-del-toro", collection_id(live)), 2
        )

    def test_single_visit_guest_is_visit_one(self):
        guest = {"slug": "wes-anderson", "visits": []}
        self.assertEqual(_resolve_visit_index(guest, "wes-anderson", "1001"), 1)

    def test_unmatched_collection_falls_back_to_visit_one(self):
        self.assertEqual(_resolve_visit_index(self.GUEST, "guillermo-del-toro", "999"), 1)

    def test_no_collection_id_falls_back_to_visit_one(self):
        self.assertEqual(_resolve_visit_index(self.GUEST, "guillermo-del-toro", None), 1)


DEL_TORO_URLS = [
    BASE + "645-guillermo-del-toro-s-closet-picks",
    BASE + "911-guillermo-del-toro-s-closet-picks",
]


def _del_toro_picks():
    return [
        {"guest_slug": "guillermo-del-toro", "film_id": "cronos", "visit_index": 1},
        {"guest_slug": "guillermo-del-toro", "film_id": "the-devils-backbone", "visit_index": 2},
    ]


class RestoreVisitIndexTest(unittest.TestCase):
    """
    The scrape blanks multi-visit guests' visit_index before re-scraping them.
    On 2026-09-29 a Cloudflare block stopped every re-scrape and 154 picks
    stayed blank.
    """

    def setUp(self):
        patcher = patch.dict(
            scrape_mod.VISIT_CRITERION_URLS, {"guillermo-del-toro": DEL_TORO_URLS}, clear=True
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_guest_with_a_page_not_re_scraped_gets_its_old_values_back(self):
        picks = _del_toro_picks()
        prior = {0: 1, 1: 2}
        picks[0]["visit_index"] = 1  # re-scraped from 645
        picks[1]["visit_index"] = None  # 911 never came back

        restored = restore_unscraped_visit_indexes(picks, prior, rescraped_ids={"645"})

        self.assertEqual([p["visit_index"] for p in picks], [1, 2])
        self.assertEqual(restored, 1)

    def test_fully_re_scraped_guest_keeps_the_new_values(self):
        picks = _del_toro_picks()
        picks[1]["visit_index"] = 1  # the re-scrape moved it, and that stands

        restored = restore_unscraped_visit_indexes(
            picks, {0: 1, 1: 2}, rescraped_ids={"645", "911"}
        )

        self.assertEqual([p["visit_index"] for p in picks], [1, 1])
        self.assertEqual(restored, 0)

    def test_blocked_run_stops_at_the_block_and_leaves_visits_intact(self):
        picks = _del_toro_picks()
        collections = [
            {"name": "Guillermo Del Toro", "slug": "guillermo-del-toro", "collection_url": url}
            for url in DEL_TORO_URLS
        ]
        scraper = FakeScraper(CHALLENGE_HTML, DEL_TORO_URLS[0], status_code=403)

        with patch.object(scrape_mod, "save_json"), patch.object(scrape_mod, "save_checkpoint"):
            _guests, new_picks, blocked = scrape_all_collections(
                scraper, catalog=[], collections=collections,
                existing_guests=[], existing_picks=picks, resume=False,
            )

        self.assertIsInstance(blocked, CriterionBlocked)
        self.assertEqual(new_picks, [])
        self.assertEqual(len(scraper.fetched), 1)
        self.assertEqual([p["visit_index"] for p in picks], [1, 2])


class ParseGuestNameTest(unittest.TestCase):
    """Criterion's collection titles carry qualifiers that are not part of the name."""

    def test_plain_and_qualified_titles(self):
        cases = {
            "Charli XCX\u2019s Closet Picks": "Charli XCX",
            "Martin Scorsese\u2019s Second Closet Picks": "Martin Scorsese",
            "Wes Anderson\u2019s Mobile Closet Picks": "Wes Anderson",
            "Guillermo del Toro\u2019s Mobile Closet Picks": "Guillermo del Toro",
            "Cate Blanchett and Todd Field\u2019s Closet Picks": "Cate Blanchett and Todd Field",
            "Ari Aster\u2019s Closet Picks 2023": "Ari Aster",
            "Watch & shopCharli XCX\u2019s Closet Picks": "Charli XCX",
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(parse_guest_name_from_link_text(text), expected)

    def test_non_guest_collections_yield_no_name(self):
        self.assertEqual(parse_guest_name_from_link_text("4K Discs 30% Off"), "")


if __name__ == "__main__":
    unittest.main()
