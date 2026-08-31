#!/usr/bin/env python3
"""Fixture tests for scripts.scrape_criterion_picks."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.scrape_criterion_picks import (
    CollectionUnavailable,
    match_films_to_catalog,
    scrape_collection_page,
)

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

        class Result:
            status_code = self.status_code
            text = self.html
            url = self.final_url

        return Result()


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


if __name__ == "__main__":
    unittest.main()
