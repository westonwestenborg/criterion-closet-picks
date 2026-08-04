#!/usr/bin/env python3
"""Fixture tests for scripts.scrape_criterion_picks."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.scrape_criterion_picks import CollectionUnavailable, scrape_collection_page

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

    def __init__(self, html, final_url):
        self.html = html
        self.final_url = final_url
        self.fetched = []

    def fetch(self, url, timeout=30):
        self.fetched.append(url)

        class Result:
            status_code = 200
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


if __name__ == "__main__":
    unittest.main()
