#!/usr/bin/env python3
"""
Scrape box set images from Criterion.com.

For catalog entries with is_box_set=True and no poster_url, fetches the
box set page and extracts the product image URL.

Usage:
  python scripts/scrape_box_set_images.py --dry-run   # Preview which entries need images
  python scripts/scrape_box_set_images.py              # Scrape and save
"""

import argparse
import sys
import time

import cloudscraper
from bs4 import BeautifulSoup

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    load_json,
    save_json,
    log,
)

RATE_LIMIT_SECONDS = 1.5


def scrape_box_set_image(url: str, scraper: cloudscraper.CloudScraper) -> str | None:
    """Fetch a Criterion box set page and extract the product image URL."""
    try:
        resp = scraper.get(url, timeout=15)

        # If redirected to /shop/browse, the URL is stale
        if "/shop/browse" in resp.url or resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Try .product-box-art img first (box set pages)
        img = soup.select_one(".product-box-art img")
        if img and img.get("src"):
            return img["src"]

        # Fallback: .boxset-hero img
        img = soup.select_one(".boxset-hero img")
        if img and img.get("src"):
            return img["src"]

        # Fallback: meta og:image
        meta = soup.select_one('meta[property="og:image"]')
        if meta and meta.get("content"):
            return meta["content"]

        return None
    except Exception as e:
        log(f"  Error fetching {url}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Scrape box set images")
    parser.add_argument("--dry-run", action="store_true", help="Preview without saving")
    args = parser.parse_args()

    catalog = load_json(CATALOG_FILE)

    # Find box set entries needing images
    needs_image = [
        entry for entry in catalog
        if entry.get("is_box_set")
        and not entry.get("poster_url")
        and entry.get("criterion_url")
        and "/boxsets/" in entry["criterion_url"]
    ]

    log(f"Found {len(needs_image)} box set entries needing images")

    if args.dry_run:
        for entry in needs_image:
            log(f"  Would fetch: {entry['film_id']} -> {entry['criterion_url']}")
        log("(dry run)")
        return

    scraper = cloudscraper.create_scraper()
    found = 0
    failed = 0

    for i, entry in enumerate(needs_image):
        url = entry["criterion_url"]
        log(f"  [{i + 1}/{len(needs_image)}] {entry['film_id']}")

        image_url = scrape_box_set_image(url, scraper)

        if image_url:
            entry["poster_url"] = image_url
            found += 1
            log(f"    Found: {image_url[:80]}...")
        else:
            failed += 1
            log(f"    No image found")

        if i < len(needs_image) - 1:
            time.sleep(RATE_LIMIT_SECONDS)

    log(f"\nDone: {found} images found, {failed} failed")

    if found > 0:
        save_json(CATALOG_FILE, catalog)
        log(f"Saved {CATALOG_FILE}")


if __name__ == "__main__":
    main()
