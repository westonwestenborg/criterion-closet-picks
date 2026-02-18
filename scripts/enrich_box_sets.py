#!/usr/bin/env python3
"""
Enrich box set catalog entries with metadata from Criterion.com box set pages.

For each box set entry with a /boxsets/ criterion_url, scrapes the page to extract:
  - description: First long paragraph from the page
  - included_films: List of films in the box set (title, year, criterion_url, film_id)
  - box_set_film_count: Derived from included_films count
  - spine_number: From .film-meta block (if available and not already set)

Follows the same pattern as scrape_box_set_images.py (cloudscraper, rate limiting).

Usage:
  python scripts/enrich_box_sets.py --dry-run      # Preview which entries need enrichment
  python scripts/enrich_box_sets.py --limit 5       # Enrich first 5
  python scripts/enrich_box_sets.py                 # Enrich all
  python scripts/enrich_box_sets.py --force          # Re-enrich already-enriched entries
"""

import argparse
import re
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
RETRY_DELAY_SECONDS = 3.0


def _create_scraper() -> cloudscraper.CloudScraper:
    return cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False}
    )


def _extract_description(soup: BeautifulSoup) -> str | None:
    """Extract the main description from the box set page."""
    # Try dedicated selectors first
    for sel in [
        ".product-description",
        ".boxset-description",
        ".synopsis",
        ".product-about",
        ".about-text",
        "section.description",
    ]:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(strip=True)
            if len(text) > 100:
                return text

    # Fallback: first long <p> that isn't boilerplate
    skip_words = {"cookie", "privacy", "subscribe", "sign up", "newsletter"}
    for p in soup.select("p"):
        text = p.get_text(strip=True)
        if len(text) > 100 and not any(w in text.lower() for w in skip_words):
            return text

    return None


def _extract_included_films(
    soup: BeautifulSoup, catalog_url_lookup: dict[str, str]
) -> list[dict]:
    """Extract the list of included films from the box set page.

    Criterion box set pages use <a href="/films/..."> wrapping <li class="film-set">
    elements. The <a> contains .film-set-title and .film-set-year children.
    Pages often have two identical lists (main + sidebar), so we deduplicate by URL.
    """
    films = []
    seen_urls = set()

    # Primary: a[href*="/films/"] links that contain .film-set-title children
    for a in soup.select('a[href*="/films/"]'):
        href = a.get("href", "")
        if not href or "/films/" not in href:
            continue

        full_url = f"https://www.criterion.com{href}" if href.startswith("/") else href
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        film: dict = {"criterion_url": full_url}

        # Extract title from .film-set-title child (preferred) or link text
        title_el = a.select_one(".film-set-title")
        if title_el:
            film["title"] = title_el.get_text(strip=True)
        else:
            text = a.get_text(strip=True)
            if text and len(text) > 1:
                film["title"] = text
            else:
                continue

        # Extract year from .film-set-year child
        year_el = a.select_one(".film-set-year")
        if year_el:
            year_text = year_el.get_text(strip=True)
            m = re.search(r"\d{4}", year_text)
            if m:
                film["year"] = int(m.group())

        # Cross-reference with catalog
        if full_url in catalog_url_lookup:
            film["film_id"] = catalog_url_lookup[full_url]

        films.append(film)

    return films


def _extract_spine_from_meta(soup: BeautifulSoup) -> int | None:
    """Extract spine number from .film-meta block if present."""
    film_meta = soup.select_one(".film-meta")
    if not film_meta:
        return None
    text = film_meta.get_text(separator=" ", strip=True)
    m = re.search(r"Spine\s*#?\s*(\d+)", text)
    if m:
        return int(m.group(1))
    return None


def scrape_box_set(
    url: str, scraper: cloudscraper.CloudScraper, catalog_url_lookup: dict[str, str]
) -> dict | None:
    """Fetch a Criterion box set page and extract enrichment data."""
    try:
        resp = scraper.get(url, timeout=30)

        if "/shop/browse" in resp.url or resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        data = {}

        description = _extract_description(soup)
        if description:
            data["description"] = description

        included_films = _extract_included_films(soup, catalog_url_lookup)
        if included_films:
            data["included_films"] = included_films
            data["box_set_film_count"] = len(included_films)

        spine = _extract_spine_from_meta(soup)
        if spine:
            data["spine_from_page"] = spine

        return data if data else None

    except Exception as e:
        log(f"  Error fetching {url}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Enrich box set catalog entries")
    parser.add_argument("--dry-run", action="store_true", help="Preview without saving")
    parser.add_argument("--limit", type=int, default=0, help="Max entries to process (0=all)")
    parser.add_argument("--force", action="store_true", help="Re-enrich already-enriched entries")
    args = parser.parse_args()

    catalog = load_json(CATALOG_FILE)

    # Build criterion_url -> film_id lookup for cross-referencing included films
    catalog_url_lookup = {}
    for entry in catalog:
        crit_url = entry.get("criterion_url", "")
        if crit_url:
            catalog_url_lookup[crit_url] = entry["film_id"]

    # Find box set entries needing enrichment
    needs_enrichment = []
    for entry in catalog:
        if not entry.get("is_box_set"):
            continue
        if not entry.get("criterion_url") or "/boxsets/" not in entry["criterion_url"]:
            continue
        if not args.force and entry.get("description"):
            continue  # Already enriched
        needs_enrichment.append(entry)

    if args.limit > 0:
        needs_enrichment = needs_enrichment[:args.limit]

    log(f"Found {len(needs_enrichment)} box set entries to enrich")

    if args.dry_run:
        for entry in needs_enrichment:
            log(f"  Would enrich: {entry['film_id']} -> {entry['criterion_url']}")
        log("(dry run)")
        return

    scraper = _create_scraper()
    enriched = 0
    failed = 0

    for i, entry in enumerate(needs_enrichment):
        url = entry["criterion_url"]
        log(f"  [{i + 1}/{len(needs_enrichment)}] {entry['film_id']}")

        data = scrape_box_set(url, scraper, catalog_url_lookup)

        if not data:
            # Retry once
            log(f"    First attempt failed, retrying...")
            time.sleep(RETRY_DELAY_SECONDS)
            data = scrape_box_set(url, scraper, catalog_url_lookup)

        if data:
            if data.get("description"):
                entry["description"] = data["description"]
                log(f"    Description: {len(data['description'])} chars")

            if data.get("included_films"):
                entry["included_films"] = data["included_films"]
                entry["box_set_film_count"] = data["box_set_film_count"]
                log(f"    Included films: {data['box_set_film_count']}")

            if data.get("spine_from_page") and not entry.get("spine_number"):
                entry["spine_number"] = data["spine_from_page"]
                log(f"    Spine: {data['spine_from_page']}")

            enriched += 1
        else:
            failed += 1
            log(f"    Failed to enrich")

        if i < len(needs_enrichment) - 1:
            time.sleep(RATE_LIMIT_SECONDS)

    log(f"\nDone: {enriched} enriched, {failed} failed")

    if enriched > 0:
        save_json(CATALOG_FILE, catalog)
        log(f"Saved {CATALOG_FILE}")


if __name__ == "__main__":
    main()
