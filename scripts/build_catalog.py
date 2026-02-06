#!/usr/bin/env python3
"""
Build the Criterion Collection catalog from The Digital Bits Spines Project.
Scrapes all sub-pages (1-100, 101-200, ..., 1301-1400).

Each sub-page contains <li> elements with spine entries in the format:
  <li><span>1301      The Man Who Wasn't There</span></li>

Output: data/criterion_catalog.json
"""

import argparse
import re
import sys
import time

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    log,
    save_json,
    load_json,
    make_film_id,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DIGITALBITS_BASE = "https://thedigitalbits.com"
DIGITALBITS_INDEX = (
    f"{DIGITALBITS_BASE}/columns/todd-doogan/the-criterion-spines-project"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

# Format tags to strip from titles (can appear multiple times)
FORMAT_TAGS = re.compile(r"\s*\((BD|4K|UHD|DVD|Blu-ray|4K UHD)\)", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Digital Bits: discover sub-page URLs
# ---------------------------------------------------------------------------

def discover_subpage_urls() -> list[str]:
    """
    Fetch the Digital Bits index and all paginated index pages to find
    every sub-page link (e.g. criterion-spines-1-to-100).
    """
    all_urls = []
    seen_hrefs = set()

    # The index itself has pagination: ?start=0, ?start=5, ?start=10 etc.
    # First page lists 5 items, additional pages list more.
    # We need to paginate through the index to find ALL sub-page links.
    index_pages = [DIGITALBITS_INDEX]
    # Add paginated index pages
    for start in range(5, 50, 5):  # up to 50 should cover all
        index_pages.append(f"{DIGITALBITS_INDEX}?start={start}")

    for idx_url in index_pages:
        try:
            resp = requests.get(idx_url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            log(f"  Warning: failed to fetch index page {idx_url}: {e}")
            continue

        soup = BeautifulSoup(resp.text, "lxml")
        links = soup.find_all("a", href=True)

        for link in links:
            href = link["href"]
            if "criterion-spines-" in href and href not in seen_hrefs:
                # Skip the "introducing" post
                if "introducing" in href:
                    continue
                full_url = href if href.startswith("http") else f"{DIGITALBITS_BASE}{href}"
                seen_hrefs.add(href)
                all_urls.append(full_url)

        time.sleep(1)

    # Only keep actual spine sub-pages (contain "N-to-N" pattern)
    spine_urls = [u for u in all_urls if re.search(r"\d+-to-\d+", u)]

    # Sort by spine range (extract start number)
    def sort_key(url):
        m = re.search(r"spines?-(\d+)-to-(\d+)", url)
        if m:
            return int(m.group(1))
        return 9999

    spine_urls = sorted(set(spine_urls), key=sort_key)
    return spine_urls


# ---------------------------------------------------------------------------
# Digital Bits: parse a sub-page
# ---------------------------------------------------------------------------

def parse_subpage(url: str) -> list[dict]:
    """
    Parse spine entries from a Digital Bits sub-page.
    Entries are in <li> elements: <li><span>NNNN      Title</span></li>
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        log(f"  Error fetching {url}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    entries = []

    # Find all <li> elements that contain spine entries
    # The format is: "NNNN      Title" with lots of whitespace
    all_lis = soup.find_all("li")

    # Pattern: one or more digits, then whitespace, then title text
    # After normalization, whitespace is collapsed to single spaces
    spine_pattern = re.compile(r"^(\d{1,4})\s+(.+)$")

    for li in all_lis:
        text = li.get_text(strip=True)
        if not text:
            continue

        # Normalize non-breaking spaces and other whitespace
        text = text.replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text).strip()

        match = spine_pattern.match(text)
        if not match:
            continue

        spine_num = int(match.group(1))

        # Spine numbers should be between 1 and 2000
        if spine_num < 1 or spine_num > 2000:
            continue

        raw_title = match.group(2).strip()

        # Skip entries that look like navigation or metadata
        if len(raw_title) < 2 or raw_title.isdigit():
            continue

        # Clean up: remove format tags like (BD), (4K), etc.
        title = FORMAT_TAGS.sub("", raw_title).strip()

        # Some entries have special characters or notes - clean those
        # Remove trailing asterisks, daggers, etc.
        title = re.sub(r"\s*[*\u2020\u2021]+\s*$", "", title)

        # Try to extract a criterion.com URL from any <a> in this <li>
        criterion_url = ""
        for a in li.find_all("a", href=True):
            href = a["href"]
            if "criterion.com" in href:
                criterion_url = href
                break

        entry = {
            "spine_number": spine_num,
            "title": title,
            "director": "",
            "year": None,
            "country": "",
            "criterion_url": criterion_url,
            "film_id": make_film_id(title, None),
            "imdb_id": None,
            "tmdb_id": None,
            "genres": [],
            "poster_url": None,
        }
        entries.append(entry)

    return entries


# ---------------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------------

def scrape_digitalbits() -> list[dict]:
    """
    Scrape all Digital Bits sub-pages to build the full catalog.
    """
    log("Discovering Digital Bits sub-page URLs...")
    subpage_urls = discover_subpage_urls()
    log(f"Found {len(subpage_urls)} sub-pages")

    if not subpage_urls:
        log("ERROR: No sub-pages found")
        return []

    catalog = []
    for url in tqdm(subpage_urls, desc="Scraping Digital Bits"):
        entries = parse_subpage(url)
        log(f"  {url.split('/')[-1]}: {len(entries)} entries")
        catalog.extend(entries)
        time.sleep(1.5)  # Rate limit

    return catalog


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def deduplicate_catalog(catalog: list[dict]) -> list[dict]:
    """Remove duplicates by spine_number, keeping the most complete entry."""
    seen = {}
    for entry in catalog:
        spine = entry.get("spine_number")
        if spine is None:
            continue
        if spine not in seen:
            seen[spine] = entry
        else:
            existing = seen[spine]
            for key in ["director", "year", "country", "criterion_url"]:
                if not existing.get(key) and entry.get(key):
                    existing[key] = entry[key]

    result = sorted(seen.values(), key=lambda x: x.get("spine_number", 0))
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Build Criterion catalog")
    parser.add_argument(
        "--source",
        choices=["digitalbits", "all"],
        default="digitalbits",
        help="Data source (default: digitalbits)",
    )
    args = parser.parse_args()

    catalog = []

    try:
        catalog = scrape_digitalbits()
        log(f"Raw entries: {len(catalog)}")
    except Exception as e:
        log(f"Digital Bits failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    if not catalog:
        log("ERROR: No catalog entries scraped")
        sys.exit(1)

    # Deduplicate
    catalog = deduplicate_catalog(catalog)
    log(f"After deduplication: {len(catalog)} entries")

    # Save
    save_json(CATALOG_FILE, catalog)
    log(f"Saved catalog to {CATALOG_FILE}")

    # Summary
    max_spine = max((e.get("spine_number", 0) for e in catalog), default=0)
    log(f"Summary: {len(catalog)} films, spine #1 to #{max_spine}")


if __name__ == "__main__":
    main()
