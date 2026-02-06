#!/usr/bin/env python3
"""
Scrape Criterion.com closet picks pages as a fallback/supplement to Letterboxd.

The Criterion site has ~344 collection pages at criterion.com/closet-picks.
Each collection page lists the films a guest picked. This script:
  1. Scrapes the index to discover all collection URLs and guest names.
  2. Scrapes each collection page for film links.
  3. Matches films to the catalog by Criterion URL or fuzzy title match.
  4. Merges results into guests.json and picks_raw.json.

Usage:
  python scripts/scrape_criterion_picks.py              # Scrape all collections
  python scripts/scrape_criterion_picks.py --index-only  # Just update criterion_page_url
  python scripts/scrape_criterion_picks.py --videos-only # Extract YouTube video IDs from collection pages
  python scripts/scrape_criterion_picks.py --guest "Cate Blanchett"  # Single guest
  python scripts/scrape_criterion_picks.py --limit 10    # Limit collections

Output: updates data/guests.json + data/picks_raw.json
"""

import argparse
import re
import sys
import time

import cloudscraper
from bs4 import BeautifulSoup
from tqdm import tqdm

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    DATA_DIR,
    GUESTS_FILE,
    PICKS_RAW_FILE,
    load_json,
    save_json,
    log,
    slugify,
    make_film_id,
    fuzzy_match_score,
    fuzzy_match_name,
)

CRITERION_BASE = "https://www.criterion.com"
CLOSET_PICKS_INDEX = f"{CRITERION_BASE}/closet-picks"
CHECKPOINT_FILE = DATA_DIR / ".criterion_scrape_progress.json"

# Rate limit between requests (seconds)
REQUEST_DELAY = 1.5


# ---------------------------------------------------------------------------
# Scraper setup
# ---------------------------------------------------------------------------

def create_scraper():
    """Create a cloudscraper session for bypassing Cloudflare."""
    return cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False}
    )


# ---------------------------------------------------------------------------
# YouTube video extraction
# ---------------------------------------------------------------------------

def extract_youtube_video_id(soup: BeautifulSoup) -> str | None:
    """
    Extract YouTube video ID from a Criterion collection page.
    Looks for YouTube embed iframes in the parsed HTML.
    """
    # Look for YouTube embeds in iframes
    for iframe in soup.select('iframe[src*="youtube.com/embed/"]'):
        src = iframe.get("src", "")
        m = re.search(r"youtube\.com/embed/([a-zA-Z0-9_-]{11})", src)
        if m:
            return m.group(1)

    # Also check for youtube-nocookie.com embeds
    for iframe in soup.select('iframe[src*="youtube-nocookie.com/embed/"]'):
        src = iframe.get("src", "")
        m = re.search(r"youtube-nocookie\.com/embed/([a-zA-Z0-9_-]{11})", src)
        if m:
            return m.group(1)

    # Fallback: regex search in raw HTML for any youtube embed URL
    raw_html = str(soup)
    m = re.search(r"youtube(?:-nocookie)?\.com/embed/([a-zA-Z0-9_-]{11})", raw_html)
    if m:
        return m.group(1)

    return None


def extract_videos_from_criterion_pages(scraper, existing_guests: list[dict]) -> int:
    """
    For guests with criterion_page_url but no youtube_video_id,
    fetch the collection page and extract the YouTube video ID.
    Returns count of updated guests.
    """
    candidates = [
        g for g in existing_guests
        if g.get("criterion_page_url") and not g.get("youtube_video_id")
    ]

    if not candidates:
        log("No guests need YouTube video extraction")
        return 0

    log(f"Checking {len(candidates)} guests for YouTube embeds...")
    updated = 0

    for guest in tqdm(candidates, desc="Extracting YouTube videos"):
        url = guest["criterion_page_url"]
        log(f"  Checking {guest['name']}: {url}")

        try:
            resp = scraper.get(url, timeout=30)
            if resp.status_code != 200:
                log(f"    HTTP {resp.status_code}")
                time.sleep(REQUEST_DELAY)
                continue
        except Exception as e:
            log(f"    Error: {e}")
            time.sleep(REQUEST_DELAY)
            continue

        soup = BeautifulSoup(resp.text, "lxml")
        video_id = extract_youtube_video_id(soup)

        if video_id:
            guest["youtube_video_id"] = video_id
            guest["youtube_video_url"] = f"https://www.youtube.com/watch?v={video_id}"
            log(f"    Found video: {video_id}")
            updated += 1
        else:
            log(f"    No YouTube embed found")

        time.sleep(REQUEST_DELAY)

    return updated


# ---------------------------------------------------------------------------
# Index scraping
# ---------------------------------------------------------------------------

def _clean_link_text(text: str) -> str:
    """
    Clean up link text from Criterion index page.
    The <a> tags sometimes include child element text like "Watch & shop",
    "Watch & shop now", or "Quick Shop" prepended/appended to the actual title.
    """
    # Remove common overlay text (with optional trailing words like "now")
    text = re.sub(r"^Watch\s*&\s*shop\s*(now\s*)?", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*Watch\s*&\s*shop\s*(now\s*)?$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^Quick\s*Shop\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*Quick\s*Shop\s*$", "", text, flags=re.IGNORECASE)
    return text.strip()


def parse_guest_name_from_link_text(text: str) -> str:
    """
    Extract guest name from Criterion collection link text.
    Examples:
      "Charli XCX's Closet Picks" -> "Charli XCX"
      "Cate Blanchett and Todd Field's Closet Picks" -> "Cate Blanchett and Todd Field"
      "Martin Scorsese's Closet Picks" -> "Martin Scorsese"
      "Watch & shopCharli XCX's Closet Picks" -> "Charli XCX"
    """
    # Clean overlay text first
    text = _clean_link_text(text)

    # Pattern: "Name's Closet Picks" (smart or straight apostrophe)
    m = re.match(
        r"^(.+?)(?:['\u2019]s)\s+(?:Second\s+)?Closet\s+Picks?",
        text,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()

    # Pattern: "Name Closet Picks" (no possessive -- rare)
    m = re.match(r"^(.+?)\s+Closet\s+Picks?", text, re.IGNORECASE)
    if m:
        name = m.group(1).strip()
        # Avoid capturing random words
        if len(name) > 2:
            return name

    return text.strip()


def scrape_index(scraper) -> list[dict]:
    """
    Scrape the Criterion closet-picks index page to discover all collections.
    Returns list of {name, slug, collection_url, collection_path}.
    """
    collections = []
    seen_paths = set()

    log("Scraping Criterion closet-picks index...")
    try:
        resp = scraper.get(CLOSET_PICKS_INDEX, timeout=30)
        if resp.status_code != 200:
            log(f"  HTTP {resp.status_code} for index page")
            return []
    except Exception as e:
        log(f"  Error fetching index: {e}")
        return []

    soup = BeautifulSoup(resp.text, "lxml")

    # Collection links match: /shop/collection/{id}-{slug}
    for a in soup.select('a[href*="/shop/collection/"]'):
        href = a.get("href", "")
        text = _clean_link_text(a.get_text(strip=True))

        if not text or len(text) < 3:
            continue

        # Normalize to path only
        path = href
        if path.startswith("http"):
            path = re.sub(r"^https?://[^/]+", "", path)

        # Skip duplicates
        if path in seen_paths:
            continue
        seen_paths.add(path)

        # Must match the collection URL pattern
        if not re.match(r"/shop/collection/\d+-", path):
            continue

        guest_name = parse_guest_name_from_link_text(text)
        if not guest_name:
            continue

        full_url = f"{CRITERION_BASE}{path}" if not href.startswith("http") else href

        collections.append({
            "name": guest_name,
            "slug": slugify(guest_name),
            "collection_url": full_url,
            "collection_path": path,
        })

    log(f"  Found {len(collections)} collections on index page")
    return collections


# ---------------------------------------------------------------------------
# Collection page scraping
# ---------------------------------------------------------------------------

def scrape_collection_page(scraper, collection_url: str) -> list[dict]:
    """
    Scrape a single Criterion collection page for film links.
    Handles pagination. Returns list of film dicts.
    """
    films = []
    seen_film_ids = set()
    page = 1

    while True:
        if page == 1:
            url = collection_url
        else:
            # Criterion pagination uses ?page=N
            separator = "&" if "?" in collection_url else "?"
            url = f"{collection_url}{separator}page={page}"

        try:
            resp = scraper.get(url, timeout=30)
            if resp.status_code != 200:
                if page > 1:
                    break
                log(f"    HTTP {resp.status_code} for {url}")
                break
        except Exception as e:
            log(f"    Error fetching {url}: {e}")
            break

        soup = BeautifulSoup(resp.text, "lxml")
        page_films = _extract_films_from_page(soup, seen_film_ids)

        if not page_films:
            if page > 1:
                break
            # No films on page 1 -- still break, nothing to paginate
            break

        films.extend(page_films)

        # Check for next page -- look for pagination links
        pagination = soup.select(".pagination a, .paginator a, nav.pagination a")
        has_next = False
        for link in pagination:
            link_text = link.get_text(strip=True).lower()
            link_href = link.get("href", "")
            if "next" in link_text or f"page={page + 1}" in link_href:
                has_next = True
                break

        if has_next:
            page += 1
            time.sleep(REQUEST_DELAY)
        else:
            break

    return films


def _extract_films_from_page(soup: BeautifulSoup, seen_film_ids: set) -> list[dict]:
    """
    Extract film data from a Criterion collection page.
    Film links look like: /films/{id}-{slug}
    Filters out Quick Shop duplicates and box set detection.

    HTML structure per film:
      <a href="/films/{id}-{slug}">
        <figure class="basicFilm">
          <img alt="Film Title" .../>
          <figcaption><dl>
            <dt>Film Title</dt>
            <dd>Director Name</dd>
          </dl></figcaption>
        </figure>
      </a>
    """
    films = []

    # Find all film links on the page
    for a in soup.select('a[href*="/films/"]'):
        href = a.get("href", "")
        raw_text = a.get_text(strip=True)

        # Skip "Quick Shop" links and empty links
        if not raw_text or "quick shop" in raw_text.lower():
            continue

        # Skip non-film links (e.g. /films/ without ID)
        path = href
        if path.startswith("http"):
            path = re.sub(r"^https?://[^/]+", "", path)

        # Match film URL pattern: /films/{id}-{slug}
        m = re.match(r"/films/(\d+)-(.+?)/?$", path)
        if not m:
            continue

        criterion_film_id = m.group(1)
        film_slug = m.group(2)

        # Skip duplicates within this collection
        if criterion_film_id in seen_film_ids:
            continue
        seen_film_ids.add(criterion_film_id)

        full_url = f"{CRITERION_BASE}{path}" if not href.startswith("http") else href

        # Extract clean title and director from structured HTML:
        # Prefer <img alt="..."> for title, then <dt>, then fallback to raw text
        title, director = _extract_title_and_director(a)

        films.append({
            "title": title,
            "director": director,
            "criterion_film_id": criterion_film_id,
            "criterion_film_url": full_url,
            "criterion_film_slug": film_slug,
            "is_box_set": False,
            "box_set_name": None,
        })

    # Also detect box set links: /boxsets/{id}-{slug}
    for a in soup.select('a[href*="/boxsets/"]'):
        href = a.get("href", "")
        raw_text = a.get_text(strip=True)

        if not raw_text or "quick shop" in raw_text.lower():
            continue

        path = href
        if path.startswith("http"):
            path = re.sub(r"^https?://[^/]+", "", path)

        m = re.match(r"/boxsets/(\d+)-(.+?)/?$", path)
        if not m:
            continue

        boxset_id = m.group(1)
        if boxset_id in seen_film_ids:
            continue
        seen_film_ids.add(boxset_id)

        full_url = f"{CRITERION_BASE}{path}" if not href.startswith("http") else href
        title, director = _extract_title_and_director(a)

        films.append({
            "title": title,
            "director": director,
            "criterion_film_id": boxset_id,
            "criterion_film_url": full_url,
            "criterion_film_slug": m.group(2),
            "is_box_set": True,
            "box_set_name": title,
        })

    return films


def _extract_title_and_director(a_tag) -> tuple[str, str]:
    """
    Extract clean film title and director from a Criterion collection <a> tag.
    The HTML structure is:
      <a href="...">
        <img alt="Film Title" .../>
        <figcaption><dl><dt>Title</dt><dd>Director</dd></dl></figcaption>
      </a>
    """
    title = ""
    director = ""

    # Method 1: Get title from <img alt="...">
    img = a_tag.select_one("img[alt]")
    if img:
        title = img.get("alt", "").strip()

    # Method 2: Get title from <dt> if img alt is empty
    if not title:
        dt = a_tag.select_one("dt")
        if dt:
            title = dt.get_text(strip=True)

    # Get director from <dd>
    dd = a_tag.select_one("dd")
    if dd:
        director = dd.get_text(strip=True)

    # Fallback: use full link text (may have title+director concatenated)
    if not title:
        title = _clean_film_title(a_tag.get_text(strip=True))

    return title, director


def _clean_film_title(raw_text: str) -> str:
    """
    Clean up film title extracted from Criterion page link text.
    Sometimes the link text has "TitleDirector" concatenated with no separator,
    or has extra whitespace. Do basic cleanup here.
    """
    # Collapse whitespace
    title = re.sub(r"\s+", " ", raw_text).strip()
    return title


# ---------------------------------------------------------------------------
# Film matching
# ---------------------------------------------------------------------------

def match_films_to_catalog(films: list[dict], catalog: list[dict]) -> list[dict]:
    """
    Match scraped Criterion films to our catalog.
    Strategy:
      1. Match by Criterion film URL (if catalog has criterion_url)
      2. Exact title match
      3. Fuzzy title match
    """
    # Build a lookup by criterion URL for fast matching
    url_lookup = {}
    for cat in catalog:
        if cat.get("criterion_url"):
            url_lookup[cat["criterion_url"]] = cat

    # Build a lookup by film slug from the criterion URL in catalog
    slug_lookup = {}
    for cat in catalog:
        if cat.get("criterion_url"):
            m = re.search(r"/films/(\d+)", cat["criterion_url"])
            if m:
                slug_lookup[m.group(1)] = cat

    for film in films:
        film["catalog_spine"] = None
        film["catalog_title"] = None
        film["match_method"] = None
        film["film_id"] = None

        crit_url = film.get("criterion_film_url", "")
        crit_id = film.get("criterion_film_id", "")
        title = film["title"]

        # 1. Match by criterion film ID (extracted from URL)
        if crit_id and crit_id in slug_lookup:
            cat = slug_lookup[crit_id]
            film["catalog_spine"] = cat["spine_number"]
            film["catalog_title"] = cat["title"]
            film["match_method"] = "criterion_url"
            film["film_id"] = cat["film_id"]
            continue

        # 2. Exact title match
        matched = False
        for cat in catalog:
            if cat["title"].lower() == title.lower():
                film["catalog_spine"] = cat["spine_number"]
                film["catalog_title"] = cat["title"]
                film["match_method"] = "exact"
                film["film_id"] = cat["film_id"]
                matched = True
                break
        if matched:
            continue

        # 3. Fuzzy title match
        best_score = 0
        best_match = None
        for cat in catalog:
            score = fuzzy_match_score(title, cat["title"])
            if score > best_score and score >= 75:
                best_score = score
                best_match = cat

        if best_match:
            film["catalog_spine"] = best_match["spine_number"]
            film["catalog_title"] = best_match["title"]
            film["match_method"] = f"fuzzy_{best_score}"
            film["film_id"] = best_match["film_id"]
        else:
            # No catalog match -- generate an ID from the title
            film["film_id"] = make_film_id(title, None)

    return films


# ---------------------------------------------------------------------------
# Guest matching and merging
# ---------------------------------------------------------------------------

def find_existing_guest(guest_name: str, guest_slug: str, existing_guests: list[dict]) -> dict | None:
    """Find an existing guest by slug or fuzzy name match."""
    # Exact slug match
    for g in existing_guests:
        if g["slug"] == guest_slug:
            return g

    # Fuzzy name match
    for g in existing_guests:
        if fuzzy_match_name(g["name"], guest_name, threshold=85):
            return g

    # Handle joint collections: "Cate Blanchett and Todd Field" should match "Cate Blanchett"
    # Check if any existing guest name is contained in the collection name
    if " and " in guest_name:
        parts = [p.strip() for p in guest_name.split(" and ")]
        for part in parts:
            for g in existing_guests:
                if fuzzy_match_name(g["name"], part, threshold=85):
                    return g

    # Reverse: check if collection name is a substring match of existing guest
    for g in existing_guests:
        if " and " in g["name"]:
            parts = [p.strip() for p in g["name"].split(" and ")]
            for part in parts:
                if fuzzy_match_name(part, guest_name, threshold=85):
                    return g

    return None


def update_index_only(collections: list[dict], existing_guests: list[dict]) -> int:
    """
    Update criterion_page_url for existing guests without scraping film pages.
    Returns count of updated guests.
    """
    updated = 0
    for coll in collections:
        guest = find_existing_guest(coll["name"], coll["slug"], existing_guests)
        if guest and not guest.get("criterion_page_url"):
            guest["criterion_page_url"] = coll["collection_url"]
            updated += 1
            log(f"  Updated criterion_page_url for {guest['name']}")
    return updated


# ---------------------------------------------------------------------------
# Checkpoint / incremental save
# ---------------------------------------------------------------------------

def load_checkpoint() -> dict:
    """Load scraping progress checkpoint."""
    data = load_json(CHECKPOINT_FILE)
    if isinstance(data, dict):
        return data
    return {"completed_urls": []}


def save_checkpoint(checkpoint: dict) -> None:
    """Save scraping progress checkpoint."""
    save_json(CHECKPOINT_FILE, checkpoint)


# ---------------------------------------------------------------------------
# Main scraping logic
# ---------------------------------------------------------------------------

def scrape_all_collections(
    scraper,
    catalog: list[dict],
    collections: list[dict],
    existing_guests: list[dict],
    existing_picks: list[dict],
    limit: int = 0,
    guest_filter: str | None = None,
    resume: bool = True,
) -> tuple[list[dict], list[dict]]:
    """
    Scrape film picks from Criterion collection pages.
    Merges into existing guests and picks data.
    Saves progress incrementally.
    """
    new_guests = []
    new_picks = []

    # Load checkpoint for resuming
    checkpoint = load_checkpoint() if resume else {"completed_urls": []}
    completed_urls = set(checkpoint.get("completed_urls", []))

    # Filter collections if --guest flag is used
    if guest_filter:
        collections = [
            c for c in collections
            if fuzzy_match_name(c["name"], guest_filter, threshold=80)
        ]
        if not collections:
            log(f"No collection found matching guest '{guest_filter}'")
            return [], []
        log(f"Filtered to {len(collections)} collection(s) matching '{guest_filter}'")

    if limit:
        collections = collections[:limit]

    # Build lookup for existing picks for dedup
    existing_pick_keys = {
        (p["guest_slug"], p["film_id"]) for p in existing_picks
    }

    for coll in tqdm(collections, desc="Scraping Criterion collections"):
        url = coll["collection_url"]

        # Skip already-completed collections (resume support)
        if url in completed_urls:
            continue

        log(f"  Scraping: {coll['name']} ({url})")

        films = scrape_collection_page(scraper, url)
        log(f"    Found {len(films)} films")

        if not films:
            # Mark as completed even if empty (don't retry empty pages)
            completed_urls.add(url)
            save_checkpoint({"completed_urls": list(completed_urls)})
            time.sleep(REQUEST_DELAY)
            continue

        # Match films to catalog
        films = match_films_to_catalog(films, catalog)
        matched = sum(1 for f in films if f["catalog_spine"])
        log(f"    Matched {matched}/{len(films)} to catalog")

        guest_name = coll["name"]
        guest_slug = coll["slug"]

        # Check if guest already exists
        existing_guest = find_existing_guest(guest_name, guest_slug, existing_guests)

        if existing_guest:
            # Update criterion_page_url
            if not existing_guest.get("criterion_page_url"):
                existing_guest["criterion_page_url"] = url
            # Use the existing slug for consistency
            guest_slug = existing_guest["slug"]
            guest_name = existing_guest["name"]
        else:
            # New guest (not in Letterboxd data)
            new_guest = {
                "name": guest_name,
                "slug": guest_slug,
                "profession": None,
                "photo_url": None,
                "youtube_video_id": None,
                "youtube_video_url": None,
                "episode_date": None,
                "letterboxd_list_url": None,
                "criterion_page_url": url,
                "pick_count": len(films),
            }
            existing_guests.append(new_guest)
            new_guests.append(new_guest)

        # Add picks (dedup against existing)
        for film in films:
            film_id = film.get("film_id", make_film_id(film["title"], None))
            key = (guest_slug, film_id)

            if key in existing_pick_keys:
                continue

            pick = {
                "guest_slug": guest_slug,
                "guest_name": guest_name,
                "film_title": film["title"],
                "film_year": None,
                "film_id": film_id,
                "catalog_spine": film.get("catalog_spine"),
                "catalog_title": film.get("catalog_title"),
                "match_method": film.get("match_method"),
                "letterboxd_url": "",
                "criterion_film_url": film.get("criterion_film_url", ""),
                "is_box_set": film.get("is_box_set", False),
                "box_set_name": film.get("box_set_name"),
                "quote": "",
                "start_timestamp": None,
                "youtube_timestamp_url": "",
                "extraction_confidence": "none",
            }
            existing_picks.append(pick)
            new_picks.append(pick)
            existing_pick_keys.add(key)

        # Update pick_count for existing guest
        if existing_guest:
            guest_picks = [p for p in existing_picks if p["guest_slug"] == guest_slug]
            existing_guest["pick_count"] = len(guest_picks)

        # Save progress incrementally
        completed_urls.add(url)
        save_checkpoint({"completed_urls": list(completed_urls)})

        # Save data after each collection (so interrupted runs keep progress)
        save_json(GUESTS_FILE, existing_guests)
        save_json(PICKS_RAW_FILE, existing_picks)

        time.sleep(REQUEST_DELAY)

    return new_guests, new_picks


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Scrape Criterion.com closet picks as fallback data source"
    )
    parser.add_argument(
        "--index-only",
        action="store_true",
        help="Only scrape the index to update criterion_page_url for existing guests",
    )
    parser.add_argument(
        "--videos-only",
        action="store_true",
        help="Only extract YouTube video IDs from Criterion collection pages",
    )
    parser.add_argument(
        "--guest",
        type=str,
        default=None,
        help="Scrape only a specific guest (fuzzy matched)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of collections to scrape",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Don't resume from checkpoint; re-scrape all collections",
    )
    args = parser.parse_args()

    # Load catalog for matching
    catalog = load_json(CATALOG_FILE)
    if not catalog:
        log("ERROR: No catalog found. Run build_catalog.py first.")
        sys.exit(1)
    log(f"Loaded catalog with {len(catalog)} entries")

    # Load existing data
    existing_guests = load_json(GUESTS_FILE)
    existing_picks = load_json(PICKS_RAW_FILE)
    log(f"Existing data: {len(existing_guests)} guests, {len(existing_picks)} picks")

    # Create scraper
    scraper = create_scraper()

    # Step 1: Scrape the index to discover all collections
    collections = scrape_index(scraper)
    if not collections:
        log("ERROR: No collections found on Criterion index page")
        sys.exit(1)

    log(f"Discovered {len(collections)} collections")

    # Index-only mode: just update criterion_page_url and exit
    if args.index_only:
        updated = update_index_only(collections, existing_guests)
        save_json(GUESTS_FILE, existing_guests)
        log(f"Updated {updated} guests with criterion_page_url")
        log(f"Saved {len(existing_guests)} guests to {GUESTS_FILE}")
        return

    # Videos-only mode: extract YouTube video IDs from Criterion pages
    if args.videos_only:
        updated = extract_videos_from_criterion_pages(scraper, existing_guests)
        save_json(GUESTS_FILE, existing_guests)
        log(f"Updated {updated} guests with YouTube video IDs")
        log(f"Saved {len(existing_guests)} guests to {GUESTS_FILE}")
        return

    # Step 2: Scrape collection pages for film picks
    new_guests, new_picks = scrape_all_collections(
        scraper=scraper,
        catalog=catalog,
        collections=collections,
        existing_guests=existing_guests,
        existing_picks=existing_picks,
        limit=args.limit,
        guest_filter=args.guest,
        resume=not args.no_resume,
    )

    # Final save (redundant with incremental but ensures clean state)
    save_json(GUESTS_FILE, existing_guests)
    save_json(PICKS_RAW_FILE, existing_picks)

    # Summary
    log(f"\nResults:")
    log(f"  New guests added: {len(new_guests)}")
    log(f"  New picks added: {len(new_picks)}")
    log(f"  Total guests: {len(existing_guests)}")
    log(f"  Total picks: {len(existing_picks)}")

    if new_picks:
        matched = sum(1 for p in new_picks if p.get("catalog_spine"))
        total = len(new_picks)
        pct = (matched / total * 100) if total else 0
        log(f"  Criterion match rate: {matched}/{total} ({pct:.1f}%)")

        box_sets = sum(1 for p in new_picks if p.get("is_box_set"))
        if box_sets:
            log(f"  Box sets found: {box_sets}")


if __name__ == "__main__":
    main()
