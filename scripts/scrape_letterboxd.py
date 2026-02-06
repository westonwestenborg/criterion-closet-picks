#!/usr/bin/env python3
"""
Scrape Letterboxd @closetpicks lists for guest picks.
For the pilot: scrape only the 10 pilot guests.
For full run: paginate all lists.

Output: data/guests.json + data/picks_raw.json
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
    GUESTS_FILE,
    PICKS_RAW_FILE,
    PILOT_GUESTS,
    load_json,
    save_json,
    log,
    slugify,
    make_film_id,
    fuzzy_match_title,
    fuzzy_match_score,
)

LETTERBOXD_BASE = "https://letterboxd.com"

# Known list URLs for pilot guests (discovered via URL probing)
PILOT_LIST_URLS = {
    "Barry Jenkins": "/closetpicks/list/barry-jenkins-criterion-closet-picks/",
    "Guillermo del Toro": "/closetpicks/list/guillermo-del-toros-criterion-closet-picks/",
    "Bill Hader": "/closetpicks/list/bill-haders-criterion-closet-picks/",
    "Denis Villeneuve": "/closetpicks/list/denis-villeneuves-criterion-closet-picks/",
    "Bong Joon-ho": "/closetpicks/list/bong-joon-hos-criterion-closet-picks/",
    "Ayo Edebiri": "/closetpicks/list/ayo-edebiris-criterion-closet-picks/",
    "Charli XCX": "/closetpicks/list/charli-xcxs-criterion-closet-picks/",
    "Andrew Garfield": "/closetpicks/list/andrew-garfields-criterion-closet-picks/",
    "Park Chan-wook": "/closetpicks/list/park-chan-wooks-criterion-closet-picks/",
    "Cate Blanchett": "/closetpicks/list/cate-blanchett-and-todd-fields-criterion/",
}


def create_scraper():
    """Create a cloudscraper session for bypassing Cloudflare."""
    return cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False}
    )


def parse_guest_name_from_title(title: str) -> str:
    """
    Extract guest name from a Letterboxd list title.
    Patterns:
      - "Barry Jenkins's Criterion Closet Picks"
      - "Charli xcx's Criterion Closet Picks"
      - "Cate Blanchett and Todd Field's Criterion Closet Picks"
      - "Closet Picks: Name"
    """
    # Pattern: "Name's Closet Picks" or "Name's Criterion Closet Picks"
    m = re.match(
        r"^(.+?)(?:'s|'s)\s+(?:Criterion\s+)?(?:Second\s+)?Closet\b",
        title,
        re.IGNORECASE,
    )
    if m:
        name = m.group(1).strip()
        # Handle "Name and Name" joint lists
        if " and " in name:
            # Return first name for now
            name = name.split(" and ")[0].strip()
        return name

    # Pattern: "Closet Picks: Name"
    m = re.match(r"Closet Picks:\s+(.+)", title, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    return title


def fetch_film_json(scraper, film_path: str) -> dict | None:
    """Fetch film metadata from Letterboxd JSON endpoint."""
    try:
        url = f"{LETTERBOXD_BASE}{film_path}json/"
        resp = scraper.get(url, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def scrape_list_films(scraper, list_url: str) -> list[dict]:
    """
    Scrape all films from a Letterboxd list using the detail view.
    The detail view (/detail/) renders film entries as <article> elements
    with title, link, and a JSON endpoint for year/director data.
    Returns list of {title, year, letterboxd_url, director}.
    """
    films = []
    page = 1

    while True:
        # Use the detail view which renders full film info in HTML
        if page == 1:
            url = f"{LETTERBOXD_BASE}{list_url}detail/"
        else:
            url = f"{LETTERBOXD_BASE}{list_url}detail/page/{page}/"

        try:
            resp = scraper.get(url, timeout=30)
            if resp.status_code != 200:
                if page > 1:
                    break
                log(f"  HTTP {resp.status_code} for {url}")
                break
        except Exception as e:
            log(f"  Error fetching {url}: {e}")
            break

        soup = BeautifulSoup(resp.text, "lxml")

        # Film entries are <article class="list-detailed-entry">
        articles = soup.select("article.list-detailed-entry")

        if not articles:
            if page > 1:
                break
            log(f"  No film entries found in detail view")
            break

        for art in articles:
            # Title from h2.name
            title_el = art.select_one("h2.name")
            title = title_el.get_text(strip=True) if title_el else ""
            if not title:
                continue

            # Film link: <a href="/film/slug/">
            link = art.select_one('a[href*="/film/"]')
            film_path = link.get("href", "") if link else ""

            # Slug from link
            slug = ""
            if film_path:
                slug_match = re.search(r"/film/([^/]+)/", film_path)
                if slug_match:
                    slug = slug_match.group(1)

            # JSON endpoint from data-details-endpoint
            json_endpoint = ""
            poster_div = art.select_one("[data-details-endpoint]")
            if poster_div:
                json_endpoint = poster_div.get("data-details-endpoint", "")

            # Get film_id from data attribute
            lb_film_id = ""
            if poster_div:
                lb_film_id = poster_div.get("data-film-id", "")

            films.append({
                "title": title,
                "year": None,  # Will be populated from JSON
                "director": "",
                "letterboxd_slug": slug,
                "letterboxd_url": f"{LETTERBOXD_BASE}/film/{slug}/" if slug else "",
                "_json_endpoint": json_endpoint,
                "_lb_film_id": lb_film_id,
            })

        # Check for next page
        next_link = soup.select_one(".paginate-nextprev .next")
        if next_link:
            page += 1
            time.sleep(1)
        else:
            break

    # Fetch year/director from JSON endpoints (batch, with rate limiting)
    if films:
        log(f"  Fetching metadata for {len(films)} films...")
        for film in films:
            endpoint = film.pop("_json_endpoint", "")
            film.pop("_lb_film_id", "")
            if endpoint:
                data = fetch_film_json(scraper, endpoint)
                if data:
                    film["year"] = data.get("releaseYear")
                    directors = data.get("directors", [])
                    if directors:
                        film["director"] = directors[0].get("name", "")
                time.sleep(0.3)  # Rate limit JSON requests

    return films


def _strip_catalog_annotations(title: str) -> str:
    """Strip parenthetical annotations from catalog titles like '(Apu Trilogy)' or '(Jacques Demy box)'."""
    # Remove parenthetical at end: "(Box Set Name)" etc.
    cleaned = re.sub(r"\s*\([^)]*(?:box|trilogy|set|collection|films)\s*\)", "", title, flags=re.IGNORECASE)
    return cleaned.strip()


def match_films_to_catalog(films: list[dict], catalog: list[dict]) -> list[dict]:
    """
    Match scraped films to the Criterion catalog using fuzzy title matching.
    Tries: exact match, exact match (stripped annotations), fuzzy match.
    Returns the films list with added catalog_match fields.
    """
    # Pre-compute cleaned catalog titles for matching
    cleaned_catalog = []
    for cat in catalog:
        cleaned_title = _strip_catalog_annotations(cat["title"])
        cleaned_catalog.append((cat, cleaned_title))

    for film in films:
        film["catalog_spine"] = None
        film["catalog_title"] = None
        film["match_method"] = None

        title = film["title"]
        year = film.get("year")

        # 1. Exact title match (against both raw and cleaned catalog titles)
        for cat, cleaned_title in cleaned_catalog:
            if cat["title"].lower() == title.lower() or cleaned_title.lower() == title.lower():
                if year and cat.get("year") and abs(year - cat["year"]) > 1:
                    continue
                film["catalog_spine"] = cat["spine_number"]
                film["catalog_title"] = cat["title"]
                film["match_method"] = "exact"
                film["film_id"] = cat["film_id"]
                break

        if film["catalog_spine"]:
            continue

        # 2. Fuzzy match against both raw and cleaned catalog titles
        best_score = 0
        best_match = None
        for cat, cleaned_title in cleaned_catalog:
            # Try both raw and cleaned titles, take the best score
            score_raw = fuzzy_match_score(title, cat["title"])
            score_clean = fuzzy_match_score(title, cleaned_title)
            score = max(score_raw, score_clean)

            if score > best_score and score >= 75:
                if year and cat.get("year") and abs(year - cat["year"]) > 1:
                    continue
                best_score = score
                best_match = cat

        if best_match:
            film["catalog_spine"] = best_match["spine_number"]
            film["catalog_title"] = best_match["title"]
            film["match_method"] = f"fuzzy_{best_score}"
            film["film_id"] = best_match["film_id"]
        else:
            film["film_id"] = make_film_id(title, year)

    return films


def scrape_pilot_guests(scraper, catalog: list[dict]) -> tuple[list[dict], list[dict]]:
    """Scrape Letterboxd lists for pilot guests only."""
    guests = []
    all_picks = []

    for name in tqdm(PILOT_GUESTS, desc="Scraping pilot guests"):
        list_path = PILOT_LIST_URLS.get(name)
        if not list_path:
            log(f"  No known URL for {name}, skipping")
            continue

        log(f"  Scraping {name}: {list_path}")
        films = scrape_list_films(scraper, list_path)
        log(f"  Found {len(films)} films")

        if not films:
            log(f"  WARNING: No films found for {name}")
            continue

        # Match to catalog
        films = match_films_to_catalog(films, catalog)

        matched = sum(1 for f in films if f["catalog_spine"])
        log(f"  Matched {matched}/{len(films)} to catalog")

        guest_slug = slugify(name)
        guest = {
            "name": name,
            "slug": guest_slug,
            "profession": None,
            "photo_url": None,
            "youtube_video_id": None,
            "youtube_video_url": None,
            "episode_date": None,
            "letterboxd_list_url": f"{LETTERBOXD_BASE}{list_path}",
            "criterion_page_url": None,
            "pick_count": len(films),
        }
        guests.append(guest)

        for film in films:
            pick = {
                "guest_slug": guest_slug,
                "guest_name": name,
                "film_title": film["title"],
                "film_year": film.get("year"),
                "film_id": film.get("film_id", make_film_id(film["title"], film.get("year"))),
                "catalog_spine": film.get("catalog_spine"),
                "catalog_title": film.get("catalog_title"),
                "match_method": film.get("match_method"),
                "letterboxd_url": film.get("letterboxd_url", ""),
                "quote": "",
                "start_timestamp": None,
                "youtube_timestamp_url": "",
                "extraction_confidence": "none",
            }
            all_picks.append(pick)

        time.sleep(2)  # Rate limit between guests

    return guests, all_picks


def scrape_all_lists(scraper, catalog: list[dict], limit: int = 0) -> tuple[list[dict], list[dict]]:
    """
    Scrape all Letterboxd @closetpicks lists.
    Paginates through the index to discover all lists.
    """
    guests = []
    all_picks = []
    discovered_lists = []

    log("Discovering all lists...")

    # Paginate through index pages
    for page in range(1, 50):
        if page == 1:
            url = f"{LETTERBOXD_BASE}/closetpicks/lists/"
        else:
            url = f"{LETTERBOXD_BASE}/closetpicks/lists/page/{page}/"

        try:
            resp = scraper.get(url, timeout=30)
            if resp.status_code != 200:
                time.sleep(3)
                resp = scraper.get(url, timeout=30)
                if resp.status_code != 200:
                    log(f"  Page {page}: HTTP {resp.status_code}")
                    continue
        except Exception as e:
            log(f"  Page {page}: {e}")
            continue

        soup = BeautifulSoup(resp.text, "lxml")

        found = 0
        for a in soup.select('a[href*="/closetpicks/list/"]'):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            if "/likes/" in href or "/edit/" in href or "#" in href:
                continue
            if not text or len(text) < 3:
                continue
            # Skip meta-lists (all-time top, never picked, etc.)
            if any(skip in href for skip in ["all-time-top", "never-picked", "releases-never"]):
                continue
            if (href, text) not in discovered_lists:
                discovered_lists.append((href, text))
                found += 1

        log(f"  Page {page}: +{found} lists (total: {len(discovered_lists)})")
        if found == 0:
            break
        time.sleep(2)

    log(f"Total lists discovered: {len(discovered_lists)}")

    if limit:
        discovered_lists = discovered_lists[:limit]

    for href, title in tqdm(discovered_lists, desc="Scraping lists"):
        guest_name = parse_guest_name_from_title(title)
        if not guest_name:
            continue

        # Strip base URL if href is absolute (defensive)
        if href.startswith(LETTERBOXD_BASE):
            href = href[len(LETTERBOXD_BASE):]

        films = scrape_list_films(scraper, href)
        if not films:
            continue

        films = match_films_to_catalog(films, catalog)

        guest_slug = slugify(guest_name)
        guest = {
            "name": guest_name,
            "slug": guest_slug,
            "profession": None,
            "photo_url": None,
            "youtube_video_id": None,
            "youtube_video_url": None,
            "episode_date": None,
            "letterboxd_list_url": f"{LETTERBOXD_BASE}{href}",
            "criterion_page_url": None,
            "pick_count": len(films),
        }
        guests.append(guest)

        for film in films:
            pick = {
                "guest_slug": guest_slug,
                "guest_name": guest_name,
                "film_title": film["title"],
                "film_year": film.get("year"),
                "film_id": film.get("film_id", make_film_id(film["title"], film.get("year"))),
                "catalog_spine": film.get("catalog_spine"),
                "catalog_title": film.get("catalog_title"),
                "match_method": film.get("match_method"),
                "letterboxd_url": film.get("letterboxd_url", ""),
                "quote": "",
                "start_timestamp": None,
                "youtube_timestamp_url": "",
                "extraction_confidence": "none",
            }
            all_picks.append(pick)

        time.sleep(2)

    return guests, all_picks


def main():
    parser = argparse.ArgumentParser(description="Scrape Letterboxd closet picks")
    parser.add_argument("--pilot", action="store_true", help="Scrape only pilot guests")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of lists")
    args = parser.parse_args()

    # Load catalog for matching
    catalog = load_json(CATALOG_FILE)
    if not catalog:
        log("ERROR: No catalog found. Run build_catalog.py first.")
        sys.exit(1)
    log(f"Loaded catalog with {len(catalog)} entries")

    # Create scraper
    scraper = create_scraper()

    # Load existing data for merge
    existing_guests = load_json(GUESTS_FILE)
    existing_picks = load_json(PICKS_RAW_FILE)

    if args.pilot:
        guests, picks = scrape_pilot_guests(scraper, catalog)
    else:
        guests, picks = scrape_all_lists(scraper, catalog, limit=args.limit)

    # Merge with existing data
    existing_guest_slugs = {g["slug"] for g in existing_guests}
    for g in guests:
        if g["slug"] not in existing_guest_slugs:
            existing_guests.append(g)
            existing_guest_slugs.add(g["slug"])
        else:
            # Update existing
            for i, eg in enumerate(existing_guests):
                if eg["slug"] == g["slug"]:
                    existing_guests[i].update({
                        k: v for k, v in g.items() if v is not None
                    })
                    break

    existing_pick_keys = {
        (p["guest_slug"], p["film_id"]) for p in existing_picks
    }
    for p in picks:
        key = (p["guest_slug"], p["film_id"])
        if key not in existing_pick_keys:
            existing_picks.append(p)
            existing_pick_keys.add(key)

    # Save
    save_json(GUESTS_FILE, existing_guests)
    save_json(PICKS_RAW_FILE, existing_picks)

    log(f"Saved {len(existing_guests)} guests to {GUESTS_FILE}")
    log(f"Saved {len(existing_picks)} picks to {PICKS_RAW_FILE}")

    # Summary
    matched = sum(1 for p in picks if p.get("catalog_spine"))
    total = len(picks)
    pct = (matched / total * 100) if total else 0
    log(f"Film matching rate: {matched}/{total} ({pct:.1f}%)")


if __name__ == "__main__":
    main()
