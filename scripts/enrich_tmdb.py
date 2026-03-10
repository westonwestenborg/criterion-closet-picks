#!/usr/bin/env python3
"""
Enrich films and guests via TMDB API.
Films: genres, posters, IMDB IDs, year, director.
Guests: profession, photo.

Output: updated criterion_catalog.json + guests.json
"""

import argparse
import re
import sys
import time

import cloudscraper
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from thefuzz import fuzz

from scripts.utils import (
    CATALOG_FILE,
    DATA_DIR,
    GUESTS_FILE,
    PICKS_FILE,
    PICKS_RAW_FILE,
    PILOT_GUESTS,
    load_json,
    save_json,
    log,
    get_env,
    slugify,
)


TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p"

# Map TMDB known_for_department to our profession enum
DEPARTMENT_MAP = {
    "Directing": "director",
    "Acting": "actor",
    "Writing": "writer",
    "Sound": "musician",
    "Production": "producer",
    "Camera": "cinematographer",
    "Editing": "editor",
}

# Films that are TV series on TMDB (use /tv/ endpoints instead of /movie/)
# Maps film_id -> TMDB TV series ID
TMDB_TV_IDS = {
    "dekalog": 42699,
}

# Manual TMDB ID overrides for films that auto-search matches incorrectly
# Maps film_id -> correct TMDB movie ID
TMDB_ID_OVERRIDES = {
    "1984": 9314,          # Orwell adaptation, not KISS music doc
    "cold-war": 440298,    # Pawlikowski's Zimna wojna, not 2017 comedy
}

# Manual TMDB person ID overrides for guests that auto-search misses
# Maps guest slug -> TMDB person ID
TMDB_PERSON_OVERRIDES = {
    "shinichiro-watanabe": 56342,  # Anime director (Cowboy Bebop), search fails on romanization
}


# ---------------------------------------------------------------------------
# Criterion URL helpers
# ---------------------------------------------------------------------------

# Cache for Criterion page metadata lookups (criterion_url -> dict or None)
_criterion_metadata_cache: dict[str, dict | None] = {}
_criterion_scraper = None
CRITERION_REQUEST_DELAY = 1.5


def _get_criterion_scraper():
    """Lazy-init a cloudscraper instance for Criterion.com."""
    global _criterion_scraper
    if _criterion_scraper is None:
        _criterion_scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "darwin", "mobile": False}
        )
    return _criterion_scraper


def get_metadata_from_criterion_url(criterion_url: str) -> dict | None:
    """
    Scrape a Criterion film page to extract metadata (year, director, image_url).
    Returns dict {"year": int|None, "director": str|None, "image_url": str|None}
    or None if the URL is empty. Results are cached to avoid re-scraping.
    """
    if not criterion_url:
        return None

    if criterion_url in _criterion_metadata_cache:
        return _criterion_metadata_cache[criterion_url]

    scraper = _get_criterion_scraper()
    year = None
    director = None
    image_url = None

    try:
        resp = scraper.get(criterion_url, timeout=30)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "lxml")

            # --- Year extraction (existing logic) ---

            # Method 1: Look for year in <h2 class="film-year"> or similar
            for el in soup.select("h2.film-year, .film-year, .release-year"):
                text = el.get_text(strip=True)
                m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
                if m:
                    year = int(m.group(1))
                    break

            # Method 2: Look for year in the page title (e.g. "Crash (1996)")
            if not year:
                title_tag = soup.select_one("title")
                if title_tag:
                    m = re.search(r"\((\d{4})\)", title_tag.get_text())
                    if m:
                        year = int(m.group(1))

            # Method 3: Look for year in meta description or og tags
            if not year:
                for meta in soup.select('meta[name="description"], meta[property="og:title"]'):
                    content = meta.get("content", "")
                    m = re.search(r"\b(19\d{2}|20\d{2})\b", content)
                    if m:
                        year = int(m.group(1))
                        break

            # Method 4: Look for year in any <p> or <span> near the title area
            if not year:
                for el in soup.select(".film-info, .film-details, .film-meta"):
                    text = el.get_text()
                    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
                    if m:
                        year = int(m.group(1))
                        break

            # --- Director extraction ---

            # Method 1: Look for <dt> with "DIRECTED BY" or "Director" followed by <dd>
            for dt in soup.select("dt"):
                dt_text = dt.get_text(strip=True).upper()
                if "DIRECTED BY" in dt_text or "DIRECTOR" in dt_text:
                    dd = dt.find_next_sibling("dd")
                    if dd:
                        director = dd.get_text(strip=True)
                        break

            # Method 2: Try meta description for "Directed by X"
            if not director:
                desc_meta = soup.select_one('meta[name="description"]')
                if desc_meta:
                    content = desc_meta.get("content", "")
                    m = re.search(r"[Dd]irected by ([^.]+)", content)
                    if m:
                        director = m.group(1).strip()

            # --- Image extraction ---

            # Priority 1: og:image
            og_img = soup.select_one('meta[property="og:image"]')
            if og_img and og_img.get("content"):
                image_url = og_img["content"]

            # Priority 2: .product-image img
            if not image_url:
                prod_img = soup.select_one(".product-image img")
                if prod_img and prod_img.get("src"):
                    image_url = prod_img["src"]

            # Priority 3: .product-box-art img
            if not image_url:
                box_img = soup.select_one(".product-box-art img")
                if box_img and box_img.get("src"):
                    image_url = box_img["src"]

        time.sleep(CRITERION_REQUEST_DELAY)

    except Exception as e:
        log(f"  Error scraping Criterion URL {criterion_url}: {e}")

    result = {"year": year, "director": director, "image_url": image_url}
    _criterion_metadata_cache[criterion_url] = result
    return result


def get_year_from_criterion_url(criterion_url: str) -> int | None:
    """Backward-compatible wrapper: returns just the year from Criterion metadata."""
    metadata = get_metadata_from_criterion_url(criterion_url)
    return metadata["year"] if metadata else None


def build_criterion_url_lookup(picks_raw: list[dict]) -> dict[str, str]:
    """Build film_id -> criterion_film_url from picks_raw entries."""
    url_map: dict[str, str] = {}
    for p in picks_raw:
        fid = p.get("film_id")
        url = p.get("criterion_film_url")
        if fid and url and fid not in url_map:
            url_map[fid] = url
    return url_map


class TMDBClient:
    """TMDB API client with rate limiting."""

    def __init__(self):
        self.token = get_env("TMDB_READ_ACCESS_TOKEN")
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }
        self._genre_cache = {}
        self._last_request = 0

    def _rate_limit(self):
        """Ensure at least 50ms between requests (~20 req/s)."""
        elapsed = time.time() - self._last_request
        if elapsed < 0.05:
            time.sleep(0.05 - elapsed)
        self._last_request = time.time()

    def _get(self, endpoint: str, params: dict = None) -> dict | None:
        """Make a GET request to the TMDB API."""
        self._rate_limit()
        url = f"{TMDB_BASE}{endpoint}"
        try:
            resp = requests.get(url, headers=self.headers, params=params, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                # Rate limited, wait and retry
                time.sleep(2)
                resp = requests.get(url, headers=self.headers, params=params, timeout=15)
                if resp.status_code == 200:
                    return resp.json()
            return None
        except Exception as e:
            log(f"  TMDB error: {e}")
            return None

    def get_genres(self) -> dict:
        """Fetch genre ID -> name mapping."""
        if self._genre_cache:
            return self._genre_cache
        data = self._get("/genre/movie/list")
        if data:
            self._genre_cache = {g["id"]: g["name"] for g in data.get("genres", [])}
        return self._genre_cache

    def search_movie(self, title: str, year: int = None, director: str = None) -> dict | None:
        """Search for a movie by title, optionally filtering by year and scoring by director."""
        params = {"query": title}
        if year:
            params["year"] = year
        data = self._get("/search/movie", params)
        results = data.get("results", []) if data else []

        # Try without year if year search failed
        if not results and year:
            data = self._get("/search/movie", {"query": title})
            results = data.get("results", []) if data else []

        # If title contains parenthetical like "(aka Something)", retry with alternatives
        if not results:
            aka_match = re.match(r"^(.+?)\s*\((?:aka\s+)?(.+?)\)\s*$", title)
            if aka_match:
                main_title = aka_match.group(1).strip()
                alt_title = aka_match.group(2).strip()
                for alt in [main_title, alt_title]:
                    alt_params = {"query": alt}
                    if year:
                        alt_params["year"] = year
                    data = self._get("/search/movie", alt_params)
                    if data and data.get("results"):
                        results = data["results"]
                        break
                    if year:
                        data = self._get("/search/movie", {"query": alt})
                        if data and data.get("results"):
                            results = data["results"]
                            break

        if not results:
            return None

        # If only one result or no director to validate, return first
        if len(results) == 1 or not director:
            return results[0]

        # Score results using director match + year proximity
        scored = []
        for result in results[:5]:  # Only score top 5 candidates
            score = 0
            tmdb_id = result.get("id")

            # Director match bonus
            if tmdb_id and director:
                credits_data = self.get_movie_credits(tmdb_id)
                if credits_data:
                    crew = credits_data.get("crew", [])
                    tmdb_directors = [c["name"] for c in crew if c.get("job") == "Director"]
                    for tmdb_dir in tmdb_directors:
                        if fuzz.ratio(director.lower(), tmdb_dir.lower()) >= 75:
                            score += 50
                            break

            # Year proximity score
            release_date = result.get("release_date", "")
            if year and release_date and len(release_date) >= 4:
                tmdb_year = int(release_date[:4])
                score += max(0, 10 - abs(year - tmdb_year))

            scored.append((score, result))

        # Sort by score descending, return best
        scored.sort(key=lambda x: x[0], reverse=True)
        return scored[0][1]

    def get_movie_external_ids(self, tmdb_id: int) -> dict | None:
        """Get external IDs (IMDB) for a movie."""
        return self._get(f"/movie/{tmdb_id}/external_ids")

    def search_person(self, name: str) -> dict | None:
        """Search for a person by name."""
        data = self._get("/search/person", {"query": name})
        if data and data.get("results"):
            return data["results"][0]
        return None

    def get_person(self, person_id: int) -> dict | None:
        """Get person details."""
        return self._get(f"/person/{person_id}")

    def get_movie_credits(self, tmdb_id: int) -> dict | None:
        """Get cast and crew credits for a movie."""
        return self._get(f"/movie/{tmdb_id}/credits")

    def get_tv_details(self, tmdb_id: int) -> dict | None:
        """Get TV series details."""
        return self._get(f"/tv/{tmdb_id}")

    def get_tv_external_ids(self, tmdb_id: int) -> dict | None:
        """Get external IDs (IMDB) for a TV series."""
        return self._get(f"/tv/{tmdb_id}/external_ids")

    def get_tv_credits(self, tmdb_id: int) -> dict | None:
        """Get aggregate credits for a TV series."""
        return self._get(f"/tv/{tmdb_id}/aggregate_credits")


def enrich_film(client: TMDBClient, film: dict, genres: dict, criterion_url_lookup: dict = None) -> dict:
    """Enrich a single film with TMDB data.

    Args:
        criterion_url_lookup: Optional dict of film_id -> criterion_film_url
            from picks_raw.json. Used to find criterion URLs for films that
            don't have one in the catalog yet.
    """
    title = film.get("title", "")
    year = film.get("year")
    film_id = film.get("film_id", "")
    is_tv = film_id in TMDB_TV_IDS or film.get("tmdb_type") == "tv"
    tmdb_id = film.get("tmdb_id")

    # --- Manual TMDB ID override (must run before skip check) ---
    if film_id in TMDB_ID_OVERRIDES:
        correct_id = TMDB_ID_OVERRIDES[film_id]
        if tmdb_id != correct_id:
            log(f"  Overriding TMDB ID for '{title}': {tmdb_id} -> {correct_id}")
            tmdb_id = correct_id
            film["tmdb_id"] = tmdb_id
            # Clear stale data so it gets re-fetched below
            for key in ("poster_url", "genres", "imdb_id", "credits", "director"):
                film.pop(key, None)

    # Skip if already fully enriched (including credits)
    if film.get("tmdb_id") and film.get("poster_url") and film.get("credits"):
        return film

    # --- Criterion URL disambiguation ---
    # Get metadata (year, director, image) from the Criterion film page
    criterion_url = film.get("criterion_url")
    if not criterion_url and criterion_url_lookup:
        criterion_url = criterion_url_lookup.get(film.get("film_id", ""))

    criterion_director = None
    if criterion_url:
        criterion_metadata = get_metadata_from_criterion_url(criterion_url)
        if criterion_metadata:
            if not year and criterion_metadata["year"]:
                log(f"  Got year {criterion_metadata['year']} from Criterion page for '{title}'")
                year = criterion_metadata["year"]
                film["year"] = year
            criterion_director = criterion_metadata.get("director")
            # Store director from Criterion if we don't have one yet
            if criterion_director and not film.get("director"):
                film["director"] = criterion_director

    # Use known TV ID if available
    if not tmdb_id and film_id in TMDB_TV_IDS:
        tmdb_id = TMDB_TV_IDS[film_id]
        film["tmdb_id"] = tmdb_id
        is_tv = True

    # Search TMDB if we don't have a tmdb_id yet
    if not tmdb_id:
        result = client.search_movie(title, year, director=criterion_director)
        if not result:
            return film

        # Cross-validate TMDB result against Criterion year
        tmdb_release_date = result.get("release_date", "")
        tmdb_year = None
        if tmdb_release_date and len(tmdb_release_date) >= 4:
            tmdb_year = int(tmdb_release_date[:4])

        if year and tmdb_year and abs(year - tmdb_year) > 2:
            log(f"  TMDB mismatch for '{title}': Criterion year={year}, TMDB year={tmdb_year} — skipping")
            return film

        tmdb_id = result.get("id")
        film["tmdb_id"] = tmdb_id

        # Year from TMDB (if missing)
        if not film.get("year") and tmdb_year:
            film["year"] = tmdb_year

        # Genres
        genre_ids = result.get("genre_ids", [])
        film["genres"] = [genres.get(gid, "") for gid in genre_ids if gid in genres]

        # Poster
        poster_path = result.get("poster_path")
        if poster_path:
            film["poster_url"] = f"{TMDB_IMAGE_BASE}/w185{poster_path}"

    # IMDB ID
    if tmdb_id and not film.get("imdb_id"):
        if is_tv:
            ext_ids = client.get_tv_external_ids(tmdb_id)
        else:
            ext_ids = client.get_movie_external_ids(tmdb_id)
        if ext_ids:
            film["imdb_id"] = ext_ids.get("imdb_id")

    # Credits (director, writer, cinematographer, editor, cast)
    if tmdb_id and not film.get("credits"):
        if is_tv:
            # For TV series, get creators from details and cast from aggregate_credits
            tv_details = client.get_tv_details(tmdb_id)
            credits_data = client.get_tv_credits(tmdb_id)
            creators = []
            if tv_details:
                creators = [
                    {"name": c["name"], "tmdb_id": c["id"]}
                    for c in tv_details.get("created_by", [])
                ]
                # Poster from TV details if missing
                if not film.get("poster_url") and tv_details.get("poster_path"):
                    film["poster_url"] = f"{TMDB_IMAGE_BASE}/w185{tv_details['poster_path']}"
                # Genres from TV details if missing
                if not film.get("genres") or film["genres"] == []:
                    film["genres"] = [g["name"] for g in tv_details.get("genres", [])]

            cast = credits_data.get("cast", []) if credits_data else []
            film["credits"] = {
                "directors": creators,
                "writers": creators,
                "cinematographers": [],
                "editors": [],
                "cast": [
                    {"name": c["name"], "tmdb_id": c["id"], "character": ""}
                    for c in cast[:8]
                ],
            }
            film["tmdb_type"] = "tv"

            if not film.get("director") and creators:
                film["director"] = creators[0]["name"]
        else:
            credits_data = client.get_movie_credits(tmdb_id)
            if credits_data:
                crew = credits_data.get("crew", [])
                cast = credits_data.get("cast", [])

                def crew_by_job(*jobs):
                    return [
                        {"name": c["name"], "tmdb_id": c["id"]}
                        for c in crew
                        if c.get("job") in jobs
                    ]

                film["credits"] = {
                    "directors": crew_by_job("Director"),
                    "writers": crew_by_job("Writer", "Screenplay"),
                    "cinematographers": crew_by_job("Director of Photography"),
                    "editors": crew_by_job("Editor"),
                    "cast": [
                        {"name": c["name"], "tmdb_id": c["id"], "character": c.get("character", "")}
                        for c in cast[:8]  # Top 8 billed
                    ],
                }

                # Also set director if not already set
                if not film.get("director") and film["credits"]["directors"]:
                    film["director"] = film["credits"]["directors"][0]["name"]

    return film


def clean_name_for_tmdb(name: str) -> list[str]:
    """Extract searchable individual names from a guest name.

    Returns a list of names to try in TMDB search order:
    - For multi-person names (contains " and " or " & "), returns individual names
    - Strips suffixes like "'s Closet Picks", "'s Criterion Picks"
    - Strips "(2nd Visit)", "(3rd Visit)", etc.
    """
    # Strip possessive suffixes
    name = re.sub(r"'s\s+(Closet|Criterion)\s+(Picks|Criterion Picks)$", "", name).strip()
    # Strip visit markers
    name = re.sub(r"\s*\(\d+\w+\s+Visit\)", "", name).strip()

    # Split multi-person names
    for sep in [" and ", " & "]:
        if sep in name:
            parts = [p.strip() for p in name.split(sep) if p.strip()]
            if len(parts) >= 2:
                return parts

    return [name] if name else []


def enrich_guest(client: TMDBClient, guest: dict, force: bool = False) -> dict:
    """Enrich a guest with TMDB person data.

    Args:
        force: If True, re-enrich guests missing either profession or photo
               (normally skips if BOTH exist).
    """
    name = guest.get("name", "")

    # Skip non-person guests
    if guest.get("guest_type") and guest["guest_type"] != "person":
        return guest

    # Skip if already fully enriched (unless force)
    if not force and guest.get("profession") and guest.get("photo_url"):
        return guest

    slug = guest.get("slug", "")

    # Use manual override if search can't find this person
    override_id = TMDB_PERSON_OVERRIDES.get(slug)
    if override_id and (not guest.get("profession") or not guest.get("photo_url")):
        result = client.get_person(override_id)
        if result:
            if not guest.get("profession"):
                department = result.get("known_for_department", "")
                guest["profession"] = DEPARTMENT_MAP.get(department, "other")
            if not guest.get("photo_url"):
                profile_path = result.get("profile_path")
                if profile_path:
                    guest["photo_url"] = f"{TMDB_IMAGE_BASE}/w185{profile_path}"
            if guest.get("profession") and guest.get("photo_url"):
                return guest

    names_to_try = clean_name_for_tmdb(name)
    if not names_to_try:
        return guest

    for try_name in names_to_try:
        result = client.search_person(try_name)
        if not result:
            continue

        # Profession (don't overwrite if already set)
        if not guest.get("profession"):
            department = result.get("known_for_department", "")
            guest["profession"] = DEPARTMENT_MAP.get(department, "other")

        # Photo (don't overwrite if already set)
        if not guest.get("photo_url"):
            profile_path = result.get("profile_path")
            if profile_path:
                guest["photo_url"] = f"{TMDB_IMAGE_BASE}/w185{profile_path}"

        # Stop if we found both
        if guest.get("profession") and guest.get("photo_url"):
            break

    return guest


def main():
    parser = argparse.ArgumentParser(description="Enrich data via TMDB")
    parser.add_argument("--pilot", action="store_true", help="Only enrich pilot data")
    parser.add_argument("--films-only", action="store_true", help="Only enrich films")
    parser.add_argument("--guests-only", action="store_true", help="Only enrich guests")
    parser.add_argument("--limit", type=int, default=0, help="Limit items to enrich")
    parser.add_argument("--force-guests", action="store_true",
                        help="Re-enrich guests missing either profession or photo")
    args = parser.parse_args()

    client = TMDBClient()

    # Load manual TMDB corrections from data file
    corrections_file = DATA_DIR / "tmdb_corrections.json"
    if corrections_file.exists():
        corrections = load_json(corrections_file)
        for film_id, correction in corrections.items():
            TMDB_ID_OVERRIDES[film_id] = correction["tmdb_id"]
        log(f"Loaded {len(corrections)} TMDB corrections from {corrections_file}")

    genres = client.get_genres()
    log(f"Loaded {len(genres)} genre mappings")

    do_films = not args.guests_only
    do_guests = not args.films_only

    # Enrich films
    if do_films:
        catalog = load_json(CATALOG_FILE)
        picks = load_json(PICKS_FILE)

        # Load picks_raw for criterion URL lookup (disambiguation)
        picks_raw = load_json(PICKS_RAW_FILE)
        criterion_url_lookup = build_criterion_url_lookup(picks_raw)
        log(f"Loaded {len(criterion_url_lookup)} criterion URLs from picks_raw")

        if args.pilot:
            # Only enrich films in pilot guests' picks
            pilot_slugs = {slugify(n) for n in PILOT_GUESTS}
            pilot_film_ids = set()
            for p in picks:
                if p.get("guest_slug") in pilot_slugs:
                    if p.get("catalog_spine"):
                        pilot_film_ids.add(p["catalog_spine"])

            films_to_enrich = [c for c in catalog if c["spine_number"] in pilot_film_ids]
            log(f"Enriching {len(films_to_enrich)} pilot films")
        else:
            films_to_enrich = catalog
            log(f"Enriching all {len(films_to_enrich)} films")

        if args.limit:
            films_to_enrich = films_to_enrich[:args.limit]

        enriched_count = 0
        for film in tqdm(films_to_enrich, desc="Enriching films"):
            before = (film.get("tmdb_id"), film.get("poster_url"))
            film = enrich_film(client, film, genres, criterion_url_lookup)
            after = (film.get("tmdb_id"), film.get("poster_url"))
            if before != after:
                enriched_count += 1

        save_json(CATALOG_FILE, catalog)
        log(f"Enriched {enriched_count} films, saved to {CATALOG_FILE}")

        # Summary
        with_tmdb = sum(1 for c in catalog if c.get("tmdb_id"))
        with_poster = sum(1 for c in catalog if c.get("poster_url"))
        with_imdb = sum(1 for c in catalog if c.get("imdb_id"))
        with_genres = sum(1 for c in catalog if c.get("genres"))
        with_year = sum(1 for c in catalog if c.get("year"))
        log(f"  TMDB IDs: {with_tmdb}, Posters: {with_poster}, IMDB: {with_imdb}")
        log(f"  Genres: {with_genres}, Years: {with_year}")

    # Enrich guests
    if do_guests:
        guests = load_json(GUESTS_FILE)

        if args.pilot:
            pilot_slugs = {slugify(n) for n in PILOT_GUESTS}
            guests_to_enrich = [g for g in guests if g["slug"] in pilot_slugs]
        else:
            guests_to_enrich = guests

        if args.limit:
            guests_to_enrich = guests_to_enrich[:args.limit]

        log(f"Enriching {len(guests_to_enrich)} guests" + (" (force mode)" if args.force_guests else ""))
        enriched_count = 0
        for guest in tqdm(guests_to_enrich, desc="Enriching guests"):
            before = (guest.get("profession"), guest.get("photo_url"))
            guest = enrich_guest(client, guest, force=args.force_guests)
            after = (guest.get("profession"), guest.get("photo_url"))
            if before != after:
                enriched_count += 1

        save_json(GUESTS_FILE, guests)
        log(f"Enriched {enriched_count} guests, saved to {GUESTS_FILE}")

        for g in guests_to_enrich:
            log(f"  {g['name']}: {g.get('profession', '?')}, photo={'yes' if g.get('photo_url') else 'no'}")


if __name__ == "__main__":
    main()
