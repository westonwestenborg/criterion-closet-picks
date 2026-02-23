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
from scripts.utils import (
    CATALOG_FILE,
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


# ---------------------------------------------------------------------------
# Criterion URL helpers
# ---------------------------------------------------------------------------

# Cache for Criterion page year lookups (criterion_url -> year or None)
_criterion_year_cache: dict[str, int | None] = {}
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


def get_year_from_criterion_url(criterion_url: str) -> int | None:
    """
    Scrape a Criterion film page to extract the release year.
    Criterion film pages show year in the page title or metadata area.
    Results are cached to avoid re-scraping.
    """
    if not criterion_url:
        return None

    if criterion_url in _criterion_year_cache:
        return _criterion_year_cache[criterion_url]

    scraper = _get_criterion_scraper()
    year = None

    try:
        resp = scraper.get(criterion_url, timeout=30)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "lxml")

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

        time.sleep(CRITERION_REQUEST_DELAY)

    except Exception as e:
        log(f"  Error scraping Criterion URL {criterion_url}: {e}")

    _criterion_year_cache[criterion_url] = year
    return year


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

    def search_movie(self, title: str, year: int = None) -> dict | None:
        """Search for a movie by title and optionally year."""
        params = {"query": title}
        if year:
            params["year"] = year
        data = self._get("/search/movie", params)
        if data and data.get("results"):
            return data["results"][0]
        # Try without year if year search failed
        if year:
            data = self._get("/search/movie", {"query": title})
            if data and data.get("results"):
                return data["results"][0]
        return None

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

    # Skip if already fully enriched (including credits)
    if film.get("tmdb_id") and film.get("poster_url") and film.get("credits"):
        return film

    film_id = film.get("film_id", "")
    is_tv = film_id in TMDB_TV_IDS or film.get("tmdb_type") == "tv"
    tmdb_id = film.get("tmdb_id")

    # --- Criterion URL disambiguation ---
    # If we have no year, try to get one from the Criterion film page
    criterion_url = film.get("criterion_url")
    if not criterion_url and criterion_url_lookup:
        criterion_url = criterion_url_lookup.get(film.get("film_id", ""))

    if not year and criterion_url:
        criterion_year = get_year_from_criterion_url(criterion_url)
        if criterion_year:
            log(f"  Got year {criterion_year} from Criterion page for '{title}'")
            year = criterion_year
            film["year"] = year

    # Use known TV ID if available
    if not tmdb_id and film_id in TMDB_TV_IDS:
        tmdb_id = TMDB_TV_IDS[film_id]
        film["tmdb_id"] = tmdb_id
        is_tv = True

    # Search TMDB if we don't have a tmdb_id yet
    if not tmdb_id:
        result = client.search_movie(title, year)
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
