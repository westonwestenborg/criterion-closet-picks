"""
Documented schemas for the JSON data files in data/.

This is the pipeline's contract: scripts pass these shapes around as plain
dicts, and src/lib/data.ts (lines ~5-69) is the frontend twin that consumes
them. The frontend renames some fields at load time; renames must touch BOTH
files:

    pipeline (here)        frontend (data.ts)
    -----------------      ----------------------------
    Pick.film_id        -> Pick.film_slug
    Pick.start_timestamp-> Pick.start_timestamp_seconds
    CatalogFilm.film_id -> Film.slug
    CatalogFilm.spine_number -> Film.criterion_spine_number

These are typing.TypedDict declarations (total=False: most fields are
optional in practice — older records predate newer fields). They document
shape and enable IDE/mypy support; they do not validate at runtime.
Field shapes surveyed from the live data files, June 2026.
"""

from typing import TypedDict


class Credits(TypedDict, total=False):
    """TMDB credits on a catalog film. Each entry: {name, tmdb_id} (+character for cast)."""

    directors: list[dict]
    writers: list[dict]
    cinematographers: list[dict]
    editors: list[dict]
    cast: list[dict]


class CatalogFilm(TypedDict, total=False):
    """One entry in criterion_catalog.json. Written by build_catalog.py,
    supplemented by backfill_films.py (non-catalog films that appear in picks)
    and group_box_sets.py (box set entries), enriched by enrich_tmdb.py."""

    film_id: str  # slug + year, e.g. "seven-samurai-1954"; frontend `slug`
    title: str
    spine_number: int | None  # frontend `criterion_spine_number`
    director: str | None
    year: int | None
    country: str
    criterion_url: str
    imdb_id: str | None
    tmdb_id: int | None
    tmdb_type: str  # rare: "tv" for the odd series entry
    poster_url: str | None
    poster_source: str  # "criterion" or "tmdb"
    genres: list[str]
    credits: Credits | None
    pick_count: int  # if present, overrides the frontend's computed count
    # Box set entries only (grouped by group_box_sets.py):
    is_box_set: bool
    description: str
    included_films: list[str]
    box_set_film_count: int


class GuestVisit(TypedDict, total=False):
    """One closet visit. Multi-visit guests (Bill Hader etc.) have 2+;
    built by normalize_guests.py from data/visit_criterion_urls.json."""

    visit_index: int  # 1-based
    youtube_video_id: str | None
    youtube_video_url: str | None
    vimeo_video_id: str | None
    episode_date: str | None  # ISO date
    criterion_page_url: str | None


class Guest(TypedDict, total=False):
    """One entry in guests.json. Written by scrape_criterion_picks.py,
    normalized by normalize_guests.py, enriched by enrich_tmdb.py
    (photo/profession) and backfill_dates.py (episode_date)."""

    name: str
    slug: str
    profession: str | None  # controlled vocabulary, see normalize_guests.py
    photo_url: str | None
    youtube_video_id: str | None
    youtube_video_url: str | None
    vimeo_video_id: str | None
    episode_date: str | None  # ISO date of (first) episode
    criterion_page_url: str  # canonical Criterion collection URL
    pick_count: int  # recalculated by migrate_source_visit.py
    visits: list[GuestVisit]  # only present for multi-visit guests
    visit_count: int
    source: str
    tmdb_person_id: int | None


class Pick(TypedDict, total=False):
    """One entry in picks.json (and, minus quote fields, picks_raw.json).
    picks_raw.json is written by scrape_criterion_picks.py; extract_quotes.py
    merges in quotes/timestamps to produce picks.json; group_box_sets.py adds
    the box_set_* fields; migrate_source_visit.py maintains source/visit_index."""

    guest_slug: str
    guest_name: str
    film_id: str  # matches CatalogFilm.film_id; frontend `film_slug`
    film_title: str  # title as scraped from the source page
    film_year: int | None
    catalog_title: str | None  # matched Criterion catalog title
    catalog_spine: int | None
    criterion_film_url: str
    match_method: str | None  # how film_id was matched (exact/fuzzy/...)
    source: str  # "criterion" (current) or "letterboxd" (legacy)
    visit_index: int | None  # 1-based; which visit this pick belongs to
    pick_order: int
    # Quote extraction (extract_quotes.py):
    quote: str  # "" when no quote was found
    start_timestamp: int | None  # seconds; frontend `start_timestamp_seconds`
    youtube_timestamp_url: str
    vimeo_timestamp_url: str
    extraction_confidence: str  # "high" | "medium" | "low" | "none"
    # Box set handling (group_box_sets.py):
    is_box_set: bool  # pick is an individual film inside a box set
    box_set_name: str | None
    box_set_criterion_url: str
    box_set_film_count: int  # present only on aggregate box-set picks
    box_set_film_titles: list[str]
