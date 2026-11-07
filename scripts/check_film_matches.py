#!/usr/bin/env python3
"""
Check each picked film's year, director and TMDB match against its own
Criterion page.

A catalog film is matched to TMDB once, often long before anyone picks it and
before it has a Criterion URL. enrich_tmdb.py compares year and director with
the Criterion page only while it searches TMDB, so a wrong match made back then
survives the film's first pick. Parker Posey's The Lovers (Louis Malle, 1958)
was built as Roland Joffé's 2015 film for that reason. The August 2026 audit
fixed 81 such films, but it only covered films picked by then.

A film fails when the Criterion page disagrees with:
  - the year we show (by more than YEAR_TOLERANCE years),
  - the director we show, or
  - the directors of the TMDB film we matched (wrong poster, cast and genres).

Films that pass are remembered in data/.film_match_checks.json (gitignored),
with the Criterion URL, TMDB id, year and director they passed with, so a
pipeline run only fetches pages for films that are new or have changed. A film
that fails is not remembered and is reported on every run until it is fixed,
usually with an entry in data/tmdb_corrections.json, or accepted under
accepted_issue_film_ids.film_criterion_mismatch in
data/validation/known_data_exceptions.json.

Usage:
  python scripts/check_film_matches.py                    # picked films not checked yet
  python scripts/check_film_matches.py --guest-slug SLUG  # one guest's picks, checked again
  python scripts/check_film_matches.py --all              # every picked film, checked again
  python scripts/check_film_matches.py --all --include-unpicked

Exits 1 when any film fails.
"""

import argparse
import sys
import unicodedata
from pathlib import Path

from thefuzz import fuzz

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    DATA_DIR,
    PICKS_FILE,
    PICKS_RAW_FILE,
    VALIDATION_DIR,
    load_json,
    log,
    save_json,
)

CHECKS_FILE = DATA_DIR / ".film_match_checks.json"
EXCEPTIONS_FILE = VALIDATION_DIR / "known_data_exceptions.json"
ACCEPTED_ISSUE = "film_criterion_mismatch"

# The thresholds the August 2026 audit used. token_set_ratio scores a name that
# is a subset of the other as 100, so "Michael Powell" passes against
# "Michael Powell and Emeric Pressburger".
YEAR_TOLERANCE = 2
DIRECTOR_MIN_SCORE = 60


def _fold(name: str) -> str:
    decomposed = unicodedata.normalize("NFKD", name)
    return "".join(c for c in decomposed if not unicodedata.combining(c)).lower()


def director_score(criterion_director: str, names: list[str]) -> int:
    """Best match between the Criterion director and any of `names`, 0 to 100."""
    return max(
        (fuzz.token_set_ratio(_fold(criterion_director), _fold(n)) for n in names if n),
        default=0,
    )


def find_problems(film: dict, criterion: dict) -> list[str]:
    """Ways the catalog film disagrees with its Criterion page metadata."""
    problems = []

    criterion_year = criterion.get("year")
    year = film.get("year")
    if criterion_year and year and abs(criterion_year - year) > YEAR_TOLERANCE:
        problems.append(f"year {year}, Criterion says {criterion_year}")

    criterion_director = criterion.get("director")
    # Anthologies (Beastie Boys Video Anthology) credit "Various".
    if criterion_director and _fold(criterion_director) != "various":
        tmdb_directors = [
            d.get("name", "") for d in (film.get("credits") or {}).get("directors") or []
        ]
        tmdb_agrees = director_score(criterion_director, tmdb_directors) >= DIRECTOR_MIN_SCORE

        # A co-director passes: we show Emeric Pressburger for Black Narcissus,
        # Criterion credits Michael Powell, and TMDB credits both.
        shown = film.get("director")
        if (
            shown
            and director_score(criterion_director, [shown]) < DIRECTOR_MIN_SCORE
            and not (tmdb_agrees and director_score(shown, tmdb_directors) >= DIRECTOR_MIN_SCORE)
        ):
            problems.append(f"director {shown}, Criterion says {criterion_director}")

        if film.get("tmdb_id") and tmdb_directors and not tmdb_agrees:
            problems.append(
                f"TMDB {film['tmdb_id']} is directed by {', '.join(tmdb_directors)}, "
                f"Criterion says {criterion_director}"
            )

    return problems


def check_key(film: dict, criterion_url: str) -> dict:
    """What a film passed with. A change to any of these means check it again."""
    return {
        "criterion_url": criterion_url,
        "tmdb_id": film.get("tmdb_id"),
        "year": film.get("year"),
        "director": film.get("director"),
    }


def load_accepted() -> dict:
    if not EXCEPTIONS_FILE.exists():
        return {}
    raw = load_json(EXCEPTIONS_FILE)
    accepted = (raw.get("accepted_issue_film_ids") or {}).get(ACCEPTED_ISSUE) or {}
    return accepted if isinstance(accepted, dict) else {fid: "" for fid in accepted}


def main():
    parser = argparse.ArgumentParser(description="Check films against their Criterion pages")
    parser.add_argument("--guest-slug", help="Check only this guest's picks, ignoring past passes")
    parser.add_argument("--all", action="store_true", help="Check every picked film again")
    parser.add_argument(
        "--include-unpicked", action="store_true",
        help="Also check catalog films nobody has picked that have a Criterion URL",
    )
    args = parser.parse_args()

    # Imported here so --help works without Playwright or TMDB keys.
    from scripts.enrich_tmdb import (
        TMDBClient,
        build_criterion_url_lookup,
        get_metadata_from_criterion_url,
    )

    catalog = load_json(CATALOG_FILE)
    picks = load_json(PICKS_FILE)
    url_lookup = build_criterion_url_lookup(load_json(PICKS_RAW_FILE))
    checks = load_json(CHECKS_FILE) if CHECKS_FILE.exists() else {}
    accepted = load_accepted()

    if args.guest_slug:
        wanted = {p["film_id"] for p in picks if p.get("guest_slug") == args.guest_slug}
        if not wanted:
            log(f"No picks for guest '{args.guest_slug}'")
            sys.exit(1)
    else:
        wanted = {p["film_id"] for p in picks}

    candidates = []
    for film in catalog:
        film_id = film.get("film_id", "")
        if film.get("is_box_set"):
            continue
        if film_id not in wanted and not (args.include_unpicked and not args.guest_slug):
            continue
        url = film.get("criterion_url") or url_lookup.get(film_id) or ""
        if "/films/" not in url:
            continue
        if not (args.all or args.guest_slug) and checks.get(film_id) == check_key(film, url):
            continue
        candidates.append((film, url))

    log(f"Checking {len(candidates)} films against their Criterion pages")

    failed, unreadable = [], []
    for i, (film, url) in enumerate(candidates, 1):
        film_id = film["film_id"]
        criterion = get_metadata_from_criterion_url(url) or {}
        if not criterion.get("year") and not criterion.get("director"):
            unreadable.append((film, url))
            continue
        problems = find_problems(film, criterion)
        if problems and film_id not in accepted:
            failed.append((film, url, criterion, problems))
            checks.pop(film_id, None)
        else:
            checks[film_id] = check_key(film, url)
        if i % 50 == 0:
            log(f"  {i}/{len(candidates)} checked, {len(failed)} failed")
            save_json(CHECKS_FILE, checks)

    save_json(CHECKS_FILE, checks)

    if unreadable:
        print(f"\nCriterion page gave no year or director ({len(unreadable)}), will retry next run:")
        for film, url in unreadable:
            print(f"  {film['film_id']}: {url}")

    if not failed:
        print(f"\nAll {len(candidates) - len(unreadable)} readable films match their Criterion pages.")
        return

    client = TMDBClient()
    print(f"\nFilms that disagree with their Criterion page ({len(failed)}):")
    for film, url, criterion, problems in failed:
        print(f"\n  {film['film_id']} ({film.get('title')}) {url}")
        for problem in problems:
            print(f"    - {problem}")
        try:
            hit = client.search_movie(film.get("title", ""), criterion.get("year"),
                                      director=criterion.get("director"))
        except Exception:  # a suggestion only; never block the report on it
            hit = None
        if hit and hit.get("id") != film.get("tmdb_id"):
            print(f"    TMDB search with Criterion's year and director: {hit['id']} "
                  f"{hit.get('title')} ({hit.get('release_date', '')[:4]})")
    print(
        "\nFor each one, confirm the right film (Criterion page, guest transcript), then "
        "pin it in data/tmdb_corrections.json and re-run enrich_tmdb.py. If the catalog "
        "is right and Criterion's page is the odd one out (a cinematographer credited "
        f"as director), add it to accepted_issue_film_ids.{ACCEPTED_ISSUE} in "
        "data/validation/known_data_exceptions.json with the reason."
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
