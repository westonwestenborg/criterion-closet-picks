#!/usr/bin/env python3
"""Fixture tests for scripts.check_film_matches."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.check_film_matches import check_key, director_score, find_problems

# The Lovers as the catalog had it before Parker Posey picked it: Roland
# Joffé's 2015 film, while its Criterion page is Louis Malle's 1958 one.
WRONG_LOVERS = {
    "film_id": "the-lovers",
    "year": 2015,
    "director": "Roland Joffé",
    "tmdb_id": 79698,
    "credits": {"directors": [{"name": "Roland Joffé", "tmdb_id": 8965}]},
}
MALLE = {"year": 1958, "director": "Louis Malle"}


class FindProblemsTest(unittest.TestCase):
    def test_wrong_film_fails_on_year_director_and_tmdb(self):
        problems = find_problems(WRONG_LOVERS, MALLE)
        self.assertEqual(len(problems), 3)
        self.assertIn("year 2015, Criterion says 1958", problems)

    def test_right_film_passes(self):
        right = {
            "year": 1958,
            "director": "Louis Malle",
            "tmdb_id": 2574,
            "credits": {"directors": [{"name": "Louis Malle"}]},
        }
        self.assertEqual(find_problems(right, MALLE), [])

    def test_year_within_tolerance_passes(self):
        # Criterion often dates a film by its premiere, TMDB by its release.
        self.assertEqual(find_problems({"year": 1960}, {"year": 1958}), [])

    def test_co_directors_pass(self):
        film = {"director": "Michael Powell", "credits": {"directors": [{"name": "Michael Powell"}]},
                "tmdb_id": 1}
        criterion = {"director": "Michael Powell and Emeric Pressburger"}
        self.assertEqual(find_problems(film, criterion), [])

    def test_shown_co_director_passes_when_tmdb_credits_both(self):
        film = {"director": "Emeric Pressburger", "tmdb_id": 16391,
                "credits": {"directors": [{"name": "Emeric Pressburger"}, {"name": "Michael Powell"}]}}
        self.assertEqual(find_problems(film, {"director": "Michael Powell"}), [])

    def test_shown_director_fails_when_tmdb_match_is_wrong_too(self):
        # Both the shown director and TMDB's come from the wrong film.
        problems = find_problems(WRONG_LOVERS, {"director": "Louis Malle"})
        self.assertEqual(len(problems), 2)

    def test_various_checks_no_director(self):
        film = {"director": "David Perez Shadi", "tmdb_id": 283462,
                "credits": {"directors": [{"name": "David Perez Shadi"}]}}
        self.assertEqual(find_problems(film, {"director": "Various"}), [])

    def test_accents_do_not_count(self):
        self.assertEqual(director_score("Alfonso Cuaron", ["Alfonso Cuarón"]), 100)

    def test_missing_criterion_fields_check_nothing(self):
        self.assertEqual(find_problems(WRONG_LOVERS, {"year": None, "director": None}), [])

    def test_tmdb_directors_checked_even_when_shown_director_is_right(self):
        # The shown director came from Criterion, but the TMDB match is a remake.
        film = {"director": "David Lean", "tmdb_id": 99,
                "credits": {"directors": [{"name": "Mike Newell"}]}}
        problems = find_problems(film, {"director": "David Lean"})
        self.assertEqual(len(problems), 1)
        self.assertIn("TMDB 99", problems[0])


class CheckKeyTest(unittest.TestCase):
    def test_changes_when_tmdb_id_changes(self):
        url = "https://www.criterion.com/films/539-the-lovers"
        fixed = dict(WRONG_LOVERS, tmdb_id=2574)
        self.assertNotEqual(check_key(WRONG_LOVERS, url), check_key(fixed, url))


if __name__ == "__main__":
    unittest.main()
