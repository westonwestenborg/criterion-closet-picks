#!/usr/bin/env python3
"""Fixture tests for scripts.backfill_film_metadata."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.backfill_film_metadata import backfill_film_metadata


def issue(film_id: str, field: str) -> dict:
    return {
        "status": "open",
        "category": "film_metadata",
        "record_key": film_id,
        "evidence": {"field": field},
    }


class FakeTMDBClient:
    def __init__(self):
        self.details = {
            1: {
                "release_date": "2000-01-01",
                "poster_path": "/poster.jpg",
                "genres": [{"name": "Drama"}],
            },
            2: {
                "release_date": "1999-01-01",
                "poster_path": None,
                "genres": [],
            },
        }
        self.external_ids = {1: {"imdb_id": "tt1"}, 2: {"imdb_id": None}}
        self.tv_details = {
            10: {
                "first_air_date": "1988-02-15",
                "poster_path": "/tv.jpg",
                "genres": [{"name": "Comedy"}],
                "created_by": [{"name": "TV Creator", "id": 20}],
            }
        }
        self.tv_external_ids = {10: {"imdb_id": "tttv"}}
        self.credits = {
            1: {
                "crew": [
                    {"job": "Director", "name": "Correct Director", "id": 10},
                    {"job": "Writer", "name": "Writer", "id": 11},
                ],
                "cast": [{"name": "Actor", "id": 12, "character": "Lead"}],
            },
            2: {"crew": [], "cast": []},
        }
        self.tv_credits = {
            10: {
                "cast": [
                    {
                        "name": "TV Actor",
                        "id": 30,
                        "roles": [{"character": "Candidate"}],
                    }
                ]
            }
        }

    def _get(self, endpoint: str, params=None):
        tmdb_id = int(endpoint.rsplit("/", 1)[1])
        return self.details.get(tmdb_id)

    def get_movie_external_ids(self, tmdb_id: int):
        return self.external_ids.get(tmdb_id)

    def get_movie_credits(self, tmdb_id: int):
        return self.credits.get(tmdb_id)

    def get_tv_details(self, tmdb_id: int):
        return self.tv_details.get(tmdb_id)

    def get_tv_external_ids(self, tmdb_id: int):
        return self.tv_external_ids.get(tmdb_id)

    def get_tv_credits(self, tmdb_id: int):
        return self.tv_credits.get(tmdb_id)


class BackfillFilmMetadataTest(unittest.TestCase):
    def test_verified_override_replaces_stale_tmdb_fields(self):
        catalog = [
            {
                "film_id": "film",
                "title": "Film",
                "tmdb_id": 99,
                "year": 2020,
                "director": "Wrong Director",
                "genres": [],
                "imdb_id": None,
                "poster_url": None,
                "credits": None,
            }
        ]

        repaired, report = backfill_film_metadata(
            catalog,
            [issue("film", "genres"), issue("film", "imdb_id")],
            FakeTMDBClient(),
            {"film": 1},
        )

        film = repaired[0]
        self.assertEqual(film["tmdb_id"], 1)
        self.assertEqual(film["year"], 2000)
        self.assertEqual(film["director"], "Correct Director")
        self.assertEqual(film["genres"], ["Drama"])
        self.assertEqual(film["imdb_id"], "tt1")
        self.assertTrue(film["poster_url"].endswith("/poster.jpg"))
        self.assertEqual(report["summary"]["review_items"], 0)

    def test_trusted_existing_id_only_fills_missing_target_fields(self):
        catalog = [
            {
                "film_id": "film",
                "title": "Film",
                "tmdb_id": 1,
                "year": 1980,
                "director": "Existing Director",
                "genres": [],
                "imdb_id": None,
            }
        ]

        repaired, report = backfill_film_metadata(
            catalog,
            [issue("film", "genres"), issue("film", "imdb_id")],
            FakeTMDBClient(),
            {},
            trusted_existing_ids={"film"},
        )

        film = repaired[0]
        self.assertEqual(film["year"], 1980)
        self.assertEqual(film["director"], "Existing Director")
        self.assertEqual(film["genres"], ["Drama"])
        self.assertEqual(film["imdb_id"], "tt1")
        self.assertEqual(report["summary"]["review_items"], 0)

    def test_untrusted_existing_id_is_reported_not_mutated(self):
        catalog = [{"film_id": "film", "title": "Film", "tmdb_id": 1, "genres": []}]

        repaired, report = backfill_film_metadata(
            catalog,
            [issue("film", "genres")],
            FakeTMDBClient(),
            {},
            trusted_existing_ids=set(),
        )

        self.assertEqual(repaired[0]["genres"], [])
        self.assertEqual(report["summary"]["review_by_type"], {"film_metadata_not_auto_repaired": 1})

    def test_missing_tmdb_id_is_reported(self):
        catalog = [{"film_id": "film", "title": "Film", "tmdb_id": None, "genres": []}]

        repaired, report = backfill_film_metadata(
            catalog,
            [issue("film", "tmdb_id")],
            FakeTMDBClient(),
            {},
        )

        self.assertIsNone(repaired[0]["tmdb_id"])
        self.assertEqual(report["summary"]["review_by_type"], {"film_metadata_missing_verified_tmdb_id": 1})

    def test_unavailable_target_field_is_reported(self):
        catalog = [{"film_id": "film", "title": "Film", "tmdb_id": 2, "genres": []}]

        repaired, report = backfill_film_metadata(
            catalog,
            [issue("film", "genres")],
            FakeTMDBClient(),
            {"film": 2},
        )

        self.assertEqual(repaired[0]["genres"], [])
        self.assertEqual(report["summary"]["review_by_type"], {"film_metadata_field_unavailable": 1})

    def test_direct_field_backfill_applies_without_tmdb(self):
        catalog = [{"film_id": "film", "title": "Film", "tmdb_id": None, "director": ""}]

        repaired, report = backfill_film_metadata(
            catalog,
            [issue("film", "director")],
            FakeTMDBClient(),
            {},
            field_backfills={"film": {"director": "Verified Director"}},
        )

        self.assertEqual(repaired[0]["director"], "Verified Director")
        self.assertEqual(report["summary"]["review_items"], 0)

    def test_verified_tv_id_uses_tv_metadata_and_marks_type(self):
        catalog = [
            {
                "film_id": "series",
                "title": "Series",
                "tmdb_id": 10,
                "genres": [],
                "imdb_id": None,
                "credits": None,
            }
        ]

        repaired, report = backfill_film_metadata(
            catalog,
            [issue("series", "genres"), issue("series", "imdb_id")],
            FakeTMDBClient(),
            {},
            verified_tv_ids={"series": 10},
        )

        film = repaired[0]
        self.assertEqual(film["tmdb_type"], "tv")
        self.assertEqual(film["genres"], ["Comedy"])
        self.assertEqual(film["imdb_id"], "tttv")
        self.assertEqual(film["director"], "TV Creator")
        self.assertEqual(film["credits"]["cast"][0]["character"], "Candidate")
        self.assertEqual(report["summary"]["review_items"], 0)


if __name__ == "__main__":
    unittest.main()
