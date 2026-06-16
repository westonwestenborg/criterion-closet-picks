#!/usr/bin/env python3
"""Fixture tests for scripts.repair_suspicious_tmdb_matches."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.repair_suspicious_tmdb_matches import repair_suspicious_tmdb_matches


def pick(**overrides):
    row = {
        "guest_slug": "guest",
        "visit_index": 1,
        "film_id": "film",
        "film_title": "Film",
        "catalog_title": "Film",
        "catalog_spine": None,
        "criterion_film_url": "",
        "match_method": "exact",
        "quote": "",
    }
    row.update(overrides)
    return row


class RepairSuspiciousTMDBMatchesTest(unittest.TestCase):
    def test_repairs_traffic_without_touching_unrelated_trafic(self):
        catalog = [
            {
                "film_id": "trafic",
                "title": "Trafic",
                "year": 2000,
                "director": "Wrong",
                "spine_number": 439,
                "criterion_url": "https://www.criterion.com/films/381-traffic",
                "tmdb_id": 1191431,
                "credits": {"directors": [{"name": "Wrong", "tmdb_id": 1}]},
            }
        ]
        picks = [
            pick(
                film_id="trafic",
                film_title="Trafic",
                catalog_title="Trafic",
                catalog_spine=439,
                criterion_film_url="https://www.criterion.com/films/381-traffic",
            )
        ]

        repaired_catalog, repaired_picks, repaired_raw, report = repair_suspicious_tmdb_matches(
            catalog,
            picks,
            picks,
        )

        self.assertEqual(repaired_catalog[0]["film_id"], "traffic")
        self.assertEqual(repaired_catalog[0]["title"], "Traffic")
        self.assertEqual(repaired_catalog[0]["spine_number"], 151)
        self.assertEqual(repaired_catalog[0]["tmdb_id"], 1900)
        self.assertNotIn("credits", repaired_catalog[0])
        self.assertEqual(repaired_picks[0]["film_id"], "traffic")
        self.assertEqual(repaired_picks[0]["catalog_spine"], 151)
        self.assertEqual(repaired_raw[0]["film_title"], "Traffic")
        self.assertGreater(report["summary"]["total_changes"], 0)

    def test_splits_the_innocent_from_the_innocents_by_url(self):
        catalog = [
            {
                "film_id": "the-innocents",
                "title": "The Innocents",
                "year": 2008,
                "director": "Bill Viola",
                "spine_number": 727,
                "criterion_url": "https://www.criterion.com/films/33649-the-innocent",
                "tmdb_id": 673155,
                "credits": {},
            }
        ]
        picks = [
            pick(
                guest_slug="jr",
                film_id="the-innocents",
                criterion_film_url="https://www.criterion.com/films/33649-the-innocent",
                catalog_spine=727,
            ),
            pick(
                guest_slug="robert-eggers",
                film_id="the-innocents",
                criterion_film_url="https://www.criterion.com/films/28569-the-innocents",
                catalog_spine=727,
            ),
        ]

        repaired_catalog, repaired_picks, _, _ = repair_suspicious_tmdb_matches(catalog, picks, picks)
        by_id = {film["film_id"]: film for film in repaired_catalog}

        self.assertEqual(by_id["the-innocents"]["tmdb_id"], 16372)
        self.assertEqual(by_id["the-innocents"]["director"], "Jack Clayton")
        self.assertEqual(by_id["the-innocent"]["spine_number"], None)
        self.assertEqual(repaired_picks[0]["film_id"], "the-innocent")
        self.assertEqual(repaired_picks[0]["film_title"], "The Innocent")
        self.assertEqual(repaired_picks[1]["film_id"], "the-innocents")

    def test_splits_straw_dogs_from_stray_dog_by_guest(self):
        catalog = [
            {
                "film_id": "stray-dog",
                "title": "Stray Dog",
                "year": 1971,
                "director": "Sam Peckinpah",
                "spine_number": 233,
                "criterion_url": "https://www.criterion.com/films/730-straw-dogs",
                "tmdb_id": 994,
                "credits": {},
            }
        ]
        picks = [
            pick(
                guest_slug="ari-aster",
                film_id="stray-dog",
                criterion_film_url="https://www.criterion.com/films/730-straw-dogs",
                catalog_spine=233,
            ),
            pick(
                guest_slug="johnnie-to",
                film_id="stray-dog",
                criterion_film_url="https://www.criterion.com/films/730-straw-dogs",
                catalog_spine=233,
            ),
        ]

        repaired_catalog, repaired_picks, _, _ = repair_suspicious_tmdb_matches(catalog, picks, picks)
        by_id = {film["film_id"]: film for film in repaired_catalog}

        self.assertEqual(by_id["stray-dog"]["tmdb_id"], 30368)
        self.assertEqual(by_id["stray-dog"]["criterion_url"], "https://www.criterion.com/films/788-stray-dog")
        self.assertEqual(by_id["straw-dogs"]["spine_number"], 182)
        self.assertEqual(repaired_picks[0]["film_id"], "straw-dogs")
        self.assertEqual(repaired_picks[0]["catalog_spine"], 182)
        self.assertEqual(repaired_picks[1]["film_id"], "stray-dog")
        self.assertEqual(repaired_picks[1]["criterion_film_url"], "https://www.criterion.com/films/788-stray-dog")

    def test_clears_che_making_of_tmdb_match_but_preserves_genre_classification(self):
        catalog = [
            {
                "film_id": "che",
                "title": "Che",
                "tmdb_id": 860565,
                "imdb_id": None,
                "genres": [],
                "credits": {"cast": [{"name": "Steven Soderbergh"}]},
            }
        ]

        repaired_catalog, _, _, _ = repair_suspicious_tmdb_matches(catalog, [], [])

        self.assertIsNone(repaired_catalog[0]["tmdb_id"])
        self.assertEqual(repaired_catalog[0]["genres"], ["Drama", "History", "War"])
        self.assertNotIn("credits", repaired_catalog[0])


if __name__ == "__main__":
    unittest.main()
