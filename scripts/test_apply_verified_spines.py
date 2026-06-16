#!/usr/bin/env python3
"""Fixture tests for scripts.apply_verified_spines."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.apply_verified_spines import apply_verified_spines


class ApplyVerifiedSpinesTest(unittest.TestCase):
    def test_verified_spine_updates_missing_catalog_spine(self):
        catalog = [
            {
                "film_id": "movie",
                "title": "Movie",
                "criterion_url": "https://www.criterion.com/films/1-movie",
                "spine_number": None,
            }
        ]
        records = [
            {
                "film_id": "movie",
                "criterion_url": "https://www.criterion.com/films/1-movie",
                "status": "verified_spine",
                "spine_number": 101,
                "evidence": "Spine #101",
            }
        ]

        repaired_catalog, report = apply_verified_spines(catalog, records)

        self.assertEqual(repaired_catalog[0]["spine_number"], 101)
        self.assertEqual(report["summary"]["spines_updated"], 1)
        self.assertEqual(report["changes"][0]["film_id"], "movie")

    def test_no_spine_visible_record_does_not_change_catalog(self):
        catalog = [
            {
                "film_id": "movie",
                "title": "Movie",
                "criterion_url": "https://www.criterion.com/films/1-movie",
                "spine_number": None,
            }
        ]
        records = [
            {
                "film_id": "movie",
                "criterion_url": "https://www.criterion.com/films/1-movie",
                "status": "no_spine_visible",
                "note": "Criterion page has no visible Spine # label.",
            }
        ]

        repaired_catalog, report = apply_verified_spines(catalog, records)

        self.assertIsNone(repaired_catalog[0]["spine_number"])
        self.assertEqual(report["summary"]["spines_updated"], 0)
        self.assertEqual(report["summary"]["no_spine_visible"], 1)

    def test_url_mismatch_is_reported_and_not_applied(self):
        catalog = [
            {
                "film_id": "movie",
                "title": "Movie",
                "criterion_url": "https://www.criterion.com/films/1-movie",
                "spine_number": None,
            }
        ]
        records = [
            {
                "film_id": "movie",
                "criterion_url": "https://www.criterion.com/films/2-other",
                "status": "verified_spine",
                "spine_number": 101,
            }
        ]

        repaired_catalog, report = apply_verified_spines(catalog, records)

        self.assertIsNone(repaired_catalog[0]["spine_number"])
        self.assertEqual(report["summary"]["review_items"], 1)
        self.assertEqual(report["review_items"][0]["action"], "criterion_url_mismatch")

    def test_report_order_is_deterministic(self):
        catalog = [
            {
                "film_id": "b",
                "title": "B",
                "criterion_url": "https://www.criterion.com/films/2-b",
                "spine_number": None,
            },
            {
                "film_id": "a",
                "title": "A",
                "criterion_url": "https://www.criterion.com/films/1-a",
                "spine_number": None,
            },
        ]
        records = [
            {
                "film_id": "b",
                "criterion_url": "https://www.criterion.com/films/2-b",
                "status": "verified_spine",
                "spine_number": 2,
            },
            {
                "film_id": "a",
                "criterion_url": "https://www.criterion.com/films/1-a",
                "status": "verified_spine",
                "spine_number": 1,
            },
        ]

        _, first = apply_verified_spines(catalog, records)
        _, second = apply_verified_spines(list(reversed(catalog)), list(reversed(records)))

        self.assertEqual(
            [item["film_id"] for item in first["changes"]],
            [item["film_id"] for item in second["changes"]],
        )


if __name__ == "__main__":
    unittest.main()
