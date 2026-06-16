#!/usr/bin/env python3
"""Fixture tests for scripts.extract_quotes helpers."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.extract_quotes import pick_index_key


class ExtractQuotesTest(unittest.TestCase):
    def test_pick_index_key_keeps_duplicate_titles_distinct(self):
        base = {
            "guest_slug": "guillermo-del-toro",
            "film_title": "Roma",
            "visit_index": 2,
            "source": "criterion",
        }
        roma_1972 = {
            **base,
            "film_id": "roma-1972",
            "catalog_spine": 848,
            "pick_order": 6,
            "criterion_film_url": "https://www.criterion.com/films/28039-roma",
        }
        roma_2018 = {
            **base,
            "film_id": "roma-2018",
            "catalog_spine": 1014,
            "pick_order": 10,
            "criterion_film_url": "https://www.criterion.com/films/30124-roma",
        }

        self.assertNotEqual(pick_index_key(roma_1972), pick_index_key(roma_2018))

    def test_pick_index_key_treats_order_drift_as_same_pick(self):
        existing = {
            "guest_slug": "wim-wenders",
            "film_id": "the-complete-jacques-tati",
            "film_title": "The Complete Jacques Tati",
            "visit_index": 1,
            "pick_order": 10,
        }
        raw = {
            **existing,
            "pick_order": 11,
        }

        self.assertEqual(pick_index_key(existing), pick_index_key(raw))


if __name__ == "__main__":
    unittest.main()
