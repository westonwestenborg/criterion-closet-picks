#!/usr/bin/env python3
"""Fixture tests for scripts.utils.apply_pick_overrides."""

import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts import utils

BERGMAN = "https://www.criterion.com/films/28206-dreams"
KUROSAWA = "https://www.criterion.com/films/28700-akira-kurosawa-s-dreams"
RULES = {
    "flying-lotus": [{
        "from_criterion_url": BERGMAN,
        "to_criterion_url": KUROSAWA,
        "to_film_id": "akira-kurosawas-dreams",
        "to_title": "Akira Kurosawa’s Dreams",
    }]
}


def scraped_pick() -> dict:
    return {
        "guest_slug": "flying-lotus",
        "film_id": "dreams",
        "film_title": "Dreams",
        "criterion_film_url": BERGMAN,
        "catalog_title": "Dreams",
        "catalog_spine": None,
        "match_method": "criterion_url",
        "quote": "Boom, Dreams, yeah.",
    }


class ApplyPickOverridesTest(unittest.TestCase):
    def test_replaces_film_and_clears_match_markers(self):
        pick = scraped_pick()
        with mock.patch.dict(utils.PICK_OVERRIDES, RULES, clear=True):
            self.assertEqual(utils.apply_pick_overrides([pick]), 1)
        self.assertEqual(pick["film_id"], "akira-kurosawas-dreams")
        self.assertEqual(pick["criterion_film_url"], KUROSAWA)
        # backfill_films.py re-resolves these from the new URL.
        self.assertIsNone(pick["match_method"])
        self.assertIsNone(pick["catalog_title"])
        self.assertIsNone(pick["catalog_spine"])

    def test_second_pass_changes_nothing(self):
        pick = scraped_pick()
        with mock.patch.dict(utils.PICK_OVERRIDES, RULES, clear=True):
            utils.apply_pick_overrides([pick])
            pick["match_method"] = "criterion_url"
            self.assertEqual(utils.apply_pick_overrides([pick]), 0)
        self.assertEqual(pick["match_method"], "criterion_url")


if __name__ == "__main__":
    unittest.main()
