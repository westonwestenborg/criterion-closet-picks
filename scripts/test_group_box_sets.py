#!/usr/bin/env python3
"""Fixture tests for scripts.group_box_sets."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.group_box_sets import build_known_box_set_map, group_picks_for_guest

FELLINI_URL = "https://www.criterion.com/boxsets/3626-essential-fellini"
FELLINI_ROMA = "https://www.criterion.com/films/28039-roma"
CUARON_ROMA = "https://www.criterion.com/films/30124-roma"

CATALOG_BY_ID = {
    "roma-1972": {"film_id": "roma-1972", "criterion_url": FELLINI_ROMA},
    "roma-2018": {"film_id": "roma-2018", "criterion_url": CUARON_ROMA},
}
FELLINI_BOX = {
    "film_id": "essential-fellini",
    "criterion_url": FELLINI_URL,
    "included_films": [{"criterion_url": FELLINI_ROMA, "title": "Roma"}],
}


def roma_pick(film_id: str, **extra) -> dict:
    return {
        "guest_slug": "guest",
        "film_id": film_id,
        "film_title": "Roma",
        "quote": "Roma is a masterpiece.",
        "extraction_confidence": "high",
        **extra,
    }


def group(picks: list[dict], catalog_by_url: dict) -> list[dict]:
    return group_picks_for_guest(
        picks, {}, build_known_box_set_map(), {"Essential Fellini": FELLINI_URL},
        CATALOG_BY_ID, catalog_by_url, {},
    )


class BoxMembershipTest(unittest.TestCase):
    def test_film_missing_from_box_film_list_is_not_tagged(self):
        [pick] = group([roma_pick("roma-2018")], {FELLINI_URL: FELLINI_BOX})
        self.assertFalse(pick.get("is_box_set"))
        self.assertIsNone(pick.get("box_set_name"))

    def test_stale_tag_from_earlier_run_is_cleared(self):
        stale = roma_pick("roma-2018", is_box_set=True, box_set_name="Essential Fellini")
        [pick] = group([stale], {FELLINI_URL: FELLINI_BOX})
        self.assertFalse(pick["is_box_set"])
        self.assertIsNone(pick["box_set_name"])

    def test_film_in_box_film_list_stays_tagged(self):
        [pick] = group([roma_pick("roma-1972")], {FELLINI_URL: FELLINI_BOX})
        self.assertTrue(pick["is_box_set"])
        self.assertEqual(pick["box_set_name"], "Essential Fellini")

    def test_box_without_film_list_keeps_title_match(self):
        # Nothing to check against, so the title match stands.
        [pick] = group([roma_pick("roma-2018")], {})
        self.assertEqual(pick["box_set_name"], "Essential Fellini")


if __name__ == "__main__":
    unittest.main()
