#!/usr/bin/env python3
"""Fixture tests for scripts.resolve_duplicate_criterion_urls."""

import sys
import unittest
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.resolve_duplicate_criterion_urls import (
    resolve_duplicate_criterion_urls,
    update_guest_pick_counts,
)


def catalog_row(film_id, title, url="", spine=None, is_box_set=False):
    return {
        "film_id": film_id,
        "title": title,
        "criterion_url": url,
        "spine_number": spine,
        "is_box_set": is_box_set,
    }


def pick_row(**overrides):
    pick = {
        "guest_slug": "guest",
        "guest_name": "Guest",
        "film_title": "Title",
        "film_id": "title",
        "catalog_spine": None,
        "catalog_title": "Title",
        "criterion_film_url": "",
        "source": "criterion",
        "match_method": "exact",
        "visit_index": 1,
        "pick_order": 1,
        "quote": "",
    }
    pick.update(overrides)
    return pick


class ResolveDuplicateCriterionUrlsTest(unittest.TestCase):
    def test_gregg_araki_box_set_numeric_id_merges_into_slug_id(self):
        catalog = [
            catalog_row(
                "gregg-arakis-teen-apocalypse-trilogy",
                "Gregg Araki’s Teen Apocalypse Trilogy",
                "https://www.criterion.com/boxsets/7581-gregg-araki-s-teen-apocalypse-trilogy",
                1233,
                True,
            ),
            catalog_row(
                "7581-gregg-araki-s-teen-apocalypse-trilogy",
                "Gregg Araki's Teen Apocalypse Trilogy",
                "https://www.criterion.com/boxsets/7581-gregg-araki-s-teen-apocalypse-trilogy",
                1233,
                True,
            ),
        ]
        picks = [
            pick_row(
                film_id="7581-gregg-araki-s-teen-apocalypse-trilogy",
                film_title="Gregg Araki's Teen Apocalypse Trilogy",
                catalog_title="Gregg Araki's Teen Apocalypse Trilogy",
                catalog_spine=1233,
                criterion_film_url="https://www.criterion.com/boxsets/7581-gregg-araki-s-teen-apocalypse-trilogy",
            )
        ]

        repaired_catalog, repaired_picks, _, report = resolve_duplicate_criterion_urls(
            catalog,
            picks,
            picks,
        )

        self.assertEqual(
            [film["film_id"] for film in repaired_catalog],
            ["gregg-arakis-teen-apocalypse-trilogy"],
        )
        self.assertEqual(
            repaired_picks[0]["film_id"],
            "gregg-arakis-teen-apocalypse-trilogy",
        )
        self.assertEqual(
            repaired_picks[0]["film_title"],
            "Gregg Araki’s Teen Apocalypse Trilogy",
        )
        self.assertEqual(
            report["summary"]["changes"]["data/criterion_catalog.json:duplicate_rows_removed"],
            1,
        )

    def test_something_wild_1961_merges_into_canonical_row_and_dedupes_raw(self):
        catalog = [
            catalog_row(
                "something-wild",
                "Something Wild",
                "https://www.criterion.com/films/28777-something-wild",
                563,
            ),
            catalog_row(
                "something-wild-1961",
                "Something Wild",
                "https://www.criterion.com/films/28777-something-wild",
                563,
            ),
        ]
        picks = [
            pick_row(
                guest_slug="colin",
                film_id="something-wild-1961",
                criterion_film_url="https://www.criterion.com/films/28777-something-wild",
                pick_order=6,
            )
        ]
        raw = [
            pick_row(
                guest_slug="colin",
                film_id="something-wild-1961",
                criterion_film_url="https://www.criterion.com/films/28777-something-wild",
                pick_order=6,
            ),
            pick_row(
                guest_slug="colin",
                film_id="something-wild",
                criterion_film_url="https://www.criterion.com/films/28777-something-wild",
                pick_order=9,
            ),
        ]

        _, repaired_picks, repaired_raw, report = resolve_duplicate_criterion_urls(
            catalog,
            picks,
            raw,
        )

        self.assertEqual(repaired_picks[0]["film_id"], "something-wild")
        self.assertEqual([pick["film_id"] for pick in repaired_raw], ["something-wild"])
        self.assertEqual(repaired_raw[0]["pick_order"], 6)
        self.assertEqual(
            report["summary"]["changes"]["data/picks_raw.json:duplicate_rows_removed"],
            1,
        )

    def test_the_killer_rows_are_mapped_by_source_url(self):
        catalog = [
            catalog_row(
                "the-killing",
                "The Killing",
                "",
                575,
            ),
            catalog_row(
                "the-killer",
                "The Killer",
                "https://www.criterion.com/films/27751-the-killing",
                8,
            ),
            catalog_row(
                "the-killers",
                "The Killers",
                "https://www.criterion.com/boxsets/176-the-killers",
                None,
                True,
            ),
            catalog_row(
                "the-killing-with-killers-kiss",
                "The Killing (with Killer’s Kiss)",
                "https://www.criterion.com/films/27751-the-killing",
                575,
            ),
        ]
        picks = [
            pick_row(
                guest_slug="kubrick",
                film_id="the-killer",
                film_title="The Killer",
                catalog_title="The Killer",
                catalog_spine=8,
                criterion_film_url="https://www.criterion.com/films/27751-the-killing",
            ),
            pick_row(
                guest_slug="siegel",
                film_id="the-killer",
                film_title="The Killer",
                catalog_title="The Killer",
                catalog_spine=8,
                criterion_film_url="https://www.criterion.com/films/725-the-killers",
            ),
        ]
        raw = [
            *picks,
            pick_row(
                guest_slug="kubrick",
                film_id="the-killing-with-killers-kiss",
                film_title="The Killing (with Killer’s Kiss)",
                catalog_title="The Killing (with Killer’s Kiss)",
                catalog_spine=575,
                criterion_film_url="https://www.criterion.com/films/27751-the-killing",
                pick_order=9,
            ),
        ]

        repaired_catalog, repaired_picks, repaired_raw, _ = resolve_duplicate_criterion_urls(
            catalog,
            picks,
            raw,
        )

        self.assertEqual(
            sorted(film["film_id"] for film in repaired_catalog),
            ["the-killers", "the-killing"],
        )
        self.assertEqual(repaired_picks[0]["film_id"], "the-killing")
        self.assertEqual(repaired_picks[0]["film_title"], "The Killing")
        self.assertEqual(repaired_picks[0]["catalog_spine"], 575)
        self.assertEqual(repaired_picks[1]["film_id"], "the-killers")
        self.assertEqual(repaired_picks[1]["film_title"], "The Killers")
        self.assertEqual([pick["film_id"] for pick in repaired_raw], ["the-killing", "the-killers"])

    def test_guest_pick_counts_are_recomputed_from_display_rule(self):
        guests = [{"slug": "guest", "name": "Guest", "pick_count": 2}]
        picks = [
            pick_row(
                guest_slug="guest",
                film_id="movie",
                source="criterion",
            )
        ]
        raw = [
            pick_row(
                guest_slug="guest",
                film_id="movie",
                source="criterion",
            )
        ]
        changes = Counter()

        repaired_guests = update_guest_pick_counts(guests, picks, raw, changes)

        self.assertEqual(repaired_guests[0]["pick_count"], 1)
        self.assertEqual(changes["data/guests.json:pick_count_updated"], 1)


if __name__ == "__main__":
    unittest.main()
