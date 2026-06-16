#!/usr/bin/env python3
"""Fixture tests for scripts.reconcile_pick_counterparts."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.audit_data_quality import pick_key
from scripts.reconcile_pick_counterparts import reconcile_pick_counterparts


def guest_row(**overrides):
    guest = {
        "slug": "guest",
        "name": "Guest",
        "pick_count": 0,
    }
    guest.update(overrides)
    return guest


def catalog_row(**overrides):
    row = {
        "film_id": "movie",
        "title": "Movie",
        "spine_number": 1,
        "criterion_url": "https://www.criterion.com/films/1-movie",
        "is_box_set": False,
    }
    row.update(overrides)
    return row


def pick_row(**overrides):
    pick = {
        "guest_slug": "guest",
        "guest_name": "Guest",
        "film_title": "Movie",
        "film_year": None,
        "film_id": "movie",
        "catalog_spine": 1,
        "catalog_title": "Movie",
        "match_method": "criterion_url",
        "letterboxd_url": "",
        "criterion_film_url": "https://www.criterion.com/films/1-movie",
        "source": "criterion",
        "quote": "keep this quote",
        "start_timestamp": 12,
        "youtube_timestamp_url": "https://www.youtube.com/watch?v=x&t=12",
        "extraction_confidence": "high",
        "visit_index": 1,
        "pick_order": 1,
    }
    pick.update(overrides)
    return pick


class ReconcilePickCounterpartsTest(unittest.TestCase):
    def test_missing_raw_visit_index_is_backfilled_from_unique_enriched_match(self):
        guests = [guest_row(pick_count=1)]
        catalog = [catalog_row()]
        picks = [pick_row(visit_index=2, pick_order=4)]
        raw = [
            pick_row(
                quote="",
                start_timestamp=None,
                youtube_timestamp_url="",
                extraction_confidence="none",
                visit_index=None,
                pick_order=4,
            )
        ]

        repaired_guests, repaired_picks, repaired_raw, report = reconcile_pick_counterparts(
            guests,
            catalog,
            picks,
            raw,
        )

        self.assertEqual(repaired_picks, picks)
        self.assertEqual(repaired_raw[0]["visit_index"], 2)
        self.assertEqual([pick_key(pick) for pick in repaired_raw], ["guest|2|movie"])
        self.assertEqual(repaired_guests[0]["pick_count"], 1)
        self.assertEqual(
            report["summary"]["changes"],
            {"data/picks_raw.json:visit_index_from_enriched_pick": 1},
        )
        self.assertEqual(
            report["summary"]["actions_by_type"],
            {"raw_visit_index_backfilled": 1},
        )
        self.assertEqual(report["summary"]["review_items"], 0)

    def test_missing_raw_visit_index_falls_back_to_unique_guest_film_match(self):
        guests = [guest_row(pick_count=1)]
        catalog = [catalog_row()]
        picks = [pick_row(visit_index=2, pick_order=4)]
        raw = [
            pick_row(
                quote="",
                start_timestamp=None,
                youtube_timestamp_url="",
                extraction_confidence="none",
                visit_index=None,
                pick_order=12,
            )
        ]

        _, repaired_picks, repaired_raw, report = reconcile_pick_counterparts(
            guests,
            catalog,
            picks,
            raw,
        )

        self.assertEqual(repaired_picks, picks)
        self.assertEqual(len(repaired_raw), 1)
        self.assertEqual(repaired_raw[0]["visit_index"], 2)
        self.assertEqual(repaired_raw[0]["pick_order"], 12)
        self.assertEqual(
            report["actions"][0]["evidence"]["match_basis"],
            "unique_guest_film",
        )

    def test_ambiguous_raw_visit_index_is_reported_and_not_auto_mutated(self):
        guests = [guest_row(pick_count=2)]
        catalog = [catalog_row()]
        picks = [
            pick_row(visit_index=1, pick_order=4),
            pick_row(visit_index=2, pick_order=4),
        ]
        raw = [
            pick_row(
                quote="",
                start_timestamp=None,
                youtube_timestamp_url="",
                extraction_confidence="none",
                visit_index=None,
                pick_order=4,
            )
        ]

        repaired_guests, repaired_picks, repaired_raw, report = reconcile_pick_counterparts(
            guests,
            catalog,
            picks,
            raw,
        )

        self.assertEqual(repaired_picks, picks)
        self.assertEqual(repaired_raw, raw)
        self.assertEqual(repaired_guests[0]["pick_count"], 2)
        self.assertEqual(report["summary"]["changes"], {})
        self.assertEqual(
            report["summary"]["review_by_type"],
            {"ambiguous_raw_visit_index": 1},
        )

    def test_stale_raw_order_collision_is_removed_when_enriched_raw_exists(self):
        guests = [guest_row(pick_count=2)]
        catalog = [
            catalog_row(film_id="before", title="Before", spine_number=10),
            catalog_row(film_id="brd", title="BRD", spine_number=11),
        ]
        picks = [
            pick_row(
                film_id="before",
                film_title="Before",
                catalog_title="Before",
                catalog_spine=10,
                pick_order=4,
            )
        ]
        raw = [
            pick_row(
                film_id="before",
                film_title="Before",
                catalog_title="Before",
                catalog_spine=10,
                quote="",
                extraction_confidence="none",
                pick_order=4,
            ),
            pick_row(
                film_id="brd",
                film_title="BRD",
                catalog_title="BRD",
                catalog_spine=11,
                quote="",
                extraction_confidence="none",
                pick_order=4,
            ),
        ]

        repaired_guests, repaired_picks, repaired_raw, report = reconcile_pick_counterparts(
            guests,
            catalog,
            picks,
            raw,
        )

        self.assertEqual([pick["film_id"] for pick in repaired_picks], ["before"])
        self.assertEqual([pick["film_id"] for pick in repaired_raw], ["before"])
        self.assertEqual(repaired_guests[0]["pick_count"], 1)
        self.assertEqual(
            report["summary"]["actions_by_type"],
            {"stale_raw_counterpart_removed": 1},
        )

    def test_stale_raw_order_collision_is_replaced_when_enriched_raw_is_missing(self):
        guests = [guest_row(pick_count=1)]
        catalog = [
            catalog_row(
                film_id="marseille",
                title="The Marseille Trilogy",
                criterion_url="https://www.criterion.com/boxsets/1264-the-marseille-trilogy",
                is_box_set=True,
            ),
            catalog_row(
                film_id="qatsi",
                title="The Qatsi Trilogy",
                criterion_url="https://www.criterion.com/boxsets/934-the-qatsi-trilogy",
                is_box_set=True,
            ),
        ]
        picks = [
            pick_row(
                film_id="marseille",
                film_title="The Marseille Trilogy",
                catalog_title="The Marseille Trilogy",
                criterion_film_url="https://www.criterion.com/boxsets/1264-the-marseille-trilogy",
                source="letterboxd",
                quote="preserve enriched quote",
                extraction_confidence="high",
                is_box_set=True,
                box_set_name="The Marseille Trilogy",
                box_set_film_count=3,
                box_set_criterion_url="https://www.criterion.com/boxsets/1264-the-marseille-trilogy",
                pick_order=10,
            )
        ]
        raw = [
            pick_row(
                film_id="qatsi",
                film_title="The Qatsi Trilogy",
                catalog_title="The Qatsi Trilogy",
                criterion_film_url="https://www.criterion.com/boxsets/1264-the-marseille-trilogy",
                source="criterion",
                quote="",
                extraction_confidence="none",
                is_box_set=True,
                box_set_name="The Marseille Trilogy",
                pick_order=10,
            )
        ]

        _, repaired_picks, repaired_raw, report = reconcile_pick_counterparts(
            guests,
            catalog,
            picks,
            raw,
        )

        self.assertEqual([pick["film_id"] for pick in repaired_picks], ["marseille"])
        self.assertEqual([pick["film_id"] for pick in repaired_raw], ["marseille"])
        self.assertEqual(repaired_raw[0]["quote"], "")
        self.assertIsNone(repaired_raw[0]["start_timestamp"])
        self.assertEqual(repaired_raw[0]["source"], "letterboxd")
        self.assertEqual(repaired_raw[0]["box_set_film_count"], 3)
        self.assertEqual(
            report["summary"]["actions_by_type"],
            {"stale_raw_counterpart_replaced": 1},
        )

    def test_raw_only_without_order_collision_is_promoted_and_keeps_aggregate_semantics(self):
        guests = [guest_row(pick_count=1)]
        catalog = [
            catalog_row(
                film_id="box",
                title="Box Set",
                criterion_url="https://www.criterion.com/boxsets/1-box-set",
                is_box_set=True,
                box_set_film_count=3,
            )
        ]
        picks = []
        raw = [
            pick_row(
                film_id="box",
                film_title="Box Set",
                catalog_title="Box Set",
                criterion_film_url="https://www.criterion.com/boxsets/1-box-set",
                source="criterion",
                quote="",
                start_timestamp=None,
                youtube_timestamp_url="",
                extraction_confidence="none",
                is_box_set=True,
                box_set_name="Box Set",
                pick_order=7,
            )
        ]

        repaired_guests, repaired_picks, repaired_raw, report = reconcile_pick_counterparts(
            guests,
            catalog,
            picks,
            raw,
        )

        self.assertEqual([pick_key(pick) for pick in repaired_raw], [pick_key(raw[0])])
        self.assertEqual(len(repaired_picks), 1)
        self.assertEqual(repaired_picks[0]["film_id"], "box")
        self.assertEqual(repaired_picks[0]["quote"], "")
        self.assertIsNone(repaired_picks[0]["start_timestamp"])
        self.assertTrue(repaired_picks[0]["is_box_set"])
        self.assertEqual(repaired_picks[0]["box_set_film_count"], 3)
        self.assertEqual(repaired_picks[0]["box_set_criterion_url"], raw[0]["criterion_film_url"])
        self.assertEqual(repaired_guests[0]["pick_count"], 1)
        self.assertEqual(
            report["summary"]["actions_by_type"],
            {"raw_counterpart_promoted": 1},
        )

    def test_duplicate_keys_are_reported_and_not_auto_mutated(self):
        guests = [guest_row()]
        catalog = [catalog_row()]
        picks = []
        raw = [
            pick_row(film_id="movie", pick_order=1, criterion_film_url="one"),
            pick_row(film_id="movie", pick_order=2, criterion_film_url="two"),
        ]

        _, repaired_picks, repaired_raw, report = reconcile_pick_counterparts(
            guests,
            catalog,
            picks,
            raw,
        )

        self.assertEqual(repaired_picks, [])
        self.assertEqual(repaired_raw, raw)
        self.assertEqual(
            report["summary"]["review_by_type"],
            {"ambiguous_duplicate_pick_key": 1},
        )


if __name__ == "__main__":
    unittest.main()
