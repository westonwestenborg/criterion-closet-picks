#!/usr/bin/env python3
"""Fixture tests for scripts.repair_data_quality."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.repair_data_quality import repair_data


def base_pick(**overrides):
    pick = {
        "guest_slug": "guest",
        "guest_name": "Guest",
        "film_title": "Movie",
        "film_year": None,
        "film_id": "movie",
        "catalog_spine": None,
        "catalog_title": None,
        "match_method": None,
        "criterion_film_url": "",
        "source": "criterion",
        "visit_index": 1,
        "quote": "keep this quote",
        "start_timestamp": 12,
        "youtube_timestamp_url": "https://example.com?t=12",
        "extraction_confidence": "high",
    }
    pick.update(overrides)
    return pick


class RepairDataQualityTest(unittest.TestCase):
    def test_regular_catalog_spine_is_backfilled_from_local_criterion_id_candidate(self):
        catalog = [
            {
                "film_id": "movie",
                "title": "Movie",
                "criterion_url": "https://www.criterion.com/films/1-movie",
                "spine_number": None,
            }
        ]
        picks = [
            base_pick(
                criterion_film_url="https://www.criterion.com/films/1-movie",
                catalog_spine=None,
            )
        ]
        raw = [
            base_pick(
                criterion_film_url="https://www.criterion.com/films/1-movie",
                catalog_spine=101,
                catalog_title="Movie",
            )
        ]

        repaired_catalog, repaired_picks, repaired_raw, report = repair_data(catalog, picks, raw)

        self.assertEqual(repaired_catalog[0]["spine_number"], 101)
        self.assertEqual(repaired_picks[0]["catalog_spine"], 101)
        self.assertEqual(repaired_raw[0]["catalog_spine"], 101)
        self.assertEqual(report["summary"]["review_items"], 0)

    def test_box_set_without_local_spine_is_not_guessed(self):
        catalog = [
            {
                "film_id": "box",
                "title": "Box",
                "criterion_url": "https://www.criterion.com/boxsets/2-box",
                "spine_number": None,
                "is_box_set": True,
            }
        ]
        picks = [
            base_pick(
                film_id="box",
                film_title="Box",
                criterion_film_url="https://www.criterion.com/boxsets/2-box",
            )
        ]

        repaired_catalog, repaired_picks, _, report = repair_data(catalog, picks, picks)

        self.assertIsNone(repaired_catalog[0]["spine_number"])
        self.assertIsNone(repaired_picks[0]["catalog_spine"])
        self.assertEqual(report["summary"]["review_items"], 0)

    def test_box_set_catalog_spine_is_not_backfilled_from_pick_candidate(self):
        catalog = [
            {
                "film_id": "box",
                "title": "Box",
                "criterion_url": "https://www.criterion.com/boxsets/2-box",
                "spine_number": None,
                "is_box_set": True,
            }
        ]
        picks = [
            base_pick(
                film_id="box",
                film_title="Box",
                criterion_film_url="https://www.criterion.com/boxsets/2-box",
                catalog_spine=101,
            )
        ]

        repaired_catalog, _, _, report = repair_data(catalog, picks, picks)

        self.assertIsNone(repaired_catalog[0]["spine_number"])
        self.assertEqual(report["summary"]["review_by_type"], {"pick_spine_without_catalog_spine": 2})

    def test_raw_enriched_reconciliation_preserves_quote_fields(self):
        catalog = [
            {
                "film_id": "movie",
                "title": "Movie",
                "criterion_url": "",
                "spine_number": 101,
            }
        ]
        picks = [
            base_pick(
                criterion_film_url="",
                catalog_title=None,
                quote="preserve me",
                start_timestamp=44,
            )
        ]
        raw = [
            base_pick(
                criterion_film_url="https://www.criterion.com/films/1-movie",
                catalog_title="Movie",
                catalog_spine=101,
                quote="",
                start_timestamp=None,
            )
        ]

        _, repaired_picks, _, _ = repair_data(catalog, picks, raw)

        self.assertEqual(
            repaired_picks[0]["criterion_film_url"],
            "https://www.criterion.com/films/1-movie",
        )
        self.assertEqual(repaired_picks[0]["catalog_title"], "Movie")
        self.assertEqual(repaired_picks[0]["quote"], "preserve me")
        self.assertEqual(repaired_picks[0]["start_timestamp"], 44)

    def test_duplicate_raw_key_is_reported_and_not_used_for_reconciliation(self):
        catalog = [
            {
                "film_id": "movie",
                "title": "Movie",
                "criterion_url": "",
                "spine_number": 101,
            }
        ]
        picks = [base_pick(criterion_film_url="")]
        raw = [
            base_pick(criterion_film_url="https://www.criterion.com/films/1-movie"),
            base_pick(criterion_film_url="https://www.criterion.com/films/2-other"),
        ]

        _, repaired_picks, _, report = repair_data(catalog, picks, raw)

        self.assertEqual(repaired_picks[0]["criterion_film_url"], "")
        self.assertIn("ambiguous_duplicate_pick_key", report["summary"]["review_by_type"])

    def test_unique_blank_raw_criterion_url_clears_enriched_value(self):
        catalog = [
            {
                "film_id": "movie",
                "title": "Movie",
                "criterion_url": "",
                "spine_number": 101,
            }
        ]
        picks = [base_pick(criterion_film_url="https://www.criterion.com/films/1-movie")]
        raw = [base_pick(criterion_film_url="")]

        _, repaired_picks, _, report = repair_data(catalog, picks, raw)

        self.assertEqual(repaired_picks[0]["criterion_film_url"], "")
        self.assertNotIn("raw_enriched_pick_mismatch", report["summary"]["review_by_type"])

    def test_match_method_and_pick_order_are_backfilled_deterministically(self):
        catalog = [
            {
                "film_id": "movie",
                "title": "Movie",
                "criterion_url": "https://www.criterion.com/films/1-movie",
                "spine_number": 101,
            },
            {
                "film_id": "other",
                "title": "Other",
                "criterion_url": "",
                "spine_number": 102,
            },
        ]
        picks = [
            base_pick(
                film_id="movie",
                film_title="Movie",
                criterion_film_url="https://www.criterion.com/films/1-movie",
            ),
            base_pick(
                film_id="other",
                film_title="Other",
                criterion_film_url="",
            ),
        ]

        _, repaired_picks, _, _ = repair_data(catalog, picks, picks)

        self.assertEqual(repaired_picks[0]["match_method"], "criterion_url")
        self.assertEqual(repaired_picks[1]["match_method"], "exact")
        self.assertEqual([p["pick_order"] for p in repaired_picks], [1, 2])

    def test_box_set_member_titles_do_not_list_the_box_set_itself(self):
        catalog = [
            {
                "film_id": "box",
                "title": "Example Box",
                "criterion_url": "https://www.criterion.com/boxsets/1-example-box",
                "is_box_set": True,
            }
        ]
        picks = [
            base_pick(
                film_id="box",
                film_title="Example Box",
                criterion_film_url="https://www.criterion.com/boxsets/1-example-box",
                is_box_set=True,
                box_set_name="Example Box",
                box_set_film_count=3,
                box_set_film_titles=["Example Box"],
            )
        ]

        _, repaired_picks, _, report = repair_data(catalog, picks, [])

        self.assertNotIn("box_set_film_titles", repaired_picks[0])
        self.assertEqual(repaired_picks[0]["box_set_film_count"], 3)
        self.assertEqual(
            report["summary"]["changes"]["data/picks.json:box_set_self_titles_removed"],
            1,
        )

    def test_world_cinema_project_count_is_repaired_from_local_catalog_members(self):
        catalog = [
            {
                "film_id": "wcp-2",
                "title": "Martin Scorsese's World Cinema Project No. 2",
                "criterion_url": "https://www.criterion.com/boxsets/1258-martin-scorsese-s-world-cinema-project-no-2",
                "is_box_set": True,
            },
            {"film_id": "insiang", "title": "Insiang (World Cinema Project No. 2)"},
            {
                "film_id": "mysterious-object",
                "title": "Mysterious Object at Noon (World Cinema Project No. 2)",
            },
        ]
        picks = [
            base_pick(
                film_id="wcp-2",
                film_title="Martin Scorsese's World Cinema Project No. 2",
                criterion_film_url="https://www.criterion.com/boxsets/1258-martin-scorsese-s-world-cinema-project-no-2",
                is_box_set=True,
                box_set_name="Martin Scorsese's World Cinema Project No. 2",
                box_set_film_count=1,
                box_set_film_titles=["Martin Scorsese’s World Cinema Project No. 2"],
            )
        ]

        _, repaired_picks, _, report = repair_data(catalog, picks, [])

        self.assertNotIn("box_set_film_titles", repaired_picks[0])
        self.assertEqual(repaired_picks[0]["box_set_film_count"], 2)
        self.assertEqual(
            report["summary"]["changes"]["data/picks.json:box_set_count_from_catalog_members"],
            1,
        )

    def test_world_cinema_project_count_is_repaired_without_member_titles(self):
        catalog = [
            {
                "film_id": "wcp-4",
                "title": "Martin Scorsese's World Cinema Project No. 4",
                "criterion_url": "https://www.criterion.com/boxsets/6183-martin-scorsese-s-world-cinema-project-no-4",
                "is_box_set": True,
            },
            {"film_id": "sambizanga", "title": "Sambizanga (World Cinema Project No. 4)"},
            {"film_id": "kalpana", "title": "Kalpana (World Cinema Project No. 4)"},
        ]
        picks = [
            base_pick(
                film_id="wcp-4",
                film_title="Martin Scorsese's World Cinema Project No. 4",
                criterion_film_url="https://www.criterion.com/boxsets/6183-martin-scorsese-s-world-cinema-project-no-4",
                is_box_set=True,
                box_set_name="Martin Scorsese's World Cinema Project No. 4",
                box_set_film_count=1,
            )
        ]

        _, repaired_picks, _, _ = repair_data(catalog, picks, [])

        self.assertEqual(repaired_picks[0]["box_set_film_count"], 2)

    def test_unresolved_regular_catalog_spine_goes_to_review(self):
        catalog = [
            {
                "film_id": "movie",
                "title": "Movie",
                "criterion_url": "https://www.criterion.com/films/1-movie",
                "spine_number": None,
            }
        ]
        picks = [base_pick(criterion_film_url="https://www.criterion.com/films/1-movie")]

        repaired_catalog, _, _, report = repair_data(catalog, picks, picks)

        self.assertIsNone(repaired_catalog[0]["spine_number"])
        self.assertEqual(
            report["summary"]["review_by_type"],
            {"unresolved_regular_catalog_spine": 1},
        )

    def test_accepted_no_spine_regular_catalog_entry_is_not_reported_for_review(self):
        catalog = [
            {
                "film_id": "movie",
                "title": "Movie",
                "criterion_url": "https://www.criterion.com/films/1-movie",
                "spine_number": None,
            }
        ]
        picks = [base_pick(criterion_film_url="https://www.criterion.com/films/1-movie")]

        repaired_catalog, _, _, report = repair_data(
            catalog,
            picks,
            picks,
            accepted_regular_no_spine_ids={"movie"},
        )

        self.assertIsNone(repaired_catalog[0]["spine_number"])
        self.assertEqual(report["summary"]["review_items"], 0)


if __name__ == "__main__":
    unittest.main()
