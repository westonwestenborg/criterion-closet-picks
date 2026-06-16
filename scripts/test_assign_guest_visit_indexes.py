#!/usr/bin/env python3
"""Fixture tests for scripts.assign_guest_visit_indexes."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.assign_guest_visit_indexes import assign_guest_visit_indexes


def guest_row(**overrides):
    row = {
        "slug": "guest",
        "name": "Guest",
        "youtube_video_id": None,
        "vimeo_video_id": None,
        "visits": [],
    }
    row.update(overrides)
    return row


def visit(**overrides):
    row = {
        "visit_index": None,
        "youtube_video_id": "video",
        "vimeo_video_id": None,
        "episode_date": "2026-01-01",
        "criterion_page_url": "https://www.criterion.com/shop/collection/1-guest",
    }
    row.update(overrides)
    return row


class AssignGuestVisitIndexesTest(unittest.TestCase):
    def test_assigns_indexes_in_existing_file_order_when_all_are_missing(self):
        guests = [
            guest_row(
                visits=[
                    visit(youtube_video_id="old", episode_date="2015-01-01"),
                    visit(youtube_video_id="new", episode_date="2025-01-01"),
                ]
            )
        ]

        repaired, report = assign_guest_visit_indexes(guests)

        self.assertEqual(
            [item["visit_index"] for item in repaired[0]["visits"]],
            [1, 2],
        )
        self.assertEqual(report["summary"]["total_changes"], 2)
        self.assertEqual(report["summary"]["actions"], 1)
        self.assertEqual(report["summary"]["review_items"], 0)

    def test_partial_missing_indexes_are_assigned_when_gap_is_unambiguous(self):
        guests = [
            guest_row(
                visits=[
                    visit(visit_index=1, youtube_video_id="old"),
                    visit(visit_index=None, youtube_video_id="new"),
                ]
            )
        ]

        repaired, report = assign_guest_visit_indexes(guests)

        self.assertEqual([item.get("visit_index") for item in repaired[0]["visits"]], [1, 2])
        self.assertEqual(report["summary"]["total_changes"], 1)
        self.assertEqual(report["summary"]["actions"], 1)
        self.assertEqual(report["summary"]["review_items"], 0)

    def test_ambiguous_partial_missing_indexes_are_reported_not_mutated(self):
        guests = [
            guest_row(
                visits=[
                    visit(visit_index=2, youtube_video_id="old"),
                    visit(visit_index=2, youtube_video_id="middle"),
                    visit(visit_index=None, youtube_video_id="new"),
                ]
            )
        ]

        repaired, report = assign_guest_visit_indexes(guests)

        self.assertEqual(
            [item.get("visit_index") for item in repaired[0]["visits"]],
            [2, 2, None],
        )
        self.assertEqual(report["summary"]["total_changes"], 0)
        self.assertEqual(report["summary"]["review_by_type"], {"ambiguous_missing_visit_index": 1})

    def test_video_less_guests_are_skipped(self):
        guests = [
            guest_row(
                youtube_video_id=None,
                vimeo_video_id=None,
                visits=[visit(youtube_video_id=None, vimeo_video_id=None)],
            )
        ]

        repaired, report = assign_guest_visit_indexes(guests)

        self.assertIsNone(repaired[0]["visits"][0]["visit_index"])
        self.assertEqual(report["summary"]["total_changes"], 0)


if __name__ == "__main__":
    unittest.main()
