#!/usr/bin/env python3
"""Fixture tests for scripts.backfill_verified_vimeo_ids."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.backfill_verified_vimeo_ids import backfill_verified_vimeo_ids


class BackfillVerifiedVimeoIdsTest(unittest.TestCase):
    def test_backfills_missing_vimeo_id(self):
        guests = [{"slug": "guest", "name": "Guest", "vimeo_video_id": None}]
        verified = {
            "guest": {
                "vimeo_video_id": "123",
                "source_url": "https://www.criterion.com/shop/collection/1-guest",
            }
        }

        repaired, report = backfill_verified_vimeo_ids(guests, verified)

        self.assertEqual(repaired[0]["vimeo_video_id"], "123")
        self.assertEqual(report["summary"]["total_changes"], 1)
        self.assertEqual(report["summary"]["review_items"], 0)

    def test_existing_conflicting_vimeo_id_is_reported_not_overwritten(self):
        guests = [{"slug": "guest", "name": "Guest", "vimeo_video_id": "old"}]
        verified = {
            "guest": {
                "vimeo_video_id": "new",
                "source_url": "https://www.criterion.com/shop/collection/1-guest",
            }
        }

        repaired, report = backfill_verified_vimeo_ids(guests, verified)

        self.assertEqual(repaired[0]["vimeo_video_id"], "old")
        self.assertEqual(report["summary"]["total_changes"], 0)
        self.assertEqual(report["summary"]["review_by_type"], {"verified_vimeo_conflict": 1})

    def test_missing_guest_is_reported(self):
        repaired, report = backfill_verified_vimeo_ids(
            [],
            {
                "missing": {
                    "vimeo_video_id": "123",
                    "source_url": "https://www.criterion.com/shop/collection/1-missing",
                }
            },
        )

        self.assertEqual(repaired, [])
        self.assertEqual(report["summary"]["total_changes"], 0)
        self.assertEqual(report["summary"]["review_by_type"], {"verified_vimeo_guest_missing": 1})


if __name__ == "__main__":
    unittest.main()
