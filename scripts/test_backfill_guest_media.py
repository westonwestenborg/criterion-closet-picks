#!/usr/bin/env python3
"""Fixture tests for scripts.backfill_guest_media."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.backfill_guest_media import backfill_guest_media


class BackfillGuestMediaTest(unittest.TestCase):
    def test_copies_episode_date_from_single_visit(self):
        repaired, report = backfill_guest_media(
            [
                {
                    "slug": "guest",
                    "name": "Guest",
                    "episode_date": None,
                    "visits": [{"visit_index": 1, "episode_date": "2026-01-01"}],
                }
            ],
            vimeo_dates={},
            professions={},
            photo_urls={},
        )

        self.assertEqual(repaired[0]["episode_date"], "2026-01-01")
        self.assertEqual(report["summary"]["changes"], {"data/guests.json:episode_date_copied_from_single_visit": 1})

    def test_backfills_verified_vimeo_episode_date(self):
        repaired, report = backfill_guest_media(
            [{"slug": "guest", "name": "Guest", "vimeo_video_id": "123", "episode_date": None}],
            vimeo_dates={
                "guest": {
                    "episode_date": "2026-01-02",
                    "vimeo_video_id": "123",
                    "source": "fixture",
                }
            },
            professions={},
            photo_urls={},
        )

        self.assertEqual(repaired[0]["episode_date"], "2026-01-02")
        self.assertEqual(report["summary"]["review_items"], 0)

    def test_conflicting_verified_vimeo_episode_date_is_reported(self):
        repaired, report = backfill_guest_media(
            [{"slug": "guest", "name": "Guest", "vimeo_video_id": "123", "episode_date": "2026-01-01"}],
            vimeo_dates={
                "guest": {
                    "episode_date": "2026-01-02",
                    "vimeo_video_id": "123",
                    "source": "fixture",
                }
            },
            professions={},
            photo_urls={},
        )

        self.assertEqual(repaired[0]["episode_date"], "2026-01-01")
        self.assertEqual(report["summary"]["review_by_type"], {"verified_vimeo_date_conflict": 1})

    def test_conflicting_vimeo_id_is_reported(self):
        repaired, report = backfill_guest_media(
            [{"slug": "guest", "name": "Guest", "vimeo_video_id": "old", "episode_date": None}],
            vimeo_dates={
                "guest": {
                    "episode_date": "2026-01-02",
                    "vimeo_video_id": "new",
                    "source": "fixture",
                }
            },
            professions={},
            photo_urls={},
        )

        self.assertIsNone(repaired[0]["episode_date"])
        self.assertEqual(report["summary"]["review_by_type"], {"verified_vimeo_date_video_conflict": 1})

    def test_backfills_missing_profession_without_overwriting_existing(self):
        repaired, report = backfill_guest_media(
            [
                {"slug": "missing", "name": "Missing", "profession": None},
                {"slug": "existing", "name": "Existing", "profession": "actor"},
            ],
            vimeo_dates={},
            professions={"missing": "director", "existing": "director"},
            photo_urls={},
        )

        by_slug = {guest["slug"]: guest for guest in repaired}
        self.assertEqual(by_slug["missing"]["profession"], "director")
        self.assertEqual(by_slug["existing"]["profession"], "actor")
        self.assertEqual(report["summary"]["review_by_type"], {"verified_profession_conflict": 1})

    def test_backfills_missing_photo_url_without_overwriting_existing(self):
        repaired, report = backfill_guest_media(
            [
                {"slug": "missing", "name": "Missing", "photo_url": None},
                {"slug": "existing", "name": "Existing", "photo_url": "https://example.com/old.jpg"},
            ],
            vimeo_dates={},
            professions={},
            photo_urls={
                "missing": {
                    "photo_url": "https://example.com/new.jpg",
                    "source_url": "https://example.com/profile",
                },
                "existing": {
                    "photo_url": "https://example.com/newer.jpg",
                    "source_url": "https://example.com/profile",
                },
            },
        )

        by_slug = {guest["slug"]: guest for guest in repaired}
        self.assertEqual(by_slug["missing"]["photo_url"], "https://example.com/new.jpg")
        self.assertEqual(by_slug["existing"]["photo_url"], "https://example.com/old.jpg")
        self.assertEqual(report["summary"]["changes"], {"data/guests.json:photo_url_backfilled_from_verified_map": 1})
        self.assertEqual(report["summary"]["review_by_type"], {"verified_photo_conflict": 1})


if __name__ == "__main__":
    unittest.main()
