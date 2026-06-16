#!/usr/bin/env python3
"""Fixture tests for scripts.audit_data_quality."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.audit_data_quality import audit_data, transcript_ids_from_disk


def issue_types(report):
    return {issue["type"] for issue in report["issues"]}


class AuditDataQualityTest(unittest.TestCase):
    def test_regular_film_missing_spine_is_flagged_separately_from_box_set(self):
        catalog = [
            {
                "film_id": "in-the-mood-for-love",
                "title": "In the Mood for Love",
                "criterion_url": "https://www.criterion.com/films/198-in-the-mood-for-love",
                "spine_number": None,
                "tmdb_id": 843,
                "poster_url": "poster.jpg",
                "year": 2000,
                "director": "Wong Kar-Wai",
                "genres": ["Drama"],
            },
            {
                "film_id": "wong-kar-wai-box",
                "title": "World of Wong Kar Wai",
                "criterion_url": "https://www.criterion.com/boxsets/4117-world-of-wong-kar-wai",
                "spine_number": None,
                "is_box_set": True,
            },
        ]
        picks = [
            {
                "guest_slug": "guest",
                "film_id": "in-the-mood-for-love",
                "film_title": "In the Mood for Love",
                "catalog_spine": None,
                "criterion_film_url": "https://www.criterion.com/films/198-in-the-mood-for-love",
                "source": "criterion",
                "match_method": "criterion_url",
                "visit_index": 1,
                "pick_order": 1,
            },
            {
                "guest_slug": "guest",
                "film_id": "wong-kar-wai-box",
                "film_title": "World of Wong Kar Wai",
                "catalog_spine": None,
                "criterion_film_url": "https://www.criterion.com/boxsets/4117-world-of-wong-kar-wai",
                "source": "criterion",
                "match_method": "criterion_url",
                "visit_index": 1,
                "pick_order": 2,
            },
        ]
        guests = [
            {
                "slug": "guest",
                "name": "Guest",
                "youtube_video_id": "abc123",
                "photo_url": "photo.jpg",
                "profession": "director",
                "episode_date": "2026-01-01",
            }
        ]

        report = audit_data(catalog, guests, picks, picks, transcript_ids={"abc123"})
        types = issue_types(report)

        self.assertIn("catalog_regular_film_missing_spine", types)
        self.assertIn("pick_regular_film_missing_spine", types)
        self.assertIn("catalog_box_set_missing_spine", types)
        self.assertIn("pick_box_set_missing_spine", types)

        regular_missing = [
            issue for issue in report["issues"]
            if issue["type"] == "pick_regular_film_missing_spine"
            and issue["file"] == "data/picks.json"
        ]
        self.assertEqual(len(regular_missing), 1)
        self.assertEqual(regular_missing[0]["record_key"], "guest|1|in-the-mood-for-love")

    def test_core_identity_and_pick_consistency_issues_are_reported(self):
        catalog = [
            {
                "film_id": "wrong-spine",
                "title": "Wrong Spine",
                "criterion_url": "https://www.criterion.com/films/1-wrong-spine",
                "spine_number": 10,
                "tmdb_id": 100,
                "poster_url": "poster.jpg",
                "imdb_id": "tt1",
                "year": 2000,
                "director": "Director",
                "genres": ["Drama"],
            },
            {
                "film_id": "duplicate-url-a",
                "title": "Duplicate URL A",
                "criterion_url": "https://www.criterion.com/films/2-duplicate",
                "spine_number": 20,
                "tmdb_id": 200,
                "poster_url": "poster.jpg",
                "imdb_id": "tt2",
                "year": 2001,
                "director": "Director",
                "genres": ["Drama"],
            },
            {
                "film_id": "duplicate-url-b",
                "title": "Duplicate URL B",
                "criterion_url": "https://www.criterion.com/films/2-duplicate",
                "spine_number": 21,
                "tmdb_id": 200,
                "poster_url": "poster.jpg",
                "imdb_id": "tt3",
                "year": 2002,
                "director": "Director",
                "genres": ["Drama"],
            },
        ]
        picks = [
            {
                "guest_slug": "guest",
                "film_id": "wrong-spine",
                "film_title": "Wrong Spine",
                "catalog_spine": 99,
                "criterion_film_url": "https://www.criterion.com/films/1-wrong-spine",
                "source": "criterion",
                "match_method": "criterion_url",
                "visit_index": 1,
                "pick_order": 1,
            },
            {
                "guest_slug": "guest",
                "film_id": "missing-catalog",
                "film_title": "Missing Catalog",
                "catalog_spine": None,
                "criterion_film_url": "https://www.criterion.com/films/999-missing",
                "source": "criterion",
                "match_method": "criterion_url",
                "visit_index": 1,
                "pick_order": 2,
            },
            {
                "guest_slug": "guest",
                "film_id": "wrong-spine",
                "film_title": "Wrong Spine",
                "catalog_spine": 99,
                "criterion_film_url": "https://www.criterion.com/films/1-wrong-spine",
                "source": "criterion",
                "match_method": "criterion_url",
                "visit_index": 1,
                "pick_order": 1,
            },
        ]
        raw = [
            {
                "guest_slug": "guest",
                "film_id": "wrong-spine",
                "film_title": "Wrong Spine",
                "catalog_spine": 10,
                "catalog_title": "Wrong Spine",
                "criterion_film_url": "https://www.criterion.com/films/1-wrong-spine",
                "source": "criterion",
                "match_method": "criterion_url",
                "visit_index": 1,
                "pick_order": 1,
            }
        ]
        guests = [
            {
                "slug": "guest",
                "name": "Guest",
                "youtube_video_id": "abc123",
                "photo_url": "photo.jpg",
                "profession": "director",
                "episode_date": "2026-01-01",
                "visit_count": 2,
                "visits": [{"visit_index": 1, "youtube_video_id": "abc123"}],
            }
        ]

        report = audit_data(catalog, guests, picks, raw, transcript_ids={"abc123"})
        types = issue_types(report)

        self.assertIn("pick_catalog_spine_mismatch", types)
        self.assertIn("duplicate_criterion_url", types)
        self.assertIn("duplicate_tmdb_id", types)
        self.assertIn("pick_missing_catalog_entry", types)
        self.assertIn("raw_enriched_pick_mismatch", types)
        self.assertIn("duplicate_guest_visit_film_pick", types)
        self.assertIn("guest_visit_count_mismatch", types)

    def test_issue_order_is_deterministic_and_exception_status_is_applied(self):
        catalog = [
            {"film_id": "b", "title": "B", "criterion_url": "", "spine_number": None},
            {"film_id": "a", "title": "A", "criterion_url": "", "spine_number": None},
        ]
        picks = [
            {
                "guest_slug": "guest",
                "film_id": "b",
                "film_title": "B",
                "catalog_spine": None,
                "source": "",
                "match_method": "",
                "visit_index": None,
                "pick_order": None,
            },
            {
                "guest_slug": "guest",
                "film_id": "a",
                "film_title": "A",
                "catalog_spine": None,
                "source": "",
                "match_method": "",
                "visit_index": None,
                "pick_order": None,
            },
        ]
        guests = [{"slug": "guest", "name": "Guest"}]

        first = audit_data(catalog, guests, picks, picks, transcript_ids=set())
        second = audit_data(list(reversed(catalog)), guests, list(reversed(picks)), list(reversed(picks)), transcript_ids=set())
        self.assertEqual(
            [issue["id"] for issue in first["issues"]],
            [issue["id"] for issue in second["issues"]],
        )

        accepted_id = first["issues"][0]["id"]
        with_exception = audit_data(
            catalog,
            guests,
            picks,
            picks,
            transcript_ids=set(),
            exceptions={accepted_id: "Known fixture exception"},
        )
        accepted = [issue for issue in with_exception["issues"] if issue["id"] == accepted_id]
        self.assertEqual(accepted[0]["status"], "accepted")
        self.assertEqual(accepted[0]["exception_note"], "Known fixture exception")

        type_exception = audit_data(
            catalog,
            guests,
            picks,
            picks,
            transcript_ids=set(),
            exceptions={"type:pick_missing_match_method": "Known fixture issue type"},
        )
        accepted_types = [
            issue for issue in type_exception["issues"]
            if issue["type"] == "pick_missing_match_method"
        ]
        self.assertTrue(accepted_types)
        self.assertTrue(all(issue["status"] == "accepted" for issue in accepted_types))

        record_exception = audit_data(
            catalog,
            guests,
            picks,
            picks,
            transcript_ids=set(),
            exceptions={"record:pick_missing_match_method:guest||a": "Known fixture record"},
        )
        accepted_record = [
            issue for issue in record_exception["issues"]
            if issue["type"] == "pick_missing_match_method"
            and issue["record_key"] == "guest||a"
        ]
        self.assertEqual(accepted_record[0]["status"], "accepted")
        self.assertEqual(accepted_record[0]["exception_note"], "Known fixture record")

    def test_film_scoped_exception_accepts_catalog_and_pick_spine_gaps(self):
        catalog = [
            {
                "film_id": "no-public-spine",
                "title": "No Public Spine",
                "criterion_url": "https://www.criterion.com/films/1-no-public-spine",
                "spine_number": None,
                "tmdb_id": 1,
                "poster_url": "poster.jpg",
                "imdb_id": "tt1",
                "year": 2026,
                "director": "Director",
                "genres": ["Drama"],
            }
        ]
        picks = [
            {
                "guest_slug": "guest",
                "film_id": "no-public-spine",
                "film_title": "No Public Spine",
                "catalog_spine": None,
                "catalog_title": "No Public Spine",
                "criterion_film_url": "https://www.criterion.com/films/1-no-public-spine",
                "source": "criterion",
                "match_method": "criterion_url",
                "visit_index": 1,
                "pick_order": 1,
            }
        ]
        guests = [
            {
                "slug": "guest",
                "name": "Guest",
                "youtube_video_id": "abc123",
                "photo_url": "photo.jpg",
                "profession": "director",
                "episode_date": "2026-01-01",
            }
        ]

        report = audit_data(
            catalog,
            guests,
            picks,
            picks,
            transcript_ids={"abc123"},
            exceptions={
                "film:catalog_regular_film_missing_spine:no-public-spine": "Criterion page has no visible public spine.",
                "film:pick_regular_film_missing_spine:no-public-spine": "Criterion page has no visible public spine.",
            },
        )
        spine_issues = [
            issue
            for issue in report["issues"]
            if issue["type"]
            in {
                "catalog_regular_film_missing_spine",
                "pick_regular_film_missing_spine",
            }
        ]

        self.assertEqual(len(spine_issues), 3)
        self.assertTrue(all(issue["status"] == "accepted" for issue in spine_issues))

    def test_video_less_guests_are_not_guest_media_remediation_targets(self):
        catalog: list[dict] = []
        picks: list[dict] = []
        guests = [
            {
                "slug": "collection-only",
                "name": "Collection Only",
                "criterion_page_url": "https://www.criterion.com/shop/collection/1-collection-only",
                "youtube_video_id": None,
                "vimeo_video_id": None,
                "photo_url": None,
                "profession": "",
                "episode_date": None,
            },
            {
                "slug": "video-backed",
                "name": "Video Backed",
                "youtube_video_id": "abc123",
                "photo_url": None,
                "profession": "",
                "episode_date": None,
            },
        ]

        report = audit_data(catalog, guests, picks, picks, transcript_ids={"abc123"})
        guest_issues = [
            issue
            for issue in report["issues"]
            if issue["category"] == "guest_media"
        ]

        self.assertFalse(
            any(issue["record_key"] == "collection-only" for issue in guest_issues)
        )
        self.assertIn(
            "guest_missing_photo",
            {issue["type"] for issue in guest_issues if issue["record_key"] == "video-backed"},
        )

    def test_local_photo_override_satisfies_guest_photo_coverage(self):
        report = audit_data(
            [],
            [
                {
                    "slug": "local-photo",
                    "name": "Local Photo",
                    "youtube_video_id": "abc123",
                    "photo_url": None,
                    "profession": "director",
                    "episode_date": "2026-01-01",
                }
            ],
            [],
            [],
            transcript_ids={"abc123"},
            local_photo_slugs={"local-photo"},
        )

        self.assertNotIn("guest_missing_photo", issue_types(report))

    def test_vimeo_guests_are_transcript_remediation_targets(self):
        picks = [
            {
                "guest_slug": "vimeo-backed",
                "film_id": "film-a",
                "film_title": "Film A",
                "visit_index": 1,
                "quote": "",
            }
        ]
        report = audit_data(
            [],
            [
                {
                    "slug": "vimeo-backed",
                    "name": "Vimeo Backed",
                    "vimeo_video_id": "123456",
                    "photo_url": "photo.jpg",
                    "profession": "director",
                    "episode_date": "2026-01-01",
                }
            ],
            picks,
            picks,
            transcript_ids=set(),
        )

        self.assertIn("guest_missing_transcript", issue_types(report))
        issue = next(
            item for item in report["issues"] if item["type"] == "guest_missing_transcript"
        )
        self.assertEqual(issue["record_key"], "vimeo-backed|vimeo:123456")

    def test_missing_transcript_is_not_flagged_when_quotes_are_complete(self):
        picks = [
            {
                "guest_slug": "quoted-video",
                "film_id": "film-a",
                "film_title": "Film A",
                "visit_index": 1,
                "quote": "A local quote.",
            },
            {
                "guest_slug": "quoted-video",
                "film_id": "film-b",
                "film_title": "Film B",
                "visit_index": 1,
                "quote": "Another local quote.",
            },
        ]

        report = audit_data(
            [],
            [
                {
                    "slug": "quoted-video",
                    "name": "Quoted Video",
                    "youtube_video_id": "abc123",
                    "photo_url": "photo.jpg",
                    "profession": "director",
                    "episode_date": "2026-01-01",
                }
            ],
            picks,
            picks,
            transcript_ids=set(),
        )

        self.assertNotIn("guest_missing_transcript", issue_types(report))

    def test_incomplete_quote_coverage_is_flagged_when_some_quotes_exist(self):
        picks = [
            {
                "guest_slug": "partial-video",
                "film_id": "film-a",
                "film_title": "Film A",
                "visit_index": 1,
                "quote": "A local quote.",
            },
            {
                "guest_slug": "partial-video",
                "film_id": "film-b",
                "film_title": "Film B",
                "visit_index": 1,
                "quote": "",
            },
        ]

        report = audit_data(
            [],
            [
                {
                    "slug": "partial-video",
                    "name": "Partial Video",
                    "youtube_video_id": "abc123",
                    "photo_url": "photo.jpg",
                    "profession": "director",
                    "episode_date": "2026-01-01",
                }
            ],
            picks,
            picks,
            transcript_ids=set(),
        )

        issue = next(
            item for item in report["issues"] if item["type"] == "guest_incomplete_quote_coverage"
        )
        self.assertNotIn("guest_missing_transcript", issue_types(report))
        self.assertEqual(issue["evidence"]["pick_count"], 2)
        self.assertEqual(issue["evidence"]["quoted_count"], 1)

    def test_missing_transcript_is_not_flagged_when_video_has_no_mapped_picks(self):
        report = audit_data(
            [],
            [
                {
                    "slug": "empty-visit-video",
                    "name": "Empty Visit Video",
                    "youtube_video_id": "abc123",
                    "photo_url": "photo.jpg",
                    "profession": "director",
                    "episode_date": "2026-01-01",
                }
            ],
            [],
            [],
            transcript_ids=set(),
        )

        self.assertNotIn("guest_missing_transcript", issue_types(report))

    def test_guest_and_visit_same_video_missing_transcript_is_reported_once(self):
        picks = [
            {
                "guest_slug": "multi-source",
                "film_id": "film-a",
                "film_title": "Film A",
                "visit_index": 1,
                "quote": "",
            }
        ]
        report = audit_data(
            [],
            [
                {
                    "slug": "multi-source",
                    "name": "Multi Source",
                    "youtube_video_id": "abc123",
                    "photo_url": "photo.jpg",
                    "profession": "director",
                    "episode_date": "2026-01-01",
                    "visits": [
                        {
                            "visit_index": 1,
                            "youtube_video_id": "abc123",
                            "criterion_page_url": "https://www.criterion.com/shop/collection/1",
                        }
                    ],
                }
            ],
            picks,
            picks,
            transcript_ids=set(),
        )

        missing = [
            item for item in report["issues"] if item["type"] == "guest_missing_transcript"
        ]
        self.assertEqual(len(missing), 1)
        self.assertEqual(missing[0]["record_key"], "multi-source|youtube:abc123")

    def test_transcript_ids_from_disk_normalizes_vimeo_prefix(self):
        with tempfile.TemporaryDirectory() as tmp:
            transcripts_dir = Path(tmp)
            (transcripts_dir / "vimeo-123456.json").write_text("{}", encoding="utf-8")
            (transcripts_dir / "abc123.json").write_text("{}", encoding="utf-8")

            self.assertEqual(
                transcript_ids_from_disk(transcripts_dir),
                {"123456", "abc123"},
            )


if __name__ == "__main__":
    unittest.main()
