#!/usr/bin/env python3
"""
Unit tests for the pipeline-idempotency guards:
  - canonical key ordering (scripts/schema.py)
  - TMDB suppression in enrich_tmdb (Phase 1b)

Run: python scripts/test_pipeline_idempotency.py
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.schema import (
    canonicalize_pick,
    canonicalize_guest,
    canonicalize_film,
)


class TestCanonicalKeyOrdering(unittest.TestCase):
    def test_pick_keys_follow_schema_order(self):
        # Scrambled input; canonical output orders present keys by schema declaration.
        p = {"quote": "x", "guest_slug": "a", "film_id": "f", "film_title": "t"}
        out = canonicalize_pick(p)
        self.assertEqual(list(out), ["guest_slug", "film_id", "film_title", "quote"])
        self.assertEqual(out, p)  # values unchanged, only order

    def test_unknown_keys_appended_sorted_never_dropped(self):
        p = {"zzz_extra": 1, "guest_slug": "a", "aaa_extra": 2}
        out = canonicalize_pick(p)
        self.assertEqual(list(out), ["guest_slug", "aaa_extra", "zzz_extra"])
        self.assertEqual(out["zzz_extra"], 1)  # not dropped

    def test_guest_visits_are_reordered(self):
        g = {"name": "N", "slug": "x",
             "visits": [{"criterion_page_url": "u", "visit_index": 1}]}
        out = canonicalize_guest(g)
        self.assertEqual(list(out), ["name", "slug", "visits"])
        self.assertEqual(list(out["visits"][0]), ["visit_index", "criterion_page_url"])

    def test_film_credits_are_reordered(self):
        f = {"title": "T", "film_id": "x",
             "credits": {"cast": [], "directors": []}}
        out = canonicalize_film(f)
        self.assertEqual(list(out), ["film_id", "title", "credits"])
        self.assertEqual(list(out["credits"]), ["directors", "cast"])

    def test_canonicalize_is_idempotent(self):
        for canon, rec in [
            (canonicalize_pick, {"quote": "x", "guest_slug": "a", "film_id": "f"}),
            (canonicalize_guest, {"slug": "x", "name": "N",
                                  "visits": [{"criterion_page_url": "u", "visit_index": 1}]}),
            (canonicalize_film, {"title": "T", "film_id": "x", "credits": {"cast": []}}),
        ]:
            once = json.dumps(canon(rec), ensure_ascii=False)
            twice = json.dumps(canon(canon(rec)), ensure_ascii=False)
            self.assertEqual(once, twice)


class TestTmdbSuppression(unittest.TestCase):
    def test_suppressed_film_is_noop_without_network(self):
        from scripts.enrich_tmdb import enrich_film

        class BoomClient:
            def __getattr__(self, name):
                def boom(*a, **k):
                    raise AssertionError(f"network call ({name}) for suppressed film")
                return boom

        film = {"film_id": "che", "title": "Che", "tmdb_id": None,
                "year": 2008, "genres": ["Drama"]}
        out = enrich_film(BoomClient(), film, {}, None, {"che"})
        self.assertIsNone(out["tmdb_id"])
        self.assertEqual(out["genres"], ["Drama"])  # untouched

    def test_load_suppressed_includes_che(self):
        from scripts.enrich_tmdb import load_suppressed_tmdb_ids
        self.assertIn("che", load_suppressed_tmdb_ids())


if __name__ == "__main__":
    unittest.main()
