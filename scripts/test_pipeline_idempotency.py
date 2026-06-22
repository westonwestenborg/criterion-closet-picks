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


class TestCatalogMerge(unittest.TestCase):
    """build_catalog merges into the existing catalog instead of overwriting it,
    so a re-build never destroys enrichment / verified spines / box-set rows."""

    def test_merge_preserves_enriched_fields(self):
        from scripts.build_catalog import merge_into_existing
        existing = [{"spine_number": 1, "film_id": "x", "title": "X",
                     "director": "Kurosawa", "tmdb_id": 999, "criterion_url": "u"}]
        # Digital Bits returns the bare row (no director/tmdb).
        scraped = [{"spine_number": 1, "title": "X", "director": "", "tmdb_id": None,
                    "criterion_url": "u"}]
        merged, n_new, n_filled = merge_into_existing(existing, scraped)
        self.assertEqual(n_new, 0)
        self.assertEqual(merged[0]["director"], "Kurosawa")  # not clobbered
        self.assertEqual(merged[0]["tmdb_id"], 999)

    def test_merge_appends_new_spine_and_keeps_unmatched(self):
        from scripts.build_catalog import merge_into_existing
        existing = [{"spine_number": None, "film_id": "boxset", "is_box_set": True},
                    {"spine_number": 1, "film_id": "x", "title": "X"}]
        scraped = [{"spine_number": 1, "title": "X"},
                   {"spine_number": 2, "title": "New Release", "criterion_url": "n"}]
        merged, n_new, n_filled = merge_into_existing(existing, scraped)
        self.assertEqual(n_new, 1)
        self.assertIn("boxset", [e.get("film_id") for e in merged])  # unmatched kept
        self.assertEqual(merged[-1]["title"], "New Release")  # appended

    def test_merge_fills_empty_criterion_url(self):
        from scripts.build_catalog import merge_into_existing
        existing = [{"spine_number": 1, "film_id": "x", "criterion_url": ""}]
        scraped = [{"spine_number": 1, "criterion_url": "https://criterion.com/x"}]
        merged, n_new, n_filled = merge_into_existing(existing, scraped)
        self.assertEqual(n_filled, 1)
        self.assertEqual(merged[0]["criterion_url"], "https://criterion.com/x")

    def test_merge_identity_on_committed_catalog(self):
        # Merging a Digital-Bits-shaped scrape of the committed catalog must be a no-op.
        import copy
        from scripts.utils import CATALOG_FILE, load_json
        from scripts.build_catalog import merge_into_existing
        catalog = load_json(CATALOG_FILE)
        scraped = [{"spine_number": e["spine_number"], "title": e.get("title", ""),
                    "criterion_url": e.get("criterion_url", "")}
                   for e in catalog if e.get("spine_number") is not None]
        merged, n_new, n_filled = merge_into_existing(copy.deepcopy(catalog), scraped)
        self.assertEqual(n_new, 0, "no spurious new entries")
        self.assertEqual(merged, catalog, "merge must preserve the catalog exactly")

    def test_verified_spines_noop_on_committed_catalog(self):
        from scripts.utils import CATALOG_FILE, load_json
        from scripts.apply_verified_spines import (
            DEFAULT_VERIFICATION_FILE, load_verification_records, apply_verified_spines,
        )
        catalog = load_json(CATALOG_FILE)
        records = load_verification_records(DEFAULT_VERIFICATION_FILE)
        _, report = apply_verified_spines(catalog, records)
        self.assertEqual(report["summary"]["spines_updated"], 0,
                         "verified spines already applied; re-apply must not churn")


if __name__ == "__main__":
    unittest.main()
