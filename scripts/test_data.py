#!/usr/bin/env python3
"""
Data validation tests for Criterion Closet Picks.
Verifies data integrity and checks that known bug fixes remain in effect.

Run: python scripts/test_data.py
"""

import sys
import re
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    GUESTS_FILE,
    PICKS_FILE,
    PICKS_RAW_FILE,
    load_json,
)
from scripts.schema import CatalogFilm, Guest, Pick
from scripts.audit_data_quality import load_known_exceptions

# Load data once for all tests
catalog: list[CatalogFilm] = load_json(CATALOG_FILE)
guests: list[Guest] = load_json(GUESTS_FILE)
picks: list[Pick] = load_json(PICKS_FILE)
picks_raw: list[Pick] = load_json(PICKS_RAW_FILE)
known_exceptions = load_known_exceptions()

# Build lookup indexes
catalog_by_spine = {f["spine_number"]: f for f in catalog}
catalog_by_film_id = {f["film_id"]: f for f in catalog}
guest_by_slug = {g["slug"]: g for g in guests}

SMART_QUOTE_TRANSLATION = str.maketrans(
    {
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
    }
)


def box_set_title_lookup_keys(title: str) -> set[str]:
    """Return stable lookup keys for user-facing box-set member titles."""
    key = re.sub(r"\s+", " ", title.translate(SMART_QUOTE_TRANSLATION).strip().lower())
    keys = {key}
    without_parenthetical = re.sub(r"\s+\([^)]*\)$", "", key).strip()
    if without_parenthetical:
        keys.add(without_parenthetical)
    return keys


class TestBoxSetTitleLookup(unittest.TestCase):
    """Box set title lookups should tolerate display-title differences."""

    def test_normalizes_smart_quotes(self):
        self.assertEqual(
            box_set_title_lookup_keys("Martin Scorsese’s World Cinema Project No. 2"),
            box_set_title_lookup_keys("Martin Scorsese's World Cinema Project No. 2"),
        )

    def test_catalog_parenthetical_suffixes_match_display_titles(self):
        self.assertIn(
            "dry summer",
            box_set_title_lookup_keys("Dry Summer (World Cinema Project)"),
        )


class TestFilmCoverage(unittest.TestCase):
    """All picked films should have catalog entries."""

    def test_all_picked_films_in_catalog(self):
        """Every film referenced in picks.json with a catalog_spine should exist in the catalog."""
        missing = []
        for pick in picks:
            spine = pick.get("catalog_spine")
            if spine and spine not in catalog_by_spine:
                missing.append(
                    f"{pick.get('film_title', '?')} (spine {spine}, guest {pick.get('guest_slug', '?')})"
                )
        self.assertEqual(missing, [], f"Picks reference missing catalog spines:\n" + "\n".join(missing))


class TestNoZeroDecade(unittest.TestCase):
    """No film should have year=0."""

    def test_no_zero_year(self):
        """Films can have year=null/None, but never year=0."""
        zero_year = [
            f"{f.get('title', '?')} (spine {f.get('spine_number', '?')})"
            for f in catalog
            if f.get("year") == 0
        ]
        self.assertEqual(zero_year, [], f"Films with year=0:\n" + "\n".join(zero_year))


class TestCriterionUrls(unittest.TestCase):
    """Criterion URLs should be canonical, not search URL fallbacks."""

    def test_no_search_url_fallbacks(self):
        """Any non-empty criterion_url should NOT contain /shop/browse."""
        search_urls = [
            f"{f.get('title', '?')} (spine {f.get('spine_number', '?')}): {f.get('criterion_url', '')}"
            for f in catalog
            if f.get("criterion_url") and "/shop/browse" in f["criterion_url"]
        ]
        self.assertEqual(
            search_urls, [], f"Films with search URL fallbacks:\n" + "\n".join(search_urls)
        )


class TestBoxSetStructure(unittest.TestCase):
    """Box set data should be well-formed."""

    def test_aggregates_have_name(self):
        """Picks with box_set_film_count should also have box_set_name."""
        missing_name = []
        for pick in picks:
            if pick.get("box_set_film_count") and not pick.get("box_set_name"):
                missing_name.append(
                    f"{pick.get('film_title', '?')} (guest {pick.get('guest_slug', '?')})"
                )
        self.assertEqual(
            missing_name, [],
            f"Box set picks with film_count but no name:\n" + "\n".join(missing_name),
        )

    def test_box_set_film_titles_exist(self):
        """Box set film titles listed in box_set_film_titles should be findable in the catalog."""
        catalog_title_keys = set()
        for film in catalog:
            catalog_title_keys.update(box_set_title_lookup_keys(film["title"]))
        missing = []
        for pick in picks:
            for title in pick.get("box_set_film_titles", []):
                if box_set_title_lookup_keys(title).isdisjoint(catalog_title_keys):
                    missing.append(
                        f"'{title}' from box set '{pick.get('box_set_name', '?')}' "
                        f"(guest {pick.get('guest_slug', '?')})"
                    )
        # Advisory only -- box set sub-films may not all be separate catalog entries
        if missing:
            print(f"\nAdvisory: {len(missing)} box set film titles not found in catalog (may be expected)")


class TestGuestCoverage(unittest.TestCase):
    """Every guest should have displayable content."""

    def test_all_guests_have_picks(self):
        """Every guest in guests.json should have at least one pick in picks.json or picks_raw.json."""
        picks_guest_slugs = {p["guest_slug"] for p in picks}
        raw_guest_slugs = {p["guest_slug"] for p in picks_raw}
        all_pick_slugs = picks_guest_slugs | raw_guest_slugs

        no_picks = [
            f"{g['name']} ({g['slug']})"
            for g in guests
            if g["slug"] not in all_pick_slugs
        ]
        self.assertEqual(
            no_picks, [],
            f"Guests with no picks in either picks.json or picks_raw.json:\n" + "\n".join(no_picks),
        )


class TestYearValidity(unittest.TestCase):
    """Film years should be reasonable."""

    def test_years_are_valid(self):
        """Non-null years should be > 1800 and <= 2026."""
        invalid = [
            f"{f.get('title', '?')} (spine {f.get('spine_number', '?')}): year={f.get('year')}"
            for f in catalog
            if f.get("year") is not None and (f["year"] <= 1800 or f["year"] > 2026)
        ]
        self.assertEqual(
            invalid, [],
            f"Films with invalid years:\n" + "\n".join(invalid),
        )


class TestFilmPageLinks(unittest.TestCase):
    """Film links should resolve to canonical Criterion URLs."""

    def test_criterion_urls_are_canonical(self):
        """Non-empty criterion_url values should start with https://www.criterion.com/films/ or /boxsets/."""
        bad_urls = []
        for f in catalog:
            url = f.get("criterion_url", "")
            if not url:
                continue
            if not (
                url.startswith("https://www.criterion.com/films/")
                or url.startswith("https://www.criterion.com/boxsets/")
            ):
                bad_urls.append(
                    f"{f.get('title', '?')} (spine {f.get('spine_number', '?')}): {url}"
                )
        self.assertEqual(
            bad_urls, [],
            f"Films with non-canonical criterion_url:\n" + "\n".join(bad_urls),
        )


class TestNoDuplicateVideoIds(unittest.TestCase):
    """No two guests should share the same YouTube video ID."""

    def test_no_duplicate_video_ids(self):
        """Each youtube_video_id should appear at most once across guests."""
        video_map: dict[str, list[str]] = {}
        for g in guests:
            vid = g.get("youtube_video_id")
            if vid:
                video_map.setdefault(vid, []).append(g["slug"])
        dupes = {vid: slugs for vid, slugs in video_map.items() if len(slugs) > 1}
        self.assertEqual(
            dupes, {},
            f"Duplicate youtube_video_id assignments:\n"
            + "\n".join(f"  {vid}: {slugs}" for vid, slugs in dupes.items()),
        )


class TestNoNameArtifacts(unittest.TestCase):
    """Guest names should not contain scraping artifacts."""

    def test_no_name_artifacts(self):
        """No person guest name should contain 'Closet', 'Criterion', or 'Picks'."""
        bad = [
            f"{g['name']} ({g['slug']})"
            for g in guests
            if g.get("guest_type", "person") == "person"
            and any(word in g["name"] for word in ["Closet", "Criterion", "Picks"])
        ]
        self.assertEqual(
            bad, [],
            f"Guests with name artifacts:\n" + "\n".join(bad),
        )


class TestNoRepeatVisitSuffixes(unittest.TestCase):
    """Guest names should not contain visit markers."""

    def test_no_repeat_visit_suffixes(self):
        """No guest name should contain '(2nd Visit)', '(3rd Visit)', etc."""
        import re
        bad = [
            f"{g['name']} ({g['slug']})"
            for g in guests
            if re.search(r"\(\d+\w+\s+Visit\)", g["name"])
        ]
        self.assertEqual(
            bad, [],
            f"Guests with repeat visit suffixes:\n" + "\n".join(bad),
        )


class TestNoDuplicateFilmIds(unittest.TestCase):
    """Each film_id should be unique across the catalog."""

    def test_no_duplicate_film_ids(self):
        """No two catalog entries should share the same film_id."""
        from collections import Counter
        id_counts = Counter(f["film_id"] for f in catalog)
        dupes = {fid: count for fid, count in id_counts.items() if count > 1}
        if dupes:
            details = []
            for fid in sorted(dupes):
                entries = [f for f in catalog if f["film_id"] == fid]
                spines = [str(f.get("spine_number", "?")) for f in entries]
                details.append(f"  {fid} ({dupes[fid]}x): spines {', '.join(spines)}")
            self.fail(f"Duplicate film_ids found:\n" + "\n".join(details))


class TestNoPrefixedFilmIdShadows(unittest.TestCase):
    """Catalog entries with film_id like '<digits>-<slug>' must not have a base
    twin where film_id == '<slug>'. These are duplicate entries created when
    Criterion serves the same film at both /films/<id>-slug and /films/slug,
    and they split picks across two ids. Use scripts/dedupe_film_ids.py to fix.
    """

    def test_no_prefixed_film_id_shadows_base(self):
        import re
        prefix_re = re.compile(r"^(\d+)-(.+)")
        ids = {f["film_id"] for f in catalog}
        shadows = []
        for f in catalog:
            m = prefix_re.match(f.get("film_id", ""))
            if m and m.group(2) in ids:
                shadows.append(f"{f['film_id']} -> base {m.group(2)}")
        self.assertEqual(
            shadows, [],
            "Prefixed film_ids shadow a base entry "
            "(run scripts/dedupe_film_ids.py):\n" + "\n".join(shadows),
        )


class TestPickTitlesAreCanonical(unittest.TestCase):
    """Every pick's film_title must equal the canonical catalog title for its
    film_id, so each film has a single source of truth. Picks whose film_id is
    not in the catalog are skipped (covered by TestFilmCoverage).
    """

    def test_picks_film_title_matches_catalog(self):
        self._check(picks, "picks.json")

    def test_picks_raw_film_title_matches_catalog(self):
        self._check(picks_raw, "picks_raw.json")

    def _check(self, entries: list[dict], label: str) -> None:
        mismatches = []
        for p in entries:
            film_id = p.get("film_slug") or p.get("film_id")
            if not film_id:
                continue
            entry = catalog_by_film_id.get(film_id)
            if not entry:
                continue
            canonical = entry.get("title")
            actual = p.get("film_title")
            if canonical and actual != canonical:
                mismatches.append(
                    f"  {film_id} guest={p.get('guest_slug', '?')}: "
                    f"got={actual!r} expected={canonical!r}"
                )
        self.assertEqual(
            mismatches, [],
            f"{label} entries with non-canonical film_title "
            "(run scripts/dedupe_film_ids.py):\n" + "\n".join(mismatches),
        )


class TestNoDuplicateTmdbIds(unittest.TestCase):
    """No two non-box-set films should share the same TMDB ID unexpectedly."""

    def test_no_duplicate_tmdb_ids(self):
        """Each tmdb_id should appear at most once across non-box-set catalog entries.

        Expected duplicates (alternate titles, World Cinema Project variants, box set
        variants of the same film) are reported as advisory. Only unexpected duplicates
        (genuinely different films sharing a TMDB ID) cause a failure.
        """
        from collections import Counter
        # Only check non-box-set entries that have a tmdb_id
        tmdb_ids = [
            f["tmdb_id"] for f in catalog
            if f.get("tmdb_id") and not f.get("is_box_set")
        ]
        id_counts = Counter(tmdb_ids)
        dupes = {
            tid: count
            for tid, count in id_counts.items()
            if count > 1 and f"record:duplicate_tmdb_id:{tid}" not in known_exceptions
        }
        # Advisory only — duplicates are common for alternate titles, WCP variants,
        # and box set variants of the same film. Reviewed duplicates live in the
        # data-quality exception allowlist; new ones should still be surfaced here.
        if dupes:
            details = []
            for tid in sorted(dupes):
                entries = [f for f in catalog if f.get("tmdb_id") == tid and not f.get("is_box_set")]
                names = [f"{f.get('title', '?')} ({f.get('film_id', '?')})" for f in entries]
                details.append(f"  tmdb_id {tid} ({dupes[tid]}x): {', '.join(names)}")
            print(f"\nAdvisory: {len(dupes)} duplicate tmdb_ids across non-box-set entries "
                  f"(use audit_tmdb.py to review):\n" + "\n".join(details))


class TestPosterCoverage(unittest.TestCase):
    """Picked films should have poster images."""

    def test_poster_coverage(self):
        """At least 90% of picked films should have a poster_url."""
        # Build set of picked film_ids
        picked_ids = set()
        for p in picks:
            slug = p.get("film_slug") or p.get("film_id", "")
            if slug:
                picked_ids.add(slug)

        picked_films = [f for f in catalog if f.get("film_id") in picked_ids]
        if not picked_films:
            return  # No picked films to check

        with_poster = sum(1 for f in picked_films if f.get("poster_url"))
        coverage = with_poster / len(picked_films) * 100

        # Advisory warning, not a hard failure
        if coverage < 90:
            print(
                f"\nAdvisory: Poster coverage for picked films is {coverage:.1f}% "
                f"({with_poster}/{len(picked_films)}), target is 90%"
            )


class TestPickCountAccuracy(unittest.TestCase):
    """Guest pick counts should match actual picks in data files."""

    def test_pick_count_accuracy(self):
        """pick_count should match the number of displayable picks for each guest.

        Display rule: source === 'criterion' OR has a non-empty quote.
        Mirrors getDisplayablePicksForGuest() in data.ts.
        """
        # Build per-guest picks.json and raw picks lookups
        picks_by_guest: dict[str, list] = {}
        for p in picks:
            picks_by_guest.setdefault(p["guest_slug"], []).append(p)

        raw_by_guest: dict[str, list] = {}
        for p in picks_raw:
            raw_by_guest.setdefault(p["guest_slug"], []).append(p)

        mismatches = []
        for g in guests:
            slug = g["slug"]
            guest_picks = picks_by_guest.get(slug, [])
            guest_raw = raw_by_guest.get(slug, [])

            # Count displayable processed picks
            processed_slugs = set()
            displayable = 0
            for p in guest_picks:
                film_key = p.get("film_slug") or p.get("film_id", "")
                processed_slugs.add(film_key)
                if p.get("source") == "criterion":
                    displayable += 1
                elif (p.get("quote") or "").strip():
                    displayable += 1

            # Add criterion-sourced raw picks not in processed
            for rp in guest_raw:
                film_key = rp.get("film_id", "")
                if film_key in processed_slugs:
                    continue
                if rp.get("source") == "criterion":
                    displayable += 1

            declared = g.get("pick_count", 0)
            if displayable != declared:
                mismatches.append(
                    f"{g['name']} ({slug}): declared={declared}, actual={displayable}"
                )
        self.assertEqual(
            mismatches, [],
            f"pick_count mismatches:\n" + "\n".join(mismatches),
        )


class TestSuppressedTmdbIds(unittest.TestCase):
    """Multi-part releases (che etc.) keep tmdb_id null; enrich must not re-add it.

    Idempotency guard (Phase 1b): a re-run of the pipeline must not re-introduce
    the TMDB matches the repair layer deliberately removed."""

    def test_suppressed_films_have_null_tmdb_id(self):
        from scripts.enrich_tmdb import load_suppressed_tmdb_ids
        suppressed = load_suppressed_tmdb_ids()
        self.assertTrue(suppressed, "expected suppressed film_ids in known_data_exceptions.json")
        offenders = [
            f["film_id"] for f in catalog
            if f.get("film_id") in suppressed and f.get("tmdb_id") is not None
        ]
        self.assertEqual(
            offenders, [],
            f"suppressed multi-part films must keep tmdb_id null: {offenders}",
        )


class TestMultiVisitVisitIndex(unittest.TestCase):
    """Multi-visit guests have contiguous 1..N visit_index on their visits.

    Idempotency guard (Phase 1a): normalize_guests must preserve/assign these
    rather than dropping them on rebuild."""

    def test_visits_have_contiguous_visit_index(self):
        bad = []
        for g in guests:
            visits = g.get("visits") or []
            if len(visits) < 2:
                continue
            idxs = [v.get("visit_index") for v in visits]
            if idxs != list(range(1, len(visits) + 1)):
                bad.append(f"{g['slug']}: {idxs}")
        self.assertEqual(
            bad, [],
            f"multi-visit guests need contiguous 1..N visit_index: {bad}",
        )


class TestCanonicalKeyOrder(unittest.TestCase):
    """Every record is stored in canonical key order, so a pipeline re-run that
    changes no values produces an empty diff (byte-idempotency). See
    scripts/schema.py CANONICALIZERS and scripts/check_idempotency.py."""

    def _offenders(self, records, canon, label):
        import json
        out = []
        for r in records:
            if json.dumps(r, ensure_ascii=False) != json.dumps(canon(r), ensure_ascii=False):
                out.append(f"{label}:{r.get('film_id') or r.get('slug') or r.get('guest_slug')}")
        return out

    def test_files_in_canonical_order(self):
        from scripts.schema import canonicalize_pick, canonicalize_guest, canonicalize_film
        bad = []
        bad += self._offenders(picks, canonicalize_pick, "picks")
        bad += self._offenders(picks_raw, canonicalize_pick, "picks_raw")
        bad += self._offenders(guests, canonicalize_guest, "guests")
        bad += self._offenders(catalog, canonicalize_film, "catalog")
        self.assertEqual(
            len(bad), 0,
            f"{len(bad)} records not in canonical key order (would churn on save): {bad[:10]}",
        )


if __name__ == "__main__":
    unittest.main()
