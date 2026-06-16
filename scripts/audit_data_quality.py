#!/usr/bin/env python3
"""
Read-only local data-quality audit for Criterion Closet Picks.

This script does not fetch external sources and does not modify canonical data.
It reads the committed JSON files, emits a machine-readable report, and writes a
Markdown triage summary for follow-up cleanup work.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.utils import (
    CATALOG_FILE,
    GUESTS_FILE,
    PICKS_FILE,
    PICKS_RAW_FILE,
    TRANSCRIPTS_DIR,
    VALIDATION_DIR,
    load_json,
    save_json,
)


REPORT_SCHEMA_VERSION = 1
DEFAULT_JSON_REPORT = VALIDATION_DIR / "data_quality_audit.json"
DEFAULT_MARKDOWN_REPORT = VALIDATION_DIR / "data_quality_audit.md"
DEFAULT_EXCEPTIONS_FILE = VALIDATION_DIR / "known_data_exceptions.json"
DEFAULT_PUBLIC_PHOTOS_DIR = Path(__file__).resolve().parent.parent / "public" / "photos"

SEVERITY_ORDER = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
CATEGORY_ORDER = {
    "identity": 0,
    "pick_consistency": 1,
    "guest_media": 2,
    "film_metadata": 3,
    "box_set": 4,
}


def criterion_url_kind(url: str | None) -> str | None:
    """Return the Criterion URL kind this audit knows how to reason about."""
    if not url:
        return None
    if url.startswith("https://www.criterion.com/films/"):
        return "film"
    if url.startswith("https://www.criterion.com/boxsets/"):
        return "box_set"
    return None


def normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def has_text(value: Any) -> bool:
    return isinstance(value, str) and value.strip() != ""


def guest_has_photo(guest: dict[str, Any], local_photo_slugs: set[str] | None = None) -> bool:
    """Return whether the site has a usable guest photo source."""
    if guest.get("photo_url"):
        return True
    slug = str(guest.get("slug") or "")
    return bool(slug and local_photo_slugs and slug in local_photo_slugs)


def pick_key(pick: dict[str, Any]) -> str:
    """Stable key for one guest/visit/film pick."""
    return "|".join(
        [
            str(pick.get("guest_slug") or ""),
            str(pick.get("visit_index") or ""),
            str(pick.get("film_id") or pick.get("film_slug") or ""),
        ]
    )


def issue(
    issue_type: str,
    severity: str,
    category: str,
    file: str,
    record_key: str,
    title: str,
    evidence: dict[str, Any] | None = None,
    confidence: str = "high",
    suggested_action: str = "Review and fix the source data if the issue is confirmed.",
) -> dict[str, Any]:
    """Create one deterministic issue record."""
    issue_id = f"{issue_type}:{file}:{record_key}"
    return {
        "id": issue_id,
        "type": issue_type,
        "severity": severity,
        "category": category,
        "file": file,
        "record_key": record_key,
        "title": title,
        "evidence": evidence or {},
        "confidence": confidence,
        "suggested_action": suggested_action,
        "status": "open",
    }


def load_known_exceptions(path: Path = DEFAULT_EXCEPTIONS_FILE) -> dict[str, str]:
    """
    Load accepted issue ids, issue types, and film-scoped issue exceptions.

    Supported shapes:
      {"accepted_issue_ids": ["id"], "notes": {"id": "why"}}
      {"accepted_issue_types": {"issue_type": "why"}}
      {"accepted_issue_types": ["issue_type"]}
      {"accepted_issue_record_keys": {"issue_type": {"record_key": "why"}}}
      {"accepted_issue_record_keys": {"issue_type": ["record_key"]}}
      {"accepted_issue_film_ids": {"issue_type": {"film_id": "why"}}}
      {"accepted_issue_film_ids": {"issue_type": ["film_id"]}}
      {"issues": [{"id": "id", "note": "why"}]}
      ["id"]
    """
    raw = load_json(path)
    if not raw:
        return {}
    if isinstance(raw, list):
        return {str(item): "" for item in raw}
    if not isinstance(raw, dict):
        return {}

    accepted: dict[str, str] = {}
    notes = raw.get("notes") if isinstance(raw.get("notes"), dict) else {}
    for issue_id in raw.get("accepted_issue_ids", []):
        accepted[str(issue_id)] = str(notes.get(issue_id, ""))

    accepted_types = raw.get("accepted_issue_types", {})
    if isinstance(accepted_types, dict):
        for issue_type, note in accepted_types.items():
            accepted[f"type:{issue_type}"] = str(note or "")
    elif isinstance(accepted_types, list):
        for issue_type in accepted_types:
            accepted[f"type:{issue_type}"] = ""

    accepted_record_keys = raw.get("accepted_issue_record_keys", {})
    if isinstance(accepted_record_keys, dict):
        for issue_type, entries in accepted_record_keys.items():
            if isinstance(entries, dict):
                for record_key, note in entries.items():
                    accepted[f"record:{issue_type}:{record_key}"] = str(note or "")
            elif isinstance(entries, list):
                for record_key in entries:
                    accepted[f"record:{issue_type}:{record_key}"] = ""

    accepted_film_ids = raw.get("accepted_issue_film_ids", {})
    if isinstance(accepted_film_ids, dict):
        for issue_type, film_entries in accepted_film_ids.items():
            if isinstance(film_entries, dict):
                for film_id, note in film_entries.items():
                    accepted[f"film:{issue_type}:{film_id}"] = str(note or "")
            elif isinstance(film_entries, list):
                for film_id in film_entries:
                    accepted[f"film:{issue_type}:{film_id}"] = ""

    for entry in raw.get("issues", []):
        if isinstance(entry, dict) and entry.get("id"):
            accepted[str(entry["id"])] = str(entry.get("note") or "")
    return accepted


def exception_film_key(item: dict[str, Any]) -> str | None:
    evidence = item.get("evidence") if isinstance(item.get("evidence"), dict) else {}
    film_id = evidence.get("film_id") or item.get("record_key")
    if not film_id:
        return None
    return f"film:{item['type']}:{film_id}"


def apply_exceptions(
    issues: list[dict[str, Any]], exceptions: dict[str, str]
) -> list[dict[str, Any]]:
    if not exceptions:
        return issues
    for item in issues:
        note = exceptions.get(item["id"])
        if note is None:
            note = exceptions.get(f"type:{item['type']}")
        if note is None:
            film_key = exception_film_key(item)
            if film_key:
                note = exceptions.get(film_key)
        if note is None:
            note = exceptions.get(f"record:{item['type']}:{item['record_key']}")
        if note is not None:
            item["status"] = "accepted"
            item["exception_note"] = note
    return issues


def sort_issues(issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        issues,
        key=lambda item: (
            SEVERITY_ORDER.get(item["severity"], 99),
            CATEGORY_ORDER.get(item["category"], 99),
            item["type"],
            item["file"],
            item["record_key"],
            item["id"],
        ),
    )


def summarize_issues(
    issues: list[dict[str, Any]],
    catalog: list[dict[str, Any]],
    guests: list[dict[str, Any]],
    picks: list[dict[str, Any]],
    picks_raw: list[dict[str, Any]],
) -> dict[str, Any]:
    open_issues = [item for item in issues if item.get("status") == "open"]
    accepted_issues = [item for item in issues if item.get("status") == "accepted"]
    return {
        "total": len(issues),
        "open": len(open_issues),
        "accepted": len(accepted_issues),
        "by_severity": dict(sorted(Counter(i["severity"] for i in issues).items())),
        "open_by_severity": dict(sorted(Counter(i["severity"] for i in open_issues).items())),
        "by_category": dict(sorted(Counter(i["category"] for i in issues).items())),
        "open_by_category": dict(sorted(Counter(i["category"] for i in open_issues).items())),
        "data_counts": {
            "catalog": len(catalog),
            "guests": len(guests),
            "picks": len(picks),
            "picks_raw": len(picks_raw),
        },
    }


def audit_catalog_identity(catalog: list[dict[str, Any]]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    by_criterion_url: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_tmdb_id: dict[int, list[dict[str, Any]]] = defaultdict(list)
    by_title_year_director: dict[tuple[str, Any, str], list[dict[str, Any]]] = defaultdict(list)

    for film in catalog:
        film_id = str(film.get("film_id") or "")
        title = str(film.get("title") or film_id or "Untitled")
        url = film.get("criterion_url") or ""
        kind = criterion_url_kind(url)

        if kind == "film" and not film.get("spine_number"):
            issues.append(
                issue(
                    "catalog_regular_film_missing_spine",
                    "P1",
                    "identity",
                    "data/criterion_catalog.json",
                    film_id,
                    title,
                    {"criterion_url": url, "spine_number": film.get("spine_number")},
                    suggested_action="Backfill the Criterion spine from the canonical film page.",
                )
            )
        elif kind == "box_set" and not film.get("spine_number"):
            issues.append(
                issue(
                    "catalog_box_set_missing_spine",
                    "P3",
                    "box_set",
                    "data/criterion_catalog.json",
                    film_id,
                    title,
                    {"criterion_url": url, "spine_number": film.get("spine_number")},
                    confidence="medium",
                    suggested_action="Review only if this box set should carry a public spine.",
                )
            )

        if url:
            by_criterion_url[url].append(film)
        tmdb_id = film.get("tmdb_id")
        if tmdb_id and not film.get("is_box_set"):
            by_tmdb_id[int(tmdb_id)].append(film)
        title_key = (
            normalize_text(film.get("title")),
            film.get("year"),
            normalize_text(film.get("director")),
        )
        if all(title_key) and not film.get("is_box_set"):
            by_title_year_director[title_key].append(film)

    for url, films in by_criterion_url.items():
        if len(films) <= 1:
            continue
        ids = sorted(str(f.get("film_id") or "") for f in films)
        issues.append(
            issue(
                "duplicate_criterion_url",
                "P1",
                "identity",
                "data/criterion_catalog.json",
                url,
                films[0].get("title") or url,
                {"criterion_url": url, "film_ids": ids},
                suggested_action="Merge or dedupe catalog entries that point at the same Criterion page.",
            )
        )

    for tmdb_id, films in by_tmdb_id.items():
        if len(films) <= 1:
            continue
        issues.append(
            issue(
                "duplicate_tmdb_id",
                "P3",
                "identity",
                "data/criterion_catalog.json",
                str(tmdb_id),
                f"TMDB {tmdb_id}",
                {
                    "tmdb_id": tmdb_id,
                    "films": [
                        {
                            "film_id": f.get("film_id"),
                            "title": f.get("title"),
                            "year": f.get("year"),
                            "director": f.get("director"),
                            "spine_number": f.get("spine_number"),
                        }
                        for f in sorted(films, key=lambda item: str(item.get("film_id") or ""))
                    ],
                },
                confidence="medium",
                suggested_action="Review whether these are legitimate alternate entries or a wrong TMDB match.",
            )
        )

    for key, films in by_title_year_director.items():
        if len(films) <= 1:
            continue
        title_key, year, director = key
        ids = sorted(str(f.get("film_id") or "") for f in films)
        issues.append(
            issue(
                "duplicate_title_year_director",
                "P3",
                "identity",
                "data/criterion_catalog.json",
                f"{title_key}|{year}|{director}",
                films[0].get("title") or title_key,
                {"year": year, "director": director, "film_ids": ids},
                confidence="medium",
                suggested_action="Review for duplicate catalog identity or intentional alternate editions.",
            )
        )

    return issues


def audit_pick_file(
    picks: list[dict[str, Any]],
    catalog_by_film_id: dict[str, dict[str, Any]],
    file: str,
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    order_by_guest_visit: dict[tuple[str, Any, Any], list[dict[str, Any]]] = defaultdict(list)
    missing_pick_order: list[dict[str, Any]] = []

    for pick in picks:
        key = pick_key(pick)
        by_key[key].append(pick)
        guest_slug = pick.get("guest_slug")
        visit_index = pick.get("visit_index")
        pick_order = pick.get("pick_order")
        order_by_guest_visit[(str(guest_slug or ""), visit_index, pick_order)].append(pick)

        title = str(pick.get("film_title") or pick.get("film_id") or "Untitled")
        film_id = str(pick.get("film_id") or pick.get("film_slug") or "")
        catalog_entry = catalog_by_film_id.get(film_id)
        criterion_url = pick.get("criterion_film_url") or ""
        kind = criterion_url_kind(criterion_url)

        if not film_id or film_id not in catalog_by_film_id:
            issues.append(
                issue(
                    "pick_missing_catalog_entry",
                    "P0",
                    "pick_consistency",
                    file,
                    key,
                    title,
                    {"film_id": film_id, "guest_slug": pick.get("guest_slug")},
                    suggested_action="Create or correct the matching catalog film_id before this pick renders.",
                )
            )
            continue

        catalog_spine = catalog_entry.get("spine_number")
        pick_spine = pick.get("catalog_spine")
        if catalog_spine != pick_spine:
            severity = "P0" if pick_spine and catalog_spine else "P1"
            category = "box_set" if catalog_entry.get("is_box_set") else "identity"
            if catalog_entry.get("is_box_set") and not pick_spine:
                severity = "P3"
            issues.append(
                issue(
                    "pick_catalog_spine_mismatch",
                    severity,
                    category,
                    file,
                    key,
                    title,
                    {
                        "film_id": film_id,
                        "pick_catalog_spine": pick_spine,
                        "catalog_spine_number": catalog_spine,
                        "criterion_film_url": criterion_url,
                    },
                    confidence="high" if severity != "P3" else "medium",
                    suggested_action="Align pick.catalog_spine with the catalog entry or fix the film_id match.",
                )
            )

        if kind == "film" and not pick_spine:
            issues.append(
                issue(
                    "pick_regular_film_missing_spine",
                    "P1",
                    "identity",
                    file,
                    key,
                    title,
                    {"film_id": film_id, "criterion_film_url": criterion_url},
                    suggested_action="Backfill catalog_spine for the picked Criterion film.",
                )
            )
        elif kind == "box_set" and not pick_spine:
            issues.append(
                issue(
                    "pick_box_set_missing_spine",
                    "P3",
                    "box_set",
                    file,
                    key,
                    title,
                    {"film_id": film_id, "criterion_film_url": criterion_url},
                    confidence="medium",
                    suggested_action="Review only if this box set should carry a pick-level spine.",
                )
            )

        catalog_title = catalog_entry.get("title")
        if catalog_title and pick.get("film_title") != catalog_title:
            issues.append(
                issue(
                    "pick_title_drift_from_catalog",
                    "P1",
                    "identity",
                    file,
                    key,
                    title,
                    {
                        "film_id": film_id,
                        "pick_title": pick.get("film_title"),
                        "catalog_title": catalog_title,
                    },
                    confidence="medium",
                    suggested_action="Canonicalize film_title or confirm the alternate title is intentional.",
                )
            )

        if not pick.get("source"):
            issues.append(
                issue(
                    "pick_missing_source",
                    "P1",
                    "pick_consistency",
                    file,
                    key,
                    title,
                    {"film_id": film_id, "guest_slug": pick.get("guest_slug")},
                    suggested_action="Backfill source as criterion or letterboxd.",
                )
            )
        if not pick.get("match_method"):
            issues.append(
                issue(
                    "pick_missing_match_method",
                    "P2",
                    "pick_consistency",
                    file,
                    key,
                    title,
                    {"film_id": film_id, "guest_slug": pick.get("guest_slug")},
                    suggested_action="Backfill match_method or document why the match cannot be traced.",
                )
            )
        if not pick.get("visit_index"):
            issues.append(
                issue(
                    "pick_missing_visit_index",
                    "P2",
                    "pick_consistency",
                    file,
                    key,
                    title,
                    {"film_id": film_id, "guest_slug": pick.get("guest_slug")},
                    suggested_action="Assign the pick to a 1-based visit_index.",
                )
            )
        if pick.get("pick_order") is None:
            missing_pick_order.append(pick)

    for key, duplicates in by_key.items():
        if len(duplicates) <= 1:
            continue
        issues.append(
            issue(
                "duplicate_guest_visit_film_pick",
                "P1",
                "pick_consistency",
                file,
                key,
                duplicates[0].get("film_title") or key,
                {"count": len(duplicates), "guest_slug": duplicates[0].get("guest_slug")},
                suggested_action="Dedupe repeated picks for the same guest, visit, and film.",
            )
        )

    for (guest_slug, visit_index, pick_order), picks_with_order in order_by_guest_visit.items():
        if pick_order is None or len(picks_with_order) <= 1:
            continue
        issues.append(
            issue(
                "duplicate_pick_order",
                "P2",
                "pick_consistency",
                file,
                f"{guest_slug}|{visit_index}|{pick_order}",
                f"{guest_slug} visit {visit_index} order {pick_order}",
                {
                    "guest_slug": guest_slug,
                    "visit_index": visit_index,
                    "pick_order": pick_order,
                    "film_ids": [p.get("film_id") for p in picks_with_order],
                },
                suggested_action="Assign unique pick_order values within the guest visit.",
            )
        )

    if missing_pick_order:
        examples = [
            {
                "guest_slug": p.get("guest_slug"),
                "visit_index": p.get("visit_index"),
                "film_id": p.get("film_id"),
                "film_title": p.get("film_title"),
            }
            for p in sorted(missing_pick_order, key=pick_key)[:20]
        ]
        issues.append(
            issue(
                "pick_missing_pick_order",
                "P2",
                "pick_consistency",
                file,
                "missing_pick_order",
                f"{len(missing_pick_order)} picks missing pick_order",
                {"count": len(missing_pick_order), "examples": examples},
                suggested_action="Backfill stable pick_order values within each guest visit.",
            )
        )

    return issues


def audit_raw_enriched_alignment(
    picks: list[dict[str, Any]],
    picks_raw: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    picks_by_key = {pick_key(p): p for p in picks}
    raw_by_key = {pick_key(p): p for p in picks_raw}

    for key, pick in sorted(picks_by_key.items()):
        raw = raw_by_key.get(key)
        if not raw:
            issues.append(
                issue(
                    "enriched_pick_missing_raw_counterpart",
                    "P2",
                    "pick_consistency",
                    "data/picks.json",
                    key,
                    pick.get("film_title") or key,
                    {"guest_slug": pick.get("guest_slug"), "film_id": pick.get("film_id")},
                    suggested_action="Confirm whether this enriched pick should still have raw source data.",
                )
            )
            continue
        for field in ["catalog_spine", "catalog_title", "criterion_film_url", "source"]:
            if pick.get(field) != raw.get(field):
                issues.append(
                    issue(
                        "raw_enriched_pick_mismatch",
                        "P1",
                        "pick_consistency",
                        "data/picks.json",
                        f"{key}|{field}",
                        pick.get("film_title") or key,
                        {
                            "field": field,
                            "picks_value": pick.get(field),
                            "picks_raw_value": raw.get(field),
                        },
                        suggested_action="Propagate the authoritative raw pick metadata into picks.json.",
                    )
                )

    for key, raw in sorted(raw_by_key.items()):
        if key in picks_by_key:
            continue
        issues.append(
            issue(
                "raw_pick_missing_enriched_counterpart",
                "P2",
                "pick_consistency",
                "data/picks_raw.json",
                key,
                raw.get("film_title") or key,
                {"guest_slug": raw.get("guest_slug"), "film_id": raw.get("film_id")},
                suggested_action="Confirm whether this raw pick should appear in picks.json or remain fallback-only.",
            )
        )

    return issues


def guest_video_ids(guest: dict[str, Any]) -> set[str]:
    ids: set[str] = set()
    for field in ["youtube_video_id", "vimeo_video_id"]:
        if guest.get(field):
            ids.add(str(guest[field]))
    for visit in guest.get("visits") or []:
        for field in ["youtube_video_id", "vimeo_video_id"]:
            if visit.get(field):
                ids.add(str(visit[field]))
    return ids


def guest_transcript_video_ids(guest: dict[str, Any]) -> list[tuple[str, str]]:
    ids: set[tuple[str, str]] = set()
    if guest.get("youtube_video_id"):
        ids.add(("youtube", str(guest["youtube_video_id"])))
    if guest.get("vimeo_video_id"):
        ids.add(("vimeo", str(guest["vimeo_video_id"])))
    for visit in guest.get("visits") or []:
        if visit.get("youtube_video_id"):
            ids.add(("youtube", str(visit["youtube_video_id"])))
        if visit.get("vimeo_video_id"):
            ids.add(("vimeo", str(visit["vimeo_video_id"])))
    return sorted(ids)


def video_quote_coverage(
    guest: dict[str, Any],
    guest_picks: list[dict[str, Any]],
    source: str,
    video_id: str,
) -> dict[str, int | bool]:
    """Return quote coverage for the picks associated with a guest video."""
    if not guest_picks:
        return {"pick_count": 0, "quoted_count": 0, "complete": False}

    source_field = f"{source}_video_id"
    visit_indexes: set[int] = set()
    if guest.get(source_field) == video_id:
        visit_indexes.add(1)
    for visit in guest.get("visits") or []:
        if visit.get(source_field) != video_id:
            continue
        try:
            visit_indexes.add(int(visit.get("visit_index")))
        except (TypeError, ValueError):
            pass

    relevant_picks = guest_picks
    if visit_indexes:
        relevant_picks = [
            pick
            for pick in guest_picks
            if int(pick.get("visit_index") or 1) in visit_indexes
        ]

    quoted_count = sum(1 for pick in relevant_picks if has_text(pick.get("quote")))
    return {
        "pick_count": len(relevant_picks),
        "quoted_count": quoted_count,
        "complete": bool(relevant_picks) and quoted_count == len(relevant_picks),
    }


def audit_guest_media(
    guests: list[dict[str, Any]],
    picks: list[dict[str, Any]],
    transcript_ids: set[str] | None,
    local_photo_slugs: set[str] | None = None,
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    known_video_ids: set[str] = set()
    picks_by_guest: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for pick in picks:
        picks_by_guest[str(pick.get("guest_slug") or "")].append(pick)

    for guest in guests:
        slug = str(guest.get("slug") or "")
        name = str(guest.get("name") or slug or "Unnamed guest")
        videos = guest_video_ids(guest)
        transcript_videos = guest_transcript_video_ids(guest)
        known_video_ids.update(video_id for _, video_id in transcript_videos)

        if not videos:
            continue
        if not guest_has_photo(guest, local_photo_slugs):
            issues.append(
                issue(
                    "guest_missing_photo",
                    "P2",
                    "guest_media",
                    "data/guests.json",
                    slug,
                    name,
                    {"guest_slug": slug},
                    suggested_action="Backfill a photo_url or accepted exception.",
                )
            )
        if not guest.get("profession"):
            issues.append(
                issue(
                    "guest_missing_profession",
                    "P2",
                    "guest_media",
                    "data/guests.json",
                    slug,
                    name,
                    {"guest_slug": slug},
                    suggested_action="Backfill normalized profession metadata.",
                )
            )
        if not guest.get("episode_date"):
            issues.append(
                issue(
                    "guest_missing_episode_date",
                    "P2",
                    "guest_media",
                    "data/guests.json",
                    slug,
                    name,
                    {"guest_slug": slug},
                    suggested_action="Backfill the closet visit episode date.",
                )
            )

        visits = guest.get("visits") or []
        visit_count = guest.get("visit_count")
        if visit_count and visits and len(visits) != visit_count:
            issues.append(
                issue(
                    "guest_visit_count_mismatch",
                    "P0",
                    "guest_media",
                    "data/guests.json",
                    slug,
                    name,
                    {"visit_count": visit_count, "visits_length": len(visits)},
                    suggested_action="Make visit_count match the visits array before visit grouping is trusted.",
                )
            )
        for visit in visits:
            visit_index = visit.get("visit_index")
            visit_key = f"{slug}|visit:{visit_index or 'missing'}"
            if not visit_index:
                issues.append(
                    issue(
                        "guest_visit_missing_index",
                        "P2",
                        "guest_media",
                        "data/guests.json",
                        visit_key,
                        name,
                        {"guest_slug": slug, "visit": visit},
                        suggested_action="Assign a 1-based visit_index.",
                    )
                )
            if not (visit.get("youtube_video_id") or visit.get("vimeo_video_id")):
                issues.append(
                    issue(
                        "guest_visit_missing_video",
                        "P2",
                        "guest_media",
                        "data/guests.json",
                        visit_key,
                        name,
                        {"guest_slug": slug, "visit_index": visit_index},
                        suggested_action="Backfill the video id for this visit or document why it has none.",
                    )
                )
            if not visit.get("criterion_page_url"):
                issues.append(
                    issue(
                        "guest_visit_missing_criterion_page",
                        "P2",
                        "guest_media",
                        "data/guests.json",
                        visit_key,
                        name,
                        {"guest_slug": slug, "visit_index": visit_index},
                        suggested_action="Backfill the Criterion collection URL for this visit.",
                    )
                )

        if transcript_ids is not None:
            for source, video_id in sorted(transcript_videos):
                if video_id not in transcript_ids:
                    coverage = video_quote_coverage(
                        guest,
                        picks_by_guest.get(slug, []),
                        source,
                        video_id,
                    )
                    if coverage["pick_count"] == 0:
                        continue
                    if coverage["complete"]:
                        continue
                    if int(coverage["quoted_count"]) > 0:
                        issues.append(
                            issue(
                                "guest_incomplete_quote_coverage",
                                "P2",
                                "guest_media",
                                "data/picks.json",
                                f"{slug}|{source}:{video_id}",
                                name,
                                {
                                    "guest_slug": slug,
                                    "video_source": source,
                                    f"{source}_video_id": video_id,
                                    "pick_count": coverage["pick_count"],
                                    "quoted_count": coverage["quoted_count"],
                                },
                                suggested_action="Review unquoted picks for this video and extract or accept missing quotes.",
                            )
                        )
                        continue
                    issues.append(
                        issue(
                            "guest_missing_transcript",
                            "P2",
                            "guest_media",
                            "data/guests.json",
                            f"{slug}|{source}:{video_id}",
                            name,
                            {
                                "guest_slug": slug,
                                "video_source": source,
                                f"{source}_video_id": video_id,
                                "pick_count": coverage["pick_count"],
                                "quoted_count": coverage["quoted_count"],
                            },
                            suggested_action="Generate or fetch a transcript for this video.",
                        )
                    )

    if transcript_ids is not None:
        for transcript_id in sorted(transcript_ids - known_video_ids):
            issues.append(
                issue(
                    "orphan_transcript",
                    "P3",
                    "guest_media",
                    "data/transcripts",
                    transcript_id,
                    transcript_id,
                    {"transcript_id": transcript_id},
                    confidence="medium",
                    suggested_action="Attach this transcript to a guest visit or remove it if obsolete.",
                )
            )

    return issues


def audit_film_metadata(
    catalog: list[dict[str, Any]],
    picks: list[dict[str, Any]],
    picks_raw: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    picked_ids = {
        str(p.get("film_id") or p.get("film_slug") or "")
        for p in [*picks, *picks_raw]
        if p.get("film_id") or p.get("film_slug")
    }

    for film in catalog:
        film_id = str(film.get("film_id") or "")
        if film_id not in picked_ids or film.get("is_box_set"):
            continue
        title = str(film.get("title") or film_id or "Untitled")
        checks = [
            ("film_missing_poster", "P2", "poster_url", "Backfill a poster_url."),
            ("film_missing_tmdb_id", "P2", "tmdb_id", "Match the film to TMDB or document an exception."),
            ("film_missing_imdb_id", "P3", "imdb_id", "Backfill IMDb id when available."),
            ("film_missing_year", "P2", "year", "Backfill release year."),
            ("film_missing_director", "P2", "director", "Backfill director metadata."),
            ("film_missing_genres", "P2", "genres", "Backfill genre metadata."),
            ("film_missing_criterion_url", "P2", "criterion_url", "Backfill canonical Criterion URL."),
        ]
        for issue_type, severity, field, action in checks:
            if film.get(field):
                continue
            issues.append(
                issue(
                    issue_type,
                    severity,
                    "film_metadata",
                    "data/criterion_catalog.json",
                    film_id,
                    title,
                    {"film_id": film_id, "field": field},
                    suggested_action=action,
                )
            )

    return issues


def audit_data(
    catalog: list[dict[str, Any]],
    guests: list[dict[str, Any]],
    picks: list[dict[str, Any]],
    picks_raw: list[dict[str, Any]],
    transcript_ids: set[str] | None = None,
    local_photo_slugs: set[str] | None = None,
    exceptions: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Run every local audit check and return the report structure."""
    catalog_by_film_id = {
        str(f.get("film_id")): f for f in catalog if f.get("film_id")
    }
    issues: list[dict[str, Any]] = []
    issues.extend(audit_catalog_identity(catalog))
    issues.extend(audit_pick_file(picks, catalog_by_film_id, "data/picks.json"))
    issues.extend(audit_pick_file(picks_raw, catalog_by_film_id, "data/picks_raw.json"))
    issues.extend(audit_raw_enriched_alignment(picks, picks_raw))
    issues.extend(audit_guest_media(guests, picks, transcript_ids, local_photo_slugs))
    issues.extend(audit_film_metadata(catalog, picks, picks_raw))

    issues = sort_issues(apply_exceptions(issues, exceptions or {}))
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "summary": summarize_issues(issues, catalog, guests, picks, picks_raw),
        "issues": issues,
    }


def render_markdown(report: dict[str, Any], max_issues: int = 200) -> str:
    summary = report["summary"]
    lines = [
        "# Data Quality Audit",
        "",
        "Read-only local audit. No external Criterion/TMDB verification was performed.",
        "",
        "## Summary",
        "",
        f"- Total issues: {summary['total']}",
        f"- Open issues: {summary['open']}",
        f"- Accepted issues: {summary['accepted']}",
        f"- Data counts: catalog={summary['data_counts']['catalog']}, "
        f"guests={summary['data_counts']['guests']}, picks={summary['data_counts']['picks']}, "
        f"picks_raw={summary['data_counts']['picks_raw']}",
        "",
        "## Open Issues by Severity",
        "",
    ]
    for severity in ["P0", "P1", "P2", "P3"]:
        lines.append(f"- {severity}: {summary['open_by_severity'].get(severity, 0)}")

    lines.extend(["", "## Open Issues by Category", ""])
    for category, count in summary["open_by_category"].items():
        lines.append(f"- {category}: {count}")

    open_issues = [item for item in report["issues"] if item["status"] == "open"]
    lines.extend(
        [
            "",
            f"## Top Open Issues (first {min(max_issues, len(open_issues))})",
            "",
            "| Severity | Category | Type | Record | Title | Suggested action |",
            "|---|---|---|---|---|---|",
        ]
    )
    for item in open_issues[:max_issues]:
        lines.append(
            "| {severity} | {category} | `{type}` | `{record}` | {title} | {action} |".format(
                severity=item["severity"],
                category=item["category"],
                type=item["type"],
                record=str(item["record_key"]).replace("|", "\\|"),
                title=str(item["title"]).replace("|", "\\|"),
                action=str(item["suggested_action"]).replace("|", "\\|"),
            )
        )

    accepted = [item for item in report["issues"] if item["status"] == "accepted"]
    if accepted:
        lines.extend(["", "## Accepted Issues", ""])
        for item in accepted:
            note = item.get("exception_note") or "accepted exception"
            lines.append(f"- `{item['id']}`: {note}")

    return "\n".join(lines) + "\n"


def transcript_ids_from_disk(transcripts_dir: Path = TRANSCRIPTS_DIR) -> set[str]:
    if not transcripts_dir.exists():
        return set()
    ids: set[str] = set()
    for path in transcripts_dir.glob("*.json"):
        transcript_id = path.stem
        if transcript_id.startswith("vimeo-"):
            transcript_id = transcript_id.removeprefix("vimeo-")
        ids.add(transcript_id)
    return ids


def local_photo_slugs_from_disk(photos_dir: Path = DEFAULT_PUBLIC_PHOTOS_DIR) -> set[str]:
    if not photos_dir.exists():
        return set()
    return {path.stem for path in photos_dir.glob("*.jpg")}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local data-quality audit")
    parser.add_argument("--json-output", type=Path, default=DEFAULT_JSON_REPORT)
    parser.add_argument("--markdown-output", type=Path, default=DEFAULT_MARKDOWN_REPORT)
    parser.add_argument("--exceptions", type=Path, default=DEFAULT_EXCEPTIONS_FILE)
    parser.add_argument("--no-write", action="store_true", help="Print summary without writing reports")
    parser.add_argument("--max-markdown-issues", type=int, default=200)
    args = parser.parse_args()

    catalog = load_json(CATALOG_FILE)
    guests = load_json(GUESTS_FILE)
    picks = load_json(PICKS_FILE)
    picks_raw = load_json(PICKS_RAW_FILE)
    exceptions = load_known_exceptions(args.exceptions)

    report = audit_data(
        catalog,
        guests,
        picks,
        picks_raw,
        transcript_ids=transcript_ids_from_disk(),
        local_photo_slugs=local_photo_slugs_from_disk(),
        exceptions=exceptions,
    )
    markdown = render_markdown(report, max_issues=args.max_markdown_issues)

    if not args.no_write:
        save_json(args.json_output, report)
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(markdown, encoding="utf-8")

    print("DATA QUALITY AUDIT")
    print("==================")
    print(f"Open issues: {report['summary']['open']}")
    print(f"Accepted issues: {report['summary']['accepted']}")
    for severity in ["P0", "P1", "P2", "P3"]:
        print(f"{severity}: {report['summary']['open_by_severity'].get(severity, 0)}")
    if not args.no_write:
        print(f"\nJSON report: {args.json_output}")
        print(f"Markdown report: {args.markdown_output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
