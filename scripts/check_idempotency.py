#!/usr/bin/env python3
"""
Idempotency check / diagnosis harness for the data pipeline.

Re-runs the DETERMINISTIC core of the pipeline (no network, no LLM) on the
currently-committed data, then diffs the result against HEAD. A pipeline that
is idempotent against its committed data produces an empty diff.

The deterministic core (subset of process_all.py that touches no external
service) is, in order:

    normalize_guests.py -> backfill_films.py -> group_box_sets.py
    -> migrate_source_visit.py -> normalize_guests.py

This harness is NON-DESTRUCTIVE: it runs the core (which mutates the working
tree), reports the value-level churn vs HEAD, then restores the four data files
with `git checkout`.

Scope: this gate covers only the network-free deterministic core. The two
network steps with idempotency fixes — build_catalog's catalog merge and
enrich_tmdb's TMDB suppression — are NOT run here; they are covered offline by
scripts/test_pipeline_idempotency.py (merge-identity + suppression tests).

Usage:
    python scripts/check_idempotency.py            # diagnose: print churn, exit 0
    python scripts/check_idempotency.py --gate      # exit 1 if any churn (CI gate)
    python scripts/check_idempotency.py --keep       # don't restore the working tree
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"

# The four committed data files the core mutates.
DATA_FILES = [
    "data/guests.json",
    "data/picks.json",
    "data/picks_raw.json",
    "data/criterion_catalog.json",
]

# Deterministic, network-free core steps (subset of process_all.py).
CORE_STEPS = [
    "normalize_guests.py",
    "backfill_films.py",
    "group_box_sets.py",
    "migrate_source_visit.py",
    "normalize_guests.py",
]

# Stable identity per file: how to match an entry across HEAD vs working so we
# can attribute a change to a specific record.
IDENTITY = {
    "data/guests.json": lambda e: ("slug", e.get("slug")),
    "data/picks.json": lambda e: ("pick", e.get("guest_slug"), e.get("film_id"), e.get("pick_order")),
    "data/picks_raw.json": lambda e: ("pick", e.get("guest_slug"), e.get("film_id"), e.get("pick_order")),
    "data/criterion_catalog.json": lambda e: ("film", e.get("film_id")),
}


def venv_python() -> str:
    p = REPO / ".venv" / "bin" / "python"
    return str(p) if p.exists() else sys.executable


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=str(REPO), capture_output=True, text=True, check=True
    ).stdout


def working_tree_dirty() -> list[str]:
    out = git("status", "--porcelain", *DATA_FILES).strip()
    return [line for line in out.splitlines() if line.strip()]


def head_entries(path: str) -> list[dict]:
    raw = subprocess.run(
        ["git", "show", f"HEAD:{path}"], cwd=str(REPO),
        capture_output=True, text=True, check=True,
    ).stdout
    return json.loads(raw)


def working_entries(path: str) -> list[dict]:
    with open(REPO / path, encoding="utf-8") as f:
        return json.load(f)


def index_by_identity(entries: list[dict], keyfn) -> dict:
    out = {}
    for e in entries:
        k = keyfn(e)
        # On identity collision, disambiguate by appending an occurrence index.
        if k in out:
            i = 2
            while (k, i) in out:
                i += 1
            k = (k, i)
        out[k] = e
    return out


def diff_file(path: str) -> dict:
    """Return a structured churn report for one file (HEAD vs working)."""
    keyfn = IDENTITY[path]
    head = index_by_identity(head_entries(path), keyfn)
    work = index_by_identity(working_entries(path), keyfn)

    added = [k for k in work if k not in head]
    removed = [k for k in head if k not in work]
    changed = []  # (identity, {field: (old, new)})
    for k in head:
        if k not in work:
            continue
        h, w = head[k], work[k]
        fields = {}
        for field in set(h) | set(w):
            if h.get(field) != w.get(field):
                fields[field] = (h.get(field), w.get(field))
        if fields:
            changed.append((k, fields))

    return {
        "path": path,
        "head_count": len(head),
        "work_count": len(work),
        "added": added,
        "removed": removed,
        "changed": changed,
    }


def summarize(report: dict, max_items: int = 25) -> None:
    path = report["path"]
    n_changed = len(report["changed"])
    n_added = len(report["added"])
    n_removed = len(report["removed"])
    if not (n_changed or n_added or n_removed):
        print(f"  OK  {path}: no churn ({report['work_count']} entries)")
        return

    print(f"  CHURN  {path}: {report['head_count']} -> {report['work_count']} "
          f"entries; {n_changed} changed, {n_added} added, {n_removed} removed")

    # Which fields changed, across all changed entries.
    field_counts: dict[str, int] = {}
    for _, fields in report["changed"]:
        for field in fields:
            field_counts[field] = field_counts.get(field, 0) + 1
    if field_counts:
        fields_str = ", ".join(f"{f}({c})" for f, c in
                               sorted(field_counts.items(), key=lambda x: -x[1]))
        print(f"         fields touched: {fields_str}")

    for ident, fields in report["changed"][:max_items]:
        diffs = "; ".join(f"{f}: {old!r}->{new!r}" for f, (old, new) in fields.items())
        print(f"           {ident}: {diffs}")
    if n_changed > max_items:
        print(f"           ... and {n_changed - max_items} more changed")
    for ident in report["added"][:max_items]:
        print(f"           + added {ident}")
    for ident in report["removed"][:max_items]:
        print(f"           - removed {ident}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Pipeline idempotency check")
    parser.add_argument("--gate", action="store_true",
                        help="Exit non-zero if any churn is detected (CI gate)")
    parser.add_argument("--keep", action="store_true",
                        help="Do not restore the working tree after running")
    args = parser.parse_args()

    dirty = working_tree_dirty()
    if dirty:
        print("ERROR: data files have uncommitted changes; commit/stash first:")
        for line in dirty:
            print(f"  {line}")
        return 2

    py = venv_python()
    print(f"Running deterministic core ({len(CORE_STEPS)} steps) on committed data...")
    for step in CORE_STEPS:
        print(f"  - {step}")
        r = subprocess.run([py, str(SCRIPTS / step)], cwd=str(REPO),
                           capture_output=True, text=True)
        if r.returncode != 0:
            print(f"STEP FAILED: {step}\n{r.stdout[-2000:]}\n{r.stderr[-2000:]}")
            if not args.keep:
                git("checkout", "--", *DATA_FILES)
            return 2

    print("\nChurn report (working tree vs HEAD):")
    reports = [diff_file(p) for p in DATA_FILES]
    for rep in reports:
        summarize(rep)

    total_churn = sum(len(r["changed"]) + len(r["added"]) + len(r["removed"])
                      for r in reports)

    if not args.keep:
        git("checkout", "--", *DATA_FILES)
        print("\n(working tree restored)")

    print(f"\nTotal churned records: {total_churn}")
    if args.gate and total_churn:
        print("GATE FAILED: pipeline is not idempotent against committed data.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
