#!/usr/bin/env python3
"""
Batch orchestrator for the Criterion Closet Picks data pipeline.
Runs all pipeline steps in order, with optional --pilot flag for the 10-video test.

Steps:
  1. Build/update Criterion catalog (build_catalog.py)
  2. Scrape Letterboxd picks (scrape_letterboxd.py)
  3. Scrape Criterion.com picks (scrape_criterion_picks.py) [fallback/supplement]
  4. Normalize guest data (normalize_guests.py)
  5. Match YouTube videos + fetch transcripts (match_youtube.py)
  6. Extract quotes via Gemini (extract_quotes.py)
  7. Backfill films & propagate URLs (backfill_films.py)
  8. Group box set films (group_box_sets.py)
  9. Scrape box set images (scrape_box_set_images.py)
  10. Enrich via TMDB (enrich_tmdb.py)
  11. Validate (validate.py)

Usage:
  python scripts/process_all.py --pilot          # 10-video pilot
  python scripts/process_all.py                   # Full pipeline
  python scripts/process_all.py --skip-catalog    # Skip catalog rebuild
  python scripts/process_all.py --skip-criterion  # Skip Criterion.com scraping
  python scripts/process_all.py --from-step 5     # Resume from step 5
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.utils import log

SCRIPTS_DIR = Path(__file__).resolve().parent


def run_step(name: str, cmd: list[str], step_num: int, total_steps: int) -> bool:
    """Run a pipeline step as a subprocess."""
    log(f"\n{'='*60}")
    log(f"  Step {step_num}/{total_steps}: {name}")
    log(f"{'='*60}")
    log(f"  Command: {' '.join(cmd)}")

    start = time.time()
    try:
        result = subprocess.run(
            cmd,
            cwd=str(SCRIPTS_DIR.parent),
            timeout=7200,  # 2 hour max per step
        )
        elapsed = time.time() - start
        if result.returncode == 0:
            log(f"  Completed in {elapsed:.1f}s")
            return True
        else:
            log(f"  FAILED with return code {result.returncode} after {elapsed:.1f}s")
            return False
    except subprocess.TimeoutExpired:
        log(f"  TIMED OUT after 7200s")
        return False
    except Exception as e:
        log(f"  ERROR: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Run the full data pipeline")
    parser.add_argument("--pilot", action="store_true", help="Only process 10 pilot guests")
    parser.add_argument("--skip-catalog", action="store_true", help="Skip catalog rebuild")
    parser.add_argument("--skip-letterboxd", action="store_true", help="Skip Letterboxd scraping")
    parser.add_argument("--skip-criterion", action="store_true", help="Skip Criterion.com scraping")
    parser.add_argument("--skip-youtube", action="store_true", help="Skip YouTube matching")
    parser.add_argument("--skip-quotes", action="store_true", help="Skip quote extraction")
    parser.add_argument("--skip-enrich", action="store_true", help="Skip TMDB enrichment")
    parser.add_argument("--skip-normalize", action="store_true", help="Skip guest normalization")
    parser.add_argument("--skip-validate", action="store_true", help="Skip validation")
    parser.add_argument("--from-step", type=int, default=1, help="Start from step N (1-11)")
    parser.add_argument("--limit", type=int, default=0, help="Limit items per step")
    args = parser.parse_args()

    # Determine Python executable (use the venv's python)
    venv_python = SCRIPTS_DIR.parent / ".venv" / "bin" / "python"
    if venv_python.exists():
        python = str(venv_python)
    else:
        python = sys.executable

    pilot_flag = ["--pilot"] if args.pilot else []
    limit_flag = ["--limit", str(args.limit)] if args.limit else []

    steps = []
    step_num = 0

    # Step 1: Build catalog
    step_num += 1
    if not args.skip_catalog and args.from_step <= step_num:
        steps.append((
            "Build Criterion Catalog",
            [python, str(SCRIPTS_DIR / "build_catalog.py")],
            step_num,
        ))

    # Step 2: Scrape Letterboxd
    step_num += 1
    if not args.skip_letterboxd and args.from_step <= step_num:
        steps.append((
            "Scrape Letterboxd Picks",
            [python, str(SCRIPTS_DIR / "scrape_letterboxd.py")] + pilot_flag + limit_flag,
            step_num,
        ))

    # Step 3: Scrape Criterion.com picks (fallback/supplement)
    step_num += 1
    if not args.skip_criterion and args.from_step <= step_num:
        steps.append((
            "Scrape Criterion.com Picks",
            [python, str(SCRIPTS_DIR / "scrape_criterion_picks.py")] + limit_flag,
            step_num,
        ))

    # Step 4: Normalize guest data
    step_num += 1
    if not args.skip_normalize and args.from_step <= step_num:
        steps.append((
            "Normalize Guest Data",
            [python, str(SCRIPTS_DIR / "normalize_guests.py")],
            step_num,
        ))

    # Step 5: Match YouTube + transcripts
    step_num += 1
    if not args.skip_youtube and args.from_step <= step_num:
        steps.append((
            "Match YouTube Videos & Fetch Transcripts",
            [python, str(SCRIPTS_DIR / "match_youtube.py")] + pilot_flag + limit_flag,
            step_num,
        ))

    # Step 6: Extract quotes
    step_num += 1
    if not args.skip_quotes and args.from_step <= step_num:
        steps.append((
            "Extract Quotes via Gemini",
            [python, str(SCRIPTS_DIR / "extract_quotes.py")] + pilot_flag + limit_flag,
            step_num,
        ))

    # Step 7: Backfill missing films + propagate URLs + flag box sets
    step_num += 1
    if args.from_step <= step_num:
        steps.append((
            "Backfill Films & Propagate URLs",
            [python, str(SCRIPTS_DIR / "backfill_films.py")],
            step_num,
        ))

    # Step 8: Group box set films
    step_num += 1
    if args.from_step <= step_num:
        steps.append((
            "Group Box Set Films",
            [python, str(SCRIPTS_DIR / "group_box_sets.py")],
            step_num,
        ))

    # Step 9: Scrape box set images (only for entries missing posters)
    step_num += 1
    if args.from_step <= step_num:
        steps.append((
            "Scrape Box Set Images",
            [python, str(SCRIPTS_DIR / "scrape_box_set_images.py")],
            step_num,
        ))

    # Step 10: Enrich via TMDB
    step_num += 1
    if not args.skip_enrich and args.from_step <= step_num:
        steps.append((
            "Enrich via TMDB",
            [python, str(SCRIPTS_DIR / "enrich_tmdb.py")] + pilot_flag + limit_flag,
            step_num,
        ))

    # Step 11: Validate
    step_num += 1
    if not args.skip_validate and args.from_step <= step_num:
        steps.append((
            "Validate Data",
            [python, str(SCRIPTS_DIR / "validate.py")] + pilot_flag,
            step_num,
        ))

    total_steps = len(steps)
    if total_steps == 0:
        log("No steps to run (all skipped)")
        return

    mode = "PILOT (10 guests)" if args.pilot else "FULL"
    log(f"Starting pipeline in {mode} mode: {total_steps} steps")
    overall_start = time.time()

    results = []
    for name, cmd, num in steps:
        success = run_step(name, cmd, num, step_num)
        results.append((name, success))
        if not success:
            log(f"\nStep '{name}' failed. Stopping pipeline.")
            log("Use --from-step to resume from this step after fixing the issue.")
            break

    # Summary
    overall_elapsed = time.time() - overall_start
    log(f"\n{'='*60}")
    log(f"  PIPELINE SUMMARY ({mode})")
    log(f"{'='*60}")
    for name, success in results:
        status = "PASS" if success else "FAIL"
        log(f"  [{status}] {name}")
    log(f"\n  Total time: {overall_elapsed:.1f}s")

    # Exit with failure if any step failed
    if not all(success for _, success in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
