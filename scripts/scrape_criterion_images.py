#!/usr/bin/env python3
"""
Scrape Criterion.com product images as primary poster source.

For all catalog entries with a criterion_url, fetches the product image
and sets it as the poster_url with poster_source="criterion".

Replaces TMDB posters with higher-quality Criterion product images.
For films without criterion_url, existing TMDB posters are preserved
with poster_source="tmdb".

Usage:
  python scripts/scrape_criterion_images.py --dry-run   # Preview
  python scripts/scrape_criterion_images.py              # Scrape and save
  python scripts/scrape_criterion_images.py --force      # Re-scrape all
  python scripts/scrape_criterion_images.py --limit 10   # First 10 only
"""

import argparse
import sys

from tqdm import tqdm

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import (
    CATALOG_FILE,
    load_json,
    save_json,
    log,
)
from scripts.enrich_tmdb import get_metadata_from_criterion_url


def main():
    parser = argparse.ArgumentParser(description="Scrape Criterion product images")
    parser.add_argument("--dry-run", action="store_true", help="Preview without saving")
    parser.add_argument("--force", action="store_true", help="Re-scrape all, even existing Criterion images")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of entries to scrape")
    args = parser.parse_args()

    catalog = load_json(CATALOG_FILE)

    # Tag existing posters that have no poster_source
    tagged_count = 0
    for entry in catalog:
        if entry.get("poster_url") and not entry.get("poster_source"):
            entry["poster_source"] = "tmdb"
            tagged_count += 1

    if tagged_count:
        log(f"Tagged {tagged_count} existing posters as tmdb")

    # Find entries to scrape
    already_criterion = 0
    to_scrape = []
    for entry in catalog:
        if not entry.get("criterion_url"):
            continue
        if entry.get("poster_source") == "criterion":
            if args.force:
                to_scrape.append(entry)
            else:
                already_criterion += 1
        else:
            to_scrape.append(entry)

    if args.limit > 0:
        to_scrape = to_scrape[:args.limit]

    log(f"Scraping Criterion images...")
    log(f"  Found {len(to_scrape)} entries to scrape ({already_criterion} already have Criterion images)")

    if args.dry_run:
        for entry in to_scrape:
            log(f"  Would fetch: {entry.get('film_id', 'unknown')} -> {entry['criterion_url']}")
        log("(dry run)")
        if tagged_count:
            save_json(CATALOG_FILE, catalog)
            log(f"Saved {CATALOG_FILE} (poster_source tags only)")
        return

    found = 0
    failed = 0

    for entry in tqdm(to_scrape, desc="Scraping", disable=not sys.stdout.isatty()):
        metadata = get_metadata_from_criterion_url(entry["criterion_url"])

        if metadata and metadata.get("image_url"):
            entry["poster_url"] = metadata["image_url"]
            entry["poster_source"] = "criterion"
            found += 1
        else:
            failed += 1

    log(f"\nDone: {found} images found, {failed} failed")
    if tagged_count:
        log(f"  Tagged {tagged_count} existing posters as tmdb")

    if found > 0 or tagged_count > 0:
        save_json(CATALOG_FILE, catalog)
        log(f"Saved {CATALOG_FILE}")


if __name__ == "__main__":
    main()
