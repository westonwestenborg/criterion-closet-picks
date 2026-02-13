#!/usr/bin/env python3
"""
Download guest photos from a manual URL manifest.

Reads data/manual_photos.json (slug -> URL mapping), downloads each image,
and saves to public/photos/{slug}.jpg for local photo fallback in the frontend.

Usage:
  python scripts/import_photos.py              # Download all missing photos
  python scripts/import_photos.py --force      # Re-download all photos
  python scripts/import_photos.py --check      # Show which guests still need photos
"""

import argparse
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.utils import DATA_DIR, GUESTS_FILE, load_json, log

MANIFEST_FILE = DATA_DIR / "manual_photos.json"
PHOTOS_DIR = Path(__file__).resolve().parent.parent / "public" / "photos"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) CriterionClosetPicks/1.0"
}


def download_photo(url: str, dest: Path) -> bool:
    """Download a photo from URL to dest path. Returns True on success."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30, stream=True)
        if resp.status_code != 200:
            log(f"  HTTP {resp.status_code} for {url}")
            return False

        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as e:
        log(f"  Error downloading {url}: {e}")
        return False


def check_missing_photos():
    """Report guests without photos."""
    guests = load_json(GUESTS_FILE)
    manifest = load_json(MANIFEST_FILE) if MANIFEST_FILE.exists() else {}

    no_photo = []
    for g in guests:
        has_tmdb = bool(g.get("photo_url"))
        has_local = (PHOTOS_DIR / f"{g['slug']}.jpg").exists()
        has_manifest = g["slug"] in manifest
        if not has_tmdb and not has_local and not has_manifest:
            no_photo.append(g)

    log(f"Guests without any photo source: {len(no_photo)}/{len(guests)}")
    for g in sorted(no_photo, key=lambda x: x["name"]):
        log(f"  {g['slug']}: {g['name']}")

    return no_photo


def main():
    parser = argparse.ArgumentParser(description="Import guest photos from URL manifest")
    parser.add_argument("--force", action="store_true", help="Re-download existing photos")
    parser.add_argument("--check", action="store_true", help="Show guests missing photos")
    args = parser.parse_args()

    if args.check:
        check_missing_photos()
        return

    if not MANIFEST_FILE.exists():
        log(f"No manifest file at {MANIFEST_FILE}")
        log("Create data/manual_photos.json with format: {\"guest-slug\": \"https://photo-url\"}")
        return

    manifest = load_json(MANIFEST_FILE)
    if not isinstance(manifest, dict):
        log("ERROR: manual_photos.json should be a JSON object (slug -> URL)")
        return

    log(f"Loaded {len(manifest)} entries from {MANIFEST_FILE}")

    downloaded = 0
    skipped = 0
    failed = 0

    for slug, url in manifest.items():
        dest = PHOTOS_DIR / f"{slug}.jpg"

        if dest.exists() and not args.force:
            log(f"  {slug}: already exists (use --force to re-download)")
            skipped += 1
            continue

        log(f"  Downloading: {slug}")
        if download_photo(url, dest):
            log(f"    Saved to {dest}")
            downloaded += 1
        else:
            failed += 1

    log(f"\nResults: {downloaded} downloaded, {skipped} skipped, {failed} failed")


if __name__ == "__main__":
    main()
