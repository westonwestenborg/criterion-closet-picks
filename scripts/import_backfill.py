#!/usr/bin/env python3
"""Import manually-filled backfill CSVs into the project data files.

Usage:
    python scripts/import_backfill.py photos   # Import guest photos
    python scripts/import_backfill.py quotes   # Import missing quotes
    python scripts/import_backfill.py tmdb     # Import TMDB IDs
    python scripts/import_backfill.py all      # Import all three
"""

import csv
import json
import os
import sys
import urllib.request
import urllib.error

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.join(SCRIPT_DIR, '..')
DATA_DIR = os.path.join(PROJECT_DIR, 'data')
BACKFILL_DIR = os.path.join(DATA_DIR, 'backfill')
PHOTOS_DIR = os.path.join(PROJECT_DIR, 'public', 'photos')


def load_json(filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json(filename, data):
    path = os.path.join(DATA_DIR, filename)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write('\n')


def read_csv(filename):
    path = os.path.join(BACKFILL_DIR, filename)
    if not os.path.exists(path):
        print(f"  ERROR: {path} not found")
        return []
    with open(path, 'r', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def download_photo(url, slug):
    """Download a photo URL to public/photos/{slug}.jpg."""
    os.makedirs(PHOTOS_DIR, exist_ok=True)
    dest = os.path.join(PHOTOS_DIR, f'{slug}.jpg')

    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            with open(dest, 'wb') as f:
                f.write(resp.read())
        return True
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
        print(f"    FAILED to download {url}: {e}")
        return False


def import_photos():
    """Import guest photos from backfill CSV."""
    print("\n--- Importing guest photos ---")
    rows = read_csv('backfill_guest_photos.csv')
    if not rows:
        return

    total = len(rows)
    filled = 0
    downloaded = 0
    failed = 0

    for row in rows:
        url = row.get('photo_url', '').strip()
        if not url:
            continue
        filled += 1
        slug = row['slug']
        print(f"  Downloading photo for {row.get('name', slug)}...")
        if download_photo(url, slug):
            downloaded += 1
        else:
            failed += 1

    print(f"\n  Summary: {total} rows read, {filled} with URLs, "
          f"{downloaded} downloaded, {failed} failed")
    if downloaded > 0:
        print(f"  Photos saved to: {os.path.abspath(PHOTOS_DIR)}")
        print("  Note: The frontend auto-detects these via public/photos/{slug}.jpg")


def import_quotes():
    """Import missing quotes from backfill CSV into picks.json."""
    print("\n--- Importing missing quotes ---")
    rows = read_csv('backfill_missing_quotes.csv')
    if not rows:
        return

    picks = load_json('picks.json')

    # Build lookup: (guest_slug, film_slug, visit_index) -> pick index
    pick_index = {}
    for i, p in enumerate(picks):
        key = (p.get('guest_slug', ''), p.get('film_id', ''), p.get('visit_index', 1))
        pick_index[key] = i

    total = len(rows)
    filled = 0
    updated = 0
    not_found = 0

    for row in rows:
        quote = row.get('quote', '').strip()
        if not quote:
            continue
        filled += 1

        guest_slug = row.get('guest_slug', '').strip()
        film_slug = row.get('film_slug', '').strip()
        try:
            visit_index = int(row.get('visit_index', '1').strip())
        except ValueError:
            visit_index = 1

        key = (guest_slug, film_slug, visit_index)
        idx = pick_index.get(key)
        if idx is None:
            print(f"  WARNING: Pick not found for ({guest_slug}, {film_slug}, visit {visit_index})")
            not_found += 1
            continue

        picks[idx]['quote'] = quote
        picks[idx]['extraction_confidence'] = 'manual'

        # Handle timestamp
        ts = row.get('start_timestamp_seconds', '').strip()
        if ts:
            try:
                ts_int = int(ts)
                picks[idx]['start_timestamp'] = ts_int
                # Update YouTube timestamp URL if there's a video
                yt_base = picks[idx].get('youtube_timestamp_url', '')
                if yt_base:
                    # Strip existing &t= parameter
                    base = yt_base.split('&t=')[0]
                    picks[idx]['youtube_timestamp_url'] = f"{base}&t={ts_int}"
                elif picks[idx].get('guest_slug'):
                    # Try to build from guest's youtube_video_url
                    pass  # Leave as-is if no base URL
            except ValueError:
                print(f"  WARNING: Invalid timestamp '{ts}' for ({guest_slug}, {film_slug})")

        updated += 1

    if updated > 0:
        save_json('picks.json', picks)
        print(f"\n  Summary: {total} rows read, {filled} with quotes, "
              f"{updated} updated, {not_found} not found")
        print("  Updated: data/picks.json")
    else:
        print(f"\n  Summary: {total} rows read, {filled} with quotes, no updates made")


def import_tmdb():
    """Import TMDB IDs from backfill CSV into criterion_catalog.json."""
    print("\n--- Importing TMDB IDs ---")
    rows = read_csv('backfill_film_tmdb.csv')
    if not rows:
        return

    catalog = load_json('criterion_catalog.json')

    # Build lookup: film_slug -> catalog index
    film_index = {}
    for i, f in enumerate(catalog):
        film_index[f.get('film_id', '')] = i

    total = len(rows)
    filled = 0
    updated = 0
    not_found = 0

    for row in rows:
        tmdb_id_str = row.get('tmdb_id', '').strip()
        if not tmdb_id_str:
            continue
        filled += 1

        try:
            tmdb_id = int(tmdb_id_str)
        except ValueError:
            print(f"  WARNING: Invalid tmdb_id '{tmdb_id_str}' for {row.get('film_slug', '?')}")
            continue

        film_slug = row.get('film_slug', '').strip()
        idx = film_index.get(film_slug)
        if idx is None:
            print(f"  WARNING: Film not found in catalog: {film_slug}")
            not_found += 1
            continue

        catalog[idx]['tmdb_id'] = tmdb_id
        updated += 1

    if updated > 0:
        save_json('criterion_catalog.json', catalog)
        print(f"\n  Summary: {total} rows read, {filled} with IDs, "
              f"{updated} updated, {not_found} not found")
        print("  Updated: data/criterion_catalog.json")
        print("  Next step: run 'python scripts/enrich_tmdb.py' to cascade poster/genres/IMDB")
    else:
        print(f"\n  Summary: {total} rows read, {filled} with IDs, no updates made")


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ('photos', 'quotes', 'tmdb', 'all'):
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd in ('photos', 'all'):
        import_photos()
    if cmd in ('quotes', 'all'):
        import_quotes()
    if cmd in ('tmdb', 'all'):
        import_tmdb()

    if cmd == 'all':
        print("\n--- All imports complete ---")
        print("Post-import steps:")
        print("  1. python scripts/enrich_tmdb.py  (if TMDB IDs were added)")
        print("  2. npm run validate")
        print("  3. npm run build")


if __name__ == '__main__':
    main()
