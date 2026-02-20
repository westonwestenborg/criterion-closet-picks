#!/usr/bin/env python3
"""Generate CSV files for manual data backfill.

Creates 3 CSVs in data/backfill/:
  - backfill_guest_photos.csv (guests missing photo_url)
  - backfill_missing_quotes.csv (picks missing quotes)
  - backfill_film_tmdb.csv (films missing tmdb_id)

Each CSV has contextual read-only columns for reference and empty columns to fill in.
"""

import csv
import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, '..', 'data')
BACKFILL_DIR = os.path.join(DATA_DIR, 'backfill')


def load_json(filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def generate_guest_photos(guests, picks):
    """Generate CSV for guests missing photo_url."""
    missing = [g for g in guests if not g.get('photo_url')]
    # Sort by pick_count descending (highest impact first)
    missing.sort(key=lambda g: g.get('pick_count', 0), reverse=True)

    path = os.path.join(BACKFILL_DIR, 'backfill_guest_photos.csv')
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            'slug', 'name', 'pick_count', 'criterion_url', 'youtube_url',
            'photo_url'
        ])
        for g in missing:
            youtube_url = g.get('youtube_video_url') or ''
            criterion_url = g.get('criterion_page_url') or ''
            writer.writerow([
                g['slug'],
                g['name'],
                g.get('pick_count', 0),
                criterion_url,
                youtube_url,
                ''  # photo_url — to fill
            ])

    print(f"  backfill_guest_photos.csv: {len(missing)} rows")
    return len(missing)


def generate_missing_quotes(guests, picks):
    """Generate CSV for picks missing quotes."""
    # Build guest lookup for YouTube URLs
    guest_lookup = {}
    for g in guests:
        visits = g.get('visits', [])
        guest_lookup[g['slug']] = {
            'youtube_url': g.get('youtube_video_url') or '',
            'has_video': bool(g.get('youtube_video_id') or g.get('vimeo_video_id')),
            'visits': {
                v.get('visit_index', i + 1): v.get('youtube_video_url') or g.get('youtube_video_url') or ''
                for i, v in enumerate(visits)
            } if visits else {}
        }

    missing = [p for p in picks if not p.get('quote')]
    # Sort by guest_name, then film_title for easy batch filling
    missing.sort(key=lambda p: (p.get('guest_name', ''), p.get('visit_index', 1), p.get('film_title', '')))

    path = os.path.join(BACKFILL_DIR, 'backfill_missing_quotes.csv')
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            'guest_slug', 'guest_name', 'film_slug', 'film_title',
            'visit_index', 'is_box_set', 'youtube_url', 'has_video',
            'quote', 'start_timestamp_seconds'
        ])
        for p in missing:
            slug = p.get('guest_slug', '')
            visit_idx = p.get('visit_index', 1)
            gl = guest_lookup.get(slug, {})

            # Try visit-specific YouTube URL, fall back to main
            youtube_url = gl.get('visits', {}).get(visit_idx, gl.get('youtube_url', ''))

            writer.writerow([
                slug,
                p.get('guest_name', ''),
                p.get('film_id', ''),
                p.get('film_title', ''),
                visit_idx,
                str(p.get('is_box_set', False)),
                youtube_url,
                str(gl.get('has_video', False)),
                '',  # quote — to fill
                ''   # start_timestamp_seconds — to fill
            ])

    print(f"  backfill_missing_quotes.csv: {len(missing)} rows")
    return len(missing)


def generate_film_tmdb(catalog, picks):
    """Generate CSV for films missing tmdb_id."""
    # Count how many times each film is picked
    pick_counts = {}
    for p in picks:
        fid = p.get('film_id', '')
        if fid:
            pick_counts[fid] = pick_counts.get(fid, 0) + 1

    missing = [f for f in catalog if not f.get('tmdb_id')]
    # Sort by times_picked descending (highest impact first)
    missing.sort(key=lambda f: pick_counts.get(f.get('film_id', ''), 0), reverse=True)

    path = os.path.join(BACKFILL_DIR, 'backfill_film_tmdb.csv')
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            'film_slug', 'title', 'year', 'spine_number', 'criterion_url',
            'is_box_set', 'times_picked',
            'tmdb_id'
        ])
        for film in missing:
            fid = film.get('film_id', '')
            writer.writerow([
                fid,
                film.get('title', ''),
                film.get('year', 0),
                film.get('spine_number') or '',
                film.get('criterion_url', ''),
                str(bool(film.get('box_set_film_count'))),
                pick_counts.get(fid, 0),
                ''  # tmdb_id — to fill
            ])

    print(f"  backfill_film_tmdb.csv: {len(missing)} rows")
    return len(missing)


def main():
    os.makedirs(BACKFILL_DIR, exist_ok=True)

    print("Loading data files...")
    guests = load_json('guests.json')
    picks = load_json('picks.json')
    catalog = load_json('criterion_catalog.json')

    print(f"  {len(guests)} guests, {len(picks)} picks, {len(catalog)} films\n")

    print("Generating backfill CSVs:")
    n_photos = generate_guest_photos(guests, picks)
    n_quotes = generate_missing_quotes(guests, picks)
    n_tmdb = generate_film_tmdb(catalog, picks)

    print(f"\nTotal: {n_photos + n_quotes + n_tmdb} rows across 3 CSVs")
    print(f"Output directory: {os.path.abspath(BACKFILL_DIR)}")


if __name__ == '__main__':
    main()
