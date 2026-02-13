---
name: update-data
description: Run the Criterion Closet Picks data pipeline to check for new episodes and update the database. Use when user says "update data", "check for new episodes", "run the pipeline", or "weekly update".
---

# Update Criterion Closet Picks Data

Run the data pipeline to discover and process new Closet Picks episodes.

## Prerequisites

- Working directory: the criterion-closet-picks repo root
- Python venv at `.venv/` with dependencies installed (`pip install -r scripts/requirements.txt`)
- `.env` file with TMDB_READ_ACCESS_TOKEN, TMDB_API_KEY, GEMINI_API_KEY
- `cloudscraper` is a required dependency (used to bypass Cloudflare on Criterion.com)

## Workflow

### Step 1: Check for new videos

Run yt-dlp to check the Closet Picks playlist for videos not yet in our data:

```bash
.venv/bin/python -c "
import json, subprocess
from pathlib import Path

# Load processed videos
guests = json.loads(Path('data/guests.json').read_text())
known_ids = {g['youtube_video_id'] for g in guests if g.get('youtube_video_id')}
print(f'Currently tracking {len(known_ids)} videos')

# Check playlist
result = subprocess.run([
    'yt-dlp', '--flat-playlist', '--dump-json', '--no-warnings',
    'https://www.youtube.com/playlist?list=PL7D89754A5DAD1E8E'
], capture_output=True, text=True, timeout=120)

new_videos = []
for line in result.stdout.strip().split('\n'):
    if not line: continue
    v = json.loads(line)
    if v.get('id') not in known_ids:
        new_videos.append({'id': v['id'], 'title': v.get('title', '?')})

if new_videos:
    print(f'\nFound {len(new_videos)} new videos:')
    for v in new_videos:
        print(f'  {v[\"title\"]} ({v[\"id\"]})')
else:
    print('\nNo new videos found. Database is up to date.')
"
```

If no new videos, stop here and tell the user.

### Step 2: Run the pipeline for new videos

If new videos were found, run the full pipeline (incremental — merges into existing data):

```bash
.venv/bin/python scripts/process_all.py --skip-catalog
```

This will:
1. Scrape Criterion.com for new collection pages (+ extract video IDs, handles multi-visit guests)
2. Normalize guest data (merge duplicates, fix names, build visits arrays)
3. Match YouTube videos to guests and fetch transcripts (incl. multi-visit second videos)
4. Extract quotes via Gemini Flash (parallel, 32 workers for throughput)
5. Backfill films, group box sets, scrape box set images
6. Migrate source/visit metadata
7. Enrich new films/guests via TMDB
8. Normalize guests (second pass), migrate again
9. Run validation

Note: The pipeline is incremental by default. It only re-scrapes multi-visit guest URLs (to keep visit_index accurate) and skips already-processed collections. Use `--fresh` flag to start from scratch.

### Step 3: Rebuild the site

```bash
npm run build && npx pagefind --site dist
```

### Step 4: Report results

After the pipeline completes, report:
- How many new guests were added
- How many new picks were extracted
- Quote extraction confidence for new entries
- Any validation issues
- Multi-visit guest attribution (from migrate_source_visit output)

Ask the user if they want to commit and push the updated data files.

### Step 5: Commit (if approved)

If the user approves:

```bash
git add data/guests.json data/picks.json data/picks_raw.json data/criterion_catalog.json data/validation/
git commit -m "Update data: add N new episodes (YYYY-MM-DD)"
git push
```

## Notes

- **Criterion.com is the sole data source** (Letterboxd dropped Feb 2025)
- The pipeline is idempotent: re-running won't duplicate data
- Checkpoint files in `data/` track progress for resume capability
- Multi-visit guests (Bill Hader, Guillermo del Toro, etc.) are handled automatically via `VISIT_CRITERION_URLS` config in `scripts/utils.py`
- If Gemini quota is exhausted, use `--skip-quotes` and run quote extraction later
- For parallel extraction: `python scripts/extract_quotes_parallel.py --workers 32`
- For a single video: `python scripts/process_video.py --youtube-url "URL"`
