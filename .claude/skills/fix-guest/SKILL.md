---
name: fix-guest
description: Targeted fixes for individual Criterion Closet Picks guests without running the full pipeline. Use when user says "fix guest", "assign video", "add criterion url", "re-extract quotes", or needs to update a specific guest's data (video ID, Criterion URL, quotes). Also use when manually linking a YouTube video to a guest, correcting a wrong video match, or adding a Criterion collection page URL.
---

# Fix Guest Data

Apply targeted fixes to individual guests without running the full 12-step pipeline.

## Prerequisites

- Working directory: criterion-closet-picks repo root
- Python venv at `.venv/`
- `.env` file with API keys (GEMINI_API_KEY for quote extraction, TMDB keys for enrichment)
- Optional: X/Twitter and Threads credentials in `.env` (for posting about new guests)

## Config Locations

Fixes involve these config dicts:

**`scripts/normalize_guests.py`:**
- `KNOWN_VIDEO_IDS` — manually assign YouTube video IDs
- `WRONG_VIDEO_FIXES` — null out incorrectly matched video IDs
- `KNOWN_CRITERION_URLS` — set `criterion_page_url` for guests

**`scripts/utils.py`:**
- `VISIT_CRITERION_URLS` — Criterion collection URLs (injected into scraper when not on index)
- `EXCLUDED_VIDEO_IDS` — non-guest YouTube videos to ignore

## Workflows

### 1. Assign a YouTube video to a guest

1. Find guest slug: `jq '.[] | select(.name | test("NAME")) | .slug' data/guests.json`
2. If guest has a wrong video, add to `WRONG_VIDEO_FIXES`: `"slug": "WRONG_ID"`
3. Add to `KNOWN_VIDEO_IDS`: `"slug": {"youtube_video_id": "CORRECT_ID"}`
4. Run:
   ```bash
   .venv/bin/python scripts/normalize_guests.py
   .venv/bin/python scripts/extract_quotes.py --guest-slug SLUG --force
   ```

### 2. Add a Criterion collection URL

1. Add to `KNOWN_CRITERION_URLS` in `normalize_guests.py`
2. Add to `VISIT_CRITERION_URLS` in `utils.py`
3. Run: `.venv/bin/python scripts/normalize_guests.py`

### 3. Re-extract quotes for a guest

```bash
.venv/bin/python scripts/extract_quotes.py --guest-slug SLUG --force
```

For a specific visit: add `--visit 2`

### 4. Scrape picks from a new Criterion collection page

1. Add URL to `VISIT_CRITERION_URLS` in `utils.py`
2. Clear checkpoint if URL was previously attempted:
   ```bash
   .venv/bin/python -c "
   import json; p='data/.criterion_scrape_progress.json'
   d=json.loads(open(p).read())
   d['completed_urls']=[u for u in d['completed_urls'] if 'COLLECTION_ID' not in u]
   json.dump(d,open(p,'w'),indent=2)"
   ```
3. Run:
   ```bash
   .venv/bin/python scripts/scrape_criterion_picks.py
   .venv/bin/python scripts/normalize_guests.py
   ```

### 5. Exclude a non-guest YouTube video

Add to `EXCLUDED_VIDEO_IDS` in `utils.py` with a descriptive comment.

### 6. Post about a new guest

After adding a new guest, committing, and **pushing** the data (so the guest page URL works), compose a post:

```bash
.venv/bin/python scripts/post_new_guests.py --dry-run --guest-slug SLUG
```

Show the proposed post text to the user. If they want to edit it, work with them
to adjust the text, then run:

```bash
.venv/bin/python scripts/post_new_guests.py --guest-slug SLUG --text "FINAL TEXT HERE"
```

Or if the dry-run text is approved as-is:

```bash
.venv/bin/python scripts/post_new_guests.py --guest-slug SLUG
```

Skip this step if the user is fixing an existing guest (not adding a new one),
or if no X/Twitter or Threads credentials are configured in `.env`.

### 7. Correct a guest's profession / descriptor

`profession` is auto-set by `enrich_tmdb.py` from TMDB's `known_for_department`
via `DEPARTMENT_MAP`. It is a **single-word controlled vocabulary** — only these
values are used site-wide: `actor`, `director`, `writer`, `musician`, `producer`,
`cinematographer`, `editor`, `other`. Do **not** invent multi-role labels like
"writer-director" or "filmmaker"; they break the existing pattern (and the social
post template + guest-page display assume a single word).

TMDB often tags a guest by the role they're most credited for, which can
misrepresent how they're known (e.g. John Cameron Mitchell → "actor" when
"director" fits better). To correct it, edit the value directly in
`data/guests.json` for that guest's slug:

```bash
.venv/bin/python -c "
import sys; sys.path.insert(0,'scripts')
from utils import load_json, save_json, GUESTS_FILE
g=load_json(GUESTS_FILE)
for x in g:
    if x['slug']=='SLUG': x['profession']='director'
save_json(GUESTS_FILE, g)"
```

Manual edits persist: `enrich_tmdb.py` never overwrites a profession that is
already set. Pick the single vocabulary value that best matches how the guest is
publicly known. Rebuild after the change so the guest page reflects it.

## Key Details

- Fix application order: `WRONG_VIDEO_FIXES` -> `KNOWN_VIDEO_IDS` -> `KNOWN_CRITERION_URLS`
- Always run `normalize_guests.py` before `extract_quotes.py` to ensure video IDs are set
- `--force` re-extracts even if the checkpoint says the guest is already processed
- New guest (no transcript on disk yet): after `normalize_guests.py` sets the video
  ID, fetch the transcript before extracting quotes — `extract_quotes.py` reads
  `data/transcripts/{video_id}.json` and does not fetch it. Use
  `match_youtube.fetch_transcript(video_id)` and save `{video_id, guest_name, segments}`.
- `profession` is a single-word controlled vocabulary (see Workflow 7) — never multi-role
- Build after fixes: `npm run build && npx pagefind --site dist`
- Use `update-data` skill instead for weekly new-episode checks (full pipeline)
