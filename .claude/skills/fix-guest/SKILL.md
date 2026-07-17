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

**`data/visit_criterion_urls.json`** (loaded by `utils.py` as `VISIT_CRITERION_URLS`):
- Criterion collection URLs per guest slug (injected into scraper when not on index)

**`data/excluded_video_ids.json`** (loaded by `utils.py` as `EXCLUDED_VIDEO_IDS`):
- non-guest YouTube videos to ignore, as `"video_id": "note"` pairs

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
2. Add to `data/visit_criterion_urls.json`
3. Run: `.venv/bin/python scripts/normalize_guests.py`

### 3. Re-extract quotes for a guest

```bash
.venv/bin/python scripts/extract_quotes.py --guest-slug SLUG --force
```

For a specific visit: add `--visit 2`

### 4. Scrape picks from a new Criterion collection page

1. Add URL to `data/visit_criterion_urls.json`
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

Add to `data/excluded_video_ids.json` as `"video_id": "descriptive note"`.

### 6. Post about a new guest

After adding a new guest, committing, and **pushing** the data (so the guest page URL works), compose a post.

**First, find and verify the guest's social handles (tag > name).** A tagged
guest may see, like, or reshare the post — reach you can't buy — so tagging the
real account is the single highest-leverage part of the post. But a wrong tag
amplifies an impersonator into a celebrity's feed, so the bar is verification,
not a guess:

1. Search for the guest's **X** and **Threads** handles (Threads = their
   Instagram handle). Use web search; check their Linktree / official site.
2. **Confirm against a primary source** — a verified badge, a link from their
   official site/Wikipedia, or the account self-identifying. Beware decoys: the
   more "official"-sounding handle is often the fake (FINNEAS publicly warns that
   `@finneasofficial` is *not* him; the real one is `@finneas`). If you can't
   confirm, leave the handle unset — the post falls back to `Name (profession)`.
3. Store confirmed handles (no leading `@`) on the guest in `data/guests.json`;
   they're per-platform because X and Threads handles diverge:

   ```bash
   .venv/bin/python -c "
   import sys; sys.path.insert(0,'scripts')
   from utils import load_json, save_json, GUESTS_FILE
   g=load_json(GUESTS_FILE)
   for x in g:
       if x['slug']=='SLUG':
           x['x_handle']='HANDLE'          # omit/skip if not verified
           x['threads_handle']='HANDLE'    # omit/skip if not verified
   save_json(GUESTS_FILE, g)"
   ```

   `post_new_guests.py` then renders `Name (@handle)` per platform (dropping the
   profession, since the tag identifies them). Do this **before** the commit so
   the handles ship with the guest.

Then compose a post. **Lead with a quote, not a list of titles.** Criterion's
own posts are "✨{Name}'s Closet Picks!✨ + video link" — they lead with the
video because they own it. We don't have the video; our differentiator is the
extracted quotes and the searchable database. A quote-led post is the one thing
Criterion structurally doesn't do, and it gives the tagged guest something
flattering-and-specific to reshare. So the default template leads with the
guest's strongest quote; **don't fall back to the title list** unless the guest
genuinely has no usable quote (e.g. a Vimeo-only guest with no transcript).

Every post is hand-curated in the session — the script never auto-picks a
punchy line, because good pull-quotes come from human judgment, not truncation.
The process:

```bash
.venv/bin/python scripts/post_new_guests.py --dry-run --guest-slug SLUG
```

The dry-run prints a quote-led draft (using the guest's featured/best quote,
trimmed to fit) **plus a "Candidate quotes" menu** — every pick's full quote,
strongest first. Read the candidates, pick the one that's most compelling and
most *this guest* (the line they'd be proud to see quoted back), and cut a tight
pull-quote from it — a short verbatim phrase, not the whole 400-char quote.
Favor the punchiest cut; add just enough context that the line lands on its own.
Recommend one to the user with your reasoning, then post the approved text:

```bash
.venv/bin/python scripts/post_new_guests.py --guest-slug SLUG --text "FINAL TEXT HERE"
```

`--text` posts the same text to both platforms; guest @handles resolve
case-insensitively on X and Threads, so a single tag works for both. Keep the
shape used for John Leguizamo:

```
{Name} (@{handle}) on {Film}:

"{tight pull-quote}"

See all {N} picks: closetpicks.westenb.org/guests/{slug}/
```

Or, only if the dry-run's auto-trimmed draft is already good, approve it as-is:

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

### 8. Curate a guest's home-page quote (optional)

The home page shows the most recent guests, each with their **best pick** — a
verbatim quote surfaced next to their name. By default `getBestPickForGuest()`
(in `src/lib/data.ts`) picks it heuristically: highest extraction confidence,
then a quote length near ~200 characters. That's a *defensible* quote, not
always the *standout* one.

To hand-pick a guest's home-page quote, set `featured_film_slug` on that guest
in `data/guests.json` to the `film_slug`/`film_id` of the pick whose quote you
want surfaced. The home page prefers it over the heuristic; leave it unset to
fall back. Only worth doing for guests likely to appear in the recent set.

```bash
.venv/bin/python -c "
import sys; sys.path.insert(0,'scripts')
from utils import load_json, save_json, GUESTS_FILE
g=load_json(GUESTS_FILE)
for x in g:
    if x['slug']=='SLUG': x['featured_film_slug']='the-red-shoes'
save_json(GUESTS_FILE, g)"
```

Purely editorial — it never affects data integrity, only which quote leads on
the home page. Rebuild after the change.

## Key Details

- Fix application order: `WRONG_VIDEO_FIXES` -> `KNOWN_VIDEO_IDS` -> `KNOWN_CRITERION_URLS`
- Always run `normalize_guests.py` before `extract_quotes.py` to ensure video IDs are set
- `--force` re-extracts even if the checkpoint says the guest is already processed
- New guest (no transcript on disk yet): after `normalize_guests.py` sets the video
  ID, fetch the transcript before extracting quotes — `extract_quotes.py` reads
  `data/transcripts/{video_id}.json` and does not fetch it. Use
  `match_youtube.fetch_transcript(video_id)` and save `{video_id, guest_name, segments}`.
- Non-English guest (only a foreign-language transcript, e.g. a Spanish-speaking
  director): `fetch_transcript` returns nothing (it only tries English), so **leave
  no transcript file on disk** — `extract_quotes.py --guest-slug SLUG --force` then
  auto-routes the guest to its audio fallback (yt-dlp downloads audio → Gemini
  transcribes+translates). Quotes come back prefixed `[Translated]`. Requires
  `yt-dlp` on PATH. This is the established path for non-English guests.
- Accented names: do **not** add a `NAME_FIXES` entry just to restore an accent.
  `enrich_tmdb.py` restores diacritics from TMDB (e.g. "Carla Simon" → "Carla
  Simón") and `normalize_guests.py` (step 8b) NFC-normalizes the guest name and
  syncs it onto every pick by slug. `NAME_FIXES` is only for non-accent fixes
  (typos, garbled overlay text). See CLAUDE.md → Key Conventions → "Guest names".
- `profession` is a single-word controlled vocabulary (see Workflow 7) — never multi-role
- Build after fixes: `npm run build && npx pagefind --site dist`
- Use `update-data` skill instead for weekly new-episode checks (full pipeline)
