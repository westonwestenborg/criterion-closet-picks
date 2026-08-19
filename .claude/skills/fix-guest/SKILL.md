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

For a specific visit: add `--visit 2`. That reads **that visit's own video
transcript** and tags the results with that `visit_index`. Before 2026-08-18 it
read the guest's primary (visit-1) transcript and tagged everything
`visit_index: 1`, which scored visit-1 speech against visit-2 films and created
duplicate visit-1 pick records — if you see a guest with two picks sharing a
`pick_order`, that is the fingerprint. `picks_raw.json` is authoritative for
which (visit, film) pairs should exist; anything in `picks.json` that is not in
it is spurious.

A multi-visit guest whose **visit 1 has no transcript** (e.g. Bill Hader's 2011
episode) routes to the audio fallback and processes no visit-2 picks in the
default run — those need the explicit `--visit 2`

**If the guest picked a box set, follow it with `group_box_sets.py`:**

```bash
.venv/bin/python scripts/group_box_sets.py
```

`extract_quotes.py` rebuilds each pick record and knows nothing about box sets —
`box_set_criterion_url` and `box_set_film_count` are added later in the pipeline
by `group_box_sets.py`. Re-extracting on its own therefore drops those fields,
and `box_set_name` reverts to the raw scraped spelling. Nothing errors and
`bun run validate` still passes, so the loss is only visible in a `git diff`.
Re-running `group_box_sets.py` restores them.

Check first with:

```bash
.venv/bin/python -c "
import sys; sys.path.insert(0,'scripts')
from utils import load_json
print([p['film_title'] for p in load_json('data/picks.json')
       if p['guest_slug']=='SLUG' and p.get('is_box_set')])"
```

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

For a **brand-new guest** the scrape is only the first half. The guest lands
with `youtube_video_id: null`, and quote extraction reads a transcript file that
nothing has written yet, so it silently produces no quotes. Continue:

4. Add the video ID to `KNOWN_VIDEO_IDS` and the collection URL to
   `KNOWN_CRITERION_URLS` in `normalize_guests.py` (keyed by the slug the scrape
   just created), then re-run `normalize_guests.py`.
5. Fetch the transcript to disk — `extract_quotes.py` reads
   `data/transcripts/{video_id}.json` and never fetches it:
   ```bash
   .venv/bin/python -c "
   import sys, json; sys.path.insert(0,'scripts')
   from match_youtube import fetch_transcript
   from pathlib import Path
   vid, name = 'VIDEO_ID', 'Guest Name'
   segs = fetch_transcript(vid)
   print(vid, '->', len(segs) if segs else 0, 'segments')
   if segs:
       Path('data/transcripts', f'{vid}.json').write_text(
           json.dumps({'video_id': vid, 'guest_name': name, 'segments': segs}, indent=2))"
   ```
   If it returns 0 segments the guest is non-English — leave **no** file on disk
   and let `extract_quotes.py` route to its audio fallback (see Key Details).
6. Run the rest of the pipeline tail, in this order:
   ```bash
   .venv/bin/python scripts/backfill_dates.py
   .venv/bin/python scripts/extract_quotes.py --guest-slug SLUG --force
   .venv/bin/python scripts/backfill_films.py
   .venv/bin/python scripts/group_box_sets.py
   .venv/bin/python scripts/scrape_box_set_images.py
   .venv/bin/python scripts/migrate_source_visit.py
   .venv/bin/python scripts/enrich_tmdb.py
   .venv/bin/python scripts/normalize_guests.py
   bun run validate
   ```
   If validate fails on a non-canonical `film_title` — common when a guest picks
   a box set whose catalog title carries a ` (box set)` suffix the collection
   page omits — run `scripts/dedupe_film_ids.py --dry-run`, confirm the scope is
   just the new picks, then run it for real.

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

`--text` posts the same text to both platforms. That is only safe when the
guest's `x_handle` and `threads_handle` are the **same string** — handles
resolve case-insensitively, but nothing more. When they differ, one `--text`
run tags a nonexistent account on one of the two platforms. Eric André is
`ericandre` on X and `ericfuckingandre` on Threads, so he needed two runs:

```bash
.venv/bin/python scripts/post_new_guests.py --guest-slug SLUG --twitter-only --text "... (@x_handle) ..."
.venv/bin/python scripts/post_new_guests.py --guest-slug SLUG --threads-only --text "... (@threads_handle) ..."
```

Check the two handles against each other before choosing one run or two. Keep
the shape used for John Leguizamo:

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

**Optionally also reply to Criterion's own announcement — but this one is
manual.** The standalone post stays primary either way: it's the canonical,
reshareable artifact that gets full algorithmic treatment. But people reading
Criterion's thread for that guest are the highest-intent audience there is, so a
reply is worth a second, separate placement. Don't quote-post instead — that
embeds Criterion's video card and subordinates our quotes to the asset we don't
own.

**Our X API tier cannot post this reply.** Replying to a third party 403s with
"You can only reply to or quote posts where you are mentioned or are the
author" (confirmed 2026-07-27 against Criterion's Matt Damon post). Draft the
text here, then hand it to the user to paste in the X app. The same tier can't
read timelines either (401), so find Criterion's post URL by web search and
sanity-check it before trusting it — decode the snowflake ID
(`timestamp_ms = (id >> 22) + 1288834974657`) and confirm the post-time matches
the episode's air date. A wrong ID puts our reply in a stranger's thread.

The `--reply-to-x` / `--reply-to-threads` flags exist and work for the cases the
tier does allow — replying to our **own** posts (threading) or to posts that
mention us:

```bash
.venv/bin/python scripts/post_new_guests.py --guest-slug SLUG --text "..." \
    --reply-to-x https://x.com/weston_w/status/1234567890123456789
```

`--reply-to-x` takes a tweet URL or ID. `--reply-to-threads` takes a **numeric
post ID only** — a Threads permalink's shortcode encodes a different ID than the
Graph API addresses posts by, so URLs are rejected rather than silently
mis-resolved. In practice that ID is only obtainable for our own posts, which is
the same constraint. Either flag can be used alone. Both are validated before
anything is sent, so a bad target can't leave a standalone post on one platform.

### 7. Correct a guest's profession / descriptor

`profession` is auto-set by `enrich_tmdb.py` from TMDB's `known_for_department`
via `DEPARTMENT_MAP`. It must be a **single word** — the guest-page label and the
social-post template both assume one. Do **not** invent multi-role labels like
"writer-director" or "filmmaker".

Beyond that, be as precise as the person deserves. The value set is open:
`actor`, `director`, `writer`, `musician`, `producer`, `cinematographer`,
`editor`, `designer`, `host`, `photographer`, `animator`, `philosopher`, `other`
are in use, and adding one needs no frontend change. A profession only earns a
filter chip once `PROFESSION_FACET_MIN` guests share it (5, in `src/lib/data.ts`);
below that the guests index folds it into the **other** chip while the guest's own
page still shows the precise label. So a one-guest label costs nothing — it never
becomes a dead-end filter button. A deep link (`?profession=photographer`) keeps
working either way, because each row carries both its own value and `other`.

Reserve `other` for guests who genuinely have no single profession — the group
entries like Five Comics and The Wolfpack. It renders literally as "other" under
the name, so never use it as a shrug for someone whose job simply isn't mapped:
`Costume & Make-Up` and `Art` were missing from `DEPARTMENT_MAP` for a long time,
which is how two Oscar-winning costume designers came out unlabelled. Do **not** invent multi-role labels like
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

### 9. Correct a pick that Criterion's own page gets wrong

Criterion.com is the sole primary source for picks, so this is the one workflow
that deliberately contradicts it. The bar is evidence from the video itself —
a transcript line naming the film, or naming something only that film has (a
writer, a director, a plot detail). "The video looks like a different film" is
not enough on its own.

Corrections live in `data/pick_overrides.json`, keyed by guest slug:

```json
"slavoj-zizek": [
  {
    "from_criterion_url": "https://www.criterion.com/films/252-stranger-than-paradise",
    "to_criterion_url":   "https://www.criterion.com/films/336-the-ice-storm",
    "to_film_id": "the-ice-storm",
    "to_title": "The Ice Storm",
    "note": "Why Criterion is wrong, with the transcript evidence."
  }
]
```

`scrape_criterion_picks.py` re-applies them after every scrape (via
`apply_pick_overrides()` in `utils.py`), so a re-scrape cannot silently revert
the correction. The override clears the quote, because the quote that was there
belonged to the film being replaced — re-extract afterwards:

```bash
.venv/bin/python -c "
import sys; sys.path.insert(0,'scripts')
from utils import load_json, save_json, PICKS_FILE, PICKS_RAW_FILE, apply_pick_overrides
for f in (PICKS_FILE, PICKS_RAW_FILE):
    d = load_json(f); n = apply_pick_overrides(d); save_json(f, d)
    print(f.name, n)"
.venv/bin/python scripts/extract_quotes.py --guest-slug SLUG --force
.venv/bin/python scripts/group_box_sets.py
```

The `note` field is the whole point of the file — it is the only record of why
we disagree with the primary source. Write it for someone who will read it in a
year with no memory of the episode.

## Key Details

- Fix application order: `WRONG_VIDEO_FIXES` -> `KNOWN_VIDEO_IDS` -> `KNOWN_CRITERION_URLS`
- Always run `normalize_guests.py` before `extract_quotes.py` to ensure video IDs are set
- `--force` re-extracts even if the checkpoint says the guest is already processed
- New guest (no transcript on disk yet): after `normalize_guests.py` sets the video
  ID, fetch the transcript before extracting quotes — `extract_quotes.py` reads
  `data/transcripts/{video_id}.json` and does not fetch it. Use
  `match_youtube.fetch_transcript(video_id)` and save `{video_id, guest_name, segments}`.
  Workflow 4 has the full new-guest sequence; do not stop at the scrape.
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
