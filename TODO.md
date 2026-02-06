# Criterion Closet Picks - Open Tasks

## 1. Clean up extracted quotes
The Gemini quote extraction produces raw auto-caption text. Post-process quotes to:
- Capitalize properly (sentence case, proper nouns like film titles and director names)
- Fix bad transcriptions (e.g. "my Winnipeg" → "My Winnipeg", known film title corrections)
- Remove filler words (um, uh, like) when they don't add meaning
- This should be a post-processing step in `scripts/extract_quotes.py` or a new `scripts/clean_quotes.py` that runs after extraction

**Example:** `"my Winnipeg where is that but have you seen my Winnipeg"` should become `"My Winnipeg — where is that? But have you seen My Winnipeg..."`

**Files:** `scripts/extract_quotes.py`, possibly new `scripts/clean_quotes.py`, `data/picks.json`

## 2. Make the quote itself the clickable timestamp link
Currently there's a separate "Watch [name] talk about this film" link below each quote. Instead, make the blockquote text itself be the link to the YouTube timestamp. Remove the separate link line.

**Files:** `src/components/QuoteBlock.astro`, `src/pages/guests/[slug].astro`, `src/pages/films/[slug].astro`, `src/styles/global.css`

## 3. Box set handling: link to box set, not individual films
When a guest picks a box set (e.g. Bergman's Cinema, Five Films by Cassavetes):
- Show ONE entry for the box set, not 39 individual films
- Link to the box set's Criterion page
- If the guest specifically discusses an individual film within the set, call that out separately with its own quote and link to the individual film
- This requires changes to both the data pipeline (grouping box set films) and frontend (rendering grouped picks)

**Files:** `scripts/scrape_letterboxd.py` (box set detection), `src/lib/data.ts` (grouping logic), `src/pages/guests/[slug].astro`

## 4. Remove quote attribution on guest pages
On guest detail pages (e.g. `/guests/cate-blanchett/`), the attribution line "Cate Blanchett on My Winnipeg" is redundant — you already know who's speaking. Remove it. Keep attribution on film detail pages where multiple guests are quoted.

**Files:** `src/pages/guests/[slug].astro`, `src/components/QuoteBlock.astro` (add an optional `hideAttribution` prop)

## 5. Link to Criterion site on film pages
Film detail pages should link to the film on criterion.com. The data already has `criterion_url` for many films (from the catalog). Make sure it's displayed as a link.

**Files:** `src/pages/films/[slug].astro` — check that `film.criterion_url` is rendered and linked. May need to construct URLs from spine number if criterion_url is empty.

## 6. Link to TMDB instead of IMDB on film pages
Replace the IMDB link with a TMDB link on film detail pages. The data has `tmdb_id` — construct the URL as `https://www.themoviedb.org/movie/{tmdb_id}`.

**Files:** `src/pages/films/[slug].astro`, `src/lib/data.ts` (add `tmdb_url` generation)

## 7. Fix "comma year" display bug
Film years are showing as `, YYYY` with a leading comma (e.g. ", 1954") instead of just the year. This happens on film pages, guest pages, and the films browse page. The issue is likely in how director/year metadata is displayed — when director is empty, the comma before the year still renders.

**Files:** `src/pages/guests/[slug].astro`, `src/pages/films/[slug].astro`, `src/pages/films/index.astro`, `src/components/FilmCard.astro`

## 8. Random page: default load with a film + "Another" button
The random page should load with a random film and its quotes already displayed (not empty with just a button). The refresh button should be below the content and say "Another" instead of "Surprise me".

**Files:** `src/pages/random.astro`, `src/components/RandomPick.astro`
