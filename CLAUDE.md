# Criterion Closet Picks

A searchable static web app that aggregates data from the Criterion Collection's "Closet Picks" YouTube series (400+ episodes). It combines curated pick lists from Letterboxd (@closetpicks), verbatim quotes extracted from YouTube transcripts via Gemini Flash, and film metadata from TMDB to create a unified, searchable database. The site lives at closetpicks.westenb.org.

## Architecture

### Stack

| Layer | Technology |
|-------|-----------|
| Static site generator | Astro |
| Search | Pagefind (client-side, built at deploy time) |
| Styling | Custom CSS (Tufte-inspired, matching westenb.org) |
| Client-side interactivity | Vanilla JS via Astro islands |
| Data pipeline | Python scripts |
| LLM extraction | Gemini 2.0 Flash |
| Film metadata | TMDB API |
| Hosting | GitHub Pages |
| CI/CD | GitHub Actions |

### Data Flow

```
Criterion.com (catalog) --> data/criterion_catalog.json
Letterboxd @closetpicks (guest picks) --> data/guests.json, data/picks_raw.json
YouTube transcripts --> data/transcripts/ (gitignored)
Gemini Flash (quote extraction) --> data/picks.json
TMDB API (enrichment) --> updates catalog + guests with posters, genres, IMDB IDs
```

All data files in `data/` (except `transcripts/`) are committed to the repo. The Astro build reads these JSON files to generate static pages.

## Build Instructions

### Frontend (Astro site)

```bash
npm install
npm run dev        # Local dev server (usually http://localhost:4321)
npm run build      # Production build to dist/
npm run preview    # Preview production build locally
```

After building, generate the search index:

```bash
npx pagefind --site dist
```

### Data Pipeline (Python)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r scripts/requirements.txt
```

#### Environment Variables

Copy `.env.example` to `.env` and fill in your keys:

```bash
TMDB_READ_ACCESS_TOKEN=<your-tmdb-read-access-token>
TMDB_API_KEY=<your-tmdb-api-key>
GEMINI_API_KEY=<your-gemini-api-key>
```

#### Running the Pipeline

Scripts are run in order. Each writes to `data/`:

```bash
python scripts/build_catalog.py       # Step 1: Build Criterion catalog reference
python scripts/scrape_letterboxd.py   # Step 2: Scrape guest picks from Letterboxd
python scripts/scrape_criterion_picks.py  # Step 2b: Supplement with Criterion.com data
python scripts/match_youtube.py       # Step 3: Match YouTube videos + pull transcripts
python scripts/extract_quotes.py      # Step 4: LLM quote extraction via Gemini Flash
python scripts/backfill_films.py      # Step 5: Backfill missing films + propagate Criterion URLs
python scripts/enrich_tmdb.py         # Step 6: TMDB enrichment (genres, posters, IMDB IDs)
python scripts/validate.py            # Validate data and generate reports
python scripts/test_data.py           # Run data integrity tests (also: npm run validate)
```

For processing a single new video:

```bash
python scripts/process_video.py --youtube-url "https://www.youtube.com/watch?v=..."
```

For batch processing:

```bash
python scripts/process_all.py --limit 10   # Pilot: first 10 videos
python scripts/process_all.py               # Full run: all videos
```

## Deployment

The site is deployed to GitHub Pages via GitHub Actions on every push to `main`.

- **Custom domain:** closetpicks.westenb.org
- **DNS:** CNAME record pointing `closetpicks` to `westonwestenborg.github.io`
- **Workflow:** `.github/workflows/deploy.yml` handles checkout, build, Pagefind indexing, and deployment
- **Data pipeline runs locally**, not in CI. Commit updated data JSON files and push to trigger a rebuild.

## Directory Structure

```
criterion-closet-picks/
├── .github/workflows/deploy.yml   # GitHub Actions: build + deploy to Pages
├── data/                           # JSON data files (committed to repo)
│   ├── criterion_catalog.json      # All Criterion titles with metadata (+ backfilled non-catalog films)
│   ├── guests.json                 # All guests with metadata
│   ├── picks.json                  # All picks with quotes + timestamps
│   ├── picks_raw.json              # Raw picks from Letterboxd/Criterion (no quotes)
│   ├── transcripts/                # Raw transcripts (gitignored, regenerable)
│   └── validation/                 # Validation reports
├── scripts/                        # Python data pipeline
│   ├── requirements.txt
│   ├── build_catalog.py            # Criterion catalog scraper
│   ├── scrape_letterboxd.py        # Letterboxd guest picks scraper
│   ├── scrape_criterion_picks.py   # Criterion.com collection page scraper
│   ├── match_youtube.py            # YouTube video matching + transcripts
│   ├── extract_quotes.py           # Gemini Flash quote extraction
│   ├── backfill_films.py           # Backfill missing films + propagate Criterion URLs
│   ├── enrich_tmdb.py              # TMDB API enrichment
│   ├── process_video.py            # Single video processing workflow
│   ├── process_all.py              # Batch processing workflow
│   ├── validate.py                 # Data validation + reporting
│   └── test_data.py                # Data integrity tests (unittest)
├── src/                            # Astro site source
│   ├── layouts/BaseLayout.astro
│   ├── pages/
│   │   ├── index.astro             # Home
│   │   ├── guests/index.astro      # Browse by guest
│   │   ├── guests/[slug].astro     # Guest detail
│   │   ├── films/index.astro       # Browse by film
│   │   ├── films/[slug].astro      # Film detail
│   │   ├── most-popular.astro      # Most popular ranking
│   │   └── llm-export.astro        # Markdown export for LLMs
│   ├── components/                 # Reusable Astro components
│   └── styles/global.css           # Tufte-inspired global styles
├── public/
│   ├── CNAME                       # Custom domain for GitHub Pages
│   └── tmdb-logo.svg               # TMDB attribution logo
├── .env.example                    # Template for API keys
├── astro.config.mjs                # Astro configuration
├── package.json
└── CLAUDE.md                       # This file
```

## Key Conventions

- **Data source of truth for picks:** Letterboxd @closetpicks account (440 curated lists), supplemented by Criterion.com collection pages. Transcripts are used only for quotes/timestamps.
- **Static site:** Everything is pre-rendered at build time. No server-side code in production.
- **Pagefind search:** Client-side static search, index generated post-build.
- **TMDB attribution required:** Footer must include TMDB logo and disclaimer per API terms.
- **Tufte-inspired design:** Typography-forward, et-book serif font, off-white (#fffff8) background, generous whitespace.
- **Data pipeline is manual:** Run scripts locally, commit JSON data files, push to trigger rebuild.
- **Transcripts are gitignored:** Large and regenerable. Only processed quote data in `picks.json` is committed.
- **Film matching uses fuzzy search:** `thefuzz` library handles title variations between sources.
- **Data integrity tests:** Run `python scripts/test_data.py` (or `npm run validate`) to verify data quality after pipeline runs. Tests check film coverage, URL validity, box set structure, guest coverage, and year validity.
- **Backfill step:** `backfill_films.py` creates catalog entries for films that appear in picks but not in the Criterion catalog, and propagates canonical Criterion URLs from picks_raw into the catalog.
- **Guests without videos:** Guests who have picks but no YouTube video/transcript get their picks from `picks_raw.json` as a fallback (displayed without quotes).
