# Criterion Closet Picks — Data Pipeline Implementation Plan

**Author:** Data Pipeline Engineer (Claude)
**Date:** 2026-02-05
**Status:** Awaiting Approval

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [requirements.txt](#2-requirementstxt)
3. [Shared Utilities Module](#3-shared-utilities-module)
4. [Script 1: build_catalog.py](#4-script-1-build_catalogpy)
5. [Script 2: scrape_letterboxd.py](#5-script-2-scrape_letterboxdpy)
6. [Script 3: match_youtube.py](#6-script-3-match_youtubepy)
7. [Script 4: extract_quotes.py](#7-script-4-extract_quotespy)
8. [Script 5: enrich_tmdb.py](#8-script-5-enrich_tmdbpy)
9. [Script 6: process_video.py](#9-script-6-process_videopy)
10. [Script 7: process_all.py](#10-script-7-process_allpy)
11. [Script 8: validate.py](#11-script-8-validatepy)
12. [10-Video Pilot Plan](#12-10-video-pilot-plan)
13. [Implementation Order and Dependencies](#13-implementation-order-and-dependencies)
14. [Key Design Decisions](#14-key-design-decisions)
15. [Risk Register](#15-risk-register)

---

## 1. Architecture Overview

```
                    +-------------------+
                    | build_catalog.py  |  Step 1
                    | (Criterion spine  |
                    |  catalog)         |
                    +---------+---------+
                              |
                              v
                    data/criterion_catalog.json
                              |
          +-------------------+-------------------+
          |                                       |
+---------+---------+               +-------------+----------+
| scrape_letterboxd |  Step 2      | match_youtube.py       | Step 3
| .py               |              | (yt-dlp + youtube-     |
| (440 lists)       |              |  transcript-api)       |
+---------+---------+              +-------------+----------+
          |                                      |
          v                                      v
  data/guests.json                     data/transcripts/*.json
  data/picks_raw.json                  (guests.json updated with
          |                             video IDs + dates)
          +-------------------+-------------------+
                              |
                    +---------+---------+
                    | extract_quotes.py |  Step 4
                    | (Gemini 2.0 Flash)|
                    +---------+---------+
                              |
                              v
                    data/picks.json
                    (with quotes + timestamps)
                              |
                    +---------+---------+
                    | enrich_tmdb.py    |  Step 5
                    | (TMDB API)        |
                    +---------+---------+
                              |
                              v
                    data/criterion_catalog.json (enriched)
                    data/guests.json (enriched)
                              |
                    +---------+---------+
                    | validate.py       |  Step 8
                    +---------+---------+
                              |
                              v
                    data/validation/*.json
                    + console report
```

All scripts are idempotent: they can be re-run without duplicating data. Each script loads existing JSON, merges/updates, and writes back.

---

## 2. requirements.txt

```
requests>=2.31.0
beautifulsoup4>=4.12.0
lxml>=5.0.0
playwright>=1.40.0
yt-dlp>=2024.1.0
youtube-transcript-api>=0.6.1
google-generativeai>=0.8.0
thefuzz[speedup]>=0.22.0
python-Levenshtein>=0.25.0
python-dotenv>=1.0.0
tqdm>=4.66.0
ratelimit>=2.2.1
jsonschema>=4.20.0
```

---

## 3. Shared Utilities Module

**File:** `scripts/utils.py`

```python
# Key functions and constants:

DATA_DIR = Path("data")
CATALOG_FILE = DATA_DIR / "criterion_catalog.json"
GUESTS_FILE = DATA_DIR / "guests.json"
PICKS_RAW_FILE = DATA_DIR / "picks_raw.json"
PICKS_FILE = DATA_DIR / "picks.json"
TRANSCRIPTS_DIR = DATA_DIR / "transcripts"
VALIDATION_DIR = DATA_DIR / "validation"

def load_json(path) -> list | dict
def save_json(path, data)
def slugify(name: str) -> str
def load_env()
def fuzzy_match_name(name1, name2, threshold=80) -> bool
def fuzzy_match_title(title1, title2, year1=None, year2=None, threshold=85) -> bool
def make_film_id(title: str, year: int) -> str
```

---

## 4. Script 1: build_catalog.py

**Purpose:** Build Criterion Collection catalog with spine numbers, titles, directors, years, countries.

**Strategy — Three approaches in priority order:**

**Approach A: The Digital Bits Criterion Spines Project (PRIMARY)**
- URL: `https://thedigitalbits.com/columns/todd-doogan/the-criterion-spines-project`
- Updated January 2026, covers all spines through 1400+
- Plain HTML, no JS rendering needed, no anti-scraping measures
- Parse spine number, title, director, year from structured entries

**Approach B: Criterion.com direct scrape (FALLBACK 1)**
- Try with `requests` first, fall back to Playwright if blocked
- URL: `https://www.criterion.com/shop/browse/list?sort=spine_number`
- Rate limit: 2-3 seconds between pages

**Approach C: GitHub R package dataset (FALLBACK 2)**
- Last resort, data from March 2022, missing ~100+ newer titles

**Output:** `data/criterion_catalog.json`

**CLI:** `python scripts/build_catalog.py [--source digitalbits|criterion|github]`

---

## 5. Script 2: scrape_letterboxd.py

**Purpose:** Scrape all 440 guest pick lists from @closetpicks on Letterboxd.

**Decision: Custom scraping** (not the L-Dot/Letterboxd-list-scraper tool) for tighter control over name extraction, inline catalog matching, and rate limiting.

**Process:**
1. Paginate `letterboxd.com/closetpicks/lists/page/{n}/` (~22 pages)
2. Parse guest name from list title (handles `"Name's Closet Picks"`, `"Closet Picks: Name"`, etc.)
3. Scrape each list for film entries (title, year from `data-film-slug` attributes)
4. Match films to catalog (exact then fuzzy)
5. Handle box sets with hardcoded known box set list

**Rate limiting:** 1-second delay between requests (~8 min total)

**Output:** `data/guests.json` + `data/picks_raw.json`

**CLI:** `python scripts/scrape_letterboxd.py [--limit N] [--resume]`

---

## 6. Script 3: match_youtube.py

**Purpose:** Match YouTube videos to guests, download transcripts.

**Process:**
1. Dump playlist metadata via `yt-dlp --flat-playlist --dump-json`
2. Parse guest name from video title (multiple format patterns)
3. Fuzzy match video→guest using `token_sort_ratio` (threshold 80)
4. Pull transcripts via `youtube-transcript-api`
5. Handle edge cases: multiple visits, compilation videos, international names

**Rate limiting:** 0.5s between transcript requests (~3-4 min total)

**Output:** Updated `data/guests.json` + `data/transcripts/{video_id}.json`

**CLI:** `python scripts/match_youtube.py [--playlist-url URL] [--limit N]`

---

## 7. Script 4: extract_quotes.py

**Purpose:** LLM quote extraction via Gemini 2.0 Flash.

**Key implementation details:**
- Use `response_mime_type="application/json"` for structured output
- Temperature 0.1 for deterministic extraction
- Add few-shot example to prompt
- Post-process: validate film titles match input list, validate timestamps, cap quote length at 500 chars

**Rate limiting:** 6-second delay between requests (10 RPM), ~40 min for all 400 videos

**Cost estimate:** ~$0.70 total

**Checkpoint file:** `data/.extraction_progress.json` for resume capability

**CLI:** `python scripts/extract_quotes.py [--limit N] [--guest-slug SLUG] [--force]`

---

## 8. Script 5: enrich_tmdb.py

**Purpose:** Enrich films (genres, posters, IMDB IDs) and guests (profession, photo) via TMDB API.

**Optimization:** Fetch genre ID→name mapping once, then only need search + external_ids per film (2 calls instead of 3).

**Guest enrichment:** Map `known_for_department` to profession enum. Manual override file `data/guest_overrides.json` for musicians/comedians not on TMDB.

**Rate limiting:** 20 req/s, ~3,240 total calls, ~2.7 min total

**CLI:** `python scripts/enrich_tmdb.py [--films-only] [--guests-only] [--limit N]`

---

## 9-11. Orchestration Scripts

- **process_video.py** — Single-video end-to-end workflow for ongoing maintenance
- **process_all.py** — Batch orchestrator with `--pilot` flag for 10-video test
- **validate.py** — Data integrity checks, coverage stats, validation reports

---

## 12. 10-Video Pilot Plan

| # | Guest | Profession | Era | Why |
|---|-------|-----------|-----|-----|
| 1 | Barry Jenkins | Director | 2017 | Known video ID (R7HLpe65fHY), baseline |
| 2 | Guillermo del Toro | Director | ~2013 | Early episode, verbose |
| 3 | Bill Hader | Actor/Comedian | ~2018 | Tests comedian path |
| 4 | Greta Gerwig | Director/Actor | ~2018 | Dual profession |
| 5 | Bong Joon-ho | Director | ~2019 | International, transcript quality |
| 6 | Ayo Edebiri | Actor | ~2024 | Recent episode format |
| 7 | Charli XCX | Musician | ~2024 | Non-filmmaker, TMDB failure path |
| 8 | Martin Scorsese | Director | ~2014 | Many picks |
| 9 | Park Chan-wook | Director | ~2016 | Name matching edge case |
| 10 | Cate Blanchett | Actor | ~2023 | High-profile, recent |

**Success criteria:**
- 8/10 videos processed end-to-end
- >70% "high" confidence quotes
- Timestamps within ~30 seconds
- >90% film matching rate

---

## 13. Implementation Order

```
Critical path: build_catalog → scrape_letterboxd → match_youtube → extract_quotes
Off critical path: enrich_tmdb (after scrape_letterboxd), validate (after all)
```

1. `utils.py` (no deps)
2. `build_catalog.py` (no deps)
3. `scrape_letterboxd.py` (needs catalog)
4. `match_youtube.py` (needs guests)
5. `extract_quotes.py` (needs guests + transcripts + picks_raw)
6. `enrich_tmdb.py` (parallel with extract_quotes, needs catalog + guests)
7. `validate.py` (needs all data)
8. `process_video.py` + `process_all.py` (needs all scripts as modules)

---

## 14. Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Catalog source | Digital Bits first | Static HTML, updated Jan 2026, no anti-scraping |
| Letterboxd scraping | Custom (not 3rd party tool) | Tighter control, inline matching, fewer deps |
| Name matching | thefuzz token_sort_ratio + manual overrides | Handles word order, accents; override file for edge cases |
| Gemini prompt | PRD prompt + JSON mode + few-shot example | Structured output, low temperature, validated against input |
| Data integrity | Graceful degradation + validation reports | Never block pipeline on partial data |

---

## 15. Risk Register

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| Criterion.com blocks scraping | Catalog incomplete | Medium | Digital Bits as primary source |
| Letterboxd HTML changes | Scraping breaks | Low | Stable structure, pin selectors |
| YouTube transcript API restricted | No quotes | Low | Fallback to yt-dlp subtitle download |
| Gemini Flash quality poor | Low-quality quotes | Medium | Pilot first; upgrade to Pro/Haiku if needed |
| Name matching wrong matches | Data errors | Medium | Manual override file + human review |
| Box sets misattributed | Data quality | Medium | Detection heuristic + known box set list |
