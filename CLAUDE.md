# Criterion Closet Picks

Astro static site (`src/`) + Python data pipeline (`scripts/`) that aggregates the Criterion Collection "Closet Picks" YouTube series into a searchable database at closetpicks.westenb.org.

## Data & pipeline

- **The pipeline runs locally, never in CI.** CI only builds the committed `data/*.json` into the site. To ship data changes: run the pipeline locally, commit the updated JSON, push.
- **Run the pipeline via the `update-data` skill** (new-episode check / full refresh); it prefers `process_all.py` over invoking steps by hand. For single-guest corrections (video match, Criterion URL, re-extract quotes) use the `fix-guest` skill.
- **Picks provenance:** Criterion.com /closet-picks collection pages are the sole primary source (Letterboxd was dropped Feb 2025). Transcripts are used only for quotes/timestamps.
- **Do NOT hand-add `NAME_FIXES` entries to restore an accent.** Diacritics are restored from TMDB automatically and synced onto picks by `slug` every run, so a manual accent edit gets clobbered; slugs stay ASCII on purpose. `NAME_FIXES` is only for genuine non-accent corrections (real typos, garbled overlay text).
- **`picks.guest_name` is a denormalized copy, never a source.** The guest record in `guests.json` owns the display name and the frontend joins by `slug`; `normalize_guests.py` (step 8b) NFC-normalizes that name and syncs it onto every pick by slug on every run, so the two cannot drift. `catalog_spine` works the same way — the catalog owns it, picks carry a synced copy, and `test_data.py` fails if the two disagree.
- **`backfill_films.py` creates catalog entries for films that appear in picks but not in the catalog**, and propagates canonical Criterion URLs from `picks_raw` into the catalog. It leaves `year` empty on purpose: `enrich_tmdb.py` fills that from the film's own Criterion page, which is authoritative.
- `data/*.json` is committed and read directly by the build; `data/transcripts/` is gitignored and regenerable. Keep generated JSON stable and human-reviewable (minimal diffs).

## Deploy

Pushes to `main` deploy to **Cloudflare Pages** via GitHub Actions (`.github/workflows/deploy-cloudflare.yml`). Custom domain: closetpicks.westenb.org.

The workflow runs `bun run test` → `bun run build` → `bunx pagefind --site dist`. **Pagefind indexes after the Astro build, not as part of it**, so a bare `bun run build` gives you a site with no search — run `bunx pagefind --site dist` too when testing search locally.

## Conventions

- **Everything is pre-rendered at build time — no server-side code in production.** Pages are static HTML served by Cloudflare Pages, and search is a client-side Pagefind index. Anything needing a request-time server does not belong here.
- **The Tufte-inspired design is deliberate, not incidental:** et-book serif, off-white `#fffff8` background, generous whitespace, typography-forward. Don't trade it away for a component library.
- **TMDB attribution is a legal requirement** — the footer TMDB logo + disclaimer must stay, per the API terms.
- Verify after changes: `bun run validate` (Python data-integrity tests) after touching `data/` or pipeline scripts; `bun run build` after frontend changes.
- Commits: short imperative subject, often with guest + pick count (`Add Bob Odenkirk's Closet Picks (13 films)`); keep data, pipeline, and UI changes in separate commits.
