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

The workflow runs `bun run test` → `bun run build` → sitemap `lastmod` → the SEO gate (`bun run test:seo`) → `bunx pagefind --site dist` → deploy → IndexNow (see SEO below). Pull requests run the same steps up to the deploy. **Pagefind indexes after the Astro build, not as part of it**, so a bare `bun run build` gives you a site with no search — run `bunx pagefind --site dist` too when testing search locally.

## SEO

Every guest, film and index page should be easy for search engines to find and understand. CI enforces this, so a page that breaks the rules fails the pull request before it can deploy.

### What every indexable page needs

Pass these to `BaseLayout` (`src/layouts/BaseLayout.astro`):

- **A unique `<title>`** that names the page's subject. Pass `title` and the layout adds " — Closet Picks"; pass `documentTitle` when the title already names the site. Guest pages read "Aaron Sorkin's Criterion Closet Picks". Film pages name the year and director, "Roma (2018) by Alfonso Cuarón — Closet Picks", because several films share a title. The builders live in `src/lib/seo.ts`.
- **A specific meta `description`** built from the data, such as the films a guest picked or the guests who picked a film. Aim for 160 characters or fewer, and only claim what the page shows: say "each quote links to the moment it was said" only when every quote has a timestamp.
- **An `ogImage`** for the page's own card when one exists (`/og/guests/<slug>.jpg`, `/og/films/<slug>.jpg`). The layout writes the canonical URL, Open Graph and Twitter tags from these props.
- **`structuredData` (JSON-LD)**: `WebSite` on the home page, a `ProfilePage` about a `Person` on guest pages, a `WebPage` about a `Movie` (or `TVSeries`) on film pages, and a `CollectionPage` with an `ItemList` on index pages (`collectionPageData()` in `src/lib/seo.ts`).
- **Exactly one `<h1>`**, an `alt` attribute on every `<img>` (`alt=""` for decorative images), and at least one normal `<a href>` link from another page. Pagefind search does not count, because crawlers do not run it.

A page that should not be in search results (Random, 404, Data Exports) passes `noindex` to `BaseLayout` **and** is added to `NOINDEX_PAGES` in `astro.config.mjs`, which keeps it out of the sitemap. Every other HTML page goes into the sitemap automatically.

### The CI gate

```bash
bun run build && bun run test:seo
```

`test:seo` (`tests/built/seo.test.ts`, logic in `tools/seo/audit.ts`) reads `dist/` and fails when:

- a sitemap page is missing its title, meta description, canonical URL, `og:title`, `og:description`, `og:image`, `og:url`, `twitter:card` or JSON-LD, or has a canonical that differs from its sitemap URL;
- two sitemap pages share a title or a meta description;
- a guest or film page (any publishable guest, film or picked box set) is missing from the sitemap;
- a `noindex` page is in the sitemap, or a built page is neither in the sitemap nor `noindex`;
- a sitemap page has no `<h1>` or more than one, or an `<img>` with no `alt`;
- no chain of links from the home page reaches a sitemap page.

Each failure line names the page and the problem, for example `/films/roma-2018/: missing <meta name="description">`. The workflow runs the gate on every pull request and before every deploy. `tests/seo.test.ts` unit-tests the title and description builders, and `tests/seo-tools.test.ts` tests the gate, lastmod and IndexNow logic against small fixture sites; both run with `bun run test`.

### Sitemap lastmod and IndexNow

The data records no edit dates, so each sitemap `<lastmod>` comes from page content. At deploy, `bun tools/seo/cli.ts lastmod` fingerprints each page's title, description and `<main>` content, and compares that with `/sitemap-lastmod.json` from the previous deploy. An unchanged page keeps its old date, and a new or changed page gets the deploy time. Changes to the header, footer or CSS do not count as page changes. If you change what goes into the fingerprint (`pageFingerprint` in `tools/seo/lastmod.ts`), bump `FINGERPRINT_VERSION` so the next deploy does not mark every page as changed.

After a successful deploy, `bun tools/seo/cli.ts indexnow` compares the new sitemap with the one that was live when the job started. It sends the added, changed and removed URLs to IndexNow (`https://api.indexnow.org/indexnow`) in one POST, split into batches of 10,000 if needed. IndexNow shares each submission with Bing, Yandex, Naver, Seznam, Yep and Amazon. Unchanged URLs are never resubmitted, because the protocol treats that as misuse. The key is public: the engines check it by fetching `public/<key>.txt` from the site, and `INDEXNOW_KEY` in `tools/seo/indexnow.ts` must match that file's name and content. Neither step can fail a deploy; problems show up as warnings on the workflow run. In CI these steps read the live site through `criterion-closet-picks.pages.dev` (`SEO_READ_ORIGIN` in the workflow), because Cloudflare answers GitHub's runners with HTTP 403 on closetpicks.westenb.org. Run `bun tools/seo/cli.ts indexnow --dry-run` to print the URLs a deploy would submit without sending them. Do not submit to production by hand.

### Google and Brave

Google and Brave do not take IndexNow. Google reads the sitemap, and the sitemap's `lastmod` tells it what changed. Brave Search has no URL-submission API. Its crawler finds pages the same way other crawlers do, and it skips anything Googlebot can't crawl. Brave's submit form is behind a CAPTCHA and is meant for re-fetching or removing a page, so don't try to automate it. For both engines, what matters is a complete sitemap, `robots.txt` pointing at it, pages reachable through links, and HTML that Googlebot can crawl. The gate checks all of these.

## Conventions

- **Everything is pre-rendered at build time — no server-side code in production.** Pages are static HTML served by Cloudflare Pages, and search is a client-side Pagefind index. Anything needing a request-time server does not belong here.
- **The Tufte-inspired design is deliberate, not incidental:** et-book serif, off-white `#fffff8` background, generous whitespace, typography-forward. Don't trade it away for a component library.
- **TMDB attribution is a legal requirement** — the footer TMDB logo + disclaimer must stay, per the API terms.
- Verify after changes: `bun run validate` (Python data-integrity tests) after touching `data/` or pipeline scripts; `bun run build && bun run test:seo` after frontend or data changes.
- Commits: short imperative subject, often with guest + pick count (`Add Bob Odenkirk's Closet Picks (13 films)`); keep data, pipeline, and UI changes in separate commits.
