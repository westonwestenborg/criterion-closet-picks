# Criterion Closet Picks Design Audit

Date: June 28, 2026

## Audit Scope

Audited the local Criterion Closet Picks Astro site from current source at `http://localhost:4321/`, plus the existing production preview output at `http://127.0.0.1:4322/` for Pagefind search behavior.

Covered screens:

1. Home page
2. Home global search in dev
3. Guests index
4. Guests filtered search
5. Guest detail: Laura Dern top
6. Guest detail: Laura Dern picks list
7. Films index
8. Films filtered search
9. Film detail: Seven Samurai
10. Most Popular
11. Production Pagefind search
12. Mobile home
13. Mobile guests
14. Mobile guests filtered search
15. Mobile guest detail
16. Mobile films
17. Guests empty-result state

Evidence lives in `audits/design-audit-2026-06-28/screenshots/`.

## User Goal And Accessibility Target

Primary user goal: browse Criterion Closet Picks by guest, film, popularity, and search, then move from a person or title into source context and quotes.

Accessibility target used for this audit: practical WCAG 2.2 AA risk review from screenshots, DOM checks, and source inspection. This is not a conformance certification because no screen reader pass, 200% zoom pass, high-contrast OS pass, or real assistive-technology session was run.

## Strengths

- The site has a clear editorial identity: serif typography, off-white paper tone, quiet rules, and roomy spacing match the documented Tufte-inspired direction.
- Core IA is understandable: Guests, Films, Most Popular, and Random are visible on every page.
- Detail pages are content-rich and trustworthy: guest pages combine source links, video, pick list, posters, and direct quotes; film pages connect external catalog links with who picked the title.
- The guest and film indexes use real anchors for cards, so basic navigation, Cmd-click, and copy-link behavior remain intact.
- Images observed in the audited pages had alt attributes, and the layout uses semantic `header`, `nav`, `main`, and `footer` landmarks.

## UX Risks

### P1 - Empty filtered states are silent

Evidence: `17-guests-empty-state.png`

Searching `zzzzzz` on Guests hides all 379 cards and jumps directly to the footer with no "no results" message, count, reset action, or suggestion. The same filtering mechanism powers Films.

Source:

- `src/components/FilterPanel.astro:27` to `src/components/FilterPanel.astro:57` hides cards by mutating `display`, but never renders a status element.
- `src/pages/guests/index.astro:16` and `src/pages/films/index.astro:17` render search controls without any result-count or empty-state target.

Recommendation: Add a shared results summary such as "2 guests shown" and an empty state with a reset action. Update it whenever search or chips change.

### P1 - Search/filter state is not shareable or back-button friendly

Evidence: `04-guests-search-laura.png`, `08-films-search-seven.png`

The filtered views work, but the URL remains `/guests/` or `/films/`. Users cannot share "films matching seven", bookmark a filtered view, or use Back to undo filter changes.

Source:

- `src/components/FilterPanel.astro:27` to `src/components/FilterPanel.astro:77`
- `src/pages/guests/index.astro:35` to `src/pages/guests/index.astro:39`
- `src/pages/films/index.astro:57` to `src/pages/films/index.astro:112`

Recommendation: Mirror `q`, `profession`, `decade`, `genre`, `sort`, and `dir` into `URLSearchParams`; initialize controls from the URL on load.

### P2 - Films filters are too dense on mobile

Evidence: `16-films-mobile.png`

On a 375px viewport, Decade, Genre, and Sort chips consume almost the entire first screen before the first film appears. The page is usable, but it makes the browsing task feel heavier than the content.

Source:

- `src/pages/films/index.astro:19` to `src/pages/films/index.astro:26`
- `src/components/FilterPanel.astro:10` to `src/components/FilterPanel.astro:17`

Recommendation: Keep desktop chips, but on mobile collapse filter groups into `details` sections or use compact segmented rows with a visible active-filter summary.

### P2 - Production search results are visually raw

Evidence: `11-production-search-bergman.png`

Pagefind returns results, but the rendering is plain text with long excerpts, weak spacing hierarchy, no result count, and no result type. It feels less polished than the index cards and detail pages.

Source:

- `src/components/SearchBar.astro:45` to `src/components/SearchBar.astro:52`

Recommendation: Render a structured result item: title, type badge (Film/Guest/Page), short excerpt, and maybe metadata like year/director or guest profession. Add a result count and loading/no-results states.

### P2 - Recent Guests mobile carousel sizing is broken by an invalid selector

Evidence: `12-home-mobile.png`

The intended mobile rule is `.recent-guests-scroll > :global(.card)`, but this selector is in `src/styles/global.css`, not an Astro scoped style block. Browser support check returned false. The first mobile recent-guest card measured about 195px inside a 347px scroller, with `flex: 0 1 auto` instead of the intended 80% snap card.

Source:

- `src/styles/global.css:772` to `src/styles/global.css:785`

Recommendation: Change the selector to `.recent-guests-scroll > .card` in the global stylesheet, or move scoped styling into the Astro component where `:global()` is meaningful.

### P3 - Guest detail pages spend a large first viewport before picks

Evidence: `05-guest-detail-laura-dern-top.png`, `15-guest-detail-mobile.png`

The video is valuable, but it pushes the actual pick list below the fold on both desktop and mobile. That is reasonable for a watch-first task, but many visitors likely arrive to inspect picks. The current page gives no jump target to the picks list.

Source:

- `src/pages/guests/[slug].astro:126` to `src/pages/guests/[slug].astro:136`

Recommendation: Add a compact subnav or link near the guest metadata: "Watch video" and "View 21 picks". This preserves the video while making the primary database task faster.

## Accessibility Risks

### P1 - Search inputs rely on placeholders instead of labels

Evidence: DOM checks found visible inputs with no `label`, `aria-label`, or `aria-labelledby` on Home, Guests, and Films.

Source:

- `src/components/SearchBar.astro:9` to `src/components/SearchBar.astro:15`
- `src/pages/guests/index.astro:16`
- `src/pages/films/index.astro:17`

WCAG relevance: labels and instructions, info and relationships, accessible names.

Recommendation: Add visible labels or visually hidden labels. Example: `<label class="sr-only" for="guest-search">Search guests</label>`.

### P1 - Filter and sort active states are visual-only

Evidence: active chips change color, but active buttons do not expose `aria-pressed` or equivalent state. DOM checks found active filter chips with `ariaPressed: null`.

Source:

- `src/components/FilterPanel.astro:12` to `src/components/FilterPanel.astro:15`
- `src/pages/films/index.astro:24` to `src/pages/films/index.astro:26`

WCAG relevance: name, role, value; state communication.

Recommendation: Treat chips as toggle buttons with `aria-pressed`, or use real radio groups for single-select filter sets.

### P1 - Dynamic result changes are not announced

Evidence: `#pagefind-results` has no `aria-live` or status role, and index filtering hides cards without any status text.

Source:

- `src/components/SearchBar.astro:16`
- `src/components/SearchBar.astro:33` to `src/components/SearchBar.astro:53`
- `src/components/FilterPanel.astro:27` to `src/components/FilterPanel.astro:57`

WCAG relevance: status messages and state changes.

Recommendation: Add a polite live region for search result counts and empty states. Keep announcements short.

### P2 - Focus treatment is incomplete

Evidence: source explicitly removes input outlines without a replacement beyond border color. Keyboard focus should be visibly stronger than hover and ordinary rest state.

Source:

- `src/styles/global.css:304` to `src/styles/global.css:323`
- `src/pages/guests/index.astro:53` to `src/pages/guests/index.astro:55`
- `src/pages/films/index.astro:133` to `src/pages/films/index.astro:135`
- `src/styles/global.css:333` to `src/styles/global.css:355`

WCAG relevance: focus visible, non-text contrast.

Recommendation: Add a site-wide `:focus-visible` treatment for links, buttons, inputs, summaries, and card links. Use outline or box-shadow with at least 3:1 contrast. Add `.card:focus-within` so keyboard users get the same card-level affordance as hover.

### P2 - No skip link

Evidence: the layout has landmarks, but no bypass link and no `main` target.

Source:

- `src/layouts/BaseLayout.astro:36` to `src/layouts/BaseLayout.astro:40`

WCAG relevance: bypass blocks.

Recommendation: Add a visually hidden "Skip to main content" link before the header and `id="main"` on `<main>`.

### P2 - Muted text is just below normal-text AA contrast

Evidence: computed contrast for `#777` on `#fffff8` was about 4.46:1. That is below the 4.5:1 AA threshold for normal text. Affected examples include small counts, stat labels, footer text, and detail metadata.

Source:

- `src/styles/global.css:101` to `src/styles/global.css:103`
- `src/styles/global.css:292` to `src/styles/global.css:297`
- `src/styles/global.css:718` to `src/styles/global.css:724`

Recommendation: Darken muted text slightly, for example toward `#707070` or `#666`, and recheck against `#fffff8`.

## Web Interface Guideline Findings

These are terse code-facing findings from the current Vercel Web Interface Guidelines pass:

- `src/components/SearchBar.astro:9` - input lacks label
- `src/components/SearchBar.astro:16` - async search results lack `aria-live` or status role
- `src/components/SearchBar.astro:45` - result HTML is inserted without a component/state model for loading, count, empty, and typed result metadata
- `src/pages/guests/index.astro:16` - input lacks label
- `src/pages/guests/index.astro:53` - `outline: none` without focus-visible replacement
- `src/pages/films/index.astro:17` - input lacks label
- `src/pages/films/index.astro:24` - active sort button lacks `aria-pressed`
- `src/pages/films/index.astro:133` - `outline: none` without focus-visible replacement
- `src/components/FilterPanel.astro:12` - active chip lacks `aria-pressed`
- `src/components/FilterPanel.astro:27` - filter state not reflected in URL
- `src/styles/global.css:341` - `transition: all`; list properties explicitly
- `src/styles/global.css:622` - `transition: all`; list properties explicitly
- `src/styles/global.css:781` - `:global(.card)` is invalid in this global stylesheet context
- `src/layouts/BaseLayout.astro:38` - no skip-link target on `main`

## Opportunity Areas

1. Make search feel like a first-class feature: result count, typed results, empty state, loading state, keyboard/a11y status, and production parity.
2. Make browse state durable: query params for search, filters, and sort.
3. Tighten mobile browsing: fix the recent guest carousel selector and compress the Films filter wall.
4. Add a small accessibility foundation: labels, skip link, focus-visible, live regions, `aria-pressed`, and a slightly darker muted color.
5. Preserve the quiet editorial style while adding more interaction feedback. The site does not need a new visual system; it needs stronger states.

## Evidence Limits And Verification Gaps

- Screenshots were taken locally with the in-app browser at 1280x720 and 375x812. Desktop images were cropped to avoid browser tooling UI at the bottom edge.
- Production search was tested against the existing `dist/` output, not a fresh build generated during this audit.
- No real screen reader, VoiceOver, high-contrast OS mode, 200% zoom, or keyboard-only completion pass was run.
- WCAG comments are risk findings, not a formal compliance claim.
- External video and poster assets loaded in the captured states, but network variability can change perceived detail-page loading behavior.

## Recommended Fix Order

1. Add labels, status/live regions, empty states, and result counts to search/filter surfaces.
2. Add skip link and site-wide `:focus-visible` styles; add `aria-pressed` to chips/sort buttons.
3. Fix `.recent-guests-scroll > :global(.card)` and verify mobile carousel sizing.
4. URL-sync browse state for Guests and Films.
5. Redesign Pagefind result rendering into structured result items.
6. Reduce mobile Films filter density.
7. Adjust muted text color and replace `transition: all`.

