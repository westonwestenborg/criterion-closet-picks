import { readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';

// Types matching the frontend expectations
export interface GuestVisit {
  youtube_video_id: string | null;
  youtube_video_url: string | null;
  vimeo_video_id: string | null;
  episode_date: string | null;
  criterion_page_url: string | null;
}

export interface Guest {
  name: string;
  slug: string;
  profession: string;
  photo_url: string | null;
  youtube_video_id: string;
  youtube_video_url: string;
  vimeo_video_id: string | null;
  episode_date: string;
  criterion_page_url: string | null;
  pick_count: number;
  visits?: GuestVisit[];
  guest_type?: 'person' | 'group' | 'character' | 'event';
  /**
   * Optional editorial override: the film_slug of this guest's standout pick,
   * surfaced on the home page. When unset, getBestPickForGuest() falls back to
   * a heuristic. Set this in guests.json to curate a guest's home-page quote.
   */
  featured_film_slug?: string | null;
}

export interface Film {
  title: string;
  slug: string;
  criterion_spine_number: number | null;
  director: string;
  year: number | null;
  country: string;
  genres: string[];
  criterion_url: string;
  imdb_id: string | null;
  imdb_url: string | null;
  tmdb_id: number | null;
  tmdb_type?: 'movie' | 'tv';
  tmdb_url: string | null;
  letterboxd_url: string | null;
  poster_url: string | null;
  pick_count: number;
  is_box_set?: boolean;
  credits?: {
    directors: { name: string; tmdb_id: number }[];
    writers: { name: string; tmdb_id: number }[];
    cinematographers: { name: string; tmdb_id: number }[];
    editors: { name: string; tmdb_id: number }[];
    cast: { name: string; tmdb_id: number; character: string }[];
  };
}

export interface Pick {
  guest_slug: string;
  film_slug: string;
  quote: string;
  start_timestamp_seconds: number;
  youtube_timestamp_url: string;
  vimeo_timestamp_url?: string;
  extraction_confidence: 'high' | 'medium' | 'low' | 'none';
  source?: 'criterion' | 'letterboxd';
  visit_index?: number;
  is_box_set?: boolean;
  box_set_name?: string;
  box_set_film_count?: number;
  box_set_film_titles?: string[];
  box_set_criterion_url?: string;
}

let dataDir = join(process.cwd(), 'data');

/** Test-only: point the loader at a fixture directory and clear all caches. */
export function setDataDir(dir: string): void {
  dataDir = dir;
  _picks = null;
  _films = null;
  _guests = null;
  _guestNameMap = null;
  _boxSetFilms = null;
  _pickedBoxSets = null;
}

function loadRawJSON(fileName: string): any[] {
  const filePath = join(dataDir, fileName);
  if (existsSync(filePath)) {
    return JSON.parse(readFileSync(filePath, 'utf-8'));
  }
  return [];
}

// Normalize picks from pipeline format (film_id, start_timestamp) to frontend format (film_slug, start_timestamp_seconds)
function normalizePicks(raw: any[]): Pick[] {
  return raw.map((p) => ({
    guest_slug: p.guest_slug,
    film_slug: p.film_slug ?? p.film_id ?? '',
    quote: p.quote ?? '',
    start_timestamp_seconds: p.start_timestamp_seconds ?? p.start_timestamp ?? 0,
    youtube_timestamp_url: p.youtube_timestamp_url ?? '',
    vimeo_timestamp_url: p.vimeo_timestamp_url ?? undefined,
    extraction_confidence: p.extraction_confidence ?? 'none',
    source: p.source ?? undefined,
    visit_index: p.visit_index ?? undefined,
    is_box_set: p.is_box_set ?? false,
    box_set_name: p.box_set_name ?? undefined,
    box_set_film_count: p.box_set_film_count ?? undefined,
    box_set_film_titles: p.box_set_film_titles ?? undefined,
    box_set_criterion_url: p.box_set_criterion_url ?? undefined,
  }));
}

// TMDB tags films with both its movie and TV genre taxonomies, which leak
// near-duplicate labels (each matching only a film or two). Fold the TV-only
// variants into their film-taxonomy equivalents so the genre filter isn't
// polluted by singletons.
const GENRE_NORMALIZE: Record<string, string> = {
  'War & Politics': 'War',
  'Sci-Fi & Fantasy': 'Science Fiction',
};
function normalizeGenres(genres: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const g of genres) {
    const n = GENRE_NORMALIZE[g] ?? g;
    if (!seen.has(n)) { seen.add(n); out.push(n); }
  }
  return out;
}

// Normalize films from pipeline catalog format (film_id, spine_number) to frontend format (slug, criterion_spine_number)
function normalizeFilms(raw: any[], pickCounts: Map<string, number>): Film[] {
  return raw.map((f) => {
    const slug = f.slug ?? f.film_id ?? '';
    const tmdbType = f.tmdb_type === 'tv' ? 'tv' : 'movie';
    return {
      title: f.title ?? '',
      slug,
      criterion_spine_number: f.criterion_spine_number ?? f.spine_number ?? null,
      director: f.director ?? '',
      year: f.year ?? null,
      country: f.country ?? '',
      genres: normalizeGenres(f.genres ?? []),
      criterion_url: f.criterion_url || '',
      imdb_id: f.imdb_id ?? null,
      imdb_url: f.imdb_url ?? (f.imdb_id ? `https://www.imdb.com/title/${f.imdb_id}/` : null),
      tmdb_id: f.tmdb_id ?? null,
      tmdb_type: f.tmdb_type ?? undefined,
      tmdb_url: f.tmdb_id ? `https://www.themoviedb.org/${tmdbType}/${f.tmdb_id}` : null,
      letterboxd_url: f.tmdb_id && tmdbType === 'movie' ? `https://letterboxd.com/tmdb/${f.tmdb_id}` : null,
      poster_url: f.poster_url ?? null,
      pick_count: f.pick_count ?? pickCounts.get(slug) ?? 0,
      is_box_set: f.is_box_set ?? false,
      credits: f.credits ?? undefined,
    };
  });
}

// Cache to avoid re-reading files
let _picks: Pick[] | null = null;
let _films: Film[] | null = null;
let _guests: Guest[] | null = null;

export function getPicks(): Pick[] {
  if (_picks) return _picks;
  _picks = normalizePicks(loadRawJSON('picks.json'));
  return _picks;
}

export function hasGuestVideo(guest: Guest): boolean {
  if (guest.youtube_video_id || guest.vimeo_video_id) return true;
  return guest.visits?.some(v => v.youtube_video_id || v.vimeo_video_id) ?? false;
}

/**
 * Check if a guest should have a page generated.
 * A guest is publishable if the project has a video-backed closet visit.
 */
export function isGuestPublishable(guest: Guest): boolean {
  return hasGuestVideo(guest);
}

export function getPublishableGuests(): Guest[] {
  return getGuests().filter(isGuestPublishable);
}

function getPublishableGuestSlugs(): Set<string> {
  return new Set(getPublishableGuests().map((g) => g.slug));
}

export function getSupportedPicks(): Pick[] {
  const publishableGuestSlugs = getPublishableGuestSlugs();
  return getPicks().filter((p) => publishableGuestSlugs.has(p.guest_slug));
}

export function getFilms(): Film[] {
  if (_films) return _films;
  const picks = getSupportedPicks();
  const pickCounts = new Map<string, number>();
  for (const p of picks) {
    // Only count picks with actual quotes
    if (p.source !== 'criterion' && (!p.quote || p.extraction_confidence === 'none')) continue;
    // Skip box set aggregate entries (they count toward the box set, not individual films)
    if (p.box_set_film_count) continue;
    pickCounts.set(p.film_slug, (pickCounts.get(p.film_slug) || 0) + 1);
  }

  const allFilms = normalizeFilms(loadRawJSON('criterion_catalog.json'), pickCounts);

  // If loading from full catalog (1000+ entries), filter to only picked films
  // Exclude box sets unless they're referenced as individual films (not aggregates)
  const individualFilmSlugs = new Set(
    picks.filter((p) => !p.box_set_film_count).map((p) => p.film_slug)
  );
  if (allFilms.length > 500) {
    const pickedSlugs = new Set(picks.map((p) => p.film_slug));
    _films = allFilms.filter((f) => {
      if (!pickedSlugs.has(f.slug)) return false;
      if ((f.is_box_set || f.criterion_url?.includes('/boxsets/')) && !individualFilmSlugs.has(f.slug)) return false;
      return true;
    });
  } else {
    _films = allFilms.filter((f) =>
      !(f.is_box_set || f.criterion_url?.includes('/boxsets/')) || individualFilmSlugs.has(f.slug)
    );
  }
  return _films;
}

export function getGuests(): Guest[] {
  if (_guests) return _guests;
  _guests = loadRawJSON('guests.json') as Guest[];

  // Check for local photo overrides in public/photos/
  const photosDir = join(process.cwd(), 'public', 'photos');
  for (const g of _guests) {
    if (!g.photo_url) {
      const localPath = join(photosDir, `${g.slug}.jpg`);
      if (existsSync(localPath)) {
        g.photo_url = `/photos/${g.slug}.jpg`;
      }
    }
  }

  return _guests;
}

export function getGuestBySlug(slug: string): Guest | undefined {
  return getGuests().find(g => g.slug === slug);
}

let _guestNameMap: Map<string, string> | null = null;
export function getGuestByName(name: string): Guest | undefined {
  if (!_guestNameMap) {
    _guestNameMap = new Map();
    for (const g of getPublishableGuests()) {
      _guestNameMap.set(g.name.toLowerCase(), g.slug);
    }
  }
  const slug = _guestNameMap.get(name.toLowerCase());
  return slug ? getGuestBySlug(slug) : undefined;
}

export function getFilmBySlug(slug: string): Film | undefined {
  return getFilms().find(f => f.slug === slug);
}

// Cache for box set catalog entries (excluded from getFilms() but needed for poster lookup)
let _boxSetFilms: Film[] | null = null;
function getBoxSetFilms(): Film[] {
  if (_boxSetFilms) return _boxSetFilms;
  const raw = loadRawJSON('criterion_catalog.json');
  const pickCounts = new Map<string, number>();
  _boxSetFilms = normalizeFilms(raw, pickCounts).filter(
    (f) => f.is_box_set || f.criterion_url?.includes('/boxsets/')
  );
  return _boxSetFilms;
}

export function getPicksForGuest(guestSlug: string): (Pick & { film: Film | undefined })[] {
  const guest = getGuestBySlug(guestSlug);
  if (!guest || !isGuestPublishable(guest)) return [];

  const films = getFilms();
  const boxSetFilms = getBoxSetFilms();
  return getPicks()
    .filter(p => p.guest_slug === guestSlug)
    .map(p => {
      let film: Film | undefined;
      // For box set aggregate picks, look up the box set catalog entry (for poster/metadata)
      if (p.box_set_film_count) {
        if (p.box_set_criterion_url) {
          film = boxSetFilms.find(f => f.criterion_url === p.box_set_criterion_url);
        }
        if (!film && p.box_set_name) {
          film = boxSetFilms.find(f => f.title === p.box_set_name);
        }
      } else {
        film = films.find(f => f.slug === p.film_slug);
      }
      return { ...p, film };
    });
}

export function getPicksForFilm(filmSlug: string): (Pick & { guest: Guest | undefined })[] {
  const guests = getPublishableGuests();
  const publishableGuestSlugs = new Set(guests.map((g) => g.slug));
  const allPicks = getPicks().filter((p) => publishableGuestSlugs.has(p.guest_slug));

  // Find box set names that contain this film (from tagged individual picks)
  const boxSetNames = new Set<string>();
  for (const p of allPicks) {
    if (p.film_slug === filmSlug && p.is_box_set && p.box_set_name) {
      boxSetNames.add(p.box_set_name);
    }
  }

  const results: Pick[] = [];
  const seenGuests = new Set<string>();

  // Direct picks with quotes for this film
  for (const p of allPicks) {
    if (p.film_slug !== filmSlug) continue;
    if (p.box_set_film_count) continue; // skip aggregates (handled below)
    if (p.source !== 'criterion' && (!p.quote || p.extraction_confidence === 'none')) continue; // skip no-quote unless criterion-sourced
    if (seenGuests.has(p.guest_slug)) continue;
    seenGuests.add(p.guest_slug);
    results.push(p);
  }

  // Aggregate box set picks with quotes (guest talked about the box set)
  for (const p of allPicks) {
    if (!p.box_set_film_count || !p.box_set_name) continue;
    if (!boxSetNames.has(p.box_set_name)) continue;
    if (p.source !== 'criterion' && (!p.quote || p.extraction_confidence === 'none')) continue;
    if (seenGuests.has(p.guest_slug)) continue;
    seenGuests.add(p.guest_slug);
    results.push(p);
  }

  return results.map(p => ({
    ...p,
    guest: guests.find(g => g.slug === p.guest_slug),
  }));
}

export function getRawPicksForGuest(guestSlug: string): { film_slug: string; film_title: string; guest_slug: string; source?: 'criterion' | 'letterboxd'; visit_index?: number }[] {
  const raw = loadRawJSON('picks_raw.json');
  return raw
    .filter((p: any) => p.guest_slug === guestSlug)
    .map((p: any) => ({
      film_slug: p.film_id ?? '',
      film_title: p.film_title ?? '',
      guest_slug: p.guest_slug,
      source: p.source ?? undefined,
      visit_index: p.visit_index ?? undefined,
    }));
}

export interface BoxSetInfo {
  name: string;
  criterion_url: string | null;
  guest_count: number;
  guest_slugs: string[];
}

export function getBoxSetInfoForFilm(filmSlug: string): BoxSetInfo[] {
  const allPicks = getSupportedPicks();

  // Find box set names that contain this film
  const boxSetNames = new Set<string>();
  const boxSetUrls = new Map<string, string>();
  for (const p of allPicks) {
    if (p.film_slug === filmSlug && p.is_box_set && p.box_set_name) {
      boxSetNames.add(p.box_set_name);
      if (!boxSetUrls.has(p.box_set_name) && p.box_set_criterion_url) {
        boxSetUrls.set(p.box_set_name, p.box_set_criterion_url);
      }
    }
  }

  // Collect guests who picked the box set but have NO quote
  // (guests with quotes are already shown in the main picks list via getPicksForFilm)
  const byBoxSet = new Map<string, Set<string>>();
  for (const bs of boxSetNames) {
    const noQuoteGuests = new Set<string>();

    // From aggregate picks without quotes
    for (const p of allPicks) {
      if (p.box_set_name === bs && p.box_set_film_count) {
        if (p.quote && p.extraction_confidence !== 'none') continue;
        noQuoteGuests.add(p.guest_slug);
        if (!boxSetUrls.has(bs) && p.box_set_criterion_url) {
          boxSetUrls.set(bs, p.box_set_criterion_url);
        }
      }
    }

    // From individual tagged picks without quotes
    for (const p of allPicks) {
      if (p.film_slug === filmSlug && p.is_box_set && p.box_set_name === bs && !p.box_set_film_count) {
        if (p.quote && p.extraction_confidence !== 'none') continue;
        noQuoteGuests.add(p.guest_slug);
      }
    }

    if (noQuoteGuests.size > 0) {
      byBoxSet.set(bs, noQuoteGuests);
    }
  }

  return Array.from(boxSetNames).map((name) => ({
    name,
    criterion_url: boxSetUrls.get(name) || null,
    guest_count: byBoxSet.get(name)?.size || 0,
    guest_slugs: [...(byBoxSet.get(name) || [])],
  }));
}

// --- Box set detail pages ---

// True when a pick carries a usable quote (matches the codebase's box-set convention).
function pickHasQuote(p: Pick): boolean {
  return !!(p.quote && p.quote.trim() !== '' && p.extraction_confidence !== 'none');
}

// A box set catalog entry matches an aggregate pick if the pick points at it by
// slug, Criterion URL, or set name (any one — box sets lack a single stable key).
function boxSetMatchesPick(f: Film, p: Pick): boolean {
  return (
    p.film_slug === f.slug ||
    (!!p.box_set_criterion_url && !!f.criterion_url && p.box_set_criterion_url === f.criterion_url) ||
    (!!p.box_set_name && p.box_set_name === f.title)
  );
}

// Cache for the subset of box set catalog entries that were actually picked.
let _pickedBoxSets: Film[] | null = null;

/**
 * Box set catalog entries referenced by a supported aggregate pick.
 * pick_count is the number of DISTINCT publishable guests who picked the set.
 */
export function getPickedBoxSets(): Film[] {
  if (_pickedBoxSets) return _pickedBoxSets;
  const aggregatePicks = getSupportedPicks().filter((p) => p.box_set_film_count);
  _pickedBoxSets = getBoxSetFilms()
    .filter((f) => aggregatePicks.some((p) => boxSetMatchesPick(f, p)))
    .map((f) => {
      const guestSlugs = new Set(
        aggregatePicks.filter((p) => boxSetMatchesPick(f, p)).map((p) => p.guest_slug)
      );
      return { ...f, pick_count: guestSlugs.size };
    });
  return _pickedBoxSets;
}

export function getBoxSetBySlug(slug: string): Film | undefined {
  return getPickedBoxSets().find((f) => f.slug === slug);
}

/**
 * Whether an internal detail page exists at /films/{slug}/ — i.e. the slug is
 * generated by films/[slug].astro's getStaticPaths (a film OR a picked box set).
 * Used to guard internal box-set links so we never point at a dead page.
 */
export function hasFilmPage(slug: string): boolean {
  return !!getFilmBySlug(slug) || !!getBoxSetBySlug(slug);
}

/**
 * Aggregate picks for a box set, one row per publishable guest (preferring the
 * pick that has a quote). Guests with quotes are ordered first.
 */
export function getPicksForBoxSet(slug: string): (Pick & { guest: Guest | undefined })[] {
  const boxSet = getBoxSetBySlug(slug);
  if (!boxSet) return [];
  const guests = getPublishableGuests();
  const picks = getSupportedPicks().filter(
    (p) => p.box_set_film_count && boxSetMatchesPick(boxSet, p)
  );

  // Dedupe by guest, preferring a pick that carries a quote.
  const byGuest = new Map<string, Pick>();
  for (const p of picks) {
    const existing = byGuest.get(p.guest_slug);
    if (!existing || (!pickHasQuote(existing) && pickHasQuote(p))) {
      byGuest.set(p.guest_slug, p);
    }
  }

  return [...byGuest.values()]
    .map((p) => ({ ...p, guest: guests.find((g) => g.slug === p.guest_slug) }))
    .sort((a, b) => Number(pickHasQuote(b)) - Number(pickHasQuote(a)));
}

/**
 * Best-effort list of the films inside a box set — only members that were also
 * picked individually (and resolve to a getFilms() entry) can be named here.
 */
export function getBoxSetMemberFilms(slug: string): { title: string; film: Film | undefined }[] {
  const boxSet = getBoxSetBySlug(slug);
  if (!boxSet) return [];
  const films = getFilms();
  const seen = new Set<string>();
  const members: { title: string; film: Film | undefined }[] = [];
  for (const p of getSupportedPicks()) {
    if (!p.is_box_set || p.box_set_film_count || p.box_set_name !== boxSet.title) continue;
    if (seen.has(p.film_slug)) continue;
    seen.add(p.film_slug);
    const film = films.find((f) => f.slug === p.film_slug);
    if (film) members.push({ title: film.title, film });
  }
  return members;
}

/**
 * Get all displayable picks for a guest, merging picks.json and picks_raw.json.
 * Display rule: show if source === 'criterion' OR pick has a quote.
 */
export function getDisplayablePicksForGuest(guestSlug: string): (Pick & { film: Film | undefined })[] {
  const guest = getGuestBySlug(guestSlug);
  if (!guest || !isGuestPublishable(guest)) return [];

  const films = getFilms();
  const boxSetFilms = getBoxSetFilms();
  const processedPicks = getPicks().filter(p => p.guest_slug === guestSlug);

  // Raw picks not covered by processed picks
  const processedFilmSlugs = new Set(processedPicks.map(p => p.film_slug));
  const rawPicks = getRawPicksForGuest(guestSlug)
    .filter(rp => !processedFilmSlugs.has(rp.film_slug));

  // Convert raw picks to Pick format
  const rawAsPicks: Pick[] = rawPicks.map(rp => ({
    guest_slug: rp.guest_slug,
    film_slug: rp.film_slug,
    quote: '',
    start_timestamp_seconds: 0,
    youtube_timestamp_url: '',
    extraction_confidence: 'none' as const,
    source: rp.source,
    visit_index: rp.visit_index,
  }));

  const allPicks = [...processedPicks, ...rawAsPicks];

  // Filter to displayable: source === 'criterion' OR has a quote
  const displayable = allPicks.filter(p => {
    // Hide individual box set picks with no quote (covered by aggregate entry)
    if (p.is_box_set && !p.box_set_film_count && (!p.quote || p.extraction_confidence === 'none')) {
      return false;
    }
    // Display rule: criterion-sourced OR has a quote
    if (p.source === 'criterion') return true;
    if (p.quote && p.quote.trim() !== '') return true;
    return false;
  });

  // Attach film metadata
  return displayable.map(p => {
    let film: Film | undefined;
    if (p.box_set_film_count) {
      if (p.box_set_criterion_url) {
        film = boxSetFilms.find(f => f.criterion_url === p.box_set_criterion_url);
      }
      if (!film && p.box_set_name) {
        film = boxSetFilms.find(f => f.title === p.box_set_name);
      }
    } else {
      film = films.find(f => f.slug === p.film_slug);
    }
    return { ...p, film };
  });
}

export function getFilmsSortedByPickCount(): Film[] {
  return [...getFilms()].sort((a, b) => b.pick_count - a.pick_count);
}

export function getAllProfessions(): string[] {
  const professions = new Set(getPublishableGuests().map(g => g.profession).filter(Boolean));
  return [...professions].sort();
}

export function getAllDecades(): string[] {
  const decades = new Set(
    getFilms().filter(f => f.year).map(f => `${Math.floor(f.year! / 10) * 10}s`)
  );
  return [...decades].sort();
}

export function getAllGenres(): string[] {
  const genres = new Set(getFilms().flatMap(f => f.genres));
  return [...genres].sort();
}

/**
 * Genres ranked by how many films carry them (most common first), for the
 * films-index filter: common genres lead, the long tail collapses behind a
 * "More genres" toggle. All genres are kept — 34% of films are single-genre
 * and tail genres (Western, Horror, …) are the sole genre for dozens of films,
 * so dropping any would make those films unreachable.
 */
export function getGenresRanked(): string[] {
  const freq = new Map<string, number>();
  for (const f of getFilms()) {
    for (const g of f.genres) freq.set(g, (freq.get(g) || 0) + 1);
  }
  return [...freq.entries()].sort((a, b) => b[1] - a[1]).map(([g]) => g);
}

export function getRecentGuests(count: number = 3): Guest[] {
  // Extract the numeric collection ID from criterion_page_url (e.g., /shop/collection/928-...)
  // These IDs are monotonically increasing and reflect publication order on Criterion.com.
  const collectionId = (g: Guest): number => {
    const m = g.criterion_page_url?.match(/\/shop\/collection\/(\d+)-/);
    return m ? parseInt(m[1], 10) : 0;
  };
  return [...getPublishableGuests()]
    .filter(g => g.criterion_page_url)
    .sort((a, b) => collectionId(b) - collectionId(a))
    .slice(0, count);
}

const CONFIDENCE_RANK: Record<string, number> = { high: 3, medium: 2, low: 1, none: 0 };

/**
 * Select a guest's strongest quote for surfacing (e.g. the home page).
 * Prefers the editorial override (guest.featured_film_slug); otherwise a
 * heuristic: highest extraction confidence, then a quote length near ~200
 * characters. Returns null when the guest has no usable quote.
 */
export function getBestPickForGuest(guestSlug: string): (Pick & { film: Film | undefined }) | null {
  const guest = getGuestBySlug(guestSlug);
  if (!guest || !isGuestPublishable(guest)) return null;

  const candidates = getPicks().filter(
    (p) => p.guest_slug === guestSlug && !p.box_set_film_count && !!p.quote && p.quote.trim().length >= 60
  );
  if (!candidates.length) return null;

  const override = guest.featured_film_slug || null;
  const score = (p: Pick): number => {
    if (override && p.film_slug === override) return Number.MAX_SAFE_INTEGER;
    const conf = CONFIDENCE_RANK[p.extraction_confidence] ?? 0;
    return conf * 1000 - Math.abs((p.quote?.length ?? 0) - 200);
  };

  const best = [...candidates].sort((a, b) => score(b) - score(a))[0];
  const films = getFilms();
  return { ...best, film: films.find((f) => f.slug === best.film_slug) };
}

/**
 * The most recent guests paired with their best pick, for the home page.
 * Skips guests without a usable quote so the layout stays consistent.
 */
export function getRecentGuestsWithBestPick(count = 4): { guest: Guest; pick: Pick & { film: Film | undefined } }[] {
  const out: { guest: Guest; pick: Pick & { film: Film | undefined } }[] = [];
  for (const guest of getRecentGuests(count * 5)) {
    const pick = getBestPickForGuest(guest.slug);
    if (pick && pick.film) out.push({ guest, pick });
    if (out.length >= count) break;
  }
  return out;
}

export function getStats() {
  return {
    totalGuests: getPublishableGuests().length,
    totalFilms: getFilms().length,
    totalPicks: getSupportedPicks().length,
  };
}
