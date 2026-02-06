import { readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';

// Types matching the frontend expectations
export interface Guest {
  name: string;
  slug: string;
  profession: string;
  photo_url: string | null;
  youtube_video_id: string;
  youtube_video_url: string;
  episode_date: string;
  letterboxd_list_url: string;
  criterion_page_url: string | null;
  pick_count: number;
}

export interface Film {
  title: string;
  slug: string;
  criterion_spine_number: number | null;
  director: string;
  year: number;
  country: string;
  genres: string[];
  criterion_url: string;
  imdb_id: string | null;
  imdb_url: string | null;
  tmdb_id: number | null;
  tmdb_url: string | null;
  poster_url: string | null;
  pick_count: number;
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
  extraction_confidence: 'high' | 'medium' | 'low' | 'none';
  is_box_set?: boolean;
  box_set_name?: string;
  box_set_film_count?: number;
  box_set_film_titles?: string[];
  box_set_criterion_url?: string;
}

const dataDir = join(process.cwd(), 'data');

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
    extraction_confidence: p.extraction_confidence ?? 'none',
    is_box_set: p.is_box_set ?? false,
    box_set_name: p.box_set_name ?? undefined,
    box_set_film_count: p.box_set_film_count ?? undefined,
    box_set_film_titles: p.box_set_film_titles ?? undefined,
    box_set_criterion_url: p.box_set_criterion_url ?? undefined,
  }));
}

// Normalize films from pipeline catalog format (film_id, spine_number) to frontend format (slug, criterion_spine_number)
function normalizeFilms(raw: any[], pickCounts: Map<string, number>): Film[] {
  return raw.map((f) => {
    const slug = f.slug ?? f.film_id ?? '';
    return {
      title: f.title ?? '',
      slug,
      criterion_spine_number: f.criterion_spine_number ?? f.spine_number ?? null,
      director: f.director ?? '',
      year: f.year ?? 0,
      country: f.country ?? '',
      genres: f.genres ?? [],
      criterion_url: f.criterion_url || `https://www.criterion.com/shop/browse?q=${encodeURIComponent(f.title ?? '')}`,
      imdb_id: f.imdb_id ?? null,
      imdb_url: f.imdb_url ?? (f.imdb_id ? `https://www.imdb.com/title/${f.imdb_id}/` : null),
      tmdb_id: f.tmdb_id ?? null,
      tmdb_url: f.tmdb_id ? `https://www.themoviedb.org/movie/${f.tmdb_id}` : null,
      poster_url: f.poster_url ?? null,
      pick_count: f.pick_count ?? pickCounts.get(slug) ?? 0,
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
  // Prefer real pipeline data, fall back to mock
  let raw = loadRawJSON('picks.json');
  if (raw.length === 0) raw = loadRawJSON('mock_picks.json');
  // If real picks reference film_id format, ensure consistency

  _picks = normalizePicks(raw);
  return _picks;
}

export function getFilms(): Film[] {
  if (_films) return _films;
  const picks = getPicks();
  const pickCounts = new Map<string, number>();
  for (const p of picks) {
    // Skip box set aggregate entries from pick counts
    if (p.box_set_film_count) continue;
    pickCounts.set(p.film_slug, (pickCounts.get(p.film_slug) || 0) + 1);
  }

  // Prefer pipeline data (criterion_catalog.json) over mock data
  let raw = loadRawJSON('criterion_catalog.json');
  if (raw.length === 0) raw = loadRawJSON('films.json');
  if (raw.length === 0) raw = loadRawJSON('mock_films.json');

  const allFilms = normalizeFilms(raw, pickCounts);

  // If loading from full catalog (1000+ entries), filter to only picked films
  if (allFilms.length > 500) {
    const pickedSlugs = new Set(picks.map((p) => p.film_slug));
    _films = allFilms.filter((f) => pickedSlugs.has(f.slug));
  } else {
    _films = allFilms;
  }
  return _films;
}

export function getGuests(): Guest[] {
  if (_guests) return _guests;
  let raw = loadRawJSON('guests.json');
  if (raw.length === 0) raw = loadRawJSON('mock_guests.json');
  _guests = raw as Guest[];
  return _guests;
}

export function getGuestBySlug(slug: string): Guest | undefined {
  return getGuests().find(g => g.slug === slug);
}

let _guestNameMap: Map<string, string> | null = null;
export function getGuestByName(name: string): Guest | undefined {
  if (!_guestNameMap) {
    _guestNameMap = new Map();
    for (const g of getGuests()) {
      _guestNameMap.set(g.name.toLowerCase(), g.slug);
    }
  }
  const slug = _guestNameMap.get(name.toLowerCase());
  return slug ? getGuestBySlug(slug) : undefined;
}

export function getFilmBySlug(slug: string): Film | undefined {
  return getFilms().find(f => f.slug === slug);
}

export function getPicksForGuest(guestSlug: string): (Pick & { film: Film | undefined })[] {
  const films = getFilms();
  return getPicks()
    .filter(p => p.guest_slug === guestSlug)
    .map(p => ({
      ...p,
      film: films.find(f => f.slug === p.film_slug),
    }));
}

export function getPicksForFilm(filmSlug: string): (Pick & { guest: Guest | undefined })[] {
  const guests = getGuests();
  return getPicks()
    .filter(p => p.film_slug === filmSlug && !p.box_set_film_count)
    .map(p => ({
      ...p,
      guest: guests.find(g => g.slug === p.guest_slug),
    }));
}

export function getFilmsSortedByPickCount(): Film[] {
  return [...getFilms()].sort((a, b) => b.pick_count - a.pick_count);
}

export function getAllProfessions(): string[] {
  const professions = new Set(getGuests().map(g => g.profession).filter(Boolean));
  return [...professions].sort();
}

export function getAllDecades(): string[] {
  const decades = new Set(
    getFilms().map(f => `${Math.floor(f.year / 10) * 10}s`)
  );
  return [...decades].sort();
}

export function getAllGenres(): string[] {
  const genres = new Set(getFilms().flatMap(f => f.genres));
  return [...genres].sort();
}

export function getStats() {
  return {
    totalGuests: getGuests().length,
    totalFilms: getFilms().length,
    totalPicks: getPicks().length,
  };
}
