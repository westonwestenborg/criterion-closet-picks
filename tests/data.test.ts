import { afterEach, describe, expect, it } from 'vitest';
import { makeFixtureDir } from './helpers/fixtures';
import {
  getBoxSetInfoForFilm,
  getDisplayablePicksForGuest,
  getFilms,
  getPicks,
  getPicksForFilm,
  getPublishableGuests,
  getRecentGuests,
  isGuestPublishable,
} from '../src/lib/data';
import type { Guest } from '../src/lib/data';

let cleanup: (() => void) | null = null;
afterEach(() => {
  cleanup?.();
  cleanup = null;
});

function guest(overrides: Partial<Guest> & { slug: string }): Guest {
  return {
    name: overrides.slug,
    profession: 'Director',
    photo_url: null,
    youtube_video_id: '',
    youtube_video_url: '',
    vimeo_video_id: null,
    episode_date: '',
    criterion_page_url: null,
    pick_count: 0,
    ...overrides,
  } as Guest;
}

describe('pick normalization', () => {
  it('maps pipeline field names to frontend names with defaults', () => {
    cleanup = makeFixtureDir({
      picks: [
        {
          guest_slug: 'test-guest-alpha',
          film_id: 'seven-samurai-1954',
          start_timestamp: 42,
        },
      ],
    });
    const [pick] = getPicks();
    expect(pick.film_slug).toBe('seven-samurai-1954');
    expect(pick.start_timestamp_seconds).toBe(42);
    expect(pick.quote).toBe('');
    expect(pick.extraction_confidence).toBe('none');
    expect(pick.source).toBeUndefined();
  });
});

describe('film normalization', () => {
  it('maps spine_number and constructs external URLs from ids', () => {
    cleanup = makeFixtureDir({
      picks: [{ guest_slug: 'test-guest-alpha', film_id: 'm-1931', quote: 'classic', extraction_confidence: 'high' }],
      catalog: [
        { film_id: 'm-1931', title: 'M', spine_number: 30, imdb_id: 'tt0022100', tmdb_id: 832 },
        { film_id: 'unlinked-film', title: 'Unlinked' },
      ],
    });
    const films = getFilms();
    const m = films.find((f) => f.slug === 'm-1931')!;
    expect(m.criterion_spine_number).toBe(30);
    expect(m.imdb_url).toBe('https://www.imdb.com/title/tt0022100/');
    expect(m.tmdb_url).toBe('https://www.themoviedb.org/movie/832');
    expect(m.letterboxd_url).toBe('https://letterboxd.com/tmdb/832');
    const unlinked = films.find((f) => f.slug === 'unlinked-film')!;
    expect(unlinked.imdb_url).toBeNull();
    expect(unlinked.tmdb_url).toBeNull();
    expect(unlinked.letterboxd_url).toBeNull();
  });
});

describe('pick-count rules', () => {
  it('counts criterion-sourced quoteless picks but not other quoteless picks or aggregates', () => {
    cleanup = makeFixtureDir({
      picks: [
        // counted: has a quote
        { guest_slug: 'g1', film_id: 'film-a', quote: 'love it', extraction_confidence: 'high' },
        // counted: criterion-sourced, even without a quote
        { guest_slug: 'g2', film_id: 'film-a', source: 'criterion' },
        // not counted: quoteless and not criterion-sourced
        { guest_slug: 'g3', film_id: 'film-a' },
        // not counted: box set aggregate
        { guest_slug: 'g4', film_id: 'film-a', quote: 'set quote', extraction_confidence: 'high', box_set_film_count: 3, box_set_name: 'Set' },
      ],
      catalog: [{ film_id: 'film-a', title: 'Film A' }],
    });
    expect(getFilms().find((f) => f.slug === 'film-a')!.pick_count).toBe(2);
  });

  it('prefers an explicit pick_count on the catalog entry', () => {
    cleanup = makeFixtureDir({
      picks: [{ guest_slug: 'g1', film_id: 'film-a', quote: 'q', extraction_confidence: 'high' }],
      catalog: [{ film_id: 'film-a', title: 'Film A', pick_count: 7 }],
    });
    expect(getFilms().find((f) => f.slug === 'film-a')!.pick_count).toBe(7);
  });
});

describe('catalog filtering', () => {
  it('filters a large catalog (>500) down to picked films', () => {
    const catalog = Array.from({ length: 501 }, (_, i) => ({
      film_id: `filler-${i}`,
      title: `Filler ${i}`,
    }));
    catalog.push({ film_id: 'picked-film', title: 'Picked' });
    cleanup = makeFixtureDir({
      picks: [{ guest_slug: 'g1', film_id: 'picked-film', quote: 'q', extraction_confidence: 'high' }],
      catalog,
    });
    const films = getFilms();
    expect(films).toHaveLength(1);
    expect(films[0].slug).toBe('picked-film');
  });

  it('excludes box sets unless referenced as individual films', () => {
    cleanup = makeFixtureDir({
      picks: [
        // aggregate-only reference to the box set entry
        { guest_slug: 'g1', film_id: 'wcp-box', quote: 'q', extraction_confidence: 'high', box_set_film_count: 3, box_set_name: 'WCP' },
      ],
      catalog: [
        { film_id: 'wcp-box', title: 'WCP', is_box_set: true },
        { film_id: 'regular-film', title: 'Regular' },
      ],
    });
    const slugs = getFilms().map((f) => f.slug);
    expect(slugs).not.toContain('wcp-box'); // only referenced as aggregate
    expect(slugs).toContain('regular-film'); // small catalog keeps unpicked non-box-sets
  });
});

describe('getPicksForFilm', () => {
  it('deduplicates multiple picks by the same guest', () => {
    cleanup = makeFixtureDir({
      picks: [
        { guest_slug: 'g1', film_id: 'film-a', quote: 'visit one', extraction_confidence: 'high', visit_index: 1 },
        { guest_slug: 'g1', film_id: 'film-a', quote: 'visit two', extraction_confidence: 'high', visit_index: 2 },
      ],
      guests: [guest({ slug: 'g1' })],
      catalog: [{ film_id: 'film-a', title: 'Film A' }],
    });
    const picks = getPicksForFilm('film-a');
    expect(picks).toHaveLength(1);
    expect(picks[0].guest?.slug).toBe('g1');
  });

  it('includes quoted box-set aggregate guests; direct picks win over aggregates', () => {
    cleanup = makeFixtureDir({
      picks: [
        // tags film-a as part of WCP and shows directly (quoted)
        { guest_slug: 'g1', film_id: 'film-a', quote: 'direct', extraction_confidence: 'high', is_box_set: true, box_set_name: 'WCP' },
        // aggregate pick of WCP with a quote -> appears on film-a's page
        { guest_slug: 'g2', film_id: 'wcp-box', quote: 'whole set', extraction_confidence: 'high', box_set_film_count: 3, box_set_name: 'WCP' },
        // has both: direct quoted pick and an aggregate -> appears once, direct wins
        { guest_slug: 'g3', film_id: 'film-a', quote: 'mine', extraction_confidence: 'high', is_box_set: true, box_set_name: 'WCP' },
        { guest_slug: 'g3', film_id: 'wcp-box', quote: 'set too', extraction_confidence: 'high', box_set_film_count: 3, box_set_name: 'WCP' },
      ],
      guests: [guest({ slug: 'g1' }), guest({ slug: 'g2' }), guest({ slug: 'g3' })],
      catalog: [{ film_id: 'film-a', title: 'Film A' }],
    });
    const picks = getPicksForFilm('film-a');
    const bySlug = new Map(picks.map((p) => [p.guest_slug, p]));
    expect(picks).toHaveLength(3);
    expect(bySlug.get('g2')!.quote).toBe('whole set');
    expect(bySlug.get('g3')!.quote).toBe('mine'); // direct, not aggregate
  });

  it('skips quoteless picks unless criterion-sourced', () => {
    cleanup = makeFixtureDir({
      picks: [
        { guest_slug: 'g1', film_id: 'film-a' },
        { guest_slug: 'g2', film_id: 'film-a', source: 'criterion' },
      ],
      guests: [guest({ slug: 'g1' }), guest({ slug: 'g2' })],
      catalog: [{ film_id: 'film-a', title: 'Film A' }],
    });
    const picks = getPicksForFilm('film-a');
    expect(picks).toHaveLength(1);
    expect(picks[0].guest_slug).toBe('g2');
  });
});

describe('getBoxSetInfoForFilm', () => {
  it('collects only no-quote guests and falls back through URL sources', () => {
    cleanup = makeFixtureDir({
      picks: [
        // tagged individual pick, quoteless -> establishes box set, counts as no-quote guest
        { guest_slug: 'g1', film_id: 'film-a', is_box_set: true, box_set_name: 'WCP' },
        // aggregate without quote -> counts, and provides the URL
        { guest_slug: 'g2', film_id: 'wcp-box', box_set_film_count: 3, box_set_name: 'WCP', box_set_criterion_url: 'https://www.criterion.com/boxsets/wcp' },
        // aggregate WITH quote -> excluded (already shown via getPicksForFilm)
        { guest_slug: 'g3', film_id: 'wcp-box', quote: 'q', extraction_confidence: 'high', box_set_film_count: 3, box_set_name: 'WCP' },
      ],
    });
    const [info] = getBoxSetInfoForFilm('film-a');
    expect(info.name).toBe('WCP');
    expect(info.criterion_url).toBe('https://www.criterion.com/boxsets/wcp');
    expect(info.guest_slugs.sort()).toEqual(['g1', 'g2']);
    expect(info.guest_count).toBe(2);
  });
});

describe('getDisplayablePicksForGuest', () => {
  it('falls back to criterion-sourced raw picks and suppresses duplicates', () => {
    cleanup = makeFixtureDir({
      picks: [
        { guest_slug: 'test-guest-alpha', film_id: 'film-a', quote: 'processed', extraction_confidence: 'high' },
      ],
      picksRaw: [
        // duplicate of a processed pick -> suppressed
        { guest_slug: 'test-guest-alpha', film_id: 'film-a', film_title: 'Film A', source: 'criterion' },
        // criterion-sourced raw-only pick -> displayed without a quote
        { guest_slug: 'test-guest-alpha', film_id: 'film-b', film_title: 'Film B', source: 'criterion' },
        // non-criterion raw pick -> hidden
        { guest_slug: 'test-guest-alpha', film_id: 'film-c', film_title: 'Film C', source: 'letterboxd' },
      ],
      catalog: [
        { film_id: 'film-a', title: 'Film A' },
        { film_id: 'film-b', title: 'Film B' },
        { film_id: 'film-c', title: 'Film C' },
      ],
    });
    const picks = getDisplayablePicksForGuest('test-guest-alpha');
    const slugs = picks.map((p) => p.film_slug).sort();
    expect(slugs).toEqual(['film-a', 'film-b']);
    const rawPick = picks.find((p) => p.film_slug === 'film-b')!;
    expect(rawPick.quote).toBe('');
    expect(rawPick.extraction_confidence).toBe('none');
    expect(rawPick.film?.title).toBe('Film B');
  });

  it('hides quoteless individual box-set picks and attaches box-set films to aggregates', () => {
    cleanup = makeFixtureDir({
      picks: [
        // individual pick inside a box set, no quote -> hidden (covered by aggregate)
        { guest_slug: 'test-guest-alpha', film_id: 'film-a', source: 'criterion', is_box_set: true, box_set_name: 'WCP' },
        // aggregate matched to catalog entry by criterion_url
        { guest_slug: 'test-guest-alpha', film_id: 'wcp-box', source: 'criterion', box_set_film_count: 3, box_set_name: 'WCP', box_set_criterion_url: 'https://www.criterion.com/boxsets/wcp' },
        // aggregate matched by title fallback (no URL)
        { guest_slug: 'test-guest-alpha', film_id: 'other-box', source: 'criterion', box_set_film_count: 2, box_set_name: 'Other Set' },
      ],
      catalog: [
        { film_id: 'wcp-box', title: 'WCP', is_box_set: true, criterion_url: 'https://www.criterion.com/boxsets/wcp', poster_url: 'wcp.jpg' },
        { film_id: 'other-box', title: 'Other Set', is_box_set: true, poster_url: 'other.jpg' },
      ],
    });
    const picks = getDisplayablePicksForGuest('test-guest-alpha');
    expect(picks.map((p) => p.film_slug).sort()).toEqual(['other-box', 'wcp-box']);
    expect(picks.find((p) => p.film_slug === 'wcp-box')!.film?.poster_url).toBe('wcp.jpg');
    expect(picks.find((p) => p.film_slug === 'other-box')!.film?.poster_url).toBe('other.jpg');
  });

  it('preserves visit_index through normalization and raw fallback', () => {
    cleanup = makeFixtureDir({
      picks: [
        { guest_slug: 'test-guest-alpha', film_id: 'film-a', quote: 'q', extraction_confidence: 'high', visit_index: 2 },
      ],
      picksRaw: [
        { guest_slug: 'test-guest-alpha', film_id: 'film-b', film_title: 'Film B', source: 'criterion', visit_index: 1 },
      ],
      catalog: [
        { film_id: 'film-a', title: 'Film A' },
        { film_id: 'film-b', title: 'Film B' },
      ],
    });
    const picks = getDisplayablePicksForGuest('test-guest-alpha');
    expect(picks.find((p) => p.film_slug === 'film-a')!.visit_index).toBe(2);
    expect(picks.find((p) => p.film_slug === 'film-b')!.visit_index).toBe(1);
  });
});

describe('guest publishability', () => {
  it('requires a criterion page URL or a video on the guest or a visit', () => {
    const byUrl = guest({ slug: 'a', criterion_page_url: 'https://www.criterion.com/shop/collection/1-a' });
    const byVideo = guest({ slug: 'b', youtube_video_id: 'abc123' });
    const byVisit = guest({
      slug: 'c',
      visits: [{ youtube_video_id: null, youtube_video_url: null, vimeo_video_id: 'v1', episode_date: null, criterion_page_url: null }],
    });
    const neither = guest({ slug: 'd' });
    expect(isGuestPublishable(byUrl)).toBe(true);
    expect(isGuestPublishable(byVideo)).toBe(true);
    expect(isGuestPublishable(byVisit)).toBe(true);
    expect(isGuestPublishable(neither)).toBe(false);

    cleanup = makeFixtureDir({ guests: [byUrl, byVideo, byVisit, neither] });
    expect(getPublishableGuests().map((g) => g.slug).sort()).toEqual(['a', 'b', 'c']);
  });
});

describe('getRecentGuests', () => {
  it('sorts by criterion collection id descending and drops guests without URLs', () => {
    cleanup = makeFixtureDir({
      guests: [
        guest({ slug: 'old', criterion_page_url: 'https://www.criterion.com/shop/collection/488-old-picks' }),
        guest({ slug: 'new', criterion_page_url: 'https://www.criterion.com/shop/collection/970-new-picks' }),
        guest({ slug: 'mid', criterion_page_url: 'https://www.criterion.com/shop/collection/720-mid-picks' }),
        guest({ slug: 'no-url' }),
      ],
    });
    expect(getRecentGuests(2).map((g) => g.slug)).toEqual(['new', 'mid']);
    expect(getRecentGuests(10).map((g) => g.slug)).toEqual(['new', 'mid', 'old']);
  });
});
