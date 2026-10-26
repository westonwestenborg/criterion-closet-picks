import { describe, expect, it } from 'vitest';
import {
  boxSetDescription,
  filmDescription,
  filmHeading,
  guestDescription,
  guestsIndexDescription,
  guestTitle,
  joinList,
  listWithin,
  possessive,
} from '../src/lib/seo';
import type { Film, Guest, Pick } from '../src/lib/data';

function guest(overrides: Partial<Guest> = {}): Guest {
  return {
    name: 'Aaron Sorkin',
    slug: 'aaron-sorkin',
    profession: 'writer',
    photo_url: null,
    youtube_video_id: 'vid',
    youtube_video_url: 'https://www.youtube.com/watch?v=vid',
    vimeo_video_id: null,
    episode_date: '2026-10-09',
    criterion_page_url: null,
    pick_count: 0,
    ...overrides,
  };
}

function film(overrides: Partial<Film> = {}): Film {
  return {
    title: 'Roma',
    slug: 'roma-2018',
    criterion_spine_number: 1014,
    director: 'Alfonso Cuarón',
    year: 2018,
    country: '',
    genres: ['Drama'],
    criterion_url: '',
    imdb_id: null,
    imdb_url: null,
    tmdb_id: null,
    tmdb_url: null,
    letterboxd_url: null,
    poster_url: null,
    pick_count: 1,
    ...overrides,
  };
}

function pick(overrides: Partial<Pick> & { film?: Film; guest?: Guest } = {}): Pick & { film?: Film; guest?: Guest } {
  return {
    guest_slug: 'aaron-sorkin',
    film_slug: 'roma-2018',
    quote: 'A quote.',
    start_timestamp_seconds: 10,
    youtube_timestamp_url: 'https://www.youtube.com/watch?v=vid&t=10',
    extraction_confidence: 'high',
    ...overrides,
  };
}

describe('titles and descriptions', () => {
  it('forms possessives, with a bare apostrophe after a final s', () => {
    expect(possessive('Aaron Sorkin')).toBe("Aaron Sorkin's");
    expect(possessive('Barry Jenkins')).toBe("Barry Jenkins'");
    expect(possessive('The Quay Brothers')).toBe("The Quay Brothers'");
    expect(guestTitle(guest())).toBe("Aaron Sorkin's Criterion Closet Picks");
  });

  it('joins lists and counts what does not fit', () => {
    expect(joinList(['A'])).toBe('A');
    expect(joinList(['A', 'B'])).toBe('A and B');
    expect(joinList(['A', 'B', 'C'])).toBe('A, B, and C');
    expect(listWithin(['Alpha', 'Beta', 'Gamma', 'Delta'], 6, 30).text).toBe('Alpha, Beta, Gamma, and 3 more');
    expect(listWithin(['Alpha', 'Beta'], 2, 3).text).toBe('Alpha and 1 more');
  });

  it('names the year and director in a film heading only when the data has them', () => {
    expect(filmHeading(film())).toBe('Roma (2018) by Alfonso Cuarón');
    expect(filmHeading(film({ year: null, director: '' }))).toBe('Roma');
  });

  it('describes a guest page from its picks', () => {
    const picks = ['Tanner ’88', 'Broadcast News', 'The Graduate', 'Network', 'All the President’s Men']
      .map((title, i) => pick({ film_slug: `f${i}`, film: film({ title, slug: `f${i}` }) }));
    expect(guestDescription(guest(), picks)).toBe(
      'Aaron Sorkin picked 5 films from the Criterion Closet: Tanner ’88, Broadcast News, The Graduate, and 2 more. ' +
      'Each quote links to the moment it was said.',
    );
  });

  it('says "a film" for one pick, names box sets, and makes no claim about quotes it lacks', () => {
    expect(guestDescription(guest(), [pick({ film: film(), quote: '' })])).toBe(
      'Aaron Sorkin picked a film from the Criterion Closet: Roma.',
    );
    const mixed = [
      pick({ film: film() }),
      pick({ film_slug: 'apu', box_set_film_count: 3, box_set_name: 'The Apu Trilogy', quote: '' }),
    ];
    expect(guestDescription(guest(), mixed)).toMatch(/^Aaron Sorkin picked 2 films and box sets from the Criterion Closet: Roma and The Apu Trilogy\./);
  });

  it('describes a film page by who picked it, with correct grammar for one guest', () => {
    const one = [pick({ guest: guest({ name: 'Brady Corbet' }) })];
    expect(filmDescription(film({ title: 'Bergman Island', year: 2021, director: 'Mia Hansen-Løve' }), one, 1)).toBe(
      'Bergman Island (2021) by Mia Hansen-Løve was picked in the Criterion Closet by Brady Corbet. Each quote links to the moment it was said.',
    );
    const many = ['Guillermo del Toro', 'Bill Hader', 'Lucy Liu', 'JR'].map((name) => pick({ guest: guest({ name }) }));
    expect(filmDescription(film(), many, 9)).toMatch(
      /^Roma \(2018\) by Alfonso Cuarón was picked by 9 guests in the Criterion Closet: Guillermo del Toro, Bill Hader, Lucy Liu, .*more\./,
    );
  });

  it('keeps descriptions within 160 characters when the subject leaves room', () => {
    const names = Array.from({ length: 30 }, (_, i) => `Guest Number ${i}`);
    const picks = names.map((name) => pick({ guest: guest({ name }) }));
    expect(filmDescription(film(), picks, 30).length).toBeLessThanOrEqual(160);
    expect(boxSetDescription(film({ title: 'The Apu Trilogy' }), 3, picks, 30).length).toBeLessThanOrEqual(160);
  });

  it('describes the guest index by its most common professions', () => {
    const guests = [
      ...Array.from({ length: 3 }, () => guest({ profession: 'director' })),
      ...Array.from({ length: 2 }, () => guest({ profession: 'actor' })),
      guest({ profession: 'musician' }),
      guest({ profession: 'other' }),
    ];
    expect(guestsIndexDescription(guests)).toBe(
      'All 7 guests who have visited the Criterion Closet, including 3 directors, 2 actors, and 1 musician, with every film each one picked.',
    );
  });
});
