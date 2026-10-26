/**
 * Titles and meta descriptions for each page type, built from the data.
 *
 * Search engines show the <title> as the result's headline and usually show
 * the meta description under it, so both name the specific guest or film and
 * what the page holds. Every indexable page needs a unique title; the SEO gate
 * (tests/built/seo.test.ts) fails the build when two pages share one.
 */
import type { Film, Guest, Pick } from './data';

export const SITE_NAME = 'Closet Picks';

/** Google cuts descriptions at roughly this many characters on desktop. */
const DESCRIPTION_BUDGET = 160;

/**
 * "Aaron Sorkin" → "Aaron Sorkin's"; "Barry Jenkins" → "Barry Jenkins'".
 * Names ending in s take a bare apostrophe, so plural group names such as
 * "The Quay Brothers" read correctly too.
 */
export function possessive(name: string): string {
  return /s$/i.test(name) ? `${name}'` : `${name}'s`;
}

/** ["A"] → "A"; ["A", "B"] → "A and B"; ["A", "B", "C"] → "A, B, and C". */
export function joinList(items: string[]): string {
  if (items.length <= 1) return items.join('');
  if (items.length === 2) return `${items[0]} and ${items[1]}`;
  return `${items.slice(0, -1).join(', ')}, and ${items[items.length - 1]}`;
}

/**
 * Names as many of `items` as fit in `budget` characters and counts the rest,
 * out of `total`: "A, B, C, and 4 more". Always names at least one item.
 */
export function listWithin(items: string[], total: number, budget: number): { text: string; shown: number } {
  const render = (n: number) => {
    const rest = total - n;
    return rest > 0 ? joinList([...items.slice(0, n), `${rest} more`]) : joinList(items.slice(0, n));
  };
  let shown = items.length;
  while (shown > 1 && render(shown).length > budget) shown--;
  return { text: render(shown), shown };
}

/** The quote shown for a pick counts only when extraction found one. */
export function hasQuote(pick: Pick): boolean {
  return !!(pick.quote && pick.quote.trim() !== '' && pick.extraction_confidence !== 'none');
}

/** A closing sentence about the quotes, worded so it holds for every quote listed. */
function quoteSentence(picks: Pick[]): string {
  const quoted = picks.filter(hasQuote);
  if (quoted.length === 0) return '';
  const timestamped = quoted.every((p) => p.youtube_timestamp_url || p.vimeo_timestamp_url);
  return timestamped ? ' Each quote links to the moment it was said.' : ' With quotes from the episode.';
}

function plural(n: number, one: string, many = `${one}s`): string {
  return n === 1 ? one : many;
}

/**
 * Fills the "{list}" slot in `template` with as many names as fit the
 * description budget, then adds `tail` if it still fits. Names are the specific
 * part, so they get the room first: the tail is dropped rather than cut the
 * list below three names.
 */
function describe(template: string, names: string[], total: number, tail: string): string {
  const fixed = template.length - '{list}'.length;
  const withTail = listWithin(names, total, DESCRIPTION_BUDGET - fixed - tail.length);
  if (tail && withTail.shown >= Math.min(3, names.length)) {
    return template.replace('{list}', withTail.text) + tail;
  }
  return template.replace('{list}', listWithin(names, total, DESCRIPTION_BUDGET - fixed).text);
}

// --- Guest pages ---

export function guestTitle(guest: Guest): string {
  return `${possessive(guest.name)} Criterion Closet Picks`;
}

/** `picks` are the picks listed on the guest page, in page order. */
export function guestDescription(guest: Guest, picks: (Pick & { film?: Film })[]): string {
  if (picks.length === 0) {
    return `${possessive(guest.name)} visit to the Criterion Closet, with the full episode.`;
  }
  const names = picks.map((p) =>
    p.box_set_film_count ? (p.box_set_name || p.film?.title || p.film_slug) : (p.film?.title || p.film_slug),
  );
  const sets = picks.filter((p) => p.box_set_film_count).length;
  const films = picks.length - sets;
  const noun = sets === 0 ? plural(films, 'film') : films === 0 ? plural(sets, 'box set') : 'films and box sets';
  const count = picks.length === 1 ? 'a' : String(picks.length);
  return describe(
    `${guest.name} picked ${count} ${noun} from the Criterion Closet: {list}.`,
    names,
    picks.length,
    quoteSentence(picks),
  );
}

// --- Film pages ---

/** "Roma (2018) by Alfonso Cuarón". Leaves out a year or director the data lacks. */
export function filmHeading(film: Film): string {
  const year = film.year ? ` (${film.year})` : '';
  const director = film.director ? ` by ${film.director}` : '';
  return `${film.title}${year}${director}`;
}

/**
 * `picks` are the guests' picks listed under "Picked by", in page order;
 * `pickCount` is the guest count in the page header.
 */
export function filmDescription(film: Film, picks: (Pick & { guest?: Guest })[], pickCount: number): string {
  return pickedByDescription(filmHeading(film), picks, pickCount);
}

/** Box-set pages: "The Apu Trilogy, a box set of 3 films, was picked by …". */
export function boxSetDescription(boxSet: Film, filmCount: number, picks: (Pick & { guest?: Guest })[], pickCount: number): string {
  const subject = filmCount > 0 ? `${boxSet.title}, a box set of ${filmCount} films,` : `${boxSet.title}, a box set,`;
  return pickedByDescription(subject, picks, pickCount);
}

function pickedByDescription(subject: string, picks: (Pick & { guest?: Guest })[], pickCount: number): string {
  const names = [...new Set(picks.map((p) => p.guest?.name).filter((n): n is string => !!n))];
  const total = Math.max(pickCount, names.length);
  const tail = quoteSentence(picks);
  if (names.length === 0) {
    return `${subject} was picked by ${total} ${plural(total, 'guest')} in the Criterion Closet.${tail}`;
  }
  if (total === 1) {
    return `${subject} was picked in the Criterion Closet by ${names[0]}.${tail}`;
  }
  return describe(`${subject} was picked by ${total} guests in the Criterion Closet: {list}.`, names, total, tail);
}

// --- Index pages ---

/** Names the three most common professions with their counts. */
export function guestsIndexDescription(guests: Guest[]): string {
  const counts = new Map<string, number>();
  for (const g of guests) {
    if (g.profession && g.profession !== 'other') counts.set(g.profession, (counts.get(g.profession) ?? 0) + 1);
  }
  const top = [...counts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, 3)
    .map(([profession, n]) => `${n} ${plural(n, profession)}`);
  const including = top.length > 0 ? `, including ${joinList(top)},` : '';
  return `All ${guests.length} guests who have visited the Criterion Closet${including} with every film each one picked.`;
}

export function filmsIndexDescription(films: Film[]): string {
  const years = films.map((f) => f.year).filter((y): y is number => !!y);
  const range = years.length > 1 ? `, from ${Math.min(...years)} to ${Math.max(...years)},` : '';
  return `All ${films.length} films picked in the Criterion Closet${range} with the guests who picked each one. Filter by decade and genre.`;
}

/** `films` sorted by pick count, most picked first. */
export function mostPopularDescription(films: Film[]): string {
  const top = films.slice(0, 3).map((f) => `${f.title} (${f.pick_count} ${plural(f.pick_count, 'pick')})`);
  return `The ${films.length} films picked in the Criterion Closet, ranked by how many guests chose each one. The top three: ${joinList(top)}.`;
}

// --- Structured data (JSON-LD) ---

/**
 * A CollectionPage whose main entity is the full list the page links to, in
 * page order. `site` is the absolute site root, e.g. Astro.site.
 */
export function collectionPageData(opts: {
  site: URL;
  path: string;
  name: string;
  description: string;
  items: { name: string; path: string }[];
  order: 'ascending' | 'descending';
}): Record<string, unknown>[] {
  const url = new URL(opts.path, opts.site).href;
  return [
    {
      '@context': 'https://schema.org',
      '@type': 'CollectionPage',
      name: opts.name,
      url,
      description: opts.description,
      isPartOf: { '@type': 'WebSite', name: SITE_NAME, url: opts.site.href },
      mainEntity: {
        '@type': 'ItemList',
        numberOfItems: opts.items.length,
        itemListOrder: opts.order === 'ascending'
          ? 'https://schema.org/ItemListOrderAscending'
          : 'https://schema.org/ItemListOrderDescending',
        itemListElement: opts.items.map((item, i) => ({
          '@type': 'ListItem',
          position: i + 1,
          name: item.name,
          url: new URL(item.path, opts.site).href,
        })),
      },
    },
    {
      '@context': 'https://schema.org',
      '@type': 'BreadcrumbList',
      itemListElement: [
        { '@type': 'ListItem', position: 1, name: SITE_NAME, item: opts.site.href },
        { '@type': 'ListItem', position: 2, name: opts.name, item: url },
      ],
    },
  ];
}
