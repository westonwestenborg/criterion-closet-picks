import type { APIRoute } from 'astro';
import { getFilms, getFilmBySlug } from '../../lib/data.js';
import { renderCached } from '../../lib/og/render.js';
import { genericCard } from '../../lib/og/generic-card.js';

// Same curated iconic films as the homepage
const ICONIC_SLUGS = [
  'seven-samurai',
  'fa-yeung-nin-wa',
  'mulholland-dr',
  'paris-texas',
  'the-400-blows',
  'stalker',
  'do-the-right-thing',
  'persona',
];

export const GET: APIRoute = async () => {
  const count = getFilms().length;
  const posters = ICONIC_SLUGS
    .map((slug) => getFilmBySlug(slug))
    .filter((f) => f?.poster_url)
    .map((f) => f!.poster_url!);

  const png = await renderCached(
    'films',
    { page: 'films', count, posters },
    () =>
      genericCard({
        title: 'Films',
        subtitle: `${count} films picked from the Criterion Closet, from classics to hidden gems`,
        posters,
      }),
  );
  return new Response(png, { headers: { 'Content-Type': 'image/png' } });
};
