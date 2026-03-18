import type { APIRoute } from 'astro';
import { getFilmsSortedByPickCount } from '../../lib/data.js';
import { renderCached } from '../../lib/og/render.js';
import { genericCard } from '../../lib/og/generic-card.js';

export const GET: APIRoute = async () => {
  const topFilms = getFilmsSortedByPickCount().slice(0, 6);
  const posters = topFilms.filter((f) => f.poster_url).map((f) => f.poster_url!);

  const png = await renderCached(
    'most-popular',
    { page: 'most-popular', posters },
    () =>
      genericCard({
        title: 'Most Popular',
        subtitle: 'The most-picked films from the Criterion Closet, ranked by number of guests',
        posters,
      }),
  );
  return new Response(png, { headers: { 'Content-Type': 'image/png' } });
};
