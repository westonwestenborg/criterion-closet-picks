import type { APIRoute } from 'astro';
import { getStats, getFilmBySlug } from '../../lib/data.js';
import { renderCached } from '../../lib/og/render.js';
import { genericCard } from '../../lib/og/generic-card.js';

// Curated iconic Criterion films with recognizable cover art
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
  const stats = getStats();
  const posters = ICONIC_SLUGS
    .map((slug) => getFilmBySlug(slug))
    .filter((f) => f?.poster_url)
    .map((f) => f!.poster_url!);

  const png = await renderCached(
    'index',
    { ...stats, posters },
    () =>
      genericCard({
        title: 'Closet Picks',
        subtitle: 'Every pick from the Criterion Collection Closet Picks series',
        stats: [
          { label: 'guests', value: stats.totalGuests },
          { label: 'films', value: stats.totalFilms },
          { label: 'picks', value: stats.totalPicks },
        ],
        posters,
      }),
  );
  return new Response(png, { headers: { 'Content-Type': 'image/png' } });
};
