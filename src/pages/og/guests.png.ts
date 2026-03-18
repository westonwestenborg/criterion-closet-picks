import type { APIRoute } from 'astro';
import { getPublishableGuests, getRecentGuests } from '../../lib/data.js';
import { renderCached, prepareCircularAvatar } from '../../lib/og/render.js';
import { genericCard } from '../../lib/og/generic-card.js';

export const GET: APIRoute = async () => {
  const count = getPublishableGuests().length;
  const recent = getRecentGuests(8).filter((g) => g.photo_url);

  // Pre-process all avatars into circular PNGs (handles webp conversion too)
  const avatarResults = await Promise.all(
    recent.map((g) => prepareCircularAvatar(g.photo_url!, 120)),
  );
  const avatarDataUris = avatarResults.filter((uri): uri is string => uri !== null);

  const png = await renderCached(
    'guests',
    { page: 'guests', count, avatarCount: avatarDataUris.length },
    () =>
      genericCard({
        title: 'Guests',
        subtitle: `${count} directors, actors, musicians, and more who visited the Criterion Closet`,
        avatarDataUris,
      }),
  );
  return new Response(png, { headers: { 'Content-Type': 'image/png' } });
};
