import type { APIRoute } from 'astro';
import { getPublishableGuests, getRecentGuests } from '../../lib/data.js';
import { renderCached, prepareCircularAvatar } from '../../lib/og/render.js';
import { genericCard } from '../../lib/og/generic-card.js';

export const GET: APIRoute = async () => {
  const count = getPublishableGuests().length;
  const photoUrls = getRecentGuests(8)
    .map((g) => g.photo_url)
    .filter((url): url is string => !!url);

  // Keyed on which photos, not how many. The old signature counted avatars,
  // so swapping who the recent guests are kept the count at 8 and served the
  // previous faces. Fetching inside the callback also keeps a cache hit off
  // the network (handles webp conversion too).
  const png = await renderCached(
    'guests',
    { page: 'guests', count, photoUrls },
    async () => {
      const results = await Promise.all(photoUrls.map((url) => prepareCircularAvatar(url, 120)));
      return genericCard({
        title: 'Guests',
        subtitle: `${count} directors, actors, musicians, and more who visited the Criterion Closet`,
        avatarDataUris: results.filter((uri): uri is string => uri !== null),
      });
    },
  );
  return new Response(png, { headers: { 'Content-Type': 'image/jpeg' } });
};
