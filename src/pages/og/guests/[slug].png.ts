import type { APIRoute, GetStaticPaths } from 'astro';
import { getPublishableGuests, getGuestBySlug, getDisplayablePicksForGuest } from '../../../lib/data.js';
import { renderCached, prepareCircularAvatar } from '../../../lib/og/render.js';
import { guestCard } from '../../../lib/og/guest-card.js';

export const getStaticPaths: GetStaticPaths = () => {
  return getPublishableGuests().map((guest) => ({ params: { slug: guest.slug } }));
};

export const GET: APIRoute = async ({ params }) => {
  const guest = getGuestBySlug(params.slug!);
  if (!guest) return new Response('Not found', { status: 404 });

  const picks = getDisplayablePicksForGuest(guest.slug);

  // Pre-process avatar into a circular PNG data URI
  const photoUrl =
    guest.photo_url ||
    (guest.youtube_video_id
      ? `https://img.youtube.com/vi/${guest.youtube_video_id}/mqdefault.jpg`
      : null);
  const avatarDataUri = photoUrl ? await prepareCircularAvatar(photoUrl, 150) : null;

  try {
    const png = await renderCached(
      `guests/${guest.slug}`,
      { slug: guest.slug, name: guest.name, profession: guest.profession, photo_url: guest.photo_url, youtube_video_id: guest.youtube_video_id, pick_count: picks.length, top_films: picks.filter(p => p.film?.poster_url && !p.box_set_film_count).slice(0, 6).map(p => p.film!.poster_url) },
      () => guestCard(guest, picks, avatarDataUri),
    );
    return new Response(png, { headers: { 'Content-Type': 'image/png' } });
  } catch {
    const { genericCard } = await import('../../../lib/og/generic-card.js');
    const png = await renderCached(
      `guests/${guest.slug}-fallback`,
      { slug: guest.slug, name: guest.name },
      () => genericCard({ title: guest.name, subtitle: guest.profession }),
    );
    return new Response(png, { headers: { 'Content-Type': 'image/png' } });
  }
};
