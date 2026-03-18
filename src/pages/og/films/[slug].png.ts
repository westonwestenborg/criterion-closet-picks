import type { APIRoute, GetStaticPaths } from 'astro';
import { getFilms, getFilmBySlug, getPicksForFilm } from '../../../lib/data.js';
import { renderCached } from '../../../lib/og/render.js';
import { filmCard } from '../../../lib/og/film-card.js';

export const getStaticPaths: GetStaticPaths = () => {
  return getFilms().map((film) => ({ params: { slug: film.slug } }));
};

export const GET: APIRoute = async ({ params }) => {
  const film = getFilmBySlug(params.slug!);
  if (!film) return new Response('Not found', { status: 404 });

  const picks = getPicksForFilm(film.slug);
  const guestNames = picks
    .filter((p) => p.guest)
    .map((p) => p.guest!.name);

  try {
    const png = await renderCached(
      `films/${film.slug}`,
      { slug: film.slug, title: film.title, director: film.director, year: film.year, pick_count: film.pick_count, poster_url: film.poster_url, spine: film.criterion_spine_number, genres: film.genres, guests: guestNames },
      () => filmCard(film, guestNames),
    );
    return new Response(png, { headers: { 'Content-Type': 'image/png' } });
  } catch {
    const { genericCard } = await import('../../../lib/og/generic-card.js');
    const png = await renderCached(
      `films/${film.slug}-fallback`,
      { slug: film.slug, title: film.title },
      () => genericCard({ title: film.title, subtitle: `${film.director}, ${film.year}` }),
    );
    return new Response(png, { headers: { 'Content-Type': 'image/png' } });
  }
};
