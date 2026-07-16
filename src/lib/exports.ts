import {
  getFilms,
  getPublishableGuests,
  getStats,
  getSupportedPicks,
  type Film,
  type Guest,
  type Pick,
} from './data';

export interface ExportEnvelope<T> {
  generated_at: string;
  source: string;
  count: number;
  data: T[];
}

const source = 'https://closetpicks.westenb.org/';

function generatedAt(): string {
  return new Date().toISOString();
}

export function buildExportEnvelope<T>(data: T[]): ExportEnvelope<T> {
  return {
    generated_at: generatedAt(),
    source,
    count: data.length,
    data,
  };
}

export function getGuestExport(): ExportEnvelope<Guest> {
  return buildExportEnvelope(getPublishableGuests().sort((a, b) => a.name.localeCompare(b.name)));
}

export function getFilmExport(): ExportEnvelope<Film> {
  return buildExportEnvelope(getFilms().sort((a, b) => a.title.localeCompare(b.title)));
}

export function getPickExport(): ExportEnvelope<Pick> {
  return buildExportEnvelope(getSupportedPicks());
}

export function buildMarkdownExport(): string {
  const guests = getPublishableGuests().sort((a, b) => a.name.localeCompare(b.name));
  const films = getFilms();
  const filmBySlug = new Map(films.map((film) => [film.slug, film]));
  const picks = getSupportedPicks();
  const picksByGuest = new Map<string, Pick[]>();
  const stats = getStats();

  for (const pick of picks) {
    const guestPicks = picksByGuest.get(pick.guest_slug) ?? [];
    guestPicks.push(pick);
    picksByGuest.set(pick.guest_slug, guestPicks);
  }

  const lines = [
    '# Criterion Closet Picks - Complete Database',
    `Generated: ${generatedAt()} | ${stats.totalGuests} guests | ${stats.totalFilms} films | ${stats.totalPicks} picks`,
    '',
  ];

  for (const guest of guests) {
    const guestPicks = picksByGuest.get(guest.slug) ?? [];
    const episodeYear = guest.episode_date ? new Date(guest.episode_date).getFullYear() : 'Unknown';

    lines.push(`## ${guest.name} (${guest.profession})`);
    const sourceLinks = [`Aired: ${episodeYear}`];
    const visits = guest.visits ?? [];

    if (visits.length >= 2) {
      for (const [index, visit] of visits.entries()) {
        const url = visit.youtube_video_url || (visit.youtube_video_id ? `https://www.youtube.com/watch?v=${visit.youtube_video_id}` : null);
        if (url) sourceLinks.push(`[Visit ${index + 1} Video](${url})`);
      }
    } else {
      if (guest.youtube_video_url) sourceLinks.push(`[Full Video](${guest.youtube_video_url})`);
      if (guest.criterion_page_url) sourceLinks.push(`[Criterion Page](${guest.criterion_page_url})`);
    }

    lines.push(sourceLinks.join(' | '), '');

    for (const pick of guestPicks) {
      if (pick.box_set_film_count) {
        const count = pick.box_set_film_count > 0 ? ` (${pick.box_set_film_count} films)` : '';
        const criterion = pick.box_set_criterion_url ? ` - [Criterion](${pick.box_set_criterion_url})` : '';
        lines.push(`- **${pick.box_set_name}**${count} [Box Set]${criterion}`);
      } else {
        const film = filmBySlug.get(pick.film_slug);
        if (!film) continue;
        const spine = film.criterion_spine_number ? ` - Spine #${film.criterion_spine_number}` : '';
        const criterion = film.criterion_url ? ` - [Criterion](${film.criterion_url})` : '';
        const imdb = film.imdb_url ? ` - [IMDB](${film.imdb_url})` : '';
        lines.push(`- **${film.title}** (${film.year}) - ${film.director}${spine}${criterion}${imdb}`);
      }

      if (pick.quote) lines.push(`  > "${pick.quote}"`);
      if (pick.youtube_timestamp_url) lines.push(`  [Watch this moment](${pick.youtube_timestamp_url})`);
      lines.push('');
    }
  }

  return `${lines.join('\n')}\n`;
}
