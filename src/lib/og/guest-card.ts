import type { Guest, Film, Pick } from '../data.js';
import { BG, TEXT, MUTED, BORDER, brandingFooter, pickBadge, posterThumb } from './render.js';

const PHOTO_SIZE = 150;

function guestPhotoMarkup(avatarDataUri: string | null, guestInitial: string) {
  if (avatarDataUri) {
    return {
      type: 'img',
      props: {
        src: avatarDataUri,
        width: PHOTO_SIZE,
        height: PHOTO_SIZE,
        style: { flexShrink: 0 },
      },
    };
  }

  // Initial letter fallback
  return {
    type: 'div',
    props: {
      style: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        width: PHOTO_SIZE,
        height: PHOTO_SIZE,
        borderRadius: PHOTO_SIZE / 2,
        backgroundColor: BORDER,
        color: MUTED,
        fontSize: 56,
        fontStyle: 'italic',
        flexShrink: 0,
      },
      children: guestInitial,
    },
  };
}

function dynamicNameSize(name: string): number {
  if (name.length > 35) return 36;
  if (name.length > 25) return 42;
  return 52;
}

export function guestCard(
  guest: Guest,
  picks: (Pick & { film: Film | undefined })[],
  avatarDataUri: string | null,
) {
  const filmsWithPosters = picks
    .filter((p) => p.film?.poster_url && !p.box_set_film_count)
    .map((p) => p.film!);

  const pickCount = picks.length;
  const nameSize = dynamicNameSize(guest.name);

  const POSTER_H = 250;
  const maxPosters = 6;
  const topPosters = filmsWithPosters.slice(0, maxPosters);

  return {
    type: 'div',
    props: {
      style: {
        display: 'flex',
        flexDirection: 'column',
        width: '100%',
        height: '100%',
        backgroundColor: BG,
        padding: 48,
        fontFamily: 'et-book',
      },
      children: [
        // Top section: photo + name + info
        {
          type: 'div',
          props: {
            style: {
              display: 'flex',
              alignItems: 'center',
              gap: 32,
            },
            children: [
              guestPhotoMarkup(avatarDataUri, guest.name.charAt(0)),
              {
                type: 'div',
                props: {
                  style: {
                    display: 'flex',
                    flexDirection: 'column',
                    flex: 1,
                  },
                  children: [
                    {
                      type: 'div',
                      props: {
                        style: {
                          fontSize: nameSize,
                          fontWeight: 700,
                          color: TEXT,
                          lineHeight: 1.15,
                        },
                        children: guest.name,
                      },
                    },
                    ...(guest.profession
                      ? [
                          {
                            type: 'div',
                            props: {
                              style: {
                                fontSize: 18,
                                color: MUTED,
                                textTransform: 'uppercase' as const,
                                letterSpacing: 2,
                                marginTop: 8,
                              },
                              children: guest.profession,
                            },
                          },
                        ]
                      : []),
                    {
                      type: 'div',
                      props: {
                        style: { display: 'flex', marginTop: 14 },
                        children: [pickBadge(pickCount, `${pickCount} picks`)],
                      },
                    },
                  ],
                },
              },
            ],
          },
        },
        // Poster row
        ...(topPosters.length > 0
          ? [
              {
                type: 'div',
                props: {
                  style: {
                    display: 'flex',
                    gap: 14,
                    marginTop: 28,
                    overflow: 'hidden',
                  },
                  children: topPosters.map((film) =>
                    posterThumb(film.poster_url, film.criterion_spine_number ? `#${film.criterion_spine_number}` : '', POSTER_H),
                  ),
                },
              },
            ]
          : []),
        brandingFooter(),
      ],
    },
  };
}
