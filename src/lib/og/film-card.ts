import type { Film } from '../data.js';
import { BG, TEXT, MUTED, brandingFooter, pickBadge } from './render.js';

// Criterion posters are ~4:5 (1288x1600), TMDB are 2:3 (185x278)
// Use 4:5 since Criterion is the primary poster source
const POSTER_H = 480;
const POSTER_W = 384; // 4:5 ratio

function posterImage(film: Film) {
  if (film.poster_url) {
    return {
      type: 'div',
      props: {
        style: {
          display: 'flex',
          width: POSTER_W,
          height: POSTER_H,
          borderRadius: 6,
          overflow: 'hidden',
          flexShrink: 0,
        },
        children: [
          {
            type: 'img',
            props: {
              src: film.poster_url,
              width: POSTER_W,
              height: POSTER_H,
            },
          },
        ],
      },
    };
  }
  return {
    type: 'div',
    props: {
      style: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        width: POSTER_W,
        height: POSTER_H,
        backgroundColor: '#333',
        borderRadius: 6,
        color: '#fff',
        fontSize: 32,
        fontWeight: 700,
        flexShrink: 0,
      },
      children: film.criterion_spine_number ? `#${film.criterion_spine_number}` : '',
    },
  };
}

function dynamicTitleSize(title: string): number {
  if (title.length > 60) return 30;
  if (title.length > 40) return 36;
  if (title.length > 25) return 42;
  return 50;
}

export function filmCard(film: Film, guestNames?: string[]) {
  const titleSize = dynamicTitleSize(film.title);
  const spineText = film.criterion_spine_number ? `Spine #${film.criterion_spine_number}` : null;

  const pickedByNames = guestNames && guestNames.length > 0
    ? guestNames.length <= 4
      ? guestNames.join(', ')
      : `${guestNames.slice(0, 4).join(', ')} + ${guestNames.length - 4} more`
    : null;

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
        {
          type: 'div',
          props: {
            style: {
              display: 'flex',
              flex: 1,
              gap: 40,
            },
            children: [
              posterImage(film),
              {
                type: 'div',
                props: {
                  style: {
                    display: 'flex',
                    flexDirection: 'column',
                    flex: 1,
                    justifyContent: 'center',
                  },
                  children: [
                    {
                      type: 'div',
                      props: {
                        style: {
                          fontSize: titleSize,
                          fontWeight: 700,
                          color: TEXT,
                          lineHeight: 1.15,
                        },
                        children: film.title,
                      },
                    },
                    {
                      type: 'div',
                      props: {
                        style: {
                          fontSize: 26,
                          color: MUTED,
                          marginTop: 12,
                        },
                        children: [film.director, film.year].filter(Boolean).join(', '),
                      },
                    },
                    ...(spineText
                      ? [
                          {
                            type: 'div',
                            props: {
                              style: {
                                fontSize: 22,
                                color: MUTED,
                                marginTop: 8,
                              },
                              children: spineText,
                            },
                          },
                        ]
                      : []),
                    {
                      type: 'div',
                      props: {
                        style: { display: 'flex', marginTop: 20 },
                        children: [pickBadge(film.pick_count)],
                      },
                    },
                    ...(pickedByNames
                      ? [
                          {
                            type: 'div',
                            props: {
                              style: {
                                fontSize: 18,
                                color: MUTED,
                                fontStyle: 'italic',
                                marginTop: 16,
                                lineHeight: 1.4,
                              },
                              children: `— ${pickedByNames}`,
                            },
                          },
                        ]
                      : []),
                  ],
                },
              },
            ],
          },
        },
        brandingFooter(),
      ],
    },
  };
}
