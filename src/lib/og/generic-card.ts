import { BG, TEXT, MUTED, brandingFooter, posterThumb, avatarCircle } from './render.js';

interface GenericCardOptions {
  title: string;
  subtitle?: string;
  stats?: { label: string; value: string | number }[];
  /** Poster URLs to show in a row */
  posters?: string[];
  /** Pre-processed circular avatar data URIs */
  avatarDataUris?: string[];
}

export function genericCard({ title, subtitle, stats, posters, avatarDataUris }: GenericCardOptions) {
  const hasPosters = posters && posters.length > 0;
  const hasAvatars = avatarDataUris && avatarDataUris.length > 0;
  // When we have stats + visuals, use a smaller poster height to leave room
  const POSTER_H = stats ? 160 : 260;

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
        // Title
        {
          type: 'div',
          props: {
            style: {
              fontSize: 64,
              fontWeight: 700,
              color: TEXT,
              lineHeight: 1.15,
            },
            children: title,
          },
        },
        // Subtitle
        ...(subtitle
          ? [
              {
                type: 'div',
                props: {
                  style: {
                    fontSize: 30,
                    fontStyle: 'italic',
                    color: MUTED,
                    marginTop: 28,
                    lineHeight: 1.3,
                  },
                  children: subtitle,
                },
              },
            ]
          : []),
        // Stats row
        ...(stats && stats.length > 0
          ? [
              {
                type: 'div',
                props: {
                  style: {
                    display: 'flex',
                    gap: 56,
                    marginTop: 48,
                  },
                  children: stats.map((s) => ({
                    type: 'div',
                    props: {
                      style: {
                        display: 'flex',
                        flexDirection: 'column',
                        alignItems: 'center',
                      },
                      children: [
                        {
                          type: 'div',
                          props: {
                            style: {
                              fontSize: 52,
                              fontWeight: 700,
                              color: TEXT,
                            },
                            children: String(s.value),
                          },
                        },
                        {
                          type: 'div',
                          props: {
                            style: {
                              fontSize: 22,
                              color: MUTED,
                              textTransform: 'uppercase' as const,
                              letterSpacing: 2,
                            },
                            children: s.label,
                          },
                        },
                      ],
                    },
                  })),
                },
              },
            ]
          : []),
        // Poster row
        ...(hasPosters
          ? [
              {
                type: 'div',
                props: {
                  style: {
                    display: 'flex',
                    gap: 14,
                    marginTop: 'auto',
                    marginBottom: 20,
                    overflow: 'hidden',
                  },
                  children: posters!.map((url) => posterThumb(url, '', POSTER_H)),
                },
              },
            ]
          : []),
        // Avatar row
        ...(hasAvatars
          ? [
              {
                type: 'div',
                props: {
                  style: {
                    display: 'flex',
                    gap: 18,
                    marginTop: 'auto',
                    marginBottom: 20,
                  },
                  children: avatarDataUris!.map((uri) => avatarCircle(uri, 120)),
                },
              },
            ]
          : []),
        brandingFooter(),
      ],
    },
  };
}
