import satori from 'satori';
import sharp from 'sharp';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { getCached, setCache, hashData } from './cache.js';

// Design tokens matching global.css
export const BG = '#fffff8';
export const TEXT = '#111';
export const MUTED = '#777';
export const BORDER = '#e0e0d8';
export const BADGE_BG = '#2b5797';

const WIDTH = 1200;
const HEIGHT = 630;

let fontsCache: { name: string; data: Buffer; weight: number; style: string }[] | null = null;

function getFonts() {
  if (fontsCache) return fontsCache;
  const dir = join(process.cwd(), 'src', 'assets', 'fonts');
  fontsCache = [
    {
      name: 'et-book',
      data: readFileSync(join(dir, 'et-book-roman-line-figures.ttf')),
      weight: 400,
      style: 'normal' as const,
    },
    {
      name: 'et-book',
      data: readFileSync(join(dir, 'et-book-bold-line-figures.ttf')),
      weight: 700,
      style: 'normal' as const,
    },
    {
      name: 'et-book',
      data: readFileSync(join(dir, 'et-book-display-italic-old-style-figures.ttf')),
      weight: 400,
      style: 'italic' as const,
    },
  ];
  return fontsCache;
}

export async function renderOgImage(markup: any): Promise<Buffer> {
  const svg = await satori(markup, {
    width: WIDTH,
    height: HEIGHT,
    fonts: getFonts() as any,
  });
  return await sharp(Buffer.from(svg)).png().toBuffer();
}

export async function renderCached(
  cacheKey: string,
  inputData: unknown,
  markupFn: () => any | Promise<any>,
): Promise<Buffer> {
  const hash = hashData(inputData);
  const cached = getCached(cacheKey, hash);
  if (cached) return cached;

  const markup = await markupFn();
  const png = await renderOgImage(markup);
  setCache(cacheKey, hash, png);
  return png;
}

/**
 * Fetch a remote image, resize to a square with cover crop, apply circular mask,
 * and return a data:image/png;base64 URI. Handles webp, jpeg, png sources.
 * Returns null on failure.
 */
export async function prepareCircularAvatar(url: string, size: number): Promise<string | null> {
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    const buf = Buffer.from(await res.arrayBuffer());

    // Resize to square (cover crop) then apply circular mask
    const resized = await sharp(buf)
      .resize(size, size, { fit: 'cover', position: 'top' })
      .png()
      .toBuffer();

    // Create circular mask
    const circle = Buffer.from(
      `<svg width="${size}" height="${size}"><circle cx="${size / 2}" cy="${size / 2}" r="${size / 2}" fill="white"/></svg>`,
    );

    const circular = await sharp(resized)
      .composite([{ input: circle, blend: 'dest-in' }])
      .png()
      .toBuffer();

    return `data:image/png;base64,${circular.toString('base64')}`;
  } catch {
    return null;
  }
}

export function brandingFooter() {
  return {
    type: 'div',
    props: {
      style: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        width: '100%',
        borderTop: `1px solid ${BORDER}`,
        paddingTop: 16,
        marginTop: 'auto',
        flexShrink: 0,
      },
      children: [
        {
          type: 'span',
          props: {
            style: { fontStyle: 'italic', fontSize: 22, color: TEXT },
            children: 'Closet Picks',
          },
        },
        {
          type: 'span',
          props: {
            style: { fontSize: 20, color: MUTED },
            children: 'closetpicks.westenb.org',
          },
        },
      ],
    },
  };
}

export function pickBadge(count: number, label?: string) {
  const text = label || `Picked by ${count} ${count === 1 ? 'guest' : 'guests'}`;
  return {
    type: 'div',
    props: {
      style: {
        display: 'flex',
        backgroundColor: BADGE_BG,
        color: '#fff',
        fontSize: 20,
        fontWeight: 700,
        padding: '8px 20px',
        borderRadius: 6,
      },
      children: text,
    },
  };
}

/** Reusable poster thumbnail for image rows. Uses 4:5 ratio (Criterion box art). */
export function posterThumb(posterUrl: string | null, alt: string, height: number = 200) {
  const width = Math.round(height * 0.8); // 4:5 aspect ratio
  if (posterUrl) {
    return {
      type: 'div',
      props: {
        style: {
          display: 'flex',
          width,
          height,
          borderRadius: 4,
          overflow: 'hidden',
          flexShrink: 0,
        },
        children: [
          {
            type: 'img',
            props: {
              src: posterUrl,
              width,
              height,
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
        width,
        height,
        backgroundColor: '#333',
        borderRadius: 4,
        color: '#fff',
        fontSize: 16,
        fontWeight: 700,
        flexShrink: 0,
      },
      children: alt,
    },
  };
}

/** Circular avatar from a pre-processed data URI (from prepareCircularAvatar) */
export function avatarCircle(dataUri: string, size: number = 120) {
  return {
    type: 'img',
    props: {
      src: dataUri,
      width: size,
      height: size,
    },
  };
}
