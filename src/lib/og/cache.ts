import { readFileSync, writeFileSync, mkdirSync, existsSync, readdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';

const CACHE_DIR = join(process.cwd(), '.cache', 'og');

// Directories whose contents decide what a card looks like: the renderers that
// build the markup, and the fonts satori lays it out with.
const RENDERER_DIRS = [
  join(process.cwd(), 'src', 'lib', 'og'),
  join(process.cwd(), 'src', 'assets', 'fonts'),
];

/**
 * Fingerprint of the code and fonts that draw the cards.
 *
 * A card's cache entry is keyed on the data that went into it, which says
 * nothing about how it was drawn. Restyle a card and every signature stays
 * identical, so the old picture is served and the new design never ships. That
 * was true of local builds already, and it would reach production the moment
 * CI restores this cache between runs. Mixing the fingerprint into every hash
 * means touching any renderer or font invalidates every card at once.
 *
 * If the sources cannot be read we cannot show a cached card is current, so
 * fall back to a per-process value: nothing hits and everything re-renders.
 * Slow beats wrong.
 */
const RENDERER_FINGERPRINT = (() => {
  const h = createHash('md5');
  try {
    for (const dir of RENDERER_DIRS) {
      for (const name of readdirSync(dir).sort()) {
        h.update(name);
        h.update(readFileSync(join(dir, name)));
      }
    }
  } catch (err) {
    console.warn(`[og-cache] cannot fingerprint renderers (${err}); ` +
                 'rendering every card fresh for this run');
    return randomUUID();
  }
  return h.digest('hex');
})();

function hashData(data: unknown): string {
  return createHash('md5')
    .update(RENDERER_FINGERPRINT)
    .update(JSON.stringify(data))
    .digest('hex');
}

function cachePath(key: string): string {
  return join(CACHE_DIR, `${key}.jpg`);
}

function hashPath(key: string): string {
  return join(CACHE_DIR, `${key}.hash`);
}

export function getCached(key: string, dataHash: string): Buffer | null {
  const image = cachePath(key);
  const hash = hashPath(key);
  if (!existsSync(image) || !existsSync(hash)) return null;
  try {
    const stored = readFileSync(hash, 'utf-8').trim();
    if (stored === dataHash) return readFileSync(image);
  } catch {
    return null;
  }
  return null;
}

export function setCache(key: string, dataHash: string, image: Buffer): void {
  const imageFile = cachePath(key);
  mkdirSync(dirname(imageFile), { recursive: true });
  writeFileSync(imageFile, image);
  writeFileSync(hashPath(key), dataHash);
}

export { hashData };
