/**
 * Sitemap <lastmod> from page content.
 *
 * The data records no edit dates (a guest's quotes can be re-extracted months
 * after the episode aired), so lastmod comes from the built pages instead. At
 * deploy, each page in the sitemap gets a fingerprint of what a reader sees:
 * its title, meta description, and the text, links and images inside <main>.
 * The previous deploy published the fingerprints it saw, with their lastmod,
 * at /sitemap-lastmod.json. A page whose fingerprint is unchanged keeps its
 * old lastmod; a new or changed page gets the time of this deploy.
 *
 * Header, footer, CSS and script changes do not touch <main>, so they do not
 * make every page look changed.
 */
import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { parse } from 'node-html-parser';
import { distFileFor, type SitemapEntry } from './sitemap';

/** Served from the site root next to the sitemap. */
export const MANIFEST_FILE = 'sitemap-lastmod.json';

/**
 * Bump this when the fingerprint inputs change. A deploy that sees a manifest
 * from another version keeps every known page's lastmod rather than treating
 * the whole site as changed.
 */
export const FINGERPRINT_VERSION = 1;

export interface LastmodManifest {
  version: number;
  generated: string;
  /** Keyed by URL path, e.g. "/guests/aaron-sorkin/". */
  pages: Record<string, { hash: string; lastmod: string }>;
}

export function pageFingerprint(html: string): string {
  const root = parse(html);
  const main = root.querySelector('main');
  const parts = {
    title: root.querySelector('title')?.text.trim() ?? '',
    description: root.querySelector('meta[name="description"]')?.getAttribute('content') ?? '',
    text: (main?.text ?? '').replace(/\s+/g, ' ').trim(),
    links: main?.querySelectorAll('a[href]').map((a) => a.getAttribute('href')) ?? [],
    images: main?.querySelectorAll('img[src]').map((img) => img.getAttribute('src')) ?? [],
  };
  return createHash('sha256').update(JSON.stringify(parts)).digest('hex').slice(0, 16);
}

/** W3C datetime to the second, as sitemaps expect: 2026-10-26T22:31:05Z. */
export function w3cDatetime(date: Date): string {
  return date.toISOString().replace(/\.\d{3}Z$/, 'Z');
}

/**
 * Works out every sitemap entry's lastmod. `previous` is the manifest the live
 * site published, or null on the first deploy (every page then counts as new).
 */
export function computeLastmod(
  entries: SitemapEntry[],
  distDir: string,
  previous: LastmodManifest | null,
  now: Date,
): { lastmodByLoc: Map<string, string>; manifest: LastmodManifest; changed: number } {
  const stamp = w3cDatetime(now);
  const sameVersion = previous?.version === FINGERPRINT_VERSION;
  const lastmodByLoc = new Map<string, string>();
  const pages: LastmodManifest['pages'] = {};
  let changed = 0;

  for (const { loc } of entries) {
    const file = distFileFor(distDir, loc);
    if (!existsSync(file)) continue;
    const path = new URL(loc).pathname;
    const hash = pageFingerprint(readFileSync(file, 'utf8'));
    const before = previous?.pages[path];
    const unchanged = before && (sameVersion ? before.hash === hash : true);
    const lastmod = unchanged ? before.lastmod : stamp;
    if (!unchanged) changed++;
    pages[path] = { hash, lastmod };
    lastmodByLoc.set(loc, lastmod);
  }

  return {
    lastmodByLoc,
    manifest: { version: FINGERPRINT_VERSION, generated: stamp, pages },
    changed,
  };
}

/** Accepts only a manifest shaped like ours; anything else counts as none. */
export function parseManifest(text: string): LastmodManifest | null {
  try {
    const data = JSON.parse(text);
    if (typeof data?.version === 'number' && data.pages && typeof data.pages === 'object') return data;
  } catch {
    // fall through
  }
  return null;
}
