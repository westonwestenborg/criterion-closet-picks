/**
 * Reading and rewriting the sitemap that @astrojs/sitemap writes into dist/.
 *
 * The integration writes dist/sitemap-index.xml, which points at one or more
 * dist/sitemap-N.xml files of <url><loc>…</loc></url> entries. These helpers
 * only need to handle that output, so they use a few regular expressions
 * rather than a full XML parser.
 */
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

export interface SitemapEntry {
  loc: string;
  lastmod?: string;
}

const decode = (s: string) =>
  s.trim()
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, '&');

const encode = (s: string) =>
  s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

export function isSitemapIndex(xml: string): boolean {
  return /<sitemapindex[\s>]/.test(xml);
}

/** The child sitemap URLs listed in a sitemap index. */
export function parseSitemapIndex(xml: string): string[] {
  return [...xml.matchAll(/<sitemap>([\s\S]*?)<\/sitemap>/g)]
    .map((m) => m[1].match(/<loc>([\s\S]*?)<\/loc>/)?.[1])
    .filter((loc): loc is string => !!loc)
    .map(decode);
}

/** The <url> entries of one sitemap file. */
export function parseSitemap(xml: string): SitemapEntry[] {
  return [...xml.matchAll(/<url>([\s\S]*?)<\/url>/g)].flatMap((m) => {
    const loc = m[1].match(/<loc>([\s\S]*?)<\/loc>/)?.[1];
    if (!loc) return [];
    const lastmod = m[1].match(/<lastmod>([\s\S]*?)<\/lastmod>/)?.[1];
    return [lastmod ? { loc: decode(loc), lastmod: decode(lastmod) } : { loc: decode(loc) }];
  });
}

/**
 * Sets each entry's <lastmod> from `lastmodByLoc`, replacing any that is
 * already there. Entries missing from the map keep what they have.
 */
export function stampLastmod(xml: string, lastmodByLoc: Map<string, string>): string {
  return xml.replace(/<url>([\s\S]*?)<\/url>/g, (whole, inner: string) => {
    const loc = inner.match(/<loc>([\s\S]*?)<\/loc>/)?.[1];
    const lastmod = loc ? lastmodByLoc.get(decode(loc)) : undefined;
    if (!lastmod) return whole;
    const withoutLastmod = inner.replace(/<lastmod>[\s\S]*?<\/lastmod>/, '');
    return `<url>${withoutLastmod.replace(/(<\/loc>)/, `$1<lastmod>${encode(lastmod)}</lastmod>`)}</url>`;
  });
}

/** Maps a sitemap URL to the file that serves it in dist/: "/guests/x/" → "guests/x/index.html". */
export function distFileFor(distDir: string, url: string): string {
  const path = decodeURIComponent(new URL(url).pathname);
  return join(distDir, path.endsWith('/') ? `${path}index.html` : path);
}

/** The sitemap files in dist/, as { file, xml } in index order. */
export function readBuiltSitemapFiles(distDir: string): { file: string; xml: string }[] {
  const indexFile = join(distDir, 'sitemap-index.xml');
  if (!existsSync(indexFile)) {
    throw new Error(`${indexFile} not found. Run \`bun run build\` first.`);
  }
  return parseSitemapIndex(readFileSync(indexFile, 'utf8')).map((loc) => {
    const file = distFileFor(distDir, loc);
    return { file, xml: readFileSync(file, 'utf8') };
  });
}

/** Every <url> entry across the built sitemap files. */
export function readBuiltSitemap(distDir: string): SitemapEntry[] {
  return readBuiltSitemapFiles(distDir).flatMap(({ xml }) => parseSitemap(xml));
}

export interface SitemapDiff {
  /** In the new sitemap but not the old one. */
  added: string[];
  /** In both, with a different <lastmod>. */
  changed: string[];
  /** In the old sitemap but not the new one. */
  removed: string[];
}

export function diffSitemaps(previous: SitemapEntry[], next: SitemapEntry[]): SitemapDiff {
  const before = new Map(previous.map((e) => [e.loc, e.lastmod]));
  const after = new Set(next.map((e) => e.loc));
  const added: string[] = [];
  const changed: string[] = [];
  for (const entry of next) {
    if (!before.has(entry.loc)) added.push(entry.loc);
    else if (before.get(entry.loc) !== entry.lastmod) changed.push(entry.loc);
  }
  const removed = previous.map((e) => e.loc).filter((loc) => !after.has(loc));
  return { added, changed, removed };
}
