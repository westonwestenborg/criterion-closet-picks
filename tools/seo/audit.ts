/**
 * The SEO gate: checks the built site in dist/ and lists every problem.
 *
 * Run by tests/built/seo.test.ts (`bun run test:seo`) after `bun run build`.
 * Each problem names the page and what to fix, so the failure output can be
 * acted on directly.
 */
import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { join, relative, sep } from 'node:path';
import { parse, type HTMLElement } from 'node-html-parser';
import { distFileFor, readBuiltSitemap } from './sitemap';

export interface AuditResult {
  problems: string[];
  /** Pages in the sitemap. */
  sitemapPages: number;
  /** Every built HTML page, as URL paths. */
  builtPages: string[];
}

/** Every built .html file in dist/, as the URL path it is served at. */
export function builtHtmlPaths(distDir: string): string[] {
  const out: string[] = [];
  const walk = (dir: string) => {
    for (const name of readdirSync(dir)) {
      const full = join(dir, name);
      if (statSync(full).isDirectory()) walk(full);
      else if (name.endsWith('.html')) out.push(full);
    }
  };
  walk(distDir);
  return out
    .map((file) => `/${relative(distDir, file).split(sep).join('/')}`)
    .map((path) => (path.endsWith('/index.html') ? path.slice(0, -'index.html'.length) : path))
    .sort();
}

function isNoindex(root: HTMLElement): boolean {
  return root
    .querySelectorAll('meta[name="robots"]')
    .some((m) => /\bnoindex\b/i.test(m.getAttribute('content') ?? ''));
}

const meta = (root: HTMLElement, selector: string) =>
  root.querySelector(selector)?.getAttribute('content')?.trim() ?? '';

/** Checks one page that is in the sitemap. */
function auditPage(root: HTMLElement, loc: string, distDir: string, origin: string): string[] {
  const problems: string[] = [];
  const add = (msg: string) => problems.push(msg);

  const title = root.querySelector('title')?.text.trim() ?? '';
  if (!title) add('missing <title>');
  if (!meta(root, 'meta[name="description"]')) add('missing <meta name="description">');

  const canonical = root.querySelector('link[rel="canonical"]')?.getAttribute('href') ?? '';
  if (!canonical) add('missing <link rel="canonical">');
  else if (canonical !== loc) add(`canonical is ${canonical}, but the sitemap lists ${loc}`);

  if (!meta(root, 'meta[property="og:title"]')) add('missing og:title');
  if (!meta(root, 'meta[property="og:description"]')) add('missing og:description');
  const ogUrl = meta(root, 'meta[property="og:url"]');
  if (ogUrl !== loc) add(`og:url is "${ogUrl}", expected ${loc}`);
  const ogImage = meta(root, 'meta[property="og:image"]');
  if (!ogImage) add('missing og:image');
  else if (!/^https:\/\//.test(ogImage)) add(`og:image must be an absolute https URL, got "${ogImage}"`);
  else if (new URL(ogImage).origin === origin && !existsSync(distFileFor(distDir, ogImage))) {
    add(`og:image ${ogImage} is not in the build`);
  }
  if (!meta(root, 'meta[name="twitter:card"]')) add('missing twitter:card');

  if (isNoindex(root)) add('is marked noindex but is in the sitemap');

  const h1s = root.querySelectorAll('h1').length;
  if (h1s !== 1) add(`has ${h1s} <h1> elements; a page needs exactly one`);

  const noAlt = root.querySelectorAll('img').filter((img) => !img.hasAttribute('alt'));
  if (noAlt.length > 0) {
    add(`${noAlt.length} <img> without an alt attribute (use alt="" for decorative images), first: ${noAlt[0].getAttribute('src')}`);
  }

  const ld = root.querySelectorAll('script[type="application/ld+json"]');
  if (ld.length === 0) add('missing JSON-LD structured data');
  for (const block of ld) {
    try {
      const data = JSON.parse(block.rawText);
      if (!data['@context'] || !data['@type']) add('JSON-LD block without @context or @type');
    } catch {
      add('JSON-LD block is not valid JSON');
    }
  }
  return problems;
}

/** Paths reachable from the home page by following <a href> links between built pages. */
function reachablePaths(distDir: string, origin: string, roots: Map<string, HTMLElement>): Set<string> {
  const seen = new Set<string>(['/']);
  const queue = ['/'];
  while (queue.length > 0) {
    const path = queue.shift()!;
    let root = roots.get(path);
    if (!root) {
      const file = distFileFor(distDir, `${origin}${path}`);
      if (!existsSync(file)) continue;
      root = parse(readFileSync(file, 'utf8'));
    }
    for (const a of root.querySelectorAll('a[href]')) {
      let url: URL;
      try {
        url = new URL(a.getAttribute('href')!, `${origin}${path}`);
      } catch {
        continue;
      }
      if (url.origin !== origin || !url.pathname.endsWith('/')) continue;
      if (!seen.has(url.pathname)) {
        seen.add(url.pathname);
        queue.push(url.pathname);
      }
    }
  }
  return seen;
}

export function auditSite(distDir: string): AuditResult {
  const problems: string[] = [];
  const entries = readBuiltSitemap(distDir);
  if (entries.length === 0) {
    return { problems: ['the sitemap lists no pages'], sitemapPages: 0, builtPages: [] };
  }
  const origin = new URL(entries[0].loc).origin;

  const robots = join(distDir, 'robots.txt');
  if (!existsSync(robots) || !readFileSync(robots, 'utf8').includes(`Sitemap: ${origin}/sitemap-index.xml`)) {
    problems.push(`robots.txt must contain "Sitemap: ${origin}/sitemap-index.xml"`);
  }

  // The deploy's lastmod step writes the manifest; when it has run, every
  // entry must carry a <lastmod>.
  if (existsSync(join(distDir, 'sitemap-lastmod.json'))) {
    const missing = entries.filter((e) => !e.lastmod).length;
    if (missing > 0) problems.push(`${missing} sitemap entries have no <lastmod> after the lastmod step ran`);
  }

  const roots = new Map<string, HTMLElement>();
  const titles = new Map<string, string[]>();
  const descriptions = new Map<string, string[]>();
  const inSitemap = new Set<string>();

  for (const { loc } of entries) {
    const url = new URL(loc);
    const path = url.pathname;
    if (url.origin !== origin || url.protocol !== 'https:') {
      problems.push(`${loc}: sitemap URL is not on ${origin}`);
      continue;
    }
    if (inSitemap.has(path)) problems.push(`${path}: listed twice in the sitemap`);
    inSitemap.add(path);
    if (!path.endsWith('/')) {
      problems.push(`${path}: sitemap lists a non-page URL; only HTML pages belong in the sitemap`);
      continue;
    }
    const file = distFileFor(distDir, loc);
    if (!existsSync(file)) {
      problems.push(`${path}: in the sitemap but not in the build`);
      continue;
    }
    const root = parse(readFileSync(file, 'utf8'));
    roots.set(path, root);
    for (const p of auditPage(root, loc, distDir, origin)) problems.push(`${path}: ${p}`);

    const title = root.querySelector('title')?.text.trim();
    if (title) titles.set(title, [...(titles.get(title) ?? []), path]);
    const description = meta(root, 'meta[name="description"]');
    if (description) descriptions.set(description, [...(descriptions.get(description) ?? []), path]);
  }

  for (const [title, paths] of titles) {
    if (paths.length > 1) problems.push(`duplicate <title> "${title}" on ${paths.join(', ')}`);
  }
  for (const [description, paths] of descriptions) {
    if (paths.length > 1) problems.push(`duplicate meta description "${description}" on ${paths.join(', ')}`);
  }

  // Every built page is either in the sitemap or deliberately kept out with
  // noindex. A new page type that is neither fails here.
  const builtPages = builtHtmlPaths(distDir);
  for (const path of builtPages) {
    if (inSitemap.has(path)) continue;
    const isGuestOrFilm = /^\/(guests|films)\/[^/]+\/$/.test(path);
    const root = parse(readFileSync(distFileFor(distDir, `${origin}${path}`), 'utf8'));
    if (isGuestOrFilm) {
      problems.push(`${path}: guest and film pages must be in the sitemap`);
    } else if (!isNoindex(root)) {
      problems.push(`${path}: not in the sitemap and not marked noindex; add it to the sitemap or pass noindex to BaseLayout`);
    }
  }

  // Search engines find pages by following links; a page only Pagefind
  // search can reach is effectively invisible to them.
  const reachable = reachablePaths(distDir, origin, roots);
  for (const path of inSitemap) {
    if (!reachable.has(path)) problems.push(`${path}: no link reaches this page from the home page`);
  }

  return { problems, sitemapPages: entries.length, builtPages };
}
