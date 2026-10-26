import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { auditSite } from '../tools/seo/audit';
import { batches, INDEXNOW_KEY, indexNowPayload, urlsToSubmit } from '../tools/seo/indexnow';
import { computeLastmod, FINGERPRINT_VERSION, pageFingerprint, type LastmodManifest } from '../tools/seo/lastmod';
import { diffSitemaps, parseSitemap, stampLastmod } from '../tools/seo/sitemap';

const SITE = 'https://closetpicks.westenb.org';

describe('sitemap helpers', () => {
  const xml = `<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>${SITE}/</loc></url><url><loc>${SITE}/guests/a/</loc><lastmod>2026-01-01T00:00:00Z</lastmod></url></urlset>`;

  it('parses entries with and without lastmod', () => {
    expect(parseSitemap(xml)).toEqual([
      { loc: `${SITE}/` },
      { loc: `${SITE}/guests/a/`, lastmod: '2026-01-01T00:00:00Z' },
    ]);
  });

  it('writes lastmod into every entry, replacing an old one', () => {
    const stamped = stampLastmod(xml, new Map([
      [`${SITE}/`, '2026-10-26T00:00:00Z'],
      [`${SITE}/guests/a/`, '2026-10-27T00:00:00Z'],
    ]));
    expect(parseSitemap(stamped)).toEqual([
      { loc: `${SITE}/`, lastmod: '2026-10-26T00:00:00Z' },
      { loc: `${SITE}/guests/a/`, lastmod: '2026-10-27T00:00:00Z' },
    ]);
  });

  it('diffs two sitemaps into added, changed and removed URLs', () => {
    const before = [
      { loc: 'a', lastmod: '1' },
      { loc: 'b', lastmod: '1' },
      { loc: 'c', lastmod: '1' },
    ];
    const after = [
      { loc: 'a', lastmod: '1' },
      { loc: 'b', lastmod: '2' },
      { loc: 'd', lastmod: '2' },
    ];
    expect(diffSitemaps(before, after)).toEqual({ added: ['d'], changed: ['b'], removed: ['c'] });
    // First deploy: no live sitemap, so everything is new.
    expect(diffSitemaps([], after).added).toEqual(['a', 'b', 'd']);
  });
});

describe('IndexNow', () => {
  it('submits only added, changed and removed URLs', () => {
    expect(urlsToSubmit({ added: ['a'], changed: ['b'], removed: ['c'] })).toEqual(['a', 'b', 'c']);
    expect(urlsToSubmit({ added: [], changed: [], removed: [] })).toEqual([]);
  });

  it('splits more than 10,000 URLs into several requests', () => {
    const urls = Array.from({ length: 23_456 }, (_, i) => `${SITE}/films/${i}/`);
    expect(batches(urls).map((b) => b.length)).toEqual([10_000, 10_000, 3_456]);
  });

  it('builds the payload around the key file served from the site root', () => {
    expect(indexNowPayload(SITE, [`${SITE}/`])).toEqual({
      host: 'closetpicks.westenb.org',
      key: INDEXNOW_KEY,
      keyLocation: `${SITE}/${INDEXNOW_KEY}.txt`,
      urlList: [`${SITE}/`],
    });
  });

  it('commits the key file in public/ with the key as its only content', () => {
    expect(readFileSync(join('public', `${INDEXNOW_KEY}.txt`), 'utf8').trim()).toBe(INDEXNOW_KEY);
  });
});

// --- Fixture sites for the lastmod step and the audit ---

let tmp: string | null = null;
afterEach(() => {
  if (tmp) rmSync(tmp, { recursive: true, force: true });
  tmp = null;
});

interface FixturePage {
  title?: string;
  description?: string;
  canonical?: boolean;
  noindex?: boolean;
  h1s?: number;
  links?: string[];
  body?: string;
  header?: string;
}

function html(path: string, page: FixturePage): string {
  const url = `${SITE}${path}`;
  const title = page.title ?? `Title for ${path}`;
  const head = [
    `<title>${title}</title>`,
    page.description !== '' ? `<meta name="description" content="${page.description ?? `About ${path}`}">` : '',
    page.noindex ? '<meta name="robots" content="noindex, follow">' : '',
    page.canonical === false ? '' : `<link rel="canonical" href="${url}">`,
    `<meta property="og:title" content="${title}">`,
    `<meta property="og:description" content="About ${path}">`,
    `<meta property="og:image" content="${SITE}/og-image.jpg">`,
    `<meta property="og:url" content="${url}">`,
    '<meta name="twitter:card" content="summary_large_image">',
    `<script type="application/ld+json">{"@context":"https://schema.org","@type":"WebPage","name":"${title}"}</script>`,
  ].join('');
  const h1s = '<h1>Heading</h1>'.repeat(page.h1s ?? 1);
  const links = (page.links ?? []).map((href) => `<a href="${href}">link</a>`).join('');
  return `<!DOCTYPE html><html><head>${head}</head><body><header>${page.header ?? ''}</header><main>${h1s}${page.body ?? ''}${links}</main></body></html>`;
}

/** Writes a small built site: pages keyed by URL path, plus a sitemap listing `sitemap`. */
function makeSite(pages: Record<string, FixturePage>, sitemap: string[]): string {
  const dir = mkdtempSync(join(tmpdir(), 'ccp-seo-'));
  tmp = dir;
  for (const [path, page] of Object.entries(pages)) {
    const file = join(dir, path.endsWith('/') ? `${path}index.html` : path);
    mkdirSync(dirname(file), { recursive: true });
    writeFileSync(file, html(path, page));
  }
  writeFileSync(join(dir, 'og-image.jpg'), '');
  writeFileSync(join(dir, 'robots.txt'), `User-agent: *\nAllow: /\n\nSitemap: ${SITE}/sitemap-index.xml\n`);
  writeFileSync(
    join(dir, 'sitemap-index.xml'),
    `<?xml version="1.0" encoding="UTF-8"?><sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><sitemap><loc>${SITE}/sitemap-0.xml</loc></sitemap></sitemapindex>`,
  );
  writeFileSync(
    join(dir, 'sitemap-0.xml'),
    `<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">${sitemap.map((p) => `<url><loc>${SITE}${p}</loc></url>`).join('')}</urlset>`,
  );
  return dir;
}

const goodPages: Record<string, FixturePage> = {
  '/': { links: ['/guests/', '/random/'] },
  '/guests/': { links: ['/guests/aaron-sorkin/'] },
  '/guests/aaron-sorkin/': { links: ['/films/roma-2018/'] },
  '/films/roma-2018/': {},
  '/random/': { noindex: true, canonical: false },
  '/404.html': { noindex: true, canonical: false },
};
const goodSitemap = ['/', '/guests/', '/guests/aaron-sorkin/', '/films/roma-2018/'];

describe('lastmod step', () => {
  const entries = goodSitemap.map((p) => ({ loc: `${SITE}${p}` }));
  const t1 = new Date('2026-10-01T12:00:00Z');
  const t2 = new Date('2026-10-26T12:00:00Z');

  it('stamps every page on the first deploy, then keeps lastmod for pages that did not change', () => {
    const dist = makeSite(goodPages, goodSitemap);
    const first = computeLastmod(entries, dist, null, t1);
    expect(first.changed).toBe(4);
    expect([...first.lastmodByLoc.values()]).toEqual(Array(4).fill('2026-10-01T12:00:00Z'));

    writeFileSync(join(dist, 'films/roma-2018/index.html'), html('/films/roma-2018/', { body: '<p>A new quote.</p>' }));
    const second = computeLastmod(entries, dist, first.manifest, t2);
    expect(second.changed).toBe(1);
    expect(second.lastmodByLoc.get(`${SITE}/films/roma-2018/`)).toBe('2026-10-26T12:00:00Z');
    expect(second.lastmodByLoc.get(`${SITE}/guests/aaron-sorkin/`)).toBe('2026-10-01T12:00:00Z');
  });

  it('keeps known lastmods when the fingerprint version changes, instead of marking every page changed', () => {
    const dist = makeSite(goodPages, goodSitemap);
    const old: LastmodManifest = {
      version: FINGERPRINT_VERSION - 1,
      generated: '2026-10-01T12:00:00Z',
      pages: { '/': { hash: 'stale', lastmod: '2026-10-01T12:00:00Z' } },
    };
    const result = computeLastmod(entries, dist, old, t2);
    expect(result.lastmodByLoc.get(`${SITE}/`)).toBe('2026-10-01T12:00:00Z');
    expect(result.changed).toBe(3);
  });

  it('ignores changes outside <main>, such as the header', () => {
    expect(pageFingerprint(html('/', { header: 'Old nav' }))).toBe(pageFingerprint(html('/', { header: 'New nav' })));
    expect(pageFingerprint(html('/', { body: 'Old text' }))).not.toBe(pageFingerprint(html('/', { body: 'New text' })));
  });
});

describe('SEO audit', () => {
  it('passes a site with complete metadata', () => {
    expect(auditSite(makeSite(goodPages, goodSitemap)).problems).toEqual([]);
  });

  it('flags a page with no meta description', () => {
    const dist = makeSite({ ...goodPages, '/films/roma-2018/': { description: '' } }, goodSitemap);
    expect(auditSite(dist).problems).toEqual(['/films/roma-2018/: missing <meta name="description">']);
  });

  it('flags a missing canonical and a page without exactly one h1', () => {
    const dist = makeSite({ ...goodPages, '/films/roma-2018/': { canonical: false, h1s: 2 } }, goodSitemap);
    expect(auditSite(dist).problems).toEqual([
      '/films/roma-2018/: missing <link rel="canonical">',
      '/films/roma-2018/: has 2 <h1> elements; a page needs exactly one',
    ]);
  });

  it('flags two pages that share a title', () => {
    const dist = makeSite({
      ...goodPages,
      '/guests/aaron-sorkin/': { title: 'Same', links: ['/films/roma-2018/'] },
      '/films/roma-2018/': { title: 'Same' },
    }, goodSitemap);
    expect(auditSite(dist).problems).toEqual(['duplicate <title> "Same" on /guests/aaron-sorkin/, /films/roma-2018/']);
  });

  it('flags a guest or film page missing from the sitemap', () => {
    const dist = makeSite(goodPages, goodSitemap.filter((p) => p !== '/films/roma-2018/'));
    expect(auditSite(dist).problems).toEqual(['/films/roma-2018/: guest and film pages must be in the sitemap']);
  });

  it('flags a noindex page in the sitemap', () => {
    const dist = makeSite(goodPages, [...goodSitemap, '/random/']);
    expect(auditSite(dist).problems).toContain('/random/: is marked noindex but is in the sitemap');
  });

  it('flags a page that is neither in the sitemap nor noindex', () => {
    const dist = makeSite({ ...goodPages, '/about/': {} }, goodSitemap);
    expect(auditSite(dist).problems).toEqual([
      '/about/: not in the sitemap and not marked noindex; add it to the sitemap or pass noindex to BaseLayout',
    ]);
  });

  it('flags a sitemap page that no link reaches', () => {
    const dist = makeSite({ ...goodPages, '/guests/aaron-sorkin/': {} }, goodSitemap);
    expect(auditSite(dist).problems).toEqual(['/films/roma-2018/: no link reaches this page from the home page']);
  });
});
