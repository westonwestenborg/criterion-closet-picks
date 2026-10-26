/**
 * The SEO gate over the built site. Run after `bun run build`:
 *
 *   bun run test:seo
 *
 * CI runs it on every pull request and before every deploy. See the SEO
 * section of AGENTS.md for what each check means and how to fix a failure.
 */
import { existsSync } from 'node:fs';
import { beforeAll, describe, expect, it } from 'vitest';
import { auditSite, type AuditResult } from '../../tools/seo/audit';
import { readBuiltSitemap } from '../../tools/seo/sitemap';
import { getFilms, getPickedBoxSets, getPublishableGuests } from '../../src/lib/data';

const DIST = 'dist';

describe('built site SEO', () => {
  let result: AuditResult;
  let sitemapPaths: Set<string>;

  beforeAll(() => {
    if (!existsSync(DIST)) throw new Error('dist/ not found. Run `bun run build` before `bun run test:seo`.');
    result = auditSite(DIST);
    sitemapPaths = new Set(readBuiltSitemap(DIST).map((e) => new URL(e.loc).pathname));
  });

  it('has no SEO problems on any page', () => {
    // Each line names a page and what is wrong with it.
    expect(result.problems).toEqual([]);
  });

  it('lists every publishable guest in the sitemap', () => {
    const missing = getPublishableGuests()
      .map((g) => `/guests/${g.slug}/`)
      .filter((path) => !sitemapPaths.has(path));
    expect(missing).toEqual([]);
  });

  it('lists every film and box-set page in the sitemap', () => {
    const slugs = new Set([...getFilms(), ...getPickedBoxSets()].map((f) => f.slug));
    const missing = [...slugs].map((slug) => `/films/${slug}/`).filter((path) => !sitemapPaths.has(path));
    expect(missing).toEqual([]);
  });
});
