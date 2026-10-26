/**
 * Deploy-time SEO steps, run by .github/workflows/deploy-cloudflare.yml:
 *
 *   bun tools/seo/cli.ts fetch-previous   before the build: save the live sitemap
 *                                         and lastmod manifest to .seo-previous/
 *   bun tools/seo/cli.ts lastmod          after the build: write <lastmod> into
 *                                         dist/'s sitemap and dist/sitemap-lastmod.json
 *   bun tools/seo/cli.ts indexnow         after the deploy: submit new, changed
 *                                         and removed URLs to IndexNow
 *
 * None of these can fail a deploy. Network trouble or a bug logs a GitHub
 * Actions warning and exits 0; the sitemap just ships without <lastmod>, or
 * the engines learn about changes on their next crawl instead.
 *
 * Add --dry-run to `indexnow` to print the URLs without sending them.
 *
 * Reads from the live site go to SEO_READ_ORIGIN when it is set. The workflow
 * sets it to the Cloudflare Pages alias (criterion-closet-picks.pages.dev),
 * which serves the same production deployment as closetpicks.westenb.org.
 * The custom domain answers GitHub's runners with HTTP 403 (Cloudflare bot
 * protection on the zone), and the pages.dev alias is outside that zone.
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { computeLastmod, MANIFEST_FILE, parseManifest, type LastmodManifest } from './lastmod';
import {
  diffSitemaps,
  isSitemapIndex,
  parseSitemap,
  parseSitemapIndex,
  readBuiltSitemap,
  readBuiltSitemapFiles,
  stampLastmod,
  type SitemapEntry,
} from './sitemap';
import { batches, describeStatus, INDEXNOW_ENDPOINT, INDEXNOW_KEY, indexNowPayload, urlsToSubmit } from './indexnow';

const DIST = 'dist';
const PREVIOUS_DIR = '.seo-previous';
const PREVIOUS_FILE = join(PREVIOUS_DIR, 'previous.json');

/** What the live site served before this deploy. */
interface Previous {
  /** ok: fetched. missing: the live site has none yet (first deploy). error: could not tell. */
  sitemap: 'ok' | 'missing' | 'error';
  entries: SitemapEntry[];
  manifest: LastmodManifest | null;
  fetchedAt: string;
}

function warn(message: string): void {
  // GitHub Actions shows ::warning:: lines as annotations on the run.
  console.log(process.env.GITHUB_ACTIONS ? `::warning title=SEO::${message}` : `warning: ${message}`);
}

/** The site's origin, from public/CNAME, which Cloudflare Pages also uses. */
function siteOrigin(): string {
  const cname = readFileSync(join('public', 'CNAME'), 'utf8').trim();
  return `https://${cname}`;
}

/** Where to read the live deployment from (see the note at the top). */
function readOrigin(): string {
  return (process.env.SEO_READ_ORIGIN || siteOrigin()).replace(/\/$/, '');
}

/** The same path on the read origin: sitemap indexes name their children on the site's own host. */
function onReadOrigin(url: string): string {
  const u = new URL(url);
  return `${readOrigin()}${u.pathname}${u.search}`;
}

/** "HTTP 403 (cf-mitigated: challenge)", so a Cloudflare block is recognizable in the log. */
function statusText(res: Response): string {
  const mitigated = res.headers.get('cf-mitigated');
  return `HTTP ${res.status}${mitigated ? ` (cf-mitigated: ${mitigated})` : ''}`;
}

async function get(url: string): Promise<Response> {
  let lastError: unknown;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(15_000), headers: { 'cache-control': 'no-cache' } });
      if (res.status < 500) return res;
      lastError = new Error(`HTTP ${res.status}`);
    } catch (err) {
      lastError = err;
    }
    await new Promise((r) => setTimeout(r, attempt * 2_000));
  }
  throw new Error(`GET ${url} failed: ${lastError}`);
}

async function fetchPrevious(): Promise<void> {
  const origin = readOrigin();
  const previous: Previous = { sitemap: 'error', entries: [], manifest: null, fetchedAt: new Date().toISOString() };
  try {
    const index = await get(`${origin}/sitemap-index.xml`);
    if (index.status === 404) {
      previous.sitemap = 'missing';
    } else if (!index.ok) {
      throw new Error(`${origin}/sitemap-index.xml returned ${statusText(index)}`);
    } else {
      const xml = await index.text();
      const children = isSitemapIndex(xml) ? parseSitemapIndex(xml) : [];
      for (const child of children) {
        const res = await get(onReadOrigin(child));
        if (!res.ok) throw new Error(`${onReadOrigin(child)} returned ${statusText(res)}`);
        previous.entries.push(...parseSitemap(await res.text()));
      }
      previous.sitemap = 'ok';
    }
  } catch (err) {
    warn(`Could not read the live sitemap, so this deploy will not submit to IndexNow: ${err}`);
  }
  try {
    const res = await get(`${origin}/${MANIFEST_FILE}`);
    if (res.ok) previous.manifest = parseManifest(await res.text());
    else if (res.status !== 404) warn(`${origin}/${MANIFEST_FILE} returned ${statusText(res)}; every page will get a new lastmod`);
  } catch (err) {
    warn(`Could not read the live ${MANIFEST_FILE}; every page will get a new lastmod: ${err}`);
  }
  mkdirSync(PREVIOUS_DIR, { recursive: true });
  writeFileSync(PREVIOUS_FILE, JSON.stringify(previous));
  console.log(
    `Live sitemap: ${previous.sitemap} (${previous.entries.length} URLs). ` +
    `Live lastmod manifest: ${previous.manifest ? `${Object.keys(previous.manifest.pages).length} pages` : 'none'}.`,
  );
}

function readPrevious(): Previous | null {
  if (!existsSync(PREVIOUS_FILE)) return null;
  return JSON.parse(readFileSync(PREVIOUS_FILE, 'utf8'));
}

function writeLastmod(): void {
  const previous = readPrevious();
  if (!previous) warn(`${PREVIOUS_FILE} not found (run fetch-previous first); every page will get a new lastmod`);
  const files = readBuiltSitemapFiles(DIST);
  const entries = files.flatMap(({ xml }) => parseSitemap(xml));
  const { lastmodByLoc, manifest, changed } = computeLastmod(entries, DIST, previous?.manifest ?? null, new Date());
  for (const { file, xml } of files) writeFileSync(file, stampLastmod(xml, lastmodByLoc));
  writeFileSync(join(DIST, MANIFEST_FILE), JSON.stringify(manifest));
  console.log(`Sitemap lastmod: ${entries.length} pages, ${changed} new or changed since the live site.`);
}

/** Waits until the live site serves this build's sitemap, so engines crawl the new pages. */
async function waitForDeploy(): Promise<boolean> {
  const origin = readOrigin();
  const expected = readBuiltSitemapFiles(DIST).map(({ xml }) => xml);
  for (let attempt = 1; attempt <= 12; attempt++) {
    try {
      const index = await get(`${origin}/sitemap-index.xml`);
      const children = parseSitemapIndex(await index.text());
      const live = await Promise.all(children.map(async (c) => (await get(onReadOrigin(c))).text()));
      if (live.length === expected.length && live.every((xml, i) => xml === expected[i])) return true;
    } catch {
      // keep waiting
    }
    await new Promise((r) => setTimeout(r, 10_000));
  }
  return false;
}

async function submitIndexNow(dryRun: boolean): Promise<void> {
  const previous = readPrevious();
  if (!previous || previous.sitemap === 'error') {
    warn('Skipping IndexNow: the sitemap that was live before this deploy is unknown, so the changed pages are too.');
    return;
  }
  if (!existsSync(join(DIST, MANIFEST_FILE))) {
    // Without this build's lastmod, every page would look changed.
    warn(`Skipping IndexNow: ${DIST}/${MANIFEST_FILE} is missing, so the lastmod step did not finish.`);
    return;
  }
  const origin = siteOrigin();
  const diff = diffSitemaps(previous.entries, readBuiltSitemap(DIST));
  const urls = urlsToSubmit(diff).filter((u) => new URL(u).origin === origin);
  console.log(
    `IndexNow: ${diff.added.length} added, ${diff.changed.length} changed, ${diff.removed.length} removed` +
    `${previous.sitemap === 'missing' ? ' (no live sitemap before this deploy, so every page counts as added)' : ''}.`,
  );
  if (urls.length === 0) {
    console.log('IndexNow: nothing to submit.');
    return;
  }
  if (dryRun) {
    console.log(urls.join('\n'));
    console.log(`IndexNow dry run: would submit ${urls.length} URLs in ${batches(urls).length} request(s).`);
    return;
  }

  if (!(await waitForDeploy())) {
    warn('The live sitemap still does not match this build after 2 minutes; submitting anyway.');
  }
  const keyUrl = `${readOrigin()}/${INDEXNOW_KEY}.txt`;
  const keyFile = await get(keyUrl);
  if (!keyFile.ok || (await keyFile.text()).trim() !== INDEXNOW_KEY) {
    warn(`Skipping IndexNow: ${keyUrl} does not serve the key (${statusText(keyFile)}).`);
    return;
  }

  for (const [i, batch] of batches(urls).entries()) {
    const res = await fetch(INDEXNOW_ENDPOINT, {
      method: 'POST',
      headers: { 'content-type': 'application/json; charset=utf-8' },
      body: JSON.stringify(indexNowPayload(origin, batch)),
      signal: AbortSignal.timeout(30_000),
    });
    const line = `IndexNow batch ${i + 1}: ${batch.length} URLs → HTTP ${res.status} (${describeStatus(res.status)})`;
    if (res.ok) console.log(line);
    else warn(`${line}: ${(await res.text()).slice(0, 300)}`);
  }
}

const [command, ...args] = process.argv.slice(2);
const commands: Record<string, () => Promise<void> | void> = {
  'fetch-previous': fetchPrevious,
  lastmod: writeLastmod,
  indexnow: () => submitIndexNow(args.includes('--dry-run')),
};

if (!commands[command]) {
  console.error(`usage: bun tools/seo/cli.ts ${Object.keys(commands).join('|')} [--dry-run]`);
  process.exit(2);
}
try {
  await commands[command]();
} catch (err) {
  warn(`seo ${command} failed: ${err instanceof Error ? err.stack : err}`);
}
