/**
 * IndexNow: tell Bing, Yandex, Naver, Seznam, Yep and Amazon which pages
 * changed in a deploy, so they recrawl those pages instead of waiting.
 *
 * One POST to the shared endpoint reaches every participating engine. The
 * engines check that we own the site by fetching /<key>.txt from it, which
 * is why the key is public and committed in public/. Google and Brave do not
 * take part; they find changes through the sitemap's <lastmod>.
 *
 * Protocol: https://www.indexnow.org/documentation
 */
import type { SitemapDiff } from './sitemap';

export const INDEXNOW_KEY = 'a4a8fe4bd2253de1a4bd81e133e7c871';
export const INDEXNOW_ENDPOINT = 'https://api.indexnow.org/indexnow';
/** The protocol's limit on URLs per request. */
export const INDEXNOW_BATCH = 10_000;

/**
 * The URLs a deploy should submit: new pages, pages whose lastmod moved, and
 * pages that were removed (so the engines recrawl them and drop them). An
 * unchanged page is never resubmitted; the protocol counts that as misuse.
 */
export function urlsToSubmit(diff: SitemapDiff): string[] {
  return [...new Set([...diff.added, ...diff.changed, ...diff.removed])];
}

export function batches<T>(items: T[], size = INDEXNOW_BATCH): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

export function indexNowPayload(site: string, urls: string[], key = INDEXNOW_KEY) {
  const origin = new URL(site);
  return {
    host: origin.host,
    key,
    keyLocation: new URL(`/${key}.txt`, origin).href,
    urlList: urls,
  };
}

/**
 * Status codes from the protocol docs. 200 and 202 mean accepted (202: the key
 * is still being checked); anything else is logged as a warning.
 */
const STATUS_MEANING: Record<number, string> = {
  200: 'URLs submitted',
  202: 'URLs received; key validation pending',
  400: 'bad request',
  403: 'key not valid (key file missing or wrong)',
  422: 'URLs do not belong to the host, or key does not match the protocol',
  429: 'too many requests',
};

export function describeStatus(status: number): string {
  return STATUS_MEANING[status] ?? 'unexpected response';
}
