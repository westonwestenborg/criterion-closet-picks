import { defineConfig } from 'astro/config';
import sitemap from '@astrojs/sitemap';

// Pages that pass `noindex` to BaseLayout. They stay out of the sitemap; the
// SEO gate (`bun run test:seo`) fails if a noindex page is listed, or if a
// page is neither listed nor noindex.
const NOINDEX_PAGES = new Set(['/404/', '/random/', '/llm-export/']);

export default defineConfig({
  output: 'static',
  site: 'https://closetpicks.westenb.org',
  integrations: [
    sitemap({
      filter: (page) => {
        const path = new URL(page).pathname;
        // Only HTML pages, which end in a slash. This keeps out the feed,
        // the data exports and the OG images.
        return path.endsWith('/') && !NOINDEX_PAGES.has(path);
      },
    }),
  ],
});
