import { defineConfig } from 'astro/config';
import sitemap from '@astrojs/sitemap';

export default defineConfig({
  output: 'static',
  site: 'https://closetpicks.westenb.org',
  integrations: [
    sitemap({
      filter: (page) => {
        const path = new URL(page).pathname;
        return !(
          path === '/404/' ||
          path === '/random/' ||
          path === '/llm-export/' ||
          path === '/feed.xml' ||
          path.startsWith('/exports/')
        );
      },
    }),
  ],
});
