import { configDefaults, defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/**/*.test.ts'],
    // tests/built/ checks the output of `bun run build`; `bun run test:seo` runs it.
    exclude: [...configDefaults.exclude, 'tests/built/**'],
  },
});
