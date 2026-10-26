import { defineConfig } from 'vitest/config';

// Checks over the built site in dist/. Run `bun run build` first.
export default defineConfig({
  test: {
    include: ['tests/built/**/*.test.ts'],
  },
});
