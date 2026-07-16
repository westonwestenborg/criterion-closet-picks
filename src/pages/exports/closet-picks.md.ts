import type { APIRoute } from 'astro';
import { buildMarkdownExport } from '../../lib/exports';

export const prerender = true;

export const GET: APIRoute = () => new Response(buildMarkdownExport(), {
  headers: {
    'Content-Type': 'text/markdown; charset=utf-8',
    'Content-Disposition': 'inline; filename="criterion-closet-picks.md"',
  },
});
