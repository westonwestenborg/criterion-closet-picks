import type { APIRoute } from 'astro';
import { getFilmExport } from '../../lib/exports';

export const prerender = true;

export const GET: APIRoute = () => Response.json(getFilmExport());
