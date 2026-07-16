import type { APIRoute } from 'astro';
import { getPickExport } from '../../lib/exports';

export const prerender = true;

export const GET: APIRoute = () => Response.json(getPickExport());
