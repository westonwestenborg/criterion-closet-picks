import type { APIRoute } from 'astro';
import { getGuestExport } from '../../lib/exports';

export const prerender = true;

export const GET: APIRoute = () => Response.json(getGuestExport());
