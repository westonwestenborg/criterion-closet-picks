import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { createHash } from 'node:crypto';

const CACHE_DIR = join(process.cwd(), '.cache', 'og');

function hashData(data: unknown): string {
  return createHash('md5').update(JSON.stringify(data)).digest('hex');
}

function cachePath(key: string): string {
  return join(CACHE_DIR, `${key}.png`);
}

function hashPath(key: string): string {
  return join(CACHE_DIR, `${key}.hash`);
}

export function getCached(key: string, dataHash: string): Buffer | null {
  const png = cachePath(key);
  const hash = hashPath(key);
  if (!existsSync(png) || !existsSync(hash)) return null;
  try {
    const stored = readFileSync(hash, 'utf-8').trim();
    if (stored === dataHash) return readFileSync(png);
  } catch {
    return null;
  }
  return null;
}

export function setCache(key: string, dataHash: string, png: Buffer): void {
  const pngFile = cachePath(key);
  mkdirSync(dirname(pngFile), { recursive: true });
  writeFileSync(pngFile, png);
  writeFileSync(hashPath(key), dataHash);
}

export { hashData };
