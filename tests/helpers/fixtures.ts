import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setDataDir } from '../../src/lib/data';

export interface FixtureData {
  picks?: any[];
  catalog?: any[];
  guests?: any[];
  picksRaw?: any[];
}

const realDataDir = join(process.cwd(), 'data');

/**
 * Write fixture JSON to a temp directory and point data.ts at it.
 * Returns a cleanup function for afterEach.
 */
export function makeFixtureDir(data: FixtureData): () => void {
  const dir = mkdtempSync(join(tmpdir(), 'ccp-fixture-'));
  writeFileSync(join(dir, 'picks.json'), JSON.stringify(data.picks ?? []));
  writeFileSync(join(dir, 'criterion_catalog.json'), JSON.stringify(data.catalog ?? []));
  writeFileSync(join(dir, 'guests.json'), JSON.stringify(data.guests ?? []));
  writeFileSync(join(dir, 'picks_raw.json'), JSON.stringify(data.picksRaw ?? []));
  setDataDir(dir);
  return () => {
    setDataDir(realDataDir);
    rmSync(dir, { recursive: true, force: true });
  };
}
