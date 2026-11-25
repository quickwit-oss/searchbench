import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import test from 'node:test';
import { chromium } from 'playwright';
import { commonGeomean } from '../common_geomean.mjs';

const CAP = 60;
const timeout = v => v >= CAP;

test('only queries every engine answered count, and the set size is reported', () => {
  // B alone answers q3 (where it is trivially fastest); q4 times out for A.
  const A = { system: 'A', r: { q1: 1, q2: 4, q4: 60 } };
  const B = { system: 'B', r: { q1: 2, q2: 1, q3: 5, q4: 1 } };
  const g = commonGeomean([A, B], ['q1', 'q2', 'q3', 'q4'], (e, id) => e.r[id], timeout, 0);
  assert.equal(g.common, 2);
  assert.equal(g.total, 4);
  // q1: A x1, B x2; q2: A x4, B x1.
  assert.ok(Math.abs(g.get('A') - 2) < 1e-9);
  assert.ok(Math.abs(g.get('B') - Math.SQRT2) < 1e-9);
});

test('no common query gives no geomean instead of a misleading x1', () => {
  const A = { system: 'A', r: { q1: 1 } }, B = { system: 'B', r: { q2: 1 } };
  const g = commonGeomean([A, B], ['q1', 'q2'], (e, id) => e.r[id], timeout);
  assert.equal(g.common, 0);
  assert.equal(g.get('A'), null);
  assert.equal(g.get('B'), null);
});

function embeddedRows(path) {
  const html = readFileSync(path, 'utf8');
  const start = html.indexOf('`', html.indexOf('JSON.parse(`[{"system"')) + 1;
  let end = start;
  while (end < html.length) {
    if (html[end] === '\\') { end += 2; continue; }
    if (html[end] === '`') break;
    end++;
  }
  return JSON.parse(html.slice(start, end).replace(/\\([\\`$])/g, '$1'));
}

test('the page geomean is computed on the queries every displayed engine answered', { timeout: 120000 }, async (t) => {
  const root = fileURLToPath(new URL('../../', import.meta.url));
  const temp = mkdtempSync(join(tmpdir(), 'searchbench-geomean-'));
  t.after(() => rmSync(temp, { recursive: true, force: true }));
  const out = join(temp, 'p.html');
  execFileSync('bash', ['frontend/build_run.sh', 'runs/hippo-20261105', out], { cwd: root, stdio: 'pipe' });
  const rows = embeddedRows(out).filter(row => row.dataset === 'otel_logs_1b');
  const ids = rows[0].query_tags.map(tag => tag.id);
  const cold = (r, id) => {
    const v = r.result[ids.indexOf(id)];
    return Array.isArray(v) && typeof v[0] === 'number' ? v[0] : null;
  };
  const expected = commonGeomean(rows, ids, cold, timeout);
  assert.equal(expected.common, 57, 'Datadog BYOC, ClickHouse and DuckDB share 57 queries');
  const geoRow = page => page.locator('.trow', { has: page.locator('.rowlab[data-key="geomean"]') });
  const shownValues = async page => (await geoRow(page).locator('.cell').allInnerTexts())
    .map(s => parseFloat(s.replace(/[^0-9.]/g, ''))).sort((a, b) => a - b);

  const browser = await chromium.launch();
  try {
    const page = await (await browser.newContext({ offline: true })).newPage();
    const errors = []; page.on('pageerror', e => errors.push(e.message));
    await page.goto(pathToFileURL(out).href); await page.locator('.sb-app').waitFor();
    await page.getByRole('button', { name: 'Cold', exact: true }).click();
    assert.match(await page.locator('.rowlab[data-key="geomean"]').innerText(), /Geomean · 57 common q\./);
    assert.deepEqual(await shownValues(page),
      [...expected.values()].map(v => Math.round(v * 100) / 100).sort((a, b) => a - b));
    // Joins: Datadog BYOC answers none, so no query is common to the three columns; the label states
    // the count, so a geomean over fewer engines cannot be read as a comparison of all three.
    await page.locator('[data-act="qtask"][data-task="join"]').click();
    assert.match(await page.locator('.rowlab[data-key="geomean"]').innerText(), /Geomean · 0 common q\./);
    assert.ok((await geoRow(page).locator('.cell').allInnerTexts()).every(s => s.trim() === '—'));
    assert.deepEqual(errors, []);
  } finally {
    await browser.close();
  }
});
