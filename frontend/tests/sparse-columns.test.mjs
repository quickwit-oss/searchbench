// Sparse columns (sparse_columns.py): fewer than half of their supported queries finished, or
// a `timeout_cap` other than 60 s (not comparable). They keep their per-query cells, but stay out
// of the geomean and CPU common query sets, so adding one leaves every other column's summary rows
// unchanged.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import test from 'node:test';
import { chromium } from 'playwright';
import { commonGeomean } from '../common_geomean.mjs';

const root = fileURLToPath(new URL('../../', import.meta.url));
function markRows(rows) {
  const temp = mkdtempSync(join(tmpdir(), 'searchbench-sparse-'));
  try {
    const path = join(temp, 'rows.json');
    writeFileSync(path, JSON.stringify(rows));
    execFileSync('python3', [join(root, 'frontend/sparse_columns.py'), path], { stdio: 'pipe' });
    return JSON.parse(readFileSync(path, 'utf8'));
  } finally {
    rmSync(temp, { recursive: true, force: true });
  }
}
const column = (system, finished, supported = 10) => ({
  system,
  dataset: 'd',
  result: [
    ...Array.from({ length: finished }, () => [1, 0.5, 0.5]),
    ...Array.from({ length: supported - finished }, () => [60, 60, 60]),
    [null, null, null],
  ],
});

test('a column is sparse when it finishes fewer than half of its supported queries', () => {
  const [half, below, none] = markRows([
    column('half', 5),
    column('below', 4),
    { system: 'none', dataset: 'd', result: [[null, null, null]] },
  ]);
  assert.equal(half.sparse, undefined, 'exactly half is not sparse');
  assert.deepEqual(below.sparse, { finished: 4, supported: 10 });
  assert.equal(none.sparse, undefined, 'no supported query: nothing to compare, not marked');
  assert.ok(!Object.hasOwn(half, 'sparse'), 'other rows are left byte-identical');
});

test('sparse engines neither shrink the common set nor set the fastest value', () => {
  const A = { system: 'A', r: { q1: 1, q2: 4, q3: 2 } };
  const B = { system: 'B', r: { q1: 2, q2: 1, q3: 2 } };
  const S = { system: 'S', sparse: { finished: 1, supported: 3 }, r: { q1: 0.1, q2: 60, q3: 60 } };
  const value = (e, id) => e.r[id];
  const timeout = v => v >= 60;
  const without = commonGeomean([A, B], ['q1', 'q2', 'q3'], value, timeout, 0);
  const withS = commonGeomean([A, B, S], ['q1', 'q2', 'q3'], value, timeout, 0);
  assert.equal(withS.common, 3);
  assert.equal(withS.sparse, 1);
  assert.equal(withS.get('A'), without.get('A'));
  assert.equal(withS.get('B'), without.get('B'));
  assert.equal(withS.get('S'), null);
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

test('the 600 s-cap Loki 1B column of hippo-20261105 leaves the other columns unchanged', { timeout: 120000 }, async (t) => {
  const temp = mkdtempSync(join(tmpdir(), 'searchbench-hippo-loki-'));
  t.after(() => rmSync(temp, { recursive: true, force: true }));
  const runWithout = join(temp, 'hippo-without-loki');
  cpSync(join(root, 'runs/hippo-20261105'), runWithout, { recursive: true });
  rmSync(join(runWithout, 'loki'), { recursive: true, force: true });
  const withoutHtml = join(temp, 'without.html');
  const withHtml = join(temp, 'with.html');
  execFileSync('bash', ['frontend/build_run.sh', runWithout, withoutHtml], { cwd: root, stdio: 'pipe' });
  execFileSync('bash', ['frontend/build_run.sh', 'runs/hippo-20261105', withHtml], { cwd: root, stdio: 'pipe' });

  const key = row => `${row.dataset}/${row.system}`;
  const before = new Map(embeddedRows(withoutHtml).map(row => [key(row), row]));
  const after = embeddedRows(withHtml);
  const lokiName = 'Loki 3.7.9 · S3 · prefilter on · c16 · 600 s cap';
  const added = after.filter(row => !before.has(key(row)));
  assert.deepEqual(added.map(row => row.system), [lokiName]);
  for (const row of after.filter(row => before.has(key(row)))) {
    const { _source: a, ...rest } = row, { _source: b, ...old } = before.get(key(row));
    assert.deepEqual(rest, old, `${key(row)} unchanged (CPU, GET sums, common n, everything)`);
  }
  const [loki] = added;
  assert.deepEqual(loki.sparse, { finished: 54, supported: 56, reason: '600 s cap; not comparable' });
  assert.equal(loki.cpu_cold, undefined, 'no CPU sum over a different query set');
  assert.equal(loki.s3_gets_cold, undefined, 'no GET sum over a different query set');
  assert.ok(loki.s3_requests && Object.keys(loki.s3_requests).length === 56, 'per-query GET counters present');

  const browser = await chromium.launch();
  try {
    const context = await browser.newContext({ offline: true, viewport: { width: 1600, height: 1000 } });
    const page = await context.newPage(), errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(pathToFileURL(withHtml).href); await page.locator('.sb-app').waitFor();
    const names = (await page.locator('.engcol').allInnerTexts()).map(s => s.replace(/^\W*/, '').split('\n')[0]);
    const idx = names.indexOf(lokiName);
    assert.ok(idx >= 0, 'Loki column rendered');
    assert.match(await page.locator('.engcol').nth(idx).innerText(), /finished 54\/56 q\. · 600 s cap; not comparable/);
    const geoCell = page.locator('.trow', { has: page.locator('.rowlab[data-key="geomean"]') }).locator('.cell').nth(idx);
    assert.equal(await geoCell.innerText(), '—');
    assert.match(await geoCell.getAttribute('title'), /^600 s cap; not comparable: finished 54 of 56/);
    assert.match(await page.locator('.sb-app').innerText(), /💀 = the column's cap: 60 s, or 600 s where noted/);
    const cellOf = id => page.locator('.trow', { has: page.locator(`.rowlab[data-key="q:${id}"]`) }).locator('.cell').nth(idx);
    await page.getByRole('button', { name: 'Cold', exact: true }).click();
    assert.doesNotMatch(await cellOf('Q01').innerText(), /💀/, 'Q01 finishes under the 600 s cap');
    assert.match(await cellOf('Q14').innerText(), /💀/, 'Q14 times out at 600 s');
    assert.match(await cellOf('Q29').innerText(), /💀/, 'Q29 times out at 600 s');
    assert.match(await cellOf('Q29').getAttribute('title'), /timed out — killed at the 600s cap/);
    assert.deepEqual(errors, []);
    await context.close();
  } finally {
    await browser.close();
  }
});
