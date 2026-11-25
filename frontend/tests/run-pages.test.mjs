import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import test from 'node:test';
import { chromium } from 'playwright';

const root = fileURLToPath(new URL('../../', import.meta.url));
function embeddedRows(path) {
  const html = readFileSync(path, 'utf8');
  const marker = 'JSON.parse(`[{"system"';
  const start = html.indexOf('`', html.indexOf(marker)) + 1;
  let end = start;
  while (end < html.length) {
    if (html[end] === '\\') { end += 2; continue; }
    if (html[end] === '`') break;
    end++;
  }
  return JSON.parse(html.slice(start, end).replace(/\\([\\`$])/g, '$1'));
}
function build(run, path) {
  execFileSync('bash', ['frontend/build_run.sh', `runs/${run}`, path], { cwd: root, stdio: 'pipe' });
  return embeddedRows(path);
}
async function metric(page, name) {
  await Promise.all([
    page.waitForEvent('load'),
    page.getByRole('button', { name, exact: true }).click(),
  ]);
  await page.locator('.sb-app').waitFor();
  assert.equal(await page.getByRole('button', { name, exact: true }).getAttribute('class'), 'on');
}

test('the published run page keeps its controls and metrics and shows storage counts offline', { timeout: 120000 }, async (t) => {
  const temp = mkdtempSync(join(tmpdir(), 'searchbench-run-pages-'));
  t.after(() => rmSync(temp, { force: true, recursive: true }));
  const out = join(temp, 'hippo.html');
  const rows = build('hippo-20261105', out);
  assert.equal(rows.length, 4);
  assert.equal(rows.filter(r => r.cpu_by_q).length, 4, 'CPU per query for every column');
  assert.equal(rows.filter(r => r.s3_requests).length, 4, 'S3 GETs and bytes for every column');
  assert.equal(rows.filter(r => r.rounds).length, 3, 'per-round timings for the three engines run in rounds');
  assert.ok(rows.every(r => typeof r.load_cpu === 'number'));
  // Load time excludes a recorded settle check; the measured wall time is kept, never altered otherwise.
  for (const r of rows) {
    if (typeof r.load_check === 'number' && r.load_check > 0) {
      assert.equal(typeof r.load_time_raw, 'number', r.system);
      assert.equal(r.load_time, Math.round(Math.max(r.load_time_raw - r.load_check, 0) * 1000) / 1000, r.system);
    } else {
      assert.ok(!Object.hasOwn(r, 'load_time_raw'), r.system);
    }
  }
  // Timeline milestones are derived only from recorded phases, never invented.
  for (const r of rows) {
    if (typeof r.load_ingest !== 'number') {
      assert.equal(r.load_searchable ?? null, null, r.system);
      assert.equal(r.load_merged ?? null, null, r.system);
      continue;
    }
    assert.equal(r.load_searchable, r.load_ingest);
  }
  const units = Object.fromEntries(rows.map(r => [r.system.split(' · ')[0], r.storage_units ?? null]));
  assert.deepEqual(units, { 'Datadog BYOC 0.1.36': { count: 64, unit: 'splits' }, 'ClickHouse': { count: 61, unit: 'parts' }, 'OpenSearch 3.8': { count: 16, unit: 'segments' }, 'DuckDB 2.0': null });

  const browser = await chromium.launch();
  try {
    const context = await browser.newContext({ offline: true, viewport: { width: 1440, height: 1000 } });
    const page = await context.newPage();
    const errors = [], network = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('request', req => { if (/^https?:/.test(req.url())) network.push(req.url()); });
    await page.goto(pathToFileURL(out).href);
    await page.locator('.sb-app').waitFor();
    assert.equal(await page.locator('.engcol').count(), 4);
    const label = page.getByRole('button', { name: 'Splits / parts / files', exact: true });
    assert.equal(await label.isDisabled(), true);
    assert.match(await page.locator('.storage-units-row').evaluate(el => el.previousElementSibling.textContent), /Load time/);
    const counts = page.locator('.storage-units-row [data-storage-system]');
    const expected = ['16 segments', '61 parts', '64 splits', '—'];
    assert.deepEqual((await counts.allTextContents()).sort(), expected);
    for (const name of ['Wall', 'CPU', 'S3 GETs', 'S3 bytes']) {
      assert.equal(await page.getByRole('button', { name, exact: true }).count(), 1);
    }
    for (const key of ['load_cpu', 'cpu_hot', 's3_gets_cold', 's3_bytes_hot']) {
      assert.equal(await page.locator(`.rowlab[data-key="${key}"]`).count(), 1);
    }
    await page.getByRole('button', { name: 'Millis', exact: true }).click();
    assert.deepEqual((await counts.allTextContents()).sort(), expected);
    const cpu = rows.find(r => r.cpu_by_q && r.details);
    const qid = Object.keys(cpu.cpu_by_q).find(id => cpu.cpu_by_q[id].every(v => typeof v === 'number'));
    const cell = page.locator(`[data-act="cell"][data-sys=${JSON.stringify(cpu.system)}][data-id=${JSON.stringify(qid)}]`);
    await page.getByRole('button', { name: 'Seconds', exact: true }).click();
    await metric(page, 'CPU');
    assert.deepEqual((await counts.allTextContents()).sort(), expected);
    assert.match(await cell.innerText(), /^\d/);
    assert.match(await cell.getAttribute('title'), /s ·/);
    const header = page.locator(`[data-act="engsort"][data-sys=${JSON.stringify(cpu.system)}]`);
    assert.ok((await header.getAttribute('title')).includes(cpu.details));
    await metric(page, 'S3 GETs');
    const s3 = rows.find(r => r.s3_requests);
    const s3id = Object.keys(s3.s3_requests).find(id => s3.s3_requests[id].every(v => v && typeof v[0] === 'number'));
    const s3cell = page.locator(`[data-act="cell"][data-sys=${JSON.stringify(s3.system)}][data-id=${JSON.stringify(s3id)}]`);
    assert.match(await s3cell.getAttribute('title'), /GETs ·/);
    await metric(page, 'Wall');
    await page.getByRole('button', { name: 'Charts', exact: true }).click();
    assert.deepEqual((await page.locator('[data-storage-system]').allTextContents()).sort(), expected);
    await page.getByRole('button', { name: 'Table', exact: true }).click();

    // Measured zero and unknown render differently in the generated bundle too.
    const zeroRows = structuredClone(rows.slice(0, 2));
    zeroRows[0].storage_units = { count: 0, unit: 'objects' };
    delete zeroRows[1].storage_units;
    const zeroJson = join(temp, 'zero.json'), zeroHtml = join(temp, 'zero.html');
    writeFileSync(zeroJson, JSON.stringify(zeroRows));
    execFileSync('node', ['frontend/inject.mjs', 'frontend/index.html', zeroJson, zeroHtml], { cwd: root, stdio: 'pipe' });
    await page.goto(pathToFileURL(zeroHtml).href);
    await page.locator('.sb-app').waitFor();
    assert.deepEqual((await counts.allTextContents()).sort(), ['0 objects', '—']);
    assert.deepEqual(errors, []);
    assert.deepEqual(network, []);
    await context.close();
  } finally { await browser.close(); }
});
