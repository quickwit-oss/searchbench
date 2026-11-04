import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import test from 'node:test';
import { chromium } from 'playwright';
import { sbRunUrl, sbSelectRun, sbShortDescription } from '../run_selector.mjs';

const hippo = 'hippo-20261105';
// Run ids for the pure URL/state tests below (no files needed).
const c8 = 'run-a-100m', m8 = 'run-b-1b', both = 'run-combined', loki = 'run-retired';
const encode = value => Buffer.from(JSON.stringify(value)).toString('base64url');
const decode = url => JSON.parse(Buffer.from(new URL(url).searchParams.get('s'), 'base64url').toString());

test('a long run description is cut to its first segments, never in a word', () => {
  assert.equal(sbShortDescription('16 vCPU ARM64 · 32 GiB RAM · local NVMe'), '16 vCPU ARM64 · 32 GiB RAM · local NVMe');
  assert.equal(sbShortDescription('16 vCPU ARM64 · 64 GiB RAM · local NVMe and S3 (us-east-1) · 100K and 1B rows · caches off'),
    '16 vCPU ARM64 · 64 GiB RAM · local NVMe and S3 (us-east-1)…');
  assert.equal(sbShortDescription('Local NVMe / S3, plus m8g EBS references. Hardware differs: see column details.'),
    'Local NVMe / S3, plus m8g EBS references. Hardware…');
  assert.equal(sbShortDescription(''), '');
  assert.equal(sbShortDescription(undefined), '');
});

test('run switch preserves view/metric/cache, resets engine filters, and validates the dataset', () => {
  const packed = { v: 1, d: 'otel_logs_1b', t: 'charts', r: 'cold', c: 'sec', q: 'count', th: 'light',
    i: 'Quickwit · previous', h: 'hidden', e: 'old', ed: -1, o: 'old,other' };
  const href = `https://example.com/searchbench/?s=${encode(packed)}&runSet=${m8}&include=old&hidden=x&order=old&sortEngine=old&sortEngineDir=-1&run=cold#m=cpu`;
  const next = sbRunUrl(href, { id: c8, datasets: ['otel_logs_100m'] }, 'otel_logs_1b');
  const url = new URL(next), data = decode(next);
  assert.equal(url.pathname, '/searchbench/');
  assert.equal(url.hash, '#m=cpu');
  assert.equal(url.searchParams.get('runSet'), c8);
  assert.equal(url.searchParams.get('run'), 'cold', 'legacy cache-state field is not repurposed');
  assert.deepEqual(data, { v: 1, d: 'otel_logs_100m', t: 'charts', r: 'cold', c: 'sec', q: 'count', th: 'light' });
  for (const key of ['include', 'hidden', 'order', 'sortEngine', 'sortEngineDir']) assert.equal(url.searchParams.has(key), false);
  assert.equal(decode(sbRunUrl('https://example.com/?s=invalid', { id: m8, datasets: ['otel_logs_100m', 'otel_logs_1b'] }, 'otel_logs_1b')).d, 'otel_logs_1b');
  assert.equal(decode(sbRunUrl(`https://example.com/?s=${encode({ v: 1, d: 'otel_logs_100m' })}&dataset=otel_logs_1b`,
    { id: m8, datasets: ['otel_logs_100m', 'otel_logs_1b'] }, 'otel_logs_100m')).d, 'otel_logs_1b');
});

test('retired run aliases preserve packed state and default to their original dataset', () => {
  const prior = { location: globalThis.location, history: globalThis.history, run: globalThis.__searchbenchRun };
  const catalog = [{ id: both, datasets: ['otel_logs_1b', 'otel_logs_100k'], aliases: [{ id: loki, dataset: 'otel_logs_100k' }] }];
  const rows = [{ run_id: both, marker: 'canonical' }, { run_id: loki, marker: 'duplicate' }];
  try {
    globalThis.history = { state: { marker: 1 }, replaceState(state, _, href) {
      assert.deepEqual(state, { marker: 1 }); globalThis.location = new URL(href);
    } };
    for (const state of [null, 'invalid', encode({ v: 1, d: 'missing-dataset' }), encode({ v: 1, r: 'cold', i: 'Loki · Snappy' }), encode({ v: 1, d: 'otel_logs_100k', r: 'cold', i: 'Loki · Snappy' })]) {
      const u = new URL(`https://example.com/?runSet=${loki}&include=Loki&cells=sec#m=cpu`);
      if (state !== null) u.searchParams.set('s', state);
      globalThis.location = u;
      assert.deepEqual(sbSelectRun(rows, catalog), [rows[0]]);
      assert.equal(location.searchParams.get('runSet'), both);
      assert.equal(location.searchParams.get('include'), 'Loki');
      assert.equal(location.searchParams.get('s'), state);
      assert.equal(location.hash, '#m=cpu');
      assert.equal(location.searchParams.get('dataset') ?? decode(location.href).d, 'otel_logs_100k');
    }
    globalThis.location = new URL(`https://example.com/?runSet=${loki}&dataset=otel_logs_1b`);
    sbSelectRun(rows, catalog);
    assert.equal(location.searchParams.get('dataset'), 'otel_logs_1b', 'explicit dataset wins');
  } finally {
    for (const [key, value] of Object.entries({ location: prior.location, history: prior.history, __searchbenchRun: prior.run })) {
      if (value === undefined) delete globalThis[key]; else globalThis[key] = value;
    }
  }
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

test('the published page selects runs, keeps state across switches, and opens on the public bench', { timeout: 120000 }, async (t) => {
  const root = fileURLToPath(new URL('../../', import.meta.url));
  const temp = mkdtempSync(join(tmpdir(), 'searchbench-unified-test-'));
  t.after(() => rmSync(temp, { recursive: true, force: true }));
  execFileSync('node', ['frontend/build_site.mjs', temp], { cwd: root, stdio: 'pipe' });
  const index = join(temp, 'index.html'), rows = embeddedRows(index);
  // The run set's rows are built unchanged from runs/hippo-20261105.
  const runRows = rows.filter(row => row.run_id === hippo).map(({ run_id, ...row }) => row);
  const runJson = join(temp, 'hippo.json');
  execFileSync('bash', ['frontend/build_run.sh', `runs/${hippo}`, runJson, '--rows-only'], { cwd: root, stdio: 'pipe' });
  assert.deepEqual(runRows, JSON.parse(readFileSync(runJson, 'utf8')), 'all columns and metrics preserved');
  assert.deepEqual(runRows.map(row => row.system).sort(), ['ClickHouse · S3 · 1.7 GiB cap · 61 parts · public bench',
    'Datadog BYOC 0.1.36 · S3 · ingest API · 64 splits · public bench', 'DuckDB 2.0 · S3 · sorted copy']);
  for (const row of runRows) {
    const source = JSON.parse(readFileSync(join(root, 'runs', hippo, row._source), 'utf8'));
    assert.deepEqual(row.result, source.result, 'UI inputs preserve the recorded measurements');
    assert.deepEqual(row.cpu, source.cpu);
    assert.equal(row.dataset, 'otel_logs_1b');
    assert.equal(row.s3_gets_common_n, 57);
  }
  assert.deepEqual(rows.filter(row => row.run_id === 'upstream').map(({ run_id, ...row }) => row),
    JSON.parse(readFileSync(join(root, 'frontend/results.json'), 'utf8')));

  const browser = await chromium.launch();
  try {
    const context = await browser.newContext({ offline: true, viewport: { width: 1440, height: 1000 } });
    const page = await context.newPage(), errors = [], network = [];
    context.on('page', p => p.on('pageerror', e => errors.push(e.message)));
    page.on('pageerror', e => errors.push(e.message));
    context.on('request', req => { if (/^https?:/.test(req.url())) network.push(req.url()); });
    const selector = page.getByRole('combobox', { name: 'RUN / ENVIRONMENT' });
    const base = pathToFileURL(index).href;
    const waitReady = async () => {
      await page.locator('.sb-app').waitFor();
      await page.waitForFunction(() => new URL(location.href).searchParams.has('runSet'));
    };
    const switchRun = async id => {
      await Promise.all([page.waitForEvent('load'), selector.selectOption(id)]);
      await waitReady();
      assert.equal(await selector.inputValue(), id);
      assert.equal(new URL(page.url()).pathname, new URL(base).pathname);
      assert.equal(new URL(page.url()).searchParams.get('runSet'), id);
    };
    // The public bench is the default page: Datadog BYOC, ClickHouse and DuckDB on S3, S3 counters over 57 queries.
    await page.goto(base); await waitReady();
    assert.equal(await selector.inputValue(), hippo);
    assert.deepEqual(await selector.locator('option').evaluateAll(os => os.map(o => o.value)), [hippo, 'upstream']);
    assert.equal(await page.locator('.engcol').count(), 3);
    assert.deepEqual((await page.locator('.storage-units-row [data-storage-system]').allTextContents()).sort(), ['61 parts', '64 splits', '—']);
    assert.match(await page.locator('.rowlab[data-key="s3_gets_cold"]').innerText(), /57 q\./);
    assert.match(await page.locator('.rowlab[data-key="geomean"]').innerText(), /57 common q\./);
    // A long description shows one line; the full text and the links are in a collapsed details.
    const full = await page.locator('.run-desc-short').getAttribute('title');
    assert.ok(full.length > 300, 'this run set has a long description');
    assert.equal(await page.locator('.run-desc-short').innerText(), sbShortDescription(full));
    assert.ok((await page.locator('.run-desc-short').boundingBox()).height < 20, 'one line');
    assert.equal(await page.locator('.run-desc').evaluate(e => e.open), false, 'collapsed by default');
    assert.equal(await page.locator('.run-desc summary').innerText(), 'details · 3 links', 'README + two run.json links');
    assert.equal(await page.locator('.run-desc-full').isVisible(), false);
    await page.locator('.run-desc summary').click();
    assert.equal(await page.locator('.run-desc-full').innerText(), full);
    const links = page.locator('.run-selector .run-links a');
    assert.ok(await links.first().isVisible());
    assert.deepEqual(await links.allTextContents(), ['README', 'Public bench runs', 'DuckDB configuration']);
    assert.match(await links.first().getAttribute('href'), new RegExp(`^https://github\\.com/.+/blob/.+/runs/${hippo}/README\\.md$`));
    assert.equal(await links.first().getAttribute('target'), '_blank');
    await page.locator('.run-desc summary').click();
    // View, metric and cache state survive a run switch; the engine filter is reset.
    const include = runRows[0].system;
    await page.goto(base + `?runSet=${hippo}&include=${encodeURIComponent(include)}&run=cold&cells=sec`); await waitReady();
    assert.equal(await page.locator('.engcol').count(), 1);
    await Promise.all([page.waitForEvent('load'), page.getByRole('button', { name: 'CPU', exact: true }).click()]);
    await waitReady();
    await switchRun('upstream');
    assert.ok(await page.locator('.engcol').count() > 2, 'previous run include filter cleared');
    assert.equal(await page.getByRole('button', { name: 'Cold', exact: true }).getAttribute('class'), 'on');
    assert.equal(await page.getByRole('button', { name: 'Seconds', exact: true }).getAttribute('class'), 'on');
    assert.equal(await page.getByRole('button', { name: 'CPU', exact: true }).getAttribute('class'), 'on');
    assert.equal(await page.locator('.rowlab[data-key="s3_gets_hot"]').innerText(), 'S3 GETs · hot', 'no S3 summary leaked from the public bench');
    await switchRun(hippo);
    assert.equal(await page.locator('.engcol').count(), 3);
    assert.match(await page.locator('.rowlab[data-key="cpu_hot"]').innerText(), /57 q\./);
    const shared = page.url();
    await page.reload(); await waitReady();
    assert.equal(await selector.inputValue(), hippo);
    await page.goBack(); await waitReady();
    assert.equal(await selector.inputValue(), 'upstream');
    await page.goForward(); await waitReady();
    assert.equal(await selector.inputValue(), hippo);
    const fresh = await context.newPage();
    await fresh.goto(shared); await fresh.locator('.sb-app').waitFor();
    assert.equal(await fresh.getByRole('combobox', { name: 'RUN / ENVIRONMENT' }).inputValue(), hippo);
    assert.equal(await fresh.getByRole('button', { name: 'CPU', exact: true }).getAttribute('class'), 'on');
    await fresh.close();
    await page.goto(base + '?runSet=unknown&dataset=otel_logs_1b'); await waitReady();
    assert.equal(await selector.inputValue(), hippo, 'unknown run set falls back to the default');
    assert.equal(await page.locator('.engcol').count(), 3);
    // The legacy per-run path redirects to the one page, keeping its state.
    const legacy = pathToFileURL(join(temp, 'runs', hippo, 'index.html'));
    legacy.search = '?dataset=otel_logs_1b&cells=sec'; legacy.hash = '#m=gets';
    await page.goto(legacy.href);
    await page.waitForURL(url => url.pathname === new URL(base).pathname);
    await waitReady();
    assert.equal(await selector.inputValue(), hippo);
    assert.equal(await page.getByRole('button', { name: 'S3 GETs', exact: true }).getAttribute('class'), 'on');
    assert.equal(await page.getByRole('button', { name: 'Seconds', exact: true }).getAttribute('class'), 'on');
    await page.screenshot({ path: join(tmpdir(), 'searchbench-public-bench.png') });
    assert.deepEqual(errors, []);
    assert.deepEqual(network, []);
    await context.close();
  } finally { await browser.close(); }
});
