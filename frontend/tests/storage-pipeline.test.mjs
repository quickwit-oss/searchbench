import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { chmodSync, copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), 'searchbench-storage-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const engine = join(root, 'mock');
  mkdirSync(engine);
  mkdirSync(join(root, 'lib'));
  mkdirSync(join(root, 'frontend'));
  copyFileSync(new URL('../../lib/benchmark.sh', import.meta.url), join(root, 'lib/benchmark.sh'));
  copyFileSync(new URL('../build_results', import.meta.url), join(root, 'frontend/build_results'));
  for (const verb of ['install', 'start', 'check', 'stop', 'load']) script(engine, verb, 'exit 0');
  script(engine, 'version', 'echo test');
  script(engine, 'data-size', 'echo 1024');
  // No queries: exercise load/persistence without restarting daemons or dropping host caches.
  writeFileSync(join(engine, 'queries.sql'), '');
  const env = { ...process.env, ENGINE_NAME: 'Mock', ENGINE_TAGS: '["Mock"]',
    SEARCHBENCH_DATA_DIR: join(root, 'corpus'), SEARCHBENCH_DATASET: 'otel_logs_100m',
    SEARCHBENCH_SKIP_DOWNLOAD: 'yes', SEARCHBENCH_MIN_DATA_SIZE: '0' };
  for (const key of ['SEARCHBENCH_INDEX', 'SEARCHBENCH_VERSION', 'SEARCHBENCH_BUILD', 'SEARCHBENCH_RESULTS']) delete env[key];
  env.SEARCHBENCH_CPU = 'off';
  return { root, engine, env };
}
function script(engine, name, body) {
  const path = join(engine, name);
  writeFileSync(path, '#!/usr/bin/env bash\n' + body + '\n');
  chmodSync(path, 0o755);
}
function run(f, args = []) {
  execFileSync('bash', [join(f.root, 'lib/benchmark.sh'), ...args], { cwd: f.engine, env: f.env, stdio: 'pipe' });
}
function json(path) { return JSON.parse(readFileSync(path, 'utf8')); }

test('runner persists counts in the named load record and query-only runs inherit them', (t) => {
  const f = fixture(t);
  script(f.engine, 'storage-units', `echo '{"count":100,"unit":"splits"}'`);
  run(f, ['--index', '--version', 'rust4_local_nvme']);
  const result = join(f.engine, 'results/mock_rust4_local_nvme_otel_logs_100m.json');
  const record = join(f.engine, 'results/load/otel_logs_100m.rust4_local_nvme.json');
  assert.deepEqual(json(result).storage_units, { count: 100, unit: 'splits' });
  assert.deepEqual(json(record).storage_units, { count: 100, unit: 'splits' });
  assert.equal(json(result).build, 'rust4_local_nvme');
  script(f.engine, 'storage-units', `echo '{"count":999,"unit":"splits"}'`);
  run(f, ['--version', 'rust4_local_nvme']);
  assert.equal(json(result).storage_units.count, 100, 'query-only must use the measured index-build record');
  // An authoritative correction must reach the UI, even with underscores in the build name.
  writeFileSync(record, JSON.stringify({ ...json(record), storage_units: { count: 101, unit: 'splits' } }));
  execFileSync('bash', [join(f.root, 'frontend/build_results')], { stdio: 'pipe' });
  assert.equal(json(join(f.root, 'frontend/results.json'))[0].storage_units.count, 101);
});

test('counts preserve main load CPU and phase metrics through the pipeline', (t) => {
  const f = fixture(t);
  f.env.SEARCHBENCH_CPU = 'on';
  f.env.MOCK_CPU_FILE = join(f.root, 'cpu-called');
  script(join(f.root, 'lib'), 'cpu-usage.sh',
    'if [[ -e "$MOCK_CPU_FILE" ]]; then echo 12; else touch "$MOCK_CPU_FILE"; echo 10; fi');
  script(f.engine, 'load', `echo '{"ingest":1,"merge_after":2}' > "$SEARCHBENCH_LOAD_PHASES_FILE"`);
  script(f.engine, 'storage-units', `echo '{"count":100,"unit":"splits"}'`);
  run(f, ['--index', '--version', 'cpu-and-counts']);
  const record = json(join(f.engine, 'results/load/otel_logs_100m.cpu-and-counts.json'));
  assert.deepEqual(record.phases, { ingest: 1, merge_after: 2 });
  assert.equal(record.load_cpu_containers, 2);
  assert.ok(record.load_cpu >= 2);
  assert.equal(typeof record.load_cpu_procs, 'number');
  assert.deepEqual(record.storage_units, { count: 100, unit: 'splits' });
  execFileSync('bash', [join(f.root, 'frontend/build_results')], { stdio: 'pipe' });
  const row = json(join(f.root, 'frontend/results.json'))[0];
  assert.equal(row.load_cpu, record.load_cpu);
  assert.deepEqual(row.storage_units, record.storage_units);
});

test('missing/failed/invalid hooks stay unknown; zero is valid', (t) => {
  const f = fixture(t);
  run(f, ['--index']);
  const result = join(f.engine, 'results/mock_otel_logs_100m.json');
  assert.equal(json(result).storage_units, null);
  for (const body of ['exit 1', "echo nope", `echo '{"count":-1,"unit":"files"}'`,
    `echo '{"count":1.5,"unit":"parts"}'`, `echo '{"count":1,"unit":"bytes"}'`,
    `printf '%s\\n' '{"count":1,"unit":"parts"}' '{"count":2,"unit":"parts"}'`]) {
    script(f.engine, 'storage-units', body);
    run(f, ['--index']);
    assert.equal(json(result).storage_units, null, body);
  }
  script(f.engine, 'storage-units', `echo '{"count":0,"unit":"files"}'`);
  run(f, ['--index']);
  assert.deepEqual(json(result).storage_units, { count: 0, unit: 'files' });
});

test('legacy load records work and explicit unknown replaces stale result metadata', (t) => {
  const f = fixture(t);
  mkdirSync(join(f.engine, 'results/load'), { recursive: true });
  const record = join(f.engine, 'results/load/otel_logs_100m.json');
  writeFileSync(record, JSON.stringify({ load_time: 1, data_size: 1024 }));
  run(f);
  const path = join(f.engine, 'results/mock_otel_logs_100m.json');
  assert.equal(json(path).storage_units, null);
  writeFileSync(path, JSON.stringify({ ...json(path), storage_units: { count: 5, unit: 'parts' } }));
  execFileSync('bash', [join(f.root, 'frontend/build_results')], { stdio: 'pipe' });
  assert.equal(json(join(f.root, 'frontend/results.json'))[0].storage_units.count, 5);
  writeFileSync(record, JSON.stringify({ load_time: 1, data_size: 1024, storage_units: null }));
  execFileSync('bash', [join(f.root, 'frontend/build_results')], { stdio: 'pipe' });
  assert.equal(json(join(f.root, 'frontend/results.json'))[0].storage_units, null);
});

test('Quickwit hook counts only published splits and rejects incomplete listings', (t) => {
  const f = fixture(t);
  const hook = join(f.engine, 'storage-units');
  copyFileSync(new URL('../../datadog-byoc/storage-units', import.meta.url), hook);
  script(f.engine, 'curl', `printf '%s' "$MOCK_SPLITS"`);
  const env = { ...f.env, PATH: `${f.engine}:${f.env.PATH}`, MOCK_SPLITS: JSON.stringify({ offset: 0, size: 3,
    splits: [{ split_state: 'Published' }, { split_state: 'Published' }, { split_state: 'MarkedForDeletion' }] }) };
  const invoke = () => execFileSync('bash', [hook], { env, stdio: 'pipe' });
  assert.deepEqual(JSON.parse(invoke()), { count: 2, unit: 'splits' });
  env.MOCK_SPLITS = JSON.stringify({ offset: 1, size: 0, splits: [] });
  assert.throws(invoke);
  env.MOCK_SPLITS = JSON.stringify({ offset: 0, size: 2, splits: [] });
  assert.throws(invoke);
  env.MOCK_SPLITS = JSON.stringify({ offset: 0, size: 0, splits: [] });
  assert.deepEqual(JSON.parse(invoke()), { count: 0, unit: 'splits' });
});

test('ClickHouse hook measures active table parts, not files or detached parts', (t) => {
  const f = fixture(t);
  const hook = join(f.engine, 'storage-units');
  copyFileSync(new URL('../../clickhouse/storage-units', import.meta.url), hook);
  const client = join(f.engine, 'mock-client');
  const args = join(f.root, 'client-args');
  script(f.engine, 'mock-client', `printf '%s\\n' "$*" > "$MOCK_ARGS"; echo 76`);
  const output = execFileSync('bash', [hook], { env: { ...f.env, CH_BACKEND: 'binary', CLICKHOUSE_BIN: client, MOCK_ARGS: args } });
  assert.deepEqual(JSON.parse(output), { count: 76, unit: 'parts' });
  const query = readFileSync(args, 'utf8');
  assert.match(query, /system\.parts WHERE active/);
  assert.match(query, /database = currentDatabase\(\)/);
  assert.match(query, /table = 'otel_logs'/);
});

test('explicit build identity wins over filename and legacy underscore slugs still resolve', (t) => {
  const f = fixture(t);
  mkdirSync(join(f.engine, 'results/load'), { recursive: true });
  const result = join(f.engine, 'results/mock_rust4_local_nvme_otel_logs_100m.json');
  const base = { dataset: 'otel_logs_100m', result: [], load_time: 9, data_size: 9 };
  writeFileSync(result, JSON.stringify(base));
  writeFileSync(join(f.engine, 'results/load/otel_logs_100m.rust4_local_nvme.json'),
    JSON.stringify({ load_time: 1, data_size: 1024, storage_units: { count: 7, unit: 'files' } }));
  writeFileSync(join(f.engine, 'results/load/otel_logs_100m.custom.json'),
    JSON.stringify({ load_time: 2, data_size: 2048, storage_units: { count: 8, unit: 'objects' } }));
  execFileSync('bash', [join(f.root, 'frontend/build_results')], { stdio: 'pipe' });
  assert.deepEqual(json(join(f.root, 'frontend/results.json'))[0].storage_units, { count: 7, unit: 'files' });
  writeFileSync(result, JSON.stringify({ ...base, build: 'custom' }));
  execFileSync('bash', [join(f.root, 'frontend/build_results')], { stdio: 'pipe' });
  assert.deepEqual(json(join(f.root, 'frontend/results.json'))[0].storage_units, { count: 8, unit: 'objects' });
});
