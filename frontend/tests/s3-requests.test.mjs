import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const script = fileURLToPath(new URL('../s3_requests.py', import.meta.url));
function aggregate(counters, extra = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'searchbench-s3-'));
  try {
    const rows = Object.keys(counters).map(system => ({ dataset: 'd', system, ...extra }));
    writeFileSync(join(dir, 'rows.json'), JSON.stringify(rows));
    writeFileSync(join(dir, 'counters.json'), JSON.stringify(Object.fromEntries(
      Object.entries(counters).map(([name, c]) => ['d/' + name, c]))));
    execFileSync('python3', [script, join(dir, 'rows.json'), join(dir, 'counters.json')], { stdio: 'pipe' });
    return JSON.parse(readFileSync(join(dir, 'rows.json'), 'utf8'));
  } finally { rmSync(dir, { recursive: true, force: true }); }
}

const trials = (g, b) => [[g, b], [g, b], [g, b]];

test('GET-only counters preserve null bytes and remove stale byte summaries', () => {
  const [r] = aggregate({ Loki: { queries: { Q01: trials(58, null), Q02: trials(0, null) } } },
    { s3_bytes_cold: 123, s3_bytes_hot: 456, s3_bytes_common_n: 92 });
  assert.equal(r.s3_gets_cold, 58);
  assert.equal(r.s3_gets_hot, 58);
  assert.equal(r.s3_gets_common_n, 2);
  assert.equal(r.s3_common_n, 2);
  for (const key of ['s3_bytes_cold', 's3_bytes_hot', 's3_bytes_common_n']) assert.ok(!(key in r));
  assert.deepEqual(r.s3_requests.Q02, trials(0, null));
});

test('measured zero is retained; hot means first hot trial, not minimum', () => {
  const [r] = aggregate({ A: { queries: { Q01: [[0, 0], [2, 20], [1, 10]] } } });
  assert.equal(r.s3_gets_cold, 0);
  assert.equal(r.s3_bytes_cold, 0);
  assert.equal(r.s3_gets_hot, 2);
  assert.equal(r.s3_bytes_hot, 20);
});

test('GET and byte coverage are independent; unmeasured columns do not poison bytes', () => {
  const [both, gets] = aggregate({
    both: { queries: { Q01: trials(1, 10), Q02: trials(2, 20), Q03: trials(3, 30) } },
    gets: { queries: { Q01: trials(4, null), Q02: trials(5, null) } },
  });
  assert.equal(both.s3_gets_common_n, 2);
  assert.equal(both.s3_gets_cold, 3);
  assert.equal(both.s3_bytes_common_n, 3);
  assert.equal(both.s3_bytes_cold, 60);
  assert.equal(gets.s3_gets_cold, 9);
  assert.ok(!('s3_bytes_cold' in gets));
});

test('legacy fully measured columns retain common-set and partial-column behavior', () => {
  const [a, b, partial] = aggregate({
    a: { queries: { Q01: trials(1, 10), Q02: trials(2, 20), Q03: trials(3, 30) } },
    b: { queries: { Q01: trials(4, 40), Q02: trials(5, 50) } },
    partial: { partial: true, queries: { Q01: trials(6, 60) } },
  });
  assert.equal(a.s3_common_n, 2);
  assert.equal(a.s3_gets_cold, 3);
  assert.equal(a.s3_bytes_cold, 30);
  assert.equal(b.s3_bytes_hot, 90);
  assert.ok(!('s3_gets_cold' in partial));
  assert.ok(!('s3_bytes_cold' in partial));
});

test('all-partial fallback and byte-only measurements work without fabricating GETs', () => {
  const [r] = aggregate({ A: { partial: true, queries: { Q01: trials(null, 0) } } });
  assert.equal(r.s3_bytes_cold, 0);
  assert.equal(r.s3_bytes_common_n, 1);
  assert.ok(!('s3_gets_cold' in r));
});

test('missing first-hot data is not summed as zero', () => {
  const [r] = aggregate({ A: { queries: { Q01: [[3, 30], null, [1, 10]] } } });
  assert.ok(!('s3_gets_cold' in r));
  assert.ok(!('s3_bytes_hot' in r));
});
