// Per-column timeout cap: a row's `timeout_cap` (default 60 s) sets where it times out, and a
// column with another cap is left out of the common query sets (sparse_columns.py `reason`).
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { commonGeomean } from '../common_geomean.mjs';
import { TIMEOUT_CAP, capNote, capOf, coverageMap, geomeanMap, isTimeout } from '../src/shared/lib/metrics.ts';

const root = fileURLToPath(new URL('../../', import.meta.url));

function markRows(rows) {
  const temp = mkdtempSync(join(tmpdir(), 'searchbench-cap-'));
  try {
    const path = join(temp, 'rows.json');
    writeFileSync(path, JSON.stringify(rows));
    execFileSync('python3', [join(root, 'frontend/sparse_columns.py'), path], { stdio: 'pipe' });
    return JSON.parse(readFileSync(path, 'utf8'));
  } finally {
    rmSync(temp, { recursive: true, force: true });
  }
}

const row = (system, result, extra = {}) => ({
  system, dataset: 'd', result, byId: Object.fromEntries(result.map((_, i) => [`q${i + 1}`, i])), ...extra,
});

test('capOf is the row timeout_cap, else the 60 s default', () => {
  assert.equal(TIMEOUT_CAP, 60);
  assert.equal(capOf({}), 60);
  assert.equal(capOf({ timeout_cap: 600 }), 600);
});

test('isTimeout compares against the given cap, 60 s by default', () => {
  assert.equal(isTimeout(60), true);
  assert.equal(isTimeout(59.9), false);
  assert.equal(isTimeout(190.7, 600), false);
  assert.equal(isTimeout(600, 600), true);
  assert.equal(isTimeout(null, 600), false);
});

test('coverage and geomean use each row\'s own cap', () => {
  const capped = row('L', [[190, null, null], [600, null, null]], { timeout_cap: 600 });
  const plain = row('C', [[190, 190, 190], [600, 600, 600]]);
  const cov = coverageMap([capped, plain], ['q1', 'q2'], 'cold');
  assert.deepEqual(cov.get('L'), { supported: 2, completed: 1 });
  assert.deepEqual(cov.get('C'), { supported: 2, completed: 0 });
  assert.equal(geomeanMap([capped], ['q1', 'q2'], 'cold').get('L'), 1, 'q2 at the 600 s cap is dropped');
});

test('the legend names other caps only when a shown column has one', () => {
  assert.equal(capNote([{}, { timeout_cap: 60 }]), null);
  assert.equal(capNote([{}, { timeout_cap: 600 }]), "💀 = the column's cap: 60 s, or 600 s where noted");
});

test('a column with another cap is excluded with a reason; cold-only tries count as finished', () => {
  const capped = row('L', [[null, null, null], [190, null, null], [595, null, null], [600, null, null]], { timeout_cap: 600 });
  const plain = row('C', [[1, 0.5, 0.5], [2, 1, 1]]);
  const explicit60 = row('E', [[1, 0.5, 0.5]], { timeout_cap: 60 });
  const [l, c, e] = markRows([capped, plain, explicit60]);
  assert.deepEqual(l.sparse, { finished: 2, supported: 3, reason: '600 s cap; not comparable' });
  assert.ok(!Object.hasOwn(c, 'sparse'));
  assert.ok(!Object.hasOwn(e, 'sparse'), 'an explicit 60 s cap is the default cap');
});

test('the excluded column changes neither the common set nor the other geomeans', () => {
  const A = { system: 'A', r: { q1: 1, q2: 4, q3: 2 } };
  const B = { system: 'B', r: { q1: 2, q2: 1, q3: 2 } };
  const L = { system: 'L', timeout_cap: 600, sparse: { finished: 2, supported: 3, reason: '600 s cap; not comparable' },
              r: { q1: 0.1, q2: 300, q3: 600 } };
  const value = (e, id) => e.r[id];
  const timeout = (v, e) => isTimeout(v, capOf(e ?? {}));
  const without = commonGeomean([A, B], ['q1', 'q2', 'q3'], value, timeout, 0);
  const withL = commonGeomean([A, B, L], ['q1', 'q2', 'q3'], value, timeout, 0);
  assert.equal(withL.common, 3);
  assert.equal(withL.sparse, 1);
  assert.equal(withL.get('A'), without.get('A'));
  assert.equal(withL.get('B'), without.get('B'));
  assert.equal(withL.get('L'), null);
  // Shown alone, it is compared at its own cap: 300 s finished, 600 s timed out.
  const alone = commonGeomean([L], ['q1', 'q2', 'q3'], value, timeout, 0);
  assert.equal(alone.common, 2);
});
