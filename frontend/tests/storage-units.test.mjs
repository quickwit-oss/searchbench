import assert from 'node:assert/strict';
import test from 'node:test';
import { sbStorageText, sbStorageValid } from '../storage_units.mjs';

test('counts retain exact units and singular/plural labels', () => {
  for (const unit of ['splits', 'parts', 'files', 'objects']) {
    assert.equal(sbStorageText({ count: 0, unit }), `0 ${unit}`);
    assert.equal(sbStorageText({ count: 1, unit }), `1 ${unit.slice(0, -1)}`);
    assert.equal(sbStorageText({ count: 12345, unit }), `12,345 ${unit}`);
  }
});

test('missing and malformed counts stay unknown, not zero', () => {
  for (const value of [null, undefined, {}, 5, { count: '100', unit: 'parts' },
    { count: -1, unit: 'files' }, { count: 1.5, unit: 'splits' },
    { count: NaN, unit: 'parts' }, { count: Infinity, unit: 'objects' },
    { count: Number.MAX_SAFE_INTEGER + 1, unit: 'files' }, { count: 1, unit: 'bytes' }]) {
    assert.equal(sbStorageValid(value), false);
    assert.equal(sbStorageText(value), '—');
  }
});
