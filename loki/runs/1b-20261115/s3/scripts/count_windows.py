#!/usr/bin/env python3
"""Verification-only exact 1B corpus count (approach of loki-frame-1b-20261102T110752Z
verification.py). Never used for benchmark query IDs.

A single unfiltered count over 1B rows risks the 600s server query timeout, so
count disjoint (lo, hi] native LogQL windows padded to millisecond boundaries;
their union covers every source timestamp exactly once (the fresh corpus has no
rows outside its known min/max). Every window must report zero ingester access.

  install(adapter, evidence): replaces the adapter's storage-only count gate in
      adapter.load(). A short total is returned as a count (the adapter keeps
      settling and retries, as with its own single count); more rows than
      expected, ingester access or a changed inventory fail the load.
  count_windows.py independent EVIDENCE: after load, against the running read
      target; exits non-zero unless the total is exactly 1,000,000,000.
"""
import json
import os
import signal
import sys
import time
from pathlib import Path


class VerificationDeadline(BaseException):
    pass


EXPECTED_ROWS = 1_000_000_000
MIN_NS = 1758585600000000000
MAX_NS = 1758587364039037200
VERIFY_SPEC = {'kind': 'count', 'filters': [], 'metadata_filters': [], 'labels': {}}


def windows(lo, hi, step_ms=60000):
    if not isinstance(lo, int) or not isinstance(hi, int) or lo > hi or step_ms <= 0:
        raise ValueError('invalid bounds')
    start = lo // 1_000_000 - 1
    end = hi // 1_000_000 + 1
    while start < end:
        stop = min(start + step_ms, end)
        yield start * 1_000_000, stop * 1_000_000, stop - start
        start = stop


# S3 adaptation (verification only): the accepted S3 1B run (loki-frame-1b-20261102T110752Z)
# needed 390s for 30 windows; allow 1800s overall and 300s per window (< server 600s).
TOTAL_TIMEOUT = int(os.environ.get('LP_COUNT_TOTAL_TIMEOUT', '1800'))
WINDOW_TIMEOUT = int(os.environ.get('LP_COUNT_WINDOW_TIMEOUT', '300'))


def count(adapter, manifest, evidence, total_timeout=TOTAL_TIMEOUT):
    lo, hi = manifest['min_timestamp_ns'], manifest['max_timestamp_ns']
    if manifest['rows'] != EXPECTED_ROWS or (lo, hi) != (MIN_NS, MAX_NS):
        raise ValueError('not the approved isolated 1B corpus')
    evidence = Path(evidence)
    evidence.parent.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    before, parts, total = None, [], 0
    old_handler = signal.getsignal(signal.SIGALRM)
    if signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0):
        raise ValueError('another alarm is active')

    def expired(*_):
        raise VerificationDeadline('verification overall deadline')

    def save(status):
        attempts = json.loads(evidence.read_text())['attempts'] if evidence.exists() else []
        if attempts and attempts[-1].get('status') == 'running':
            attempts.pop()
        attempts.append({'status': status, 'count': total, 'elapsed': time.monotonic() - start,
                         'inventory_before': before, 'parts': parts})
        evidence.write_text(json.dumps({
            'method': 'disjoint native time-window corpus verification; benchmark queries unchanged',
            'expected_rows': EXPECTED_ROWS, 'min_timestamp_ns': lo, 'max_timestamp_ns': hi,
            'attempts': attempts}, indent=2) + '\n')

    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, total_timeout)
    try:
        receipt = adapter.load_receipt()
        adapter.validate_contract(manifest, receipt)
        if manifest.get('source_files') != receipt.get('source_files'):
            raise ValueError('manifest/receipt source inventory mismatch')
        before = adapter.object_inventory()
        for a, b, duration in windows(lo, hi):
            remaining = total_timeout - (time.monotonic() - start)
            if remaining <= 0:
                raise VerificationDeadline('verification overall deadline')
            query = f'sum(count_over_time({{dataset="otel_logs"}} | keep dataset [{duration}ms]))'
            t = time.monotonic()
            payload = adapter.api('/loki/api/v1/query', {'query': query, 'time': str(b)},
                                  timeout=min(WINDOW_TIMEOUT, remaining)).json()
            rows = adapter.normalize(VERIFY_SPEC, payload, manifest)
            stats = payload['data'].get('stats', {})
            reached = stats.get('ingester', {}).get('totalReached')
            if type(reached) not in (int, float) or reached != 0:
                raise ValueError('missing/invalid/nonzero ingester evidence')
            if len(rows) != 1 or not isinstance(rows[0]['count'], int):
                raise ValueError('invalid count response')
            n = rows[0]['count']
            total += n
            if n < 0 or total > EXPECTED_ROWS:
                raise ValueError(f'invalid/duplicate persisted rows (cumulative {total})')
            parts.append({'start_exclusive_ns': a, 'end_inclusive_ns': b, 'query': query, 'count': n,
                          'wall': time.monotonic() - t, 'stats': stats})
            save('running')
            print(f'verification window {len(parts)}: {n} rows; cumulative {total}', flush=True)
        after = adapter.object_inventory()
        if before != after:
            raise ValueError('persisted inventory changed during verification')
        save('verified' if total == EXPECTED_ROWS else 'short')
        return ([{'count': total}], time.monotonic() - start,
                {'verification_method': 'partitioned-native-count', 'parts': parts,
                 'ingester': {'totalReached': sum(p['stats']['ingester']['totalReached'] for p in parts)}})
    except (Exception, VerificationDeadline) as exc:
        signal.setitimer(signal.ITIMER_REAL, 0)
        save('failed')
        # ValueError deliberately escapes adapter.load's request-error retry loop.
        raise ValueError('partitioned persisted-count verification failed: ' + str(exc)) from exc
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)


def install(adapter, evidence):
    original = adapter.query_response

    def wrapped(spec, manifest, timeout=610, diagnostics=None):
        if spec == VERIFY_SPEC:
            return count(adapter, manifest, evidence)
        return original(spec, manifest, timeout=timeout, diagnostics=diagnostics)
    adapter.query_response = wrapped


if __name__ == '__main__':
    if sys.argv[1:2] != ['independent'] or len(sys.argv) != 3:
        raise SystemExit('usage: count_windows.py independent EVIDENCE.json')
    sys.path.insert(0, os.environ.get('LP_ROOT', '/srv/lp') + '/source/loki')
    import adapter  # noqa: E402
    rows, elapsed, _ = count(adapter, adapter.read_manifest(), sys.argv[2])
    print(json.dumps({'count': rows[0]['count'], 'elapsed': elapsed}))
    sys.exit(0 if rows[0]['count'] == EXPECTED_ROWS else 1)
