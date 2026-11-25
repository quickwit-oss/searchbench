#!/usr/bin/env python3
"""Approved external load-only limits overlay (same as loki-frame-1b-20261102T110752Z
load_config.py and the accepted 10M prefilter run); no source edits. Only these four rate-ceiling paths differ; they are
ceilings, not allocations. Bounded Rust queues/request sizes unchanged; 429 and
partial pushes are still never retried. Runs adapter.load() (claim, Rust ingest,
flush, stop, read+compactor finalization, exact storage-only count gate, manifest),
then stop + frozen-inventory check. Query starts afterwards use the stock config.
The adapter's storage-only count gate is the windowed 1B count (count_windows.py,
approach of the accepted framed 1B run): one unfiltered 1B count risks the 600s
server query timeout. --dry-check validates source files/rows and the effective
limits without starting Loki or claiming the destination.
"""
import copy, json, os, resource, sys, time
from pathlib import Path

OVERRIDES = {
    'ingestion_rate_mb': 1024,
    'ingestion_burst_size_mb': 2048,
    'per_stream_rate_limit': '1024MB',
    'per_stream_rate_limit_burst': '2048MB',
}
DRY = sys.argv[1:2] == ['--dry-check']
OUT = Path(sys.argv[2] if DRY else sys.argv[1])
LP = os.environ.get('LP_ROOT', '/srv/lp')
sys.path.insert(0, LP + '/source/loki')
sys.path.insert(0, LP)
import adapter  # noqa: E402
import count_windows  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

EXPECTED_ROWS = count_windows.EXPECTED_ROWS


def source_check():
    """Exactly the files adapter.load() will read, and their row total."""
    files = sorted(Path(os.environ['SEARCHBENCH_DATA_DIR']).glob('part_*.parquet'))
    rows = {str(f): pq.ParquetFile(f).metadata.num_rows for f in files}
    assert [f.name for f in files] == ['part_000.parquet'] and sum(rows.values()) == EXPECTED_ROWS, rows
    return rows


def slice_usage():
    if os.environ.get('LP_LOCAL_DRYRUN'):
        return 0.0
    path = Path('/sys/fs/cgroup') / os.environ.get('SEARCHBENCH_CPU_SLICE', 'searchbench.slice') / 'cpu.stat'
    return int(next(l.split()[1] for l in path.read_text().splitlines() if l.startswith('usage_usec'))) / 1e6


def rusage():
    s, c = resource.getrusage(resource.RUSAGE_SELF), resource.getrusage(resource.RUSAGE_CHILDREN)
    return s.ru_utime + s.ru_stime + c.ru_utime + c.ru_stime


stock_make_config = adapter.make_config


def effective_config(*args, **kwargs):
    stock = stock_make_config(*args, **kwargs)
    effective = copy.deepcopy(stock)
    for key, value in OVERRIDES.items():
        assert key in effective['limits_config'], key
        effective['limits_config'][key] = value
    (OUT / 'load-stock-config.json').write_text(json.dumps(stock, indent=2, sort_keys=True))
    (OUT / 'load-effective-config.json').write_text(json.dumps(effective, indent=2, sort_keys=True))
    return effective


adapter.make_config = effective_config
if os.environ.get('LP_LOCAL_DRYRUN'):  # local preflight only: native Loki instead of Docker
    sys.path.insert(0, LP + '/source/loki/tests')
    import native_engine  # noqa: E402
    native_engine.make_config = effective_config
    adapter.start, adapter.stop = native_engine.start, native_engine.stop
OUT.mkdir(parents=True, exist_ok=True)
if DRY:
    s = adapter.settings()
    assert s['target'] == 'all' and not Path(s['data_dir']).exists(), 'fresh destination required'
    if os.environ.get('LP_LOCAL_DRYRUN') != 'filesystem':
        assert s['storage'] == 's3' and next(adapter.s3_objects(), None) is None, 'S3 prefix must be empty'
    limits = effective_config(port=s['port'], listen='127.0.0.1')['limits_config']
    assert {k: limits[k] for k in OVERRIDES} == OVERRIDES, limits
    stock = stock_make_config(port=s['port'], listen='127.0.0.1')['limits_config']
    print(json.dumps({'source_dir': os.environ['SEARCHBENCH_DATA_DIR'], 'source_files': source_check(),
                      'data_dir': s['data_dir'], 'container': s['container'], 'target': s['target'],
                      'effective_limits': {k: limits[k] for k in OVERRIDES},
                      'stock_limits': {k: stock[k] for k in OVERRIDES},
                      'query_timeout': stock['query_timeout'],
                      'storage': s['storage'], 'bucket': s['bucket'], 'prefix': s['prefix'],
                      'chunk_encoding': stock_make_config(port=s['port'], listen='127.0.0.1')['ingester']['chunk_encoding'],
                      'storage_config': stock_make_config(port=s['port'], listen='127.0.0.1')['common']['storage'],
                      'object_prefix': stock_make_config(port=s['port'], listen='127.0.0.1').get('storage_config', {}).get('object_prefix')}, indent=2))
    sys.exit(0)
(OUT / 'load-overrides.json').write_text(json.dumps({
    'approved_by': 'parent supervisor: load-only overlay from the start (10M attempt 1 hit HTTP 429 at 128MB/s)',
    'scope': 'load process only, including finalization; query starts use the stock config',
    'limits_config': OVERRIDES, 'source': 'loki-frame-1b-20261102T110752Z load_config.py'}, indent=2))
s = adapter.settings()
assert s['target'] == 'all' and not (Path(s['data_dir']) / 'load-started.json').exists(), 'fresh destination required'
(OUT / 'load-source-check.json').write_text(json.dumps(source_check(), indent=2))
count_windows.install(adapter, OUT / 'load-verification.json')
cpu0, r0, start = slice_usage(), rusage(), time.monotonic()
status = 'failed'
try:
    adapter.load()
    adapter.stop()
    adapter.verify_layout()
    manifest = json.loads((Path(s['data_dir']) / 'manifest.json').read_text())
    assert manifest['rows'] == EXPECTED_ROWS, manifest['rows']
    assert manifest['verification_stats']['verification_method'] == 'partitioned-native-count'
    assert json.loads((OUT / 'load-verification.json').read_text())['attempts'][-1]['status'] == 'verified'
    status = 'complete'
finally:
    wall = time.monotonic() - start
    containers, procs = slice_usage() - cpu0, rusage() - r0
    record = {'status': status, 'wall_seconds': wall, 'load_cpu_seconds': containers + procs,
              'load_cpu_containers_seconds': containers, 'load_cpu_procs_seconds': procs,
              'includes': ['all-target start', 'Rust ingestion', 'flush', 'stop', 'read+compactor finalization',
                           'compaction settle', 'exact storage-only windowed count', 'stop', 'frozen inventory check'],
              'excludes': ['corpus download/hash', 'client build', 'image pull']}
    if status == 'complete' and s['storage'] != 's3':  # local dry run only
        record['data_size_bytes'] = sum(p.stat().st_size for p in (Path(s['data_dir']) / 'chunks').rglob('*') if p.is_file())
    elif status == 'complete':
        # S3: every object under the dedicated prefix (adapter ./data-size); the
        # 1-object load lock is excluded only from storage_units (adapter ./storage-units).
        objects = list(adapter.s3_objects())
        lock = s['prefix'] + '_searchbench_load_lock.json'
        record['data_size_bytes'] = sum(o['Size'] for o in objects)
        record['storage_units'] = {'count': sum(o['Key'] != lock for o in objects), 'unit': 'objects'}
        record['s3_objects_by_top'] = {}
        for o in objects:
            top = o['Key'][len(s['prefix']):].split('/', 1)[0]
            entry = record['s3_objects_by_top'].setdefault(top, {'objects': 0, 'bytes': 0})
            entry['objects'] += 1
            entry['bytes'] += o['Size']
    (OUT / 'load-record.json').write_text(json.dumps(record, indent=2) + '\n')
    print(json.dumps(record), flush=True)
