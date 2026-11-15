#!/usr/bin/env bash
set -euo pipefail
ROOT=/mnt/local-ssd/searchbench
BASE=<home>/loki-results
OUT=$BASE/read-codecs
mkdir "$OUT"
exec >> "$OUT/run.log" 2>&1
status() { echo "$(date -u +%FT%TZ) $*" | tee -a "$BASE/status" "$OUT/status"; }
finish() {
    rc=$?
    trap - EXIT
    mkdir -p "$OUT/loki-results"
    cp -a "$ROOT/loki/results/." "$OUT/loki-results/" || true
    cp "$ROOT/SOURCE_COMMIT" "$OUT/SOURCE_COMMIT" || true
    (cd "$ROOT/loki"; ./stop) || true
    status "read-codecs exit rc=$rc"
    exit "$rc"
}
trap finish EXIT
trap 'exit 143' TERM
source /mnt/local-ssd/loki-env.sh
export SEARCHBENCH_SKIP_DOWNLOAD=yes LOKI_TARGET=auto
unset LOKI_SPLIT_METRIC_QUERIES_INTERVAL LOKI_SPLIT_QUERIES_INTERVAL LOKI_TSDB_MAX_BYTES_PER_SHARD LOKI_QUERY_CONCURRENCY
cp "$ROOT/SOURCE_COMMIT" "$OUT/SOURCE_COMMIT"
cp <home>/run-read-codecs.sh "$OUT/run-read-codecs.sh"
status 'fresh read-profile codec loads + complete cold/hot/hot suites start'
cd "$ROOT/loki"
python - <<'PY' > "$OUT/old-baseline-inventory.json"
import json
from pathlib import Path
root=Path('/mnt/local-ssd/loki-100k-baseline-r1/chunks')
files=[{'key':str(p.relative_to(root)),'size':p.stat().st_size} for p in sorted(root.rglob('*')) if p.is_file()]
print(json.dumps({'files':files,'total_bytes':sum(x['size'] for x in files)},indent=2))
PY
for codec in snappy lz4-256k zstd; do
    export LOKI_DATA_DIR="/mnt/local-ssd/loki-100k-read-$codec" LOKI_CHUNK_ENCODING="$codec"
    test ! -e "$LOKI_DATA_DIR"
    version="3.7.9-m8gd4-100k-read-$codec"
    status "$codec full load and cold suite start"
    ./benchmark.sh --index --version "$version" > "$OUT/$codec-benchmark.log" 2>&1
    mkdir "$OUT/$codec"
    for file in manifest.json config.json runtime.json ingest-metrics.txt load-started.json; do
        if [[ -f "$LOKI_DATA_DIR/$file" ]]; then cp "$LOKI_DATA_DIR/$file" "$OUT/$codec/"; fi
    done
    docker inspect searchbench-loki --format '{{json .Config.Cmd}}' > "$OUT/$codec/container-command.json"
    ./stop
    python adapter.py verify-layout | tee "$OUT/$codec/layout-after-cold.txt"
    python - "$version" "$OUT/$codec" <<'PY'
import json,os,sys
from pathlib import Path
version,out=sys.argv[1:];out=Path(out)
r=json.loads(Path(f'results/loki_{version}_otel_logs_100k.json').read_text())
ids={json.loads(s)['id'] for s in Path('queries.logql').read_text().splitlines() if s.startswith('{')}
assert len(r['query_tags'])==len(r['result'])==len(r['cpu'])==92
seen=set()
for tag,wall,cpu in zip(r['query_tags'],r['result'],r['cpu']):
    if tag['id'] not in ids: continue
    assert len(wall)==3 and all(isinstance(x,(int,float)) and 0<=x<60 for x in wall),(tag,wall)
    assert len(cpu)==3 and all(isinstance(x,(int,float)) and x>=0 for x in cpu),(tag,cpu)
    seen.add(tag['id'])
assert seen==ids and len(seen)==56
assert r['load_time']>0 and r['data_size']>0
root=Path(os.environ['LOKI_DATA_DIR'])/'chunks'
files=[{'key':str(p.relative_to(root)),'size':p.stat().st_size} for p in sorted(root.rglob('*')) if p.is_file()]
(out/'object-inventory.json').write_text(json.dumps({'files':files,'total_bytes':sum(x['size'] for x in files)},indent=2))
print('Validated all 56 supported queries with wall and CPU samples; layout unchanged')
PY
    mkdir -p "$OUT/loki-results"
    cp -a "$ROOT/loki/results/." "$OUT/loki-results/"
    status "$codec full load and cold suite passed"
done
cd "$ROOT"
python lib/check_results.py otel_logs_100k --ref ClickHouse --out "$OUT/correctness-vs-clickhouse.txt" > "$OUT/correctness-summary.txt" 2>&1
status 'all codecs agree with ClickHouse correctness reference'
cd "$ROOT/loki"
export LOKI_DATA_DIR=/mnt/local-ssd/loki-100k-read-snappy LOKI_CHUNK_ENCODING=snappy
status 'repeat six query settings on the frozen read-profile layout'
python experiments/tune_100k.py --out "$OUT/query-sweep" > "$OUT/query-sweep.log" 2>&1
status 'read-profile query setting sweep passed'
status 'three-codec rotated warm sweep start'
python experiments/tune_100k.py --codecs-prefix /mnt/local-ssd/loki-100k-read- --out "$OUT/codec-sweep" > "$OUT/codec-sweep.log" 2>&1
status 'codec warm sweep passed'
for codec in snappy lz4-256k zstd; do
    export LOKI_DATA_DIR="/mnt/local-ssd/loki-100k-read-$codec" LOKI_CHUNK_ENCODING="$codec"
    python adapter.py verify-layout | tee "$OUT/$codec/layout-final.txt"
done
status 'all final inventories unchanged; read-codecs complete'
