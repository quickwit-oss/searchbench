#!/usr/bin/env bash
# Datadog CloudPrem v0.1.36 (datadog/cloudprem:v0.1.36, Quickwit fork) on S3, 1B, through the INGEST API, one
# m8gd.4xlarge. Rust client 16 threads, 2,000 docs/request; shard_throughput_limit 20mb, min_shards 48,
# commit 30 s, 10M-doc splits, stable_log, validation off, ingest queue 32/80 GiB, in-memory indexing, document
# clustering as the bench node config. 0) smoke test: one row group through the ingest API on local disk, exact
# count, clustering enabled. 1) 1B load (exact count, merges settled). 2) 3 query rounds alternated with the 82-split
# Parquet bulk-load index (built by Quickwit 71c9181, queried here with CloudPrem), order rotated. Data caches off,
# restart + page-cache drop before every query, 3 tries, 60 s cap.
set -Eeuo pipefail
ROOT=/opt/sb-cp-c30-v2
TAG=20261106T-cloudprem-0136-ingestapi-1b-c30-v2
export ROOT AWS_REGION=us-east-1
mkdir -p "$ROOT"; exec >> "$ROOT/series.log" 2>&1
stage() { printf '%s %s\n' "$(date -Is)" "$*" | tee "$ROOT/status"; }
RAW=s3://<bucket>/searchbench/runs-raw/$TAG
push() { aws s3 sync "$ROOT" "$RAW/" --exclude '*/source/*' --only-show-errors || true; }
stopall() { docker ps -q | xargs -r docker stop -t 60 >/dev/null 2>&1 || true; }
finish() {
    rc=$?; trap - EXIT; set +e
    stopall; printf '%s\n' "$rc" > "$ROOT/exit-code"
    if (( rc == 0 )); then stage completed; else stage "failed rc=$rc"; fi
    sync; shutdown -c || true; push
    shutdown -h +30 'cloudprem c30 runs over'
    exit "$rc"
}
trap finish EXIT
shutdown -h +420 'SearchBench cloudprem c30 safety limit'
CORPUS=/mnt/local/corpus/otel_logs_1b/part_000.parquet
QW_S3_PREFIX=searchbench/m8gd4x-1b-quickwit-s3-$TAG
QW_IMAGE=datadog/cloudprem:v0.1.36
export QW_EXTRA_ENV="CP_ENABLE_REVERSE_CONNECTION=false"   # no Datadog API key / reverse connection in the bench
# resume on this box: bootstrap, corpus, CloudPrem image and Rust client are already in place
[[ -f $CORPUS ]] && docker image inspect "$QW_IMAGE" >/dev/null
QW_CLIENT_BIN=/mnt/local/build/client-src/quickwit/client/target/release/qw-parquet-ingest; [[ -x $QW_CLIENT_BIN ]]
[[ -z "$(aws s3 ls "s3://<bucket>/$QW_S3_PREFIX/" 2>/dev/null)" && -z "$(aws s3 ls "$RAW/" 2>/dev/null)" ]]
P0=/opt/sb-cp-c30
cp "$P0"/{lscpu.txt,memory.txt,disks.txt,corpus.json,quickwit-version.txt,client-build.log,cloudprem-image.txt,cloudprem-pull.log} "$ROOT/" 2>/dev/null || true
tar -xzf "$ROOT/loads.tar.gz" -C "$ROOT"
docker ps -aq | xargs -r docker rm -f >/dev/null 2>&1 || true; rm -rf /mnt/local/cp-smoke /mnt/local/corpus-smoke /mnt/local/cp-1b-s3-api
prepare() {   # $1 trial
    RUN="$ROOT/$1"; mkdir -p "$RUN"; export RUN
    stage "running-$1"
    REPO="$RUN/source"; rm -rf "$REPO"; mkdir -p "$REPO"; tar -xzf "$ROOT/benchmark-source.tar.gz" -C "$REPO" 2>/dev/null
    python3 - <<'PY'
import hashlib,json,os,pathlib
root=pathlib.Path(os.environ['RUN'])/'source'
for n,d in json.loads((pathlib.Path(os.environ['ROOT'])/'source-files.json').read_text()).items():
    assert hashlib.sha256((root/n).read_bytes()).hexdigest()==d,n
PY
    ! grep -q use_text_index_header_cache "$REPO/clickhouse/query"   # header cache stays on (#43)
    export SEARCHBENCH_PYTHON=/opt/searchbench-venv/bin/python3 SEARCHBENCH_DATASET=otel_logs_1b SEARCHBENCH_DATA_DIR=/mnt/local/corpus
    export SEARCHBENCH_CPU=on SEARCHBENCH_CPU_SLICE=searchbench.slice SEARCHBENCH_SKIP_DOWNLOAD=yes EXPECTED_ROWS=1000000000
}
finish_trial() {   # $1 container, $2 rc
    docker logs --timestamps "$1" > "$RUN/server.log" 2>&1 || true
    docker inspect "$1" > "$RUN/container-exit.json" 2>&1 || true
    mkdir -p "$RUN/results"; cp -a results/. "$RUN/results/" 2>/dev/null || true
    printf '%s\n' "$2" > "$RUN/exit-code"
}
ingest() {   # $1 trial, $2 engine dir, $3 container
    stopall; prepare "$1"; cd "$REPO/$2"
    : > "$RUN/ingest-only.queries"; export SEARCHBENCH_QUERIES="$RUN/ingest-only.queries"
    env | grep -E '^(SEARCHBENCH_|QW_|CH_|SERENED_|AWS_REGION)' | sort > "$RUN/environment.txt"
    sync; echo 3 > /proc/sys/vm/drop_caches
    ( while sleep 30; do echo "$(date -Is) $(df -BG --output=used,avail /mnt/local | tail -1) $(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)"; done ) > "$RUN/resources.log" 2>&1 & local mon=$!
    set +e; bash ./benchmark.sh --index --version "$SEARCHBENCH_VERSION" > "$RUN/benchmark.log" 2>&1; local brc=$?; set -e
    kill "$mon" 2>/dev/null || true
    docker stop -t 60 "$3" >/dev/null 2>&1 || true
    finish_trial "$3" "$brc"; (( brc == 0 )) || { echo "ingest $1 failed (rc=$brc)" >&2; return 1; }
    mkdir -p "$ROOT/$1-load"; cp "$RUN/results/load/"*.json "$ROOT/$1-load/"; push
}
queries() {   # $1 trial, $2 engine dir, $3 container, $4 queries file, $5 version, $6 load build, $7 load dir
    stopall; prepare "$1"; cd "$REPO/$2"
    mkdir -p results/load; cp "$7"/*.json results/load/
    export SEARCHBENCH_QUERIES="$4" SEARCHBENCH_BUILD="$6"
    export SEARCHBENCH_TRIES=3 SEARCHBENCH_QUERY_TIMEOUT=60 SEARCHBENCH_COLD_TIMEOUT=60 SEARCHBENCH_RESTART=yes
    if [[ $2 == clickhouse ]]; then
        export CLICKHOUSE_BIN="$PWD/ch-exec"
        ./start >/dev/null 2>&1; for i in $(seq 1 60); do ./check >/dev/null 2>&1 && break; sleep 1; done
        echo 'SELECT count() FROM otel_logs' | ./query > "$RUN/preflight.txt" 2>&1; grep -qx 1000000000 "$RUN/preflight.txt"
    fi
    env | grep -E '^(SEARCHBENCH_|QW_|CH_|CLICKHOUSE_|SERENED_|AWS_REGION)' | sort > "$RUN/environment.txt"
    sha256sum "$4" > "$RUN/queries-sha256.txt"
    date -u '+%Y-%m-%d %H:%M:%S' > "$RUN/query-window-start.txt"
    set +e; bash ./benchmark.sh --version "$5" > "$RUN/benchmark.log" 2>&1; local brc=$?; set -e
    date -u '+%Y-%m-%d %H:%M:%S' > "$RUN/query-window-end.txt"
    docker stop -t 60 "$3" >/dev/null 2>&1 || true
    finish_trial "$3" "$brc"; (( brc == 0 )) || { echo "trial $1 failed (rc=$brc)" >&2; return 1; }
    push
}

# 0) smoke test: one row group, local disk, ingest API, exact count
stage smoke
SMK=/mnt/local/corpus-smoke/otel_logs_1b; mkdir -p "$SMK"
/opt/searchbench-venv/bin/python3 - "$CORPUS" "$SMK/part_000.parquet" <<'PY'
import sys, pyarrow.parquet as pq
t=pq.ParquetFile(sys.argv[1]).read_row_group(0); pq.write_table(t, sys.argv[2], row_group_size=t.num_rows, compression='zstd'); print('smoke rows', t.num_rows)
PY
REPO=$ROOT/smoke-source; rm -rf "$REPO"; mkdir -p "$REPO"; tar -xzf "$ROOT/benchmark-source.tar.gz" -C "$REPO" 2>/dev/null
smoke_ok=1
( cd "$REPO/quickwit"
  export QW_IMAGE QW_PULL_IMAGE=no QW_STORAGE=local QW_CONTAINER=searchbench-cp-smoke QW_DATA_DIR=/mnt/local/cp-smoke
  export QW_ENABLE_IN_MEMORY_INDEXING=true QW_INGEST=http SEARCHBENCH_LOAD_CLIENT=rust QW_CLIENT_BIN SEARCHBENCH_LOAD_PROCESSES=16
  export QW_MIN_SHARDS=48 QW_SHARD_THROUGHPUT_LIMIT=20mb QW_COMMIT_TIMEOUT_SECS=30 QW_SPLIT_NUM_DOCS=10000000 QW_MERGE_POLICY=no_merge
  export QW_VALIDATE_DOCS=off QW_WAIT_MERGES=no SEARCHBENCH_DATASET=otel_logs_1b SEARCHBENCH_DATA_DIR=$SMK SEARCHBENCH_PYTHON=/opt/searchbench-venv/bin/python3
  ./start > "$ROOT/smoke-start.log" 2>&1; echo "start rc=$?" >> "$ROOT/smoke-start.log"
  ./load > "$ROOT/smoke-load.log" 2>&1; echo "load rc=$?" >> "$ROOT/smoke-load.log"
  docker logs "$QW_CONTAINER" > "$ROOT/smoke-server.log" 2>&1 || true
  docker stop -t 30 "$QW_CONTAINER" >/dev/null 2>&1; docker rm -f "$QW_CONTAINER" >/dev/null 2>&1; true )
grep -q '^start rc=0$' "$ROOT/smoke-start.log" || smoke_ok=0
grep -q '^load rc=0$' "$ROOT/smoke-load.log" || smoke_ok=0
grep -q 'otel_logs documents=614400$' "$ROOT/smoke-load.log" || smoke_ok=0
grep -q 'Datadog BYOC version: 0.1.36' "$ROOT/smoke-server.log" || smoke_ok=0
grep -q 'docs_clustering_config: Some(' "$ROOT/smoke-server.log" || smoke_ok=0
grep -q 'CP_ENABLE_REVERSE_CONNECTION' "$ROOT/smoke-server.log" || smoke_ok=0
(( smoke_ok == 1 )) || { echo "SMOKE TEST FAILED" >&2; exit 1; }
rm -rf /mnt/local/cp-smoke /mnt/local/corpus-smoke
stage smoke-ok
push

# common ingest-API settings (1B has no dataset preset: every knob explicit)
api_env() {   # $1 shards, $2 commit, $3 data dir, $4 S3 root
    export QW_IMAGE QW_PULL_IMAGE=no QW_STORAGE=s3 QW_CONTAINER=searchbench-qw-1b-api QW_DATA_DIR=$3 QW_S3_ROOT=$4
    export QW_ENABLE_IN_MEMORY_INDEXING=true QW_INGEST=http SEARCHBENCH_LOAD_CLIENT=rust QW_CLIENT_BIN
    export SEARCHBENCH_LOAD_PROCESSES=16 QW_MIN_SHARDS=$1 QW_COMMIT_TIMEOUT_SECS=$2 QW_SPLIT_NUM_DOCS=10000000
    export QW_MERGE_POLICY=stable_log QW_VALIDATE_DOCS=off QW_INGEST_QUEUE_MEMORY=32GiB QW_INGEST_QUEUE_DISK=80GiB
    export QW_SHARD_THROUGHPUT_LIMIT=20mb
}
sampler() {   # $1 out file: every 10 s, ingest/indexing counters + memory
    while sleep 10; do
        m=$(curl -fsS --max-time 5 http://127.0.0.1:7280/metrics 2>/dev/null | awk '
            /^quickwit_ingest_docs_total/ {i+=$NF} /^quickwit_indexing_processed_docs_total.*valid/ {p+=$NF}
            /^quickwit_indexing_published_split_docs_total/ {s+=$NF} /^quickwit_ingest_shards\{.*state="open"/ {o+=$NF}
            END {printf "%d %d %d", i, p, s; printf " open_shards=%d", o > "/dev/stderr"}' 2>"$1.shards.tmp")
        o=$(cat "$1.shards.tmp" 2>/dev/null)
        mem=$(docker stats --no-stream --format '{{.MemUsage}}' "$QW_CONTAINER" 2>/dev/null | awk '{print $1}')
        echo "$(date +%s) ${m:-0 0 0} ${mem:-?} $(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)"
        echo "$(date +%s) ${o:-open_shards=?}" >> "$1.shards"
    done > "$1"
}

# fixed settings (no probes): the previous load (20261105T-qw-ingestapi-1b-r2) showed the default 5 MiB/s
# shard_throughput_limit makes Quickwit scale min_shards 32 up to ~75 shards (~191k-doc first splits).
# This load raises the limit to 20 MiB/s (Quickwit's maximum) so it stays at min_shards.
SHARDS=48; COMMIT=30
# 2) full 1B load through the ingest API
QB=ingestapi-cloudprem0136-rust16-shards$SHARDS-stl20mb-commit$COMMIT-split10m-stablelog-m8gd4x-64g-s3-1b-$TAG
D=/mnt/local/cp-1b-s3-api; R=s3://<bucket>/$QW_S3_PREFIX
( api_env "$SHARDS" "$COMMIT" "$D" "$R"; export SEARCHBENCH_VERSION=$QB; [[ ! -e "$D" ]]
  sampler "$ROOT/load.samples" & smp=$!   # api_env set QW_CONTAINER
  ingest quickwit-api-ingest quickwit searchbench-qw-1b-api; rc=$?; kill "$smp" 2>/dev/null || true; exit $rc ) || exit 1
touch "$D/.searchbench-loaded"
push

# 3) three rounds, qwapi and qw82, order rotated
declare -A IROOT=([qwapi]=$R [qw82]=s3://<bucket>/searchbench/m8gd4x-1b-quickwit-s3-20261102T-qw-71c9181-10m-s3)
declare -A IQB=([qwapi]=$QB [qw82]=parquet-bulk-71c9181-pipelines16-split10m-noTT-m8gd4x-64g-s3-1b-20261102T-qw-71c9181-10m-s3)
declare -A ILOAD=([qwapi]="$ROOT/quickwit-api-ingest-load" [qw82]="$ROOT/load-qw82")
mkdir -p "$ROOT/load-qw82"; cp "$ROOT/loads/otel_logs_1b.${IQB[qw82]}.json" "$ROOT/load-qw82/"
col() {   # $1 column, $2 round
    local c=$1 r=$2
    ( export QW_IMAGE QW_PULL_IMAGE=no QW_STORAGE=s3 QW_CONTAINER=searchbench-qw-1b-s3-$c QW_DATA_DIR=/mnt/local/quickwit-1b-s3-q-$c
      export QW_ENABLE_IN_MEMORY_INDEXING=true QW_S3_ROOT=${IROOT[$c]}
      mkdir -p "$QW_DATA_DIR"; touch "$QW_DATA_DIR/.searchbench-loaded"   # query-only: searcher + metastore
      export SEARCHBENCH_QW_METRICS_LOG="$ROOT/r$r-$c-metrics.jsonl"
      queries r$r-$c quickwit "$QW_CONTAINER" queries.dsl "s3-$c-r$r" "${IQB[$c]}" "${ILOAD[$c]}"
      grep -q '^  fast_field_cache_capacity: 0$' "$RUN/source/quickwit/config/quickwit_s3.yaml" )
}
for r in 1 2 3; do
    if (( r == 2 )); then seq=(qw82 qwapi); else seq=(qwapi qw82); fi
    for c in "${seq[@]}"; do
        col "$c" "$r" || { echo "column $c round $r failed" >&2; echo "r$r $c" >> "$ROOT/failed-trials.txt"; }
    done
done
push
