#!/usr/bin/env bash
# 1B on S3, one m8gd.4xlarge, the two S3 columns retained for the blog, query-only, interleaved (median of 3):
#   ch13  ClickHouse 26.10.2.103, 1.3 GiB part cap, 84 parts: the table loaded by 20261103T-s3-final-1b-ch13,
#         reattached from its saved local part metadata (no reload)
#   qw82  Quickwit #6843 head 71c9181, 10M-doc splits, TimestampTime excluded, 82 splits (existing S3 index)
# ClickHouse: #17 queries, header cache on, text-index data caches + statistics off, max_threads=32.
# Quickwit: data caches off. Every column: all engines stopped first, restart + page-cache drop before every
# query, 3 tries, 60 s cap. 3 rounds, order rotated. Results: s3://<bucket>/searchbench/runs-raw/$TAG/.
set -Eeuo pipefail
ROOT=/opt/sb-s3-pair
TAG=20261103T-s3-pair-1b
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
    shutdown -h +60 's3 pair runs over'
    exit "$rc"
}
trap finish EXIT
shutdown -h +300 'SearchBench s3 pair safety limit'
CH_IMAGE=clickhouse/clickhouse-server@sha256:ec7bc702d862b9f7af6c604f0fa8f9688d62d10db44f0cd340bcdceaa25db2a8
CORPUS=/mnt/local/corpus/otel_logs_1b/part_000.parquet
CH_S3_PREFIX=searchbench/m8gd4x-1b-clickhouse-s3-20261103T-s3-final-1b-ch13
QW_SHA=71c9181d4a9c9517f40107b097b031fbdddf18fc
QW_IMAGE=searchbench-quickwit:parquet-bulk-71c9181
stage bootstrap
[[ $(uname -m) == aarch64 && $(nproc) == 16 ]]
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq docker.io docker-buildx git curl jq python3-venv python3-pip nvme-cli rsync sysstat time unzip >/dev/null
command -v aws >/dev/null || { curl -sS -o /tmp/awscli.zip "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip"; unzip -q -o /tmp/awscli.zip -d /tmp; /tmp/aws/install >/dev/null; }
systemctl stop docker.service docker.socket containerd || true
disk=$(lsblk -J -d -o PATH,MODEL | python3 -c 'import json,sys; ds=[d["path"] for d in json.load(sys.stdin)["blockdevices"] if "Instance Storage" in (d.get("model") or "")]; assert len(ds)==1,ds; print(ds[0])')
[[ -z $(lsblk -n -o MOUNTPOINTS "$disk" | tr -d '[:space:]') ]]
mkfs.ext4 -q -F "$disk"; mkdir -p /mnt/local; mount -o noatime "$disk" /mnt/local
mkdir -p /mnt/local/{docker,containerd,corpus/otel_logs_1b,build}
printf '{"data-root":"/mnt/local/docker","cgroup-parent":"searchbench.slice","log-driver":"json-file","log-opts":{"max-size":"100m","max-file":"3"}}\n' > /etc/docker/daemon.json
mkdir -p /etc/systemd/system/docker.service.d /etc/systemd/system/containerd.service.d
printf '[Unit]\nRequiresMountsFor=/mnt/local/docker\n' > /etc/systemd/system/docker.service.d/searchbench-storage.conf
printf '[Unit]\nRequiresMountsFor=/mnt/local/containerd\n[Service]\nExecStart=\nExecStart=/usr/bin/containerd --root /mnt/local/containerd\n' > /etc/systemd/system/containerd.service.d/searchbench-storage.conf
printf '[Unit]\nDescription=SearchBench container accounting\n[Slice]\nCPUAccounting=yes\nMemoryAccounting=yes\nIOAccounting=yes\n' > /etc/systemd/system/searchbench.slice
systemctl daemon-reload; systemctl start searchbench.slice containerd docker
[[ $(docker info --format '{{.DockerRootDir}}') == /mnt/local/docker ]]
[[ -r /sys/fs/cgroup/searchbench.slice/cpu.stat ]]
docker buildx version > "$ROOT/buildx-version.txt"
aws sts get-caller-identity --query Arn --output text | grep -q 'assumed-role/<benchmark-role>/'
[[ -n "$(aws s3 ls "s3://<bucket>/$CH_S3_PREFIX-ch13/" 2>/dev/null)" && -z "$(aws s3 ls "$RAW/" 2>/dev/null)" ]]   # the retained table exists; fresh results prefix
python3 -m venv /opt/searchbench-venv
/opt/searchbench-venv/bin/pip install -q certifi==2026.10.23 charset-normalizer==3.5.2 idna==3.20 orjson==3.13.0 pyarrow==26.0.0 requests==2.34.2 urllib3==2.8.0
lscpu > "$ROOT/lscpu.txt"; free -b > "$ROOT/memory.txt"; lsblk -o NAME,SIZE,MODEL,FSTYPE,MOUNTPOINTS > "$ROOT/disks.txt"

stage corpus-and-images
docker pull -q "$CH_IMAGE" > "$ROOT/ch-image-pull.log" 2>&1
tar -xzf "$ROOT/loads.tar.gz" -C "$ROOT"
( cd /mnt/local/build && rm -rf quickwit && mkdir quickwit && cd quickwit && git init -q . \
  && git remote add origin https://github.com/quickwit-oss/quickwit.git && git fetch -q --depth 1 origin "$QW_SHA" \
  && git checkout -q --detach FETCH_HEAD && [[ "$(git rev-parse HEAD)" == "$QW_SHA" ]] \
  && python3 -c "
from pathlib import Path
p=Path('Dockerfile');s=p.read_text();n='ARG CARGO_FEATURES=release-feature-set';assert s.count(n)==1
p.write_text(s.replace(n,'ENV CARGO_BUILD_JOBS=12\n\nARG CARGO_FEATURES=release-feature-set,parquet'))" \
  && docker build --progress=plain --build-arg QW_COMMIT_HASH="$QW_SHA" --build-arg QW_COMMIT_DATE="$(git show -s --format=%cI HEAD)" \
       --build-arg QW_COMMIT_TAGS=parquet-local-ingest --label org.opencontainers.image.revision="$QW_SHA" \
       -t "$QW_IMAGE" . ) > "$ROOT/quickwit-build.log" 2>&1 & qwb=$!
aws s3 cp --only-show-errors s3://<bucket>/searchbench/data/otel_logs_1b/part_000.parquet "$CORPUS"
/opt/searchbench-venv/bin/python3 - <<'PY'
import hashlib,json,os,pathlib,pyarrow.parquet as pq
p=pathlib.Path('/mnt/local/corpus/otel_logs_1b/part_000.parquet');md=pq.ParquetFile(p).metadata;h=hashlib.sha256()
with p.open('rb') as f:
    while c:=f.read(16<<20): h.update(c)
got={'rows':md.num_rows,'row_groups':md.num_row_groups,'file_bytes':p.stat().st_size,'sha256':h.hexdigest()}
assert got=={'rows':1000000000,'row_groups':1624,'file_bytes':40991166725,'sha256':'7489ed3eb8b2a012ade92d11f7bd785f5cc322393c4e0414dce843fec9fa06fc'}, got
(pathlib.Path(os.environ['ROOT'])/'corpus.json').write_text(json.dumps(got,indent=2)+'\n')
PY
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
ch_env() {   # $1 ch13 (S3)
    export CH_BACKEND=docker CH_IMAGE CH_CONTAINER=searchbench-ch-1b-$1 CH_DATA_DIR=/mnt/local/clickhouse-1b-$1 CH_STORAGE=s3
    export CH_ORDER_BY="(ServiceName, Timestamp)" CH_MAX_PART_BYTES=1395864371 CH_PHRASE_INDEX=on   # 1.3 GiB
    export CH_S3_ENDPOINT=https://<bucket>.s3.us-east-1.amazonaws.com/$CH_S3_PREFIX-$1/
    return 0
}

# 1) reattach the ClickHouse table from its saved local part metadata
stage restore-ch13
aws s3 cp --only-show-errors s3://<bucket>/$CH_S3_PREFIX-ch13-localstate/clickhouse-1b-ch13-localstate.tar.gz /mnt/local/
[[ ! -e /mnt/local/clickhouse-1b-ch13 ]]
tar -C /mnt/local -xzf /mnt/local/clickhouse-1b-ch13-localstate.tar.gz; rm /mnt/local/clickhouse-1b-ch13-localstate.tar.gz
mkdir -p "$ROOT/loads-ch13"; cp "$ROOT"/loads/otel_logs_1b.26.10.2.103-*ch13*.json "$ROOT/loads-ch13/"
( c=ch13; ch_env $c; prepare restore-check-ch13; cd "$REPO/clickhouse"; export CLICKHOUSE_BIN="$PWD/ch-exec"; stopall
  ./start > "$RUN/start.log" 2>&1; for i in $(seq 1 120); do ./check >/dev/null 2>&1 && break; sleep 1; done
  docker exec -i "$CH_CONTAINER" clickhouse client -q "SELECT count(), sum(rows), sum(bytes_on_disk), max(bytes_on_disk), min(bytes_on_disk) FROM system.parts WHERE active AND table='otel_logs'" > "$ROOT/clickhouse-ch13-parts.txt"
  docker exec -i "$CH_CONTAINER" clickhouse client -q "SELECT count() FROM otel_logs" > "$ROOT/clickhouse-ch13-count.txt"
  docker logs --timestamps "$CH_CONTAINER" > "$RUN/server.log" 2>&1 || true
  docker stop -t 30 "$CH_CONTAINER" >/dev/null ) || exit 1
read -r np nrows _ < "$ROOT/clickhouse-ch13-parts.txt"
[[ $np == 84 && $nrows == 1000000000 && "$(cat "$ROOT/clickhouse-ch13-count.txt")" == 1000000000 ]]
push
stage waiting-quickwit-build
wait "$qwb"
docker run --rm "$QW_IMAGE" --version > "$ROOT/quickwit-version.txt"
[[ "$(docker image inspect -f '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$QW_IMAGE")" == "$QW_SHA" ]]

# 2) three rounds, ch13 and qw82, order rotated
declare -A ITAG=([qw82]=20261102T-qw-71c9181-10m-s3 [qw76]=20261031T153657Z)
declare -A IQB=([qw82]=parquet-bulk-71c9181-pipelines16-split10m-noTT-m8gd4x-64g-s3-1b-20261102T-qw-71c9181-10m-s3
                [qw76]=parquet-bulk-febe485c-pipelines16-m8gd4x-64g-s3-1b-20261031T153657Z)
declare -A ISPL=([qw82]=82 [qw76]=76)
col() {   # $1 column, $2 round
    local c=$1 r=$2
    case $c in
      ch13) local CB=26.10.2.103-m8gd.4xlarge-s3-sorted-$c-1b-20261103T-s3-final-1b-ch13   # the build of the retained load
        ( ch_env $c; export CH_QUERY_SETTINGS="--max_threads=32"
          queries r$r-$c clickhouse searchbench-ch-1b-$c queries-pr17.sql "s3-$c-threads32-r$r" "$CB" "$ROOT/loads-ch13" ) ;;
      qw82|qw76) local QB=${IQB[$c]}
        mkdir -p "$ROOT/load-$c"; cp "$ROOT/loads/otel_logs_1b.$QB.json" "$ROOT/load-$c/"
        ( export QW_IMAGE QW_PULL_IMAGE=no QW_STORAGE=s3 QW_CONTAINER=searchbench-qw-1b-s3-$c QW_DATA_DIR=/mnt/local/quickwit-1b-s3-$c
          export QW_ENABLE_IN_MEMORY_INDEXING=true QW_S3_ROOT=s3://<bucket>/searchbench/m8gd4x-1b-quickwit-s3-${ITAG[$c]}
          mkdir -p "$QW_DATA_DIR"; touch "$QW_DATA_DIR/.searchbench-loaded"   # query-only: searcher + metastore
          export SEARCHBENCH_QW_METRICS_LOG="$ROOT/r$r-$c-metrics.jsonl"
          queries r$r-$c quickwit "$QW_CONTAINER" queries.dsl "s3-$c-r$r" "$QB" "$ROOT/load-$c"
          grep -q '^  fast_field_cache_capacity: 0$' "$RUN/source/quickwit/config/quickwit_s3.yaml"
          python3 - "$RUN" "${ISPL[$c]}" <<'PY'
import json,glob,sys
d=json.load(open([f for f in glob.glob(sys.argv[1]+'/results/*_otel_logs_1b.json')][0]))
assert d['storage_units']=={'count':int(sys.argv[2]),'unit':'splits'},d['storage_units']
PY
        ) ;;
    esac
}
for r in 1 2 3; do
    if (( r == 2 )); then seq=(qw82 ch13); else seq=(ch13 qw82); fi
    for c in "${seq[@]}"; do
        col "$c" "$r" || { echo "column $c round $r failed" >&2; echo "r$r $c" >> "$ROOT/failed-trials.txt"; }
    done
done

# 3) ClickHouse per-try ProfileEvents (S3 GETs/bytes)
c=ch13; ( ch_env $c; cd "$ROOT/r1-$c/source/clickhouse"; export CLICKHOUSE_BIN="$PWD/ch-exec"; stopall
    ./start >/dev/null 2>&1; for i in $(seq 1 60); do ./check >/dev/null 2>&1 && break; sleep 1; done
    docker exec -i "$CH_CONTAINER" clickhouse client --query "SYSTEM FLUSH LOGS"
    docker exec -i "$CH_CONTAINER" clickhouse client --query "
        SELECT event_time_microseconds t, query_duration_ms/1000 s, type, read_rows, read_bytes, peak_threads_usage,
               ProfileEvents['S3GetObject'] gets, ProfileEvents['ReadBufferFromS3Bytes'] bytes,
               Settings['max_threads'] max_threads, Settings['use_statistics'] use_statistics,
               toJSONString(ProfileEvents) events, replaceAll(query, '\n', ' ') q
        FROM system.query_log WHERE type IN ('QueryFinish','ExceptionWhileProcessing') AND is_initial_query
          AND event_time >= toDateTime('$(cat "$ROOT/r1-$c/query-window-start.txt")')
        ORDER BY event_time_microseconds FORMAT TSVWithNames" | gzip > "$ROOT/clickhouse-$c-querylog.tsv.gz"
    docker stop -t 30 "$CH_CONTAINER" >/dev/null ) || true
push
