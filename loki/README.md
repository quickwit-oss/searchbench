# Loki adapter

Grafana Loki **3.7.9** (`grafana/loki:3.7.9`), single binary, replication
factor 1, TSDB index (schema v13), in Docker. Storage is local disk or S3. The
native binary is used only by the integration tests. No Grafana, Kubernetes
or paid service is needed.

## Published results

| Columns | Scale | Storage | Run |
|---|---|---|---|
| `Loki (3.7.9-m8gd4-100k-read-snappy)`, `Loki (3.7.9-m8gd4-100k-s3-gets)` | 100K | local NVMe, S3 | [`runs/100k-20261101`](runs/100k-20261101/README.md) |
| `Loki (3.7.9-m8gd4-1b-local-frame-prefilter-{off,on}-c8-20261115)`, `Loki (3.7.9-m8gd4-1b-s3-gets)` | 1B | local NVMe, S3 | [`runs/1b-20261115`](runs/1b-20261115/README.md) |
| `Loki (3.7.9-m8gd4-1b-s3-frame-prefilter-on-c16-600s-20261119)` | 1B (600 s cap) | S3 | [`runs/hippo-20261105/extra/loki-s3-600s-hot-1b-20261119`](../runs/hippo-20261105/extra/loki-s3-600s-hot-1b-20261119/README.md) |

All ran on AWS `m8gd.4xlarge` (16 Graviton4 vCPUs, 64 GiB, local NVMe), not on
the repository's reference machine. At 100K all 56 supported queries finish. At
1B, 50 of them time out at 60 s; the run README lists the six that finish.

The 100K columns used an earlier revision of this adapter: raw `Body` line,
no literal prefilter, querier concurrency 8. The 1B columns use the encoding
and query translation described below. Each run README records the source
commit of every column.

## Run

Rust is the default ingestion client. `install` builds it before load timing
starts, with local Cargo if available and otherwise in a Rust Docker image.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
export SEARCHBENCH_PYTHON="$PWD/.venv/bin/python"
export SEARCHBENCH_DATA_DIR=/mnt/data          # the driver appends the dataset
export SEARCHBENCH_DATASET=otel_logs_1m
export LOKI_DATA_DIR=/mnt/data/loki-1m-local
export SEARCHBENCH_CPU=on
./benchmark.sh --index --version 3.7.9-local
```

After a successful load, rerun the queries without `--index`, with the same
`LOKI_DATA_DIR`, storage settings and image.

- `$LOKI_DATA_DIR/manifest.json` records the source row count, exact time
  bounds, load timings, the persisted object inventory and the storage-only row
  count.
- `$LOKI_DATA_DIR/runtime.json` records the service target, image, querier
  concurrency and literal-prefilter setting of the running server.

**Each load needs a fresh destination.** The adapter never deletes local data or
S3 objects. A failed or partial load can't be reused: pick a new directory or
prefix. `SEARCHBENCH_SKIP_LOAD=yes` requires a completed manifest. `install`
does not wipe an index.

### S3

```bash
export LOKI_STORAGE=s3
export LOKI_S3_BUCKET=<bucket>
export LOKI_S3_PREFIX=searchbench/loki/otel-1m-run1/
export LOKI_DATA_DIR=/mnt/data/loki-1m-s3-run1
export AWS_REGION=us-east-1
./benchmark.sh --index --version 3.7.9-s3
```

- The bucket must exist. The prefix must match `searchbench/loki/<run>/`, be
  empty and be used by this load only. The adapter checks it at setup and again
  at load time, then reserves it with a conditional PUT of
  `_searchbench_load_lock.json`. That object stays, even after a failure.
- Credentials come from the EC2 instance role or the standard AWS environment
  variables; the host SDK and the container both need them. Nothing is
  written into config files. The container uses host networking for instance
  metadata; HTTP and gRPC listen on loopback only.
- `storage_config.object_prefix` scopes Loki's objects to the prefix.

`./data-size` counts persisted chunks and shipped indexes, not the local WAL or
caches. On S3 it also counts the small reservation object. `./storage-units`
reports the chunk and index count (files on disk, objects on S3).

## Data mapping

| Source | Loki |
|---|---|
| `Timestamp` | entry timestamp, nanoseconds, unchanged |
| `Body` | the exact Body, inside a versioned JSON frame (below) |
| `ServiceName`, `SeverityText` | stream labels `service_name`, `severity_text` |
| constant | stream label `dataset="otel_logs"` |
| other fields | structured metadata; nested values as JSON strings |
| source file name and row offset | frame `id`, plus the `sb_row_id` metadata field |

`TimestampTime` is left out (it duplicates `Timestamp`). TraceId and SpanId are
metadata, not labels.

**Frame encoding.** The stored line is compact UTF-8 JSON with a fixed field
order: `{"v":1,"id":"part_000.parquet:123","body":"<exact Body>"}`. Python and
Rust produce the same bytes (`encoding.py`, `client/src/encoding.rs`). Every
source row gets a distinct id, so identical rows stay distinct.

The raw Body alone is not enough. Loki deduplicates samples across chunks by
line hash and ignores metadata. In `tests/test_raw_body_gate.py`, nine identical
rows with distinct `sb_row_id`, flushed to separate chunks, count as **1**
after a restart. With frames they count as 9 in every layout. The adapter
rejects any encoding other than frame v1.

Each load writes a receipt (`load-started.json`) with the encoding descriptor,
a new destination id and the source file inventory (name, size, mtime). The
manifest carries the same fields. Queries, restarts and layout checks refuse a
destination whose receipt is missing, different or from an older encoding.

### Rust ingestion client

`client/` reads Parquet with Arrow and pushes Loki protobuf `PushRequest`s,
raw-Snappy compressed:

1. Check the whole input first: Body sizes against Loki's 1 MiB line limit, and
   a source time span under one hour (Loki's out-of-order window with
   `max_chunk_age=2h`). The benchmark corpus spans about 29 minutes.
2. Hand row groups to parallel readers (several per file if needed).
3. Encode the frame and metadata from typed Arrow columns.
4. Split batches under a 3 MiB uncompressed request limit (Loki's internal gRPC
   limit is 4 MiB). Rows are never truncated.
5. Send through a bounded queue to persistent HTTP senders.

Defaults: 8 readers, 8 senders, queue depth 8. **Nothing is retried** (429, 5xx,
timeouts): a partly accepted push must not be replayed. Any failure marks the
load as partial and unpublishable. If 429s occur, raise the ingestion limits
and load into a fresh destination.

`LOKI_LOAD_CLIENT=python` uses the Python JSON loader instead. It is the
reference implementation in the tests and uses one process per source file.

```bash
./client/build.sh                    # auto: local Cargo, otherwise Docker
LOKI_CLIENT_BUILD=docker ./client/build.sh
```

Builds use the committed `Cargo.lock` with `--locked`.

## Load completion

A load is complete only after:

1. `/flush` and a graceful shutdown that flushes the WAL;
2. a restart as `read,query-scheduler,compactor` (no ingester, so no WAL replay);
3. at least two compaction passes, then 30 s with an unchanged persisted object
   inventory;
4. a storage-only row count equal to the source row count.

Only then is the manifest written. It fingerprints the persisted inventory.
`python adapter.py verify-layout` checks it after a query run.

## Queries

`gen_queries.py` reads `elastic/queries.dsl` and writes `queries.logql`: one
JSON request spec per query, with the original 92 tags. `./query` builds the
LogQL (`logql.py`), calls the HTTP API and reports the client HTTP round-trip
time, the row count and Loki's own stats.

`logql.py` is named `queries.py` in the source commits of the published runs.
It was renamed here because `frontend/build_results` reads every
`<engine>/queries.*` file as query text, so `queries.logql` must be the only
one. The code is unchanged.

**56 queries are supported:**

- 27 counts: terms, AND/OR, phrases, regex, prefix/wildcard, negation, windows.
- 13 group-bys, including UTC minute histograms.
- All 16 recent/limited log queries.

**36 are `UNSUPPORTED`** and print no timing: Q09 (minimum-should-match), Q13
(proximity), Q22–Q24 and Q59 (fuzzy), Q33–Q53 (BM25 relevance ranking) and
Q84–Q92 (trace joins). They are not emulated client side.

Translation details:

- **Terms.** `|= "error"` is a substring match, not a term match, so terms
  become boundary-aware RE2 expressions. The delimiters follow the reference
  analyzer: punctuation, symbols and Java whitespace.
- **Lowercasing** happens in the query pipeline (`| json` of the frame, then
  Loki's `lower`), not at ingestion. A `(?i)` regex is not equivalent: RE2 folds
  `ſ` to `s` and does not fold `İ` to `i`. Both cases are tested.
- **Literal prefilter** (`LOKI_LITERAL_PREFILTER`, default `on`). For a
  positive Body filter made of one plain literal, the adapter adds
  `|~ "(?i)<literal>"` on the raw frame, before `| json` and `lower`. The
  literal is the longest run of `[0-9a-hj-z]` (at least 3 characters) that
  every match must contain. It is a superset check: the exact regex still
  decides every answer. `i` and a leading `k` are excluded because Go's
  lowercasing maps `İ` and `K` (Kelvin) to them. Native tests compare answers
  with it on and off for all 56 queries plus adversarial Unicode bodies. It
  saves CPU, not I/O: lines and bytes processed are the same. `off` gives
  exactly the requests of the translator before the prefilter was added
  (`tests/fixtures/requests_prefilter_off.json`). Use the same value for
  `./start` and `./query`, and name an `off` run with `--version`.
- **Cardinality.** Structured metadata is dropped with `keep` before metric
  aggregation, except the grouping keys.
- **Time windows.** Metric range vectors select `(start, end]`; the reference
  bounds are inclusive. The adapter widens the range and enforces the exact
  nanosecond lower bound on the entry timestamp. It skips that guard only
  when the window starts at or before the corpus minimum recorded in a
  completed manifest. Log-range requests use `end = inclusive_end + 1ns`.
- **Minute histograms** group by `(__timestamp__).UTC.Format`, not by query
  steps, which can shift bucket boundaries.
- **Q67** returns the global top 20 severity/scope pairs, like the SQL
  reference and the DuckDB adapter.
- Output is JSON lines, so multiline bodies don't break row counts.

`./query` prints `LOKI_QUERY_SETTINGS={line_encoding, literal_prefilter,
query_concurrency}` on stderr. `lib/benchmark.sh` doesn't keep it, so
`runtime.json` is the record of a run's settings.

## Cold runs and caches

- `LOKI_TARGET=auto`: `all` during ingestion, then `read,query-scheduler` for
  every query restart. The scheduler runs in-process with loopback addresses;
  the `read` target alone expects an external scheduler. Query processes don't
  start an ingester, so restarts don't replay the WAL or upload index files.
- `querier.query_store_only=true`: answers come from storage only. Native tests
  check that no ingester is reached and that pushes to a read process are
  rejected.
- Result, instant-result and embedded chunk caches are off. Index metadata
  caches are allowed, as in the other adapters. Bloom filters are not
  available in single-binary mode.
- Loki's own `query_timeout` is 600 s, above the driver's 60 s cap.

## Settings

| Variable | Default | Purpose |
|---|---|---|
| `LOKI_IMAGE` | `grafana/loki:3.7.9` | Image; record the digest for published runs |
| `LOKI_CONTAINER` | `searchbench-loki` | Container name |
| `LOKI_DATA_DIR` | `./loki-data` | Local state, config and manifest |
| `LOKI_STORAGE` | `filesystem` | `filesystem` or `s3` |
| `LOKI_S3_BUCKET`, `LOKI_S3_PREFIX` | | S3 destination (see above) |
| `LOKI_TARGET` | `auto` | `auto`, `all` or `read` |
| `LOKI_PORT` | `3100` | Loopback HTTP port (gRPC uses 9096) |
| `LOKI_LOAD_CLIENT` | `rust` | `rust` or `python` |
| `LOKI_CLIENT_BIN` | `client/target/release/loki-parquet-ingest` | Prebuilt client |
| `LOKI_CLIENT_BUILD` | `auto` | `auto`, `native` or `docker` |
| `LOKI_LOAD_WORKERS` | `8` | Rust row-group readers; Python file workers |
| `LOKI_HTTP_WORKERS` | `8` | Rust HTTP senders |
| `LOKI_BATCH_ROWS` | `5000` | Arrow batch size |
| `LOKI_QUEUE_DEPTH` | `8` | Rust request queue |
| `LOKI_MAX_REQUEST_BYTES` | `3145728` | Uncompressed request limit, 1024 to 3145728 |
| `LOKI_INGESTION_RATE_MB` | `128` | Tenant ingestion rate, MB/s |
| `LOKI_QUERY_CONCURRENCY` | `16` | Querier `max_concurrent` (one per vCPU of the test host) |
| `LOKI_LITERAL_PREFILTER` | `on` | `on` or `off`; any other value fails |
| `LOKI_SPLIT_QUERIES_INTERVAL` | `15m` | Log-range split interval; `0s` disables |
| `LOKI_SPLIT_METRIC_QUERIES_INTERVAL` | `1h` | Instant-metric split interval; `0s` disables |
| `LOKI_TSDB_MAX_QUERY_PARALLELISM` | `32` | TSDB query parallelism |
| `LOKI_TSDB_MAX_BYTES_PER_SHARD` | `600MB` | Target bytes per TSDB shard |
| `LOKI_CHUNK_ENCODING` | `snappy` | Chunk compression |
| `LOKI_CHUNK_TARGET_SIZE` | `1572864` | Target compressed chunk size |
| `LOKI_SETTLE_TIMEOUT` | `1800` | Load settle deadline, seconds |
| `LOKI_SETTLE_QUIET_SECS` | `30` | Unchanged-inventory interval, seconds |

## Tests

```bash
.venv/bin/pip install -r requirements-test.txt
./client/build.sh
(cd client && cargo test --locked)
# Against a downloaded Loki 3.7.9 binary:
LOKI_TEST_BINARY=/absolute/path/to/loki \
LOKI_TEST_CLIENT="$PWD/client/target/release/loki-parquet-ingest" \
  .venv/bin/python -m unittest discover -s tests -v
```

Tests that need the Loki binary or the Rust client skip when these variables
are unset. The native tests start temporary loopback servers. They run every
supported query against an independent fixture oracle (`tests/oracle.py`),
after both Python and Rust ingestion, and again after a read-only restart.
They also cover forced cross-chunk duplicates, nanosecond windows, UTC buckets
and multiline bodies. A separate Python protobuf/Snappy decoder checks the Rust
wire format. S3 prefix ownership is tested with mocks. Set
`LOKI_TEST_ARTIFACTS` to a directory to keep server configs and logs.

## Prefilter pair driver

`experiments/query_pair.py` compares `LOKI_LITERAL_PREFILTER=off` and `on` on an
existing completed destination. It produced the two local 1B columns. It never
loads, and refuses an incomplete manifest or a changed layout.

For each query, both variants run back to back, with cold / hot / hot tries.
Each variant starts with `lib/benchmark.sh`'s cold cycle: stop, `sync` +
`drop_caches`, start. The cap is `SEARCHBENCH_QUERY_TIMEOUT` (60 s) or
`--cap-seconds`. A try that hits the cap is recorded as censored, with no time.
The variant that goes first alternates by query; `--reverse` swaps the order.
Answers of the two variants must be identical, or the run fails.

```bash
LOKI_DATA_DIR=/mnt/data/loki-1b LOKI_IMAGE=grafana/loki@sha256:<digest> \
  python3 loki/experiments/query_pair.py --out /path/to/new/dir
```
