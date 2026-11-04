# DuckDB: direct Parquet scan baseline

DuckDB **2.0.0** (the run set's 2026-10-05/06 columns used 1.4.4), reading the original dataset files through a view. No ingestion,
materialized table, token column, search index, or FTS extension. Local and S3
runs are separate benchmark variants: `DuckDB-Parquet (local-no-index)` and
`DuckDB-Parquet (s3-no-index)`.

How the published configuration was chosen, with measurements and trade-offs:
[`TUNING.md`](TUNING.md).

DuckDB 2.0 needed three changes from the 1.4.4 version: `requirements.in` pins 2.0.0 (`requirements.txt` is compiled from it with hashes by `uv pip compile`, see its header; `install` uses `--require-hashes`, and `.github/workflows/lockfiles.yml` fails a PR whose `.txt` is stale);
lambdas use the new `lambda x: ...` syntax (2.0 rejects `x -> ...`); and the per-minute group-bys
(Q61-Q65) compute the minute from `epoch_ns(Timestamp)`, because 2.0 reads the corpus's
`timestamp[ns, tz=UTC]` as `TIMESTAMPTZ_NS`, which `date_trunc` does not accept (truncating, like
the other engines, rather than casting, which would round).

## Run against local files

Requirements: Python 3.10+, Bash 4+, `jq`, and the shared runner's Linux/GNU tools
for full cold-cache benchmarks. Installation uses an adapter-local `.venv`.

```bash
cd duckdb-parquet
SEARCHBENCH_DATA_DIR=/path/to/data \
  SEARCHBENCH_DATASET=otel_logs_1m \
  DUCKDB_THREADS=8 DUCKDB_MEMORY_LIMIT=8GB \
  ./benchmark.sh --index
```

The shared downloader creates `/path/to/data/otel_logs_1m/part_*.parquet`.
Here `--index` means install dependencies and prepare the view, **not build an
index**. Omit `--index` to run again with the same settings and existing files.

To use files already present elsewhere, set an explicit file/glob instead:

```bash
DUCKDB_PARQUET_PATH='/mnt/corpus/part_*.parquet' \
  SEARCHBENCH_DATASET=otel_logs_1b \
  DUCKDB_THREADS=8 DUCKDB_MEMORY_LIMIT=8GB \
  ./benchmark.sh --index
```

An explicit path disables the shared download step. Always set
`SEARCHBENCH_DATASET` to match the files: it labels the results; it does **not**
slice, limit, or validate an explicit local/S3 path. Quote globs so DuckDB, not
the shell, expands them.

## Run against S3

```bash
DUCKDB_PARQUET_PATH='s3://my-bucket/dataset/part_*.parquet' \
  AWS_REGION=eu-west-3 \
  SEARCHBENCH_DATASET=otel_logs_1b \
  DUCKDB_THREADS=8 DUCKDB_MEMORY_LIMIT=8GB \
  ./benchmark.sh --index
```

The `httpfs` and `aws` extensions are installed during `./install`, outside query
timing. By default an in-memory S3 secret uses DuckDB's AWS credential chain
(environment, shared AWS configuration, instance/container role, etc.). No
credentials are stored in the adapter or result files. The region comes from
`AWS_REGION`, then `AWS_DEFAULT_REGION`, then `us-east-1`.

For a public bucket, add `DUCKDB_S3_AUTH=unsigned` to bypass credential discovery.
Globs need bucket-list permission; explicit object paths do not need listing.
Object reads need read permission. S3 uses range requests for metadata and
column chunks, not a preliminary download to a local Parquet copy. Broad text
searches may nevertheless read most of the text column. Request and transfer
charges still apply.

When switching paths, credentials, or resource settings, stop the old worker
first with `./stop`. Re-run `./install` when switching a local-only installation
to S3 without `--index`.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `DUCKDB_PARQUET_PATH` | Driver-resolved `$SEARCHBENCH_DATA_DIR/part_*.parquet` | Explicit local file/glob or `s3://` file/glob |
| `DUCKDB_THREADS` | Logical CPU count | DuckDB execution threads; set explicitly for comparisons |
| `DUCKDB_MEMORY_LIMIT` | `4GB` | DuckDB buffer-manager memory limit, not a hard process RSS cap |
| `DUCKDB_S3_AUTH` | `credential_chain` | `credential_chain` or `unsigned` |
| `DUCKDB_S3_ENDPOINT` | AWS S3 | Optional S3-compatible host[:port], without scheme |
| `DUCKDB_S3_URL_STYLE` | `vhost` | `vhost` or `path` (often needed for compatible stores) |
| `DUCKDB_S3_USE_SSL` | `true` | `true` or `false`; disable only for trusted local fixtures |
| `DUCKDB_EXTERNAL_FILE_CACHE` | `false` | DuckDB's in-memory cache of remote (S3) file data, `enable_external_file_cache`. Off so every try reads from S3, like the other engines' S3 runs |
| `DUCKDB_HTTP_STATS_LOG` | unset | Opt-in: append one JSON line per try with the HTTP GET requests and response bytes it caused (DuckDB's in-memory HTTP log, cleared after each query; includes the metadata read of a cold try). Only counts are written, never request headers |
| `DUCKDB_METADATA_CACHE` | `true` | Keep Parquet footers and HTTP object metadata in memory between queries (`parquet_metadata_cache`, `enable_http_metadata_cache`), as the other engines keep their metadata; no data is cached |
| `DUCKDB_SETTINGS` | (none) | More DuckDB settings for the session, `name=value;name=value` (e.g. `preserve_insertion_order=false`); shown by `./version` and so in the results' version label |
| `DUCKDB_SORT_FROM` | unset | Sorted variant: `./load` writes `DUCKDB_PARQUET_PATH` (one file, local or `s3://`) as these files sorted by `DUCKDB_SORT_BY`; the sort counts in `load_time` |
| `DUCKDB_SORT_BY` | `ServiceName, Timestamp` | Sort key of the sorted copy, the ClickHouse and DEN table order |
| `DUCKDB_SORT_ROW_GROUP_SIZE` | `122880` | Rows per row group of the sorted copy (DuckDB's default); with the sort, `ServiceName`/`Timestamp` row-group statistics let DuckDB skip row groups |
| `DUCKDB_STOP_TIMEOUT` | `30` | `./stop` returns only once the worker process has exited; after this many seconds it kills it (a query interrupted at the runner's cap can take a while to unwind, and the next worker cannot start until the old one is gone) |
| `DUCKDB_PYTHON` | `.venv/bin/python` | Optional pre-provisioned Python with the pinned DuckDB dependency |
| `DUCKDB_STATE_DIR` | Checkout-specific private directory under `/tmp` | Worker socket/log and temporary spill files; keep path short for Unix sockets |

`./version` records DuckDB's version, thread count, and configured memory limit.
Defaults use separate result and load-record names for local and S3 runs. Give
additional configurations distinct labels, for example
`--version local-8t-8GB` or `--version s3-eu-west-3-8t-8GB`. Explicit labels replace
the default, so include the storage mode yourself. Standard shared-driver
settings (`SEARCHBENCH_TRIES`, timeouts, restart policy, etc.) still apply.

For direct adapter calls, `SEARCHBENCH_DATA_DIR` is the **resolved dataset
directory**, not the root used by `benchmark.sh`. An explicit
`DUCKDB_PARQUET_PATH` works identically in both cases:

```bash
export DUCKDB_PARQUET_PATH='/mnt/corpus/part_*.parquet'
./install
./start
./load
printf 'SELECT count(*) FROM otel_logs;\n' | ./query
./data-size
./stop
```

## Semantics and methodology

- All **92 query slots** retain the existing tags/order. **70 run**; Q13
  (Lucene-style proximity/slop) and Q33–Q53 (BM25 ranking) are `UNSUPPORTED`,
  producing null timings rather than substituting a different task.
- `macros.sql` defines scan-time SQL helpers. Each is the token test written as a
  regex on `Body`, without building a token list or even `lower(Body)`:
  - "t is a token of `lower(Body)`" is "t occurs with a separator or the string edge on
    each side". DuckDB's `lower()` turns exactly A–Z, U+0130 and U+212A (Kelvin sign) into
    ASCII letters (checked over every code point), so the regex matches each letter of t
    as a case class (`[eE]`, `i` as `[iIİ]`, `k` as `[kKK]`) and a separator is any other
    character. A plain case-class search for t runs first; RE2 scans for it quickly.
  - `sb_any` is an OR of term searches up to 3 terms and one alternation beyond (RE2 has no
    fast literal search for an alternation), `sb_phrase` one regex, `sb_all` a flat AND chain (rows are
    dropped at the first term that fails). Prefix, suffix, infix and per-token regexp
    queries have helpers of the same kind.
  - Fuzzy (`sb_fuzzy`): DuckDB's `damerau_levenshtein` is the unrestricted distance, so the
    tokens within 1 or 2 of t are those reachable by 1 or 2 edits. `sb_ball` lists them
    (with "any token character" for inserted or substituted letters) and `sb_ball_re`
    turns them into one regex that selects rows; tokens are then compared only on those.
  - `test_adapter.py` checks every helper against the token-list definition (edge cases,
    the `lower()` code-point property, random edits for the fuzzy regex). On the 100M
    corpus all 70 queries return the same rows as the token-list version (top-100
    queries differ only in the order of rows tied on `Timestamp`, which neither version
    fixes: the old version does not repeat it either).
  - m8gd.4xlarge, 100M, 16 threads, all 70 queries: cold sum 449.5 s (3 fuzzy queries at
    the 60 s cap) -> 99.2 s, slowest query 4.2 s.
  Terms are whole **ASCII
  alphanumeric tokens**, lowercased, without stemming or stopword removal,
  following the ClickHouse scan adapter. Phrases are contiguous token sequences;
  punctuation and whitespace are separators. Regex/prefix/wildcard and
  Damerau–Levenshtein fuzzy predicates operate on tokens, not the whole body.
  This is not a claim of exact Unicode/analyzer equivalence with every engine.
- The session timezone is pinned to **UTC** for time-window literals and minute
  grouping. The real corpus has `timestamp[ns, tz=UTC]`; fetching those values
  through DuckDB's Python API requires the pinned `pytz` dependency. DuckDB
  reads timezone-aware timestamps as **microsecond-resolution TIMESTAMPTZ**,
  so sub-microsecond distinctions are lost (including possible top-K ordering
  ties or boundary differences). Numeric epoch-nanosecond datasets instead use
  `make_timestamp_ns` and retain ns precision internally; timezone-naive
  timestamp-typed Parquet is accepted too.
- A private Unix-socket worker keeps one in-memory DuckDB connection alive
  across warm tries. The shared runner restarts it for cold queries. A client
  disconnect (including runner timeout) interrupts execution so work does not
  continue into the next query. `./stop` is idempotent.
- Reported time covers SQL execution, fetching all rows, serialization, and
  sending result batches to the client. It excludes Python startup, the IPC
  handshake. The first query after a (re)start also binds the view, i.e. reads the
  Parquet metadata, and that is **timed with it**: cold tries include opening the
  metadata, as the other engines' cold tries do. Hot tries reuse the bound view.
  Timing is measured in the worker (no client connect), where the other adapters
  report client round-trip time; the difference is a local socket round trip.
- `load_time` is view/schema setup plus the shared runner's accounting overhead,
  not ingestion. `data_size` is the sum of **full Parquet file/object bytes**,
  obtained from file metadata. It is neither extra index storage nor bytes
  transferred. Temporary query spill is not counted as stored dataset/index
  size. No local/S3 performance equivalence is implied; record region and
  network placement alongside remote results. Dropping local OS caches alone
  does not establish fully cold S3 storage.

## Validate without a benchmark

```bash
./install
.venv/bin/python -m unittest discover -s . -p 'test_*.py' -v
```

Tests use tiny synthetic Parquet files, exercise every supported query, and
check token/phrase semantics, timestamps, file sizes, lifecycle, result timing,
timeout cancellation, and the shared runner in an isolated copy (including
`--index` and load-record reuse when GNU date is available). The copied test
driver has cache flushing and global `sync` explicitly disabled: restart=no
alone does not disable the shared runner's cache flushing. Tests never download
the real corpus or drop system caches. The S3 test uses a loopback S3
fixture for glob listing, range reads, and dummy-credential signing; it skips
unless extensions are installed. To enable it without accessing a real bucket:

```bash
.venv/bin/python adapter.py extensions
.venv/bin/python -m unittest discover -s . -p 'test_*.py' -v
```

Real AWS IAM policies, role/SSO credential refresh, and production-scale
performance still require validation in the intended benchmark environment.
