# Previous public-bench S3 pair (2026-11-03, replaced 2026-11-06)

These two columns were the S3 numbers of the public bench from 2026-11-05 to 2026-11-06, then replaced by the
CloudPrem 0.1.36 ingest-API / ClickHouse 1.7 GiB pair (see [`../../README.md`](../../README.md)). Kept unchanged:

- `clickhouse-pr17-results/`, `quickwit-results/`: the per-query, per-try medians of the 3 rounds + load records;
- `names.json`, `details.json`, `rounds.json`, `s3.json`: their entries as they were in the run set;
- `raw/`: the rounds, query logs, environment and series script of the pair.

## The pair (as described when it was public)

AWS m8gd.4xlarge (16 vCPU Graviton4 ARM64, 64 GiB), us-east-1, instance `<instance>`, 2026-11-03.
Dataset `otel_logs_1b`: the public `part_000.parquet` (1,000,000,000 rows, SHA-256 `7489ed3e…fa06fc`,
`extra/pair/corpus.json`). Both columns are **query-only** on existing S3 data, on **one box**, **interleaved**:
3 rounds, 10:39–11:43 UTC, ClickHouse first in rounds 1 and 3, Quickwit first in round 2.

| column | layout | size | load | rounds (cold / hot) | median of 3 | cold S3 GETs |
|---|--:|--:|--:|---|--:|--:|
| ClickHouse · S3 · 1.3 GiB cap · 84 parts | 84 parts | 58.2 GiB | 1,041 s | 1,336/933 · 1,407/964 · 1,311/930 | **1,352 / 933 ms** | 74.2k |
| Quickwit · S3 · 71c9181 · 82 splits | 82 splits | 42.1 GiB | 1,941 s | 397/287 · 410/293 · 388/277 | **389 / 275 ms** | 33.5k |
| ClickHouse vs Quickwit | | 1.4x | | | **3.5x / 3.4x** | 2.2x |

Geomean over the **57 queries both engines answer**. Median of 3 = per query, the median across rounds
(cold = try 1, hot = min of tries 2–3), then the geomean. Cold GETs: try 1 of round 1, summed over the 57.

## Setup

- **ClickHouse** 26.10.2.103 (`clickhouse/clickhouse-server@sha256:ec7bc702…`), `ORDER BY (ServiceName, Timestamp)`,
  `DateTime64(9)`, text index with phrase positions, **1.3 GiB part cap** (1,395,864,371 B), `s3` disk with no
  filesystem cache. [serenedb/searchbench#17](https://github.com/serenedb/searchbench/pull/17) queries,
  **`max_threads=32`**. Off: query cache, condition cache, text-index data caches (tokens, negative tokens,
  pattern bypass, postings), statistics. On: mark, primary-index and text-index header caches.
  Loaded 2026-11-03 08:35 UTC (run `20261103T-s3-final-1b-ch13`); its local part metadata was saved to S3 and
  reattached here (84 parts, 1B rows checked before the rounds, `extra/pair/clickhouse-ch13-parts.txt`).
- **Quickwit** `71c9181` (head of [quickwit-oss/quickwit#6843](https://github.com/quickwit-oss/quickwit/pull/6843),
  built on the box, `extra/pair/quickwit-version.txt`). Parquet bulk load, 16 pipelines, 10M-doc splits, stable_log,
  `TimestampTime` excluded; 82 splits. Off: fast-field, partial-request and predicate caches. On: split footer cache.
- **Method:** every column starts with all engines stopped; engine restart + page-cache drop before every query;
  3 tries; 60 s cap (none hit). Series script: `extra/pair/run.sh`.

## Files

| path | content |
|---|---|
| `clickhouse-pr17/results/`, `quickwit/results/` | per-query, per-try median of the 3 rounds (the column) + the load record |
| `rounds.json` | the 3 rounds per query (cold, hot), keyed like `names.json` |
| `s3_requests.json` | S3 GETs and bytes per try, round 1 (ClickHouse `system.query_log`, matched in benchmark order; Quickwit `/metrics` deltas) |
| `extra/pair/rounds/` | raw result files, result logs (Quickwit's gzipped: they hold the returned documents), environment and query windows of each round; Quickwit metrics |
| `names.json`, `details.json` | column names and setup lines, marked as public bench |
| `extra/pair/clickhouse-ch13-querylog.tsv.gz` | `system.query_log` of the rounds |

Raw upload: `s3://<bucket>/searchbench/runs-raw/20261103T-s3-pair-1b/`.

## Caveat

Earlier runs of the same two layouts, on other boxes or windows, agree within ±4%: ClickHouse 1.3 GiB
1,360 / 927 ms (08:53–09:58 UTC, a slower window) and Quickwit 82 splits 384 / 279 ms
(`20261103T-s3-final-1b`, 06:27–08:18 UTC). The ClickHouse GET count is 74.2k over all 57 queries: an earlier
text-matched count (73.5k over 56) missed Q20, whose SQL appears twice in the query set.
