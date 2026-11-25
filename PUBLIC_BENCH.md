# Runs used in the public bench

Every number published in the public benchmark (2026-11 blog) comes from a run listed here. Each entry
points to the committed results; the column's `details.json` line and its README say
**USED IN THE PUBLIC BENCH**.

## S3 · otel_logs_1b · m8gd.4xlarge · run set [`hippo-20261105`](runs/hippo-20261105/README.md)

The site opens on this run set. Columns updated 2026-11-06; OpenSearch added 2026-11-19 (index sort Timestamp desc 2026-11-20). Geomean over the 57 queries all
four engines answer; Datadog BYOC, ClickHouse and OpenSearch: median of 3 rounds, each on its own box. Load CPU: the engine only (ingest client excluded).

| engine | column | layout | median cold / hot | cold S3 GETs | load / load CPU | results |
|---|---|--:|--:|--:|--:|---|
| Datadog BYOC 0.1.36 | Datadog BYOC 0.1.36 · S3 · ingest API · 64 splits | 64 splits, 42.1 GiB | 515 / 338 ms | 26.4k | 2,364 s / 26,241 s | [`hippo-20261105/datadog-byoc/`](runs/hippo-20261105/datadog-byoc/) |
| ClickHouse 26.10.2.103 | ClickHouse · S3 · 1.7 GiB cap · 61 parts | 61 parts, 58.1 GiB | 1,343 / 894 ms | 64.1k | 1,288 s / 9,344 s | [`hippo-20261105/clickhouse-pr17/`](runs/hippo-20261105/clickhouse-pr17/) |
| OpenSearch 3.8.0 | OpenSearch 3.8 · S3 · searchable snapshot · 16 shards | 16 shards x 1 segment, 80.0 GiB | 6,089 / 266 ms\* | 10.6k | 8,847 s / 50,340 s | [`hippo-20261105/opensearch/`](runs/hippo-20261105/opensearch/) |
| DuckDB 2.0.0 | DuckDB 2.0 · S3 · sorted copy | no index, 24.7 GiB sorted copy | 7,769 / 6,683 ms | 401.6k | 1,453 s / 10,057 s | [`hippo-20261105/duckdb-parquet/`](runs/hippo-20261105/duckdb-parquet/) |

- Datadog BYOC: 2026-11-04 12:08–12:47 UTC; evidence in [`extra/datadog-byoc-c30/`](runs/hippo-20261105/extra/datadog-byoc-c30/).
- ClickHouse: 2026-11-03 21:32–22:30 UTC; evidence in [`extra/clickhouse-1p7g/`](runs/hippo-20261105/extra/clickhouse-1p7g/).
- DuckDB: sorted copy on S3, 2026-11-01, one run; evidence in [`extra/duckdb/`](runs/hippo-20261105/extra/duckdb/).
- OpenSearch: 2026-11-19, lean mapping, index sort ServiceName asc + Timestamp desc; evidence in [`extra/opensearch-s3-1b/`](runs/hippo-20261105/extra/opensearch-s3-1b/).
  \* Its hot tries read its local file cache (0 S3 GETs): only its cold try is comparable with the other columns.
- Previously public (2026-11-05 to 2026-11-06): ClickHouse 1.3 GiB / Quickwit 71c9181 bulk load, in
  [`hippo-20261105/extra/previous-pair-20261103/`](runs/hippo-20261105/extra/previous-pair-20261103/README.md).

Setup, method and caveats: [README](runs/hippo-20261105/README.md).

## Local disk · otel_logs_1b · m8gd.4xlarge

Not chosen yet.
