# SearchBench / OpenSearch engine

ES-API-compatible fork of Elasticsearch. Single-pass adapter: of the 92
queries (Q01–Q92, aligned 1:1 with `serenedb`), 83 are native DSL
`_search` bodies and the 9 join queries map to NULL (OpenSearch has no
self-join and no ES|QL).

## Run

Docker is the only supported backend:
```bash
SEARCHBENCH_DATA_DIR=/path/to/data SEARCHBENCH_DATASET=otel_logs_100k ./benchmark.sh --index
# -> results/opensearch_otel_logs_100k.json
```

`--index` triggers the corpus download + load + index build; omit it to
re-run queries only. `SEARCHBENCH_DATA_DIR` (required, no default) is the
root data directory.

Prerequisites: `python3` with `pyarrow`/`orjson`/`requests`, `jq`, `curl`,
`docker`, `sudo sysctl -w vm.max_map_count=262144`. `./start` adds `--ulimit
memlock=-1:-1` automatically.

## Env

| Var | Default | Meaning |
|---|---|---|
| `OS_PORT` | `9200` | HTTP port |
| `OS_HEAP` | `30g` | JVM heap |
| `OS_DATA_DIR` | `${PWD}/os-data` | data + logs |
| `OS_IMAGE` | `opensearchproject/opensearch:3.8.0` | image |
| `OS_CONTAINER` | `searchbench-os` | container |

Index mapping in [`config/index_mapping.json`](config/index_mapping.json)
(same v3+positions shape as `elastic/`); workload in
[`queries.dsl`](queries.dsl). Ingest is the same vendored TextBench
`ingest.py`.

## Mappings (`OS_MAPPING`)

`./load` creates the index from one of two mappings:

- **`default`**: [`config/index_mapping.json`](config/index_mapping.json), the mapping of the
  published local column. `_source` stored, `best_compression`, and doc values on every keyword.
- **`lean`**: [`config/index_mapping_lean.json`](config/index_mapping_lean.json). It indexes only
  what the queries use, like the Quickwit adapter's index config, and does not store `_source`:
  - **Indexed:** `Body` (text with positions and norms), `ServiceName` / `SeverityText` (keyword
    + doc values) and `SeverityNumber`, as before.
  - **Doc values only:** `ScopeName` (group-by). `Timestamp` keeps its points index *and* doc
    values: without the points (as in Quickwit, which prunes by split time range instead), every
    time-window filter reads Timestamp's doc values for each candidate. On S3 at 1B that made the
    windowed counts Q30-Q32 3.3x slower (3.2 s -> 10.6 s cold) and the recent queries 1.3x, for
    ~3 B/doc saved.
  - **Stored only:** `TraceId`, `SpanId`, `TraceFlags`, the schema URLs and `ScopeVersion`.
    No query reads them.
  - **Derived source** (`index.derived_source.enabled`, OpenSearch 3.2+): no `_source`.
    Documents are rebuilt from stored fields and doc values; `Body` is stored for that.
  - **Codec:** `zstd`, level 6.
  - **The attribute maps** are sent as one JSON string each (`ingest.py --maps-as-json`) and
    mapped as keywords that are not indexed:
    - `ResourceAttributes` and `ScopeAttributes` as **doc values**, i.e. a per-segment dictionary
      of distinct values, which suits them: 1,173 and 1 distinct values in 1M docs;
    - `LogAttributes` (~250k distinct values in 1M docs) as a stored field.

    The maps cannot be searched by key, as with `enabled: false` in the default mapping.

  Searches return the same results. Returned documents differ: maps come back as JSON strings,
  `Timestamp` in the field's date format.

Size after force-merging to one segment, sum of segment files (B/doc):

| mapping | 1M | 10M |
|---|--:|--:|
| default | 123.4 | 124.6 |
| zstd 6 + no doc values on unqueried keywords | 101.1 | 102.4 |
| lean, Timestamp without points (first version) | 64.4 | - |
| lean without the map dictionary (maps as stored strings) | 92.8 | - |

At 10M, lean without the `Timestamp` / `TraceFlags` changes is 68.3. The 57 common queries
return the reference results on the lean index, both on local disk and mounted as a searchable
snapshot.

**1B on S3, lean vs default** (searchable snapshot, 16 shards x 1 segment, m8gd.4xlarge, cold =
restart + file-cache wipe before every query; 57 common queries):

| | default (2026-11-15) | lean (2026-11-18) |
|---|--:|--:|
| load | 14,841 s | **8,066 s** |
| ingest | 5,702 s (175k docs/s) | 4,036 s (248k docs/s) |
| load CPU, OpenSearch container | 67,050 s | 51,731 s |
| size on S3 | 143.7 GiB | **80.2 GiB** (-44%) |
| cold / hot geomean, median of 3 rounds | 5,205 / 348 ms | 6,446 / 378 ms |
| results equal to ClickHouse's | 56/57 | 56/57 |

The rounds ran on different boxes and days (S3 GETs cost 63 vs 75 ms on average). Both indices
mounted on the same box, cold ([`results/extra/`](results/extra/): one line per query and index):

| queries | default | lean | lean, Timestamp without points |
|---|--:|--:|--:|
| counts without a time window (25) | 5.17 s | 5.02 s (same as next column) | 5.02 s |
| group-bys (13) | 7.48 s | 6.87 s (same as next column) | 6.87 s |
| counts with a time window (Q30-Q32) | 3.30 s | 3.45 s | 11.75 s |
| recent (Q68-Q83, 16) | 7.89 s | 9.24 s | 10.14 s |

The lean index is as fast on counts and group-bys and ~17% slower on the recent queries, which
return 100 documents: rebuilding each from stored fields and doc values reads more 8 MiB blocks
from S3 (4,427 vs 3,400 GETs) than one `_source` read. On local disk this cost is small.

## Index sort: `ServiceName asc, Timestamp desc`

Both mappings sort each segment by `ServiceName`, then `Timestamp` **descending**, newest first within each
service. This is the order Elasticsearch's logsdb index mode uses by default (`host.name asc, @timestamp desc`).
Before 2026-11-20 the order was `Timestamp asc`.

- **10M, local disk** (1 shard, 1 segment, best of 5 hot tries, interleaved): size 68.17 vs 68.02 B/doc; the 16
  recent queries (sort by `Timestamp` desc, 100 hits) 29.9 vs 42.5 ms geomean (Q73 27 vs 183 ms), counts, top-k
  and group-bys unchanged. Results: the same sort keys; documents differ only between equal scores or timestamps.
- **1B on S3** (16 x 1, lean mapping, median of 3 rounds, m8gd.4xlarge; asc = the 2026-11-18 rounds, other box):

  | 57 common queries | asc | desc |
  |---|--:|--:|
  | cold / hot geomean | 6,446 / 378 ms | **6,089 / 266 ms** |
  | recent (16), cold / hot | 9,194 / 312 ms | **7,497 / 89 ms** |
  | the other 41, cold / hot | 5,612 / 407 ms | 5,615 / 407 ms |
  | size on S3 | 80.2 GiB | 80.0 GiB |
  | load (ingest) | 8,066 s (4,036 s) | 8,847 s (4,362 s) |

  Same box, both snapshots mounted, cold, one run each ([`results/extra/ab-1b-s3-lean-asc-vs-desc.jsonl`](results/extra/ab-1b-s3-lean-asc-vs-desc.jsonl)):
  recent queries 9.67 -> 7.76 s geomean, 4,427 -> 3,975 S3 GETs; 8 control queries 4.87 -> 4.87 s, same GETs.

## S3: searchable snapshots (`OS_STORAGE=s3`)

```bash
OS_STORAGE=s3 OS_S3_BUCKET=<bucket> OS_S3_BASE_PATH=searchbench/<run> ./benchmark.sh --index
```

`./load` builds the index on local disk exactly as above, snapshots it to an S3 repository,
deletes the local copy and restores the snapshot with `storage_type: remote_snapshot`: S3
stays the store of the index data, and the node fetches it on demand, in **8 MiB blocks**
(`AbstractBlockIndexInput.DEFAULT_BLOCK_SIZE_SHIFT = 23`, not configurable), into a file
cache on local disk.

- `./install` builds `searchbench/opensearch-s3:<version>`: the official image plus the
  `repository-s3` plugin, which it does not bundle. Credentials: the instance profile.
- `./start` gives the single node the `warm` role (`node.roles=cluster_manager,data,ingest,warm`)
  and a file cache of `OS_FILE_CACHE_SIZE` (200gb), `cluster.filecache.remote_data_ratio=5`.
- The snapshot repository is registered with `max_snapshot_bytes_per_sec: 2gb` (default
  40 MB/s, which caps a 1B snapshot at about an hour).

**Cache policy.** The file cache is a data cache, and three properties of it decide how cold
reads are measured:
- it survives restarts (restored from disk at startup);
- `POST <index>/_cache/clear?file=true` only drops blocks no open reader holds, which after a
  query is none of them;
- a `remote_snapshot` index cannot be closed (`FORBIDDEN/13/remote index is read-only`).

So `./start` deletes the cache directory while the node is down (`OS_FILE_CACHE_WIPE=on`).
With the driver's restart before every query, try 1 reads its data from S3, while the segment
metadata Lucene loads at startup stays cached, like the other engines' footer caches. Tries 2
and 3 find the data in the local file cache: **OpenSearch's hot tries are local-disk reads**,
unlike the S3 engines run with data caches off. Only try 1 is comparable with those.
`SEARCHBENCH_OS_METRICS_LOG=<file>` logs per query the repository's S3 GetObject count and time, the bytes
downloaded (file-cache growth: whole 8 MiB blocks, exact while nothing is evicted), request/query-cache hits and
the host's received network bytes.

**No result caching; hot = local file cache.** Checked at 1B (lean mapping): the request and query caches count
0 hits, and a hot try makes 0 S3 GETs. The file cache cannot be disabled (a `warm` node needs one), and a small
`node.search.cache.size` does not force hot tries back to S3: blocks stay referenced by the open readers and are
not evicted (4 GB: 0 GETs on 4 of 5 hot tries; 1 GB: queries failed). Only a restart + cache wipe before every
try would make every try read S3.

**Layout.** Cold reads cost one 8 MiB block per segment and file touched, fetched one at a
time within a segment, so the shard x segment layout decides S3 performance (as splits /
parts do for Quickwit / ClickHouse). `OS_SHARDS` (default: the mapping's 1) and
`OS_FORCE_MERGE_SEGMENTS` (default: none) set it; `thread_pool.force_merge.size` is 8 in
S3 mode so shards merge in parallel.
The `storage-units` hook reports the result as the run page's "Splits / parts / files" count:
the searchable Lucene segments of the primaries (`{"count":16,"unit":"segments"}` for the 1B S3
runs), comparable with Quickwit's splits and ClickHouse's parts. `./load` refreshes after the force
merge: before a refresh, `_cat/segments` still lists the merged-away segments (the 1B logs said 38-39;
the snapshots' shard manifests list one `.si` per shard, 16 in all three 1B S3 snapshots). At 100M, cold (restart + file-cache wipe before every
query; restart to green ~70 s), the 57 common queries:

| layout | cold geomean | slowest | cold S3 GETs | build after the load |
|---|--:|--:|--:|--:|
| 1 shard, 57 segments (as built) | 5,167 ms | 12.8 s | 4,198 | - |
| 1 shard, 1 segment | 1,530 ms | 7.6 s | 706 | force merge 1,306 s (single-threaded) |
| 16 shards x 1 segment | 1,951 ms | 4.0 s | 3,188 | split + force merge 1,092 s |

The S3 runs use **16 shards x 1 segment** (`OS_SHARDS=16 OS_FORCE_MERGE_SEGMENTS=1`): one
segment per shard keeps the block count low, and the 16 shards fetch in parallel; a single
1B segment would be read one block at a time and take hours to merge.

**1B, m8gd.4xlarge, 2026-11-15 (default mapping)**.

- **Load: 14,841 s.**
  - Ingest: 5,702 s, at 175k docs/s.
  - Most of the rest is the force merge. One `_forcemerge` request runs a node's shards **one after
    another** (`TransportBroadcastByNodeAction`), whatever `thread_pool.force_merge.size` is. The 16
    shards of 62.5M docs took ~2.3 h.
  - The snapshot to S3 takes minutes.
- **Size:** 143.7 GiB on S3, 16 shards x 1 segment.
- **Restart:** ~44 s until the node is green.
- **Queries:** 3 rounds, restart + file-cache wipe before every query. Over the 57 common queries:
  - **Median cold: 5,205 ms; median hot: 348 ms** (geomean). Hot = local file cache.
  - By task, cold / hot: count 4,105 / 248 ms; group-by 6,494 / 1,180 ms; recent 6,587 / 234 ms.
  - 56/57 results equal ClickHouse's in every round. Q67's two-key group-by only reports outer
    buckets, as in the local column.
  - No timeouts; all 83 queries answered.
  - Cold S3 GETs: 10.1k over the 57 queries, the same count in all 3 rounds.
- **Same 57 queries, other columns:**
  - Datadog BYOC 0.1.36 (S3): 515 / 344 ms
  - OpenSearch local (2026-09, 1 shard): 957 / 322 ms
  - ClickHouse 1.7 GiB cap (S3): 1,343 / 904 ms
  - DuckDB, sorted Parquet (S3): 7,769 / 6,683 ms

  OpenSearch's cold cost is the 8 MiB blocks it fetches from S3, which it cannot prefetch. Its hot
  tries read the blocks from local disk, so they are not comparable with the S3 engines' hot tries,
  which re-read S3.
- **Two bugs found on the way:**
  - The bulk client re-sent timed-out batches without `_id`, which duplicated documents (fixed above).
  - `./check` accepted a yellow cluster, so the first 1B round queried shards that were still
    starting: Q01 counted 23.4M instead of 93.6M. That round is discarded. `./check` now waits for
    green, and `./query` rejects partial results.
