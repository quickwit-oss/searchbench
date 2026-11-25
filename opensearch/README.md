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
`SEARCHBENCH_OS_METRICS_LOG=<file>` logs per query the repository's S3 GetObject count and time.

**Layout.** Cold reads cost one 8 MiB block per segment and file touched, fetched one at a
time within a segment, so the shard x segment layout decides S3 performance.
`OS_SHARDS` (default: the mapping's 1) and `OS_FORCE_MERGE_SEGMENTS` (default: none) set
it. Note that one `_forcemerge` request merges a node's shards one after another
(`TransportBroadcastByNodeAction`), whatever `thread_pool.force_merge.size` is. At 100M
on a 16 vCPU / 64 GiB machine, cold (restart + file-cache wipe before every query; restart
to green ~70 s), over 57 of the term, phrase, pattern, group-by and recent queries:

| layout | cold geomean | slowest | cold S3 GETs | build after the load |
|---|--:|--:|--:|--:|
| 1 shard, 57 segments (as built) | 5,167 ms | 12.8 s | 4,198 | - |
| 1 shard, 1 segment | 1,530 ms | 7.6 s | 706 | force merge 1,306 s |
| 16 shards x 1 segment | 1,951 ms | 4.0 s | 3,188 | split + force merge 1,092 s |

The S3 runs use **16 shards x 1 segment** (`OS_SHARDS=16 OS_FORCE_MERGE_SEGMENTS=1`): one
segment per shard keeps the block count low, and the 16 shards fetch in parallel; a single
1B segment would be read one block at a time. At 1B this layout's force merge takes ~2.3 h
(16 shards of 62.5M docs, one after another).

The bulk client used to re-send a batch after a 120 s read timeout; the batch has no
`_id`, so OpenSearch indexed it twice (101.15M docs at 100M). `ingest.py` now has no read
timeout and `./load` fails unless the count equals the corpus's. `./check` waits for
**green** (not yellow): after a restart, a remote_snapshot index's shards start one by one,
and a query meanwhile silently counts a fraction of the documents; `./query` also rejects
partial results (`allow_partial_search_results=false`).
