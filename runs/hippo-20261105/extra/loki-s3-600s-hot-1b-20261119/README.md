# Loki 1B on S3, 600 s cap, cold + 2 hot: all 56 supported queries (2026-11-19)

**This run is the Loki S3 1B column.** It replaces the 60 s-cap column of 2026-11-15
([`loki/runs/1b-20261115/`](../../../../loki/runs/1b-20261115/README.md)). It is the S3
counterpart of the local 600 s-cap run of 2026-11-18:
same source, driver, image and configuration but for the storage backend, on a new host and a
new load into S3, with the harness's **cold + 2 hot** tries per query.

**Not comparable on the geomean.** Every other column in the S3 set ran with the harness's 60 s
cap. This one has a **600 s** cap, so that the slow Loki 1B queries finish instead of showing 💀
at 60 s. It is shown in [`hippo-20261105`](../../README.md#loki-379--s3--prefilter-on--c16--600-s-cap) as the column
**Loki 3.7.9 · S3 · prefilter on · c16 · 600 s cap**. The UI marks 💀 at this column's own cap (600 s)
and leaves the column out of the geomean, CPU and GET common query sets (its summary cells show
"—", its header says "600 s cap; not comparable"). Its Hot view shows the best hot try.

| | |
|---|---|
| Host | one dedicated AWS **m8gd.4xlarge** (16 vCPU Graviton4, 64 GiB), us-east-1, page cache dropped before every cold try; chunks and index in S3 (us-east-1), the local NVMe holds only the WAL, the TSDB cache and the config |
| Engine | Loki **3.7.9 d3b4a2f7**, `grafana/loki@sha256:1a779c5f714e795ac698b125fc265ae97292f792a91535fa29726073d03d5580`, query target `read,query-scheduler`, stock adapter defaults: searchbench JSON frame encoding, `LOKI_LITERAL_PREFILTER=on`, querier concurrency **16**, `tsdb_max_query_parallelism` 32, split 15m, `tsdb_max_bytes_per_shard` 600MB, server `query_timeout` 600 s, result/chunk caches off; `object_store: s3` |
| Source | commit `61a0e6c` (`raw/provenance/SOURCE_COMMIT`); `source.tar.gz` = `git archive 61a0e6c`, sha256 `43550c4a…ed5b` (`raw/provenance/source-archive.sha256`) |
| Corpus | `otel_logs_1b/part_000.parquet`, sha256 `7489ed3e…a06fc` (`raw/provenance/corpus.sha256`) |
| Load | fresh S3 prefix (empty before the load, `raw/load/destination.json`; bucket name redacted), 1,000,000,000 rows, wall **5,256.0 s**, load CPU 43,776 s (containers 31,690 s + processes 12,086 s), **74,847,449,747** bytes in **109,811** objects: 109,810 chunk objects + 1 compacted index object (a 482-byte load lock object is in the bytes, not in the count); exact storage-only windowed count and an independent windowed count both 1,000,000,000 (`raw/load/load-verification.json.gz`, `raw/load/independent-count.stdout`). Load-only ingestion limit overlay (`raw/load/load-overrides.json`); queries used the stock config. The object inventory was unchanged after round 1 |
| Evidence | `evidence.tar.gz`, 10,142,127 bytes, sha256 `26386f957949f8abe057b69f78d8e557ed719f8da6fbae466d7d010c5b28acc1`; the files below are copied from it (all 648 entries of its `SHA256SUMS` verified) |

**Same as 2026-11-18 but for the storage.** Byte-identical to the local 2026-11-18 run:
`source.tar.gz`, `query_pair_variants.py`, `test_query_pair_variants.py`, `answers.py`,
`check_answers.py` and the reference set (`raw/driver/driver.sha256`, all five hashes equal and
re-checked on the host: `raw/provenance/driver-sha256-check.txt`), `run.sh`, `checkpoint.sh`, the
client binary hash, corpus hash, Loki binary version, load overrides and load source check. The
effective Loki config differs from 2026-11-18 only in the storage keys (filesystem
`chunks_directory`/`rules_directory` → `s3` bucket and region, `object_store: s3`, an
`object_prefix`), and from the 2026-11-15 S3 run only in the `object_prefix`. The image inspect
differs only in `LastTagTime`; `runtime.json` only in the container name. Script changes: the
collector lists the S3 inventory, `count_windows.py` has longer timeouts (1,800 / 300 s), and
`load_overlay.py` takes its root from the environment and has a dry-run check.

## Setup and method

- One host, one load, no relaunch, retry or restart. The host ran setup → load → round 1 →
  collect, unattended. Load 13:17–14:45 UTC on 2026-11-19; round 1 from 14:51 to 01:26 UTC.
- Driver: `query_pair_variants.py` (`raw/driver/`), the single-variant port of the adapter's
  `query_pair.py`, unchanged from 2026-11-17/18. One variant, `c16` =
  `LOKI_QUERY_CONCURRENCY=16,LOKI_LITERAL_PREFILTER=on` (the stock defaults); every try checks
  them against the server's `/config` and `runtime.json` (`raw/load/query-runtime.json`).
- **Round 1, the only round:** all 56 supported queries of `loki/queries.logql` in file order (the
  36 UNSUPPORTED slots are skipped). Per query: stop, sync + drop the page cache, start, **try 1
  cold**, then **tries 2 and 3 hot** on the same server lifetime (lib/benchmark.sh's shape). Cap
  **600 s** per try (client cap = server `query_timeout`). CPU per try.
  `--skip-hot-after-cold-timeout`: after a cold timeout the hot tries are skipped.
- **GET counters:** the driver reads CPU through `cpu-usage`, here wrapped by `sbwrap-cpu-usage`
  (`raw/round1-gets/`, the same wrapper and `getscrape.py` as the 2026-11-15 run), which also
  scrapes Loki's `/metrics` for `loki_s3_request_duration_seconds_count{operation="S3.GetObject"}`
  before and after every executed try (`raw/round1-gets/calls.jsonl`, 328 records = 2 × 164).
- Timeouts are censored: "> 600 s", never a time. Every finished hot try processed the same number
  of lines as its cold try and returned the same answer (same multiset sha256).

## Conversion to the results file

[`to_results.py`](to_results.py) maps the trials with lib/benchmark.sh's `run_one_query` rules,
like the 2026-11-18 run's `to_results.py`:

- ok try: its client time; timed-out try: the cap, **600**, with the CPU of the try;
- the tries after a timeout are padded with the cap (**600**) and CPU null, so they render as
  timed out. Here both timeouts are cold: **Q14 = Q29 = [600, 600, 600]**, CPU
  `[8781.729, null, null]` and `[8803.903, null, null]`.

The results file has `"timeout_cap": 600`. The load record follows the 2026-11-15 S3 column's
(`data_size` = every object under the prefix, `storage_units` = chunk + index objects).

It also writes this column's entry of [`../../s3_requests.json`](../../s3_requests.json) (and
removes the 2026-11-15 one), in the format of the 2026-11-15 entry: `[gets, null]` per try (bytes
were not collected), `partial: true`. Executed try *i* (in `trials.jsonl` order) is paired with
scrape records 2*i*−1 / 2*i*; the converter checks the roles, that both scrapes succeeded, that
the Loki process is new at each cold try and the same for its hot tries, and that the wrapper's
CPU delta equals the trial's `engine_cpu_seconds` (it does, exactly, for all 164). GETs = the
counter delta summed over its `200` and `500` labels. A query with a timed-out try has
`[null, null]` on every try, as in the 2026-11-15 entry: its counter delta is a lower bound of an
unfinished try (Q14 109,411, Q29 109,513 at the kill), not a cell. `--check` compares all three
files with their committed versions.

## Results

**54 of 56 finish all three tries under 600 s**, from 1.268 s (Q73 hot) to 592.747 s (Q26 hot);
**Q14 and Q29 time out cold (> 600 s)**, so their hot tries were not run.

**S3 vs local (2026-11-18, same configuration on local NVMe).** On the 54 queries that finish cold
in both runs, S3 cold ÷ local cold has a **median of 1.096** (1.055 to 1.454); 1.059–1.126 on the
36 full scans (109,810 or 109,825 chunks, +14 to +35 s), and the largest ratios on the short log queries
(Q76 7.39 → 10.75 s). Cold geomean over these 54: **143.11 s on S3 vs 129.73 s local**. Best hot
on S3 ÷ best hot local: median 1.098. Loki CPU per cold try, S3 ÷ local: median 1.073. 48 of the
54 take more than 60 s cold; the six under 60 s are Q68, Q72, Q73, Q76, Q80 and Q81, as in every
Loki 1B run.

**Hot vs cold.** Loki's result and chunk caches are off, so a hot try fetches the chunks from S3
again: on the 38 metric queries its GET count equals the cold try's (Q21 hot 2: +1), on the log
queries it is within 3 % of it but for Q73/Q81 (up to +27 %; `raw/round1-gets/`). Best hot ÷ cold over
the 54: **median 0.998**, from **0.745** (Q76, 10.75 → 8.00 s), 0.787 (Q73) and 0.805 (Q81) to
1.011 (Q70). 50 of 54 are within 5 %. Geomean over these 54: cold 143.11 s, best hot 140.10 s.
These geomeans are **not comparable** with the geomean row of the UI: that row is a ratio over
the common 60 s-capped queries, and this column is not in it.

**vs the 2026-11-15 S3 run.** Its 600 s diagnostic (one cold + 2 hot of Q01, Q25, Q61 on that
host and load, same config but for the prefix) was 7.3–8.0 s faster cold (Q01 203.5 vs 211.5 s).
Its six 60 s-cap finishers are within a few seconds of these (Q68 cold 9.19 vs 9.18 s).

CPU is the Loki containers' CPU during the try, in seconds. Total, all 164 tries run: 496,500 s
(cold tries: 177,173 s). GETs are per try, cold / hot 1 / hot 2. "local 11-18" is the cold try
of the local 600 s-cap run (2026-11-18);
"11-15 diag600" the cold try of the [2026-11-15 S3 run](../../../../loki/runs/1b-20261115/README.md)
and this run's Δ to it.

| query | cold (s) | hot 1 (s) | hot 2 (s) | best hot ÷ cold | CPU cold / hot 1 / hot 2 (s) | GETs cold / hot 1 / hot 2 | local 11-18 cold (s) | Δ vs local (s) | 11-15 diag600 cold (s) / Δ | answer |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Q01 | 211.494 | 212.368 | 211.141 | 0.998 | 2617.0 / 2625.7 / 2616.3 | 109,810 / 109,810 / 109,810 | 190.899 | +20.60 | 203.521 / +7.97 | ok |
| Q02 | 171.228 | 171.440 | 170.848 | 0.998 | 2119.6 / 2121.5 / 2118.1 | 109,810 / 109,810 / 109,810 | 152.020 | +19.21 |  | ok |
| Q03 | 201.705 | 202.075 | 202.314 | 1.002 | 2523.3 / 2528.3 / 2528.6 | 109,810 / 109,810 / 109,810 | 182.945 | +18.76 |  | ok |
| Q04 | 226.094 | 224.817 | 226.000 | 0.994 | 2737.1 / 2741.5 / 2735.8 | 109,810 / 109,810 / 109,810 | 206.319 | +19.77 |  | ok |
| Q05 | 172.313 | 172.708 | 173.050 | 1.002 | 2156.9 / 2159.1 / 2159.9 | 109,810 / 109,810 / 109,810 | 155.461 | +16.85 |  | ok |
| Q06 | 479.945 | 480.075 | 480.220 | 1.000 | 6510.4 / 6516.9 / 6518.4 | 109,810 / 109,810 / 109,810 | 445.673 | +34.27 |  | ok |
| Q07 | 527.296 | 527.493 | 527.825 | 1.000 | 7389.0 / 7395.5 / 7396.6 | 109,810 / 109,810 / 109,810 | 495.486 | +31.81 |  | ok |
| Q08 | 580.498 | 581.531 | 578.999 | 0.997 | 8148.9 / 8155.7 / 8151.3 | 109,810 / 109,810 / 109,810 | 546.862 | +33.64 |  | ok |
| Q10 | 195.234 | 195.115 | 195.124 | 0.999 | 2469.4 / 2476.6 / 2473.6 | 109,810 / 109,810 / 109,810 | 174.126 | +21.11 |  | ok |
| Q11 | 195.250 | 194.058 | 195.158 | 0.994 | 2472.0 / 2469.8 / 2472.3 | 109,810 / 109,810 / 109,810 | 175.842 | +19.41 |  | ok |
| Q12 | 172.249 | 171.778 | 172.455 | 0.997 | 2120.3 / 2127.4 / 2127.1 | 109,810 / 109,810 / 109,810 | 154.784 | +17.47 |  | ok |
| Q14 | 💀 > 600 | not run | not run | — | 8781.7 / — / — | — | 599.514 | — |  | — (no answer) |
| Q15 | 176.057 | 175.723 | 176.004 | 0.998 | 2223.8 / 2220.3 / 2225.2 | 109,810 / 109,810 / 109,810 | 156.964 | +19.09 |  | ok |
| Q16 | 197.047 | 196.886 | 196.485 | 0.997 | 2483.3 / 2483.4 / 2481.6 | 109,810 / 109,810 / 109,810 | 175.776 | +21.27 |  | ok |
| Q17 | 207.447 | 208.084 | 207.484 | 1.000 | 2639.9 / 2650.9 / 2646.1 | 109,810 / 109,810 / 109,810 | 186.350 | +21.10 |  | ok |
| Q18 | 185.738 | 185.840 | 187.277 | 1.001 | 2340.6 / 2347.3 / 2347.8 | 109,810 / 109,810 / 109,810 | 166.693 | +19.04 |  | ok |
| Q19 | 224.144 | 224.246 | 224.522 | 1.000 | 2858.7 / 2862.6 / 2861.1 | 109,810 / 109,810 / 109,810 | 204.827 | +19.32 |  | ok |
| Q20 | 186.769 | 187.653 | 187.182 | 1.002 | 2348.1 / 2355.9 / 2350.9 | 109,810 / 109,810 / 109,810 | 169.497 | +17.27 |  | ok |
| Q21 | 199.039 | 196.304 | 195.512 | 0.982 | 2478.5 / 2483.3 / 2473.0 | 109,810 / 109,810 / 109,811 | 178.141 | +20.90 |  | ok |
| Q25 | 184.922 | 185.642 | 185.171 | 1.001 | 2341.8 / 2346.7 / 2338.7 | 109,810 / 109,810 / 109,810 | 166.131 | +18.79 | 177.667 / +7.25 | ok |
| Q26 | 590.816 | 592.747 | 590.777 | 1.000 | 8309.3 / 8314.8 / 8317.0 | 109,810 / 109,810 / 109,810 | 557.976 | +32.84 |  | ok |
| Q27 | 192.818 | 192.115 | 192.792 | 0.996 | 2415.8 / 2415.4 / 2414.4 | 109,810 / 109,810 / 109,810 | 176.079 | +16.74 |  | ok |
| Q28 | 244.033 | 243.686 | 244.339 | 0.999 | 2927.0 / 2929.6 / 2927.7 | 109,810 / 109,810 / 109,810 | 223.326 | +20.71 |  | ok |
| Q29 | 💀 > 600 | not run | not run | — | 8803.9 / — / — | — | 💀 > 600 | — |  | — (no answer) |
| Q30 | 210.762 | 210.467 | 210.765 | 0.999 | 2616.2 / 2612.4 / 2611.4 | 109,825 / 109,825 / 109,825 | 192.122 | +18.64 |  | ok |
| Q31 | 108.887 | 108.558 | 108.652 | 0.997 | 1188.3 / 1187.6 / 1186.2 | 40,294 / 40,294 / 40,294 | 101.160 | +7.73 |  | ok |
| Q32 | 580.858 | 578.778 | 579.500 | 0.996 | 8133.7 / 8131.0 / 8139.3 | 109,825 / 109,825 / 109,825 | 548.605 | +32.25 |  | ok |
| Q54 | 480.729 | 479.892 | 482.002 | 0.998 | 6536.6 / 6528.9 / 6541.7 | 109,810 / 109,810 / 109,810 | 450.325 | +30.40 |  | ok |
| Q55 | 197.370 | 199.232 | 198.645 | 1.006 | 2488.7 / 2502.3 / 2494.2 | 109,810 / 109,810 / 109,810 | 178.534 | +18.84 |  | ok |
| Q56 | 203.515 | 202.589 | 202.821 | 0.995 | 2536.4 / 2533.8 / 2531.8 | 109,810 / 109,810 / 109,810 | 184.968 | +18.55 |  | ok (multiset; no order) |
| Q57 | 481.269 | 481.546 | 479.137 | 0.996 | 6521.3 / 6535.1 / 6526.0 | 109,810 / 109,810 / 109,810 | 447.761 | +33.51 |  | ok |
| Q58 | 196.965 | 196.580 | 197.603 | 0.998 | 2486.0 / 2483.4 / 2491.1 | 109,810 / 109,810 / 109,810 | 178.955 | +18.01 |  | ok |
| Q60 | 479.660 | 480.019 | 479.806 | 1.000 | 6516.9 / 6520.8 / 6517.4 | 109,810 / 109,810 / 109,810 | 448.519 | +31.14 |  | ok (multiset; no order) |
| Q61 | 238.934 | 239.494 | 240.090 | 1.002 | 2899.5 / 2904.1 / 2903.9 | 109,810 / 109,810 / 109,810 | 223.241 | +15.69 | 231.598 / +7.34 | ok |
| Q62 | 214.474 | 214.188 | 214.913 | 0.999 | 2671.9 / 2675.3 / 2676.6 | 109,810 / 109,810 / 109,810 | 197.049 | +17.43 |  | ok |
| Q63 | 201.628 | 201.298 | 201.562 | 0.998 | 2551.2 / 2554.0 / 2548.9 | 109,810 / 109,810 / 109,810 | 184.791 | +16.84 |  | ok |
| Q64 | 504.541 | 503.719 | 504.700 | 0.998 | 7048.3 / 7046.2 / 7051.7 | 109,810 / 109,810 / 109,810 | 474.534 | +30.01 |  | ok |
| Q65 | 237.855 | 238.783 | 239.216 | 1.004 | 2886.7 / 2895.2 / 2888.0 | 109,810 / 109,810 / 109,810 | 223.833 | +14.02 |  | ok (multiset; no order) |
| Q66 | 111.078 | 110.747 | 111.257 | 0.997 | 1206.5 / 1205.8 / 1210.5 | 40,291 / 40,291 / 40,291 | 104.531 | +6.55 |  | ok |
| Q67 | 483.851 | 481.710 | 480.608 | 0.993 | 6550.1 / 6546.0 / 6542.6 | 109,810 / 109,810 / 109,810 | 449.181 | +34.67 |  | ok |
| Q68 | 9.184 | 7.944 | 10.135 | 0.865 | 67.6 / 67.1 / 68.8 | 8,281 / 8,399 / 8,431 | 7.391 | +1.79 |  | ok |
| Q69 | 116.310 | 115.488 | 113.938 | 0.980 | 1446.1 / 1440.9 / 1438.3 | 75,590 / 75,561 / 75,534 | 104.732 | +11.58 |  | ok |
| Q70 | 120.111 | 121.432 | 121.960 | 1.011 | 1524.3 / 1533.2 / 1530.4 | 76,497 / 76,366 / 76,529 | 113.873 | +6.24 |  | ok (boundary tie) |
| Q71 | 138.852 | 136.207 | 137.421 | 0.981 | 1777.1 / 1768.2 / 1771.2 | 80,727 / 80,690 / 80,709 | 124.863 | +13.99 |  | ok |
| Q72 | 4.365 | 4.256 | 4.242 | 0.972 | 34.0 / 34.2 / 34.4 | 3,775 / 3,879 / 3,864 | 4.090 | +0.27 |  | ok |
| Q73 | 1.612 | 1.268 | 1.350 | 0.787 | 14.2 / 13.7 / 14.4 | 2,065 / 2,573 / 2,628 | 1.304 | +0.31 |  | ok |
| Q74 | 117.251 | 117.127 | 115.366 | 0.984 | 1470.0 / 1473.5 / 1468.8 | 67,938 / 67,969 / 67,993 | 108.632 | +8.62 |  | ok (tie order) |
| Q75 | 277.167 | 277.457 | 283.156 | 1.001 | 3714.1 / 3716.8 / 3722.3 | 53,185 / 53,141 / 53,147 | 262.016 | +15.15 |  | ok (tie order) |
| Q76 | 10.748 | 8.391 | 8.005 | 0.745 | 68.9 / 67.5 / 67.0 | 8,431 / 8,381 / 8,449 | 7.393 | +3.36 |  | ok |
| Q77 | 115.800 | 113.219 | 113.525 | 0.978 | 1443.9 / 1436.4 / 1438.7 | 75,622 / 75,555 / 75,551 | 105.823 | +9.98 |  | ok |
| Q78 | 122.624 | 119.477 | 123.377 | 0.974 | 1540.3 / 1533.0 / 1548.5 | 76,409 / 76,455 / 76,493 | 112.125 | +10.50 |  | ok (boundary tie) |
| Q79 | 138.174 | 138.112 | 136.552 | 0.988 | 1769.6 / 1770.2 / 1765.1 | 80,707 / 80,694 / 80,696 | 124.664 | +13.51 |  | ok |
| Q80 | 4.460 | 4.382 | 4.430 | 0.983 | 34.3 / 34.4 / 34.5 | 3,831 / 3,874 / 3,874 | 4.070 | +0.39 |  | ok |
| Q81 | 1.637 | 1.317 | 1.393 | 0.805 | 14.9 / 14.7 / 15.7 | 2,145 / 2,380 / 2,570 | 1.378 | +0.26 |  | ok |
| Q82 | 117.752 | 115.063 | 115.525 | 0.977 | 1479.1 / 1475.7 / 1477.4 | 67,989 / 67,976 / 67,925 | 107.098 | +10.65 |  | ok (tie order) |
| Q83 | 281.171 | 276.966 | 278.560 | 0.985 | 3720.1 / 3706.6 / 3712.0 | 53,186 / 53,211 / 53,213 | 266.001 | +15.17 |  | ok (tie order) |

## Q14 and Q29

Both time out cold at the 600 s cap, so the column shows 💀 on all three tries (Cold and Hot
views) and their hot tries were not run.

- **Q29** times out in every Loki 1B run at a 600 s cap (2026-11-17 and 2026-11-18 local, here).
  Its cold try used 8,804 s of CPU (local 2026-11-18: 8,640 s), about 14.7 busy cores for 600 s.
- **Q14** finished at the cap on local NVMe (2026-11-18 cold 599.514 s, its first hot try killed
  at 600 s; 2026-11-17 cold 596.0 s). The ~10 % S3 penalty of the full scans above puts it over
  the cap here: killed at 600 s, CPU 8,782 s (local cold 8,629 s). A Q14 time on S3 would be
  above 600 s, but this run does not say by how much.
- At the kill, their GetObject deltas were 109,411 (Q14) and 109,513 (Q29) of 109,810 chunk
  objects: both were still fetching chunks. These are lower bounds of unfinished tries, so the
  `s3_requests.json` cells are `null`.

## Answer checks

Every successful try was checked by `check_answers.py` (`raw/driver/`) against the 1B
references (DuckDB 1B oracle + ClickHouse 1B HTTP JSON results), plus the known 1B values (Q01
93,638,388; Q04 54,410,241; Q25 19,554,788; Q27 19,539,754; Q61 30 buckets summing to Q01;
Q68/Q72 multisets). Results: `raw/all-references.json.gz` and `raw/all-references.stdout`
(re-run off the host on the trials and answers of the evidence archive: byte-identical). Of the
168 tries: **135 match, 12 match-tie-order** (Q74, Q75, Q82, Q83: logs, order differs only
within equal timestamps), **6 unverified** (not an answer: Q14 and Q29, tries 1–3) and **15
flagged MISMATCH: Q56, Q60, Q65, Q70, Q78, all three tries each**. None is a wrong answer; they
are the checker limits of the 2026-11-17 and 2026-11-18 runs:

- **Q56, Q60, Q65:** `queries.logql` marks these "NO order by". Loki's multiset of rows equals the
  oracle's (`oracle_multiset: true`) and the ClickHouse check passes; only the row order differs,
  which the query does not fix.
- **Q70, Q78:** top 100 rows, sorted by timestamp descending. 99 rows equal the oracle's; the
  hundredth is at the boundary timestamp `2025-09-23T00:29:24.037000`, where Loki returns a
  different one of the tied rows than the oracle (it matches the filter, the ClickHouse check
  passes).
- **Same answers as on local NVMe.** For every query, every finished try returned the same
  multiset sha256 as every finished try of the local 2026-11-18 run, so the verdicts are the
  same, Q14 apart (finished cold locally, not here). Every hot try returned the same answer as
  its cold try.

The reference file itself (`references.json`, sha256 in `raw/driver/driver.sha256`) is not
copied: it contains corpus log bodies. It is the reference set of the 2026-11-15 S3 run.

## GET counters

- **SDK calls, not HTTP requests**, as in the [2026-11-15 run](../../../../loki/runs/1b-20261115/README.md):
  outer GetObject invocations of Loki's S3 client; SDK-internal HTTP retries can be hidden. Labels
  `200`/`500` mean nil/error returned to the instrumentation wrapper. 11,325 of the 14,312,414
  calls of the returned tries (0.08 %) carry the error label, all on Q21 (1 call) and the log
  queries Q68–Q83; they are counted in the GETs, as the native metric counts them.
- **Metric queries: one GetObject per downloaded chunk.** On the 38 metric queries (Q01–Q67),
  GETs per try = Loki's `chunksDownloaded`: 109,810 on the 34 full scans of every chunk object,
  109,825 on Q30/Q32, 40,291–40,294 on Q31/Q66; the one exception is Q21 hot 2, 109,811 with
  its one error-labelled call.
- **Log queries: about two GetObject calls per downloaded chunk** (Q69–Q83 but Q73/Q81:
  ×2.01–2.15; Q68/Q76 ×1.8; Q73/Q81 ×1.24–1.52), as in the 2026-11-15 run's six finishers
  (Q68 8,384 GETs there, 8,281 here). Not investigated further.
- **Bytes were not collected.** Byte cells and summaries are `—`, not zero.
- The column is `partial` and sparse, so its GETs constrain no common set and it has no GET
  summary cell: the S3 GET and byte rows of the other columns are unchanged.

## Files

| file | content |
|---|---|
| [`to_results.py`](to_results.py) | round-1 trials and GET scrapes → `loki/results/loki_3.7.9-m8gd4-1b-s3-frame-prefilter-on-c16-600s-20261119_otel_logs_1b.json`, its load record and its `s3_requests.json` entry; `--check` compares with the committed files |
| [`test_to_results.py`](test_to_results.py) | `python3 -m unittest test_to_results` (in this directory) |
| `raw/round1/` | `trials.jsonl.gz` (one line per try, 168), `plan.json`, `summary.json`, `geomeans.json`, `equality.json`, `requests.json` |
| `raw/round1-gets/` | `calls.jsonl` (GetObject counters and CPU before/after every executed try), `sbwrap-cpu-usage`, `getscrape.py`, `wrappers.sha256` (as installed on the host) |
| `raw/load/` | `load-record.json` (incl. the S3 objects by top-level prefix), `load-verification.json.gz`, `independent-count.stdout`, `load-source-check.json`, `load-overrides.json`, `verify-layout.txt`, `version.txt`, `runtime.json` (load), `query-runtime.json` (queries), `destination.json` (bucket name redacted) |
| `raw/driver/` | `query_pair_variants.py` (sha256 `8b5ef890…a8cd`), `test_query_pair_variants.py`, `answers.py` (sha256 `7e4c48b1…51a3`), `check_answers.py` (sha256 `904bd595…5aca`), `driver.sha256` (all hashes as recorded on the host) |
| `raw/provenance/` | source commit, source archive, corpus and client hashes, image inspect, Loki binary version |
| `raw/all-references.json.gz`, `raw/all-references.stdout` | answer checks of all tries |

Not copied (in the evidence archive): the per-try `/metrics` scrapes, Loki container logs,
pidstat/vmstat, the S3 inventory listings and the per-try answer files.

## Caveats

- **Not comparable on the geomean.** A 10× cap, versus a 60 s cap for every other column. The
  column is left out of the geomean, CPU and GET common sets so that it does not change any
  other column's summary.
- **Separate instance and load.** Same instance type, corpus and queries as the other columns,
  but its own host and fresh load into S3 (run 2026-11-19), not those of the other columns, of
  the 2026-11-15 S3 run or of the local 2026-11-18 run. The S3 vs local ratios compare two hosts
  and two loads (109,810 chunk objects here vs 109,896 chunk files locally).
- **Q14 is at the cap on either storage**; on S3 it is over it. One round only.
- `load_time` is load_overlay.py's wall time, 5,256.0 s; like the 2026-11-15 S3 column it
  includes finalization, compaction settle, the exact count check and the frozen inventory check.
- `os` is inferred, not recorded by this run: the same AMI as the 2026-11-15, 2026-11-17 and
  2026-11-18 runs (kernel 7.0.0-1014-aws, Ubuntu 24.04).
