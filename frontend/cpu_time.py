#!/usr/bin/env python3
"""Add CPU-time fields to page rows from each result's `cpu` array (SEARCHBENCH_CPU=on).

    cpu_time.py <rows.json>   (rewrites rows.json)

`cpu` runs parallel to `result`: the engine's CPU seconds (user + system, all cores) per try.
Per row with CPU data:
  cpu_by_q    {"Q01": [cold, hot, hot]}                          query detail panel
  cpu_cold, cpu_hot                                               summary rows (seconds)
  cpu_common_n                                                    size of the query set they cover
The sums cover the same queries for every column of a dataset that has CPU data: those with a
number for the cold try and the first hot try in all of them. Hot = try 2. Sparse columns
(sparse_columns.py) are not part of that set and get no sums, unless every column is sparse.
"""
import json, sys

rows_path = sys.argv[1]
rows = json.load(open(rows_path))
num = lambda v: isinstance(v, (int, float))
by_ds = {}
for r in rows:
    cpu = r.get("cpu")
    if not isinstance(cpu, list) or not any(isinstance(t, list) and any(num(x) for x in t) for t in cpu):
        continue
    ids = [t.get("id") or "Q%02d" % (i + 1) for i, t in enumerate(r.get("query_tags") or [{}] * len(cpu))]
    res = r.get("result") or []
    # CPU only for queries the engine ran (an UNSUPPORTED sentinel or a failed try does no work).
    # A cold-only column (no hot try run anywhere: [cold, null, null] rows) ran a query when its
    # cold try has a number.
    cold_only = all(x is None for t in res if isinstance(t, list) for x in t[1:])
    ran = lambda i: i < len(res) and isinstance(res[i], list) and (
        all(num(x) for x in res[i]) or cold_only and num(res[i][0]))
    r["cpu_by_q"] = {q: t for i, (q, t) in enumerate(zip(ids, cpu)) if isinstance(t, list) and ran(i)}
    by_ds.setdefault(r["dataset"], []).append(r)
ok = lambda t: isinstance(t, list) and len(t) >= 2 and num(t[0]) and num(t[1])
for ds, cols in by_ds.items():
    cols = [r for r in cols if "sparse" not in r] or cols
    common = set.intersection(*({q for q, t in r["cpu_by_q"].items() if ok(t)} for r in cols))
    for r in cols:
        if common:
            r.update(cpu_cold=round(sum(r["cpu_by_q"][q][0] for q in common), 3),
                     cpu_hot=round(sum(r["cpu_by_q"][q][1] for q in common), 3), cpu_common_n=len(common))
    print(f"cpu time {ds}: {len(cols)} columns with CPU data, {len(common)} common queries", file=sys.stderr)
json.dump(rows, open(rows_path, "w"))
