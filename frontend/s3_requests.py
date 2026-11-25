#!/usr/bin/env python3
"""Add S3 request fields to page rows from <run>/s3_requests.json.

    s3_requests.py <rows.json> <s3_requests.json>   (rewrites rows.json)

Counters are keyed by "<dataset>/<system as recorded>" before names.json renames:
  s3_requests   {"Q01": [[gets, bytes], ...one per try]}   query detail panel
Either component can be null (unmeasured); a measured zero remains zero.

Each metric has its own common query set across columns that measured that metric,
using cold and first-hot samples. Columns marked "partial" do not constrain the
common set, but receive sums only if they cover it. Hot = try 2, not a minimum.
Summary coverage is s3_gets_common_n / s3_bytes_common_n; s3_common_n remains a
compatibility alias, preferring GET coverage. Missing summaries are omitted.
"""
import json
import math
import sys


def available(tries, component):
    return (
        isinstance(tries, list)
        and len(tries) >= 2
        and all(
            isinstance(t, list)
            and len(t) > component
            and isinstance(t[component], (int, float))
            and not isinstance(t[component], bool)
            and math.isfinite(t[component])
            and t[component] >= 0
            for t in tries[:2]
        )
    )


rows_path, s3_path = sys.argv[1], sys.argv[2]
with open(rows_path) as f:
    rows = json.load(f)
with open(s3_path) as f:
    s3 = json.load(f)
by_ds = {}
for r in rows:
    c = s3.get(r["dataset"] + "/" + r["system"])
    if c:
        by_ds.setdefault(r["dataset"], []).append((r, c))
        r["s3_requests"] = c["queries"]
        # Reprocessing must not retain a formerly measured summary for missing data.
        for key in ("s3_common_n", "s3_gets_common_n", "s3_bytes_common_n",
                    "s3_gets_cold", "s3_gets_hot", "s3_bytes_cold", "s3_bytes_hot"):
            r.pop(key, None)
for ds, cols in by_ds.items():
    for component, metric in enumerate(("gets", "bytes")):
        eligible = [(r, c, {q for q, t in c["queries"].items() if available(t, component)})
                    for r, c in cols]
        eligible = [(r, c, qs) for r, c, qs in eligible if qs]
        full = [entry for entry in eligible if not entry[1].get("partial")] or eligible
        common = set.intersection(*(qs for _, _, qs in full)) if full else set()
        for r, c, coverage in eligible:
            if common and common <= coverage:
                qs = [c["queries"][q] for q in common]
                r[f"s3_{metric}_cold"] = sum(t[0][component] for t in qs)
                r[f"s3_{metric}_hot"] = sum(t[1][component] for t in qs)
                r[f"s3_{metric}_common_n"] = len(common)
        print(f"s3 {metric} {ds}: {len(eligible)} columns with counters, "
              f"{len(common)} common queries", file=sys.stderr)
    for r, _ in cols:
        n = r.get("s3_gets_common_n", r.get("s3_bytes_common_n"))
        if n is not None:
            r["s3_common_n"] = n
with open(rows_path, "w") as f:
    json.dump(rows, f)
