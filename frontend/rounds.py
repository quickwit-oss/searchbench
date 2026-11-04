#!/usr/bin/env python3
"""Add per-round timings to page rows from <run>/rounds.json (query panel).

    rounds.py <rows.json> <rounds.json>   (rewrites rows.json)

rounds.json, keyed like names.json by "<dataset>/<system as recorded>":
    {"session": "...", "s3_ms_per_get": [...], "queries": {"Q01": [[cold, hot], ...one per round]}}
A row with an entry gets `rounds` = that object; the query panel shows the range across rounds.
The row's own result is expected to be the per-query median of those rounds.
"""
import json, sys

rows_path, rounds_path = sys.argv[1], sys.argv[2]
rows, rounds = json.load(open(rows_path)), json.load(open(rounds_path))
n = 0
for r in rows:
    e = rounds.get(r["dataset"] + "/" + r["system"])
    if e:
        r["rounds"] = e; n += 1
print(f"rounds: {n} columns with per-round timings", file=sys.stderr)
json.dump(rows, open(rows_path, "w"))
