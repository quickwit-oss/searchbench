#!/usr/bin/env python3
"""Mark the columns left out of the common query sets: sparse ones and those with another cap.

    sparse_columns.py <rows.json>   (rewrites rows.json; run by build_run.sh before cpu_time.py)

A sparse column (e.g. Loki at 1B, 6 of 56 queries under the 60 s cap) would shrink the common
query set of every other column to its few finished queries. It is therefore left out of the
common sets: the static CPU sums (cpu_time.py) and the page geomean (common_geomean.mjs). Its own
per-query cells and timeouts are still shown; its geomean/CPU summary cells show "—" with a note.

A column with its own `timeout_cap` (seconds) other than TIMEOUT_CAP ran under a different
limit, so its times are not comparable on the common sets either, however many queries finished.
It is marked the same way, with a `reason` the UI shows instead of the sparse note.

  supported  queries with a recorded number in `result` (not an UNSUPPORTED sentinel or an error)
  finished   cold try is a number below the column's cap, and so is the first hot try when it
             was run (a cold-only column has null hot tries; as cpu_time.py's ok(), plus not
             timed out)
Only marked rows get the field `sparse: {"finished": n, "supported": m[, "reason": text]}`;
other rows are untouched.
"""
import json
import sys

# A column is sparse when finished < SPARSE_FINISHED_FRACTION * supported.
SPARSE_FINISHED_FRACTION = 0.5
# Wall-time timeout cap in seconds: lib/benchmark.sh SEARCHBENCH_QUERY_TIMEOUT default, the UI's E0.
TIMEOUT_CAP = 60


def num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def cap_of(row):
    """The column's wall-time cap: its `timeout_cap`, else the harness default."""
    cap = row.get("timeout_cap")
    return cap if num(cap) else TIMEOUT_CAP


def coverage(row):
    """(finished, supported) query counts of one column."""
    cap = cap_of(row)
    tries = [t for t in row.get("result") or [] if isinstance(t, list)]
    supported = sum(1 for t in tries if any(num(v) for v in t))
    cold_only = all(v is None for t in tries for v in t[1:])
    finished = sum(1 for t in tries
                   if len(t) >= 2 and num(t[0]) and t[0] < cap
                   and (num(t[1]) and t[1] < cap or cold_only and t[1] is None))
    return finished, supported


def is_sparse(finished, supported):
    return finished < SPARSE_FINISHED_FRACTION * supported


def other_cap_reason(row):
    """Why a column with a non-default cap is left out, or None."""
    cap = cap_of(row)
    return None if cap == TIMEOUT_CAP else f"{cap:g} s cap; not comparable"


def mark(rows):
    for row in rows:
        finished, supported = coverage(row)
        reason = other_cap_reason(row)
        if reason and supported:
            row["sparse"] = {"finished": finished, "supported": supported, "reason": reason}
        elif is_sparse(finished, supported):
            row["sparse"] = {"finished": finished, "supported": supported}
    return rows


if __name__ == "__main__":
    path = sys.argv[1]
    rows = mark(json.load(open(path)))
    for r in rows:
        if "sparse" in r:
            why = r["sparse"].get("reason", "sparse")
            print(f"{why} column {r['dataset']}/{r['system']}: finished {r['sparse']['finished']} of "
                  f"{r['sparse']['supported']}; left out of the common query sets", file=sys.stderr)
    json.dump(rows, open(path, "w"))
