#!/usr/bin/env python3
"""Round-1 cold/hot/hot trials -> one standard results file, its load record and its GET counters
(Loki 1B on S3, 600 s cap, 2026-11-19).

Deterministic; reads only files in this repository:
  raw/round1/trials.jsonl.gz       query_pair_variants.py trials (variant c16, 3 tries, 600 s cap, CPU)
  raw/round1/plan.json             cap, tries, skip rule, query order, storage
  raw/round1-gets/calls.jsonl      sbwrap-cpu-usage + getscrape.py: Loki /metrics GetObject counters
                                   before and after every executed try, in trial order
  raw/load/load-record.json        load_overlay.py: wall seconds, load CPU, S3 objects and bytes
  raw/load/query-runtime.json      querier concurrency and prefilter of the query server
  ../../loki/queries.logql         the 92 query slots, in order (same file as the run's source)

Writes (relative to the run directory):
  loki/results/loki_<version>_otel_logs_1b.json    the 600 s-cap S3 column, cold + 2 hot
  loki/results/load/otel_logs_1b.<build>.json      its load record
  s3_requests.json, key KEY                        GET counts per try (only that key is rewritten)

Mapping (lib/benchmark.sh run_one_query / write_json / write_load_record, 3 tries), as
../../../2026-11-01-m8gd-4xlarge-1b-local-nocache/extra/loki-600s-hot-1b-20261118/to_results.py:
  - One row per non-comment line of queries.logql. UNSUPPORTED: result and cpu [null,null,null].
  - Try 1 is cold (stop, page-cache drop, start); tries 2 and 3 are hot, on the same server
    lifetime, with no cycle in between.
  - ok try: the last bare-numeric stderr line (parse_timing), verbatim; checked equal to
    http_seconds / latency_seconds. CPU of the try, printf "%.3f".
  - Timed-out try (driver cap): the cap, 600, and the CPU of the try. The remaining tries are
    padded with the cap (600) and cpu null, then stop. Here both timeouts are cold (Q14, Q29);
    their hot tries are "skipped-after-cold-timeout" (no latency, no CPU reading):
    Q14 = Q29 = [600, 600, 600].
  - Any other status is refused (none occurs).
  - `timeout_cap`: 600 (top level): the UI draws 💀 at this column's cap and leaves the column out
    of the geomean and CPU common sets (frontend/sparse_columns.py).
  - load_time = load_overlay.py wall seconds; data_size = bytes of every object under the
    prefix; storage_units = chunk objects + index objects (the load lock object is in data_size,
    not in the count), unit "objects", as the 2026-11-15 S3 column's record; load_cpu =
    containers + procs, each rounded to 3 decimals first.

GET counters (s3_requests.json, as the 2026-11-15 S3 column's entry):
  - Executed try i (ok or timeout, in trials.jsonl order) <-> calls.jsonl records 2i+1 (before)
    and 2i+2 (after). Checked: roles, scrape ok, same Loki process (process_start) within a try,
    a new process at every cold try and the same one for its hot tries, and the wrapper's CPU
    delta equal to the trial's engine_cpu_seconds.
  - GETs of a try = after - before of loki_s3_request_duration_seconds_count{operation=
    "S3.GetObject"}, summed over its status_code labels ("200", "500"): SDK calls, as counted by
    the native metric.
  - ok try: [gets, null] (bytes not collected). A timed-out query: [null, null] on all three
    tries (its counter delta is a lower bound of an unfinished try; it is in the README, not a cell).
  - "partial": true: the column does not constrain the common GET set (frontend/s3_requests.py).
Usage: python3 to_results.py [--check]   (--check: compare with the committed files, write nothing)
"""
import gzip
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
RUN = HERE.parents[1]
RAW = HERE / "raw"
QUERIES = RUN / "loki" / "queries.logql"
RESULTS = RUN / "loki" / "results"
S3_REQUESTS = RUN / "s3_requests.json"

ENGINE_NAME = "Loki"
ENGINE_TAGS = '["Go","Loki"]'            # loki/benchmark.sh
DATASET = "otel_logs_1b"
DATE = "2026-11-19"                      # UTC date of the load and round 1
# Same AMI as the 2026-11-15, 2026-11-17 and 2026-11-18 runs; uname: 7.0.0-1014-aws, Ubuntu 24.04.
OS_NAME = "Ubuntu 24.04.5 LTS"
LOADED_BY = "load_overlay.py (adapter.load; extra/loki-s3-600s-hot-1b-20261119)"
BUILD = "3.7.9-m8gd.4xlarge-s3-frame-1b-20261119"
VERSION = "3.7.9-m8gd4-1b-s3-frame-prefilter-on-c16-600s-20261119"
KEY = f"{DATASET}/{ENGINE_NAME} ({VERSION})"
# The entry this column replaces; it is removed from s3_requests.json (its copy is the host's
# collation in ../loki-s3-1b-20261115/raw/phase2/collated/s3_requests.json).
SUPERSEDED_KEY = f"{DATASET}/{ENGINE_NAME} (3.7.9-m8gd4-1b-s3-gets)"
GETS_SOURCE = ("Native GetObject SDK-call deltas, same trials as wall/CPU; bytes not collected. "
               "Internal HTTP retries may not be separately counted.")
VARIANT = "c16"
CAP = 600
TRIES = 3                                # [cold, hot, hot]
PADDED = {"skipped-after-cold-timeout", "not-run-after-timeout"}
EXECUTED = {"ok", "timeout"}
TIMING = re.compile(r"^[ \t]*[0-9]+(\.[0-9]+)?[ \t]*$")


def slug(label):
    return re.sub(r"[^A-Za-z0-9._-]", "_", label)


def results_path():
    return RESULTS / f"{ENGINE_NAME.lower()}_{slug(VERSION)}_{DATASET}.json"


def load_record_path():
    return RESULTS / "load" / f"{DATASET}.{slug(BUILD)}.json"


def query_slots(path=QUERIES):
    """[(query line, tag)] like lib/benchmark.sh main(): tags from the preceding `-- QNN task=` line."""
    slots, tag = [], {"id": "", "task": "", "filter": "", "freq": ""}
    for line in path.read_text().split("\n"):
        if re.match(r"^\s*--.*task=", line):
            def grep(pattern, cut):
                found = re.findall(pattern, line)
                return "\n".join(m.split("=", 1)[1] if cut else m for m in found)
            ids = re.findall(r"Q[0-9]+", line)
            tag = {"id": ids[0] if ids else "", "task": grep(r"task=[a-z_]+", True),
                   "filter": grep(r"filter=[a-z,_]+", True), "freq": grep(r"freq=[a-z]+", True)}
            continue
        if not line.strip() or re.match(r"^\s*--", line):
            continue
        slots.append((line, tag))
        tag = {"id": "", "task": "", "filter": "", "freq": ""}
    return slots


def parse_timing(stderr):
    """lib/benchmark.sh parse_timing: last bare-numeric line, whitespace stripped."""
    lines = [l for l in stderr.replace("\r", "\n").split("\n") if TIMING.match(l)]
    return re.sub(r"\s", "", lines[-1]) if lines else ""


def cpu_text(seconds):
    return "%.3f" % (seconds if seconds > 0 else 0.0)


def query_row(trials):
    """(result cells, cpu cells) of one supported query's tries, as run_one_query records them."""
    assert [t["try"] for t in trials] == list(range(1, TRIES + 1)), trials
    results, cpus = [], []
    for t in trials:
        assert t["cap_seconds"] == CAP, t["query"]
        if t["try"] == 1:
            assert (t["kind"], t["cycle"]["cache"]) == ("cold", "dropped"), t["query"]
        else:
            assert (t["kind"], t["cycle"]) == ("hot", None), t["query"]
        status = t["status"]
        if status == "ok":
            timing = parse_timing(t["stderr_tail"])
            assert timing and float(timing) == t["http_seconds"] == t["latency_seconds"], t["query"]
            results.append(timing)
        elif status == "timeout":
            assert t["timeout_source"] == "driver-cap" and t["censored_at_seconds"] == CAP, t
            results.append(str(CAP))                  # timeout(1) rc 124/137 -> the cap
        else:
            raise ValueError(f"{t['query']} try {t['try']}: no harness mapping for status {status!r}")
        cpus.append(cpu_text(t["engine_cpu_seconds"]) if t["engine_cpu_seconds"] is not None else "null")
        if status == "timeout":
            rest = trials[t["try"]:]
            # Tries after a timeout were not run: no latency, no CPU reading.
            assert all(r["status"] in PADDED and r["latency_seconds"] is None
                       and r.get("engine_cpu_seconds") is None for r in rest), rest
            while len(results) < TRIES:               # pad with the cap, cpu null, stop
                results.append(str(CAP))
                cpus.append("null")
            break
    return results, cpus


def array_text(rows):
    """write_json layout: '[' + ',\\n        [..]' per row + '\\n    ]'."""
    return "[" + ",".join("\n        [" + ",".join(r) + "]" for r in rows) + "\n    ]"


def by_query(trials):
    out = {}
    for t in trials:
        assert t["variant"] == VARIANT, t["query"]
        out.setdefault(t["query"], []).append(t)
    return {q: sorted(ts, key=lambda t: t["try"]) for q, ts in out.items()}


def results_text(slots, trials, load):
    queries = by_query(trials)
    result_rows, cpu_rows, tags = [], [], []
    for line, tag in slots:
        if line.startswith("UNSUPPORTED:"):
            assert tag["id"] not in queries, tag
            result_rows.append(["null"] * TRIES)
            cpu_rows.append(["null"] * TRIES)
        else:
            spec = json.loads(line)
            assert spec["id"] == tag["id"], (spec["id"], tag)
            r, c = query_row(queries.pop(spec["id"]))
            result_rows.append(r)
            cpu_rows.append(c)
        tags.append(json.dumps(tag, separators=(",", ":")))
    assert not queries, f"trials without a query slot: {sorted(queries)}"
    qj = "[" + ",".join("\n        " + t for t in tags) + "\n    ]"
    s = lambda v: json.dumps(v, ensure_ascii=False)
    return ("{\n"
            f'  "system": {s(f"{ENGINE_NAME} ({VERSION})")},\n'
            f'  "version": {s(load["engine_version"])},\n'
            f'  "os": {s(OS_NAME)},\n'
            f'  "date": {s(DATE)},\n'
            f'  "dataset": {s(DATASET)},\n'
            f'  "build": {s(BUILD)},\n'
            f'  "tags": {ENGINE_TAGS},\n'
            f'  "load_time": {load["load_time_text"]},\n'
            f'  "data_size": {load["data_size"]},\n'
            f'  "storage_units": {json.dumps(load["storage_units"], separators=(",", ":"))},\n'
            f'  "timeout_cap": {CAP},\n'
            f'  "result": {array_text(result_rows)},\n'
            f'  "query_tags": {qj},\n'
            f'  "cpu": {array_text(cpu_rows)}\n'
            "}\n")


def load_inputs():
    record = json.loads((RAW / "load" / "load-record.json").read_text())
    assert record["status"] == "complete", record
    tops = record["s3_objects_by_top"]
    assert sorted(tops) == ["_searchbench_load_lock.json", "fake", "index"], sorted(tops)
    assert record["data_size_bytes"] == sum(v["bytes"] for v in tops.values()), "data_size = every object"
    count = tops["fake"]["objects"] + tops["index"]["objects"]     # chunks + index; not the lock
    assert record["storage_units"] == {"count": count, "unit": "objects"}, record["storage_units"]
    version = (RAW / "load" / "version.txt").read_text().strip()
    containers = "%.3f" % record["load_cpu_containers_seconds"]
    procs = "%.3f" % record["load_cpu_procs_seconds"]
    return {"engine_version": version, "load_time_text": "%.3f" % record["wall_seconds"],
            "data_size": record["data_size_bytes"], "storage_units": record["storage_units"],
            "load_cpu": "%.3f" % (float(containers) + float(procs)),
            "load_cpu_containers": containers, "load_cpu_procs": procs}


def load_record_text(load):
    record = {"engine": ENGINE_NAME, "dataset": DATASET, "build": BUILD,
              "engine_version": load["engine_version"],
              "load_time": float(load["load_time_text"]), "data_size": load["data_size"],
              "storage_units": load["storage_units"], "os": OS_NAME, "date": DATE,
              "loaded_by": LOADED_BY, "load_cpu": float(load["load_cpu"]),
              "load_cpu_containers": float(load["load_cpu_containers"]),
              "load_cpu_procs": float(load["load_cpu_procs"])}
    text = json.dumps(record, indent=2) + "\n"
    # jq prints numbers as parsed; keep the 3-decimal harness strings (e.g. no float repr drift).
    for key in ("load_time", "load_cpu", "load_cpu_containers", "load_cpu_procs"):
        value = load["load_time_text"] if key == "load_time" else load[key]
        text = text.replace(f'"{key}": {json.dumps(record[key])}', f'"{key}": {value}')
    return text


def read_trials():
    with gzip.open(RAW / "round1" / "trials.jsonl.gz", "rt") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def read_calls():
    return [json.loads(line) for line in (RAW / "round1-gets" / "calls.jsonl").read_text().splitlines()
            if line.strip()]


def try_gets(trials, calls):
    """[(trial, gets, gets by status label)] of every executed try, from its before/after scrape pair."""
    executed = [t for t in trials if t["status"] in EXECUTED]
    assert len(calls) == 2 * len(executed), (len(calls), len(executed))
    assert [c["seq"] for c in calls] == list(range(1, len(calls) + 1)), "calls.jsonl not contiguous"
    out, previous = [], None
    for i, t in enumerate(executed):
        before, after = calls[2 * i], calls[2 * i + 1]
        assert (before["role"], after["role"]) == ("before", "after"), (t["query"], t["try"])
        assert before["scrape"]["ok"] and after["scrape"]["ok"], (t["query"], t["try"])
        start = before["scrape"]["process_start"]
        assert after["scrape"]["process_start"] == start, (t["query"], t["try"])
        # A cold try runs on a freshly started Loki; its hot tries on the same process.
        assert (start != previous) if t["try"] == 1 else (start == previous), (t["query"], t["try"])
        previous = start
        cpu = float(after["cpu"]) - float(before["cpu"])
        assert abs(cpu - t["engine_cpu_seconds"]) < 1e-6, (t["query"], t["try"], cpu, t["engine_cpu_seconds"])
        b, a = before["scrape"]["getobject"], after["scrape"]["getobject"]
        labels = {k: a.get(k, 0.0) - b.get(k, 0.0) for k in sorted(set(a) | set(b))}
        assert set(labels) <= {"200", "500"} and all(v >= 0 and v == int(v) for v in labels.values()), labels
        labels = {k: int(v) for k, v in labels.items()}
        out.append((t, sum(labels.values()), labels))
    return out


def s3_entry(trials, calls):
    measured = {}
    for t, gets, _ in try_gets(trials, calls):
        measured.setdefault(t["query"], {})[t["try"]] = (t["status"], gets)
    queries = {}
    for q, ts in by_query(trials).items():
        tries = measured[q]
        if all(tries.get(k, (None,))[0] == "ok" for k in range(1, TRIES + 1)):
            queries[q] = [[tries[k][1], None] for k in range(1, TRIES + 1)]
        else:
            # A query with a timed-out try: no GET cell (lower bound of an unfinished try).
            assert any(s == "timeout" for s, _ in tries.values()), q
            queries[q] = [None] * TRIES
    return {"source": GETS_SOURCE, "partial": True, "queries": queries}


def s3_requests_text(entry):
    """s3_requests.json with KEY in the place of SUPERSEDED_KEY (or KEY's own place), the rest unchanged."""
    current = json.loads(S3_REQUESTS.read_text())
    out, placed = {}, False
    for key, value in current.items():
        if key in (KEY, SUPERSEDED_KEY):
            if not placed:
                out[KEY], placed = entry, True
            continue
        out[key] = value
    assert placed, f"{KEY} or {SUPERSEDED_KEY} must be in s3_requests.json"
    return json.dumps(out, indent=1, ensure_ascii=False) + "\n"


def outputs():
    plan = json.loads((RAW / "round1" / "plan.json").read_text())
    assert plan["tries"] == TRIES and plan["cold_only"] is False and plan["skip_hot_after_cold_timeout"] is True, plan
    assert plan["cap_seconds"] == plan["cold_cap_seconds"] == plan["server_query_timeout_seconds"] == CAP, plan
    assert plan["variants"] == [VARIANT] and plan["reverse"] is False and plan["storage"] == "s3", plan
    runtime = json.loads((RAW / "load" / "query-runtime.json").read_text())
    assert (runtime["query_concurrency"], runtime["literal_prefilter"]) == (16, True), runtime
    trials = read_trials()
    assert [t["query"] for t in trials if t["try"] == 1] == plan["queries"], "round 1 trials differ from the plan"
    assert all(t["runtime"]["query_concurrency"] == 16 and t["runtime"]["literal_prefilter"] is True
               for t in trials)
    assert all(t["settings"]["literal_prefilter"] is True and t["settings"]["query_concurrency"] == 16
               for t in trials if t["status"] == "ok")
    slots, load = query_slots(), load_inputs()
    return {results_path(): results_text(slots, trials, load), load_record_path(): load_record_text(load),
            S3_REQUESTS: s3_requests_text(s3_entry(trials, read_calls()))}


def main(argv):
    files = outputs()
    if "--check" in argv:
        stale = [str(p.relative_to(RUN)) for p, text in files.items()
                 if not p.exists() or p.read_text() != text]
        if stale:
            print("differ from to_results.py output: " + ", ".join(stale), file=sys.stderr)
            return 1
        print(f"{len(files)} files match")
        return 0
    for path, text in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        print(f"wrote {path.relative_to(RUN)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
