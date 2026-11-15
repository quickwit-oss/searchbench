#!/usr/bin/env python3
"""Query-only literal-prefilter A/B over an EXISTING completed Loki destination.

Never ingests: no install/load call, refuses a destination without a completed
manifest, a target that would start an ingester, or a changed persisted layout.
For every selected spec, both variants (LOKI_LITERAL_PREFILTER off/on) run the
shared driver's cold/hot/hot tries through the adapter's ./stop ./check ./start
./query scripts. Each variant's cold try follows the lib/benchmark.sh cold_cycle
(stop, wait until ./check fails, sync + drop page cache, start, wait for
./check). The per-try cap matches lib/benchmark.sh (SIGTERM at the cap,
SIGKILL 10s later; remaining tries of that variant are not run). The effective
ceiling of a try is min(cap, the server's limits_config.query_timeout): a try
killed at the cap, or failing at/after the server timeout, is censored
(latency_seconds null, censored_at_seconds = the ceiling); no time is invented
for it. Variant order alternates per query: odd query numbers run off first,
even numbers on first; --reverse swaps that. --queries keeps the given order.

Hot statistics use only hot tries whose native totalLinesProcessed equals the
cold try's (Loki can pick a different shard plan); raw values and mismatching
tries are kept. Geomeans cover the queries where both variants finished.

Every successful try's normalized answer must be identical between variants
(order-insensitive multiset of output rows): any mismatch is a correctness
failure (exit code 2). A persisted layout change after the run is exit code 4.
The deadline stops the run between pairs (never inside one); unfinished pairs
or query errors make the run incomplete (exit code 3). Not a benchmark-results
writer; records evidence only.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

LOKI = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LOKI))

VARIANTS = ("off", "on")
TRIES = 3
TIMING = re.compile(r"^\s*[0-9]+(\.[0-9]+)?\s*$")
EXIT_MISMATCH, EXIT_INCOMPLETE, EXIT_LAYOUT = 2, 3, 4
KILL_GRACE_SECONDS = 10
# Default restart estimate for the deadline before any cold cycle was measured.
DEFAULT_CYCLE_SECONDS = 120
# A failed try this close to the server query timeout is the server timeout.
SERVER_TIMEOUT_SLACK_SECONDS = 2
CENSORED = ("timeout", "server-timeout")


def load_specs(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.startswith("{")]


def query_number(query_id):
    match = re.fullmatch(r"Q(\d+)", query_id)
    if not match:
        raise ValueError(f"unexpected query id {query_id!r}")
    return int(match[1])


def variant_order(query_id, reverse=False):
    """Odd query numbers off-first, even on-first; reversed for round 2."""
    order = ["off", "on"] if query_number(query_id) % 2 else ["on", "off"]
    return order[::-1] if reverse else order


def server_query_timeout_seconds():
    """limits_config.query_timeout of the stock query config, in seconds."""
    from config import make_config
    value = make_config()["limits_config"]["query_timeout"]
    match = re.fullmatch(r"([0-9]+)([smh])", value)
    if not match:
        raise ValueError(f"unexpected query_timeout {value!r}")
    return int(match[1]) * {"s": 1, "m": 60, "h": 3600}[match[2]]


def classify(rc, killed, wall, parsed, limit, server_timeout):
    """(status, censored_at_seconds, timeout_source) of one executed try."""
    ceiling = min(limit, server_timeout)
    if killed:
        return "timeout", ceiling, "driver-cap"
    if rc == 0 and parsed["http_seconds"] is not None:
        return "ok", None, None
    if wall >= server_timeout - SERVER_TIMEOUT_SLACK_SECONDS:
        return "server-timeout", ceiling, "server-query-timeout"
    return "error", None, None


def pair_budget_seconds(cycle_seconds, tries, cap, cold_cap):
    """Worst case of one pair: two cold cycles plus every try killed at its cap."""
    per_variant = cycle_seconds + cold_cap + (tries - 1) * cap + tries * KILL_GRACE_SECONDS
    return 2 * per_variant


def plan(specs, reverse=False):
    return [{"query": spec["id"], "variant": variant, "position": position}
            for spec in specs for position, variant in enumerate(variant_order(spec["id"], reverse), 1)]


def parse_stderr(text):
    """Extract the adapter's tagged stderr lines and the last bare timing line."""
    out = {"settings": None, "stats": None, "rows": None, "http_seconds": None}
    for line in text.replace("\r", "\n").splitlines():
        if line.startswith("LOKI_QUERY_SETTINGS="):
            out["settings"] = json.loads(line.split("=", 1)[1])
        elif line.startswith("LOKI_QUERY_STATS="):
            out["stats"] = json.loads(line.split("=", 1)[1])
        elif line.startswith("SEARCHBENCH_ROWS="):
            out["rows"] = int(line.split("=", 1)[1])
        elif TIMING.match(line):
            out["http_seconds"] = float(line.strip())
    return out


def answer_hashes(stdout):
    """Exact output hash, and the order-insensitive one used for equality.

    Recent-log queries order by timestamp; equal-timestamp rows have no defined
    order, so equality compares the sorted multiset of output rows.
    """
    lines = stdout.splitlines()
    return {"exact_sha256": hashlib.sha256(stdout.encode()).hexdigest(),
            "multiset_sha256": hashlib.sha256("\n".join(sorted(lines)).encode()).hexdigest(),
            "lines": len(lines)}


def native_summary(stats):
    """Selected native fields; full stats are kept in the try record too."""
    if not isinstance(stats, dict):
        return None
    summary = stats.get("summary", {})
    store = stats.get("querier", {}).get("store", {})
    return {key: summary.get(key) for key in (
        "execTime", "queueTime", "totalLinesProcessed", "totalBytesProcessed", "totalEntriesReturned",
        "shards", "splits", "totalPostFilterLines", "totalChunksRef", "totalChunksDownloaded")} | {
        "chunksDownloaded": store.get("totalChunksDownloaded"),
        "ingesterReached": stats.get("ingester", {}).get("totalReached")}


def category(spec):
    """Report categories; specs without any Body filter are 'unfiltered'."""
    if not spec.get("filters"):
        return "unfiltered"
    return {"count": "body-count", "groups": "group-by", "histogram": "histogram", "logs": "logs"}[spec["kind"]]


def geomean(values):
    values = [max(v, 1e-9) for v in values]
    return math.exp(sum(map(math.log, values)) / len(values)) if values else None


def hot_statistics(tries):
    """Best hot over tries whose totalLinesProcessed equals the cold try's.

    A different shard plan can process fewer lines and look faster; such tries
    stay in the raw values and are flagged, but do not set the statistic.
    """
    cold = tries[0]
    cold_lines = (cold.get("native") or {}).get("totalLinesProcessed") if cold["status"] == "ok" else None
    hot_ok = [r for r in tries[1:] if r["status"] == "ok"]
    matched, mismatched = [], []
    for r in hot_ok:
        lines = (r.get("native") or {}).get("totalLinesProcessed")
        (matched if cold_lines is not None and lines == cold_lines else mismatched).append(r)
    return {"hot_raw": [r["latency_seconds"] for r in hot_ok],
            "best_hot_raw": min((r["latency_seconds"] for r in hot_ok), default=None),
            "best_hot": min((r["latency_seconds"] for r in matched), default=None),
            "hot_matched_tries": [r["try"] for r in matched],
            "hot_lines_mismatch_tries": [{"try": r["try"], "lines": (r.get("native") or {}).get("totalLinesProcessed"),
                                          "cold_lines": cold_lines} for r in mismatched]}


def summarize(records, specs_by_id, cap, prefiltered=()):
    """Per-query cold / best-hot HTTP seconds; censored tries have no time.

    Geomeans over queries where both variants finished (per metric); timeout
    counts per variant. Groups: categories, all, and whether 'on' differs.
    """
    rows = {}
    for record in records:
        key = (record["query"], record["variant"])
        rows.setdefault(key, []).append(record)
    per_query = []
    for (query, variant), tries in sorted(rows.items()):
        tries.sort(key=lambda r: r["try"])
        cold = tries[0]
        native = cold.get("native") or {}
        per_query.append({
            "query": query, "variant": variant, "category": category(specs_by_id[query]),
            "latencies": [r["latency_seconds"] for r in tries], "statuses": [r["status"] for r in tries],
            "censored_at_seconds": [r.get("censored_at_seconds") for r in tries],
            "cold": cold["latency_seconds"] if cold["status"] == "ok" else None,
            "cold_status": cold["status"], "cold_censored_at_seconds": cold.get("censored_at_seconds"),
            **hot_statistics(tries),
            "cold_timeout": cold["status"] in CENSORED,
            "timeout": any(r["status"] in CENSORED for r in tries),
            "errors": sum(r["status"] == "error" for r in tries),
            "cold_native_exec": native.get("execTime"),
            "cold_lines_processed": native.get("totalLinesProcessed"),
            "cold_bytes_processed": native.get("totalBytesProcessed"),
            "cold_shards": native.get("shards"), "cold_splits": native.get("splits"),
            "rows": next((r["rows"] for r in tries if r["status"] == "ok"), None),
            "answer_multiset_sha256": sorted({r["answer"]["multiset_sha256"] for r in tries if r["answer"]})})
    by_key = {(r["query"], r["variant"]): r for r in per_query}
    queries = sorted({q for q, _ in by_key if all((q, v) in by_key for v in VARIANTS)})
    geomeans = {}
    groups = {g: [q for q in queries if by_key[(q, "off")]["category"] == g]
              for g in sorted({by_key[(q, "off")]["category"] for q in queries})}
    groups["all"] = queries
    groups["prefiltered"] = [q for q in queries if q in prefiltered]
    groups["not-prefiltered"] = [q for q in queries if q not in prefiltered]
    for group, members in groups.items():
        cold_timeouts = {v: [q for q in members if by_key[(q, v)]["cold_timeout"]] for v in VARIANTS}
        result = {"queries": len(members), "cap_seconds": cap,
                  "timeouts": {v: sum(by_key[(q, v)]["timeout"] for q in members) for v in VARIANTS},
                  "cold_timeouts": {v: len(cold_timeouts[v]) for v in VARIANTS},
                  "errors": {v: sum(by_key[(q, v)]["errors"] > 0 for q in members) for v in VARIANTS},
                  "cold_timeout_both": sorted(set(cold_timeouts["off"]) & set(cold_timeouts["on"])),
                  "cold_timeout_only_off": sorted(set(cold_timeouts["off"]) - set(cold_timeouts["on"])),
                  "cold_timeout_only_on": sorted(set(cold_timeouts["on"]) - set(cold_timeouts["off"]))}
        for metric in ("cold", "best_hot", "best_hot_raw"):
            usable = [q for q in members if all(by_key[(q, v)][metric] is not None for v in VARIANTS)]
            for variant in VARIANTS:
                result[f"{variant}_{metric}_geomean"] = geomean([by_key[(q, variant)][metric] for q in usable])
            off, on = result[f"off_{metric}_geomean"], result[f"on_{metric}_geomean"]
            result[f"{metric}_ratio_on_over_off"] = on / off if off and on else None
            result[f"{metric}_queries"] = len(usable)
        geomeans[group] = result
    return per_query, geomeans


def equality(records):
    """Every successful try of both variants must yield one identical answer."""
    by_query = {}
    for record in records:
        by_query.setdefault(record["query"], {v: set() for v in VARIANTS})
        if record["status"] == "ok":
            by_query[record["query"]][record["variant"]].add(record["answer"]["multiset_sha256"])
    verdicts = {}
    for query, hashes in sorted(by_query.items()):
        if not hashes["off"] or not hashes["on"]:
            verdicts[query] = "unverified-no-successful-pair"
        elif len(hashes["off"] | hashes["on"]) == 1:
            verdicts[query] = "identical"
        else:
            verdicts[query] = "MISMATCH"
    return verdicts


class Engine:
    """lib/benchmark.sh lifecycle via the adapter scripts in --engine-dir."""

    def __init__(self, engine_dir, log_dir, env, allow_no_cache_drop):
        self.dir, self.log_dir, self.env = Path(engine_dir), log_dir, env
        self.allow_no_cache_drop = allow_no_cache_drop
        self.cycles = 0
        self.cycle_records = []

    def script(self, name, stdin=None, timeout=None, env=None):
        return subprocess.run([str(self.dir / name)], cwd=self.dir, env=env or self.env, input=stdin,
                              capture_output=True, text=True, timeout=timeout)

    def check(self):
        try:
            return self.script("check", timeout=30).returncode == 0
        except subprocess.TimeoutExpired:
            return False

    def flush_caches(self):
        """sync + drop_caches as root, or via non-interactive sudo, like benchmark.sh."""
        subprocess.run(["sync"], check=True)
        drop = Path("/proc/sys/vm/drop_caches")
        if drop.exists() and os.access(drop, os.W_OK):
            drop.write_text("3\n")
            return "dropped"
        if drop.exists() and subprocess.run(["sudo", "-n", "true"], capture_output=True).returncode == 0:
            subprocess.run(["sudo", "-n", "tee", str(drop)], input="3\n", text=True, check=True,
                           capture_output=True)
            return "dropped-sudo"
        if not self.allow_no_cache_drop:
            raise RuntimeError("cannot drop the page cache; cold tries would run hot")
        return "unavailable-allowed"

    def variant_env(self, variant):
        return {**self.env, "LOKI_LITERAL_PREFILTER": variant}

    def cold_cycle(self, variant):
        """Restart under the variant's env so runtime.json records its prefilter setting."""
        started = time.perf_counter()
        self.cycles += 1
        log = self.log_dir / f"cycle-{self.cycles:04}.log"
        stop = self.script("stop", timeout=300)
        for _ in range(60):
            if not self.check():
                break
            time.sleep(1)
        else:
            raise RuntimeError("engine did not stop within 60s")
        cache = self.flush_caches()
        start = self.script("start", timeout=300, env=self.variant_env(variant))
        log.write_text(f"stop rc={stop.returncode}\n{stop.stdout}{stop.stderr}\n"
                       f"start rc={start.returncode}\n{start.stdout}{start.stderr}\n")
        for _ in range(60):
            if self.check():
                break
            time.sleep(1)
        else:
            raise RuntimeError(f"./check did not succeed within 60s; see {log}")
        record = {"cycle": self.cycles, "stop_rc": stop.returncode, "start_rc": start.returncode,
                  "cache": cache, "seconds": time.perf_counter() - started}
        self.cycle_records.append(record)
        return record

    def query(self, spec_line, variant, cap):
        env = self.variant_env(variant)
        before = time.perf_counter()
        # Own process group: like timeout(1), the cap signals the whole group.
        process = subprocess.Popen([str(self.dir / "query")], cwd=self.dir, env=env, text=True,
                                   start_new_session=True,
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            stdout, stderr = process.communicate(spec_line + "\n", timeout=cap)
            return process.returncode, stdout, stderr, time.perf_counter() - before, False
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                stdout, stderr = process.communicate(timeout=KILL_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                stdout, stderr = process.communicate()
            return process.returncode, stdout, stderr, time.perf_counter() - before, True

    def cpu(self):
        if not (self.dir / "cpu-usage").is_file():
            return None
        result = self.script("cpu-usage", timeout=30)
        try:
            return float(result.stdout.strip())
        except ValueError:
            return None


def validate_destination():
    """Completed manifest + receipt; persisted inventory unchanged. Never loads."""
    import adapter
    from config import settings
    s = settings()
    if s["target"] != "read":
        raise ValueError("refusing: destination has no completed manifest, or LOKI_TARGET would start an ingester")
    manifest = adapter.read_manifest()
    if manifest.get("query_store_only") is not True or "persisted_inventory" not in manifest:
        raise ValueError("refusing: manifest is not a completed store-only load")
    inventory = adapter.object_inventory()
    if inventory != manifest["persisted_inventory"]:
        raise ValueError("refusing: persisted objects differ from the completed manifest")
    return manifest, inventory, s


def request_evidence(specs, manifest):
    from logql import build_request
    rows = []
    for spec in specs:
        off = build_request(spec, manifest, literal_prefilter=False)
        on = build_request(spec, manifest, literal_prefilter=True)
        rows.append({"query": spec["id"], "off": off, "on": on, "prefiltered": off != on})
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path, help="fresh output directory")
    parser.add_argument("--engine-dir", type=Path, default=LOKI, help="dir with start/stop/check/query scripts")
    parser.add_argument("--queries-file", type=Path, default=LOKI / "queries.logql")
    parser.add_argument("--queries", default="all", help="'all' or comma-separated ids, e.g. Q01,Q04")
    parser.add_argument("--reverse", action="store_true", help="round 2: off-first for even, on-first for odd")
    parser.add_argument("--round", type=int, default=1)
    parser.add_argument("--deadline-epoch", type=float,
                        help="stop before a pair that might not finish before this time; never inside a pair")
    parser.add_argument("--pair-budget-seconds", type=float, default=0,
                        help="floor for the computed worst-case pair duration used against the deadline")
    parser.add_argument("--cap-seconds", type=int,
                        help="diagnostic per-try cap overriding SEARCHBENCH_QUERY_TIMEOUT/COLD_TIMEOUT")
    parser.add_argument("--cold-only", action="store_true", help="one cold try per variant, no hot tries")
    parser.add_argument("--skip-hot-after-cold-timeout", action="store_true",
                        help="record hot tries after a cold timeout as skipped (both variants)")
    parser.add_argument("--allow-no-cache-drop", action="store_true", help="local dev only (no /proc)")
    args = parser.parse_args(argv)
    if "LOKI_LITERAL_PREFILTER" in os.environ:
        raise ValueError("unset LOKI_LITERAL_PREFILTER: the driver sets it per variant")
    if os.environ.get("SEARCHBENCH_TRIES", str(TRIES)) != str(TRIES):
        raise ValueError("cold/hot/hot: SEARCHBENCH_TRIES must be 3")
    if args.cap_seconds is not None:
        cap = cold_cap = args.cap_seconds
        cap_source = "--cap-seconds"
    else:
        cap = int(os.environ.get("SEARCHBENCH_QUERY_TIMEOUT", "60"))
        cold_cap = int(os.environ.get("SEARCHBENCH_COLD_TIMEOUT", cap))
        cap_source = "SEARCHBENCH_QUERY_TIMEOUT/SEARCHBENCH_COLD_TIMEOUT"
    if cap <= 0 or cold_cap <= 0:
        raise ValueError("query caps must be positive")
    tries = 1 if args.cold_only else TRIES
    env = dict(os.environ)
    manifest, inventory, s = validate_destination()
    specs = load_specs(args.queries_file)
    if args.queries != "all":
        wanted = args.queries.split(",")
        unknown = set(wanted) - {spec["id"] for spec in specs}
        if unknown or len(set(wanted)) != len(wanted):
            raise ValueError(f"unknown or duplicate query ids: {sorted(unknown)}")
        by_id = {spec["id"]: spec for spec in specs}
        specs = [by_id[query] for query in wanted]
    specs_by_id = {spec["id"]: spec for spec in specs}
    args.out.mkdir(parents=True, exist_ok=False)
    out = args.out.resolve()
    (out / "cycles").mkdir()
    (out / "answers").mkdir()

    def save(name, data):
        (out / name).write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")

    steps = plan(specs, args.reverse)
    server_timeout = server_query_timeout_seconds()
    save("plan.json", {"round": args.round, "reverse": args.reverse, "queries": [s["id"] for s in specs],
                       "steps": steps, "tries": tries, "cold_only": args.cold_only,
                       "skip_hot_after_cold_timeout": args.skip_hot_after_cold_timeout,
                       "cap_seconds": cap, "cold_cap_seconds": cold_cap, "cap_source": cap_source,
                       "server_query_timeout_seconds": server_timeout,
                       "effective_ceiling_seconds": min(cap, server_timeout),
                       "pair_budget_floor_seconds": args.pair_budget_seconds,
                       "data_dir": s["data_dir"], "storage": s["storage"], "container": s["container"],
                       "image": s["image"], "destination_id": manifest["destination_id"],
                       "manifest_rows": manifest["rows"], "inventory_before": inventory,
                       "deadline_epoch": args.deadline_epoch, "started_epoch": time.time(),
                       "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                       "queries_sha256": hashlib.sha256((LOKI / "logql.py").read_bytes()).hexdigest()})
    request_rows = request_evidence(specs, manifest)
    save("requests.json", request_rows)
    prefiltered = {row["query"] for row in request_rows if row["prefiltered"]}
    engine = Engine(args.engine_dir, out / "cycles", env, args.allow_no_cache_drop)
    records, unfinished, deadline_stop = [], [], None
    lines = {spec["id"]: json.dumps(spec, separators=(",", ":")) for spec in specs}
    with (out / "trials.jsonl").open("w", buffering=1) as trials:
        for index in range(0, len(steps), 2):
            query = steps[index]["query"]
            cycle_estimate = max([c["seconds"] for c in engine.cycle_records] or [DEFAULT_CYCLE_SECONDS])
            budget = max(args.pair_budget_seconds, pair_budget_seconds(cycle_estimate, tries, cap, cold_cap))
            if args.deadline_epoch and time.time() + budget > args.deadline_epoch:
                unfinished = [steps[i]["query"] for i in range(index, len(steps), 2)]
                deadline_stop = {"epoch": time.time(), "deadline_epoch": args.deadline_epoch,
                                 "pair_budget_seconds": budget, "cycle_estimate_seconds": cycle_estimate,
                                 "next_query": query}
                print(f"[deadline] stopping before {query}; unfinished {unfinished}", flush=True)
                break
            for step in steps[index:index + 2]:
                variant = step["variant"]
                cycle = engine.cold_cycle(variant)
                runtime = json.loads((Path(s["data_dir"]) / "runtime.json").read_text())
                if "literal_prefilter" in runtime and runtime["literal_prefilter"] is not (variant == "on"):
                    raise AssertionError(f"{query}-{variant}: runtime.json literal_prefilter does not match variant")
                stopped = None  # status for tries after a timeout: they are not run
                for attempt in range(1, tries + 1):
                    limit = cold_cap if attempt == 1 else cap
                    record = {"round": args.round, "query": query, "variant": variant,
                              "position": step["position"], "try": attempt,
                              "kind": "cold" if attempt == 1 else "hot",
                              "cycle": cycle if attempt == 1 else None, "runtime": runtime,
                              "cap_seconds": limit, "cap_source": cap_source,
                              "server_query_timeout_seconds": server_timeout,
                              "effective_ceiling_seconds": min(limit, server_timeout)}
                    if stopped:
                        record.update(status=stopped, latency_seconds=None, censored_at_seconds=None,
                                      answer=None, native=None, settings=None)
                    else:
                        cpu0 = engine.cpu()
                        rc, stdout, stderr, wall, killed = engine.query(lines[query], variant, limit)
                        cpu1 = engine.cpu()
                        parsed = parse_stderr(stderr)
                        name = f"{query}-{variant}-r{args.round}-t{attempt}"
                        (out / "answers" / f"{name}.stdout").write_text(stdout)
                        status, censored_at, source = classify(rc, killed, wall, parsed, limit, server_timeout)
                        if status in CENSORED:
                            stopped = ("skipped-after-cold-timeout"
                                       if attempt == 1 and args.skip_hot_after_cold_timeout
                                       else "not-run-after-timeout")
                        if status == "ok" and (parsed["settings"] or {}).get("literal_prefilter") != (variant == "on"):
                            raise AssertionError(f"{name}: LOKI_QUERY_SETTINGS does not match variant")
                        record.update(status=status,
                                      latency_seconds=parsed["http_seconds"] if status == "ok" else None,
                                      censored_at_seconds=censored_at, timeout_source=source, rc=rc,
                                      process_wall_seconds=wall,
                                      http_seconds=parsed["http_seconds"], rows=parsed["rows"],
                                      settings=parsed["settings"], stats=parsed["stats"],
                                      native=native_summary(parsed["stats"]),
                                      engine_cpu_seconds=None if cpu0 is None or cpu1 is None else cpu1 - cpu0,
                                      answer=answer_hashes(stdout) if status == "ok" else None,
                                      stderr_tail=stderr[-2000:])
                    trials.write(json.dumps(record, ensure_ascii=False) + "\n")
                    records.append(record)
            verdict = equality([r for r in records if r["query"] == query])[query]
            print(f"[pair] {query} {verdict}", flush=True)
    engine.script("stop", timeout=300)
    import adapter
    validate_after = adapter.object_inventory()
    per_query, geomeans = summarize(records, specs_by_id, cap, prefiltered)
    verdicts = equality(records)
    mismatches = sorted(q for q, v in verdicts.items() if v == "MISMATCH")
    save("summary.json", per_query)
    save("geomeans.json", geomeans)
    errors = sorted({f"{r['query']}-{r['variant']}-t{r['try']}" for r in records if r["status"] == "error"})
    save("equality.json", {"verdicts": verdicts, "mismatches": mismatches, "unfinished_by_deadline": unfinished,
                           "deadline_stop": deadline_stop, "errors": errors,
                           "all_identical": not mismatches and all(v == "identical" for v in verdicts.values()),
                           "inventory_after": validate_after, "inventory_unchanged": validate_after == inventory,
                           "finished_epoch": time.time(), "cold_cycles": engine.cycles})
    print(json.dumps({"pairs": len(verdicts), "mismatches": mismatches, "unfinished": unfinished, "errors": errors,
                      "inventory_unchanged": validate_after == inventory,
                      "geomeans_all": geomeans.get("all")}), flush=True)
    if mismatches:
        return EXIT_MISMATCH
    if validate_after != inventory:
        return EXIT_LAYOUT
    if unfinished or errors:
        return EXIT_INCOMPLETE
    return 0


if __name__ == "__main__":
    sys.exit(main())
