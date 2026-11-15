"""Query-only prefilter A/B driver: ordering, refusal, timeouts, mismatches, native run."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE / "experiments"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import query_pair  # noqa: E402

DRIVER = HERE / "experiments/query_pair.py"
SPECS = query_pair.load_specs(HERE / "queries.logql")
T = 1758585600000000000


def write_script(path, body):
    path.write_text("#!/usr/bin/env bash\nset -euo pipefail\n" + body + "\n")
    path.chmod(0o755)


def clean_env(**extra):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("LOKI_", "SEARCHBENCH_"))}
    env.update(extra)
    return env


class PlanAndSummaryTests(unittest.TestCase):
    def test_variant_order_alternates_by_query_number_and_reverses(self):
        self.assertEqual(len(SPECS), 56)
        steps = query_pair.plan(SPECS)
        self.assertEqual(len(steps), 112)
        for first, second in zip(steps[::2], steps[1::2]):
            self.assertEqual(first["query"], second["query"])
            self.assertEqual((first["position"], second["position"]), (1, 2))
            odd = query_pair.query_number(first["query"]) % 2 == 1
            self.assertEqual([first["variant"], second["variant"]], ["off", "on"] if odd else ["on", "off"])
        reversed_steps = query_pair.plan(SPECS, reverse=True)
        for a, b in zip(steps, reversed_steps):
            self.assertEqual(a["query"], b["query"])
            self.assertNotEqual(a["variant"], b["variant"])
        with self.assertRaises(ValueError):
            query_pair.query_number("plain-count")

    def test_parse_stderr_and_answer_hash(self):
        stderr = ('LOKI_QUERY_SETTINGS={"line_encoding":"x","literal_prefilter":true,"query_concurrency":8}\n'
                  'LOKI_QUERY_STATS={"summary":{"execTime":0.5,"totalLinesProcessed":7,"totalBytesProcessed":70,'
                  '"shards":2,"splits":3},"ingester":{"totalReached":0},"querier":{"store":{"totalChunksDownloaded":4}}}\n'
                  "SEARCHBENCH_ROWS=2\n0.123456\n")
        parsed = query_pair.parse_stderr(stderr)
        self.assertEqual(parsed["http_seconds"], 0.123456)
        self.assertEqual(parsed["rows"], 2)
        self.assertTrue(parsed["settings"]["literal_prefilter"])
        native = query_pair.native_summary(parsed["stats"])
        self.assertEqual((native["execTime"], native["totalLinesProcessed"], native["totalBytesProcessed"],
                          native["shards"], native["splits"], native["chunksDownloaded"], native["ingesterReached"]),
                         (0.5, 7, 70, 2, 3, 4, 0))
        a, b = query_pair.answer_hashes('{"a":1}\n{"a":2}\n'), query_pair.answer_hashes('{"a":2}\n{"a":1}\n')
        self.assertNotEqual(a["exact_sha256"], b["exact_sha256"])
        self.assertEqual(a["multiset_sha256"], b["multiset_sha256"])
        self.assertNotEqual(a["multiset_sha256"], query_pair.answer_hashes('{"a":1}\n')["multiset_sha256"])
        self.assertNotEqual(a["multiset_sha256"], query_pair.answer_hashes('{"a":1}\n{"a":1}\n{"a":2}\n')["multiset_sha256"])

    def test_equality_and_geomeans(self):
        def record(query, variant, attempt, status, latency, answer="h"):
            return {"query": query, "variant": variant, "try": attempt, "status": status,
                    "latency_seconds": latency, "rows": 1,
                    "answer": {"multiset_sha256": answer} if status == "ok" else None,
                    "native": {"execTime": latency, "totalLinesProcessed": 10, "totalBytesProcessed": 100}}
        records = []
        for attempt, (off, on) in enumerate([(4., 2.), (2., 1.), (1., .5)], 1):
            records += [record("Q01", "off", attempt, "ok", off), record("Q01", "on", attempt, "ok", on)]
            records += [record("Q73", "off", attempt, "ok", 1.), record("Q73", "on", attempt, "ok", 1., "other")]
        records += [dict(record("Q61", "off", 1, "timeout", None), censored_at_seconds=60),
                    record("Q61", "off", 2, "not-run-after-timeout", None),
                    record("Q61", "off", 3, "not-run-after-timeout", None)]
        records += [record("Q61", "on", n, "ok", 2.) for n in (1, 2, 3)]
        verdicts = query_pair.equality(records)
        self.assertEqual(verdicts, {"Q01": "identical", "Q61": "unverified-no-successful-pair", "Q73": "MISMATCH"})
        specs = {s["id"]: s for s in SPECS}
        per_query, geomeans = query_pair.summarize(records, specs, 60, {"Q01"})
        by_key = {(r["query"], r["variant"]): r for r in per_query}
        self.assertEqual((by_key[("Q01", "off")]["cold"], by_key[("Q01", "off")]["best_hot"]), (4., 1.))
        self.assertTrue(by_key[("Q61", "off")]["timeout"])
        # Censored: no invented time, only the ceiling it reached.
        self.assertEqual((by_key[("Q61", "off")]["cold"], by_key[("Q61", "off")]["best_hot"]), (None, None))
        self.assertEqual(by_key[("Q61", "off")]["cold_censored_at_seconds"], 60)
        self.assertAlmostEqual(geomeans["body-count"]["cold_ratio_on_over_off"], .5)
        self.assertEqual(geomeans["histogram"]["timeouts"], {"off": 1, "on": 0})
        self.assertEqual(geomeans["histogram"]["cold_timeout_only_off"], ["Q61"])
        self.assertEqual(geomeans["histogram"]["cold_queries"], 0)
        self.assertIsNone(geomeans["histogram"]["cold_ratio_on_over_off"])
        self.assertEqual(geomeans["all"]["cold_queries"], 2)
        self.assertEqual(query_pair.category(specs["Q73"]), "unfiltered")
        self.assertEqual(query_pair.category(specs["Q72"]), "logs")
        self.assertEqual(geomeans["unfiltered"]["queries"], 1)
        self.assertNotIn("logs", geomeans)
        self.assertEqual(geomeans["all"]["queries"], 3)
        self.assertEqual(geomeans["prefiltered"]["queries"], 1)
        self.assertEqual(geomeans["not-prefiltered"]["queries"], 2)
        self.assertEqual({query_pair.category(s) for s in SPECS},
                         {"body-count", "group-by", "histogram", "logs", "unfiltered"})


    def test_hot_statistic_only_over_tries_matching_cold_lines(self):
        def record(attempt, latency, lines, status="ok"):
            return {"query": "Q75", "variant": "off", "try": attempt, "status": status, "latency_seconds": latency,
                    "rows": 1, "answer": {"multiset_sha256": "h"}, "native": {"totalLinesProcessed": lines}}
        stats = query_pair.hot_statistics([record(1, 2.5, 753176), record(2, 2.4, 753176), record(3, 0.97, 385091)])
        self.assertEqual(stats["best_hot"], 2.4)
        self.assertEqual(stats["best_hot_raw"], 0.97)
        self.assertEqual(stats["hot_raw"], [2.4, 0.97])
        self.assertEqual(stats["hot_matched_tries"], [2])
        self.assertEqual(stats["hot_lines_mismatch_tries"], [{"try": 3, "lines": 385091, "cold_lines": 753176}])
        # No successful cold try: nothing to match against.
        stats = query_pair.hot_statistics([record(1, None, None, "timeout"), record(2, 1.0, 5)])
        self.assertIsNone(stats["best_hot"])
        self.assertEqual(stats["best_hot_raw"], 1.0)

    def test_classify_censors_driver_cap_and_server_timeout(self):
        ok = {"http_seconds": 1.5}
        failed = {"http_seconds": None}
        self.assertEqual(query_pair.classify(0, False, 2.0, ok, 60, 600), ("ok", None, None))
        self.assertEqual(query_pair.classify(-15, True, 60.1, failed, 60, 600), ("timeout", 60, "driver-cap"))
        self.assertEqual(query_pair.classify(-15, True, 600.1, failed, 900, 600), ("timeout", 600, "driver-cap"))
        self.assertEqual(query_pair.classify(1, False, 599.5, failed, 900, 600),
                         ("server-timeout", 600, "server-query-timeout"))
        self.assertEqual(query_pair.classify(1, False, 30.0, failed, 900, 600), ("error", None, None))
        with patch.dict(os.environ, clean_env(), clear=True):
            self.assertEqual(query_pair.server_query_timeout_seconds(), 600)

    def test_pair_budget_is_worst_case_of_both_variants(self):
        self.assertEqual(query_pair.pair_budget_seconds(5, 3, 60, 60), 2 * (5 + 60 + 2 * 60 + 3 * 10))
        self.assertEqual(query_pair.pair_budget_seconds(5, 1, 600, 600), 2 * (5 + 600 + 10))


class FakeEngineTests(unittest.TestCase):
    """Script lifecycle, refusal, timeout padding and mismatch detection, no engine."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="query-pair-fake-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.data = self.root / "data"
        self.engine = self.root / "engine"
        self.engine.mkdir()
        state = self.root / "state"
        # Like adapter.start: runtime.json records the start env's prefilter (default on).
        write_script(self.engine / "start", f'touch {state}; echo start >> {self.root}/events\n'
                     'on=true; [[ "${LOKI_LITERAL_PREFILTER:-on}" == off ]] && on=false\n'
                     f'printf \'{{"query_concurrency":8,"literal_prefilter":%s}}\' "$on" > "$LOKI_DATA_DIR/runtime.json"')
        write_script(self.engine / "stop", f'rm -f {state}; echo stop >> {self.root}/events')
        write_script(self.engine / "check", f'test -f {state}')
        write_script(self.engine / "query", r'''
spec=$(cat)
echo "query $LOKI_LITERAL_PREFILTER $spec" >> ''' + str(self.root) + r'''/events
on=false; [[ "$LOKI_LITERAL_PREFILTER" == on ]] && on=true
case "$spec" in
  *'"Q03"'*) sleep 30 ;;  # grandchild: the cap must signal the process group
  *'"Q05"'*) sleep 1; echo '{"count":5}' ;;
  *'"Q02"'*) [[ $on == true ]] && echo '{"count":2}' || echo '{"count":1}' ;;
  *) echo '{"count":5}' ;;
esac
echo "LOKI_QUERY_SETTINGS={\"literal_prefilter\":$on,\"query_concurrency\":8}" >&2
echo 'LOKI_QUERY_STATS={"summary":{"execTime":0.01,"totalLinesProcessed":3,"totalBytesProcessed":30}}' >&2
echo SEARCHBENCH_ROWS=1 >&2
echo 0.010000 >&2''')
        self.specs = self.root / "queries.logql"
        self.specs.write_text("".join(json.dumps({"id": q, "kind": "count", "filters": []}) + "\n"
                                      for q in ("Q01", "Q02", "Q03", "Q05")))
        self.env = clean_env(LOKI_STORAGE="filesystem", LOKI_DATA_DIR=str(self.data),
                             SEARCHBENCH_QUERY_TIMEOUT="2")

    def complete_destination(self):
        import adapter
        with patch.dict(os.environ, self.env, clear=True):
            self.data.mkdir()
            receipt = adapter.claim_load(self.data, "corpus", [])
            (self.data / "chunks").mkdir()
            (self.data / "chunks/one").write_bytes(b"persisted")
            (self.data / "manifest.json").write_text(json.dumps({
                **receipt, "rows": 5, "min_timestamp_ns": T, "max_timestamp_ns": T + 10, "query_store_only": True,
                "persisted_inventory": adapter.object_inventory()}))

    def run_driver(self, *args, env=None):
        return subprocess.run([sys.executable, str(DRIVER), "--engine-dir", str(self.engine),
                               "--queries-file", str(self.specs), "--allow-no-cache-drop", *args],
                              env=env or self.env, capture_output=True, text=True, timeout=180)

    def test_refuses_without_completed_load_and_never_ingests(self):
        result = self.run_driver("--out", str(self.root / "out"))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("refusing", result.stderr)
        self.assertFalse((self.root / "out").exists())
        self.assertFalse((self.root / "events").exists())
        self.complete_destination()
        for env in ({**self.env, "LOKI_TARGET": "all"}, {**self.env, "LOKI_LITERAL_PREFILTER": "on"},
                    {**self.env, "SEARCHBENCH_TRIES": "1"}):
            result = self.run_driver("--out", str(self.root / "out"), env=env)
            self.assertNotEqual(result.returncode, 0, result.stderr)
        (self.data / "chunks/two").write_bytes(b"unexpected")
        result = self.run_driver("--out", str(self.root / "out"))
        self.assertIn("persisted objects differ", result.stderr)
        self.assertFalse((self.root / "events").exists())

    def test_cold_cycles_timeouts_padding_and_mismatch(self):
        self.complete_destination()
        out = self.root / "out"
        result = self.run_driver("--out", str(out), "--queries", "Q01,Q02,Q03")
        self.assertEqual(result.returncode, query_pair.EXIT_MISMATCH, result.stderr)
        equality = json.loads((out / "equality.json").read_text())
        self.assertEqual(equality["verdicts"], {"Q01": "identical", "Q02": "MISMATCH",
                                                "Q03": "unverified-no-successful-pair"})
        self.assertEqual(equality["mismatches"], ["Q02"])
        self.assertTrue(equality["inventory_unchanged"])
        trials = [json.loads(line) for line in (out / "trials.jsonl").read_text().splitlines()]
        self.assertEqual(len(trials), 3 * 2 * 3)
        order = [(t["query"], t["variant"]) for t in trials if t["try"] == 1]
        self.assertEqual(order, [("Q01", "off"), ("Q01", "on"), ("Q02", "on"), ("Q02", "off"),
                                 ("Q03", "off"), ("Q03", "on")])
        q03 = [t for t in trials if t["query"] == "Q03"]
        self.assertEqual([t["status"] for t in q03], ["timeout", "not-run-after-timeout", "not-run-after-timeout"] * 2)
        self.assertEqual([t["latency_seconds"] for t in q03], [None] * 6)
        self.assertEqual([t["censored_at_seconds"] for t in q03], [2, None, None] * 2)
        self.assertTrue(all(t["cap_seconds"] == 2 and t["effective_ceiling_seconds"] == 2 for t in trials))
        events = (self.root / "events").read_text().splitlines()
        # Every variant: stop, start, then exactly one query per non-padded try.
        self.assertEqual(sum(e == "start" for e in events), 6)
        self.assertEqual(sum(e.startswith("query") for e in events), 2 * 2 * 3 + 2)
        for t in trials:
            if t["status"] == "ok":
                self.assertEqual(t["settings"]["literal_prefilter"], t["variant"] == "on")
                self.assertEqual(t["runtime"], {"query_concurrency": 8, "literal_prefilter": t["variant"] == "on"})
                self.assertEqual(t["latency_seconds"], 0.01)
            if t["try"] == 1:
                # Root/sudo hosts really drop; elsewhere the flag permits skipping.
                self.assertIn(t["cycle"]["cache"], ("dropped", "dropped-sudo", "unavailable-allowed"))
        self.assertTrue((out / "answers/Q02-on-r1-t1.stdout").is_file())

    def test_runtime_json_must_match_variant_even_before_a_timeout(self):
        # A server that ignores the variant env (always on) must stop the run at the
        # first off variant, before any query: timed-out tries have no settings to check.
        write_script(self.engine / "start", f'touch {self.root}/state\n'
                     'printf \'{"query_concurrency":8,"literal_prefilter":true}\' > "$LOKI_DATA_DIR/runtime.json"')
        self.complete_destination()
        result = self.run_driver("--out", str(self.root / "out"), "--queries", "Q03")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("runtime.json literal_prefilter does not match variant", result.stderr)
        self.assertFalse((self.root / "events").exists() and
                         any(e.startswith("query") for e in (self.root / "events").read_text().splitlines()))

    def test_round_two_subset_reversed_and_deadline(self):
        self.complete_destination()
        out = self.root / "r2"
        result = self.run_driver("--out", str(out), "--queries", "Q01,Q02", "--reverse", "--round", "2")
        self.assertEqual(result.returncode, query_pair.EXIT_MISMATCH, result.stderr)
        trials = [json.loads(line) for line in (out / "trials.jsonl").read_text().splitlines()]
        self.assertEqual([(t["query"], t["variant"]) for t in trials if t["try"] == 1],
                         [("Q01", "on"), ("Q01", "off"), ("Q02", "off"), ("Q02", "on")])
        self.assertTrue(all(t["round"] == 2 for t in trials))
        import time
        result = self.run_driver("--out", str(self.root / "late"), "--queries", "Q01",
                                 "--deadline-epoch", str(time.time() + 5))
        self.assertEqual(result.returncode, query_pair.EXIT_INCOMPLETE, result.stderr)
        equality = json.loads((self.root / "late/equality.json").read_text())
        self.assertEqual(equality["unfinished_by_deadline"], ["Q01"])
        self.assertEqual(equality["deadline_stop"]["next_query"], "Q01")
        self.assertFalse((self.root / "late/trials.jsonl").read_text())
        self.assertNotEqual(self.run_driver("--out", str(self.root / "x"), "--queries", "Q01,Q99").returncode, 0)

    def test_skip_hot_after_cold_timeout_cap_override_and_query_order(self):
        self.complete_destination()
        out = self.root / "skip"
        result = self.run_driver("--out", str(out), "--queries", "Q03,Q01", "--cap-seconds", "1",
                                 "--skip-hot-after-cold-timeout")
        self.assertEqual(result.returncode, 0, result.stderr)
        trials = [json.loads(line) for line in (out / "trials.jsonl").read_text().splitlines()]
        self.assertEqual([(t["query"], t["variant"]) for t in trials if t["try"] == 1],
                         [("Q03", "off"), ("Q03", "on"), ("Q01", "off"), ("Q01", "on")])
        q03 = [t for t in trials if t["query"] == "Q03"]
        self.assertEqual([t["status"] for t in q03],
                         ["timeout", "skipped-after-cold-timeout", "skipped-after-cold-timeout"] * 2)
        self.assertEqual([t["censored_at_seconds"] for t in q03], [1, None, None] * 2)
        self.assertTrue(all(t["cap_seconds"] == 1 and t["cap_source"] == "--cap-seconds" for t in trials))
        events = (self.root / "events").read_text().splitlines()
        self.assertEqual(sum(e.startswith("query") and '"Q03"' in e for e in events), 2)
        plan = json.loads((out / "plan.json").read_text())
        self.assertEqual((plan["cap_seconds"], plan["server_query_timeout_seconds"],
                          plan["effective_ceiling_seconds"]), (1, 600, 1))
        geomeans = json.loads((out / "geomeans.json").read_text())
        self.assertEqual(geomeans["all"]["cold_timeout_both"], ["Q03"])
        self.assertEqual(geomeans["all"]["cold_timeouts"], {"off": 1, "on": 1})
        self.assertEqual(geomeans["all"]["cold_queries"], 1)
        summary = {(r["query"], r["variant"]): r for r in json.loads((out / "summary.json").read_text())}
        self.assertEqual((summary[("Q03", "off")]["cold"], summary[("Q03", "off")]["cold_censored_at_seconds"]),
                         (None, 1))

    def test_cold_only_deadline_stops_between_pairs(self):
        self.complete_destination()
        out = self.root / "cold"
        argv = ["--out", str(out), "--engine-dir", str(self.engine), "--queries-file", str(self.specs),
                "--allow-no-cache-drop", "--queries", "Q05,Q01", "--cold-only", "--cap-seconds", "2"]
        import time
        # Budget per pair = 2 * (cycle + 2 + 10) s; Q05 takes >= 2 s, so the
        # deadline admits Q05 and then stops before Q01, never inside a pair.
        with patch.dict(os.environ, self.env, clear=True), patch.object(query_pair, "DEFAULT_CYCLE_SECONDS", 0):
            rc = query_pair.main(argv + ["--deadline-epoch", str(time.time() + 25.5)])
        self.assertEqual(rc, query_pair.EXIT_INCOMPLETE)
        trials = [json.loads(line) for line in (out / "trials.jsonl").read_text().splitlines()]
        self.assertEqual([(t["query"], t["variant"], t["try"]) for t in trials], [("Q05", "off", 1), ("Q05", "on", 1)])
        equality = json.loads((out / "equality.json").read_text())
        self.assertEqual(equality["unfinished_by_deadline"], ["Q01"])
        self.assertEqual(equality["verdicts"], {"Q05": "identical"})
        self.assertEqual(json.loads((out / "plan.json").read_text())["tries"], 1)
        summary = json.loads((out / "summary.json").read_text())
        self.assertTrue(all(r["best_hot"] is None and r["cold"] == 0.01 for r in summary))


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@unittest.skipUnless(os.environ.get("LOKI_TEST_BINARY") and os.environ.get("LOKI_TEST_CLIENT"),
                     "set LOKI_TEST_BINARY and LOKI_TEST_CLIENT for the native pair-driver run")
class NativePairDriverTests(unittest.TestCase):
    """Real adapter load (Rust client) into native Loki, then the driver over all 56 specs."""

    @classmethod
    def setUpClass(cls):
        import pyarrow as pa
        import pyarrow.parquet as pq
        import adapter
        import native_engine
        from test_literal_prefilter import ADVERSARIAL
        cls.tmp = tempfile.TemporaryDirectory(prefix="loki-query-pair-")
        cls.addClassCleanup(cls.tmp.cleanup)
        cls.root = Path(cls.tmp.name)
        if os.environ.get("LOKI_TEST_ARTIFACTS"):
            import shutil
            destination = Path(os.environ["LOKI_TEST_ARTIFACTS"]) / cls.root.name
            cls.addClassCleanup(lambda: shutil.copytree(cls.root, destination, dirs_exist_ok=True))
        source = cls.root / "corpus"
        source.mkdir()
        bodies = ADVERSARIAL + ["failed to place order", "connection refused", "charge card cache failed",
                                "post to email service expected 200 got 500", "plain successful checkout"]
        docs = [{"Timestamp": T + i * 1_000_003_000, "Body": bodies[i % len(bodies)],
                 "ServiceName": ["frontend", "checkout", "payment", "cart"][i % 4],
                 "SeverityText": ["INFO", "ERROR", "WARN"][i % 3], "SeverityNumber": [9, 17, 13][i % 3],
                 "ScopeName": "scope" + str(i % 5), "TraceId": str(i)} for i in range(600)]
        docs += [dict(docs[7]) for _ in range(3)]  # exact duplicate source rows
        table = pa.Table.from_pylist(docs)
        table = table.set_column(0, "Timestamp", pa.array([d["Timestamp"] for d in docs],
                                                          type=pa.timestamp("ns", tz="UTC")))
        pq.write_table(table, source / "part_000.parquet", row_group_size=100)
        cls.rows = len(docs)
        cls.engine = cls.root / "engine"
        cls.engine.mkdir()
        python = sys.executable
        for name in ("start", "stop", "check"):
            write_script(cls.engine / name, f'exec "{python}" "{HERE / "tests/native_engine.py"}" {name}')
        write_script(cls.engine / "query", f'export SEARCHBENCH_PYTHON="{python}"\nexec "{HERE / "query"}"')
        cls.data = cls.root / "data"
        cls.env = clean_env(LOKI_STORAGE="filesystem", LOKI_DATA_DIR=str(cls.data),
                            LOKI_PORT=str(free_port()), LOKI_TEST_GRPC_PORT=str(free_port()),
                            LOKI_TEST_BINARY=os.environ["LOKI_TEST_BINARY"])
        load_env = {**cls.env, "SEARCHBENCH_DATA_DIR": str(source), "LOKI_LOAD_CLIENT": "rust",
                    "LOKI_CLIENT_BIN": os.environ["LOKI_TEST_CLIENT"], "LOKI_SETTLE_QUIET_SECS": "5",
                    "LOKI_SETTLE_TIMEOUT": "300"}
        # The genuine adapter load path (claim, Rust ingest, flush, settle, exact
        # count gate, manifest), with native processes instead of Docker.
        with patch.dict(os.environ, load_env, clear=True), \
                patch.object(adapter, "start", native_engine.start), patch.object(adapter, "stop", native_engine.stop):
            try:
                adapter.load()
            finally:
                native_engine.stop()
        cls.manifest = json.loads((cls.data / "manifest.json").read_text())
        cls.out = cls.root / "pair"
        cls.result = subprocess.run([sys.executable, str(DRIVER), "--out", str(cls.out), "--engine-dir", str(cls.engine),
                                     "--allow-no-cache-drop", "--skip-hot-after-cold-timeout"], env=cls.env, capture_output=True, text=True, timeout=1800)
        (cls.root / "driver.log").write_text(cls.result.stdout + cls.result.stderr)

    def trials(self):
        return [json.loads(line) for line in (self.out / "trials.jsonl").read_text().splitlines()]

    def test_completed_fixture_load(self):
        self.assertEqual(self.manifest["rows"], self.rows)
        self.assertTrue(self.manifest["query_store_only"])

    def test_all_56_specs_both_variants_identical(self):
        self.assertEqual(self.result.returncode, 0, self.result.stdout + self.result.stderr[-3000:])
        equality = json.loads((self.out / "equality.json").read_text())
        self.assertEqual(len(equality["verdicts"]), 56)
        self.assertTrue(equality["all_identical"], equality)
        self.assertTrue(equality["inventory_unchanged"])
        self.assertEqual(equality["cold_cycles"], 112)
        trials = self.trials()
        self.assertEqual(len(trials), 56 * 2 * 3)
        self.assertTrue(all(t["status"] == "ok" for t in trials))
        requests = json.loads((self.out / "requests.json").read_text())
        self.assertEqual(len(requests), 56)
        self.assertGreater(sum(r["prefiltered"] for r in requests), 30)

    def test_per_try_evidence_and_real_restarts(self):
        trials = self.trials()
        for t in trials:
            self.assertEqual(t["settings"]["literal_prefilter"], t["variant"] == "on")
            self.assertEqual(t["settings"]["query_concurrency"], 16)
            self.assertEqual(t["runtime"]["query_concurrency"], 16)
            self.assertEqual(t["runtime"]["literal_prefilter"], t["variant"] == "on")
            self.assertEqual(t["runtime"]["target"], "read,query-scheduler")
            self.assertEqual(t["native"]["ingesterReached"], 0)
            for key in ("execTime", "totalLinesProcessed", "totalBytesProcessed", "shards", "splits"):
                self.assertIn(key, t["native"])
            self.assertIsNotNone(t["answer"]["exact_sha256"])
            self.assertGreater(t["process_wall_seconds"], t["http_seconds"])
        cold = [t for t in trials if t["try"] == 1]
        self.assertEqual([c["cycle"]["cycle"] for c in cold], list(range(1, 113)))
        self.assertTrue(all(c["cycle"]["start_rc"] == 0 for c in cold))
        starts = (self.data / "native-starts.jsonl").read_text().splitlines()
        read_starts = [json.loads(s) for s in starts if json.loads(s)["target"] == "read,query-scheduler"]
        self.assertEqual(len(read_starts), 112)
        self.assertEqual(len({s["pid"] for s in read_starts}), 112)
        self.assertFalse((self.data / "native.pid").exists(), "driver must stop the engine at the end")
        geomeans = json.loads((self.out / "geomeans.json").read_text())
        self.assertEqual(geomeans["all"]["queries"], 56)
        self.assertEqual((geomeans["all"]["cold_queries"], geomeans["all"]["cold_timeouts"]), (56, {"off": 0, "on": 0}))
        for row in json.loads((self.out / "summary.json").read_text()):
            # The hot statistic only uses tries with the cold try's line count.
            matched = [t for t in trials if (t["query"], t["variant"]) == (row["query"], row["variant"])
                       and t["try"] in row["hot_matched_tries"]]
            self.assertTrue(all(t["native"]["totalLinesProcessed"] == row["cold_lines_processed"] for t in matched))
            self.assertEqual(len(matched) + len(row["hot_lines_mismatch_tries"]), 2)
        self.assertEqual(set(geomeans), {"all", "body-count", "group-by", "histogram", "logs", "unfiltered",
                                         "prefiltered", "not-prefiltered"})


if __name__ == "__main__":
    unittest.main()
