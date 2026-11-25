import copy
import importlib.util
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from config import make_config, settings
from gen_queries import generate, translate, Unsupported
from ingest import entry, ingest_file, push
from logql import build_request, normalize, timestamp_string, verified_corpus_lower_bound
from encoding import ENCODING

CONTRACT = {"encoding": ENCODING, "destination_id": "native-fixture-v1",
            "destination": {"storage": "filesystem", "data_dir": "fixture"}}


def legacy_request(spec, manifest):
    """Frozen pre-optimization pipeline, including its unconditional ns guard."""
    path, params = build_request(spec, {k: v for k, v in manifest.items() if k != "verification_stats"},
                                 literal_prefilter=False)
    params["query"] = params["query"].replace(
        '| json sb_original="body" | line_format `{{ lower .sb_original }}`',
        '| line_format `{{ lower (fromJson (__line__)).body }}`').replace(
        '| drop sb_frame,sb_original', '| drop sb_frame')
    return path, params


def completed_manifest(manifest):
    return {**manifest, "rows": 1, "query_store_only": True,
            "persisted_inventory": "a" * 64,
            "verification_stats": {"ingester": {"totalReached": 0}}}


class UnitTests(unittest.TestCase):
    def test_generated_workload(self):
        output = generate()
        self.assertEqual(output, (HERE / "queries.logql").read_text())
        self.assertEqual(sum(line.startswith("-- Q") for line in output.splitlines()), 92)
        self.assertEqual(sum(line.startswith("{") for line in output.splitlines()), 56)
        self.assertEqual(sum(line.startswith("UNSUPPORTED:") for line in output.splitlines()), 36)

    def test_unsupported_is_explicit(self):
        with self.assertRaises(Unsupported):
            translate(22, "task=count", '{"query":{"fuzzy":{"Body":{"value":"connection","fuzziness":1}}}}')
        with self.assertRaises(Unsupported):
            translate(33, "task=top_k", "{}")

    def test_count_window_and_labels(self):
        spec = {"kind": "count", "filters": [], "window": [1_000_000_000, 2_000_000_000]}
        path, params = build_request(spec, CONTRACT)
        self.assertEqual(path, "/loki/api/v1/query")
        self.assertEqual(params["time"], "2000000000")
        self.assertIn("[1001ms]", params["query"])
        self.assertIn("UnixNano 1000000000", params["query"])
        self.assertIn("| keep dataset", params["query"])

    def test_recent_restores_body_and_inclusive_end(self):
        spec = {"kind": "logs", "filters": ['|~ "error"'], "window": [10, 20]}
        _, params = build_request(spec, CONTRACT)
        self.assertEqual(params["end"], "21")
        self.assertIn("lower", params["query"])
        self.assertIn("{{ .sb_frame }}", params["query"])
        self.assertIn('json sb_original="body"', params["query"])
        self.assertIn("drop sb_frame,sb_original", params["query"])
        self.assertNotIn("fromJson", params["query"])

    def test_verified_guard_proof_and_boundaries(self):
        manifest = completed_manifest({**CONTRACT, "min_timestamp_ns": 1000001, "max_timestamp_ns": 2000001})
        for stats in [{"ingester": {"totalReached": 0}},
                      {"verification_method": "partitioned-native-count", "parts": [], "ingester": {"totalReached": 0}}]:
            manifest["verification_stats"] = stats
            self.assertEqual(verified_corpus_lower_bound(manifest), 1000001)
            for lo in [0, 1000000, 1000001, 1000002]:
                _, params = build_request({"kind": "count", "window": [lo, 2000001]}, manifest)
                self.assertEqual("sb_window" in params["query"], lo > 1000001)
        bad_values = {
            "min_timestamp_ns": [True, "1000001", 1000001., None, 3000000],
            "max_timestamp_ns": [False, "2000001", 2000001., None, 0],
            "rows": [True, 0, -1, "1", 1., None],
            "query_store_only": [1, False, "true", None],
            "persisted_inventory": ["a" * 63, "A" * 64, "z" * 64, {}, None],
        }
        bad_values["verification_stats"] = [None, [], {}, {"ingester": []}, {"ingester": {}}] + [
            {"ingester": {"totalReached": v}} for v in [None, True, False, "0", 1, float("nan")]]
        for key, values in bad_values.items():
            for value in values:
                with self.subTest(key=key, value=value):
                    bad = {**manifest, key: value}
                    self.assertIsNone(verified_corpus_lower_bound(bad))
                    _, params = build_request({"kind": "count", "window": [0, 2000001]}, bad)
                    self.assertIn("sb_window", params["query"])
            missing = {k: v for k, v in manifest.items() if k != key}
            self.assertIsNone(verified_corpus_lower_bound(missing))

    def test_metadata_identity_and_precision(self):
        labels, value = entry({"Body": "Error\nline", "ServiceName": "cart", "SeverityText": "ERROR",
                               "TraceId": "abc", "ResourceAttributes": {"k": "v"}}, 1758585600000000001, "p:0")
        self.assertEqual(value[0], "1758585600000000001")
        self.assertNotIn("TraceId", labels)
        self.assertEqual(value[2]["sb_row_id"], "p:0")
        self.assertEqual(json.loads(value[2]["ResourceAttributes"]), {"k": "v"})
        self.assertEqual(timestamp_string(value[0]), "2025-09-23T00:00:00.000000001Z")

    def test_empty_count_and_invalid_response(self):
        self.assertEqual(normalize({"kind": "count"}, {"status": "success", "data": {"resultType": "vector", "result": []}}, CONTRACT), [{"count": 0}])
        with self.assertRaises(ValueError):
            normalize({"kind": "count"}, {"status": "error"}, CONTRACT)

    def test_no_data_caches_and_storage_only(self):
        with patch.dict(os.environ, {"LOKI_STORAGE": "filesystem"}):
            c = make_config()
        self.assertTrue(c["querier"]["query_store_only"])
        self.assertFalse(c["query_range"]["cache_results"])
        self.assertFalse(c["query_range"]["cache_instant_metric_results"])
        self.assertFalse(c["chunk_store_config"]["chunk_cache_config"]["embedded_cache"]["enabled"])
        self.assertFalse(c["limits_config"]["increment_duplicate_timestamp"])

    def test_target_switches_after_completed_load(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {"LOKI_STORAGE": "filesystem", "LOKI_DATA_DIR": tmp, "LOKI_TARGET": "auto"}):
            self.assertEqual(settings()["target"], "all")
            (Path(tmp) / "manifest.json").write_text("{}")
            self.assertEqual(settings()["target"], "read")
            with patch.dict(os.environ, {"LOKI_TARGET": "all"}):
                self.assertEqual(settings()["target"], "all")
            with patch.dict(os.environ, {"LOKI_TARGET": "invalid"}):
                with self.assertRaises(ValueError):
                    settings()

    def test_query_tuning_knobs(self):
        with patch.dict(os.environ, {"LOKI_STORAGE": "filesystem", "LOKI_SPLIT_QUERIES_INTERVAL": "0s",
                                    "LOKI_SPLIT_METRIC_QUERIES_INTERVAL": "0s", "LOKI_TSDB_MAX_BYTES_PER_SHARD": "16MB",
                                    "LOKI_TSDB_MAX_QUERY_PARALLELISM": "16", "LOKI_QUERY_CONCURRENCY": "4"}):
            config = make_config()
        self.assertEqual(config["limits_config"]["split_queries_by_interval"], "0s")
        self.assertEqual(config["limits_config"]["split_instant_metric_queries_by_interval"], "0s")
        self.assertEqual(config["limits_config"]["tsdb_max_bytes_per_shard"], "16MB")
        self.assertEqual(config["limits_config"]["tsdb_max_query_parallelism"], 16)
        self.assertEqual(config["querier"]["max_concurrent"], 4)
        with patch.dict(os.environ, {"LOKI_STORAGE": "filesystem", "LOKI_SPLIT_QUERIES_INTERVAL": "-1m"}):
            with self.assertRaises(ValueError):
                make_config()

    def test_s3_destination_guard(self):
        with patch.dict(os.environ, {"LOKI_STORAGE": "s3", "LOKI_S3_BUCKET": "bucket", "LOKI_S3_PREFIX": ""}):
            with self.assertRaises(ValueError):
                settings()
        with patch.dict(os.environ, {"LOKI_STORAGE": "s3", "LOKI_S3_BUCKET": "bucket", "LOKI_S3_PREFIX": "searchbench/loki/test-1/"}):
            self.assertEqual(make_config()["storage_config"]["object_prefix"], "searchbench/loki/test-1/")


@unittest.skipUnless(os.environ.get("LOKI_TEST_BINARY"), "set LOKI_TEST_BINARY for native-binary integration tests")
class IntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import requests
        cls.requests = requests
        cls.tmp = tempfile.TemporaryDirectory(prefix="searchbench-loki-test-")
        cls.root = Path(cls.tmp.name)
        cls.addClassCleanup(cls.tmp.cleanup)
        if os.environ.get("LOKI_TEST_ARTIFACTS"):
            import shutil
            destination = Path(os.environ["LOKI_TEST_ARTIFACTS"]) / (cls.__name__ + "-" + cls.root.name)
            cls.addClassCleanup(lambda: shutil.copytree(cls.root, destination))
        def port():
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                return sock.getsockname()[1]
        cls.port, grpc = port(), port()
        cls.url = f"http://127.0.0.1:{cls.port}"
        with patch.dict(os.environ, {"LOKI_STORAGE": "filesystem"}):
            config = make_config(str(cls.root / "data"), cls.port, grpc, "127.0.0.1")
        cls.config = cls.root / "config.json"
        cls.config.write_text(json.dumps(config))
        cls.log = (cls.root / "server.log").open("w+")
        cls.addClassCleanup(cls.log.close)
        cls.target = "all"
        cls.process_history = []
        cls.start_server()
        cls.addClassCleanup(cls.stop_server)
        cls.t = 1758585600000000000
        bodies = ["ERROR", "errorless terror", "failed-order", "failed to place order", "FAILED\nTO\tPLACE—ORDER",
                  "post to email service expected 200 got 500", "charge cache", "connection request conversion post",
                  "payment exception refused send confirmation email expected deadline", "cAche charged chargeback",
                  "failed to place order charge", "foo_error", "éerror erroré", "error\u0301", "CHARGE", "ſend", "plain",
                  "error\u200dfoo", "error\x00foo", "error\u00a0foo", "error\x1cfoo"]
        bodies += ["", "\n", "雪 😀 café", 'quotes " backslash \\ slash /\r\t\b\f',
                   "".join(chr(i) for i in range(32)), '{"v":1,"id":"legacy","body":"error"}',
                   "v id body", "failed charge card cache",
                   "failed send order confirmation email service expected post",
                   "error\u2028failed\u2029order", "error\u2007foo", "error\u202ffoo",
                   "İ I ı i K K k ſ S s Σ σ ς ẞ ß", "\\u0065rror", "ERROR\\nFAILED"]
        cls.docs = []
        for i, body in enumerate(bodies):
            cls.docs.append({"Timestamp": cls.t + i * 10_000_000_000, "Body": body,
                             "ServiceName": ["frontend", "checkout", "payment", "cart"][i % 4],
                             "SeverityText": ["INFO", "ERROR"][i % 2], "SeverityNumber": 17 if i % 2 else 9,
                             "ScopeName": "scope" + str(i % 3), "TraceId": str(i)})
        cls.docs += copy.deepcopy(cls.docs)
        cls.docs += [{"Timestamp": cls.t + 400_000_000_001, "Body": "ERROR failed order charge",
                      "ServiceName": "limit-test", "SeverityText": "ERROR", "SeverityNumber": 17,
                      "ScopeName": "tail", "TraceId": "tail"} for _ in range(130)]
        # Exact duplicates must remain separate source rows. Also exercise the
        # instant/range and histogram boundaries at ns resolution.
        for offset in [-1, 0, 1, 59_999_999_999, 60_000_000_000, 60_000_000_001, 21_600_000_000_000, 21_600_000_000_001]:
            cls.docs.append({"Timestamp": cls.t + offset, "Body": "ERROR failed order charge", "ServiceName": "checkout",
                             "SeverityText": "ERROR", "SeverityNumber": 17, "ScopeName": "edge", "TraceId": "edge"})
        cls.docs.append(copy.deepcopy(cls.docs[-4]))
        # Prevent >1h out-of-order rejection while testing the six-hour range.
        cls.docs.sort(key=lambda d: d["Timestamp"])
        cls.ingest_fixture()
        r = requests.post(cls.url + "/flush", timeout=30)
        r.raise_for_status()
        cls.stop_server()  # flush and ship TSDB head
        cls.target = "read,query-scheduler,compactor"
        cls.start_server()
        cls.manifest = {**CONTRACT, "min_timestamp_ns": min(d["Timestamp"] for d in cls.docs),
                        "max_timestamp_ns": max(d["Timestamp"] for d in cls.docs)}
        (cls.root / "fixture.json").write_text(json.dumps(cls.docs))
        (cls.root / "fixture-manifest.json").write_text(json.dumps(cls.manifest))
        for _ in range(60):
            rows, stats = cls.execute({"kind": "count", "filters": []})
            if rows[0]["count"] == len(cls.docs):
                break
            time.sleep(1)
        else:
            raise AssertionError(f"persisted count mismatch: {rows}; expected {len(cls.docs)}")
        cls.manifest = {**completed_manifest(cls.manifest), "rows": len(cls.docs), "verification_stats": stats}
        (cls.root / "fixture-manifest.json").write_text(json.dumps(cls.manifest))

    @classmethod
    def fixture_batches(cls):
        # Keep <1h out-of-order batches, then advance to six-hour edge rows.
        # Adjacent equal source rows go into DIFFERENT durable chunks.
        groups = {}
        for i, d in enumerate(cls.docs):
            groups.setdefault((d["Timestamp"] - cls.docs[0]["Timestamp"]) // (3600 * 10**9), []).append((i, d))
        return [group[parity::2] for group in groups.values() for parity in (0, 1) if group[parity::2]]

    @classmethod
    def flush_fixture(cls, previous):
        cls.requests.post(cls.url + "/flush", timeout=30).raise_for_status()
        for _ in range(150):
            if sum(p.is_file() for p in (cls.root / "data/chunks/fake").rglob("*")) > previous:
                return
            time.sleep(.1)
        raise AssertionError("new durable chunk not observed before next batch")

    @classmethod
    def ingest_fixture(cls):
        with cls.requests.Session() as session:
            for batch in cls.fixture_batches():
                previous = sum(p.is_file() for p in (cls.root / "data/chunks/fake").rglob("*"))
                push(session, cls.url, [entry(d, d["Timestamp"], f"fixture:{i}") for i, d in batch])
                cls.flush_fixture(previous)

    @classmethod
    def start_server(cls):
        cls.process = subprocess.Popen([os.environ["LOKI_TEST_BINARY"], "-config.file=" + str(cls.config), "-target=" + cls.target],
                                       stdout=cls.log, stderr=cls.log)
        cls.process_history.append({"pid": cls.process.pid, "target": cls.target, "stopped": False})
        for _ in range(120):
            if cls.process.poll() is not None:
                raise AssertionError((cls.root / "server.log").read_text()[-3000:])
            try:
                if cls.requests.get(cls.url + "/ready", timeout=1).status_code == 200:
                    return
            except cls.requests.RequestException:
                pass
            time.sleep(.25)
        cls.stop_server()
        raise AssertionError("Loki readiness timed out")

    @classmethod
    def stop_server(cls):
        if getattr(cls, "process", None):
            if cls.process.poll() is None:
                cls.process.terminate()
                try:
                    cls.process.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    cls.process.kill(); cls.process.wait(timeout=10)
                    raise
            cls.process_history[-1].update(stopped=True, exit_code=cls.process.returncode)
            (cls.root / "processes.json").write_text(json.dumps(cls.process_history))

    @classmethod
    def execute(cls, spec, literal_prefilter=False):
        path, params = build_request(spec, cls.manifest, literal_prefilter=literal_prefilter)
        response = cls.requests.get(cls.url + path, params=params, timeout=30)
        if response.status_code != 200:
            raise AssertionError(response.text)
        data = response.json()
        if os.environ.get("LOKI_TEST_ARTIFACTS"):
            with (cls.root / "queries.jsonl").open("a") as file:
                file.write(json.dumps({"spec": spec, "params": params, "response": data}) + "\n")
        stats = data["data"]["stats"]
        assert stats["ingester"]["totalReached"] == 0, stats
        assert stats["querier"]["store"]["chunk"]["totalDuplicates"] == 0, stats
        return normalize(spec, data, cls.manifest), stats

    def test_native_old_new_equivalence_and_scratch_cleanup(self):
        specs = [json.loads(line) for line in (HERE / "queries.logql").read_text().splitlines() if line.startswith("{")]
        specs += [{"kind": kind, "limit": 500, "filters": [predicate]}
                  for kind in ("count", "logs")
                  for predicate in ['!~ "absent_token_xyz"', '|~ "i|k|s|σ|ß"', '|~ "\\\\\\\\u0065"']]
        for lo in [self.manifest["min_timestamp_ns"] - 1, self.manifest["min_timestamp_ns"], self.t, self.t + 1]:
            specs.append({"kind": "count", "window": [lo, self.t + 60_000_000_000], "filters": ['|~ "error"']})
        def prefilter_off(spec, manifest):
            return build_request(spec, manifest, literal_prefilter=False)
        def prefiltered(spec, manifest):
            return build_request(spec, manifest, literal_prefilter=True)
        for spec in specs:
            with self.subTest(spec=spec):
                responses = []
                for builder in (legacy_request, prefilter_off, prefiltered):
                    path, params = builder(spec, self.manifest)
                    response = self.requests.get(self.url + path, params=params, timeout=30)
                    self.assertEqual(response.status_code, 200, response.text)
                    data = response.json()
                    responses.append(normalize(spec, data, self.manifest))
                    self.assertEqual(data["data"]["stats"]["ingester"]["totalReached"], 0)
                    for stream in data["data"]["result"]:
                        labels = stream.get("stream", stream.get("metric", {}))
                        self.assertFalse(set(labels) & {"sb_original", "sb_frame", "sb_window", "__error__"})
                self.assertCountEqual(responses[0], responses[1])
                self.assertCountEqual(responses[0], responses[2])

    def test_native_targeted_json_preserves_exact_strings(self):
        # Inspect the extracted value before production scratch cleanup. Every
        # control byte, escaped backslash/quote, empty string and Unicode Body
        # must be identical to the strict decoded frame, not its JSON spelling.
        from encoding import decode_frame
        spec = {"kind": "logs", "limit": 500, "filters": ['!~ "absent_token_xyz"']}
        path, params = build_request(spec, self.manifest)
        params["query"] = params["query"].replace(" | drop sb_frame,sb_original", "")
        response = self.requests.get(self.url + path, params=params, timeout=30)
        self.assertEqual(response.status_code, 200, response.text)
        seen = 0
        for stream in response.json()["data"]["result"]:
            for _, line, *metadata in stream["values"]:
                self.assertEqual(stream["stream"].get("sb_original", ""), decode_frame(line)["body"])
                seen += 1
        self.assertEqual(seen, len(self.docs))

    def test_store_only_and_duplicates(self):
        rows, stats = self.execute({"kind": "count", "filters": []})
        self.assertEqual(rows[0]["count"], len(self.docs))
        self.assertEqual(stats["ingester"]["totalReached"], 0)
        self.assertGreater(stats["querier"]["store"]["totalChunksDownloaded"], 0)

    def test_inclusive_bounds(self):
        for low, high in [(self.t, self.t + 60_000_000_000), (self.t, self.t), (self.t + 1, self.t + 1)]:
            spec = {"kind": "count", "filters": [], "window": [low, high]}
            rows, _ = self.execute(spec)
            self.assertEqual(rows[0]["count"], sum(low <= d["Timestamp"] <= high for d in self.docs))

    def test_utc_histogram(self):
        from collections import Counter
        rows, _ = self.execute({"kind": "histogram", "groups": ["minute"], "filters": []})
        expected = Counter(timestamp_string(d["Timestamp"])[:16] + ":00Z" for d in self.docs)
        self.assertEqual({r["minute"]: r["count"] for r in rows}, expected)

    def test_all_supported_queries_against_reference(self):
        tested = 0
        from oracle import expected_rows
        source = (HERE.parent / "elastic/queries.dsl").read_text().splitlines()
        for i, tag in enumerate(source):
            import re
            match = re.match(r"-- Q(\d+) ", tag)
            if not match:
                continue
            number = int(match[1])
            try:
                spec = translate(number, tag, source[i + 1])
            except Unsupported:
                continue
            expected = expected_rows(number, spec, json.loads(source[i + 1]), self.docs)
            for literal_prefilter in (False, True):
                with self.subTest(query=number, literal_prefilter=literal_prefilter):
                    actual, _ = self.execute(spec, literal_prefilter)
                    if spec["kind"] == "logs":
                        self.assertCountEqual(actual, expected)
                        self.assertEqual([r["Timestamp"] for r in actual], sorted((r["Timestamp"] for r in actual), reverse=True))
                    else:
                        self.assertEqual(actual, expected)
                    tested += 1
        self.assertEqual(tested, 2 * 56)

    def test_exact_body_projection_and_duplicate_limits(self):
        rows, _ = self.execute({"kind": "logs", "limit": 500, "filters": ['!~ "absent_token_xyz"']})
        expected = [{"Timestamp": timestamp_string(d["Timestamp"]), "ServiceName": d["ServiceName"],
                     "SeverityText": d["SeverityText"], "Body": d["Body"]} for d in self.docs]
        self.assertCountEqual(rows, expected)
        self.assertEqual([r["Timestamp"] for r in rows], sorted((r["Timestamp"] for r in rows), reverse=True))
        for limit in (1, 7, 100, 129, 130, 150):
            for filters in ([], ['|~ "error"']):
                rows, _ = self.execute({"kind": "logs", "limit": limit, "filters": filters,
                                        "labels": {"service_name": "limit-test"}})
                self.assertEqual(len(rows), min(limit, 130))
                self.assertTrue(all(row == rows[0] for row in rows))
        for lo, hi in [(self.t, self.t), (self.t + 1, self.t + 1), (self.t - 2, self.t - 2)]:
            rows, _ = self.execute({"kind": "logs", "window": [lo, hi], "limit": 500})
            self.assertEqual(len(rows), sum(lo <= d["Timestamp"] <= hi for d in self.docs))

    def test_native_merge_requires_identity_after_projection(self):
        # 130 same-timestamp/Body rows in separately flushed chunks. Distinct
        # physical frames survive keep; Body-only output loses multiplicity.
        spec = {"kind": "logs", "limit": 500, "labels": {"service_name": "limit-test"},
                "filters": ['|~ "error"']}
        path, params = build_request(spec, self.manifest)
        for suffix, lossless in [
            (' | keep dataset,service_name,severity_text', True),
            (' | line_format `{{ (fromJson (__line__)).body }}`', True),
            (' | line_format `{{ (fromJson (__line__)).body }}` | keep dataset,service_name,severity_text', False),
        ]:
            response = self.requests.get(self.url + path, params={**params, "query": params["query"] + suffix}, timeout=30)
            response.raise_for_status()
            data = response.json()["data"]
            count = sum(len(s["values"]) for s in data["result"])
            if lossless:
                self.assertEqual(count, 130)
            else:
                self.assertLess(count, 130)
            self.assertEqual(data["stats"]["ingester"]["totalReached"], 0)

    def test_zzz_parquet_ns_ingest(self):
        # Run AFTER read-only invariance checks: restarting an ingester replays
        # WAL and creates replicated copies (legitimate native dedup), so it is
        # not a cold read-only query restart. Query roles reject pushes.
        self.stop_server()
        type(self).target = "all"
        self.start_server()
        import pyarrow as pa
        import pyarrow.parquet as pq
        doc = {"Timestamp": self.t + 900_000_000_001, "Body": "unique Parquet", "ServiceName": "parquet-test",
               "SeverityText": "INFO", "TraceId": "parquet"}
        table = pa.Table.from_pylist([doc]).set_column(0, "Timestamp", pa.array([doc["Timestamp"]], type=pa.timestamp("ns", tz="UTC")))
        path = self.root / "part_ns_probe.parquet"
        pq.write_table(table, path)
        result = ingest_file((str(path), self.url, 1))
        self.assertEqual(result["min_timestamp_ns"], doc["Timestamp"])
        # Cleanup registered at class scope performs the final flush/stop.

    def test_zz_read_only_restarts_preserve_object_inventory(self):
        self.stop_server()
        type(self).target = "read,query-scheduler"
        self.start_server()
        def inventory():
            return {str(p.relative_to(self.root)): (p.stat().st_size, p.stat().st_mtime_ns)
                    for p in (self.root / "data/chunks").rglob("*") if p.is_file()}
        before = inventory()
        self.test_all_supported_queries_against_reference()
        response = self.requests.post(self.url + "/loki/api/v1/push", json={"streams": []}, timeout=10)
        self.assertNotEqual(response.status_code, 204, "read target must not accept ingestion")
        self.stop_server()
        self.start_server()
        self.test_all_supported_queries_against_reference()
        self.assertEqual(before, inventory(), "cold read-only restarts must not create/rewrite persisted objects")


if __name__ == "__main__":
    unittest.main()
