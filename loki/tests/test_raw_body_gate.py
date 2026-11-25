"""Raw-Body encoding gate: does native Loki keep distinct-ID equal raw lines?

Raw mode (physical line == exact Body, identity only in structured metadata)
is evaluated here only as a NEGATIVE control. Production never writes it. The
framed v1 encoding is the positive control under identical pushes/queries.
Scenarios use nine rows with identical stream labels, timestamp and Body but
distinct sb_row_id metadata: (a) one push, (b) separate pushes into the same
head chunk, (c) separate pushes each flushed to its own durable chunk. All are
queried store-only after flush and restart, with log limits and metric counts.
"""
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import requests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import make_config
from encoding import decode_frame
from ingest import entry

T = 1758585600000000000
ROWS = 9
LIMITS = (1, 4, 9, 100)
EDGE_BODIES = ["", "\n", "a\r\nb\rc\n", "x" * 600_000]


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def raw_value(body, timestamp, identity):
    """Raw-mode physical entry (negative control only, never a fallback)."""
    labels, value = entry({"Body": body, "ServiceName": "x", "SeverityText": "INFO"}, timestamp, identity)
    value[1] = body
    return value


class NativeLoki:
    def __init__(self, directory):
        self.directory = directory
        http, grpc = port(), port()
        self.url = f"http://127.0.0.1:{http}"
        with patch.dict(os.environ, {"LOKI_STORAGE": "filesystem"}):
            config = make_config(str(directory / "data"), http, grpc, "127.0.0.1")
        self.config = directory / "config.json"
        self.config.write_text(json.dumps(config))
        self.log = (directory / "server.log").open("w")
        self.process = None
        self.history = []

    def chunks(self):
        return sum(p.is_file() for p in (self.directory / "data/chunks/fake").rglob("*"))

    def start(self, target):
        self.process = subprocess.Popen([os.environ["LOKI_TEST_BINARY"], "-config.file=" + str(self.config),
                                         "-target=" + target], stdout=self.log, stderr=self.log)
        self.history.append({"pid": self.process.pid, "target": target, "stopped": False})
        for _ in range(200):
            if self.process.poll() is not None:
                raise AssertionError((self.directory / "server.log").read_text()[-3000:])
            try:
                if requests.get(self.url + "/ready", timeout=1).status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(.2)
        raise AssertionError("native readiness timeout")

    def stop(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    self.process.kill(); self.process.wait(timeout=10)
                    raise
            self.history[-1].update(stopped=True, exit_code=self.process.returncode)
            self.process = None

    def push(self, labels, values):
        response = requests.post(self.url + "/loki/api/v1/push",
                                 json={"streams": [{"stream": labels, "values": values}]}, timeout=60)
        return response.status_code, response.text

    def flush_new_chunk(self):
        before = self.chunks()
        requests.post(self.url + "/flush", timeout=30).raise_for_status()
        for _ in range(150):
            if self.chunks() > before:
                return
            time.sleep(.1)
        raise AssertionError("new durable chunk was not observed")

    def get(self, path, params):
        response = requests.get(self.url + path, params=params, timeout=60)
        response.raise_for_status()
        return response.json()


@unittest.skipUnless(os.environ.get("LOKI_TEST_BINARY"), "set LOKI_TEST_BINARY")
class RawBodyDuplicateGate(unittest.TestCase):
    """Proves raw Body lines lose distinct duplicates; framed v1 keeps them."""

    def run_mode(self, root, mode):
        directory = root / mode
        directory.mkdir()
        loki = NativeLoki(directory)
        observations = {"mode": mode, "scenarios": {}, "edges": []}
        def value(i, scenario):
            identity = f"part_0.parquet:{scenario}:{i}"
            if mode == "raw":
                return raw_value("duplicate body", T, identity)
            return entry({"Body": "duplicate body", "ServiceName": "x", "SeverityText": "INFO"}, T, identity)[1]
        def labels(scenario):
            return {"dataset": "otel_logs", "service_name": scenario, "severity_text": "INFO"}
        try:
            loki.start("all")
            pushes = {}
            # (a) one push carrying all nine rows.
            pushes["a_one_push"] = [loki.push(labels("a_one_push"), [value(i, "a") for i in range(ROWS)])]
            # (b) nine separate pushes, no flush between them: same head chunk.
            pushes["b_separate_pushes"] = [loki.push(labels("b_separate_pushes"), [value(i, "b")]) for i in range(ROWS)]
            loki.flush_new_chunk()
            # (c) nine separate pushes, each flushed into its own durable chunk.
            pushes["c_flushed_chunks"] = []
            for i in range(ROWS):
                pushes["c_flushed_chunks"].append(loki.push(labels("c_flushed_chunks"), [value(i, "c")]))
                loki.flush_new_chunk()
            for scenario, results in pushes.items():
                for status, text in results:
                    self.assertEqual(status, 204, f"{mode} {scenario}: {text}")
            # Edge bodies: empty, newline/CR, long line (each its own stream).
            edge_pushes = []
            for index, body in enumerate(EDGE_BODIES):
                edge_labels = {"dataset": "otel_logs", "service_name": f"edge{index}", "severity_text": "INFO"}
                line = raw_value(body, T + 1, f"edge:{index}") if mode == "raw" else \
                    entry({"Body": body, "ServiceName": "x", "SeverityText": "INFO"}, T + 1, f"edge:{index}")[1]
                edge_pushes.append(loki.push(edge_labels, [line]))
            loki.flush_new_chunk()
            loki.stop()
            # (d) store-only after flush and restart.
            loki.start("read,query-scheduler")
            for scenario in pushes:
                selector = f'{{dataset="otel_logs",service_name="{scenario}"}}'
                result = {"logs": {}, "logs_filtered": {}, "count": None, "count_filtered": None}
                for limit in LIMITS:
                    for key, query in (("logs", selector), ("logs_filtered", selector + ' |~ "duplicate"')):
                        data = loki.get("/loki/api/v1/query_range", {"query": query, "start": str(T), "end": str(T + 1),
                                                                     "direction": "backward", "limit": limit})
                        values = [v for s in data["data"]["result"] for v in s["values"]]
                        # Without categorize-labels, metadata is merged into stream labels.
                        ids = sorted({s["stream"].get("sb_row_id") for s in data["data"]["result"] for _ in s["values"]} - {None})
                        result[key][str(limit)] = {"returned": len(values), "distinct_ids": len(ids),
                                                   "ingester_reached": data["data"]["stats"]["ingester"]["totalReached"]}
                for key, query in (("count", selector), ("count_filtered", selector + ' |~ "duplicate"')):
                    data = loki.get("/loki/api/v1/query", {"query": f"sum(count_over_time({query} | keep dataset [1ms]))",
                                                           "time": str(T)})
                    rows = data["data"]["result"]
                    result[key] = {"count": int(rows[0]["value"][1]) if rows else 0,
                                   "duplicates": data["data"]["stats"]["querier"]["store"]["chunk"]["totalDuplicates"],
                                   "ingester_reached": data["data"]["stats"]["ingester"]["totalReached"]}
                observations["scenarios"][scenario] = result
            for index, body in enumerate(EDGE_BODIES):
                status, text = edge_pushes[index]
                data = loki.get("/loki/api/v1/query_range", {"query": f'{{dataset="otel_logs",service_name="edge{index}"}}',
                                                             "start": str(T), "end": str(T + 2), "limit": 10})
                lines = [v[1] for s in data["data"]["result"] for v in s["values"]]
                bodies = lines if mode == "raw" else [decode_frame(line)["body"] for line in lines]
                observations["edges"].append({"body_repr": repr(body[:20]), "body_bytes": len(body.encode()),
                                              "push_status": status, "push_error": text[:300],
                                              "returned": len(lines), "exact": bodies == [body]})
        finally:
            loki.stop()
            loki.log.close()
            observations["processes"] = loki.history
        return observations

    def test_raw_body_loses_duplicates_framed_preserves_them(self):
        with tempfile.TemporaryDirectory(prefix="loki-raw-body-gate-") as tmp:
            root = Path(tmp)
            try:
                results = {mode: self.run_mode(root, mode) for mode in ("framed", "raw")}
                (root / "results.json").write_text(json.dumps(results, indent=2))
                framed, raw = results["framed"], results["raw"]
                # Positive control: every scenario, limit and query kind keeps 9.
                for scenario, result in framed["scenarios"].items():
                    for key in ("logs", "logs_filtered"):
                        for limit in LIMITS:
                            self.assertEqual(result[key][str(limit)]["returned"], min(limit, ROWS), (scenario, key, limit))
                    self.assertEqual(result["count"]["count"], ROWS, scenario)
                    self.assertEqual(result["count_filtered"]["count"], ROWS, scenario)
                self.assertTrue(all(edge["exact"] for edge in framed["edges"]), framed["edges"])
                # Gate: raw Body lines merge distinct rows in at least one path.
                # Log queries (Entry.Equal compares metadata) keep all rows; the
                # cross-chunk metric merge hashes the line only and does not.
                for scenario, result in raw["scenarios"].items():
                    for key in ("logs", "logs_filtered"):
                        for limit in LIMITS:
                            self.assertEqual(result[key][str(limit)]["returned"], min(limit, ROWS), (scenario, key, limit))
                for scenario in ("a_one_push", "b_separate_pushes"):
                    self.assertEqual(raw["scenarios"][scenario]["count"]["count"], ROWS, scenario)
                cross_chunk = raw["scenarios"]["c_flushed_chunks"]
                # Observed with Loki 3.7.9: nine distinct rows count as ONE.
                for key in ("count", "count_filtered"):
                    self.assertEqual(cross_chunk[key]["count"], 1, cross_chunk)
                    self.assertEqual(cross_chunk[key]["duplicates"], ROWS - 1, cross_chunk)
                # Edge Bodies themselves round-trip in either encoding.
                self.assertTrue(all(edge["exact"] and edge["push_status"] == 204 for edge in raw["edges"]), raw["edges"])
            finally:
                if os.environ.get("LOKI_TEST_ARTIFACTS"):
                    shutil.copytree(root, Path(os.environ["LOKI_TEST_ARTIFACTS"]) / root.name)


class RawModeIsNotAcceptedTests(unittest.TestCase):
    """Raw mode is rejected (gate failed): no query can mix or assume it."""

    def test_raw_descriptor_rejected_by_contract_request_and_normalize(self):
        from encoding import ENCODING, validate_contract
        from logql import build_request, normalize
        from test_adapter import CONTRACT
        raw = {**CONTRACT, "encoding": {"name": "searchbench-loki-raw-body", "version": 1,
                                        "identity": "source-file-row-v1"},
               "min_timestamp_ns": T, "max_timestamp_ns": T}
        framed = {**CONTRACT, "min_timestamp_ns": T, "max_timestamp_ns": T}
        for manifest, receipt in ((raw, None), (raw, CONTRACT), (framed, {**CONTRACT, "encoding": raw["encoding"]})):
            with self.assertRaises(ValueError):
                validate_contract(manifest, receipt)
        for literal_prefilter in (False, True):
            with self.assertRaises(ValueError):
                build_request({"kind": "count", "filters": ['|~ "error"']}, raw, literal_prefilter=literal_prefilter)
        response = {"status": "success", "data": {"resultType": "streams", "result": [
            {"stream": {"sb_row_id": "p:0"}, "values": [[str(T), "raw body"]]}]}}
        with self.assertRaises(ValueError):
            normalize({"kind": "logs"}, response, raw)
        with self.assertRaises(ValueError):  # framed manifest never accepts a raw line
            normalize({"kind": "logs"}, response, framed)
        self.assertEqual(ENCODING["name"], "searchbench-loki-json-frame")


if __name__ == "__main__":
    unittest.main()
