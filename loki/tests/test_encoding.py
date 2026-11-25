"""Production encoding, destination provenance, preflight and query accounting."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import requests
import adapter
import ingest
from encoding import (ENCODING, ENCODING_FLAG, MAX_LINE_BYTES, MAX_REQUEST_BYTES,
                      RESERVED, decode_frame, encode_frame, frame_overhead, validate_contract)
from logql import build_request, normalize
from test_adapter import CONTRACT

T = 1758585600000000001


def document(body="error", **metadata):
    return {"Timestamp": T, "Body": body, "ServiceName": "cart", "SeverityText": "ERROR", **metadata}


class EncodingTests(unittest.TestCase):
    def test_exact_codec_and_no_heuristic_raw_fallback(self):
        for body in ["", "\n", "雪😀ſ\u2028\u2029", '\"\\\b\f\n\r\t\0\x1c',
                     "".join(chr(i) for i in range(128)), '{"v":1,"id":"legacy","body":"error"}']:
            labels, value = ingest.entry(document(body), T, "part_é.parquet:12")
            frame = decode_frame(value[1])
            self.assertEqual(frame, {"v": 1, "id": "part_é.parquet:12", "body": body})
            self.assertEqual(set(labels), {"dataset", "service_name", "severity_text"})
            self.assertEqual(value[0], str(T))
            response = {"status": "success", "data": {"resultType": "streams", "result": [
                {"stream": {**labels, **value[2]}, "values": [value[:2]]}]}}
            self.assertEqual(normalize({"kind": "logs"}, response, CONTRACT)[0]["Body"], body)
        for bad in ['raw', '{}', '{"v":2,"id":"p:0","body":"x"}',
                    '{"v":true,"id":"p:0","body":"x"}', '{"v":1,"id":"","body":"x"}',
                    '{"v":1,"id":"p:0","body":null}', '{"v":1,"id":"p:0","body":"x","body":"y"}',
                    '{"v":1,"id":"p:0","body":"x","extra":1}']:
            with self.assertRaises(ValueError):
                decode_frame(bad)
        for field in RESERVED:
            with self.assertRaisesRegex(ValueError, "reserved"):
                ingest.entry(document(**{field: "collision"}), T, "p:0")
        with self.assertRaises(UnicodeError):
            encode_frame("\ud800", "p:0")

    def test_exact_physical_line_cap_not_raw_body_cap(self):
        overhead = frame_overhead("p:0")
        for body in ["x" * (MAX_LINE_BYTES - overhead),
                     "\0" * ((MAX_LINE_BYTES - overhead) // 6) + "x" * ((MAX_LINE_BYTES - overhead) % 6)]:
            self.assertEqual(len(encode_frame(body, "p:0").encode()), MAX_LINE_BYTES)
            with self.assertRaisesRegex(ValueError, "physical frame"):
                encode_frame(body + "x", "p:0")
        with self.assertRaisesRegex(ValueError, "physical frame"):
            encode_frame("\0" * 180000, "p:0")

    def test_strict_contract_and_identity_mismatch(self):
        for bad in [{}, {**CONTRACT, "encoding": {**ENCODING, "version": True}},
                    {**CONTRACT, "encoding": {**ENCODING, "version": 2}},
                    {**CONTRACT, "encoding": "raw"}, {**CONTRACT, "destination_id": "wrong"},
                    {**CONTRACT, "destination": {"storage": "other"}}]:
            with self.assertRaises(ValueError):
                validate_contract(bad, CONTRACT)
        response = {"status": "success", "data": {"resultType": "streams", "result": [
            {"stream": {"sb_row_id": "wrong"}, "values": [[str(T), encode_frame("x", "p:0")]]}]}}
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            normalize({"kind": "logs"}, response, CONTRACT)
        for manifest in [{}, {**CONTRACT, "encoding": "raw"}]:
            with self.assertRaises(ValueError):
                build_request({"kind": "count", "window": [T, T]}, manifest)
        _, params = build_request({"kind": "count", "window": [T, T]}, CONTRACT)
        self.assertNotIn("fromJson", params["query"])
        self.assertNotIn("sb_row_id", params["query"])

    def test_python_wire_and_conservative_internal_budget(self):
        rows = [ingest.entry(document("\0😀" * 12, TraceId=str(i)), T, f"p:{i}") for i in range(30)]
        with requests.Session() as session, patch.object(session, "send", return_value=Mock(status_code=204)) as send:
            with patch.dict(os.environ, {"LOKI_MAX_REQUEST_BYTES": "1024"}):
                ingest.push(session, "http://unused", rows)
            seen = []
            for call in send.call_args_list:
                prepared = call.args[0]
                self.assertLessEqual(len(prepared.body), 1024)
                payload = json.loads(prepared.body)
                internal = 0
                for stream in payload["streams"]:
                    for value in stream["values"]:
                        internal += ingest.internal_row_bound(stream["stream"], value)
                        seen.append(decode_frame(value[1])["id"])
                self.assertLessEqual(internal, 1024)
                self.assertFalse(call.kwargs["allow_redirects"])
            self.assertEqual(sorted(seen), sorted(f"p:{i}" for i in range(30)))
            self.assertGreater(send.call_count, 1)
        # Plan the complete batch before sending any smaller prefix.
        bad = ingest.entry(document("small", TraceId="x" * 4000), T, "p:oversize")
        with requests.Session() as session, patch.object(session, "send") as send:
            with patch.dict(os.environ, {"LOKI_MAX_REQUEST_BYTES": "1024"}):
                with self.assertRaisesRegex(ValueError, "one row exceeds"):
                    ingest.push(session, "http://unused", rows[:1] + [bad])
            send.assert_not_called()
        with requests.Session() as session, patch.object(session, "send", side_effect=requests.Timeout("ambiguous")) as send:
            with self.assertRaises(requests.Timeout):
                ingest.push(session, "http://unused", rows[:1])
            self.assertEqual(send.call_count, 1)
        for invalid in ["1023", str(MAX_REQUEST_BYTES + 1)]:
            with patch.dict(os.environ, {"LOKI_MAX_REQUEST_BYTES": invalid}):
                with self.assertRaises(ValueError):
                    ingest.request_budget()

    @unittest.skipUnless(importlib.util.find_spec("google"), "protobuf test dependency needed")
    def test_internal_bound_covers_actual_protobuf(self):
        from push_protocol import PushRequest
        request = PushRequest()
        total = 0
        for i, body in enumerate(["", "😀" * 200, "\0" * 200, '"\\\n']):
            labels, value = ingest.entry(document(body, TraceId="雪" * 100), T, f"p:{i}")
            total += ingest.internal_row_bound(labels, value)
            stream = request.streams.add(labels="{" + ",".join(k + "=" + json.dumps(v, ensure_ascii=False)
                                                              for k, v in sorted(labels.items())) + "}")
            entry = stream.entries.add(line=value[1])
            entry.timestamp.seconds, entry.timestamp.nanos = divmod(T, 10**9)
            for name, val in value[2].items():
                entry.structured_metadata.add(name=name, value=val)
        self.assertGreaterEqual(total, len(request.SerializeToString()))


class DestinationAndDiagnosticsTests(unittest.TestCase):
    def test_fresh_receipt_binding_legacy_and_mixed_rejected_before_http(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {
                "LOKI_STORAGE": "filesystem", "LOKI_DATA_DIR": tmp}):
            root = adapter.prepare()
            receipt = adapter.claim_load(root, "corpus", [{"name": "p", "bytes": 1, "mtime_ns": 2}])
            manifest = {**receipt, "min_timestamp_ns": T, "max_timestamp_ns": T}
            (root / "manifest.json").write_text(json.dumps(manifest))
            self.assertEqual(adapter.read_manifest(), manifest)
            for key, value in [("encoding", "raw"), ("destination_id", "different"),
                               ("destination", {"storage": "filesystem", "data_dir": "/other"})]:
                with patch.object(adapter, "api") as api:
                    with self.assertRaises(ValueError):
                        adapter.query_response({"kind": "count"}, {**manifest, key: value})
                    api.assert_not_called()
            (root / "manifest.json").write_text(json.dumps({**manifest, "source_files": []}))
            with self.assertRaisesRegex(ValueError, "source inventory"):
                adapter.read_manifest()
            (root / "load-started.json").write_text(json.dumps({"source": "legacy"}))
            with self.assertRaisesRegex(ValueError, "legacy"):
                adapter.read_manifest()
            with self.assertRaises(FileExistsError):
                adapter.claim_load(root, "second", [])

    def test_claim_refuses_existing_physical_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "chunks").mkdir()
            (root / "chunks/data").write_text("legacy")
            with self.assertRaisesRegex(ValueError, "persisted objects"):
                adapter.claim_load(root, "corpus", [])
            self.assertFalse((root / "load-started.json").exists())

    def test_optional_diagnostics_preserve_three_tuple_and_native_stats(self):
        manifest = {**CONTRACT, "min_timestamp_ns": T, "max_timestamp_ns": T}
        stats = {"summary": {"execTime": 0.123}}
        response = Mock()
        response.json.return_value = {"status": "success", "data": {"resultType": "vector", "result": [], "stats": stats}}
        diagnostics = {}
        with patch.object(adapter, "load_receipt", return_value=CONTRACT), patch.object(adapter, "api", return_value=response), \
                patch.object(adapter.time, "perf_counter", side_effect=[10., 11., 13., 14., 18.]), \
                patch.object(adapter.time, "process_time", side_effect=[1., 2., 3.]):
            rows, elapsed, native = adapter.query_response({"kind": "count"}, manifest, diagnostics=diagnostics)
        self.assertEqual(rows, [{"count": 0}])
        self.assertEqual(elapsed, 2.)
        self.assertIs(native, stats)
        self.assertEqual(stats, {"summary": {"execTime": 0.123}})
        self.assertEqual(diagnostics, {"success": True, "error_type": None, "http_elapsed_seconds": 2.,
            "full_normalized_wall_seconds": 8., "total_client_cpu_seconds": 2.,
            "normalization_wall_seconds": 4., "normalization_cpu_seconds": 1.})
        with patch.object(adapter, "load_receipt", return_value=CONTRACT), patch.object(adapter, "api", return_value=response):
            self.assertEqual(len(adapter.query_response({"kind": "count"}, manifest)), 3)

    def test_timeout_and_decode_failure_are_partial_diagnostics_not_success(self):
        manifest = {**CONTRACT, "min_timestamp_ns": T, "max_timestamp_ns": T}
        diagnostics = {}
        with patch.object(adapter, "load_receipt", return_value=CONTRACT), patch.object(adapter, "api", side_effect=requests.Timeout):
            with self.assertRaises(requests.Timeout):
                adapter.query_response({"kind": "count"}, manifest, diagnostics=diagnostics)
        self.assertFalse(diagnostics["success"])
        self.assertEqual(diagnostics["error_type"], "Timeout")
        self.assertIsNone(diagnostics["normalization_wall_seconds"])
        class TrialDeadline(BaseException):
            pass
        deadline = TrialDeadline("external cap")
        with patch.object(adapter, "load_receipt", return_value=CONTRACT), patch.object(adapter, "api", side_effect=deadline):
            with self.assertRaises(TrialDeadline) as caught:
                adapter.query_response({"kind": "count"}, manifest, diagnostics=diagnostics)
        self.assertIs(caught.exception, deadline)
        self.assertFalse(diagnostics["success"])
        self.assertEqual(diagnostics["error_type"], "TrialDeadline")
        self.assertIsNone(diagnostics["normalization_wall_seconds"])
        response = Mock()
        response.json.return_value = {"status": "success", "data": {"resultType": "streams", "result": [
            {"stream": {}, "values": [[str(T), "legacy raw"]]}]}}
        with patch.object(adapter, "load_receipt", return_value=CONTRACT), patch.object(adapter, "api", return_value=response):
            with self.assertRaises(ValueError):
                adapter.query_response({"kind": "logs"}, manifest, diagnostics=diagnostics)
        self.assertFalse(diagnostics["success"])
        self.assertIsNotNone(diagnostics["normalization_wall_seconds"])
        class LateDeadline(dict):
            def get(self, key, default=None):
                raise deadline
        response.json.return_value = {"status": "success", "data": LateDeadline(resultType="vector", result=[])}
        with patch.object(adapter, "load_receipt", return_value=CONTRACT), patch.object(adapter, "api", return_value=response):
            with self.assertRaises(TrialDeadline) as caught:
                adapter.query_response({"kind": "count"}, manifest, diagnostics=diagnostics)
        self.assertIs(caught.exception, deadline)
        self.assertFalse(diagnostics["success"])
        self.assertEqual(diagnostics["error_type"], "TrialDeadline")


@unittest.skipUnless(importlib.util.find_spec("pyarrow"), "pyarrow needed")
class PreflightTests(unittest.TestCase):
    def write(self, root, bodies, name="part_é.parquet"):
        import pyarrow as pa
        import pyarrow.parquet as pq
        path = root / name
        pq.write_table(pa.Table.from_pylist([document(body) for body in bodies]), path, row_group_size=3)
        return path

    def test_vectorized_fast_path_and_exact_near_limit_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write(Path(tmp), ["\0" * 100, "😀" * 100, "small"] * 3000)
            with patch.object(ingest, "encode_frame", wraps=encode_frame) as exact:
                self.assertEqual(ingest.preflight([path])["rows"], 9000)
                exact.assert_not_called()
            # Conservative 6x bound fails, but actual ASCII frame fits.
            path = self.write(Path(tmp), ["small", "x" * 200000])
            with patch.object(ingest, "encode_frame", wraps=encode_frame) as exact:
                ingest.preflight([path])
                self.assertEqual(exact.call_count, 1)
                self.assertEqual(exact.call_args.args[1], "part_é.parquet:1")
            path = self.write(Path(tmp), ["\0" * 180000])
            with self.assertRaisesRegex(ValueError, "physical frame"):
                ingest.preflight([path])

    def test_six_byte_bound_threshold(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            identity = "part_é.parquet:0"
            threshold = (MAX_LINE_BYTES - frame_overhead(identity)) // 6
            path = self.write(root, ["\0" * threshold])
            with patch.object(ingest, "encode_frame", wraps=encode_frame) as exact:
                ingest.preflight([path])
                exact.assert_not_called()
            self.write(root, ["\0" * (threshold + 1)])
            with patch.object(ingest, "encode_frame", wraps=encode_frame) as exact:
                with self.assertRaisesRegex(ValueError, "physical frame"):
                    ingest.preflight([path])
                self.assertEqual(exact.call_count, 1)

    def test_entire_corpus_line_preflight_before_any_push(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write(root, ["good"], "part_000.parquet")
            self.write(root, ["\0" * 180000], "part_999.parquet")
            with patch.dict(os.environ, {"LOKI_LOAD_CLIENT": "python"}), patch.object(ingest, "push") as push:
                with self.assertRaisesRegex(ValueError, "physical frame"):
                    ingest.ingest_directory(root, "http://unused", {**CONTRACT,
                        "source_files": ingest.source_inventory(sorted(root.glob("part_*.parquet")))})
                push.assert_not_called()

    def test_source_receipt_mismatch_rejected_before_client(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = self.write(root, ["good"])
            receipt = {**CONTRACT, "source_files": ingest.source_inventory([path])}
            self.write(root, ["different source bytes"])
            with patch.object(ingest, "ingest_python_directory") as python, patch.object(ingest.subprocess, "run") as rust:
                for client in ("python", "rust"):
                    with patch.dict(os.environ, {"LOKI_LOAD_CLIENT": client}):
                        with self.assertRaisesRegex(ValueError, "source inventory"):
                            ingest.ingest_directory(root, "http://unused", receipt)
                python.assert_not_called(); rust.assert_not_called()

    def test_rust_flags_and_summary_handshake(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write(root, ["good"], "part_000.parquet")
            expected = ingest.preflight([root / "part_000.parquet"])
            # The actual old client rejects these unknown flags before pushes;
            # separately reject a client returning an old/raw success summary.
            with patch.dict(os.environ, {"LOKI_LOAD_CLIENT": "rust", "LOKI_CLIENT_BIN": sys.executable}), \
                    patch.object(ingest.subprocess, "run", return_value=Mock(stdout=json.dumps(expected))) as run:
                with self.assertRaisesRegex(ValueError, "handshake"):
                    ingest.ingest_directory(root, "http://unused", {**CONTRACT,
                        "source_files": ingest.source_inventory(sorted(root.glob("part_*.parquet")))})
                command = run.call_args.args[0]
                self.assertEqual(command[command.index("--line-encoding") + 1], ENCODING_FLAG)
                self.assertEqual(command[command.index("--destination-id") + 1], CONTRACT["destination_id"])
                self.assertEqual(command[command.index("--max-request-bytes") + 1], str(MAX_REQUEST_BYTES))
