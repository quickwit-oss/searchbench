import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import adapter
from config import make_config
from ingest import preflight, ingest_file


class SafetyTests(unittest.TestCase):
    def test_query_layout_validation_detects_changes(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {"LOKI_STORAGE": "filesystem", "LOKI_DATA_DIR": tmp}):
            root = Path(tmp)
            receipt = adapter.claim_load(root, "corpus", [])
            (root / "chunks").mkdir()
            (root / "chunks/one").write_bytes(b"persisted")
            (root / "manifest.json").write_text(json.dumps({**receipt, "persisted_inventory": adapter.object_inventory()}))
            adapter.verify_layout()
            (root / "chunks/two").write_bytes(b"unexpected index upload")
            with self.assertRaisesRegex(ValueError, "changed during query"):
                adapter.verify_layout()

    def test_query_concurrency_and_prefilter_recorded_for_results(self):
        def fake_run(*args, check=True):
            return SimpleNamespace(returncode=1 if args[1] == "inspect" else 0, stdout="", stderr="")
        # (concurrency, LOKI_LITERAL_PREFILTER, expected prefilter): default is on.
        for concurrency, prefilter, enabled in ((None, None, True), ("8", "on", True), (None, "off", False)):
            env = {"LOKI_STORAGE": "filesystem"}
            if concurrency:
                env["LOKI_QUERY_CONCURRENCY"] = concurrency
            if prefilter:
                env["LOKI_LITERAL_PREFILTER"] = prefilter
            with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {**env, "LOKI_DATA_DIR": tmp}, clear=True), \
                    patch.object(adapter, "run", side_effect=fake_run), patch.object(adapter, "check"):
                adapter.start()
                runtime = json.loads((Path(tmp) / "runtime.json").read_text())
                expected = int(concurrency or 16)
                self.assertEqual(runtime["query_concurrency"], expected)
                self.assertIs(runtime["literal_prefilter"], enabled)
                self.assertEqual(json.loads((Path(tmp) / "config.json").read_text())["querier"]["max_concurrent"], expected)
                self.assertEqual(adapter.query_settings(), {"line_encoding": "searchbench-loki-json-frame-v1",
                                                            "literal_prefilter": enabled,
                                                            "query_concurrency": expected})

    def test_invalid_prefilter_setting_fails_before_container_changes(self):
        with tempfile.TemporaryDirectory() as tmp, \
                patch.dict(os.environ, {"LOKI_STORAGE": "filesystem", "LOKI_DATA_DIR": tmp,
                                        "LOKI_LITERAL_PREFILTER": "true"}, clear=True), \
                patch.object(adapter, "run") as run:
            with self.assertRaisesRegex(ValueError, "LOKI_LITERAL_PREFILTER"):
                adapter.start()
            run.assert_not_called()
            self.assertFalse((Path(tmp) / "runtime.json").exists())

    def test_unrelated_container_is_never_stopped(self):
        info = [{"Config": {"Labels": None}, "State": {"Running": True}}]
        with patch.object(adapter, "run", return_value=SimpleNamespace(returncode=0, stdout=json.dumps(info))) as run:
            with self.assertRaisesRegex(ValueError, "unrelated"):
                adapter.stop()
            self.assertEqual(run.call_count, 1)

    def test_load_claim_is_exclusive_locally(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {"LOKI_STORAGE": "filesystem"}):
            adapter.claim_load(Path(tmp), "corpus", [])
            with self.assertRaises(FileExistsError):
                adapter.claim_load(Path(tmp), "corpus", [])

    @unittest.skipUnless(importlib.util.find_spec("boto3"), "boto3 needed for S3 mock")
    def test_stale_s3_prepare_does_not_allow_second_load(self):
        objects = []
        client = Mock()
        def put(**kwargs):
            self.assertEqual(kwargs["IfNoneMatch"], "*")
            objects.append({"Key": kwargs["Key"], "Size": len(kwargs["Body"])})
        client.put_object.side_effect = put
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {"LOKI_STORAGE": "s3", "LOKI_S3_BUCKET": "b", "LOKI_S3_PREFIX": "searchbench/loki/test/"}), patch.object(adapter, "s3_objects", side_effect=lambda: iter(objects)), patch("boto3.client", return_value=client):
            roots = [Path(tmp) / "a", Path(tmp) / "b"]
            for root in roots:
                with patch.dict(os.environ, {"LOKI_DATA_DIR": str(root)}):
                    adapter.prepare()
            receipt = adapter.claim_load(roots[0], "first-corpus", [])
            self.assertEqual(json.loads(client.put_object.call_args.kwargs["Body"]), receipt)
            with self.assertRaisesRegex(ValueError, "no longer empty"):
                adapter.claim_load(roots[1], "second-corpus", [])
            self.assertEqual(client.put_object.call_count, 1)
            self.assertFalse((roots[1] / "load-started.json").exists())

    @unittest.skipUnless(os.environ.get("LOKI_TEST_BINARY"), "set LOKI_TEST_BINARY")
    def test_s3_config_validates_against_pinned_binary(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {"LOKI_STORAGE": "s3", "LOKI_S3_BUCKET": "test-bucket", "LOKI_S3_PREFIX": "searchbench/loki/test/"}):
            config = Path(tmp) / "config.json"
            config.write_text(json.dumps(make_config()))
            result = subprocess.run([os.environ["LOKI_TEST_BINARY"], "-config.file=" + str(config), "-verify-config=true"], capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)


@unittest.skipUnless(importlib.util.find_spec("pyarrow"), "pyarrow needed for input validation")
class ParquetSafetyTests(unittest.TestCase):
    def test_parallel_ordering_is_validated_before_push(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        with tempfile.TemporaryDirectory() as tmp:
            files = [Path(tmp) / f"part_{i}.parquet" for i in range(2)]
            base = 1758585600000000000
            for path, ts in zip(files, [base + 3600 * 10**9, base]):
                pq.write_table(pa.table({"Timestamp": pa.array([ts], type=pa.timestamp("ns")),
                                         "Body": [""], "ServiceName": ["s"], "SeverityText": ["INFO"]}), path)
            with self.assertRaisesRegex(ValueError, "spans >=1h"):
                preflight(files)
            pq.write_table(pa.table({"Timestamp": pa.array([base + 123], type=pa.timestamp("ns")),
                                     "Body": [""], "ServiceName": ["s"], "SeverityText": ["INFO"]}), files[0])
            self.assertEqual(preflight(files), {"rows": 2, "min_timestamp_ns": base, "max_timestamp_ns": base + 123})

    def test_missing_and_duplicate_timestamp_rejected(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "part_0.parquet"
            pq.write_table(pa.table({"Body": ["error"], "OtherInt": [1758585600000000000]}), path)
            with patch("ingest.push") as push:
                with self.assertRaises((ValueError, KeyError)):
                    ingest_file((str(path), "http://unused", 1))
                push.assert_not_called()
            duplicate = pa.Table.from_arrays([pa.array([1]), pa.array([2])], names=["Timestamp", "Timestamp"])
            pq.write_table(duplicate, path)
            with self.assertRaisesRegex(ValueError, "exactly one Timestamp"):
                preflight([path])

    def test_null_timestamp_rejected(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "part_0.parquet"
            pq.write_table(pa.table({"Timestamp": pa.array([None], type=pa.timestamp("ns"))}), path)
            with self.assertRaisesRegex(ValueError, "null Timestamp"):
                preflight([path])
