"""clickhouse/query: data caches and statistics are off by default, overridable."""
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
OFF = ["--use_text_index_tokens_cache=0", "--use_text_index_negative_tokens_cache=0",
       "--use_text_index_pattern_bypass_cache=0", "--use_text_index_postings_cache=0",
       "--use_statistics=0",
       "--use_statistics_for_part_pruning=0", "--use_statistics_for_min_max_aggregation=0"]


class QuerySettingsTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root)
        shutil.copy(HERE / "query", self.root / "query")
        self.calls = self.root / "calls"
        # Fake client: record argv, answer one row and a timing on stderr.
        client = self.root / "clickhouse"
        # Like the real client, reject an option given twice.
        client.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$MOCK_CALLS"\n'
                          'dups=$(printf "%s\\n" "$@" | grep -- "^--" | sed "s/=.*//" | sort | uniq -d)\n'
                          'if [ -n "$dups" ]; then echo "Bad arguments: option $dups cannot be specified more than once" >&2; exit 2; fi\n'
                          'echo 1\necho 0.010 >&2\n')
        client.chmod(0o755)
        self.env = dict(os.environ, CLICKHOUSE_BIN=str(client), MOCK_CALLS=str(self.calls))
        for key in ["CH_DATA_CACHES", "CH_QUERY_SETTINGS"]:
            self.env.pop(key, None)

    def run_query(self, **env):
        result = subprocess.run(["bash", str(self.root / "query")], input="SELECT 1",
                                env=dict(self.env, **env), text=True, capture_output=True)
        return result, (self.calls.read_text().splitlines() if self.calls.exists() else [])

    def test_result_and_data_caches_and_statistics_off_by_default(self):
        result, argv = self.run_query()
        self.assertEqual(result.returncode, 0, result.stderr)
        for flag in ["--use_query_cache=0", "--use_query_condition_cache=0", *OFF]:
            self.assertIn(flag, argv)
        # The header cache holds metadata only (the dictionary's sparse index): it stays on.
        self.assertFalse(any(a.startswith("--use_text_index_header_cache") for a in argv), argv)

    def test_data_caches_on_restores_engine_defaults_but_keeps_result_caches_off(self):
        result, argv = self.run_query(CH_DATA_CACHES="on")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--use_query_cache=0", argv)
        self.assertIn("--use_query_condition_cache=0", argv)
        for flag in OFF:
            self.assertNotIn(flag, argv)

    def test_explicit_query_settings_replace_the_defaults_without_duplicates(self):
        on = "--use_statistics=1 --use_statistics_for_part_pruning=1 --use_statistics_for_min_max_aggregation=1"
        result, argv = self.run_query(CH_QUERY_SETTINGS=on)
        self.assertEqual(result.returncode, 0, result.stderr)
        for flag in on.split():
            self.assertIn(flag, argv)
            self.assertNotIn(flag[:-1] + "0", argv)
        for flag in OFF[:4]:   # the text-index data caches stay off
            self.assertIn(flag, argv)
        names = [a.split("=")[0] for a in argv if a.startswith("--")]
        self.assertEqual(len(names), len(set(names)), names)

    def test_invalid_mode_is_rejected_before_querying(self):
        result, argv = self.run_query(CH_DATA_CACHES="typo")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("CH_DATA_CACHES must be on or off", result.stderr)
        self.assertEqual(argv, [])


if __name__ == "__main__":
    unittest.main()
