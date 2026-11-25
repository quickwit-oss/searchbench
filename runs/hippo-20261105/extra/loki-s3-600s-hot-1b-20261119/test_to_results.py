#!/usr/bin/env python3
"""Checks of to_results.py and the committed Loki S3 1B 600 s cold/hot/hot column.

    python3 -m unittest -v test_to_results   (from this directory)
"""
import gzip
import json
import statistics
import unittest
from pathlib import Path

import to_results

HERE = Path(__file__).resolve().parent
LOCAL = HERE.parents[2] / "2026-11-01-m8gd-4xlarge-1b-local-nocache/extra/loki-600s-hot-1b-20261118"
UNSUPPORTED = 36
CHUNK_OBJECTS = 109810


def committed():
    return json.loads(to_results.results_path().read_text())


def read_by_query(path):
    """{query: {try: trial}} of a round-1 trials.jsonl.gz."""
    by = {}
    with gzip.open(path, "rt") as fh:
        for t in map(json.loads, fh):
            by.setdefault(t["query"], {})[t["try"]] = t
    return by


def trials():
    return read_by_query(HERE / "raw/round1/trials.jsonl.gz")


def cell(qid):
    r = committed()
    i = [t["id"] for t in r["query_tags"]].index(qid)
    return r["result"][i], r["cpu"][i]


def s3_entry():
    return json.loads(to_results.S3_REQUESTS.read_text())[to_results.KEY]


class CommittedFiles(unittest.TestCase):
    def test_committed_files_are_the_converter_output(self):
        for path, text in to_results.outputs().items():
            self.assertEqual(path.read_text(), text, path.name)

    def test_shape_is_cold_hot_hot_with_a_600_s_cap(self):
        r = committed()
        self.assertEqual(r["timeout_cap"], 600)
        self.assertEqual([t["id"] for t in r["query_tags"]], [f"Q{i:02d}" for i in range(1, 93)])
        self.assertEqual((len(r["result"]), len(r["cpu"])), (92, 92))
        unsupported = [i for i, row in enumerate(r["result"]) if row == [None] * 3]
        self.assertEqual(len(unsupported), UNSUPPORTED)
        self.assertTrue(all(r["cpu"][i] == [None] * 3 for i in unsupported))
        supported = [i for i in range(92) if i not in unsupported]
        self.assertTrue(all(all(isinstance(v, (int, float)) for v in r["result"][i]) for i in supported),
                        "every supported query has 3 numbers (timeouts at the cap)")
        capped = {r["query_tags"][i]["id"]: r["result"][i] for i in supported if 600 in r["result"][i]}
        self.assertEqual(capped, {"Q14": [600, 600, 600], "Q29": [600, 600, 600]})
        nulls = [(r["query_tags"][i]["id"], k) for i in supported for k in range(3) if r["cpu"][i][k] is None]
        self.assertEqual(nulls, [("Q14", 1), ("Q14", 2), ("Q29", 1), ("Q29", 2)], "cpu null only for tries not run")

    def test_identity_fields(self):
        r = committed()
        self.assertEqual(r["system"], "Loki (3.7.9-m8gd4-1b-s3-frame-prefilter-on-c16-600s-20261119)")
        self.assertEqual((r["dataset"], r["date"], r["version"], r["tags"]),
                         ("otel_logs_1b", "2026-11-19", "3.7.9 d3b4a2f7", ["Go", "Loki"]))
        self.assertEqual(r["build"], "3.7.9-m8gd.4xlarge-s3-frame-1b-20261119")
        self.assertEqual(r["storage_units"], {"count": 109811, "unit": "objects"})
        self.assertEqual((r["load_time"], r["data_size"]), (5255.952, 74847449747))
        load = json.loads(to_results.load_record_path().read_text())
        self.assertEqual(load["build"], r["build"])
        self.assertEqual((load["load_cpu"], load["load_cpu_containers"], load["load_cpu_procs"]),
                         (43775.942, 31689.557, 12086.385))

    def test_s3_requests_entry_replaces_the_2026_11_15_one(self):
        current = json.loads(to_results.S3_REQUESTS.read_text())
        self.assertNotIn(to_results.SUPERSEDED_KEY, current)
        s3 = current[to_results.KEY]
        self.assertTrue(s3["partial"])
        self.assertEqual(s3["source"], json.loads(
            (HERE.parent / "loki-s3-1b-20261115/raw/phase2/collated/s3_requests.json").read_text()
        )[to_results.SUPERSEDED_KEY]["source"], "same source and method as the 2026-11-15 entry")
        self.assertEqual(len(s3["queries"]), 56)
        self.assertEqual(sorted(q for q, t in s3["queries"].items() if t == [None] * 3), ["Q14", "Q29"])
        self.assertTrue(all(len(t) == 3 and all(p[0] > 0 and p[1] is None for p in t)
                            for q, t in s3["queries"].items() if q not in ("Q14", "Q29")), "bytes not collected")
        self.assertEqual(s3["queries"]["Q01"], [[CHUNK_OBJECTS, None]] * 3)
        self.assertEqual(s3["queries"]["Q68"], [[8281, None], [8399, None], [8431, None]])


class RecomputedCells(unittest.TestCase):
    """Cells recomputed straight from the raw trials, independently of the converter."""

    def test_ok_query_uses_client_http_seconds_and_cpu_per_try(self):
        t = trials()["Q01"]
        self.assertEqual(cell("Q01"), ([t[k]["latency_seconds"] for k in (1, 2, 3)],
                                       [round(t[k]["engine_cpu_seconds"], 3) for k in (1, 2, 3)]))
        self.assertEqual(cell("Q01")[0], [211.494015, 212.368488, 211.14078])

    def test_cold_timeouts_are_the_cap_and_hot_tries_are_padded(self):
        for q, cpu in (("Q14", 8781.729), ("Q29", 8803.903)):
            t = trials()[q]
            self.assertEqual([t[k]["status"] for k in (1, 2, 3)],
                             ["timeout", "skipped-after-cold-timeout", "skipped-after-cold-timeout"], q)
            self.assertEqual((t[1]["timeout_source"], t[1]["censored_at_seconds"]), ("driver-cap", 600))
            self.assertEqual(cell(q), ([600, 600, 600], [cpu, None, None]))

    def test_unsupported_slot(self):
        self.assertEqual(cell("Q09"), ([None] * 3, [None] * 3))

    def test_hot_tries_read_what_the_cold_try_read(self):
        for q, t in trials().items():
            for k in (2, 3):
                if t[k]["status"] == "ok":
                    self.assertEqual(t[k]["native"]["totalLinesProcessed"], t[1]["native"]["totalLinesProcessed"], (q, k))
                    self.assertEqual(t[k]["answer"]["multiset_sha256"], t[1]["answer"]["multiset_sha256"], (q, k))

    def test_best_hot_over_cold(self):
        ratios = {q: min(t[2]["latency_seconds"], t[3]["latency_seconds"]) / t[1]["latency_seconds"]
                  for q, t in trials().items() if all(t[k]["status"] == "ok" for k in (1, 2, 3))}
        self.assertEqual(len(ratios), 54)
        self.assertAlmostEqual(statistics.median(ratios.values()), 0.998, places=3)
        self.assertEqual(sorted(q for q, v in ratios.items() if v < 0.9), ["Q68", "Q73", "Q76", "Q81"])
        self.assertLess(max(ratios.values()), 1.012)

    def test_s3_against_local_2026_11_18(self):
        local, s3 = read_by_query(LOCAL / "raw/round1/trials.jsonl.gz"), trials()
        self.assertEqual(sorted(local), sorted(s3))
        both = [q for q in s3 if s3[q][1]["status"] == "ok" and local[q][1]["status"] == "ok"]
        self.assertEqual(sorted(set(s3) - set(both)), ["Q14", "Q29"])
        ratios = [s3[q][1]["latency_seconds"] / local[q][1]["latency_seconds"] for q in both]
        self.assertAlmostEqual(statistics.median(ratios), 1.096, places=3)
        self.assertGreater(min(ratios), 1.05, "S3 is slower cold on every query")
        # Same answers on both storages, every finished try.
        for q in both:
            shas = {t["answer"]["multiset_sha256"] for t in list(s3[q].values()) + list(local[q].values())
                    if t["status"] == "ok"}
            self.assertEqual(len(shas), 1, q)
        self.assertEqual(local["Q14"][1]["status"], "ok", "Q14 finished cold locally (599.5 s), not on S3")

    def test_metric_query_gets_are_downloaded_chunks(self):
        by = {(t["query"], t["try"]): g for t, g, _ in to_results.try_gets(
            to_results.read_trials(), to_results.read_calls())}
        for q, ts in trials().items():
            if q < "Q68":
                for k, t in ts.items():
                    if t["status"] == "ok":
                        extra = 1 if (q, k) == ("Q21", 3) else 0     # one error-labelled call
                        self.assertEqual(by[q, k], t["native"]["chunksDownloaded"] + extra, (q, k))
        self.assertEqual((by["Q14", 1], by["Q29", 1]), (109411, 109513), "lower bounds at the kill: no cell")


class Mapping(unittest.TestCase):
    def tri(self, k, status, **extra):
        base = {"query": "Q", "try": k, "kind": "cold" if k == 1 else "hot",
                "cycle": {"cache": "dropped"} if k == 1 else None, "cap_seconds": 600,
                "status": status, "latency_seconds": None, "engine_cpu_seconds": None}
        return {**base, **extra}

    def test_parse_timing_takes_last_bare_number(self):
        self.assertEqual(to_results.parse_timing("x\nSEARCHBENCH_ROWS=3\n 2.250000 \n"), "2.250000")

    def test_unmapped_status_is_refused(self):
        with self.assertRaises(ValueError):
            to_results.query_row([self.tri(1, "error", engine_cpu_seconds=1.0),
                                  self.tri(2, "ok"), self.tri(3, "ok")])

    def test_a_try_after_a_timeout_must_not_have_run(self):
        timeout = self.tri(1, "timeout", timeout_source="driver-cap", censored_at_seconds=600, engine_cpu_seconds=1.0)
        with self.assertRaises(AssertionError):
            to_results.query_row([timeout, self.tri(2, "ok", latency_seconds=1.0), self.tri(3, "skipped-after-cold-timeout")])
        self.assertEqual(to_results.query_row([timeout, self.tri(2, "skipped-after-cold-timeout"),
                                               self.tri(3, "skipped-after-cold-timeout")]),
                         (["600", "600", "600"], ["1.000", "null", "null"]))

    def scrape(self, seq, role, cpu, start, gets):
        return {"seq": seq, "role": role, "cpu": str(cpu),
                "scrape": {"ok": True, "process_start": start, "getobject": gets}}

    def test_gets_pair_with_executed_tries_and_sum_status_labels(self):
        cold = self.tri(1, "ok", engine_cpu_seconds=2.0)
        hot = self.tri(2, "ok", engine_cpu_seconds=1.0)
        skipped = self.tri(3, "not-run-after-timeout")
        calls = [self.scrape(1, "before", 0.5, 10.0, {}), self.scrape(2, "after", 2.5, 10.0, {"200": 7.0, "500": 1.0}),
                 self.scrape(3, "before", 2.5, 10.0, {"200": 7.0, "500": 1.0}),
                 self.scrape(4, "after", 3.5, 10.0, {"200": 12.0, "500": 1.0})]
        self.assertEqual([(t["try"], g) for t, g, _ in to_results.try_gets([cold, hot, skipped], calls)], [(1, 8), (2, 5)])

    def test_gets_refuse_a_cpu_mismatch_or_a_process_change(self):
        cold = self.tri(1, "ok", engine_cpu_seconds=2.0)
        hot = self.tri(2, "ok", engine_cpu_seconds=1.0)
        good = [self.scrape(1, "before", 0.0, 10.0, {}), self.scrape(2, "after", 2.0, 10.0, {"200": 1.0}),
                self.scrape(3, "before", 2.0, 10.0, {"200": 1.0}), self.scrape(4, "after", 3.0, 10.0, {"200": 2.0})]
        to_results.try_gets([cold, hot], good)
        for i, change in ((1, {"cpu": "2.5"}), (2, {"scrape": {**good[2]["scrape"], "process_start": 11.0}})):
            bad = [dict(c) for c in good]
            bad[i].update(change)
            with self.assertRaises(AssertionError):
                to_results.try_gets([cold, hot], bad)


if __name__ == "__main__":
    unittest.main()
