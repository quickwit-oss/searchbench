"""Literal prefilter (default on): a provable superset of the exact Body predicates."""
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from gen_queries import Unsupported, bounded, translate
from ingest import entry, push
from logql import (PREFILTER_CHARS, build_request, literal_prefilter_enabled, literal_prefilters,
                     normalize, quote, required_literal)
from test_adapter import CONTRACT, completed_manifest

T = 1758585600000000000
SPECS = [json.loads(line) for line in (HERE / "queries.logql").read_text().splitlines() if line.startswith("{")]
# Every supported spec's chosen literals. OR branches and negations have none.
EXPECTED = {
    "Q01": ["error"], "Q02": ["payment"], "Q03": ["led", "order"],
    "Q04": ["led", "charge", "card", "cache"],
    "Q05": ["led", "send", "order", "conf", "ema", "serv", "expected", "post"],
    "Q06": [], "Q07": [], "Q08": [], "Q10": ["place"], "Q11": ["place"], "Q12": ["expected"], "Q14": [],
    "Q15": ["place", "charge"], "Q16": ["charg"], "Q17": ["ord"], "Q18": ["conn"], "Q19": ["che"],
    "Q20": ["conn"], "Q21": ["charg"], "Q25": ["conn"], "Q26": [], "Q27": ["nnec"], "Q28": ["error"],
    "Q29": [], "Q30": ["error"], "Q31": ["led"], "Q32": [], "Q54": [], "Q55": ["charge"],
    "Q56": ["led", "order"], "Q57": [], "Q58": ["charg"], "Q60": [], "Q61": ["error"],
    "Q62": ["led", "order"], "Q63": ["place"], "Q64": [], "Q65": ["error"], "Q66": ["led"], "Q67": [],
    "Q68": ["led", "order"], "Q69": [], "Q70": ["error"], "Q71": ["place"], "Q72": ["charge"], "Q73": [],
    "Q74": ["charg"], "Q75": [], "Q76": ["led", "order"], "Q77": [], "Q78": ["error"], "Q79": ["place"],
    "Q80": ["charge"], "Q81": [], "Q82": ["charg"], "Q83": [],
}
# Body edge cases for the native superset check. Mixed case, U+0130/U+212A/U+017F,
# Greek sigma forms, sharp s, length-changing lowercase, JSON-escaped
# neighbors, ligatures and literals split by escapes or delimiters.
ADVERSARIAL = [
    "ERROR", "eRrOr", "Error:", "terror", "error_x", "errors", "err\x00or", "err\nor", "err\"or",
    '"error"', "\\error\\", "\terror\r", "error\u0301", "éerror", "error\u200dx",
    "FAİLED order", "failed ORDER", "faİled", "FAILED", "fa\u0130led", "faıled", "ﬁled", "FAILED\nTO\tPLACE—ORDER",
    "\u212Aafka", "kafka", "KAFKA", "ka\u212Afka", "\u212A", "k",
    # Kelvin sign inside the chosen literal "afka": the exact path matches, so
    # the native (?i) prefilter must keep these rows.
    "kaf\u212Aa", "KAF\u212AA", "x kaf\u212Aa y",
    "ſend", "SEND", "ſ", "σ ς Σ", "ß ẞ SS ss", "İnfo", "info", "INFO",
    "\u0130\u0130\u0130", "ȺȾ", "ⱥⱦ", "café CAFÉ", "CHARGE charge Charged chargeback",
    "conn CONNECTION connection-pool", "C\u0130CHE cache CACHE cAche", "place ORDER failed TO",
    "post to email service expected 200 got 500", "POST TO EMAIL SERVICE EXPECTED 200 GOT 500",
    "\\u0065rror", "error\\n", "", "\n", "雪 😀 error", "\u212Aelvin-afka \u212Aafka",
]


def custom_specs():
    """Translate adversarial DSL shapes through the production translator."""
    nodes = [
        {"match": {"Body": "kafka"}}, {"match": {"Body": "info"}}, {"match": {"Body": "send"}},
        {"match": {"Body": "café"}}, {"match": {"Body": "σ"}}, {"match": {"Body": "ss"}},
        {"match": {"Body": {"query": "failed order", "operator": "and"}}},
        {"match": {"Body": {"query": "kafka info", "operator": "and"}}},
        {"match_phrase": {"Body": "failed to place order"}}, {"match_phrase": {"Body": "kafka send"}},
        {"prefix": {"Body": "kaf"}}, {"prefix": {"Body": "conn"}}, {"wildcard": {"Body": "*afka"}},
        {"wildcard": {"Body": "c?che"}}, {"wildcard": {"Body": "k*a"}}, {"regexp": {"Body": "c.che"}},
        {"bool": {"should": [{"match": {"Body": "kafka"}}, {"match": {"Body": "error"}}]}},
        {"bool": {"should": [{"match": {"Body": "kafka"}}]}},
        {"bool": {"must": [{"match": {"Body": "error"}}], "must_not": [{"match": {"Body": "kafka"}}]}},
        {"bool": {"must_not": [{"match": {"Body": "error"}}]}},
    ]
    specs = []
    for index, node in enumerate(nodes):
        for task, extra in (("count", {}), ("recent", {"size": 500})):
            specs.append(translate(100 + index, f"-- Q{100 + index} task={task}", json.dumps({"query": node, **extra})))
    return specs


class LiteralPrefilterUnitTests(unittest.TestCase):
    def manifest(self):
        return {**CONTRACT, "min_timestamp_ns": T, "max_timestamp_ns": T + 10}

    def test_default_on_and_env_switch(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(literal_prefilter_enabled())
        with patch.dict(os.environ, {"LOKI_LITERAL_PREFILTER": "on"}):
            self.assertTrue(literal_prefilter_enabled())
        with patch.dict(os.environ, {"LOKI_LITERAL_PREFILTER": "off"}):
            self.assertFalse(literal_prefilter_enabled())
        for bad in ("1", "true", "ON", "OFF", "0", ""):
            with patch.dict(os.environ, {"LOKI_LITERAL_PREFILTER": bad}), self.assertRaises(ValueError):
                literal_prefilter_enabled()

    def test_off_requests_byte_identical_to_pre_prefilter_translator(self):
        # Frozen build_request output of the translator before the prefilter was
        # added, for every supported spec, with and without a trusted
        # completed-load manifest.
        frozen = json.loads((HERE / "tests/fixtures/requests_prefilter_off.json").read_text())
        self.assertEqual(sorted(frozen), sorted(s["id"] for s in SPECS))
        for env in ({}, {"LOKI_LITERAL_PREFILTER": "off"}):
            with patch.dict(os.environ, env, clear=True):
                for spec in SPECS:
                    for name, manifest in (("plain", self.manifest()), ("completed", completed_manifest(self.manifest()))):
                        expected = tuple(frozen[spec["id"]][name])
                        self.assertEqual(build_request(spec, manifest, literal_prefilter=False), expected, spec["id"])
                        if env:
                            self.assertEqual(build_request(spec, manifest), expected, spec["id"])

    def test_default_requests_prefiltered_and_prefilter_only_inserts_stages(self):
        with patch.dict(os.environ, {}, clear=True):
            for spec in SPECS:
                default = build_request(spec, self.manifest())
                self.assertEqual(default, build_request(spec, self.manifest(), literal_prefilter=True))
                off = build_request(spec, self.manifest(), literal_prefilter=False)
                self.assertNotIn("(?i)", off[1]["query"])
                stages = literal_prefilters(spec.get("filters", []))
                self.assertEqual("(?i)" in default[1]["query"], bool(stages), spec["id"])
                selector = off[1]["query"].split("} ", 1)
                self.assertEqual(default[1]["query"],
                                 selector[0] + "}" + "".join(" " + s for s in stages) + " " + selector[1], spec["id"])

    def test_all_supported_specs_literals(self):
        self.assertEqual(len(SPECS), 56)
        actual = {s["id"]: [json.loads(f[3:])[4:] for f in literal_prefilters(s["filters"])] for s in SPECS}
        self.assertEqual(actual, EXPECTED)

    def test_every_literal_is_a_substring_of_its_exact_pattern_matches(self):
        # Necessary condition on the lowered Body: the chosen literal occurs in
        # every string the exact regex accepts. Test against generated matches.
        samples = ["error", "failed", "order", "charge", "cache", "conn", "connection", "place", "post",
                   "send", "confirmation", "email", "service", "expected", "payment", "card", "chargeback"]
        for spec in SPECS:
            for text_filter in spec["filters"]:
                literal = required_literal(text_filter)
                if literal is None:
                    continue
                # Python re approximation of the RE2 classes, exact for these ASCII samples.
                pattern = json.loads(text_filter[3:]).replace(r"\p{P}\p{S}", r"\W_")
                pattern = re.sub(r"\\x\{([0-9a-f]+)\}", lambda m: "\\u" + m[1].zfill(4), pattern)
                matched = 0
                for a in samples:
                    for b in samples:
                        for body in (a, f"{a} {b}", f"{a} to {b}", f"{b} {a}", "failed to place order",
                                     "failed send order confirmation email service expected post",
                                     "failed charge card cache", "post to email service expected 200 got 500"):
                            if re.search(pattern, body):
                                matched += 1
                                self.assertIn(literal, body, (spec["id"], text_filter, body))
                self.assertGreater(matched, 0, spec["id"])

    def test_unsafe_shapes_have_no_prefilter(self):
        unsafe = [
            '!~ ' + quote(bounded("error")),             # negation
            '|~ ' + quote(bounded("error|failed")),      # OR branches
            '|~ ' + quote(bounded("e(rr)or")),           # group
            '|~ ' + quote(bounded("erro*r")),            # quantified literal
            '|~ ' + quote(bounded("err?or")),
            '|~ ' + quote(bounded("err{2}or")),
            '|~ ' + quote(bounded("err.or")),            # wildcard dot
            '|~ ' + quote(bounded(r"err\dor")),          # class escape
            '|~ ' + quote(bounded(r"err\por")),
            '|~ ' + quote(bounded("[e]rror")),           # character class
            '|~ ' + quote(bounded("^error")),
            '|~ ' + quote("error"),                      # not bounded(...)
            '|= ' + quote("error"), '|~ ' + quote(bounded("ab")),  # too short
            '|~ ' + quote(bounded("iii")), '|~ ' + quote(bounded("kkk")),
        ]
        for text_filter in unsafe:
            with self.subTest(text_filter=text_filter):
                self.assertIsNone(required_literal(text_filter))
                self.assertEqual(literal_prefilters([text_filter]), [])

    def test_literal_alphabet_excludes_go_lower_to_ascii_runes(self):
        # Go strings.ToLower: U+0130 -> "i", U+212A -> "k" (Python differs for U+0130).
        self.assertNotIn("i", PREFILTER_CHARS)
        self.assertEqual(required_literal('|~ ' + quote(bounded("kafka"))), "afka")
        self.assertEqual(required_literal('|~ ' + quote(bounded("failed"))), "led")
        self.assertEqual(required_literal('|~ ' + quote(bounded("info"))), "nfo")
        self.assertEqual(required_literal('|~ ' + quote(bounded("café"))), "caf")
        self.assertIsNone(required_literal('|~ ' + quote(bounded("σσσσ"))))
        self.assertEqual(required_literal('|~ ' + quote(bounded(r"foo\-barbaz"))), "barbaz")
        self.assertEqual(required_literal('|~ ' + quote(bounded("order" + "[^x]*"))), None)

    def test_custom_specs_translate_and_classify(self):
        specs = custom_specs()
        self.assertEqual(len(specs), 40)
        by_id = {}
        for spec in specs:
            by_id.setdefault(spec["id"], []).append(literal_prefilters(spec["filters"]))
        self.assertEqual(by_id["Q116"][0], [])          # OR of two terms
        self.assertEqual(by_id["Q117"][0], ['|~ "(?i)afka"'])
        self.assertEqual(by_id["Q118"][0], ['|~ "(?i)error"'])  # positive only, not must_not
        self.assertEqual(by_id["Q119"][0], [])          # negation only
        with self.assertRaises(Unsupported):
            translate(1, "-- Q1 task=count", json.dumps({"query": {"fuzzy": {"Body": {"value": "x"}}}}))


@unittest.skipUnless(os.environ.get("LOKI_TEST_BINARY"), "set LOKI_TEST_BINARY")
class NativeLiteralPrefilterTests(unittest.TestCase):
    """Native Loki: prefilter on/off answers identical; Go lower rune audit."""

    @classmethod
    def setUpClass(cls):
        from test_raw_body_gate import NativeLoki
        cls.tmp = tempfile.TemporaryDirectory(prefix="loki-literal-prefilter-")
        cls.addClassCleanup(cls.tmp.cleanup)
        cls.root = Path(cls.tmp.name)
        if os.environ.get("LOKI_TEST_ARTIFACTS"):
            destination = Path(os.environ["LOKI_TEST_ARTIFACTS"]) / cls.root.name
            cls.addClassCleanup(lambda: shutil.copytree(cls.root, destination, dirs_exist_ok=True))
        cls.loki = NativeLoki(cls.root)
        cls.addClassCleanup(cls.loki.log.close)
        cls.addClassCleanup(cls.loki.stop)
        cls.loki.start("all")
        import requests
        cls.docs = []
        for i, body in enumerate(ADVERSARIAL * 2):  # duplicates are distinct rows
            cls.docs.append({"Timestamp": T + i, "Body": body, "ServiceName": ["frontend", "checkout"][i % 2],
                             "SeverityText": ["INFO", "ERROR"][i % 2], "SeverityNumber": [9, 17][i % 2],
                             "ScopeName": "s" + str(i % 3)})
        # Every Unicode scalar value, chunked, in a separate stream (Go lower audit).
        scalars = [c for c in range(0x110000) if not 0xD800 <= c <= 0xDFFF]
        cls.audit = ["".join(map(chr, scalars[i:i + 1000])) for i in range(0, len(scalars), 1000)]
        with requests.Session() as session:
            push(session, cls.loki.url, [entry(d, d["Timestamp"], f"adv:{i}") for i, d in enumerate(cls.docs)])
            push(session, cls.loki.url, [entry({"Body": body, "ServiceName": "audit", "SeverityText": "INFO"},
                                               T + 10_000 + i, f"audit:{i}") for i, body in enumerate(cls.audit)])
        cls.loki.flush_new_chunk()
        cls.loki.stop()
        cls.loki.start("read,query-scheduler")
        cls.manifest = completed_manifest({**CONTRACT, "min_timestamp_ns": T, "max_timestamp_ns": T + len(cls.docs) - 1})
        cls.manifest["rows"] = len(cls.docs)

    def answer(self, spec, literal_prefilter, manifest=None):
        manifest = manifest or self.manifest
        path, params = build_request(spec, manifest, literal_prefilter=literal_prefilter)
        data = self.loki.get(path, params)
        self.assertEqual(data["data"]["stats"]["ingester"]["totalReached"], 0)
        return normalize(spec, data, manifest), params["query"]

    def test_go_lower_maps_only_u0130_and_u212a_to_ascii_letters(self):
        # Exact Go strings.ToLower of every scalar value, via the production
        # pipeline's own `lower` template stage on the decoded Body.
        # Page by time: one >4 MiB response exceeds Loki's internal gRPC limit.
        query = ('{dataset="otel_logs",service_name="audit"} | json sb_original="body"'
                 ' | line_format `{{ lower .sb_original }}` | drop sb_original')
        lowered = {}
        for start in range(0, len(self.audit), 100):
            data = self.loki.get("/loki/api/v1/query_range", {"query": query, "start": str(T + 10_000 + start),
                                                              "end": str(T + 10_000 + start + 100), "limit": 1000})
            lowered.update({int(v[0]) - T - 10_000: v[1] for s in data["data"]["result"] for v in s["values"]})
        self.assertEqual(len(lowered), len(self.audit))
        to_ascii = {}
        for index, original in enumerate(self.audit):
            low = lowered[index]
            self.assertEqual(len(low), len(original), "Go lower must be one rune per rune")
            for a, b in zip(original, low):
                if ord(a) > 127 and ord(b) < 128:
                    to_ascii[f"U+{ord(a):04X}"] = b
        # Pre-existing: Loki's json extractor maps U+FFFD to a space. Both are
        # analyzer delimiters and neither is a prefilter literal character.
        self.assertEqual(to_ascii, {"U+0130": "i", "U+212A": "k", "U+FFFD": " "})
        (self.root / "go-lower-to-ascii.json").write_text(json.dumps(to_ascii))

    def test_prefilter_answers_identical_on_adversarial_bodies(self):
        specs = SPECS + custom_specs()
        specs += [{**s, "kind": "logs", "limit": 500} for s in SPECS if s["kind"] != "logs" and s["filters"]]
        compared = with_prefilter = 0
        for spec in specs:
            spec = {k: v for k, v in spec.items() if k != "window"}
            with self.subTest(spec=spec["id"], kind=spec["kind"]):
                off, _ = self.answer(spec, False)
                on, query = self.answer(spec, True)
                self.assertCountEqual(off, on)
                compared += 1
                with_prefilter += "(?i)" in query
        self.assertGreater(with_prefilter, 60)
        self.assertEqual(compared, len(specs))

    def test_exact_path_matches_go_fold_cases_the_prefilter_must_keep(self):
        # Exact lowered-Body semantics count U+212A "afka" as "kafka" and
        # U+0130 "nfo" as "info". The trimmed prefilters still keep them.
        for node, body in [({"match": {"Body": "kafka"}}, "\u212Aafka"), ({"match": {"Body": "info"}}, "İnfo")]:
            spec = translate(1, "-- Q1 task=recent", json.dumps({"query": node, "size": 500}))
            rows, query = self.answer(spec, True)
            self.assertIn(body, [r["Body"] for r in rows])
            self.assertNotIn('"(?i)kafka"', query); self.assertNotIn('"(?i)info"', query)

    def test_native_case_insensitive_regex_is_not_equivalent(self):
        # RE2 (?i) folds ſ to s (Go lower does not) and does not fold İ to i
        # (Go lower does). A raw-line (?i) token regex is therefore NOT a
        # drop-in replacement for lower + exact regex; record the counter-examples.
        counts = {}
        for word, body in (("send", "ſend"), ("info", "İnfo")):
            spec = translate(1, "-- Q1 task=recent", json.dumps({"query": {"match": {"Body": word}}, "size": 500}))
            exact, _ = self.answer(spec, False)
            pattern = json.loads(spec["filters"][0][3:])
            query = ('{dataset="otel_logs"} | json sb_original="body" | line_format `{{ .sb_original }}` |~ '
                     + quote("(?i)" + pattern))
            data = self.loki.get("/loki/api/v1/query_range", {"query": query, "start": str(T),
                                                              "end": str(T + len(self.docs)), "limit": 500})
            native = [v[1] for s in data["data"]["result"] for v in s["values"]]
            counts[word] = {"exact_has": body in [r["Body"] for r in exact], "native_has": body in native}
        self.assertEqual(counts, {"send": {"exact_has": False, "native_has": True},
                                  "info": {"exact_has": True, "native_has": False}})


if __name__ == "__main__":
    unittest.main()
