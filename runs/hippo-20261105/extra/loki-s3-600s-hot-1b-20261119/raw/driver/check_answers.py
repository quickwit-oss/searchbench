#!/usr/bin/env python3
"""HOST. Every successful try in the given phase dirs vs the 1B references (P0 on mismatch).

references.json (built locally for loki-s3-1b-20261115T123351Z, queries.logql sha
4be0e3b6..., identical in PR #68): per supported query the DuckDB 1B oracle rows
and the ClickHouse 1B http-json normalized value (cross-checked equal on all 56),
plus the multiset sha of prior local Loki 1B answers where one finished.
answers.verdict(): "match" / "match-tie-order" (logs, equal-timestamp order only),
else "MISMATCH". On top, the known 1B values must hold exactly (HARD below).
A query with no reference (none here: all 56 have oracle + ClickHouse) would be
"no-reference" with its sha recorded, never a match. Exit 2 on any mismatch.

  check_answers.py OUT.json PHASE_DIR...
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import answers  # noqa: E402

REFS = json.loads((Path(__file__).resolve().parent / "references.json").read_text())["queries"]
Q01 = 93_638_388
HARD_COUNT = {"Q01": Q01, "Q04": 54_410_241, "Q25": 19_554_788, "Q27": 19_539_754}
HARD_MULTISET = {"Q68": "7d7fc16677eff73d6c12658fb7d362c3ec79dde301d88d642acfc780ca32b2ac",
                 "Q72": "ce1a8229537315b0c77b3cb918dfd4898ad52778e6435185eea88adc26454c4a"}


def hard_check(query, stdout, sha):
    rows = [json.loads(line) for line in stdout.splitlines() if line.strip()]
    if query in HARD_COUNT:
        return rows == [{"count": HARD_COUNT[query]}]
    if query == "Q61":
        return len(rows) == 30 and sum(r["count"] for r in rows) == Q01
    if query in HARD_MULTISET:
        return sha == HARD_MULTISET[query]
    return None


def main(out, phase_dirs):
    tries, bad = [], []
    for phase in map(Path, phase_dirs):
        trials = phase / "trials.jsonl"
        if not trials.is_file():
            continue
        for line in trials.read_text().splitlines():
            r = json.loads(line)
            base = {"phase": phase.name, "query": r["query"], "variant": r["variant"], "round": r["round"],
                    "try": r["try"], "status": r["status"]}
            if r["status"] != "ok":
                tries.append({**base, "verdict": "unverified"})
                continue
            stdout = (phase / "answers" / f"{r['query']}-{r['variant']}-r{r['round']}-t{r['try']}.stdout").read_text()
            sha = answers.multiset_sha(stdout)
            if sha != r["answer"]["multiset_sha256"]:
                raise AssertionError(f"{phase}/{r['query']}: stored answer differs from trials.jsonl")
            ref = REFS.get(r["query"])
            if ref is None or ref.get("clickhouse") is None:
                tries.append({**base, "multiset_sha256": sha, "verdict": "no-reference"})
                continue
            checks = answers.verdict(ref, stdout)
            hard = hard_check(r["query"], stdout, sha)
            verdict = checks["verdict"] if hard is not False else "MISMATCH"
            entry = {**base, **checks, "known_1b_value_check": hard, "verdict": verdict}
            tries.append(entry)
            if verdict == "MISMATCH":
                bad.append(entry)
    ok = [t for t in tries if t["verdict"] in ("match", "match-tie-order")]
    result = {"references": "references.json (DuckDB 1B oracle + ClickHouse 1B http-json; prior Loki multiset)",
              "hard": {"count": HARD_COUNT, "Q61": "30 buckets summing to Q01", "multiset": HARD_MULTISET},
              "tries": tries, "mismatches": bad, "verified_ok_tries": len(ok),
              "no_reference_tries": sum(t["verdict"] == "no-reference" for t in tries)}
    Path(out).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"verified_ok_tries": len(ok), "mismatches": len(bad),
                      "no_reference": result["no_reference_tries"]}))
    return 2 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2:]))
