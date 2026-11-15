#!/usr/bin/env python3
"""HOST (and LOCAL re-run). Pair phase-2 wrapper records with lib/benchmark.sh results.

  collate_phase2.py RESULTS_JSON HARNESS_LOG GETS_DIR ANSWERS_DIR REFERENCES_JSON OUT_DIR

Pairing: lib/benchmark.sh calls ./cpu-usage once before and once after every try
(after only if before printed a value), and ./query once per try, all in order.
Both wrappers label each call with the last "==> Q<n>" harness line (n = 1-based
position in result[]/query_tags[]). Per query n the k-th query call, the k-th
cpu-usage pair and result[n-1][k-1] are the same try. Every pair must be in one
Loki process (process_start_time_seconds equal); GETs = after - before.

Outputs in OUT_DIR:
  s3_requests.json  {"<dataset>/<system>": {source, partial, queries: {Qxx: [[gets, null] | null x3]}}}
                    GETs only for tries that returned (timed-out/err/unrun -> null).
                    partial = true iff a supported query lacks cold + first-hot GETs.
  tries.jsonl       one record per executed try (timeout-window GETs kept, lower bound, diagnostic)
  answers.json      verdict per successful try (answers.py); mismatches = P0
  checks.json       consistency checks
Exit 2 on any answer mismatch, 1 on a consistency failure, else 0.
"""
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import answers  # noqa: E402
import getscrape  # noqa: E402

SOURCE = ("Native GetObject SDK-call deltas, same trials as wall/CPU; bytes not collected. "
          "Internal HTTP retries may not be separately counted.")


def jsonl(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()] if Path(path).exists() else []


def main(results_path, harness_log, gets_dir, answers_dir, refs_path, out_dir):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    res = json.loads(Path(results_path).read_text())
    refs = json.loads(Path(refs_path).read_text())["queries"]
    log = Path(harness_log).read_text(errors="replace")
    timeouts = {(int(q), int(t)) for q, t in re.findall(r"\[WARN\]  Q(\d+) try (\d+): exceeded", log)}
    problems = []

    pairs, pending = {}, None
    for c in sorted(jsonl(Path(gets_dir) / "calls.jsonl"), key=lambda c: c["seq"]):
        if c["role"] == "before":
            if pending is not None:
                problems.append(f"unpaired before seq {pending['seq']}")
            pending = c if c["cpu"] else None
            if not c["cpu"]:
                pairs.setdefault(int(c["harness_q"]), []).append((c, None))
        else:
            if pending is None or pending["harness_q"] != c["harness_q"]:
                problems.append(f"after without before seq {c['seq']}")
                pending = None
                continue
            pairs.setdefault(int(c["harness_q"]), []).append((pending, c))
            pending = None
    if pending is not None:
        problems.append(f"trailing before seq {pending['seq']}")
    qcalls = {}
    for c in sorted(jsonl(Path(answers_dir) / "calls.jsonl"), key=lambda c: c["seq"]):
        qcalls.setdefault(int(c["harness_q"]), []).append(c)

    tags, timings, cpus = res["query_tags"], res["result"], res.get("cpu") or []
    queries, tries_out, verdicts, mismatches = {}, [], [], []
    for n, tag in enumerate(tags, 1):
        qid = tag["id"]
        calls, qpairs = qcalls.get(n, []), pairs.get(n, [])
        if len(calls) != len(qpairs):
            problems.append(f"Q{n} {qid}: {len(calls)} query calls vs {len(qpairs)} cpu-usage pairs")
        if qid not in refs:  # UNSUPPORTED sentinel
            continue
        per_try = [None] * 3
        for k, call in enumerate(calls, 1):
            stdout_path = Path(answers_dir) / f"{call['seq']}.stdout"
            stdout = stdout_path.read_text(errors="replace") if stdout_path.exists() else ""
            timing = timings[n - 1][k - 1] if n - 1 < len(timings) and k - 1 < len(timings[n - 1]) else None
            cpu = cpus[n - 1][k - 1] if n - 1 < len(cpus) and cpus[n - 1] and k - 1 < len(cpus[n - 1]) else None
            status = "timeout" if (n, k) in timeouts else ("err" if timing is None else "ok")
            rec = {"query": qid, "qnum": n, "try": k, "status": status, "latency": timing, "cpu": cpu,
                   "query_seq": call["seq"], "gets": None, "gets_by_status": None, "gets_note": None}
            pair = qpairs[k - 1] if k - 1 < len(qpairs) else None
            if pair is None or pair[1] is None:
                rec["gets_note"] = "no cpu-usage pair (engine not running at before-call)"
            else:
                b, a = pair[0]["scrape"], pair[1]["scrape"]
                rec.update(cpu_seq=[pair[0]["seq"], pair[1]["seq"]], process_start=a.get("process_start"))
                if not (b.get("ok") and a.get("ok")):
                    rec["gets_note"] = f"scrape failed: {b.get('error')} / {a.get('error')}"
                elif b["process_start"] != a["process_start"] or b["process_start"] is None:
                    rec["gets_note"] = "process changed inside try"
                else:
                    try:
                        d = getscrape.delta(b["getobject"], a["getobject"])
                        rec.update(gets=sum(d.values()), gets_by_status=d)
                        if status != "ok":
                            rec["gets_note"] = "lower bound: try did not return (diagnostic only)"
                    except ValueError as exc:
                        rec["gets_note"] = str(exc)
                if status == "ok" and cpu is not None:
                    pair_cpu = float(pair[1]["cpu"]) - float(pair[0]["cpu"])
                    if abs(max(pair_cpu, 0) - cpu) > 0.0015:
                        problems.append(f"{qid} try {k}: harness cpu {cpu} != wrapper pair {pair_cpu:.3f}")
            if status == "ok":
                v = answers.verdict(refs[qid], stdout)
                rec["answer"] = v
                verdicts.append({"query": qid, "try": k, **v})
                if v["verdict"] == "MISMATCH":
                    mismatches.append({"query": qid, "try": k, **v})
                if rec["gets"] is not None:
                    per_try[k - 1] = [rec["gets"], None]
            tries_out.append(rec)
        queries[qid] = per_try
    partial = any(t[0] is None or t[1] is None for t in queries.values())
    key = f"{res['dataset']}/{res['system']}"
    (out / "s3_requests.json").write_text(json.dumps(
        {key: {"source": SOURCE, "partial": True if partial else None, "queries": queries}}, indent=1) + "\n")
    with (out / "tries.jsonl").open("w") as f:
        for rec in tries_out:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
    summary = {"verified_ok_tries": sum(v["verdict"] != "MISMATCH" for v in verdicts),
               "verdict_counts": {k: sum(v["verdict"] == k for v in verdicts)
                                  for k in ("match", "match-tie-order", "MISMATCH")},
               "mismatches": mismatches, "verdicts": verdicts}
    (out / "answers.json").write_text(json.dumps(summary, indent=1) + "\n")
    checks = {"supported_queries": len(queries), "executed_tries": len(tries_out),
              "ok_tries": sum(r["status"] == "ok" for r in tries_out),
              "timeout_tries": sum(r["status"] == "timeout" for r in tries_out),
              "err_tries": sum(r["status"] == "err" for r in tries_out),
              "ok_tries_with_gets": sum(r["status"] == "ok" and r["gets"] is not None for r in tries_out),
              "queries_with_cold_and_hot_gets": sum(t[0] is not None and t[1] is not None for t in queries.values()),
              "partial": partial, "answer_mismatches": len(mismatches), "problems": problems}
    (out / "checks.json").write_text(json.dumps(checks, indent=1) + "\n")
    print(json.dumps({k: v for k, v in checks.items() if k != "problems"}), f"problems={len(problems)}")
    for p in problems[:20]:
        print("PROBLEM", p)
    return 2 if mismatches else (1 if problems else 0)


if __name__ == "__main__":
    raise SystemExit(main(*sys.argv[1:7]))
