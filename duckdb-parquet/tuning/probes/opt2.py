import duckdb, time
c = duckdb.connect(config={"threads": 16, "memory_limit": "48GB"})
c.execute("SET parquet_metadata_cache=true")
c.execute("CREATE VIEW otel_logs AS SELECT * FROM read_parquet(\x27/mnt/local/corpus/otel_logs_100m/part_000.parquet\x27)")
c.execute(open("/tmp/macros-head.sql").read())
# Pigeonhole check on the generated balls: every edit sequence keeps one piece as consecutive literal atoms.
for d, pieces in ((1, ["con","nec","tion"]), (2, ["co","nn","ec","ti","on"])):
    bad = c.execute(f"SELECT count(*) FROM (SELECT unnest(sb_ball(\x27connection\x27, {d})) b) WHERE NOT list_bool_or(list_transform({pieces}, lambda p: contains(array_to_string(list_transform(b, lambda x: CASE WHEN x = \x27.\x27 THEN \x27\x01\x27 ELSE x END), \x27\x27), p)))").fetchone()[0]
    print(f"ball d={d}: atom lists without an intact piece: {bad}", flush=True)
def run(name, where, ref=None):
    best = 9e9
    for _ in range(2):
        t = time.perf_counter(); n = c.execute(f"SELECT count(*) FROM otel_logs WHERE {where}").fetchone()[0]; best = min(best, time.perf_counter() - t)
    print(f"{name:34s} {best:6.2f}s  {n}" + ("" if ref is None else ("  SAME" if n == ref else "  DIFF!")), flush=True); return n
t8 = ["payment","exception","refused","send","confirmation","email","expected","deadline"]
r = run("Q08 now sb_any 8", f"sb_any(Body, {t8})")
run("Q08 OR of 8 sb_term", " OR ".join(f"sb_term(Body, \x27{t}\x27)" for t in t8), r)
mm = ["error","failed","charge","cache"]
r = run("Q09 now (6 sb_all)", "(" + " OR ".join(f"sb_all(Body, [\x27{a}\x27, \x27{b}\x27])" for i,a in enumerate(mm) for b in mm[i+1:]) + ")")
run("Q09 sum of 4 sb_term >= 2", "(" + " + ".join(f"sb_term(Body, \x27{t}\x27)::INT" for t in mm) + ") >= 2", r)
run("Q09 OR-has prefilter + sum", "(" + " OR ".join(f"sb_has(Body, \x27{t}\x27)" for t in mm) + ") AND (" + " + ".join(f"sb_term(Body, \x27{t}\x27)::INT" for t in mm) + ") >= 2", r)
r = run("Q19 now token_re c.che", "sb_token_re(Body, \x27[cC]\x27 || sb_alnum() || \x27[cC][hH][eE]\x27)")
run("Q19 + sb_has che", "sb_has(Body, \x27che\x27) AND sb_token_re(Body, \x27[cC]\x27 || sb_alnum() || \x27[cC][hH][eE]\x27)", r)
r = run("Q22 now fuzzy 1", "sb_fuzzy(Body, \x27connection\x27, 1)")
run("Q22 pieces con|nec|tion + fuzzy", "(sb_has(Body,\x27con\x27) OR sb_has(Body,\x27nec\x27) OR sb_has(Body,\x27tion\x27)) AND sb_fuzzy(Body, \x27connection\x27, 1)", r)
run("  rows with a d=1 piece", "sb_has(Body,\x27con\x27) OR sb_has(Body,\x27nec\x27) OR sb_has(Body,\x27tion\x27)")
r = run("Q23 now fuzzy 2", "sb_fuzzy(Body, \x27connection\x27, 2)")
run("Q23 pieces co|nn|ec|ti|on + fuzzy", "(" + " OR ".join(f"sb_has(Body,\x27{p}\x27)" for p in ["nn","ec","ti","co","on"]) + ") AND sb_fuzzy(Body, \x27connection\x27, 2)", r)
run("  rows with a d=2 piece", " OR ".join(f"sb_has(Body,\x27{p}\x27)" for p in ["nn","ec","ti","co","on"]))
