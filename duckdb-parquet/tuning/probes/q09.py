import duckdb, time, re
c = duckdb.connect(config={"threads": 16, "memory_limit": "48GB"})
c.execute("SET parquet_metadata_cache=true; SET TimeZone=\x27UTC\x27")
c.execute("CREATE VIEW otel_logs AS SELECT * FROM read_parquet(\x27/mnt/local/corpus/otel_logs_100m/part_000.parquet\x27)")
c.execute(open("/tmp/macros-v6.sql").read())
mm = ["error","failed","charge","cache"]; pairs = [(a,b) for i,a in enumerate(mm) for b in mm[i+1:]]
sumt = "(" + " + ".join(f"sb_term(Body, \x27{t}\x27)::INT" for t in mm) + ") >= 2"
pre = "sb_re(Body, array_to_string(list_transform(%s, lambda t: sb_ci(t)), \x27|\x27))" % mm
V = {"now (6 sb_all)": "(" + " OR ".join(f"sb_all(Body, [\x27{a}\x27, \x27{b}\x27])" for a,b in pairs) + ")",
     "alt has prefilter + sum": f"{pre} AND {sumt}"}
for k, p in V.items():
    best = 9e9
    for _ in range(2):
        t = time.perf_counter(); n = c.execute(f"SELECT count(*) FROM otel_logs WHERE {p}").fetchone()[0]; best = min(best, time.perf_counter() - t)
    print(f"{k:26s} {best:6.2f}s {n}", flush=True)
