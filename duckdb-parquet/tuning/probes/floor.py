import duckdb, time
c = duckdb.connect(config={"threads": 16, "memory_limit": "48GB"})
c.execute("SET TimeZone=\x27UTC\x27; SET parquet_metadata_cache=true")
c.execute("CREATE VIEW otel_logs AS SELECT * FROM read_parquet(\x27/mnt/local/corpus/otel_logs_100m/part_000.parquet\x27)")
c.execute(open("/tmp/macros-head.sql").read())
T = {
 "count(*)": "SELECT count(*) FROM otel_logs",
 "read Body: sum(length)": "SELECT sum(length(Body)) FROM otel_logs",
 "read Body: strlen": "SELECT sum(strlen(Body)) FROM otel_logs",
 "contains literal error": "SELECT count(*) FROM otel_logs WHERE contains(Body, \x27error\x27)",
 "regex literal error": "SELECT count(*) FROM otel_logs WHERE regexp_matches(Body, \x27error\x27)",
 "regex case class error": "SELECT count(*) FROM otel_logs WHERE regexp_matches(Body, \x27[eE][rR][rR][oO][rR]\x27)",
 "sb_term error (Q01)": "SELECT count(*) FROM otel_logs WHERE sb_term(Body, \x27error\x27)",
 "sb_any 2 (Q06)": "SELECT count(*) FROM otel_logs WHERE sb_any(Body, [\x27error\x27, \x27failed\x27])",
 "sb_any 3 (Q07)": "SELECT count(*) FROM otel_logs WHERE sb_any(Body, [\x27connection\x27, \x27request\x27, \x27conversion\x27])",
 "sb_fuzzy 1 (Q22)": "SELECT count(*) FROM otel_logs WHERE sb_fuzzy(Body, \x27connection\x27, 1)",
 "group by SeverityText only": "SELECT SeverityText, count(*) FROM otel_logs GROUP BY 1",
 "group by ScopeName only": "SELECT ScopeName, count(*) FROM otel_logs GROUP BY 1",
}
for k, q in T.items():
    best = 9e9
    for _ in range(2):
        t = time.perf_counter(); r = c.execute(q).fetchall(); best = min(best, time.perf_counter() - t)
    print(f"{k:28s} {best:6.2f}s  {str(r[0])[:40]}", flush=True)
