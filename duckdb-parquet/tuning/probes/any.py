import duckdb, time
c = duckdb.connect(config={"threads": 16, "memory_limit": "48GB"})
c.execute("SET parquet_metadata_cache=true")
c.execute("CREATE VIEW otel_logs AS SELECT * FROM read_parquet(\x27/mnt/local/corpus/otel_logs_100m/part_000.parquet\x27)")
c.execute(open("/tmp/macros-head.sql").read())
S="[^a-zA-Z0-9\\x{0130}\\x{212A}]"
def ci(t): return "".join("[iI\\x{0130}]" if ch=="i" else "[kK\\x{212A}]" if ch=="k" else f"[{ch}{ch.upper()}]" if ch.isalpha() else ch for ch in t)
terms=["error","failed"]; alt="|".join(ci(t) for t in terms)
T = {
 "now sb_any": f"sb_any(Body, {terms})",
 "alt prefilter only": f"regexp_matches(Body, \x27{alt}\x27)",
 "boundary alt only": f"regexp_matches(Body, \x27(^|{S})({alt})($|{S})\x27)",
 "OR of sb_has": " OR ".join(f"sb_has(Body, \x27{t}\x27)" for t in terms),
 "OR of sb_term": " OR ".join(f"sb_term(Body, \x27{t}\x27)" for t in terms),
 "(?i) alt prefilter": "regexp_matches(Body, \x27(?i)error|fa[iI\\x{0130}]led\x27)",
 "(?i) literal error": "regexp_matches(Body, \x27(?i)error\x27)",
 "two literal contains OR": "contains(Body,\x27error\x27) OR contains(Body,\x27failed\x27)",
}
for k, p in T.items():
    best = 9e9
    for _ in range(2):
        t = time.perf_counter(); n = c.execute(f"SELECT count(*) FROM otel_logs WHERE {p}").fetchone()[0]; best = min(best, time.perf_counter() - t)
    print(f"{k:24s} {best:6.2f}s  {n}", flush=True)
