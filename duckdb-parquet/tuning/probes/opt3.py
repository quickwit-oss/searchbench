import duckdb, time
c = duckdb.connect(config={"threads": 16, "memory_limit": "48GB"})
c.execute("SET parquet_metadata_cache=true")
c.execute("CREATE VIEW otel_logs AS SELECT * FROM read_parquet(\x27/mnt/local/corpus/otel_logs_100m/part_000.parquet\x27)")
c.execute(open("/tmp/macros-head.sql").read())
# Candidate: sb_any as OR of sb_term in up to 3 slots, alternation beyond (CASE on a constant should fold).
c.execute("""CREATE MACRO sb_any_v(body, terms) AS CASE WHEN len(terms) <= 3 THEN
    sb_term(body, terms[1]) OR (len(terms) >= 2 AND sb_term(body, terms[2])) OR (len(terms) >= 3 AND sb_term(body, terms[3]))
    ELSE sb_any(body, terms) END""")
c.execute("""CREATE MACRO sb_phrase_all(body, phrase) AS
    list_bool_and(list_transform(string_split(phrase, \x27 \x27), lambda w: sb_has(body, w))) AND sb_phrase(body, phrase)""")
def run(name, where, ref=None):
    best = 9e9
    for _ in range(2):
        t = time.perf_counter(); n = c.execute(f"SELECT count(*) FROM otel_logs WHERE {where}").fetchone()[0]; best = min(best, time.perf_counter() - t)
    print(f"{name:36s} {best:6.2f}s  {n}" + ("" if ref is None else ("  SAME" if n == ref else "  DIFF!")), flush=True); return n
for terms in (["error","failed"], ["error","failed","charge"], ["connection","request","conversion"], ["connection","request","conversion","post"]):
    r = run(f"sb_any {len(terms)} now", f"sb_any(Body, {terms})")
    run(f"sb_any {len(terms)} OR/alt macro", f"sb_any_v(Body, {terms})", r)
    run(f"sb_any {len(terms)} explicit OR", " OR ".join(f"sb_term(Body, \x27{t}\x27)" for t in terms), r)
for ph, rare in (("place order", "place"), ("failed to place order", "place"), ("post to email service expected 200 got 500", "500")):
    r = run(f"phrase now: {ph[:22]}", f"sb_phrase(Body, \x27{ph}\x27)")
    run("  all words has + phrase", f"sb_phrase_all(Body, \x27{ph}\x27)", r)
    run(f"  has {rare} + phrase", f"sb_has(Body, \x27{rare}\x27) AND sb_phrase(Body, \x27{ph}\x27)", r)
