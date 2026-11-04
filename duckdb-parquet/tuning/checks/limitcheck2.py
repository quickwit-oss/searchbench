import duckdb, re
RR = "/opt/searchbench-duckdb-20261101T001507Z"
ORIG = "/mnt/local/corpus/otel_logs_100m/part_000.parquet"; COPY = "/mnt/local/rewrite/rw-svcts-122k/part_000.parquet"
def qs(p):
    out = {}; cur = None
    for l in open(p):
        m = re.match(r"--\s*(Q\d+)", l)
        if m: cur = m.group(1); continue
        if l.startswith("--") or not l.strip(): continue
        if cur: out[cur] = l.strip().rstrip(";"); cur = None
    return out
QA, QB = qs(RR + "/v1-rewrite/queries.base.sql"), qs(RR + "/v6-anyor/queries.sql")
c = duckdb.connect(config={"threads": 16, "memory_limit": "48GB"}); c.execute("SET TimeZone=\x27UTC\x27; SET temp_directory=\x27/mnt/local/lc-spill\x27")
c.execute(open(RR + "/v1-rewrite/macros.base.sql").read())   # the ORIGINAL token-list helpers
c.execute(f"CREATE VIEW copy AS SELECT * FROM read_parquet(\x27{COPY}\x27)")
c.execute(f"CREATE VIEW orig AS SELECT * FROM read_parquet(\x27{ORIG}\x27)")
c.execute(open(RR + "/v6-anyor/macros.sql").read().replace("CREATE MACRO sb_", "CREATE MACRO nb_").replace("sb_", "nb_"))  # new helpers as nb_*
for q in ["Q77", "Q78", "Q82", "Q83", "Q70", "Q74", "Q75"]:
    a = QA[q]; b = QB[q].replace("sb_", "nb_")
    where_a = re.search(r"WHERE (.*?)( ORDER BY| LIMIT)", a).group(1)
    # Run the copy query, keep all columns of the returned rows (inside DuckDB, no Python round trip),
    # then evaluate the ORIGINAL token-list predicate on exactly those rows.
    inner = b.replace("SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs", "SELECT * FROM copy")
    c.execute(f"CREATE OR REPLACE TEMP TABLE got AS {inner}")
    n, bad = c.execute(f"SELECT count(*), count(*) FILTER (WHERE NOT coalesce({where_a}, false)) FROM got").fetchone()
    # and those rows exist in the original file (all columns)
    missing = c.execute("SELECT count(*) FROM (SELECT * FROM got EXCEPT ALL SELECT * FROM orig)").fetchone()[0] if False else "n/a"
    print(f"{q}: copy returned {n} rows; failing the original predicate: {bad}; rows absent from the original file: {missing}", flush=True)
