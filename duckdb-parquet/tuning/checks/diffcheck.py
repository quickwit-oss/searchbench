# diffcheck.py <parquet> <macrosA> <queriesA> <macrosB> <queriesB> <out.json>
# Full results of every supported query under A and B (same file, same session settings),
# compared row by row as canonical text; row order is compared only when the query has ORDER BY.
import duckdb, hashlib, json, re, sys, time
pq, mA, qA, mB, qB, out = sys.argv[1:7]
def queries(p):
    res, cur = {}, None
    for line in open(p):
        m = re.match(r'--\s*(Q\d+)', line)
        if m: cur = m.group(1); continue
        if line.startswith('--') or not line.strip(): continue
        if cur: res[cur] = line.strip().rstrip(';'); cur = None
    return res
def conn(m):
    c = duckdb.connect(); c.execute("SET threads=16; SET memory_limit='48GB'; SET TimeZone='UTC'")
    c.execute(f"CREATE VIEW otel_logs AS SELECT * FROM read_parquet('{pq}')"); c.execute(open(m).read()); return c
cA, cB = conn(mA), conn(mB); QA, QB = queries(qA), queries(qB); rep = {}
for q in QA:
    if QA[q].startswith('UNSUPPORTED'): continue
    r = {}
    for k, c, s in (('A', cA, QA[q]), ('B', cB, QB[q])):
        t = time.perf_counter(); rows = c.execute(s).fetchall(); d = time.perf_counter() - t
        txt = [repr(x) for x in rows]
        if not re.search(r'\bORDER BY\b', s, re.I): txt.sort()
        r[k] = {'rows': len(rows), 'sha256': hashlib.sha256('\n'.join(txt).encode()).hexdigest(), 's': round(d, 3)}
    r['same'] = r['A']['sha256'] == r['B']['sha256']; rep[q] = r
    print(q, 'SAME' if r['same'] else 'DIFF', r['A']['rows'], r['A']['s'], r['B']['s'], flush=True)
json.dump(rep, open(out, 'w'), indent=1)
print('identical', sum(v['same'] for v in rep.values()), 'of', len(rep))
