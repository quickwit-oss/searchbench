-- Adapted from clickhouse/queries.sql; same Q01-Q92 tags and result order.
-- Helpers in macros.sql match tokens at query time; no index or stored tokens. Token
-- patterns (prefix, wildcard, regexp, fuzzy) use the sb_prefix/sb_suffix/sb_infix/
-- sb_token_re/sb_fuzzy helpers, which give the rows of the per-token lambda over
-- sb_tokens(Body) they replace (see macros.sql and test_adapter.py).
-- Q13 and Q33-Q53 are deliberately unsupported (proximity and BM25).
-- DuckDB 2.0 reads the corpus's ns Timestamp as TIMESTAMPTZ_NS, which date_trunc does not
-- take: minute buckets are computed from epoch_ns (truncating, like the other engines).

-- Q01 task=count filter=term freq=hi
SELECT count() FROM otel_logs WHERE sb_term(Body, 'error');
-- Q02 task=count filter=term freq=lo
SELECT count() FROM otel_logs WHERE sb_term(Body, 'payment');
-- Q03 task=count filter=and freq=hi (AND, 2 tokens)
SELECT count() FROM otel_logs WHERE sb_all(Body, ['failed', 'order']);
-- Q04 task=count filter=and freq=hi (AND, 4 tokens)
SELECT count() FROM otel_logs WHERE sb_all(Body, ['failed', 'charge', 'card', 'cache']);
-- Q05 task=count filter=and freq=lo (AND, 8 tokens)
SELECT count() FROM otel_logs WHERE sb_all(Body, ['failed', 'send', 'order', 'confirmation', 'email', 'service', 'expected', 'post']);
-- Q06 task=count filter=or freq=hi (OR, 2 tokens)
SELECT count() FROM otel_logs WHERE sb_any(Body, ['error', 'failed']);
-- Q07 task=count filter=or freq=hi (OR, 4 tokens)
SELECT count() FROM otel_logs WHERE sb_any(Body, ['connection', 'request', 'conversion', 'post']);
-- Q08 task=count filter=or freq=mid (OR, 8 tokens)
SELECT count() FROM otel_logs WHERE sb_any(Body, ['payment', 'exception', 'refused', 'send', 'confirmation', 'email', 'expected', 'deadline']);
-- Q09 task=count filter=or,minmatch freq=hi (>=2 of 4)
SELECT count() FROM otel_logs WHERE (sb_all(Body, ['error', 'failed']) OR sb_all(Body, ['error', 'charge']) OR sb_all(Body, ['error', 'cache']) OR sb_all(Body, ['failed', 'charge']) OR sb_all(Body, ['failed', 'cache']) OR sb_all(Body, ['charge', 'cache']));
-- Q10 task=count filter=phrase freq=mid (phrase, 2 tokens)
SELECT count() FROM otel_logs WHERE sb_phrase(Body, 'place order');
-- Q11 task=count filter=phrase freq=mid (phrase, 4 tokens)
SELECT count() FROM otel_logs WHERE sb_phrase(Body, 'failed to place order');
-- Q12 task=count filter=phrase freq=lo (phrase, 8 tokens)
SELECT count() FROM otel_logs WHERE sb_phrase(Body, 'post to email service expected 200 got 500');
-- Q13 task=count filter=phrase,proximity freq=hi (failed within 2 of order)
UNSUPPORTED: proximity -- no equivalent Lucene slop implementation
-- Q14 task=count filter=phrase,or freq=hi (phrase OR term)
SELECT count() FROM otel_logs WHERE sb_phrase(Body, 'failed to place order') OR sb_term(Body, 'charge');
-- Q15 task=count filter=phrase,and freq=mid (phrase AND term)
SELECT count() FROM otel_logs WHERE sb_phrase(Body, 'failed to place order') AND sb_term(Body, 'charge');
-- Q16 task=count filter=regexp freq=hi (charg.*)
SELECT count() FROM otel_logs WHERE sb_prefix(Body, 'charg');
-- Q17 task=count filter=regexp freq=hi (ord.*)
SELECT count() FROM otel_logs WHERE sb_prefix(Body, 'ord');
-- Q18 task=count filter=regexp freq=mid (conn.*)
SELECT count() FROM otel_logs WHERE sb_prefix(Body, 'conn');
-- Q19 task=count filter=regexp freq=hi (single-char wildcard mid: c.che -> cache)
SELECT count() FROM otel_logs WHERE sb_has(Body, 'che') AND sb_token_re(Body, '[cC]' || sb_alnum() || '[cC][hH][eE]');
-- Q20 task=count filter=prefix freq=mid (conn)
SELECT count() FROM otel_logs WHERE sb_prefix(Body, 'conn');
-- Q21 task=count filter=prefix freq=hi (charg)
SELECT count() FROM otel_logs WHERE sb_prefix(Body, 'charg');
-- Q22 task=count filter=fuzzy freq=mid (levenshtein distance 1)
SELECT count() FROM otel_logs WHERE sb_fuzzy(Body, 'connection', 1);
-- Q23 task=count filter=fuzzy freq=mid (levenshtein distance 2)
SELECT count() FROM otel_logs WHERE sb_fuzzy(Body, 'connection', 2);
-- Q24 task=count filter=fuzzy,prefix freq=mid (levenshtein-2 AND prefix, same 'conn' root)
SELECT count() FROM otel_logs WHERE sb_prefix(Body, 'conn') AND sb_fuzzy(Body, 'connection', 2);
-- Q25 task=count filter=like freq=mid (prefix wildcard conn%)
SELECT count() FROM otel_logs WHERE sb_prefix(Body, 'conn');
-- Q26 task=count filter=like freq=hi (suffix wildcard %tion)
SELECT count() FROM otel_logs WHERE sb_suffix(Body, 'tion');
-- Q27 task=count filter=like freq=mid (middle wildcard %nnec%)
SELECT count() FROM otel_logs WHERE sb_infix(Body, 'nnec');
-- Q28 task=count filter=and,negation freq=hi (error but NOT cache)
SELECT count() FROM otel_logs WHERE sb_term(Body, 'error') AND NOT sb_term(Body, 'cache');
-- Q29 task=count filter=or,negation freq=hi (error/failed, excluding charge)
SELECT count() FROM otel_logs WHERE sb_any(Body, ['error', 'failed']) AND NOT sb_term(Body, 'charge');
-- Q30 task=count filter=term,window freq=hi (term + Timestamp BETWEEN 6h)
SELECT count() FROM otel_logs WHERE sb_term(Body, 'error') AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 06:00:00';
-- Q31 task=count filter=and,window freq=hi (term + service + Timestamp BETWEEN 6h)
SELECT count() FROM otel_logs WHERE ServiceName = 'frontend' AND sb_term(Body, 'failed') AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 06:00:00';
-- Q32 task=count filter=or,window freq=mid (8-token OR within a BETWEEN 6h window)
SELECT count() FROM otel_logs WHERE sb_any(Body, ['payment', 'exception', 'refused', 'send', 'confirmation', 'email', 'expected', 'deadline']) AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 06:00:00';
-- Q33 task=top_k filter=term freq=hi
UNSUPPORTED: top_k BM25 -- no ranking
-- Q34 task=top_k filter=term freq=mid
UNSUPPORTED: top_k BM25 -- no ranking
-- Q35 task=top_k filter=term freq=lo
UNSUPPORTED: top_k BM25 -- no ranking
-- Q36 task=top_k filter=and freq=hi (AND, 2 tokens)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q37 task=top_k filter=and freq=hi (AND, 4 tokens)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q38 task=top_k filter=and freq=lo (AND, 8 tokens)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q39 task=top_k filter=or freq=hi (OR, 2 tokens)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q40 task=top_k filter=or freq=hi (OR, 4 tokens)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q41 task=top_k filter=or freq=mid (OR, 8 tokens)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q42 task=top_k filter=or,minmatch freq=hi (>=2 of 4)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q43 task=top_k filter=phrase freq=mid (phrase, 2 tokens)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q44 task=top_k filter=phrase freq=mid (phrase, 4 tokens)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q45 task=top_k filter=phrase freq=lo (phrase, 8 tokens)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q46 task=top_k filter=regexp freq=mid (conn.*)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q47 task=top_k filter=prefix freq=hi (charg)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q48 task=top_k filter=fuzzy freq=mid (levenshtein distance 1)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q49 task=top_k filter=fuzzy freq=mid (levenshtein distance 2)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q50 task=top_k filter=like freq=mid (prefix wildcard conn%)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q51 task=top_k filter=phrase,or freq=hi (phrase OR term)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q52 task=top_k filter=and,negation freq=hi (error but NOT cache)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q53 task=top_k filter=and,window freq=hi (term + service + Timestamp BETWEEN 6h)
UNSUPPORTED: top_k BM25 -- no ranking
-- Q54 task=group_by filter=or freq=hi (key=SeverityText, ordered)
SELECT SeverityText, count() AS cnt FROM otel_logs WHERE sb_any(Body, ['error', 'failed']) GROUP BY SeverityText ORDER BY cnt DESC;
-- Q55 task=group_by filter=term freq=hi (key=SeverityText, ordered)
SELECT SeverityText, count() AS cnt FROM otel_logs WHERE sb_term(Body, 'charge') GROUP BY SeverityText ORDER BY cnt DESC;
-- Q56 task=group_by filter=and freq=hi (key=SeverityText, NO order by)
SELECT SeverityText, count() AS cnt FROM otel_logs WHERE sb_all(Body, ['failed', 'order']) GROUP BY SeverityText;
-- Q57 task=group_by filter=or freq=hi (key=ScopeName, top 20 ordered)
SELECT ScopeName, count() AS cnt FROM otel_logs WHERE sb_any(Body, ['error', 'failed']) GROUP BY ScopeName ORDER BY cnt DESC LIMIT 20;
-- Q58 task=group_by filter=regexp freq=hi (key=ScopeName)
SELECT ScopeName, count() AS cnt FROM otel_logs WHERE sb_prefix(Body, 'charg') GROUP BY ScopeName ORDER BY cnt DESC LIMIT 20;
-- Q59 task=group_by filter=fuzzy freq=mid (key=ScopeName)
SELECT ScopeName, count() AS cnt FROM otel_logs WHERE sb_fuzzy(Body, 'connection', 1) GROUP BY ScopeName ORDER BY cnt DESC LIMIT 20;
-- Q60 task=group_by filter=or freq=hi (key=ScopeName, NO order by)
SELECT ScopeName, count() AS cnt FROM otel_logs WHERE sb_any(Body, ['error', 'failed']) GROUP BY ScopeName;
-- Q61 task=group_by filter=term freq=hi (key=minute)
SELECT make_timestamptz(epoch_ns(Timestamp) // 60000000000 * 60000000) AS minute, count() AS cnt FROM otel_logs WHERE sb_term(Body, 'error') GROUP BY minute ORDER BY minute;
-- Q62 task=group_by filter=and freq=hi (key=minute)
SELECT make_timestamptz(epoch_ns(Timestamp) // 60000000000 * 60000000) AS minute, count() AS cnt FROM otel_logs WHERE sb_all(Body, ['failed', 'order']) GROUP BY minute ORDER BY minute;
-- Q63 task=group_by filter=phrase freq=mid (key=minute)
SELECT make_timestamptz(epoch_ns(Timestamp) // 60000000000 * 60000000) AS minute, count() AS cnt FROM otel_logs WHERE sb_phrase(Body, 'failed to place order') GROUP BY minute ORDER BY minute;
-- Q64 task=group_by filter=or freq=hi (key=minute)
SELECT make_timestamptz(epoch_ns(Timestamp) // 60000000000 * 60000000) AS minute, count() AS cnt FROM otel_logs WHERE sb_any(Body, ['error', 'failed', 'charge']) GROUP BY minute ORDER BY minute;
-- Q65 task=group_by filter=term freq=hi (key=minute, NO order by)
SELECT make_timestamptz(epoch_ns(Timestamp) // 60000000000 * 60000000) AS minute, count() AS cnt FROM otel_logs WHERE sb_term(Body, 'error') GROUP BY minute;
-- Q66 task=group_by filter=and freq=hi (key=SeverityText, Body term + indexed service)
SELECT SeverityText, count() AS cnt FROM otel_logs WHERE ServiceName = 'frontend' AND sb_term(Body, 'failed') GROUP BY SeverityText ORDER BY cnt DESC;
-- Q67 task=group_by filter=or freq=hi (two keys: SeverityText, ScopeName)
SELECT SeverityText, ScopeName, count() AS cnt FROM otel_logs WHERE sb_any(Body, ['error', 'failed']) GROUP BY SeverityText, ScopeName ORDER BY cnt DESC LIMIT 20;
-- Q68 task=recent filter=and,window freq=hi (recent failed-order logs from checkout)
SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs WHERE ServiceName = 'checkout' AND sb_all(Body, ['failed', 'order']) AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 00:30:00' ORDER BY Timestamp DESC LIMIT 100;
-- Q69 task=recent filter=or,window freq=hi (recent error/failed/charge, severity>=warn)
SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs WHERE sb_any(Body, ['error', 'failed', 'charge']) AND SeverityNumber >= 13 AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 00:30:00' ORDER BY Timestamp DESC LIMIT 100;
-- Q70 task=recent filter=term,window freq=hi (recent error logs in a 6h window)
SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs WHERE sb_term(Body, 'error') AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 06:00:00' ORDER BY Timestamp DESC LIMIT 100;
-- Q71 task=recent filter=phrase,window freq=mid (recent 'failed to place order')
SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs WHERE sb_phrase(Body, 'failed to place order') AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 06:00:00' ORDER BY Timestamp DESC LIMIT 100;
-- Q72 task=recent filter=and,window freq=hi (recent payment charges)
SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs WHERE ServiceName = 'payment' AND sb_term(Body, 'charge') AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 00:30:00' ORDER BY Timestamp DESC LIMIT 100;
-- Q73 task=recent filter=window (pure time-series tail: recent cart logs, no text search)
SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs WHERE ServiceName = 'cart' AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 00:30:00' ORDER BY Timestamp DESC LIMIT 100;
-- Q74 task=recent filter=regexp,window freq=hi (recent charg* logs in a 6h window)
SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs WHERE sb_prefix(Body, 'charg') AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 06:00:00' ORDER BY Timestamp DESC LIMIT 100;
-- Q75 task=recent filter=or,window freq=mid (recent connection/request/conversion logs)
SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs WHERE sb_any(Body, ['connection', 'request', 'conversion']) AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 00:30:00' ORDER BY Timestamp DESC LIMIT 100;
-- Q76 task=recent filter=and,window freq=hi (checkout failed&order, NO order by)
SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs WHERE ServiceName = 'checkout' AND sb_all(Body, ['failed', 'order']) AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 00:30:00' LIMIT 100;
-- Q77 task=recent filter=or,window freq=hi (error/failed/charge sev>=warn, NO order by)
SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs WHERE sb_any(Body, ['error', 'failed', 'charge']) AND SeverityNumber >= 13 AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 00:30:00' LIMIT 100;
-- Q78 task=recent filter=term,window freq=hi (error 6h, NO order by)
SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs WHERE sb_term(Body, 'error') AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 06:00:00' LIMIT 100;
-- Q79 task=recent filter=phrase,window freq=mid (failed to place order 6h, NO order by)
SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs WHERE sb_phrase(Body, 'failed to place order') AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 06:00:00' LIMIT 100;
-- Q80 task=recent filter=and,window freq=hi (payment charges, NO order by)
SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs WHERE ServiceName = 'payment' AND sb_term(Body, 'charge') AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 00:30:00' LIMIT 100;
-- Q81 task=recent filter=window (cart tail, NO order by, no text search)
SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs WHERE ServiceName = 'cart' AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 00:30:00' LIMIT 100;
-- Q82 task=recent filter=regexp,window freq=hi (charg.* 6h, NO order by)
SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs WHERE sb_prefix(Body, 'charg') AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 06:00:00' LIMIT 100;
-- Q83 task=recent filter=or,window freq=mid (connection/request/conversion, NO order by)
SELECT Timestamp, ServiceName, SeverityText, Body FROM otel_logs WHERE sb_any(Body, ['connection', 'request', 'conversion']) AND Timestamp BETWEEN '2025-09-23 00:00:00' AND '2025-09-23 00:30:00' LIMIT 100;
-- Q84 task=join filter=term freq=hi (frontend 'failed' traces that also involve payment)
SELECT count(DISTINCT a.TraceId) FROM otel_logs a JOIN otel_logs b ON a.TraceId = b.TraceId WHERE a.TraceId != '' AND a.ServiceName = 'frontend' AND sb_term(a.Body, 'failed') AND b.ServiceName = 'payment';
-- Q85 task=join filter=or freq=hi
SELECT count(DISTINCT a.TraceId) FROM otel_logs a JOIN otel_logs b ON a.TraceId = b.TraceId WHERE a.TraceId != '' AND sb_any(a.Body, ['error', 'failed']) AND b.ServiceName = 'payment';
-- Q86 task=join filter=phrase freq=mid
SELECT count(DISTINCT a.TraceId) FROM otel_logs a JOIN otel_logs b ON a.TraceId = b.TraceId WHERE a.TraceId != '' AND sb_phrase(a.Body, 'failed to place order') AND b.ServiceName = 'payment';
-- Q87 task=join filter=regexp freq=hi
SELECT count(DISTINCT a.TraceId) FROM otel_logs a JOIN otel_logs b ON a.TraceId = b.TraceId WHERE a.TraceId != '' AND sb_prefix(a.Body, 'charg') AND b.ServiceName = 'frontend';
-- Q88 task=join filter=and freq=hi (failed&order traces that also involve cart)
SELECT count(DISTINCT a.TraceId) FROM otel_logs a JOIN otel_logs b ON a.TraceId = b.TraceId WHERE a.TraceId != '' AND sb_all(a.Body, ['failed', 'order']) AND b.ServiceName = 'cart';
-- Q89 task=join filter=or freq=mid
SELECT count(DISTINCT a.TraceId) FROM otel_logs a JOIN otel_logs b ON a.TraceId = b.TraceId WHERE a.TraceId != '' AND sb_any(a.Body, ['connection', 'request']) AND b.ServiceName = 'frontend';
-- Q90 task=join filter=and freq=hi
SELECT count(DISTINCT a.TraceId) FROM otel_logs a JOIN otel_logs b ON a.TraceId = b.TraceId WHERE a.TraceId != '' AND sb_all(a.Body, ['charge', 'request']) AND b.ServiceName = 'frontend';
-- Q91 task=join filter=term freq=hi
SELECT count(DISTINCT a.TraceId) FROM otel_logs a JOIN otel_logs b ON a.TraceId = b.TraceId WHERE a.TraceId != '' AND sb_term(a.Body, 'order') AND b.ServiceName = 'payment';
-- Q92 task=join filter=prefix freq=hi
SELECT count(DISTINCT a.TraceId) FROM otel_logs a JOIN otel_logs b ON a.TraceId = b.TraceId WHERE a.TraceId != '' AND sb_prefix(a.Body, 'charg') AND b.ServiceName = 'frontend';
