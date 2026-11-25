-- Scan-time helpers, not Python UDFs or stored/indexed token columns.
-- Tokens are the maximal ASCII alphanumeric runs of lower(body): no stemming or stopword removal.
CREATE MACRO sb_tokens(body) AS list_filter(regexp_split_to_array(lower(coalesce(body, '')), '[^a-z0-9]+'), lambda x: x != '');
-- Every predicate below is the token test written as a regex on lower(body), which gives
-- the same rows without splitting the body into a list: "t is a token" is "t occurs with a
-- non-alphanumeric character or the string edge on each side" (t alphanumeric).
-- DuckDB evaluates lower(body) once per occurrence in the expression, including inside
-- lambdas, so each predicate uses one regex (alternation for sb_any) rather than a lambda
-- over the terms, and sb_all is a flat AND chain: rows are dropped at the first term that
-- fails. A substring check runs first where it is cheaper than the regex.
-- test_adapter.py checks each one against the token-list definition.
CREATE MACRO sb_lb(body) AS lower(coalesce(body, ''));
CREATE MACRO sb_has(body, term) AS contains(sb_lb(body), term);
CREATE MACRO sb_term(body, term) AS contains(sb_lb(body), term)
    AND regexp_matches(sb_lb(body), '(^|[^a-z0-9])' || term || '($|[^a-z0-9])');
CREATE MACRO sb_any(body, terms) AS regexp_matches(sb_lb(body), '(^|[^a-z0-9])(' || array_to_string(terms, '|') || ')($|[^a-z0-9])');
CREATE MACRO sb_slot(body, terms, i) AS (len(terms) < i OR sb_term(body, terms[i]));
CREATE MACRO sb_all(body, terms) AS CASE WHEN len(terms) BETWEEN 1 AND 8 THEN
    sb_slot(body, terms, 1) AND sb_slot(body, terms, 2) AND sb_slot(body, terms, 3) AND sb_slot(body, terms, 4)
    AND sb_slot(body, terms, 5) AND sb_slot(body, terms, 6) AND sb_slot(body, terms, 7) AND sb_slot(body, terms, 8)
    ELSE error('sb_all takes 1 to 8 terms') END;
-- Contiguous token sequence: any run of punctuation/whitespace separates two words.
CREATE MACRO sb_phrase(body, phrase) AS regexp_matches(sb_lb(body), '(^|[^a-z0-9])' || replace(phrase, ' ', '[^a-z0-9]+') || '($|[^a-z0-9])');
-- Token patterns (p, s alphanumeric): some token starts with p / ends with s / contains p.
CREATE MACRO sb_prefix(body, p) AS regexp_matches(sb_lb(body), '(^|[^a-z0-9])' || p);
CREATE MACRO sb_suffix(body, s) AS regexp_matches(sb_lb(body), s || '($|[^a-z0-9])');
CREATE MACRO sb_infix(body, p) AS contains(sb_lb(body), p);
-- Some token fully matches r, a regex over [a-z0-9] characters only.
CREATE MACRO sb_token_re(body, r) AS regexp_matches(sb_lb(body), '(^|[^a-z0-9])(' || r || ')($|[^a-z0-9])');
-- Some token is within Damerau-Levenshtein distance d of t. The distance is at least the
-- length difference, so tokens of other lengths are dropped before computing it.
CREATE MACRO sb_fuzzy(body, t, d) AS list_count(list_filter(
    list_filter(sb_tokens(body), lambda x: abs(length(x) - length(t)) <= d),
    lambda x: damerau_levenshtein(x, t) <= d)) > 0;
