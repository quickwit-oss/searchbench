-- Scan-time helpers, not Python UDFs or stored/indexed token columns.
-- Tokens are the maximal ASCII alphanumeric runs of lower(body): no stemming or stopword removal.
CREATE MACRO sb_tokens(body) AS list_filter(regexp_split_to_array(lower(coalesce(body, '')), '[^a-z0-9]+'), lambda x: x != '');
-- The predicates are the token test written as regexes on body itself, without building
-- lower(body) for every row. DuckDB's lower() turns exactly three sets of characters into
-- ASCII letters or digits: A-Z, U+0130 (to i) and U+212A KELVIN SIGN (to k); every other
-- character keeps one character (checked over all code points, test_adapter.py). So with
--   sb_ci(t): t with each letter x replaced by [xX] (i by [iI\x{0130}], k by [kK\x{212A}]),
--   sb_sep:   a character that lower() does not turn into [a-z0-9],
-- "t is a token of lower(body)" is "sb_ci(t) occurs in body with sb_sep or the string edge on
-- each side" (t alphanumeric). Each predicate first checks sb_ci(t) as a plain substring
-- regex (RE2 scans for it quickly), then the boundaries. One regex per predicate (an
-- alternation for sb_any), and sb_all is a flat AND chain, so rows are dropped at the first
-- term that fails. test_adapter.py checks each one against the token-list definition.
CREATE MACRO sb_ci(t) AS array_to_string(list_transform(string_split(t, ''), lambda c:
    CASE WHEN c = 'i' THEN '[iI\x{0130}]' WHEN c = 'k' THEN '[kK\x{212A}]'
         WHEN c BETWEEN 'a' AND 'z' THEN '[' || c || upper(c) || ']' ELSE c END), '');
CREATE MACRO sb_sep() AS '[^a-zA-Z0-9\x{0130}\x{212A}]';
CREATE MACRO sb_alnum() AS '[a-zA-Z0-9\x{0130}\x{212A}]';
CREATE MACRO sb_re(body, r) AS regexp_matches(coalesce(body, ''), r);
CREATE MACRO sb_has(body, term) AS sb_re(body, sb_ci(term));
CREATE MACRO sb_term(body, term) AS sb_has(body, term)
    AND sb_re(body, '(^|' || sb_sep() || ')' || sb_ci(term) || '($|' || sb_sep() || ')');
CREATE MACRO sb_any(body, terms) AS sb_re(body, array_to_string(list_transform(terms, lambda t: sb_ci(t)), '|'))
    AND sb_re(body, '(^|' || sb_sep() || ')(' || array_to_string(list_transform(terms, lambda t: sb_ci(t)), '|') || ')($|' || sb_sep() || ')');
CREATE MACRO sb_slot(body, terms, i) AS (len(terms) < i OR sb_term(body, terms[i]));
CREATE MACRO sb_all(body, terms) AS CASE WHEN len(terms) BETWEEN 1 AND 8 THEN
    sb_slot(body, terms, 1) AND sb_slot(body, terms, 2) AND sb_slot(body, terms, 3) AND sb_slot(body, terms, 4)
    AND sb_slot(body, terms, 5) AND sb_slot(body, terms, 6) AND sb_slot(body, terms, 7) AND sb_slot(body, terms, 8)
    ELSE error('sb_all takes 1 to 8 terms') END;
-- Contiguous token sequence: any run of separators between two words.
CREATE MACRO sb_phrase(body, phrase) AS sb_has(body, string_split(phrase, ' ')[1])
    AND sb_re(body, '(^|' || sb_sep() || ')'
        || array_to_string(list_transform(string_split(phrase, ' '), lambda w: sb_ci(w)), sb_sep() || '+')
        || '($|' || sb_sep() || ')');
-- Token patterns (p, s alphanumeric): some token starts with p / ends with s / contains p.
CREATE MACRO sb_prefix(body, p) AS sb_has(body, p) AND sb_re(body, '(^|' || sb_sep() || ')' || sb_ci(p));
CREATE MACRO sb_suffix(body, s) AS sb_has(body, s) AND sb_re(body, sb_ci(s) || '($|' || sb_sep() || ')');
CREATE MACRO sb_infix(body, p) AS sb_has(body, p);
-- Some token fully matches r, a regex over token characters written with sb_alnum() for
-- "any token character" (e.g. 'c' || sb_alnum() || 'che'; r must not match a separator).
CREATE MACRO sb_token_re(body, r) AS sb_re(body, '(^|' || sb_sep() || ')(' || r || ')($|' || sb_sep() || ')');
-- Some token is within Damerau-Levenshtein distance d of t. The distance is at least the
-- length difference, so tokens of other lengths are dropped before computing it.
CREATE MACRO sb_fuzzy(body, t, d) AS list_count(list_filter(
    list_filter(sb_tokens(body), lambda x: abs(length(x) - length(t)) <= d),
    lambda x: damerau_levenshtein(x, t) <= d)) > 0;
