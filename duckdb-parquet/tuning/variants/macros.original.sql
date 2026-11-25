-- Scan-time helpers, not Python UDFs or stored/indexed token columns.
-- ASCII alphanumeric tokens, lowercased, no stemming or stopword removal.
CREATE MACRO sb_tokens(body) AS list_filter(regexp_split_to_array(lower(coalesce(body, '')), '[^a-z0-9]+'), lambda x: x != '');
-- Each predicate first checks that the term(s) occur as substrings of lower(body), a
-- necessary condition for a token match (tokens are lowercased substrings of the
-- body), and cheap compared with tokenizing. Rows that pass are then tokenized, so
-- the results are exactly those of the token test alone. For phrases each word is
-- checked separately: tokens may be separated by any punctuation, so the phrase as a
-- whole need not be a substring.
CREATE MACRO sb_has(body, term) AS contains(lower(coalesce(body, '')), term);
CREATE MACRO sb_term(body, term) AS sb_has(body, term) AND list_contains(sb_tokens(body), term);
CREATE MACRO sb_all(body, terms) AS list_bool_and(list_transform(terms, lambda t: sb_has(body, t)))
    AND list_has_all(sb_tokens(body), terms);
CREATE MACRO sb_any(body, terms) AS list_bool_or(list_transform(terms, lambda t: sb_has(body, t)))
    AND list_has_any(sb_tokens(body), terms);
-- Contiguous token sequence: punctuation/whitespace are equivalent separators.
CREATE MACRO sb_phrase(body, phrase) AS list_bool_and(list_transform(string_split(phrase, ' '), lambda w: sb_has(body, w)))
    AND contains(' ' || coalesce(array_to_string(sb_tokens(body), ' '), '') || ' ', ' ' || phrase || ' ');
