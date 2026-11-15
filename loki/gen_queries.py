#!/usr/bin/env python3
"""Generate aligned request specifications, not SQL. Fail closed on unknown DSL.

Loki regexes use RE2. The reference char_group analyzer splits punctuation,
symbols and Java whitespace, then lowercases. We lowercase inside the query (not at ingest), avoiding RE2's
Unicode case-folding differences. Recent queries restore the original line.
"""
import json
import re
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# Character.isWhitespace (not Unicode White_Space): NBSP, figure space and
# narrow NBSP are NOT delimiters. Format/control characters such as ZWJ/NUL
# remain in tokens. This matches elastic/config/index_mapping.json.
DELIMITERS = r"\p{P}\p{S}\x{0009}-\x{000d}\x{001c}-\x{0020}\x{1680}\x{2000}-\x{2006}\x{2008}-\x{200a}\x{2028}\x{2029}\x{205f}\x{3000}"
TOKEN = "[^" + DELIMITERS + "]"
SEP = "[" + DELIMITERS + "]"
FIELDS = {"ServiceName": "service_name", "SeverityText": "severity_text", "ScopeName": "ScopeName"}


class Unsupported(ValueError):
    pass


def quote(value):
    return json.dumps(value, ensure_ascii=False)


def bounded(pattern):
    return f"(^|{SEP})({pattern})({SEP}|$)"


def body_pattern(node):
    """Return a single regular expression, only for representable predicates."""
    if "match" in node:
        value = node["match"]["Body"]
        terms = value.split() if isinstance(value, str) else value["query"].split()
        if len(terms) > 1 and not isinstance(value, str) and (value.get("operator") == "and" or value.get("minimum_should_match", 1) != 1):
            raise Unsupported("AND/minmatch needs separate pipeline stages")
        return bounded("|".join(re.escape(t.lower()) for t in terms))
    if "match_phrase" in node:
        value = node["match_phrase"]["Body"]
        if not isinstance(value, str):
            raise Unsupported("phrase proximity not yet validated")
        return bounded((SEP + "+").join(re.escape(t.lower()) for t in value.split()))
    if "prefix" in node:
        return bounded(re.escape(node["prefix"]["Body"]) + TOKEN + "*")
    if "wildcard" in node:
        value = node["wildcard"]["Body"]
        return bounded("".join(TOKEN + "*" if c == "*" else TOKEN if c == "?" else re.escape(c) for c in value))
    if "regexp" in node:
        value = node["regexp"]["Body"]
        # Only the four regexp forms actually used by SearchBench. Do not
        # pretend arbitrary Lucene regex syntax is RE2-compatible.
        if value not in ("charg.*", "ord.*", "conn.*", "c.che"):
            raise Unsupported("untranslated Lucene regular expression")
        return bounded(value.replace(".", TOKEN))
    if "fuzzy" in node:
        raise Unsupported("fuzzy token matching")
    raise Unsupported("predicate cannot be represented as one line regex")


def timestamp_ns(value):
    # Reference bounds have whole-second precision. Never convert corpus ns
    # through float or datetime; that path is handled by Arrow in ingest.py.
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = dt - epoch
    return (delta.days * 86400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1000


def translate_predicate(node, spec):
    if "bool" in node:
        b = node["bool"]
        for child in b.get("must", []) + b.get("filter", []):
            translate_predicate(child, spec)
        for child in b.get("must_not", []):
            spec["filters"].append("!~ " + quote(body_pattern(child)))
        if b.get("should"):
            if b.get("minimum_should_match", 1) != 1:
                raise Unsupported("boolean minimum_should_match")
            spec["filters"].append("|~ " + quote("|".join(body_pattern(c) for c in b["should"])))
        return
    if "range" in node:
        for field, bounds in node["range"].items():
            if field == "Timestamp":
                spec["window"] = [timestamp_ns(bounds["gte"]), timestamp_ns(bounds["lte"])]
            elif field == "SeverityNumber" and set(bounds) == {"gte"}:
                spec["metadata_filters"].append(f"| SeverityNumber >= {int(bounds['gte'])}")
            else:
                raise Unsupported(f"range on {field}")
        return
    if "term" in node:
        field, value = next(iter(node["term"].items()))
        if field != "ServiceName":
            raise Unsupported(f"term on {field}")
        spec["labels"]["service_name"] = value
        return
    if "match" in node and isinstance(node["match"]["Body"], dict):
        value = node["match"]["Body"]
        terms = value["query"].split()
        n = len(terms) if value.get("operator") == "and" else int(value.get("minimum_should_match", 1))
        if n == len(terms):
            spec["filters"].extend("|~ " + quote(bounded(re.escape(t.lower()))) for t in terms)
            return
        if n != 1:
            # Avoid exponential regexes or per-log template evaluation in the
            # initial adapter. This query stays visibly unsupported.
            raise Unsupported("minimum_should_match not yet validated")
    spec["filters"].append("|~ " + quote(body_pattern(node)))


def translate(number, tag, dsl):
    if "task=top_k" in tag:
        raise Unsupported("BM25 document ranking; LogQL topk ranks metric series")
    if "task=join" in tag:
        raise Unsupported("relational trace join; no client-side emulation")
    body = json.loads(dsl)
    spec = {"id": f"Q{number:02d}", "kind": "count", "labels": {}, "filters": [], "metadata_filters": []}
    translate_predicate(body["query"], spec)
    if "task=recent" in tag:
        spec.update(kind="logs", limit=body["size"], ordered="sort" in body)
    elif "task=group_by" in tag:
        spec["kind"] = "groups"
        agg = next(iter(body["aggs"].values()))
        if "date_histogram" in agg:
            spec.update(kind="histogram", groups=["minute"])
        else:
            spec["groups"] = [FIELDS[agg["terms"]["field"]]]
            if "aggs" in agg:
                spec["groups"].append("ScopeName")
                # Q67 follows the SQL baseline's global top 20 pairs.
                spec["limit"] = 20
            elif number in (57, 58):
                spec["limit"] = 20
    return spec


def generate():
    lines = (ROOT / "elastic/queries.dsl").read_text().splitlines()
    output = ["-- Loki request specifications; generated by gen_queries.py."]
    count = 0
    for i, line in enumerate(lines):
        match = re.match(r"-- Q(\d+) ", line)
        if not match:
            continue
        count += 1
        output.append(line)
        try:
            output.append(json.dumps(translate(int(match[1]), line, lines[i + 1]), separators=(",", ":")))
        except Unsupported as exc:
            output.append("UNSUPPORTED: " + str(exc))
    assert count == 92
    return "\n".join(output) + "\n"


if __name__ == "__main__":
    print(generate(), end="")
