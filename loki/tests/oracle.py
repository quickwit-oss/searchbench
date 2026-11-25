"""Independent fixture oracle based on ES DSL, not the emitted Loki regexes."""
import re
import unicodedata
from collections import Counter
from gen_queries import timestamp_ns
from logql import timestamp_string


def tokenize(body):
    tokens, word = [], []
    for c in body.lower():
        category = unicodedata.category(c)
        java_space = (category in ("Zs", "Zl", "Zp") and c not in "\u00a0\u2007\u202f") or c in "\t\n\v\f\r\x1c\x1d\x1e\x1f"
        if not java_space and category[0] not in "PS":
            word.append(c)
        elif word:
            tokens.append("".join(word)); word = []
    if word:
        tokens.append("".join(word))
    return tokens


def matches(node, doc):
    tokens = tokenize(doc["Body"])
    if "bool" in node:
        b = node["bool"]
        return (all(matches(n, doc) for n in b.get("must", []) + b.get("filter", []))
                and not any(matches(n, doc) for n in b.get("must_not", []))
                and (not b.get("should") or sum(matches(n, doc) for n in b["should"]) >= b.get("minimum_should_match", 1)))
    if "match" in node:
        value = node["match"]["Body"]
        words = value.split() if isinstance(value, str) else value["query"].split()
        minimum = 1 if isinstance(value, str) else len(words) if value.get("operator") == "and" else value.get("minimum_should_match", 1)
        return sum(w in tokens for w in words) >= minimum
    if "match_phrase" in node:
        words = node["match_phrase"]["Body"].split()
        return any(tokens[i:i + len(words)] == words for i in range(len(tokens)))
    if "regexp" in node:
        return any(re.fullmatch(node["regexp"]["Body"], token) for token in tokens)
    if "prefix" in node:
        return any(token.startswith(node["prefix"]["Body"]) for token in tokens)
    if "wildcard" in node:
        pattern = re.escape(node["wildcard"]["Body"]).replace(r"\*", ".*").replace(r"\?", ".")
        return any(re.fullmatch(pattern, token) for token in tokens)
    if "term" in node:
        return all(doc[k] == v for k, v in node["term"].items())
    if "range" in node:
        for key, bounds in node["range"].items():
            for op, value in bounds.items():
                if key == "Timestamp":
                    value = timestamp_ns(value)
                if op == "gte" and doc[key] < value or op == "lte" and doc[key] > value:
                    return False
        return True
    raise AssertionError(node)


def expected_rows(number, spec, body, docs):
    selected = [d for d in docs if matches(body["query"], d)]
    if spec["kind"] == "count":
        return [{"count": len(selected)}]
    if spec["kind"] == "logs":
        selected.sort(key=lambda d: d["Timestamp"], reverse=True)
        return [{"Timestamp": timestamp_string(d["Timestamp"]), "ServiceName": d["ServiceName"],
                 "SeverityText": d["SeverityText"], "Body": d["Body"]} for d in selected[:100]]
    groups = spec["groups"]
    def key(d):
        mapping = {**d, "severity_text": d["SeverityText"], "service_name": d["ServiceName"],
                   "minute": timestamp_string(d["Timestamp"])[:16] + ":00Z"}
        return tuple(mapping[k] for k in groups)
    counts = Counter(key(d) for d in selected)
    rows = [{**dict(zip(groups, key)), "count": count} for key, count in counts.items()]
    if spec["kind"] == "histogram":
        rows.sort(key=lambda r: r["minute"])
    else:
        rows.sort(key=lambda r: (-r["count"], tuple(str(r[k]) for k in groups)))
    return rows[:spec["limit"]] if "limit" in spec else rows
