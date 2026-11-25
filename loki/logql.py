"""Build LogQL requests and normalize responses for SearchBench."""
import json
import os
import re
from datetime import datetime, timezone
from decimal import Decimal
from encoding import decode_frame, validate_contract
from gen_queries import SEP, TOKEN

NS = 1_000_000_000
# Characters a prefilter literal may contain. Go strings.ToLower maps exactly
# two non-ASCII runes to ASCII: U+0130 -> "i" and U+212A -> "k". Excluding "i"
# and a leading "k" keeps |~ "(?i)literal" a superset under BOTH Loki paths:
# containsLower (its first-byte fast path only folds ASCII) and RE2 (?i) (whose
# fold orbits omit U+0130). JSON frame escaping never touches [0-9a-z].
PREFILTER_CHARS = frozenset("0123456789abcdefghjklmnopqrstuvwxyz")
PREFILTER_MIN_LENGTH = 3


def quote(value):
    return json.dumps(value, ensure_ascii=False)


def literal_prefilter_enabled():
    value = os.environ.get("LOKI_LITERAL_PREFILTER", "on")
    if value not in ("on", "off"):
        raise ValueError("LOKI_LITERAL_PREFILTER must be on or off")
    return value == "on"


def required_literal(text_filter):
    """Longest lowercase literal every match of one positive filter contains.

    Accepts only the exact single bounded(...) concatenation emitted by
    gen_queries.py: escaped literals, TOKEN/SEP atoms and quantifiers on those
    atoms. Negations, alternations (OR branches), groups, escapes other than
    escaped punctuation, and quantified literals return None (no prefilter).
    """
    if not text_filter.startswith("|~ "):
        return None
    pattern = json.loads(text_filter[3:])
    head, tail = "(^|" + SEP + ")(", ")(" + SEP + "|$)"
    if not (pattern.startswith(head) and pattern.endswith(tail)):
        return None
    inner, runs, run, i = pattern[len(head):-len(tail)], [], "", 0
    while i < len(inner):
        atom = next((a for a in (TOKEN, SEP) if inner.startswith(a, i)), None)
        if atom is not None:
            runs.append(run); run = ""
            i += len(atom)
            if i < len(inner) and inner[i] in "*+?":
                i += 1
            continue
        char = inner[i]
        if char == "\\":
            if i + 1 == len(inner) or inner[i + 1].isalnum() or ord(inner[i + 1]) > 127:
                return None
            char, i = inner[i + 1], i + 2
        elif char in "()[]{}|.^$*+?":
            return None
        else:
            i += 1
        if i < len(inner) and inner[i] in "*+?{":
            return None
        run += char
    runs.append(run)
    candidates = []
    for run in runs:
        for piece in re.split("[^" + "".join(sorted(PREFILTER_CHARS)) + "]+", run):
            piece = piece.lstrip("k")
            if len(piece) >= PREFILTER_MIN_LENGTH:
                candidates.append(piece)
    return max(candidates, key=len, default=None)


def literal_prefilters(text_filters):
    literals = []
    for text_filter in text_filters:
        literal = required_literal(text_filter)
        if literal is not None and literal not in literals:
            literals.append(literal)
    return ["|~ " + quote("(?i)" + literal) for literal in literals]


def verified_corpus_lower_bound(manifest):
    """Trust completed fresh-load provenance, never a caller's bare minimum.

    The adapter validates the receipt/destination/source inventory before calling
    us. The loader's exact source bounds apply only to that exclusively owned,
    immutable corpus; a persisted count alone cannot prove timestamp bounds.
    Incomplete/malformed verification markers conservatively retain the guard.
    """
    lo, hi = manifest.get("min_timestamp_ns"), manifest.get("max_timestamp_ns")
    rows = manifest.get("rows")
    inventory = manifest.get("persisted_inventory")
    stats = manifest.get("verification_stats")
    ingester = stats.get("ingester") if isinstance(stats, dict) else None
    reached = ingester.get("totalReached") if isinstance(ingester, dict) else None
    if (type(lo) is int and type(hi) is int and lo <= hi
            and type(rows) is int and rows > 0
            and manifest.get("query_store_only") is True
            and isinstance(inventory, str) and re.fullmatch(r"[0-9a-f]{64}", inventory)
            and type(reached) in (int, float) and reached == 0):
        return lo
    return None


def build_request(spec, manifest, literal_prefilter=None):
    validate_contract(manifest)
    if literal_prefilter is None:
        literal_prefilter = literal_prefilter_enabled()
    lo, hi = spec["window"] if "window" in spec else (manifest["min_timestamp_ns"], manifest["max_timestamp_ns"])
    if lo > hi:
        raise ValueError("invalid time range")
    labels = {"dataset": "otel_logs", **spec.get("labels", {})}
    pipeline = "{" + ",".join(k + "=" + quote(v) for k, v in sorted(labels.items())) + "}"
    text = spec.get("filters", [])
    if literal_prefilter:
        # Case-insensitive raw-frame substring stages: necessary conditions of
        # the exact lowered-Body regexes below, never a replacement for them.
        pipeline += "".join(" " + stage for stage in literal_prefilters(text))
    pipeline += " " + " ".join(spec.get("metadata_filters", []))
    if text:
        if spec["kind"] == "logs":
            pipeline += ' | label_format sb_frame=`{{ __line__ }}`'
        # Target only the string Body; retain the same Go lower function (RE2
        # case folding is not equivalent). These aliases are reserved at load.
        pipeline += ' | json sb_original="body" | line_format `{{ lower .sb_original }}` ' + " ".join(text)
        if spec["kind"] == "logs":
            # Entry.Equal sees the processed line and post-pipeline metadata.
            # Keep physical identity through native merge/limit, even if a
            # later projection removes metadata. Decode only in normalize().
            pipeline += ' | line_format `{{ .sb_frame }}` | drop sb_frame,sb_original'
    if spec["kind"] == "logs":
        # Loki log-range end is exclusive; the reference's lte is inclusive.
        return "/loki/api/v1/query_range", {
            "query": pipeline, "start": str(lo), "end": str(hi + 1),
            "direction": "backward", "limit": spec.get("limit", 100),
        }
    # LogQL range vectors select (start, end]. Durations have ms precision,
    # but logs retain ns timestamps. Widen by <=1ms and explicitly enforce the
    # inclusive lower bound inside Loki, without float conversion or changing
    # the corpus. Instant-query evaluation time retains ns in Loki 3.7.9.
    duration_ms = (hi - lo) // 1_000_000 + 1
    corpus_lo = verified_corpus_lower_bound(manifest)
    if corpus_lo is None or lo > corpus_lo:
        pipeline += f' | label_format sb_window=`{{{{ ge (__timestamp__).UnixNano {lo} }}}}` | sb_window="true"'
    groups = spec.get("groups", [])
    if spec["kind"] == "histogram":
        # UTC bucketing in the engine avoids query_range's step alignment and
        # millisecond timestamps moving logs across SQL minute boundaries.
        pipeline += ' | label_format minute=`{{ (__timestamp__).UTC.Format "2006-01-02T15:04:00Z" }}`'
    # Structured metadata otherwise becomes metric labels: TraceId alone could
    # produce millions of series. Drop it BEFORE the range aggregation.
    pipeline += " | keep " + ",".join(groups or ["dataset"])
    agg = "sum" + (" by (" + ",".join(groups) + ")" if groups else "")
    query = f"{agg}(count_over_time({pipeline} [{duration_ms}ms]))"
    if spec.get("limit"):
        query = f"topk({int(spec['limit'])}, {query})"
    return "/loki/api/v1/query", {"query": query, "time": str(hi)}


def integer_count(value):
    n = Decimal(str(value))
    if not n.is_finite() or n != n.to_integral_value() or n < 0:
        raise ValueError(f"invalid count: {value}")
    return int(n)


def timestamp_string(ns):
    seconds, nanos = divmod(int(ns), NS)
    return datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + f".{nanos:09d}Z"


def normalize(spec, response, manifest):
    validate_contract(manifest)
    if response.get("status") != "success":
        raise ValueError(f"Loki query failed: {response}")
    data = response["data"]
    result = data["result"]
    if spec["kind"] == "logs":
        if data["resultType"] != "streams":
            raise ValueError("expected streams response")
        rows = []
        for stream in result:
            labels = stream["stream"]
            if labels.get("__error__"):
                raise ValueError("native log pipeline error: " + labels["__error__"])
            for value in stream["values"]:
                frame = decode_frame(value[1])
                if "sb_row_id" in labels and labels["sb_row_id"] != frame["id"]:
                    raise ValueError("physical frame/metadata identity mismatch")
                rows.append((int(value[0]), {"Timestamp": timestamp_string(value[0]),
                    "ServiceName": labels.get("service_name", ""),
                    "SeverityText": labels.get("severity_text", ""), "Body": frame["body"]}))
        rows.sort(key=lambda row: row[0], reverse=True)
        if len(rows) > spec.get("limit", 100):
            raise ValueError("Loki returned more rows than requested")
        return [row for _, row in rows]
    if data["resultType"] != "vector":
        raise ValueError("expected vector response")
    if spec["kind"] == "count":
        return [{"count": sum(integer_count(row["value"][1]) for row in result)}]
    rows = []
    for row in result:
        keys = {key: row["metric"].get(key, "") for key in spec["groups"]}
        rows.append({**keys, "count": integer_count(row["value"][1])})
    if spec["kind"] == "histogram":
        rows.sort(key=lambda r: r["minute"])
    else:
        rows.sort(key=lambda r: (-r["count"], tuple(str(r[k]) for k in spec["groups"])))
    return rows
