"""Physical line contract. Legacy raw storage requires a new load, not detection.

Receipts are trusted fresh-writer provenance, not cryptographic attestation of
arbitrary external writes. Never relabel an existing/mixed destination as v1.
"""
import json

ENCODING = {"name": "searchbench-loki-json-frame", "version": 1,
            "identity": "source-file-row-v1"}
ENCODING_FLAG = "searchbench-loki-json-frame-v1"
MAX_LINE_BYTES = 1024 * 1024
MAX_REQUEST_BYTES = 3 * 1024 * 1024
RESERVED = {"sb_row_id", "sb_frame", "sb_window", "sb_original"}


def validate_contract(record, receipt=None):
    if not isinstance(record, dict):
        raise ValueError("encoding contract must be an object")
    encoding = record.get("encoding")
    if (encoding != ENCODING or not isinstance(encoding, dict)
            or type(encoding.get("version")) is not int):
        raise ValueError("unknown, absent or legacy line encoding; a fresh v1 load is required")
    if not isinstance(record.get("destination_id"), str) or not record["destination_id"]:
        raise ValueError("missing destination identity")
    destination = record.get("destination")
    if not isinstance(destination, dict) or not destination:
        raise ValueError("missing physical destination binding")
    if receipt is not None:
        validate_contract(receipt)
        if any(record[key] != receipt[key] for key in ("encoding", "destination_id", "destination")):
            raise ValueError("manifest/receipt destination or encoding mismatch")


def encode_frame(body, identity):
    if not isinstance(body, str) or not isinstance(identity, str) or not identity:
        raise ValueError("string Body and nonempty source identity are required")
    body.encode("utf-8"); identity.encode("utf-8")  # reject lone surrogates, like Arrow/Rust
    line = json.dumps({"v": 1, "id": identity, "body": body}, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False)
    if len(line.encode("utf-8")) > MAX_LINE_BYTES:
        raise ValueError("physical frame exceeds the 1 MiB Loki line cap; refusing truncation")
    return line


def frame_overhead(identity):
    return len(encode_frame("", identity).encode("utf-8"))


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate physical frame key")
        result[key] = value
    return result


def decode_frame(line):
    frame = json.loads(line, object_pairs_hook=_unique_object)
    if (not isinstance(frame, dict) or set(frame) != {"v", "id", "body"}
            or type(frame["v"]) is not int or frame["v"] != 1
            or not isinstance(frame["id"], str) or not frame["id"]
            or not isinstance(frame["body"], str)):
        raise ValueError("invalid/unknown physical frame; refusing raw fallback")
    frame["id"].encode("utf-8"); frame["body"].encode("utf-8")
    return frame
