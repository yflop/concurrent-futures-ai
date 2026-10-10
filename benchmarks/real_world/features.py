"""Explicit, bounded fixture metadata for opt-in predictive admission.

No files, payloads, URLs, text, hashes, results or callable internals are read.
See docs/WORKLOAD-FEATURES.md for schema, limits and confidence limitations.
"""
from __future__ import annotations

MAX_BATCH_SIZE = 10000
MAX_VALUE = 1 << 30


def _integer(value, cap=MAX_VALUE):
    # Exact built-in types avoid custom conversion hooks and reject bool/float.
    return value if type(value) is int and 1 <= value <= cap else None


def _count(value):
    return len(value) if type(value) in (list, tuple) and 1 <= len(value) <= 10000 else None


def _product(*values):
    if any(value is None for value in values):
        return None
    result = 1
    for value in values:
        result *= value
        if result > MAX_VALUE:
            return None
    return result


def _pixels(value, side_cap=16384):
    if type(value) not in (list, tuple) or len(value) != 2:
        return None
    return _product(*(_integer(side, side_cap) for side in value))


def _declared(name, task):
    if name == "images":
        return {"source_pixels": _pixels(task.get("source_size"))}
    if name == "ocr":
        pages = _integer(task.get("page_count"), 3)
        pixels = _pixels(task.get("expected_render_size"), 2400)
        return {"pages": pages, "render_pixels": _product(pages, pixels)}
    if name == "video":
        frames = _integer(task.get("frames"), 48)
        pixels = _product(_integer(task.get("width"), 640),
                          _integer(task.get("height"), 360))
        return {"frames": frames, "frame_pixels": _product(frames, pixels)}
    if name in ("http", "cached_http", "downloads"):
        result = {"payload_bytes": _integer(task.get("size"), 16 * 1024 * 1024),
                  "response_chunk": _integer(task.get("response_chunk"), 65536)}
        if name == "downloads":
            result["parts"] = _integer(task.get("parts"), 64)
        return result
    if name == "discovery":
        # Missing probes is not silently treated as the workload's default of 1.
        return {"probes": _integer(task.get("probes"), 32)}
    if name == "geospatial":
        origins, destinations = _count(task.get("origins")), _count(task.get("destinations"))
        return {"origins": origins, "destinations": destinations,
                "route_pairs": _product(origins, destinations)}
    if name == "sqlite":
        return {"query_length": _integer(task.get("length"), 1048576)}
    return {}


def _log2_ceiling(value):
    return (value - 1).bit_length()


def batch_features(name: str, tasks: list[dict] | tuple[dict, ...]) -> dict[str, int]:
    """Summarize only allowlisted fixture metadata; never mutate tasks.

    Invalid/missing dimensions are omitted for the *whole* batch (not summed
    partially). Unknown workloads and metadata-free batches use batch_size only.
    Malformed outer containers and batches over 10,000 raise ValueError.
    Values are positive built-in integers; bools, floats and coercions are ignored.
    Each declared dimension has ceil(log2(sum)) and ceil(log2(max)) buckets,
    retaining aggregate work and largest-task information for mixed-cost batches.
    Zero-size batches have only batch_size=0. Feature names are schema-stable;
    identities and order never enter the summary. These are cost proxies, not
    confidence scores or claims about CPU, GIL, memory or scheduling performance.
    """
    if type(name) is not str:
        raise ValueError("workload name must be a string")
    if type(tasks) not in (list, tuple) or len(tasks) > MAX_BATCH_SIZE:
        raise ValueError("tasks must be a list or tuple of at most 10000 dictionaries")
    if any(type(task) is not dict for task in tasks):
        raise ValueError("each task must be a plain dictionary")
    result = {"batch_size": len(tasks)}
    if not tasks:
        return result
    rows = [_declared(name, task) for task in tasks]
    for dimension in rows[0]:
        values = [row[dimension] for row in rows]
        if any(value is None for value in values):
            continue
        result[dimension + "_sum_log2"] = _log2_ceiling(sum(values))
        result[dimension + "_max_log2"] = _log2_ceiling(max(values))
    return result
