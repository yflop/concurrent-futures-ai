"""Read bounded UTF-8 JSON evidence, optionally stored as deterministic gzip."""
from __future__ import annotations

import gzip
import json
from pathlib import Path

MAX_REPORT_BYTES = 32 * 1024 * 1024


def resolve_report(path: str | Path) -> Path:
    """Resolve an explicit archive or its plain-JSON name; reject ambiguity."""
    path = Path(path)
    if path.name.endswith('.json.gz'):
        return path
    if path.suffix != '.json':
        raise ValueError('report must be .json or .json.gz')
    archive = path.with_name(path.name + '.gz')
    if path.exists() and archive.exists():
        raise ValueError('both plain and compressed report exist')
    return archive if archive.exists() else path


def _read_payload(path: str | Path, *, max_bytes: int = MAX_REPORT_BYTES):
    """Decode JSON only, limiting decompressed bytes before parsing."""
    if type(max_bytes) is not int or not 0 < max_bytes <= MAX_REPORT_BYTES:
        raise ValueError('max_bytes must be a positive integer at most 32 MiB')
    path = resolve_report(path)
    opener = gzip.open if path.name.endswith('.json.gz') else open
    with opener(path, 'rb') as stream:
        payload = stream.read(max_bytes + 1)
    if len(payload) > max_bytes:
        raise ValueError('report exceeds decompressed byte limit')
    json.loads(payload.decode('utf-8'))
    return payload


def read_report(path: str | Path, *, max_bytes: int = MAX_REPORT_BYTES):
    """Load a bounded JSON report from plain or compressed storage."""
    return json.loads(_read_payload(path, max_bytes=max_bytes).decode('utf-8'))


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('--output', type=Path, required=True,
                        help='new plain JSON path; existing files are never overwritten')
    args = parser.parse_args()
    payload = _read_payload(args.input)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('xb') as stream:
        stream.write(payload)


if __name__ == '__main__':
    main()
