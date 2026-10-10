"""Losslessly archive large public JSON reports; keep summary/ranking JSON readable."""
from __future__ import annotations
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path


def deterministic_gzip(payload: bytes) -> bytes:
    target = io.BytesIO()
    with gzip.GzipFile(filename='', mode='wb', fileobj=target, mtime=0, compresslevel=9) as stream:
        stream.write(payload)
    return target.getvalue()


def archive(root: Path, *, threshold: int = 100_000):
    root = Path(root)
    manifest_path = root / 'archives.json'
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {'schema_version': 1, 'archives': []}
    records = {row['path']: row for row in manifest['archives']}
    for path in sorted(root.rglob('*.json')):
        if (path == manifest_path or path.stat().st_size <= threshold
                or (path.stat().st_size <= 512_000
                    and any(word in path.stem for word in ('summary', 'rankings')))):
            continue
        payload = path.read_bytes()
        json.loads(payload.decode('utf-8'))
        compressed = deterministic_gzip(payload)
        if gzip.decompress(compressed) != payload:
            raise ValueError('archive round-trip mismatch')
        destination = path.with_name(path.name + '.gz')
        if destination.exists():
            raise ValueError('archive already exists beside plain report')
        destination.write_bytes(compressed)
        if gzip.decompress(destination.read_bytes()) != payload:
            raise ValueError('written archive round-trip mismatch')
        relative = str(destination.relative_to(root).as_posix())
        records[relative] = {'path': relative, 'payload_path': str(path.relative_to(root).as_posix()),
                            'payload_bytes': len(payload), 'archive_bytes': len(compressed),
                            'payload_sha256': hashlib.sha256(payload).hexdigest(),
                            'archive_sha256': hashlib.sha256(compressed).hexdigest()}
        path.unlink()
    manifest['scope'] = 'Lossless storage of already public-projected JSON; payload hashes identify the exact decompressed report bytes, not private raw checkpoints or measured source.'
    manifest['format'] = 'gzip level 9; mtime 0; no filename header; UTF-8 JSON payload'
    manifest['archives'] = [records[key] for key in sorted(records)]
    manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path, help='public report directory; updates paths and writes archives.json')
    args = parser.parse_args()
    print(json.dumps(archive(args.root), indent=2))


if __name__ == '__main__':
    main()
