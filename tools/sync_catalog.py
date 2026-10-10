"""Validate canonical docs/concepts.json and synchronize the packaged copy.

Run from any working directory: python tools/sync_catalog.py [--check]
"""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from intelligent_futures import ConceptRegistry


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail on catalog drift without writing")
    args = parser.parse_args()
    source = ROOT / "docs" / "concepts.json"
    destination = ROOT / "src" / "intelligent_futures" / "concepts.json"
    payload = source.read_bytes()
    registry = ConceptRegistry(json.loads(payload))
    if args.check:
        if not destination.exists() or destination.read_bytes() != payload:
            raise SystemExit("Catalog drift: run python tools/sync_catalog.py")
    else:
        destination.write_bytes(payload)
    print(f"Validated {len(registry)} concepts; packaged catalog matches.")


if __name__ == "__main__":
    main()
