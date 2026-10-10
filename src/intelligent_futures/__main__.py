"""python -m intelligent_futures [query] [--status planned]"""
import argparse
import json
from .catalog import ConceptRegistry


def main():
    parser = argparse.ArgumentParser(description="Explore the non-executable optimization concept library")
    parser.add_argument("query", nargs="?", default="")
    parser.add_argument("--status", choices=("implemented", "experimental", "planned"))
    parser.add_argument("--category")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    entries = ConceptRegistry.bundled().search(args.query, status=args.status, category=args.category)
    if args.json:
        print(json.dumps(entries, indent=2))
    else:
        for entry in entries:
            print(f'{entry["id"]} [{entry["status"]}] {entry["title"]}: {entry["mechanism"]}')
        print(f"\n{len(entries)} concepts. Catalog labels do not activate runtime features.")


if __name__ == "__main__":
    main()
