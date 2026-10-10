"""Read-only concept registry; catalog entries never execute code."""
import json
from importlib.resources import files


class ConceptRegistry:
    """Validate and query an extensible JSON concept collection.

    Runtime availability is determined by code, not catalog labels.
    Loading a catalog performs no network access or plugin import.
    """
    def __init__(self, catalog):
        # JSON round trip provides an owned copy and rejects arbitrary objects.
        self._catalog = json.loads(json.dumps(catalog))
        if self._catalog.get("schema_version") != 1:
            raise ValueError("unsupported catalog schema_version")
        entries = self._catalog.get("concepts")
        if not isinstance(entries, list):
            raise ValueError("concepts must be a list")
        self._index = {}
        required = ("id", "title", "status", "category", "mechanism",
                    "applicability", "risks", "benchmark_method")
        for entry in entries:
            if not isinstance(entry, dict) or any(not isinstance(entry.get(k), str) or not entry[k] for k in required):
                raise ValueError("concepts require nonempty text fields")
            if entry["status"] not in {"implemented", "experimental", "planned"}:
                raise ValueError("invalid concept status")
            if entry["id"] in self._index:
                raise ValueError("duplicate concept id")
            for field in ("references", "depends_on"):
                if not isinstance(entry.get(field), list) or any(not isinstance(v, str) for v in entry[field]):
                    raise ValueError(f"{field} must be a list of strings")
            self._index[entry["id"]] = entry
        for entry in entries:
            if any(dependency not in self._index or dependency == entry["id"] for dependency in entry["depends_on"]):
                raise ValueError("missing or self-referential dependency")
        # Reject cycles so future roadmap tools can traverse dependencies safely.
        visited, active = set(), set()
        def visit(key):
            if key in active:
                raise ValueError("cyclic concept dependency")
            if key in visited:
                return
            active.add(key)
            for dependency in self._index[key]["depends_on"]:
                visit(dependency)
            active.remove(key)
            visited.add(key)
        for key in self._index:
            visit(key)

    @classmethod
    def bundled(cls):
        return cls(json.loads(files("intelligent_futures").joinpath("concepts.json").read_text(encoding="utf-8")))

    def get(self, concept_id):
        return json.loads(json.dumps(self._index[concept_id]))

    def search(self, text="", *, status=None, category=None):
        if status is not None and status not in {"implemented", "experimental", "planned"}:
            raise ValueError("invalid status")
        needle = text.casefold()
        return [self.get(key) for key, entry in self._index.items()
                if (status is None or entry["status"] == status)
                and (category is None or entry["category"] == category)
                and needle in json.dumps(entry, ensure_ascii=False).casefold()]

    def __len__(self):
        return len(self._index)
