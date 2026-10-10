import copy
import json
from pathlib import Path
import unittest
from intelligent_futures import ConceptRegistry


class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.data = {"schema_version": 1, "concepts": [{
            "id": "A", "title": "Admission", "status": "implemented",
            "category": "queue", "mechanism": "Bound unfinished tasks",
            "applicability": "Fast producers", "risks": "Not a byte bound",
            "benchmark_method": "Saturate workers", "references": [], "depends_on": []}]}

    def test_search_and_owned_copies(self):
        registry = ConceptRegistry(self.data)
        self.data["concepts"][0]["title"] = "mutated"
        self.assertEqual(registry.get("A")["title"], "Admission")
        result = registry.search("ADMISSION", status="implemented", category="queue")
        self.assertEqual(len(result), 1)
        result[0]["title"] = "mutated"
        self.assertEqual(registry.get("A")["title"], "Admission")
        self.assertEqual(registry.search(status="planned"), [])
        self.assertEqual(len(registry), 1)

    def test_invalid_schema(self):
        self.data["schema_version"] = 2
        with self.assertRaises(ValueError):
            ConceptRegistry(self.data)

    def test_duplicate_id(self):
        self.data["concepts"].append(copy.deepcopy(self.data["concepts"][0]))
        with self.assertRaises(ValueError):
            ConceptRegistry(self.data)

    def test_bad_fields(self):
        for key in ("title", "status", "references", "depends_on"):
            with self.subTest(key=key):
                data = copy.deepcopy(self.data)
                data["concepts"][0][key] = 42
                with self.assertRaises(ValueError):
                    ConceptRegistry(data)

    def test_missing_dependency(self):
        self.data["concepts"][0]["depends_on"] = ["missing"]
        with self.assertRaises(ValueError):
            ConceptRegistry(self.data)

    def test_self_dependency(self):
        self.data["concepts"][0]["depends_on"] = ["A"]
        with self.assertRaises(ValueError):
            ConceptRegistry(self.data)

    def test_cycle(self):
        other = copy.deepcopy(self.data["concepts"][0])
        other.update(id="B", depends_on=["A"])
        self.data["concepts"][0]["depends_on"] = ["B"]
        self.data["concepts"].append(other)
        with self.assertRaises(ValueError):
            ConceptRegistry(self.data)

    def test_unknown_status(self):
        with self.assertRaises(ValueError):
            ConceptRegistry(self.data).search(status="magic")

    def test_bundled_catalog(self):
        registry = ConceptRegistry.bundled()
        self.assertGreaterEqual(len(registry), 40)
        self.assertGreater(len(registry.search(status="planned")), 0)
        source = Path(__file__).resolve().parents[1] / "docs" / "concepts.json"
        if source.exists():
            self.assertEqual(registry.search(), ConceptRegistry(json.loads(source.read_text())).search())


if __name__ == "__main__":
    unittest.main()
