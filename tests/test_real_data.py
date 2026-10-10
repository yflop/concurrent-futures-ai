"""Correctness checks for original data replicas, including adversarial dedup."""
import builtins
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import closing
import importlib.util
import hashlib
import heapq
import zlib
import json
import multiprocessing
from pathlib import Path
import pickle
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from benchmarks.real_world import data


class DataWorkloadsTests(unittest.TestCase):
    def test_dedup_exact_groups_and_collisions(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "nested").mkdir()
            prefix = b"p" * data.PREFIX
            values = {"a": prefix+b"aaa", "nested/b": prefix+b"aaa", "c": prefix+b"bbb", "d": prefix+b"bbb", "e": prefix+b"ccc", "empty": b"", "nested/empty": b""}
            for path, content in values.items():
                (root/path).write_bytes(content)
            (root/"link").symlink_to(root/"a")
            (root/"dirlink").symlink_to(root/"nested", target_is_directory=True)
            (root/"dangling").symlink_to(root/"missing")
            expected = [["a", "nested/b"], ["c", "d"], ["empty", "nested/empty"]]
            # The API returns native relative path strings on each platform.
            expected = [[str(Path(name)) for name in group] for group in expected]
            self.assertEqual(data.find_duplicates(root), expected)
            with patch.object(data, "_digest_file", return_value="forced-collision"):
                self.assertEqual(data.find_duplicates(root), expected)

    def test_dedup_skips_unreadable_file(self):
        real_open = builtins.open
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name in ("a", "b", "denied"):
                (root/name).write_bytes(b"same")
            def guarded(path, *args, **kwargs):
                if Path(path).name == "denied":
                    raise PermissionError("test denial")
                return real_open(path, *args, **kwargs)
            with patch("builtins.open", side_effect=guarded):
                self.assertEqual(data.find_duplicates(root), [["a", "b"]])

    def test_unique_sizes_never_opened(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for n in range(10):
                (root/str(n)).write_bytes(b"a"*n)
            with patch("builtins.open", side_effect=AssertionError("unique sizes must not be read")):
                self.assertEqual(data.find_duplicates(root), [])

    def test_prefix_filter_avoids_full_hash(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root/"a").write_bytes(b"a" * (data.CHUNK*2))
            (root/"b").write_bytes(b"b" * (data.CHUNK*2))
            with patch.object(data, "_digest_file", side_effect=AssertionError("different prefixes must not be hashed")):
                self.assertEqual(data.find_duplicates(root), [])

    def test_dedup_matches_independent_content_oracle(self):
        with tempfile.TemporaryDirectory() as temp:
            task = data.prepare("dedup", Path(temp), 1, 42)[0]
            root = Path(task["path"])
            groups = {}
            for path in root.rglob("*"):
                if path.is_file():
                    groups.setdefault(path.read_bytes(), []).append(str(path.relative_to(root)))
            oracle = sorted(sorted(g) for g in groups.values() if len(g)>1)
            result = data.run_task(task)
            self.assertEqual(result["groups"], oracle)
            self.assertEqual(result["duplicate_files"], 14)

    def test_pipeline_independent_counts(self):
        with tempfile.TemporaryDirectory() as temp:
            task = data.prepare("pipeline", Path(temp), 1, 3)[0]
            expected = sum(json.loads(line)["text"].lower().split().count("python") for line in Path(task["path"]).read_text().splitlines())
            result = data.run_task(task)
            self.assertEqual(result["word_count"], expected)
            self.assertEqual(result["pages"], 32)

    def test_sqlite_readonly_unchanged_and_integer_aggregates(self):
        with tempfile.TemporaryDirectory() as temp:
            tasks = data.prepare("sqlite", Path(temp), 8, 8)
            before = Path(tasks[0]["path"]).read_bytes()
            with ThreadPoolExecutor(max_workers=4) as executor:
                results = list(executor.map(data.run_task, tasks))
            self.assertTrue(all(r["rows"] == 1024 for r in results))
            self.assertEqual(before, Path(tasks[0]["path"]).read_bytes())
            with closing(sqlite3.connect(tasks[0]["path"])) as db:
                self.assertEqual(db.execute("PRAGMA integrity_check").fetchone(), ("ok",))
                self.assertEqual(db.execute("SELECT COUNT(*) FROM bars").fetchone(), (8192,))

    @unittest.skipUnless(importlib.util.find_spec("networkx"), "optional networkx not installed")
    def test_geospatial_shortest_paths_match_floyd_warshall(self):
        import networkx as nx
        with tempfile.TemporaryDirectory() as temp:
            task = data.prepare("geospatial", Path(temp), 1, 2)[0]
            fixture = json.loads(Path(task["path"]).read_text())
            graph = nx.Graph()
            graph.add_weighted_edges_from(fixture["edges"], weight="km")
            distances = nx.floyd_warshall(graph, weight="km")
            expected = round(sum(round(distances[a][b],6) for a in task["origins"] for b in task["destinations"]),6)
            result = data.run_task(task)
            self.assertEqual(result["routes"], 128)
            self.assertEqual(result["total_km"], expected)
            self.assertAlmostEqual(data.haversine_km((0,0),(0,1)), 111.19508, places=4)

    def test_determinism_serialization_and_spawn(self):
        names = [n for n in data.NAMES if n != "geospatial" or importlib.util.find_spec("networkx")]
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name in names:
                with self.subTest(name=name):
                    tasks = data.prepare(name, root/"one", 2, 17)
                    other = data.prepare(name, root/"two", 2, 17)
                    self.assertEqual(tasks, pickle.loads(pickle.dumps(tasks)))
                    self.assertEqual(tasks, json.loads(json.dumps(tasks)))
                    reference = list(map(data.run_task,tasks))
                    self.assertEqual(reference, list(map(data.run_task,other)))
                    with ProcessPoolExecutor(max_workers=2, mp_context=multiprocessing.get_context("spawn")) as executor:
                        self.assertEqual(reference, list(executor.map(data.run_task,tasks)))

    def test_representative_dedup_exact_oracle_and_heterogeneity(self):
        with tempfile.TemporaryDirectory() as temp:
            tasks = data.prepare("dedup", Path(temp), 3, 42, scale="representative")
            counts = []
            for task in tasks:
                root = Path(task["path"])
                groups = {}
                sizes = set()
                for path in root.rglob("*"):
                    if path.is_file():
                        payload = path.read_bytes()
                        sizes.add(len(payload))
                        groups.setdefault(payload, []).append(str(path.relative_to(root)))
                counts.append(sum(map(len, groups.values())))
                self.assertGreaterEqual(len(sizes), 8)
                self.assertEqual(data.run_task(task)["groups"], sorted(sorted(g) for g in groups.values() if len(g)>1))
            self.assertGreaterEqual(min(counts), 128)
            self.assertLessEqual(max(counts), 512)
            self.assertEqual(len(set(counts)), 3)

    def test_representative_pipeline_independent_streaming_oracle(self):
        with tempfile.TemporaryDirectory() as temp:
            tasks = data.prepare("pipeline", Path(temp), 3, 18, scale="representative")
            for task, pages in zip(tasks, (128,256,512)):
                digest = hashlib.sha256()
                total = compressed = 0
                with Path(task["path"]).open() as source:
                    for line in source:
                        page = json.loads(line)
                        count = page["text"].lower().split().count("python")
                        encoded = json.dumps({"id":page["id"], "count":count}, sort_keys=True, separators=(",", ":")).encode()
                        digest.update(encoded)
                        total += count
                        compressed += len(zlib.compress(encoded))
                # Any accidental whole-file convenience read fails here.
                with patch.object(Path, "read_text", side_effect=AssertionError("must stream")):
                    result = data.run_task(task)
                self.assertEqual(result, {"id":task["id"], "pages":pages, "word_count":total,
                                          "compressed_bytes":compressed, "sha256":digest.hexdigest()})

    def test_representative_sqlite_independent_filter_and_sort(self):
        with tempfile.TemporaryDirectory() as temp:
            tasks = data.prepare("sqlite", Path(temp), 6, 8, scale="representative")
            before = hashlib.sha256(Path(tasks[0]["path"]).read_bytes()).hexdigest()
            with closing(sqlite3.connect(tasks[0]["path"])) as db:
                # Fetch each symbol once, then independently filter/sort in Python.
                self.assertEqual(db.execute("SELECT COUNT(*) FROM bars").fetchone()[0], 98304)
                source = {task["symbol"]: db.execute("SELECT day, open, high, low, close, volume FROM bars WHERE symbol=?", (task["symbol"],)).fetchall() for task in tasks}
            for task in tasks:
                rows = sorted((row for row in source[task["symbol"]] if task["start"] <= row[0] < task["start"]+task["length"] and row[5] >= task["min_volume"]), reverse=task["descending"])
                expected = {"id":task["id"], "symbol":task["symbol"], "rows":len(rows),
                            "price_volume":sum(r[4]*r[5] for r in rows), "volume":sum(r[5] for r in rows),
                            "sha256":hashlib.sha256(json.dumps(rows,separators=(",", ":")).encode()).hexdigest()}
                self.assertEqual(data.run_task(task), expected)
            self.assertEqual(before, hashlib.sha256(Path(tasks[0]["path"]).read_bytes()).hexdigest())

    @unittest.skipUnless(importlib.util.find_spec("networkx"), "optional networkx not installed")
    def test_representative_graph_independent_heap_dijkstra(self):
        with tempfile.TemporaryDirectory() as temp:
            task = data.prepare("geospatial", Path(temp), 1, 2, scale="representative")[0]
            fixture = json.loads(Path(task["path"]).read_text())
            self.assertEqual(len(fixture["nodes"]), 1024)
            adjacency = {node:[] for node in fixture["nodes"]}
            for a,b,weight in fixture["edges"]:
                self.assertAlmostEqual(weight, data.haversine_km(fixture["nodes"][a], fixture["nodes"][b]))
                adjacency[a].append((b,weight))
                adjacency[b].append((a,weight))
            total = 0
            for origin in task["origins"]:
                best = {origin:0.0}
                queue = [(0.0,origin)]
                while queue:
                    distance,node = heapq.heappop(queue)
                    if distance != best[node]:
                        continue
                    for neighbor,weight in adjacency[node]:
                        candidate = distance+weight
                        if candidate < best.get(neighbor,float("inf")):
                            best[neighbor] = candidate
                            heapq.heappush(queue,(candidate,neighbor))
                total += sum(round(best[d],6) for d in task["destinations"])
            result = data.run_task(task)
            self.assertEqual(result["total_km"],round(total,6))
            self.assertEqual(result["routes"],128)

    def test_representative_thread_spawn_and_seed_parity(self):
        names = [n for n in data.NAMES if n != "geospatial" or importlib.util.find_spec("networkx")]
        with tempfile.TemporaryDirectory() as temp:
            for name in names:
                with self.subTest(name=name):
                    tasks = data.prepare(name, Path(temp)/"one", 2, 12, scale="representative")
                    other = data.prepare(name, Path(temp)/"two", 2, 12, scale="representative")
                    self.assertEqual(tasks,json.loads(json.dumps(tasks)))
                    self.assertEqual(tasks,pickle.loads(pickle.dumps(tasks)))
                    reference = list(map(data.run_task,tasks))
                    self.assertEqual(reference,list(map(data.run_task,other)))
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        self.assertEqual(reference,list(pool.map(data.run_task,tasks)))
                    with ProcessPoolExecutor(max_workers=2,mp_context=multiprocessing.get_context("spawn")) as pool:
                        self.assertEqual(reference,list(pool.map(data.run_task,tasks)))

    def test_missing_geospatial_dependency_is_actionable(self):
        with patch.dict("sys.modules", {"networkx": None}):
            with self.assertRaisesRegex(RuntimeError, "pip install networkx"):
                data._networkx()

    def test_invalid_inputs(self):
        with tempfile.TemporaryDirectory() as temp:
            for count in (0,-1,True,1.1,10001):
                with self.assertRaises(ValueError):
                    data.prepare("pipeline",Path(temp),count,1)
            for scale, count in (("large",1),("representative",65)):
                with self.assertRaises(ValueError):
                    data.prepare("pipeline",Path(temp),count,1,scale=scale)
            with self.assertRaises(ValueError):
                data.prepare("unknown",Path(temp),1,1)
            with self.assertRaises(ValueError):
                data.run_task({"name":"unknown"})


if __name__ == "__main__":
    unittest.main()
