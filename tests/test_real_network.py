"""Optional real-loopback integration coverage; no third-party network access."""
import importlib.util
import json
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from benchmarks.real_world import network

HAS_REQUESTS = importlib.util.find_spec("requests") is not None
HAS_CACHE = importlib.util.find_spec("requests_cache") is not None


@unittest.skipUnless(HAS_REQUESTS, "optional requests dependency is not installed")
class NetworkTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.context = network.fixture(self.root)
        self.server = self.context.__enter__()

    def tearDown(self):
        self.context.__exit__(None, None, None)
        self.temp.cleanup()

    def test_http_and_ranges(self):
        for name in ("http", "downloads"):
            tasks = network.prepare(name, self.root, 3, 17)
            json.dumps(tasks)
            with ThreadPoolExecutor(2) as pool:
                outputs = list(pool.map(network.run_task, tasks))
            for task, result in zip(tasks, outputs):
                self.assertEqual(result["sha256"], task["sha256"])
                self.assertEqual(result["bytes"], task["size"])
                if name == "downloads":
                    self.assertEqual(result["parts"], 4)
                    self.assertEqual(self.server.requests[network.urlsplit(task["url"]).path], 4)

    def test_measurement_subdirectories(self):
        left = network.prepare("http", self.root / "cold", 2, 19)
        right = network.prepare("http", self.root / "warm", 2, 19)
        self.assertNotEqual(left[0]["work"], right[0]["work"])
        self.assertEqual(list(map(network.run_task, left)), list(map(network.run_task, right)))

    def test_spawn_safe(self):
        for name in ("http", "downloads", "discovery", "cached_http"):
            if name == "cached_http" and not HAS_CACHE:
                continue
            tasks = network.prepare(name, self.root, 2, 11)
            with ProcessPoolExecutor(2, mp_context=multiprocessing.get_context("spawn")) as pool:
                actual = list(pool.map(network.run_task, tasks))
            expected = [network.run_task(task) for task in tasks]
            self.assertEqual(actual, expected)

    def test_discovery_both_outcomes(self):
        tasks = network.prepare("discovery", self.root, 4, 0)
        results = list(map(network.run_task, tasks))
        self.assertEqual([r["open"] for r in results], [True, False, True, False])
        self.assertEqual(results[0]["device"], "Local fixture")

    @unittest.skipUnless(HAS_CACHE, "optional requests-cache dependency is not installed")
    def test_cache_isolation_and_warm_hit(self):
        task = network.prepare("cached_http", self.root, 1, 4)[0]
        for _ in range(2):
            result = network.run_task(task)
            self.assertEqual((result["cold_misses"], result["warm_hits"]), (1, 1))
        self.assertEqual(self.server.requests[network.urlsplit(task["url"]).path], 2)
        self.assertEqual(list(Path(task["work"]).glob("cache-*")), [])

    def test_representative_payloads_and_sustained_connections(self):
        for name in ("http", "downloads", "cached_http"):
            if name == "cached_http" and not HAS_CACHE:
                continue
            tasks = network.prepare(name, self.root / name / "trial", 24, 71,
                                    scale="representative")
            self.assertEqual(len(tasks), 24)
            self.assertEqual({t["size"] for t in tasks}, set(network.REPRESENTATIVE_SIZES))
            self.assertEqual({t["response_chunk"] for t in tasks}, set(network.RESPONSE_CHUNKS))
            json.dumps(tasks)
            # Exercise every size/chunk combination without a throughput benchmark.
            selected = tasks if name == "http" else tasks[:6]
            with ThreadPoolExecutor(8) as pool:
                first = list(pool.map(network.run_task, selected))
                second = list(pool.map(network.run_task, selected))
            self.assertEqual(first, second)
            for task, output in zip(selected, first):
                self.assertEqual(output["bytes"], task["size"])
                self.assertEqual(output["sha256"], task["sha256"])
                expected_requests = task["parts"] * 2 if name == "downloads" else 2
                self.assertEqual(self.server.requests[network.urlsplit(task["url"]).path],
                                 expected_requests)
                self.assertEqual(list(Path(task["work"]).glob("parts-*")), [])
                self.assertEqual(list(Path(task["work"]).glob("cache-*")), [])

    def test_representative_discovery_is_bounded_and_repeatable(self):
        tasks = network.prepare("discovery", self.root / "probes", 24, 71,
                                scale="representative")
        with ThreadPoolExecutor(8) as pool:
            outputs = list(pool.map(network.run_task, tasks))
        self.assertEqual([r["open"] for r in outputs], [i % 2 == 0 for i in range(24)])
        self.assertTrue(all(r["probes"] == 4 for r in outputs))
        self.assertEqual(self.server.requests["/common/basic_info"], 48)
        for count in (0, True, 33):
            with self.assertRaisesRegex(ValueError, "probe count"):
                network.run_task(dict(tasks[0], probes=count))

    def test_injected_failures_are_not_retried(self):
        import requests
        task = network.prepare("http", self.root, 1, 0)[0]
        base = task["url"].split("/data/")[0]
        failures = (("/failure", requests.HTTPError),
                    ("/missing", requests.HTTPError),
                    ("/truncated", requests.exceptions.ChunkedEncodingError),
                    ("/redirect", ValueError))
        with patch.dict("os.environ", {"HTTP_PROXY": "http://127.0.0.1:1",
                                       "ALL_PROXY": "http://127.0.0.1:1", "NO_PROXY": ""}):
            for path, exception in failures:
                with self.assertRaises(exception):
                    network.run_task(dict(task, url=base + path))
                self.assertEqual(self.server.requests[path], 1)
            # The handler waits for a teardown event; the client has a shorter
            # injected timeout. Normal workloads never wait or sleep artificially.
            with patch.object(network, "TIMEOUT", 0.05):
                with self.assertRaises(requests.exceptions.ReadTimeout):
                    network.run_task(dict(task, url=base + "/blocked"))
            self.assertEqual(self.server.requests["/blocked"], 1)

    def test_range_protocol(self):
        import requests
        task = network.prepare("http", self.root, 1, 0)[0]
        with requests.Session() as session:
            session.trust_env = False
            for header in ("bytes=99999999-", "bytes=5-1", "bytes=-5", "garbage"):
                with session.get(task["url"], headers={"Range": header}, timeout=3) as response:
                    self.assertEqual(response.status_code, 416)
            with session.get(task["url"], headers={"Range": "bytes=3-9"}, timeout=3) as response:
                self.assertEqual(response.status_code, 206)
                self.assertEqual(len(response.content), 7)

    def test_failures_surface(self):
        import requests
        task = network.prepare("http", self.root, 1, 0)[0]
        with self.assertRaisesRegex(ValueError, "manifest"):
            network.run_task(dict(task, sha256="incorrect"))
        base = task["url"].split("/data/")[0]
        with self.assertRaises(requests.HTTPError):
            network.run_task(dict(task, url=base + "/failure"))
        # Discovery protocol endpoint ignores Range, which download must reject.
        with self.assertRaisesRegex(ValueError, "byte range"):
            network.run_task(dict(task, name="downloads", url=base + "/common/basic_info"))
        self.assertEqual(list(Path(task["work"]).glob("parts-*")), [])
        download = network.prepare("downloads", self.root, 1, 0)[0]
        with self.assertRaisesRegex(ValueError, "manifest"):
            network.run_task(dict(download, sha256="incorrect"))
        self.assertEqual(list(Path(download["work"]).glob("parts-*")), [])
        with self.assertRaisesRegex(ValueError, "127.0.0.1"):
            network.run_task(dict(task, url="https://example.com/"))

    @unittest.skipUnless(HAS_CACHE, "optional requests-cache dependency is not installed")
    def test_cache_error_surfaces_and_cleanup(self):
        import requests
        task = network.prepare("cached_http", self.root, 1, 0)[0]
        base = task["url"].split("/data/")[0]
        with self.assertRaises(requests.HTTPError):
            network.run_task(dict(task, url=base + "/failure"))
        self.assertEqual(list(Path(task["work"]).glob("cache-*")), [])


class NetworkValidationTests(unittest.TestCase):
    def test_bad_arguments(self):
        for count in (-1, True, 10001):
            with self.assertRaises(ValueError):
                network.prepare("http", Path("unused"), count, 0)
        with self.assertRaises(ValueError):
            network.prepare("http", Path("unused"), 1, 0, scale="huge")
        with self.assertRaises(ValueError):
            network.prepare("not-real", Path("unused"), 1, 0)

    def test_fixture_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with network.fixture(root):
                self.assertTrue((root / "network-fixture.json").exists())
            self.assertFalse((root / "network-fixture.json").exists())


if __name__ == "__main__":
    unittest.main()
