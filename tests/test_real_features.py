"""Metadata-only checks: no fixtures, optional libraries, network or subprocesses."""
import copy
import json
import math
import unittest
from unittest.mock import patch

from benchmarks.real_world.features import batch_features
from intelligent_futures.prediction import normalize_features


class FeatureTests(unittest.TestCase):
    def test_representative_fixtures(self):
        examples = {
            "images": ({"source_size": [1600, 1200]}, "source_pixels", 21),
            "ocr": ({"page_count": 3, "expected_render_size": [1654, 2339]}, "render_pixels", 24),
            "video": ({"width": 640, "height": 360, "frames": 48}, "frame_pixels", 24),
            "http": ({"size": 4194304, "response_chunk": 8192}, "payload_bytes", 22),
            "cached_http": ({"size": 16384}, "payload_bytes", 14),
            "downloads": ({"size": 65536, "parts": 8}, "parts", 3),
            "discovery": ({"probes": 4}, "probes", 2),
            "geospatial": ({"origins": ["private"] * 16, "destinations": ["private"] * 128}, "route_pairs", 11),
            "sqlite": ({"length": 4096, "start": 123, "symbol": "private"}, "query_length", 12),
        }
        for name, (task, dimension, bucket) in examples.items():
            with self.subTest(name=name):
                result = batch_features(name, [task])
                self.assertEqual(result[dimension + "_sum_log2"], bucket)
                self.assertEqual(result[dimension + "_max_log2"], bucket)
                self.assertLessEqual(len(result), 32)
                self.assertTrue(all(type(v) is int and math.isfinite(v) for v in result.values()))
                normalize_features(result)

    def test_mixed_cost_total_and_max_are_distinct(self):
        mixed = batch_features("http", [{"size": 1024}, {"size": 3072}])
        equal = batch_features("http", [{"size": 2048}, {"size": 2048}])
        self.assertEqual(mixed["payload_bytes_sum_log2"], equal["payload_bytes_sum_log2"])
        self.assertNotEqual(mixed["payload_bytes_max_log2"], equal["payload_bytes_max_log2"])
        self.assertNotEqual(batch_features("images", [{"source_size": [640, 480]}]),
                            batch_features("images", [{"source_size": [2560, 1440]}]))

    def test_stable_reordering_and_no_mutation_or_file_access(self):
        tasks = [{"source_size": [640, 480], "path": "/secret", "input_sha256": "private"},
                 {"source_size": [2560, 1440], "rawresults": b"secret"}]
        saved = copy.deepcopy(tasks)
        with patch("builtins.open", side_effect=AssertionError("must not read files")):
            result = batch_features("images", tasks)
            self.assertEqual(result, batch_features("images", list(reversed(tasks))))
        self.assertEqual(tasks, saved)
        self.assertNotIn("private", json.dumps(result))
        self.assertNotIn("secret", json.dumps(result))

    def test_fallbacks_and_no_raw_content_length(self):
        for name in ("unknown", "pipeline", "dedup", "discovery", "images"):
            self.assertEqual(batch_features(name, [{"path": "/x", "text": "x" * 100,
                "payload": b"x" * 100, "size": 123, "expected_pages": ["text"]}]), {"batch_size": 1})
        self.assertEqual(batch_features("images", []), {"batch_size": 0})

    def test_invalid_metadata_is_ignored_without_coercion(self):
        for value in (True, False, 1.0, float("nan"), float("inf"), "100", None,
                      -1, 0, 1 << 10000, [], {}, object()):
            with self.subTest(value=type(value).__name__):
                self.assertEqual(batch_features("http", [{"size": value}]), {"batch_size": 1})
        self.assertEqual(batch_features("images", [{"source_size": [100, True]}]), {"batch_size": 1})
        self.assertEqual(batch_features("images", [{"source_size": [100, 100, 100]}]), {"batch_size": 1})
        self.assertEqual(batch_features("geospatial", [{"origins": "private", "destinations": [1]}]),
                         {"batch_size": 1, "destinations_sum_log2": 0, "destinations_max_log2": 0})

    def test_incomplete_batch_dimension_is_not_partial_sum(self):
        self.assertEqual(batch_features("http", [{"size": 1024}, {}]), {"batch_size": 2})
        result = batch_features("ocr", [{"page_count": 2, "expected_render_size": [500, 500]},
                                        {"page_count": 3}])
        self.assertEqual(result, {"batch_size": 2, "pages_sum_log2": 3, "pages_max_log2": 2})

    def test_bounds_and_outer_validation(self):
        result = batch_features("http", [{"size": 16 * 1024 * 1024}] * 10000)
        self.assertEqual(result["payload_bytes_sum_log2"], 38)
        for tasks in ([{}] * 10001, iter([{}]), {}, [None]):
            with self.assertRaises(ValueError):
                batch_features("http", tasks)
        with self.assertRaises(ValueError):
            batch_features(None, [{}])


if __name__ == "__main__":
    unittest.main()
