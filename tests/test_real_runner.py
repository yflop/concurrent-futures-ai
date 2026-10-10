"""Measurement contract tests, independent of optional workload libraries."""
import unittest
from unittest.mock import patch
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor

from benchmarks.real_world import execute_batch, module_for
from benchmarks.real_world.run import Configuration, measure_pool, summarize, score, acceptance_failures, main


def example(task):
    if task.get("fail"):
        raise ValueError("expected failure")
    return {"number": task["number"] * 2}


class RunnerTests(unittest.TestCase):
    def test_unknown_workload_rejected(self):
        with self.assertRaises(ValueError):
            module_for("remote-scan")

    def test_batch_captures_error_without_replay(self):
        with patch("benchmarks.real_world.module_for", return_value=SimpleNamespace(run_task=example)):
            rows = execute_batch(("fixture", [{"number": 3}, {"fail": True}]))
        self.assertEqual(rows[0]["value"], {"number": 6})
        self.assertEqual(rows[1]["error"], "ValueError: expected failure")
        self.assertGreaterEqual(rows[0]["service_seconds"], 0)

    def test_bounded_measurement_correct_count_and_order(self):
        tasks = [{"number": n} for n in range(11)]
        config = Configuration("thread", 2, 4)
        with patch("benchmarks.real_world.module_for", return_value=SimpleNamespace(run_task=example)):
            with ThreadPoolExecutor(max_workers=2) as pool:
                row = measure_pool(pool, "stdlib", config, "fixture", tasks)
        self.assertEqual(row["tasks"], 11)
        self.assertEqual(row["error_rate"], 0)
        self.assertEqual(len(row["completion_trace"]), 3)
        self.assertGreater(row["batch_completion_latency_p95_seconds"], 0)

    def test_selection_uses_calibration_not_heldout(self):
        def row(engine, workers, cold, warm, seed, correct=True):
            return {"engine": engine, "configuration": {"backend": "thread", "workers": workers, "batch_size": 1},
                    "seed": seed, "cold_including_construction_seconds": cold,
                    "cold": {"seconds": cold, "correct": correct}, "warm": {"seconds": warm, "correct": correct}}
        calibration = [row("stdlib", 1, 1, 1, 1), row("stdlib", 2, 2, 2, 1),
                       row("intelligent", 1, 1, 1, 1), row("intelligent", 2, .1, .1, 1, False)]
        heldout = [row("stdlib", 1, 2, 2, 9), row("intelligent", 1, 3, 1, 9)]
        result = summarize(calibration, heldout)
        self.assertEqual(result["chosen_from_calibration"]["intelligent"]["workers"], 1)
        self.assertEqual(result["near_optimal_coverage"], .5)
        self.assertEqual(result["convergence"]["status"], "not_established")
        self.assertEqual(score(calibration[-1]), float("inf"))


class AcceptanceTests(unittest.TestCase):
    def test_heldout_correctness_and_resource_failures_fail_closed(self):
        row = {"engine": "intelligent", "seed": 9,
               "cold": {"correct": False, "error_rate": .25, "batch_completion_latency_p95_seconds": 99},
               "warm": {"correct": True, "error_rate": 0, "batch_completion_latency_p95_seconds": .1},
               "sampled_tree_peak_rss_bytes": 2**40, "lifecycle_cpu_seconds": None}
        failures = acceptance_failures([row])
        self.assertGreaterEqual(len(failures), 4)
        self.assertTrue(any("incorrect" in item for item in failures))

    def test_invalid_or_absent_measurements_cannot_pass(self):
        self.assertTrue(acceptance_failures([]))
        rows = [{"engine": engine, "seed": 1,
                 "cold": {"correct": True, "error_rate": 0, "batch_completion_latency_p95_seconds": float("nan")},
                 "warm": {"correct": True, "error_rate": 0},
                 "sampled_tree_peak_rss_bytes": float("nan"), "lifecycle_cpu_seconds": -1}
                for engine in ("stdlib", "intelligent")]
        self.assertEqual(len(acceptance_failures(rows)), 8)

    def test_cli_preserves_bad_rows_and_returns_failure(self):
        import json
        import tempfile
        from pathlib import Path
        row = {"engine": "intelligent", "seed": 9,
               "cold": {"correct": False}, "warm": {"correct": True},
               "sampled_tree_peak_rss_bytes": 100, "lifecycle_cpu_seconds": .1}
        report = {"name": "images", "heldout": [row], "summary": {"near_optimal_coverage": 0}}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.json"
            with patch("sys.argv", ["runner", "--quick", "--workloads", "images", "--output", str(path)]), \
                 patch("benchmarks.real_world.run.workload_environment", return_value={}), \
                 patch("benchmarks.real_world.run.benchmark", return_value=report):
                self.assertEqual(main(), 1)
            saved = json.loads(path.read_text())
        self.assertFalse(saved["workloads"][0]["heldout"][0]["cold"]["correct"])
        self.assertFalse(saved["workloads"][0]["accepted"])
        self.assertEqual(saved["failures"][0]["error"], "held-out acceptance failed")

    def test_zero_count_is_rejected(self):
        with patch("sys.argv", ["runner", "--count", "0", "--output", "unused.json"]):
            with self.assertRaises(SystemExit) as caught:
                main()
        self.assertEqual(caught.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
