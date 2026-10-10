"""Harness correctness tests without performance thresholds.

Run: PYTHONPATH=src python -m unittest benchmarks.test_run -v
"""
import contextlib
import doctest
import io
import unittest

from .run import distribution, make_executor, parse_args, percentile, run_batch, run_trial, summarize
from .workloads import build_workload, cpu_work
from . import run, workloads


class BenchmarkTests(unittest.TestCase):
    def test_documented_calculations(self):
        self.assertEqual(doctest.testmod(run, verbose=False).failed, 0)
        self.assertEqual(doctest.testmod(workloads, verbose=False).failed, 0)

    def test_percentiles_are_interpolated(self):
        self.assertEqual(percentile([0.0, 10.0], .95), 9.5)
        self.assertEqual(percentile([3.0], .95), 3.0)
        self.assertEqual(distribution([3.0])["stdev"], 0.0)
        with self.assertRaises(ValueError):
            percentile([], .5)

    def test_fixed_seed_workloads_are_reproducible(self):
        first = build_workload("mixed", 12, 50, 0.0, 42)
        self.assertEqual(first, build_workload("mixed", 12, 50, 0.0, 42))
        self.assertEqual(sum(spec.kind == "cpu" for spec in first), 6)
        self.assertEqual(sum(spec.kind == "io" for spec in first), 6)
        self.assertEqual(cpu_work(50, 1), cpu_work(50, 1))
        self.assertNotEqual(cpu_work(50, 1), cpu_work(50, 2))

    def test_invalid_workload_is_rejected(self):
        with self.assertRaises(ValueError):
            build_workload("other", 12, 50, 0.0, 42)

    def test_cli_validation(self):
        cases = (["--tasks", "0"], ["--workers", "0"],
                 ["--hybrid-process-workers", "-1"], ["--io-delay", "nan"],
                 ["--tasks", "1", "--workloads", "mixed"],
                 ["--output", "same.json", "--markdown", "same.json"])
        for args in cases:
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    parse_args(args)

    def test_quick_defaults_and_explicit_override(self):
        args = parse_args(["--quick", "--tasks", "8"])
        self.assertEqual(args.tasks, 8)
        self.assertEqual(args.repeats, 2)
        self.assertEqual(args.hybrid_thread_workers + args.hybrid_process_workers, args.workers)

    def test_thread_and_sequential_results_match(self):
        config = vars(parse_args(["--quick", "--tasks", "4", "--cpu-iterations", "50",
                                  "--io-delay", "0", "--max-pending", "1"]))
        specs = build_workload("mixed", 4, 50, 0.0, 42)
        direct = run_batch(None, "sequential", specs)
        self.assertIsNone(direct["submission_call_seconds"])
        for engine in ("thread", "intelligent-threads"):
            executor = make_executor(engine, config)
            try:
                result = run_batch(executor, engine, specs)
            finally:
                executor.shutdown(wait=True)
            self.assertEqual(direct["checksum"], result["checksum"])
            self.assertEqual(len(result["submission_call_seconds"]), len(specs))
            self.assertTrue(all(value >= 0 for value in result["task_latency_seconds"]))

    def test_warm_route_counts_exclude_warmups(self):
        config = vars(parse_args(["--quick", "--tasks", "4", "--cpu-iterations", "50",
                                  "--io-delay", "0", "--max-pending", "1"]))
        specs = build_workload("cpu", 4, 50, 0.0, 42)
        result = run_trial("intelligent-threads", specs, "warm", config)
        self.assertEqual(result["measured_backend_counts"], {"thread": 4, "process": 0})
        self.assertEqual(result["executor_snapshot"]["counts"]["completed"], 12)
        self.assertEqual(result["executor_snapshot"]["pending"], 0)
        self.assertEqual(result["timed_seconds"], result["batch_seconds"])
        result.update(workload="cpu", engine="intelligent-threads")
        stats = summarize([result])["cpu"]["intelligent-threads"]["warm"]
        self.assertEqual(stats["timed_seconds"]["count"], 1)
        self.assertEqual(stats["task_latency_seconds"]["count"], 4)


if __name__ == "__main__":
    unittest.main()
