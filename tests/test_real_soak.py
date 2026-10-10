"""Pure tiny operations only; never prepare representative fixtures in unit tests."""
import copy
import unittest
from unittest.mock import patch

from benchmarks.real_world import soak


class Clock:
    def __init__(self, step=.1):
        self.value = 0.
        self.step = step

    def __call__(self):
        self.value += self.step
        return self.value


def identity(value):
    return value


class SoakTests(unittest.TestCase):
    def test_duration_and_final_accounting(self):
        row = soak.observe(identity, [1, 2], [1, 2], seconds=2,
                           min_checks=2, clock=Clock())
        self.assertEqual(row["failures"], [])
        self.assertTrue(row["duration_completed"])
        self.assertEqual(row["completed"], row["submitted"])
        self.assertEqual(row["callback_count"], row["submitted"])
        self.assertTrue(soak.accounting_ok(row["snapshot_after_shutdown"]))
        self.assertLessEqual(row["snapshot_after_shutdown"]["counts"]["peak_pending"], 4)
        self.assertEqual(sum(w["tasks"] for w in row["time_windows"]), row["completed"])

    def test_task_cap_does_not_claim_sustained_success(self):
        row = soak.observe(identity, [1], [1], seconds=60,
                           task_budget=8, min_checks=2, clock=Clock())
        self.assertEqual(row["stop_reason"], "task_budget_exhausted")
        self.assertFalse(row["duration_completed"])
        self.assertEqual(row["submitted"], 8)
        self.assertTrue(row["failures"])

    def test_wrong_reference_fails(self):
        row = soak.observe(identity, [1], [2], seconds=2,
                           min_checks=1, clock=Clock())
        self.assertGreater(row["reference_mismatches"], 0)
        self.assertTrue(row["failures"])

    def test_exception_and_callback_error_are_reported(self):
        def bad(_):
            raise RuntimeError("task failure")
        def bad_callback(_):
            raise RuntimeError("callback failure")
        row = soak.observe(bad, [1], [1], seconds=2, min_checks=1,
                           clock=Clock(), completion_callback=bad_callback)
        self.assertEqual(row["errors"], row["completed"])
        self.assertGreater(len(row["callback_errors"]), 0)
        self.assertEqual(row["snapshot_after_shutdown"]["counts"]["failed"], row["errors"])
        self.assertTrue(row["failures"])

    def test_min_checks_and_mutation_are_not_ignored(self):
        def mutate(task):
            task["changed"] = True
            return 1
        row = soak.observe(mutate, [{}], [1], seconds=1,
                           min_checks=100, clock=Clock())
        self.assertIn("minimum completed checks not reached", row["failures"])
        self.assertIn("task inputs were mutated", row["failures"])

    def test_separate_fault_probe(self):
        self.assertTrue(soak.exception_probe()["passed"])

    def test_sampler_endpoints_and_read_failure(self):
        sampler = soak.MemorySampler(read=iter([100, 150, 120]).__next__)
        for _ in range(3):
            sampler.sample()
        self.assertEqual(sampler.report()["end_minus_start_bytes"], 20)
        self.assertEqual(sampler.peak, 150)
        sampler.sample()
        self.assertTrue(sampler.errors)

    def test_budget_acceptance_missing_metrics_and_cleanup(self):
        good = {"failures": [], "memory": {"errors": [], "peak_bytes": 100,
                "end_minus_start_bytes": 2}, "lifecycle_cpu_seconds": 1.,
                "new_live_threads": [], "new_live_child_processes": []}
        budgets = dict(max_rss_bytes=200, max_growth_bytes=20, max_cpu_seconds=2)
        self.assertEqual(soak.acceptance_failures(good, **budgets), [])
        for field, value in [("peak_bytes", 201), ("peak_bytes", None),
                             ("end_minus_start_bytes", 21)]:
            row = copy.deepcopy(good)
            row["memory"][field] = value
            self.assertTrue(soak.acceptance_failures(row, **budgets))
        for field, value in [("lifecycle_cpu_seconds", None),
                             ("lifecycle_cpu_seconds", 3), ("new_live_threads", ["worker"]),
                             ("new_live_child_processes", [(123, 1)])]:
            row = copy.deepcopy(good)
            row[field] = value
            self.assertTrue(soak.acceptance_failures(row, **budgets))

    def test_validation_precedes_work(self):
        for kwargs in [dict(seconds=float("nan")), dict(seconds=301),
                       dict(task_budget=50001), dict(min_checks=0)]:
            with self.assertRaises(ValueError):
                soak.observe(identity, [1], [1], **kwargs)

    def test_cli_fails_closed_and_writes_report(self):
        import json
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)/"report.json"
            with patch.object(soak, "workload_environment", return_value={}), \
                 patch.object(soak, "soak", side_effect=RuntimeError("fixture unavailable")):
                code = soak.main(["--workloads", "http", "--output", str(output)])
            self.assertEqual(code, 1)
            self.assertIn("fixture unavailable", json.loads(output.read_text())["failures"][0]["error"])


if __name__ == "__main__":
    unittest.main()
