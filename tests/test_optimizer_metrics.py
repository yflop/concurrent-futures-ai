"""Synthetic metric audits only; no workload execution or media tools."""
import copy
from dataclasses import asdict
import json
import math
import unittest

from intelligent_futures.tuning import Configuration
from benchmarks.real_world.optimizer_metrics import (
    GateBudgets, audit_grid, candidate_key, gate_failures, phase_seconds,
    search_cost_summary, summarize_phases, summarize_selection, validate_split,
)


A = Configuration("thread", 1, 1)
B = Configuration("thread", 2, 1)
C = Configuration("process", 2, 4)
SEEDS = (101, 202, 303)
BUDGETS = GateBudgets(max_p95_seconds=10, max_rss_bytes=1000, max_cpu_seconds=50)


def row(config, seed, *, cold=2., warm=1.):
    def phase(seconds):
        return {"correct": True, "seconds": seconds, "tasks": 4,
                "throughput_per_second": 4 / seconds, "error_rate": 0,
                "errors": [], "batch_completion_latency_p95_seconds": 0.5}
    return {"engine": "intelligent", "configuration": asdict(config), "seed": seed,
            "cold": phase(cold), "warm": phase(warm),
            "cold_lifecycle_seconds": cold + 0.5,
            "sampled_tree_peak_rss_bytes": 100, "lifecycle_cpu_seconds": 2.}


def grid(configs=(A, B), seeds=SEEDS):
    return [row(config, seed, cold=4/config.workers, warm=2/config.workers)
            for config in configs for seed in seeds]


def summary(selected, rows, **kwargs):
    options = dict(candidates=(A, B), heldout_seeds=SEEDS, budgets=BUDGETS)
    options.update(kwargs)
    return summarize_selection(selected, rows, **options)


class SplitTests(unittest.TestCase):
    def test_valid_seed_split_and_declared_limits(self):
        report = validate_split((1, 2), SEEDS, selection_fixture_seeds=[1, 1, 2])
        self.assertTrue(report["seeds_disjoint"])
        self.assertTrue(report["selection_inputs_checked"])
        self.assertFalse(report["fixture_content_disjoint_checked"])
        self.assertIn("do not establish", report["independence_limit"])

    def test_training_and_heldout_seeds_cannot_overlap(self):
        with self.assertRaisesRegex(ValueError, "overlap"):
            validate_split((1, 2), (2, 3))

    def test_duplicate_empty_bool_and_invalid_seeds_rejected(self):
        for bad in ([], [1, 1], [True], [-1], [2**63], ["1"]):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                validate_split(bad, SEEDS)
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                validate_split((1, 2), bad)

    def test_content_overlap_detected_despite_distinct_seeds(self):
        with self.assertRaisesRegex(ValueError, "content overlaps"):
            validate_split((1, 2), (3, 4), training_fixture_ids=["data-a", "data-b"],
                           heldout_fixture_ids=["data-c", "data-a"])
        report = validate_split((1, 2), (3, 4), training_fixture_ids=["a", "b"],
                                heldout_fixture_ids=["c", "d"])
        self.assertTrue(report["fixture_content_disjoint_checked"])

    def test_content_ids_are_paired_complete_and_strings(self):
        for kwargs in ({"training_fixture_ids": ["a", "b"]},
                       {"training_fixture_ids": ["a"], "heldout_fixture_ids": ["c", "d"]},
                       {"training_fixture_ids": ["a", ""], "heldout_fixture_ids": ["c", "d"]}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                validate_split((1, 2), (3, 4), **kwargs)

    def test_selector_cannot_observe_holdout_or_undeclared_seed(self):
        for seen in ([1, 101], [1, 7], [True]):
            with self.subTest(seen=seen), self.assertRaisesRegex(ValueError, "outside"):
                validate_split((1, 2), SEEDS, selection_fixture_seeds=seen)


class GateTests(unittest.TestCase):
    def test_correct_metrics_and_exact_caps_pass(self):
        measured = row(A, SEEDS[0])
        measured["sampled_tree_peak_rss_bytes"] = BUDGETS.max_rss_bytes
        measured["lifecycle_cpu_seconds"] = BUDGETS.max_cpu_seconds
        measured["cold"]["batch_completion_latency_p95_seconds"] = BUDGETS.max_p95_seconds
        self.assertEqual(gate_failures(measured, BUDGETS), [])

    def test_both_phases_must_pass_correctness_and_errors(self):
        for phase in ("cold", "warm"):
            for key, value in (("correct", False), ("correct", 1), ("errors", ["bad"]),
                               ("errors", None), ("error_rate", 0.1), ("error_rate", None),
                               ("error_rate", False)):
                measured = row(A, SEEDS[0])
                measured[phase][key] = value
                with self.subTest(phase=phase, key=key, value=value):
                    self.assertTrue(gate_failures(measured, BUDGETS))

    def test_missing_invalid_and_exceeded_resources_fail_closed(self):
        for key, value, reason in (("sampled_tree_peak_rss_bytes", None, "rss_unavailable"),
                                  ("sampled_tree_peak_rss_bytes", 1001, "rss_cap"),
                                  ("sampled_tree_peak_rss_bytes", True, "rss_unavailable"),
                                  ("lifecycle_cpu_seconds", None, "cpu_unavailable"),
                                  ("lifecycle_cpu_seconds", 51, "cpu_cap"),
                                  ("lifecycle_cpu_seconds", math.nan, "cpu_unavailable")):
            measured = row(A, SEEDS[0])
            measured[key] = value
            with self.subTest(key=key, value=value):
                self.assertIn(reason, gate_failures(measured, BUDGETS))

    def test_missing_invalid_and_exceeded_latency_fail_closed(self):
        for phase in ("cold", "warm"):
            for value in (None, math.inf, -1, True, 11):
                measured = row(A, SEEDS[0])
                measured[phase]["batch_completion_latency_p95_seconds"] = value
                with self.subTest(phase=phase, value=value):
                    self.assertTrue(gate_failures(measured, BUDGETS))

    def test_times_counts_and_throughput_are_required(self):
        for key, value in (("seconds", 0), ("seconds", math.nan),
                           ("throughput_per_second", None), ("tasks", 0), ("tasks", True)):
            measured = row(A, SEEDS[0])
            measured["warm"][key] = value
            self.assertTrue(gate_failures(measured, BUDGETS))
        measured = row(A, SEEDS[0])
        measured["warm"]["tasks"] = 3
        self.assertIn("phase_task_counts_differ", gate_failures(measured, BUDGETS))

    def test_no_silent_lifecycle_timing_fallback(self):
        measured = row(A, SEEDS[0])
        del measured["cold_lifecycle_seconds"]
        measured["cold_including_construction_seconds"] = 1.0
        self.assertIsNone(phase_seconds(measured, "cold"))
        self.assertIn("invalid_phase_timing", gate_failures(measured, BUDGETS))

    def test_combined_timing_overflow_is_not_an_eligible_metric(self):
        measured = row(A, SEEDS[0])
        measured["cold_lifecycle_seconds"] = 1.6e308
        measured["warm"]["seconds"] = 1.6e308
        self.assertIsNone(phase_seconds(measured, "combined"))
        self.assertIn("invalid_phase_timing", gate_failures(measured, BUDGETS))

    def test_gate_validation(self):
        for kwargs in ({"max_p95_seconds": None}, {"max_cpu_seconds": -1},
                       {"max_cpu_seconds": 10**1000}, {"max_rss_bytes": True},
                       {"max_rss_bytes": 1.5}, {"max_rss_bytes": 2**63}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                GateBudgets(**kwargs)
        with self.assertRaises(ValueError):
            gate_failures(row(A, SEEDS[0]), {})


class OracleTests(unittest.TestCase):
    def test_complete_grid_aggregate_regret_and_coverage(self):
        report = summary(A, grid())
        self.assertTrue(report["oracle_valid"])
        self.assertTrue(report["quality_gate_eligible"])
        self.assertEqual(report["oracle_configuration"], asdict(B))
        self.assertAlmostEqual(report["aggregate_relative_regret"], 6.5/3.5 - 1)
        self.assertEqual(report["within_10_percent_coverage"], 0.)
        self.assertEqual(report["coverage_denominator"], 3)
        json.dumps(report, allow_nan=False)

    def test_frozen_choice_is_not_changed_by_oracle(self):
        rows = grid()
        original = copy.deepcopy(rows)
        report = summary(A, rows)
        self.assertEqual(report["selected_configuration"], asdict(A))
        self.assertEqual(rows, original)

    def test_all_repeat_eligibility_prevents_survivor_only_oracle(self):
        rows = grid()
        rows[-1]["warm"]["correct"] = False
        report = summary(A, rows)
        self.assertTrue(report["oracle_valid"])
        self.assertEqual(report["grid_audit"]["eligible_candidate_count"], 1)
        self.assertEqual(report["oracle_configuration"], asdict(A))
        self.assertEqual(report["within_10_percent_coverage"], 1.)
        self.assertTrue(all(item["per_seed_oracle_configuration"] == asdict(A)
                            for item in report["comparisons"]))

    def test_fast_but_resource_invalid_candidate_cannot_be_oracle(self):
        rows = grid()
        rows[-1]["sampled_tree_peak_rss_bytes"] = 1001
        report = summary(A, rows)
        self.assertEqual(report["oracle_configuration"], asdict(A))
        self.assertEqual(report["aggregate_relative_regret"], 0.)

    def test_failed_selected_repeat_stays_in_coverage_denominator(self):
        rows = grid()
        rows[-1]["warm"]["correct"] = False
        report = summary(B, rows)
        self.assertFalse(report["selected_eligible"])
        self.assertFalse(report["quality_gate_eligible"])
        self.assertIsNone(report["selected_mean_seconds"])
        self.assertIsNone(report["aggregate_relative_regret"])
        self.assertEqual(report["within_10_percent_coverage"], 2/3)
        self.assertEqual(report["per_seed_regret_observation_count"], 2)
        self.assertFalse(report["aggregate_within_10_percent"])

    def test_missing_candidate_row_invalidates_exhaustive_claim(self):
        report = summary(A, grid()[:-1])
        self.assertFalse(report["grid_audit"]["grid_complete"])
        self.assertFalse(report["oracle_valid"])
        self.assertIsNone(report["oracle_configuration"])
        self.assertEqual(report["within_10_percent_coverage"], 0)
        self.assertEqual(report["coverage_denominator"], 3)

    def test_duplicate_row_cannot_improve_a_mean(self):
        rows = grid()
        rows.append(row(B, SEEDS[0], cold=.01, warm=.01))
        report = summary(A, rows)
        self.assertFalse(report["oracle_valid"])
        self.assertTrue(any("duplicate" in reason for reason in report["grid_audit"]["structural_failures"]))

    def test_undeclared_candidate_or_seed_does_not_expand_grid(self):
        for extra in (row(C, SEEDS[0]), row(A, 909)):
            report = summary(A, grid() + [extra])
            self.assertFalse(report["oracle_valid"])
            self.assertEqual(report["grid_audit"]["declared_candidate_count"], 2)

    def test_explicit_failed_trial_is_complete_but_ineligible(self):
        rows = grid()
        rows[-1] = {"engine": "intelligent", "configuration": asdict(B), "seed": SEEDS[-1],
                    "error": "worker exited"}
        report = summary(A, rows)
        self.assertTrue(report["grid_audit"]["grid_complete"])
        self.assertTrue(report["oracle_valid"])
        self.assertEqual(report["oracle_configuration"], asdict(A))

    def test_no_admissible_candidates_has_no_invented_fallback(self):
        rows = grid()
        for measured in rows:
            measured["cold"]["correct"] = False
        report = summary(B, rows)
        self.assertTrue(report["grid_audit"]["grid_complete"])
        self.assertFalse(report["oracle_valid"])
        self.assertIsNone(report["oracle_configuration"])
        self.assertIsNone(report["aggregate_relative_regret"])
        self.assertEqual(report["within_10_percent_coverage"], 0.)

    def test_absent_selection_retains_the_entire_denominator(self):
        report = summary(None, grid())
        self.assertTrue(report["oracle_valid"])
        self.assertFalse(report["quality_gate_eligible"])
        self.assertIsNone(report["selected_configuration"])
        self.assertEqual(report["coverage_denominator"], 3)
        self.assertEqual(report["within_10_percent_coverage"], 0.)

    def test_per_seed_hindsight_minimum_is_distinct_from_mean_oracle(self):
        rows = [row(A, 101, cold=.5, warm=1), row(A, 202, cold=8.5, warm=1),
                row(B, 101, cold=5.5, warm=1), row(B, 202, cold=5.5, warm=1)]
        report = summary(A, rows, heldout_seeds=(101, 202), phase="cold")
        self.assertEqual(report["oracle_configuration"], asdict(A))
        self.assertEqual(report["aggregate_relative_regret"], 0.)
        self.assertEqual(report["within_10_percent_coverage"], .5)
        self.assertEqual(report["comparisons"][1]["per_seed_oracle_configuration"], asdict(B))
        self.assertAlmostEqual(report["comparisons"][1]["relative_regret"], .5)

    def test_deterministic_ties_ignore_row_and_grid_order(self):
        rows = [row(config, seed) for config in (A, B) for seed in SEEDS]
        first = summary(A, rows)
        second = summary(A, reversed(rows), candidates=(B, A))
        self.assertEqual(first, second)
        self.assertEqual(first["oracle_configuration"], asdict(min(A, B)))

    def test_phases_keep_cold_lifecycle_and_reused_pool_separate(self):
        rows = [row(config, seed, cold=(1 if config == A else 2),
                    warm=(4 if config == A else 1)) for config in (A, B) for seed in SEEDS]
        reports = summarize_phases(A, iter(rows), candidates=iter((A, B)),
                                   heldout_seeds=iter(SEEDS), budgets=BUDGETS)
        self.assertEqual(reports["cold"]["oracle_configuration"], asdict(A))
        self.assertEqual(reports["warm"]["oracle_configuration"], asdict(B))
        self.assertEqual(reports["combined"]["oracle_configuration"], asdict(B))
        self.assertEqual(reports["cold"]["selected_mean_seconds"], 1.5)

    def test_mixed_engine_grid_rejected_even_with_unique_keys(self):
        rows = grid()
        rows[-1]["engine"] = "stdlib"
        self.assertFalse(summary(A, rows)["oracle_valid"])

    def test_mismatched_candidate_task_counts_invalidate_comparison(self):
        rows = grid()
        rows[-1]["cold"]["tasks"] = rows[-1]["warm"]["tasks"] = 2
        report = summary(A, rows)
        self.assertFalse(report["oracle_valid"])
        self.assertTrue(any("different task counts" in reason
                            for reason in report["grid_audit"]["structural_failures"]))

    def test_invalid_engine_metadata_fails_closed(self):
        rows = grid()
        rows[0]["engine"] = {}
        self.assertFalse(summary(A, rows)["oracle_valid"])

    def test_invalid_universe_and_selection_are_errors(self):
        for configs in ((), (A, A), ({"backend": "magic"},)):
            with self.subTest(configs=configs), self.assertRaises(ValueError):
                summary(A, grid(), candidates=configs)
        with self.assertRaises(ValueError):
            summary(C, grid())
        with self.assertRaises(ValueError):
            summary(A, grid(), phase="startup")
        with self.assertRaises(ValueError):
            candidate_key({**asdict(A), "silently_ignored": 1})

    def test_overflow_regret_is_null_but_never_within_threshold(self):
        rows = [row(A, SEEDS[0], cold=1e307, warm=1e307),
                row(B, SEEDS[0], cold=1e-307, warm=1e-307)]
        rows[1]["cold_lifecycle_seconds"] = 1e-307
        report = summary(A, rows, heldout_seeds=SEEDS[:1])
        self.assertTrue(report["quality_gate_eligible"])
        self.assertIsNone(report["aggregate_relative_regret"])
        self.assertFalse(report["aggregate_within_10_percent"])
        json.dumps(report, allow_nan=False)

    def test_boundary_is_inclusive(self):
        rows = [row(A, SEEDS[0]), row(B, SEEDS[0])]
        rows[0]["cold_lifecycle_seconds"] = 1.1
        rows[1]["cold_lifecycle_seconds"] = 1.0
        report = summary(A, rows, heldout_seeds=SEEDS[:1], phase="cold")
        self.assertTrue(report["aggregate_within_10_percent"])
        self.assertEqual(report["within_10_percent_coverage"], 1.)

    def test_invalid_rows_fail_closed_without_claiming_completeness(self):
        for bad in ({}, None, {"configuration": asdict(A), "seed": True}):
            report = summary(A, grid() + [bad])
            self.assertFalse(report["oracle_valid"])
            self.assertTrue(report["grid_audit"]["structural_failures"])

    def test_audit_can_be_used_without_selection(self):
        report = audit_grid(grid(), candidates=(A, B), heldout_seeds=SEEDS, budgets=BUDGETS)
        self.assertTrue(report["grid_complete"])
        self.assertEqual(report["observed_row_count"], 6)
        self.assertEqual(report["expected_row_count"], 6)
        self.assertEqual(report["eligible_candidate_count"], 2)


class SearchCostTests(unittest.TestCase):
    def records(self):
        return [{"configuration": asdict(config), "fixture_seed": seed,
                 "callback_seconds": cost} for config, seed, cost in
                ((A, 1, 2.), (A, 2, 3.), (B, 1, 8.), (B, 2, 9.))]

    def test_replay_cost_uses_requested_reference_pairs_not_replay_timers(self):
        references = self.records()
        queried = copy.deepcopy(references[:2])
        for item in queried:
            item["callback_seconds"] = 0.00001
        report = search_cost_summary(queried, references, replay=True, actual_wall_seconds=.01)
        self.assertEqual(report["callback_count"], 2)
        self.assertEqual(report["sample_fraction"], .5)
        self.assertEqual(report["recorded_requested_callback_seconds"], 5.)
        self.assertEqual(report["recorded_exhaustive_callback_seconds"], 22.)
        self.assertEqual(report["actual_search_wall_seconds"], .01)
        self.assertIn("Virtual", report["cost_scope"])
        self.assertNotEqual(report["sample_fraction"], report["recorded_execution_cost_fraction"])

    def test_reused_configuration_zero_callback_cost(self):
        report = search_cost_summary([], self.records(), replay=False, actual_wall_seconds=.0001)
        self.assertEqual(report["callback_count"], 0)
        self.assertEqual(report["recorded_requested_callback_seconds"], 0)
        self.assertEqual(report["sample_reduction_fraction"], 1)

    def test_queries_outside_training_matrix_or_duplicates_fail(self):
        references = self.records()
        for requested in ([references[0], references[0]],
                          [{"configuration": asdict(A), "fixture_seed": 999}]):
            with self.assertRaises(ValueError):
                search_cost_summary(requested, references, replay=True)
        with self.assertRaises(ValueError):
            search_cost_summary([], references + [references[0]], replay=True)

    def test_partial_reference_matrix_is_not_an_exhaustive_cost_denominator(self):
        with self.assertRaisesRegex(ValueError, "not complete"):
            search_cost_summary([], self.records()[:-1], replay=True)
        # Dropping an entire candidate is detectable only with a declared grid.
        report = search_cost_summary([], self.records()[:2], replay=True)
        self.assertFalse(report["declared_reference_universe_checked"])
        with self.assertRaisesRegex(ValueError, "not complete"):
            search_cost_summary([], self.records()[:2], replay=True,
                                candidates=(A, B), training_seeds=(1, 2))
        report = search_cost_summary([], self.records(), replay=True,
                                     candidates=(A, B), training_seeds=(1, 2))
        self.assertTrue(report["declared_reference_universe_checked"])

    def test_reference_universe_arguments_are_paired_and_validated(self):
        for kwargs in ({"candidates": (A, B)}, {"training_seeds": (1, 2)},
                       {"candidates": (A, A), "training_seeds": (1, 2)},
                       {"candidates": (A, B), "training_seeds": (1, 1)}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                search_cost_summary([], self.records(), replay=True, **kwargs)

    def test_cost_metrics_require_finite_observations(self):
        for value in (None, math.nan, math.inf, -1, True, 10**1000):
            references = self.records()
            references[0]["callback_seconds"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                search_cost_summary([], references, replay=True)
        with self.assertRaises(ValueError):
            search_cost_summary([], [], replay=True)
        with self.assertRaises(ValueError):
            search_cost_summary([], self.records(), replay="yes")
        with self.assertRaises(ValueError):
            search_cost_summary([], self.records(), replay=True, actual_wall_seconds=math.inf)


if __name__ == "__main__":
    unittest.main()
