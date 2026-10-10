"""Budgeted selection checks with synthetic measurements, not CPU workloads."""
from concurrent.futures import CancelledError
import dataclasses
import itertools
import json
import math
import random
import unittest
from unittest.mock import patch

from intelligent_futures.prediction import HardwareCapabilities
from intelligent_futures.tuning import (
    CalibrationObjective, Configuration, MAX_CANDIDATES, Measurement,
    export_configuration, fixture_seeds, inspect_holdout, load_configuration, tune,
)


class BudgetedTuningTests(unittest.TestCase):
    def setUp(self):
        self.configs = [Configuration("thread", n, 1) for n in (1, 2, 3, 4)]

    def run_tuning(self, evaluate, **kwargs):
        options = dict(max_evaluations=12, seed=123, min_repeats=2, max_repeats=4)
        options.update(kwargs)
        return tune(self.configs, evaluate, **options)

    def test_twenty_four_candidates_save_exactly_one_quarter_of_calls(self):
        configs = [Configuration("thread", n, 1) for n in range(1, 25)]
        calls = []
        def evaluate(config, seed):
            calls.append((config, seed))
            return Measurement(25 - config.workers, 1, True)
        report = tune(configs, evaluate, max_evaluations=72, seed=7,
                      min_repeats=2, max_repeats=4)
        self.assertEqual(report.evaluations, 72)
        self.assertEqual(len(calls), 72)
        self.assertEqual(report.unused_evaluations, 0)
        self.assertEqual(report.selected, configs[-1])
        self.assertEqual(report.stopping_reason, "budget_exhausted")
        self.assertEqual([p.evaluations for p in report.trace], [24, 48, 60, 72])
        self.assertEqual([len(p.active_candidates) for p in report.trace], [24, 12, 12, 12])
        for result in report.candidates:
            self.assertEqual(result.evaluations, 4 if result.configuration.workers >= 13 else 2)
            self.assertEqual(result.status, "evaluated" if result.configuration.workers >= 13 else "eliminated")
        self.assertEqual(report.evaluations / (len(configs) * 4), .75)
        self.assertAlmostEqual(report.callback_seconds, sum(r.callback_seconds for r in report.candidates))
        self.assertGreaterEqual(report.elapsed_seconds, report.callback_seconds)
        json.dumps(report.to_dict(), allow_nan=False)

    def test_common_fixture_seeds_complete_rounds_and_input_order_independence(self):
        calls = []
        def evaluate(config, seed):
            calls.append((config, seed))
            return Measurement(config.workers, 1, True)
        first = self.run_tuning(evaluate)
        original_calls = calls[:]
        calls.clear()
        second = tune(reversed(self.configs), evaluate, max_evaluations=12,
                      seed=123, min_repeats=2, max_repeats=4)
        self.assertEqual(calls, original_calls)
        self.assertEqual(first.selected, second.selected)
        self.assertEqual(first.trace, second.trace)
        seeds = fixture_seeds(seed=123, repeats=4)
        for result in first.candidates:
            observations = [r for r in first.records if r.configuration == result.configuration]
            self.assertEqual([r.seed for r in observations], list(seeds[:result.evaluations]))
            self.assertEqual([r.repeat for r in observations], list(range(result.evaluations)))
        for repeat in range(4):
            records = [r for r in first.records if r.repeat == repeat]
            self.assertEqual(len({r.seed for r in records}), 1)

    def test_fixture_seed_helper_matches_legacy_sequence_and_is_bounded(self):
        rng = random.Random(45)
        self.assertEqual(fixture_seeds(seed=45, repeats=4), tuple(rng.randrange(2**63) for _ in range(4)))
        self.assertEqual(fixture_seeds(seed=45, repeats=2), fixture_seeds(seed=45, repeats=4)[:2])
        for kwargs in ({"seed": True, "repeats": 2}, {"seed": -1, "repeats": 2},
                       {"seed": 1, "repeats": 0}, {"seed": 1, "repeats": 33}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                fixture_seeds(**kwargs)

    def test_median_is_robust_to_one_large_outlier(self):
        counts = {}
        def evaluate(config, seed):
            counts[config] = counts.get(config, 0) + 1
            value = 100 if config.workers == 1 and counts[config] == 3 else config.workers
            return Measurement(value, 1, True)
        report = self.run_tuning(evaluate, min_repeats=3, max_repeats=3)
        self.assertEqual(report.selected.workers, 1)
        winner = report.candidates[0]
        self.assertEqual((winner.score_seconds, winner.mad_seconds), (1, 0))
        self.assertEqual((winner.minimum_seconds, winner.maximum_seconds), (1, 100))
        self.assertTrue(report.uncertainty.observed_ranges_overlap)
        self.assertEqual(report.uncertainty.paired_repeats, 3)
        self.assertEqual(report.uncertainty.selected_faster_repeats, 2)

    def test_eliminated_lower_partial_median_is_not_reactivated_or_selected(self):
        configs = self.configs[:2]
        counts = {}
        def evaluate(config, seed):
            counts[config] = counts.get(config, 0) + 1
            value = 100 if config.workers == 1 and counts[config] > 2 else config.workers
            return Measurement(value, 1, True)
        report = tune(configs, evaluate, max_evaluations=6, seed=2,
                      min_repeats=2, max_repeats=4)
        self.assertEqual(report.selected, configs[0])
        self.assertEqual(report.candidates[0].score_seconds, 50.5)
        self.assertEqual(report.candidates[1].score_seconds, 2)
        self.assertEqual(report.candidates[1].status, "eliminated")
        self.assertEqual(report.uncertainty.lower_scoring_eliminated, (configs[1],))
        self.assertEqual(counts, {configs[0]: 4, configs[1]: 2})
        self.assertIsNone(report.uncertainty.runner_up)

    def test_late_invalid_survivor_cannot_reactivate_eliminated_candidate(self):
        counts = {}
        def evaluate(config, seed):
            counts[config] = counts.get(config, 0) + 1
            return Measurement(config.workers, 1, counts[config] < 3)
        report = tune(self.configs[:2], evaluate, max_evaluations=6, seed=2,
                      min_repeats=2, max_repeats=4)
        self.assertIsNone(report.selected)
        self.assertEqual(report.stopping_reason, "no_eligible_finalists")
        self.assertEqual([r.status for r in report.candidates], ["excluded", "eliminated"])
        self.assertEqual(report.evaluations, 5)
        self.assertEqual(report.stable_selection_rounds, 0)

    def test_partial_round_budget_is_left_unused_for_equal_final_counts(self):
        report = self.run_tuning(lambda c, s: Measurement(c.workers, 1, True), max_evaluations=11)
        self.assertEqual(report.evaluations, 10)
        self.assertEqual(report.unused_evaluations, 1)
        self.assertEqual(report.stopping_reason, "incomplete_round_budget")
        self.assertEqual({r.evaluations for r in report.candidates if r.status == "evaluated"}, {3})
        self.assertEqual(report.trace[-1].evaluations, report.evaluations)

    def test_no_elimination_without_budget_for_another_cohort(self):
        report = self.run_tuning(lambda c, s: Measurement(c.workers, 1, True), max_evaluations=9)
        self.assertEqual(report.evaluations, 8)
        self.assertEqual([r.status for r in report.candidates], ["evaluated"] * 4)
        self.assertEqual(report.uncertainty.paired_repeats, 2)

    def test_budget_can_remain_after_repeat_limit(self):
        report = self.run_tuning(lambda c, s: Measurement(c.workers, 1, True), max_evaluations=30)
        self.assertEqual(report.evaluations, 12)
        self.assertEqual(report.unused_evaluations, 18)
        self.assertEqual(report.stopping_reason, "repeat_limit")

    def test_ties_break_by_configuration_without_confidence_claim(self):
        report = self.run_tuning(lambda c, s: Measurement(1, 1, True))
        self.assertEqual(report.selected, min(self.configs))
        self.assertEqual(report.uncertainty.relative_margin, 0)
        self.assertEqual(report.uncertainty.tied_repeats, 4)
        self.assertEqual(report.uncertainty.selected_faster_repeats, 0)
        self.assertTrue(any("tie" in note for note in report.uncertainty.notes))
        self.assertTrue(any("no statistical confidence" in note for note in report.uncertainty.notes))

    def test_convergence_trace_is_observed_not_monotone_optimism(self):
        counts = {}
        def evaluate(config, seed):
            counts[config] = counts.get(config, 0) + 1
            value = 10 if config.workers == 1 and counts[config] > 1 else config.workers
            return Measurement(value, 1, True)
        report = self.run_tuning(evaluate, min_repeats=3, max_repeats=3)
        self.assertEqual([p.best_configuration.workers for p in report.trace], [1, 2, 2])
        self.assertEqual(report.selection_changes, 1)
        self.assertEqual(report.stable_selection_rounds, 2)
        self.assertEqual([p.best_score_seconds for p in report.trace], [1, 2, 2])

    def test_warm_and_combined_objectives_are_used(self):
        evaluate = lambda c, s: Measurement(8 / c.workers, c.workers, True)
        warm = self.run_tuning(evaluate, objective=CalibrationObjective(elapsed="warm"))
        combined = self.run_tuning(evaluate, objective=CalibrationObjective(elapsed="combined"))
        self.assertEqual(warm.selected.workers, 1)
        self.assertEqual(combined.selected.workers, 3)

    def test_exclusion_gates_fail_closed_and_stop_known_invalid_callbacks(self):
        cases = [
            (Measurement(1, 1, False), CalibrationObjective(), "incorrect"),
            (Measurement(1, 1, True, error_count=1), CalibrationObjective(), "task_errors"),
            (Measurement(1, 1, True), CalibrationObjective(max_cpu_seconds=2), "cpu_unavailable"),
            (Measurement(1, 1, True, cpu_seconds=3), CalibrationObjective(max_cpu_seconds=2), "cpu_cap"),
            (Measurement(1, 1, True), CalibrationObjective(max_peak_rss_bytes=3), "rss_unavailable"),
            (Measurement(1, 1, True, peak_rss_bytes=4), CalibrationObjective(max_peak_rss_bytes=3), "rss_cap"),
            (Measurement(1, 1, True), CalibrationObjective(max_batch_p95_seconds=2), "batch_p95_unavailable"),
            (Measurement(1, 1, True, batch_p95_seconds=3), CalibrationObjective(max_batch_p95_seconds=2), "batch_p95_cap"),
        ]
        for measurement, objective, reason in cases:
            with self.subTest(reason=reason):
                report = self.run_tuning(lambda c, s: measurement, objective=objective)
                self.assertIsNone(report.selected)
                self.assertEqual(report.evaluations, 4)
                self.assertEqual({r.exclusion for r in report.records}, {reason})
                self.assertEqual({r.status for r in report.candidates}, {"excluded"})
                self.assertTrue(all(r.score_seconds is None for r in report.candidates))
                self.assertEqual(report.stopping_reason, "no_eligible_finalists")

    def test_caps_check_later_observations_and_accept_inclusive_boundary(self):
        counts = {}
        def evaluate(config, seed):
            counts[config] = counts.get(config, 0) + 1
            p95 = 3 if config.workers == 1 and counts[config] == 2 else 2
            return Measurement(config.workers, 1, True, cpu_seconds=2,
                               peak_rss_bytes=100, batch_p95_seconds=p95)
        objective = CalibrationObjective(max_cpu_seconds=2, max_peak_rss_bytes=100,
                                         max_batch_p95_seconds=2)
        report = self.run_tuning(evaluate, objective=objective)
        self.assertEqual(report.selected.workers, 2)
        self.assertEqual(report.candidates[0].exclusion_reasons, ("batch_p95_cap",))
        self.assertEqual(counts[self.configs[0]], 2)

    def test_invalid_callback_results_and_exceptions_are_private_exclusions(self):
        forged = Measurement(1, 1, True)
        object.__setattr__(forged, "batch_p95_seconds", math.inf)
        for value in (None, {}, math.nan, forged):
            report = self.run_tuning(lambda c, s: value)
            self.assertEqual(report.evaluations, len(self.configs))
            self.assertIsNone(report.selected)
            self.assertTrue(all(r.exclusion == "callback_error_or_invalid_measurement" for r in report.records))
        def fail(config, seed):
            raise RuntimeError("private callback payload")
        report = self.run_tuning(fail)
        self.assertNotIn("private callback payload", json.dumps(report.to_dict()))

    def test_cancellation_interrupt_and_system_exit_propagate_immediately(self):
        for exception in (CancelledError, KeyboardInterrupt, SystemExit):
            calls = []
            def fail(config, seed):
                calls.append(config)
                raise exception()
            with self.subTest(exception=exception), self.assertRaises(exception):
                self.run_tuning(fail)
            self.assertEqual(len(calls), 1)

    def test_process_candidates_need_consent_before_any_callback(self):
        config = Configuration("process", 2, 4)
        calls = []
        def evaluate(config, seed):
            calls.append(config)
            return Measurement(1, 1, True)
        for consent in (False, None, 1, "yes"):
            with self.subTest(consent=consent), self.assertRaises(ValueError):
                tune([config], evaluate, max_evaluations=2, seed=0,
                     min_repeats=2, max_repeats=2, allow_process=consent)
        self.assertEqual(calls, [])
        report = tune([config], evaluate, max_evaluations=2, seed=0,
                      min_repeats=2, max_repeats=2, allow_process=True)
        self.assertEqual(report.selected, config)
        with self.assertRaises(ValueError):
            inspect_holdout(report, evaluate, repeats=1, seed=9)
        self.assertEqual(len(calls), 2)
        inspect_holdout(report, evaluate, repeats=1, seed=9, allow_process=True)
        self.assertEqual(len(calls), 3)

    def test_numeric_and_candidate_validation_precedes_callbacks(self):
        def forbidden(config, seed):
            self.fail("callback must not run for invalid input")
        options = [
            {"max_evaluations": value} for value in (True, 0, -1, 7, 1.5, 4097)
        ] + [
            {"min_repeats": value} for value in (True, 0, 5, 33)
        ] + [
            {"max_repeats": value} for value in (True, 0, 1, 33)
        ] + [
            {"reduction_factor": value} for value in (True, 1, 129, 2.0)
        ] + [
            {"seed": value} for value in (True, -1, 2**63)
        ] + [{"objective": {}}, {"allow_process": None}]
        for update in options:
            with self.subTest(update=update), self.assertRaises(ValueError):
                self.run_tuning(forbidden, **update)
        for configs in ([], [None], [self.configs[0]] * 2):
            with self.subTest(configs=configs), self.assertRaises(ValueError):
                tune(configs, forbidden, max_evaluations=4, seed=0, min_repeats=1)
        with self.assertRaises(ValueError):
            self.run_tuning(None)

    def test_unbounded_candidate_input_is_stopped_at_the_limit(self):
        seen = []
        def infinite_candidates():
            for n in itertools.count(1):
                seen.append(n)
                yield Configuration("thread", n, 1)
        with self.assertRaises(ValueError):
            tune(infinite_candidates(), lambda c, s: self.fail("must not run"),
                 max_evaluations=1000, seed=0)
        self.assertEqual(len(seen), MAX_CANDIDATES + 1)

    def test_new_latency_fields_validate_and_zero_caps_are_supported(self):
        for value in (True, -1, math.inf, math.nan, "1", 10**1000):
            with self.subTest(value=str(value)[:30]), self.assertRaises(ValueError):
                Measurement(1, 1, True, batch_p95_seconds=value)
            with self.subTest(cap=str(value)[:30]), self.assertRaises(ValueError):
                CalibrationObjective(max_batch_p95_seconds=value)
        report = self.run_tuning(lambda c, s: Measurement(1, 1, True, batch_p95_seconds=0),
                                 objective=CalibrationObjective(max_batch_p95_seconds=0))
        self.assertIsNotNone(report.selected)

    def test_extreme_finite_medians_and_gaps_remain_json_safe(self):
        huge = float.fromhex("0x1.fffffffffffffp+1023")
        tiny = float.fromhex("0x0.0000000000001p-1022")
        for value in (huge, tiny):
            report = self.run_tuning(lambda c, s: Measurement(value, value, True))
            self.assertEqual(report.candidates[0].score_seconds, value)
            self.assertEqual(report.candidates[0].mad_seconds, 0)
            json.dumps(report.to_dict(), allow_nan=False)
        report = self.run_tuning(lambda c, s: Measurement(tiny if c.workers == 1 else huge, 1, True))
        self.assertIsNone(report.uncertainty.relative_margin)
        json.dumps(report.to_dict(), allow_nan=False)
        report = self.run_tuning(lambda c, s: Measurement(huge, huge, True),
                                 objective=CalibrationObjective(elapsed="combined"))
        self.assertIsNone(report.selected)
        self.assertEqual({r.exclusion for r in report.records}, {"elapsed_overflow"})
        json.dumps(report.to_dict(), allow_nan=False)

    def test_holdout_and_configuration_roundtrip_do_not_reselect_or_replay(self):
        hardware = HardwareCapabilities(4, 4, None, "test", "test", "test")
        calls = []
        def evaluate(config, seed):
            calls.append((config, seed))
            return Measurement(config.workers, 1, True)
        with patch.object(HardwareCapabilities, "detect", return_value=hardware):
            report = self.run_tuning(evaluate)
        count = len(calls)
        original = report.to_dict()
        payload = export_configuration(report, workload_key="test-v1", measured_scope="synthetic")
        saved = load_configuration(payload, workload_key="test-v1", hardware=hardware)
        self.assertEqual(saved.configuration, report.selected)
        self.assertEqual(saved.provenance, report.provenance)
        saved.executor_kwargs()
        report.to_dict()
        self.assertEqual(len(calls), count)
        heldout_calls = []
        def holdout(config, seed):
            heldout_calls.append(config)
            return Measurement(100, 100, False)
        observations = inspect_holdout(report, holdout, repeats=2, seed=999)
        self.assertEqual(heldout_calls, [report.selected] * 2)
        self.assertEqual([r.seed for r in observations], list(fixture_seeds(seed=999, repeats=2)))
        self.assertTrue(all(r.exclusion == "incorrect" for r in observations))
        self.assertEqual(report.to_dict(), original)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            report.selected = None

    def test_export_and_holdout_reject_eliminated_or_corrupted_selections(self):
        report = self.run_tuning(lambda c, s: Measurement(c.workers, 1, True))
        eliminated = next(r.configuration for r in report.candidates if r.status == "eliminated")
        for invalid in (dataclasses.replace(report, selected=eliminated),
                        dataclasses.replace(report, selected=None),
                        dataclasses.replace(report, candidates=())):
            with self.subTest(invalid=invalid.selected), self.assertRaises(ValueError):
                export_configuration(invalid, workload_key="test")
            with self.subTest(holdout=invalid.selected), self.assertRaises(ValueError):
                inspect_holdout(invalid, lambda c, s: self.fail("must not run"), repeats=1, seed=0)
        with self.assertRaises(ValueError):
            export_configuration(dataclasses.replace(report, records=()), workload_key="test")


if __name__ == "__main__":
    unittest.main()
