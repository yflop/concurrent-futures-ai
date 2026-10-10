import dataclasses
import json
import math
from concurrent.futures import CancelledError
import unittest

from intelligent_futures.tuning import (
    CalibrationObjective, Configuration, Measurement, calibrate, inspect_holdout,
)


class TuningTests(unittest.TestCase):
    def setUp(self):
        self.configs = [Configuration('thread', n, 1) for n in (1, 2, 4)]

    def run_calibration(self, evaluate, **kwargs):
        return calibrate(self.configs, evaluate, repeats=2, seed=123, **kwargs)

    def test_selection_repeat_cost_and_no_implicit_work(self):
        calls = []
        def evaluate(config, seed):
            calls.append((config, seed))
            return Measurement(5 / config.workers, config.workers, True)
        report = self.run_calibration(evaluate)
        self.assertEqual(len(calls), 6)
        self.assertEqual(report.selected.workers, 4)
        self.assertEqual(len(report.records), 6)
        self.assertEqual(len(report.trace), 3)
        self.assertEqual(report.trace[-1].relative_gap_to_best_observed, 0)
        self.assertIsNotNone(report.evaluations_to_stable_near_best)
        self.assertEqual(sum(c.callback_seconds for c in report.candidates),
                         sum(r.callback_seconds for r in report.records))
        self.assertGreaterEqual(report.elapsed_seconds, sum(r.callback_seconds for r in report.records))
        json.dumps(report.to_dict(), allow_nan=False)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            report.selected = self.configs[0]
        # Exporting and constructing executor options cannot invoke a callback.
        report.to_dict()
        report.selected.executor_kwargs()
        self.assertEqual(len(calls), 6)

    def test_warm_objective(self):
        report = self.run_calibration(lambda c, s: Measurement(5 / c.workers, c.workers, True),
                                      objective=CalibrationObjective(elapsed='warm'))
        self.assertEqual(report.selected.workers, 1)

    def test_combined_objective_and_overflow_exclusion(self):
        report = self.run_calibration(lambda c, s: Measurement(5 / c.workers, c.workers, True),
                                      objective=CalibrationObjective(elapsed='combined'))
        self.assertEqual(report.selected.workers, 2)
        huge = float.fromhex('0x1.fffffffffffffp+1023')
        report = self.run_calibration(lambda c, s: Measurement(huge, huge, True),
                                      objective=CalibrationObjective(elapsed='combined'))
        self.assertIsNone(report.selected)
        self.assertTrue(all(r.exclusion == 'elapsed_overflow' for r in report.records))
        json.dumps(report.to_dict(), allow_nan=False)

    def test_deterministic_order_fixture_seeds_and_ties(self):
        calls = []
        def evaluate(c, s):
            calls.append((c, s))
            return Measurement(1, 1, True)
        first = self.run_calibration(evaluate)
        first_calls = list(calls)
        calls.clear()
        second = calibrate(reversed(self.configs), evaluate, repeats=2, seed=123)
        self.assertEqual(calls, first_calls)
        self.assertEqual(first.selected, second.selected)
        self.assertEqual(first.selected, min(self.configs))
        self.assertEqual(first.trace, second.trace)
        self.assertEqual(first_calls[0][1], first_calls[2][1])

    def test_correctness_and_error_exclusion_all_repeats(self):
        def evaluate(c, s):
            return Measurement(1 / c.workers, 1, c.workers != 4,
                               error_count=int(c.workers == 2))
        report = self.run_calibration(evaluate)
        self.assertEqual(report.selected.workers, 1)
        self.assertEqual({r.exclusion for r in report.records}, {None, 'incorrect', 'task_errors'})
        count = {}
        def inconsistent(c, s):
            count[c] = count.get(c, 0) + 1
            return Measurement(1, 1, count[c] != 2)
        self.assertIsNone(self.run_calibration(inconsistent).selected)

    def test_caps_fail_closed_and_all_observations_checked(self):
        objective = CalibrationObjective(max_cpu_seconds=2, max_peak_rss_bytes=100)
        cases = [
            (Measurement(1, 1, True), 'cpu_unavailable'),
            (Measurement(1, 1, True, cpu_seconds=3), 'cpu_cap'),
            (Measurement(1, 1, True, cpu_seconds=1), 'rss_unavailable'),
            (Measurement(1, 1, True, cpu_seconds=1, peak_rss_bytes=101), 'rss_cap'),
        ]
        for measurement, reason in cases:
            report = self.run_calibration(lambda c, s: measurement, objective=objective)
            self.assertIsNone(report.selected)
            self.assertTrue(all(r.exclusion == reason for r in report.records))
            self.assertIsNone(report.evaluations_to_stable_near_best)
        self.assertIsNotNone(self.run_calibration(
            lambda c, s: Measurement(1, 1, True, cpu_seconds=2, peak_rss_bytes=100),
            objective=objective).selected)

    def test_process_consent_before_any_callback(self):
        config = Configuration('process', 2, 4)
        calls = []
        def evaluate(c, s):
            calls.append(c)
            return Measurement(1, 1, True)
        with self.assertRaises(ValueError):
            calibrate([self.configs[0], config], evaluate, repeats=1, seed=0)
        self.assertEqual(calls, [])
        report = calibrate([config], evaluate, repeats=1, seed=0, allow_process=True)
        self.assertEqual(report.selected, config)
        with self.assertRaises(ValueError):
            config.executor_kwargs()
        self.assertEqual(config.executor_kwargs(allow_process=True)['process_workers'], 2)
        with self.assertRaises(ValueError):
            inspect_holdout(report, evaluate, repeats=1, seed=999)
        self.assertEqual(len(calls), 1)
        inspect_holdout(report, evaluate, repeats=1, seed=999, allow_process=True)

    def test_fixed_and_predictive_options(self):
        self.assertFalse(self.configs[0].executor_kwargs()['predictive'])
        predictive = Configuration('thread', 2, 16, 8).executor_kwargs()
        self.assertTrue(predictive['predictive'])
        self.assertEqual(predictive['profile_sample_interval'], 8)
        self.assertEqual(predictive['process_workers'], 0)

    def test_invalid_measurements_do_not_win_or_leak_errors(self):
        invalid = [None, {}, float('nan'), {'cold_seconds': 1}]
        for value in invalid:
            report = self.run_calibration(lambda c, s: value)
            self.assertIsNone(report.selected)
            self.assertTrue(all(r.measurement is None for r in report.records))
        def fail(c, s):
            raise RuntimeError('private task inputs')
        report = self.run_calibration(fail)
        self.assertNotIn('private', json.dumps(report.to_dict()))
        forged = Measurement(1, 1, True)
        object.__setattr__(forged, 'cold_seconds', math.inf)
        self.assertIsNone(self.run_calibration(lambda c, s: forged).selected)

    def test_cancellation_and_interrupts_propagate_without_next_callback(self):
        for exception in (CancelledError, KeyboardInterrupt, SystemExit):
            calls = []
            def fail(c, s):
                calls.append(c)
                raise exception()
            with self.assertRaises(exception):
                self.run_calibration(fail)
            self.assertEqual(len(calls), 1)

    def test_holdout_does_not_reselect_or_mutate(self):
        report = self.run_calibration(lambda c, s: Measurement(c.workers, 1, True))
        original = report.to_dict()
        calls = []
        def heldout(c, s):
            calls.append(c)
            return Measurement(100, 100, False)
        observations = inspect_holdout(report, heldout, repeats=3, seed=999)
        self.assertEqual(calls, [report.selected] * 3)
        self.assertEqual(len(observations), 3)
        self.assertTrue(all(r.exclusion == 'incorrect' for r in observations))
        self.assertEqual(report.to_dict(), original)

    def test_configuration_validation(self):
        for kwargs in ({'backend': 'gpu'}, {'workers': True}, {'workers': 0},
                       {'batch_size': -1}, {'profile_sample_interval': False},
                       {'profile_sample_interval': -1}):
            arguments = dict(backend='thread', workers=1, batch_size=1)
            arguments.update(kwargs)
            with self.assertRaises(ValueError):
                Configuration(**arguments)

    def test_measurement_validation(self):
        for field, bad_values in {
            'cold_seconds': [0, -1, math.nan, math.inf, True, '1', 10**1000],
            'warm_seconds': [0, math.inf], 'correct': [0, None],
            'error_count': [True, -1], 'cpu_seconds': [-1, math.nan],
            'peak_rss_bytes': [True, -1, 1.5],
        }.items():
            for value in bad_values:
                arguments = dict(cold_seconds=1, warm_seconds=1, correct=True)
                arguments[field] = value
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    Measurement(**arguments)

    def test_input_bounds_before_work(self):
        def forbidden(c, s):
            self.fail('must validate before callback')
        for candidates in ([], [self.configs[0]] * 2, [{}],
                           [Configuration('thread', 1, i + 1) for i in range(129)]):
            with self.assertRaises(ValueError):
                calibrate(candidates, forbidden, repeats=1, seed=0)
        for argument, value in [('repeats', 0), ('repeats', 33), ('repeats', True),
                                ('seed', -1), ('seed', True), ('allow_process', 1),
                                ('objective', {})]:
            kwargs = dict(repeats=1, seed=0)
            kwargs[argument] = value
            with self.assertRaises(ValueError):
                calibrate(self.configs, forbidden, **kwargs)

    def test_trace_is_only_retrospective_best_observed_and_stable(self):
        report = self.run_calibration(lambda c, s: Measurement({1: 1.09, 2: 2, 4: 1}[c.workers], 1, True))
        seen = []
        for candidate, point in zip(report.candidates, report.trace):
            seen.append(candidate.score_seconds)
            self.assertEqual(point.best_score_seconds, min(seen))
            self.assertAlmostEqual(point.relative_gap_to_best_observed, min(seen) - 1)
            if point.evaluations >= report.evaluations_to_stable_near_best:
                self.assertTrue(point.within_ten_percent)
        self.assertIn('calibration only', report.to_dict()['convergence_reference'])

    def test_unbounded_candidates_are_consumed_only_to_limit(self):
        count = []
        def candidates():
            for batch in range(1, 10000):
                count.append(batch)
                yield Configuration('thread', 1, batch)
        with self.assertRaises(ValueError):
            calibrate(candidates(), lambda c, s: self.fail('not called'), repeats=1, seed=0)
        self.assertEqual(len(count), 129)

    def test_extreme_finite_timings_have_json_safe_report(self):
        for value in (float.fromhex('0x1.fffffffffffffp+1023'), 5e-324):
            report = self.run_calibration(lambda c, s: Measurement(value, value, True))
            self.assertEqual(report.candidates[0].score_seconds, value)
            json.dumps(report.to_dict(), allow_nan=False)


if __name__ == '__main__':
    unittest.main()
