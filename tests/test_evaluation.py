"""Deterministic evidence protocol tests, not fabricated performance evidence."""
from dataclasses import replace
import json
import math
import unittest
from unittest.mock import patch

from intelligent_futures.evaluation import (
    AdoptionRecommendation, EvaluationConfiguration as Config, EvaluationPlan,
    RepeatableWorkload, evaluate_workload, _median_lower, LOG_MARGIN_GUARD,
)
from intelligent_futures.tuning import Measurement


class EvaluationTests(unittest.TestCase):
    def workload(self, measure=None, **kw):
        if measure is None:
            measure = lambda engine, config, seed: Measurement(1.0 if engine == 'stdlib' else .75,
                                                              1.0 if engine == 'stdlib' else .75, True)
        return RepeatableWorkload('test-fixture', 'version-1', measure, repeatable=True,
                                  independent=True, metric_scope='test_declared', **kw)

    def run_evaluation(self, workload=None, *, configs=(Config(),), intelligent=None, plan=None, **kwargs):
        return evaluate_workload(workload or self.workload(), stdlib_configurations=configs,
            intelligent_configurations=configs if intelligent is None else intelligent,
            fixed_baseline=configs[0], plan=plan or EvaluationPlan(expected_reuses=1_000_000), **kwargs)

    def test_clear_gain_recommends_exact_mode_and_strict_json(self):
        report = self.run_evaluation()
        self.assertEqual(report.status, 'recommend_intelligent')
        self.assertEqual(report.recommendation.configuration, Config())
        self.assertEqual(report.comparisons[0].lower_order_statistic, 7)
        self.assertAlmostEqual(report.comparisons[0].lower_speedup, 1/0.75-1)
        self.assertEqual(report.attempted_evaluations, 6+48)
        self.assertEqual(report.expected_evaluations, report.attempted_evaluations)
        self.assertEqual(report.failed_evaluations, 0)
        json.dumps(report.to_json(), allow_nan=False)
        self.assertEqual(report.recommendation.checked_configuration(self.workload()), Config())

    def test_consent_precedes_all_callbacks(self):
        calls = []
        w = replace(self.workload(lambda *args: calls.append(args)), repeatable=False)
        with self.assertRaisesRegex(ValueError, 'repeatable=True'):
            self.run_evaluation(w)
        self.assertEqual(calls, [])

    def test_independence_is_required_for_inference_not_measurement(self):
        r = self.run_evaluation(replace(self.workload(), independent=False))
        self.assertEqual(r.status, 'inconclusive')
        self.assertIn('independent_representative_blocks_not_declared', r.reasons)
        self.assertGreater(r.attempted_evaluations, 0)
        self.assertIsNone(r.recommendation)

    def test_unknown_or_short_horizon_does_not_recommend(self):
        unknown = self.run_evaluation(plan=EvaluationPlan())
        self.assertEqual(unknown.status, 'inconclusive')
        self.assertIn('amortization_horizon_unknown', unknown.reasons)
        short = self.run_evaluation(plan=EvaluationPlan(expected_reuses=0))
        self.assertEqual(short.status, 'retain_stdlib')
        self.assertIn('evaluation_cost_not_amortized_in_declared_horizon', short.reasons)

    def test_ties_and_regressions_retain_stdlib(self):
        for candidate_time in (1.0, 1.01, 1.5):
            with self.subTest(candidate_time=candidate_time):
                r = self.run_evaluation(self.workload(lambda engine, config, seed:
                    Measurement(1.0 if engine == 'stdlib' else candidate_time, 1.0, True)))
                self.assertEqual(r.status, 'retain_stdlib')
                self.assertIsNone(r.recommendation)

    def test_practical_margin_ties_are_not_wins(self):
        # A lower interval equal to the requested margin is insufficient.
        r = self.run_evaluation(plan=EvaluationPlan(minimum_speedup=1/0.75-1, expected_reuses=100000))
        self.assertEqual(r.status, 'retain_stdlib')
        for comparison in r.comparisons:
            self.assertEqual(comparison.wins_above_margin, 0)
            self.assertEqual(comparison.sign_p_value, 1)
            self.assertIn('practical_improvement_not_established', comparison.reasons)

    def test_numerical_margin_guard_is_conservative_across_timing_scales(self):
        margin = .05
        threshold = math.log1p(margin)
        for scale in (1e-300, 1.0, 1e300):
            for offset, wins in ((-1e-10, 0), (0.0, 0),
                                 (LOG_MARGIN_GUARD / 4, 0),
                                 (LOG_MARGIN_GUARD * 2, 24), (1e-10, 24)):
                candidate = scale / math.exp(threshold + offset)
                workload = self.workload(lambda engine, config, seed:
                    Measurement(scale if engine == 'stdlib' else candidate, 1.0, True))
                plan = EvaluationPlan(minimum_speedup=margin, minimum_measured_seconds=0,
                                      expected_reuses=100000)
                with self.subTest(scale=scale, offset=offset):
                    r = self.run_evaluation(workload, plan=plan)
                    for comparison in r.comparisons:
                        self.assertEqual(comparison.wins_above_margin, wins)
                        self.assertEqual(comparison.sign_p_value, 1 if not wins else 2**-24)
                        self.assertEqual(comparison.passed, bool(wins))
                    if not wins:
                        self.assertEqual(r.status, 'retain_stdlib')

    def test_few_ulp_libm_rounding_cannot_turn_tie_into_win(self):
        margin = 1/.75-1
        threshold = math.log1p(margin)
        for steps in (-4, -1, 1, 4):
            rounded = threshold
            for _ in range(abs(steps)):
                rounded = math.nextafter(rounded, math.inf if steps > 0 else -math.inf)
            def rounded_log(value):
                self.assertIn(value, (1.0, .75))
                return 0.0 if value == 1.0 else -rounded
            with self.subTest(steps=steps), patch(
                    'intelligent_futures.evaluation.math.log', side_effect=rounded_log):
                r = self.run_evaluation(plan=EvaluationPlan(minimum_speedup=margin,
                                                           expected_reuses=100000))
            self.assertEqual(r.status, 'retain_stdlib')
            self.assertTrue(all(c.wins_above_margin == 0 and c.sign_p_value == 1
                                and not c.passed for c in r.comparisons))

    def test_small_fixed_plan_has_no_finite_bound(self):
        r = self.run_evaluation(plan=EvaluationPlan(heldout_blocks=5, expected_reuses=100000))
        self.assertEqual(r.status, 'inconclusive')
        self.assertIsNone(r.comparisons[0].lower_speedup)
        self.assertEqual(_median_lower([1.0]*5, .025), (None, None))
        self.assertEqual(_median_lower([1.0]*30, .025), (1.0, 10))

    def test_training_and_holdout_disjoint_and_orders_reproducible(self):
        a = self.run_evaluation(configs=(Config(workers=1), Config(workers=2)))
        b = self.run_evaluation(configs=(Config(workers=2), Config(workers=1)))
        self.assertFalse(set(a.training_seeds) & set(a.heldout_seeds))
        self.assertEqual([(r.phase,r.block,r.order,r.seed,r.engine,r.configuration) for r in a.records[:12]],
                         [(r.phase,r.block,r.order,r.seed,r.engine,r.configuration) for r in b.records[:12]])
        # Candidate order is randomized within each paired common-seed block.
        orders = [tuple(r.engine for r in a.records if r.phase=='heldout' and r.block==i)
                  for i in range(a.plan.heldout_blocks)]
        self.assertGreater(len(set(orders)), 1)

    def test_frozen_selection_cannot_chase_heldout_winner(self):
        config1, config2 = Config(workers=1), Config(workers=2)
        calls = 0
        def measure(engine, config, seed):
            nonlocal calls
            calls += 1
            training = calls <= 12
            seconds = (0.5 if config.workers == 1 else 1.0) if training else (2.0 if config.workers == 1 else .1)
            return Measurement(seconds, seconds, True)
        r = self.run_evaluation(self.workload(measure), configs=(config1, config2))
        self.assertEqual(r.selected_intelligent, config1)
        self.assertEqual(r.selected_stdlib, config1)
        self.assertEqual({x.configuration for x in r.records if x.phase=='heldout'}, {config1})
        self.assertEqual(r.status, 'retain_stdlib')

    def test_bad_training_candidate_stays_ineligible(self):
        c1, c2 = Config(workers=1), Config(workers=2)
        seen = set()
        def measure(engine, config, seed):
            key=(engine, config)
            first = key not in seen
            seen.add(key)
            return Measurement(.1 if config.workers==2 else 1.0, 1.0,
                               not (engine=='intelligent' and config.workers==2 and first))
        r = self.run_evaluation(self.workload(measure), configs=(c1, c2))
        self.assertEqual(r.selected_intelligent, c1)
        self.assertEqual(r.failed_evaluations, 1)

    def test_every_failure_vetoes_and_no_exception_text_leaks(self):
        calls=0
        def measure(engine, config, seed):
            nonlocal calls
            calls+=1
            if calls==15:
                raise RuntimeError('very-secret-payload')
            return Measurement(1 if engine=='stdlib' else .75, 1, True)
        r = self.run_evaluation(self.workload(measure))
        self.assertEqual(r.status, 'inconclusive')
        self.assertEqual(r.failed_evaluations, 1)
        self.assertEqual(r.attempted_evaluations, r.expected_evaluations)
        self.assertNotIn('very-secret-payload', json.dumps(r.to_json()))

    def test_resource_caps_fail_closed(self):
        for plan in (EvaluationPlan(max_cpu_seconds=3), EvaluationPlan(max_peak_rss_bytes=1000),
                     EvaluationPlan(max_batch_p95_seconds=3), EvaluationPlan(max_observed_seconds=.5)):
            r = self.run_evaluation(plan=plan)
            self.assertEqual(r.status, 'inconclusive')
            self.assertEqual(r.attempted_evaluations, 6)
            self.assertGreater(r.skipped_evaluations, 0)

    def test_catastrophic_single_tail_cannot_hide_in_median(self):
        count=0
        def measure(engine, config, seed):
            nonlocal count
            if engine=='intelligent':
                count+=1
            seconds=10 if engine=='intelligent' and count==24 else .5 if engine=='intelligent' else 1
            return Measurement(seconds, seconds, True)
        r=self.run_evaluation(self.workload(measure))
        self.assertEqual(r.status, 'retain_stdlib')
        self.assertIn('observed_tail_regression', r.comparisons[0].reasons)
        self.assertGreater(r.comparisons[0].lower_speedup, 0)

    def test_noise_and_resolution_floor(self):
        count=0
        def noisy(engine, config, seed):
            nonlocal count
            if engine=='intelligent': count+=1
            seconds=(.2 if count%2 else .8) if engine=='intelligent' else 1
            return Measurement(seconds, seconds, True)
        r=self.run_evaluation(self.workload(noisy))
        self.assertEqual(r.status,'inconclusive')
        self.assertIn('paired_noise_exceeds_threshold',r.comparisons[0].reasons)
        tiny=self.run_evaluation(self.workload(lambda engine,c,s: Measurement(1e-6 if engine=='stdlib' else 1e-7,1,True)))
        self.assertIn('timing_below_resolution_floor',tiny.comparisons[0].reasons)

    def test_process_consent_and_baseline_equivalence(self):
        process=Config('process',2)
        with self.assertRaisesRegex(ValueError,'allow_process'):
            self.run_evaluation(configs=(process,))
        adaptive=Config('adaptive',4,process_workers=2)
        with self.assertRaisesRegex(ValueError,'equivalent'):
            self.run_evaluation(intelligent=(adaptive,),allow_process=True)
        with self.assertRaisesRegex(ValueError,'deterministic'):
            self.run_evaluation(configs=(adaptive,),allow_process=True)
        report=self.run_evaluation(configs=(process,),allow_process=True)
        with self.assertRaisesRegex(ValueError,'renewed'):
            report.recommendation.checked_configuration(self.workload())
        self.assertEqual(report.recommendation.checked_configuration(self.workload(),allow_process=True),process)

    def test_invalid_or_overbudget_plan_does_not_call(self):
        calls=[]
        for plan in (EvaluationPlan(max_evaluations=1),EvaluationPlan(max_workers=1)):
            with self.assertRaises(ValueError):
                self.run_evaluation(self.workload(lambda *a:calls.append(a)),plan=plan)
        self.assertEqual(calls,[])
        for kwargs in ({'heldout_blocks':True},{'family_alpha':.5},{'max_workers':0},
                       {'minimum_speedup':float('nan')},{'expected_reuses':-1},{'metric':'choose_best'}):
            with self.assertRaises(ValueError): EvaluationPlan(**kwargs)
        for kwargs in ({'workers':True},{'backend':'adaptive'},{'process_workers':1},
                       {'predictive':'yes'},{'profile_sample_interval':2}):
            with self.assertRaises(ValueError): Config(**kwargs)

    def test_invalid_measurements_sanitized_and_interruptions_propagate(self):
        r=self.run_evaluation(self.workload(lambda *a:None))
        self.assertEqual(r.failed_evaluations,6)
        self.assertIsNone(r.selected_intelligent)
        def interrupted(*a): raise KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt): self.run_evaluation(self.workload(interrupted))
        from concurrent.futures import CancelledError
        def cancelled(*a): raise CancelledError
        with self.assertRaises(CancelledError): self.run_evaluation(self.workload(cancelled))

    def test_extreme_valid_numbers_fail_closed_with_strict_json(self):
        r=self.run_evaluation(self.workload(lambda e,c,s:Measurement(1e300 if e=='stdlib' else 1e-300,1,True)))
        self.assertNotEqual(r.status,'recommend_intelligent')
        json.dumps(r.to_json(),allow_nan=False)

    def test_reuse_identity_expiry_runtime_checks(self):
        rec=self.run_evaluation().recommendation
        with self.assertRaisesRegex(ValueError,'runtime'):
            replace(rec,evaluation_revision='paired-heldout-sign-v1').checked_configuration(self.workload())
        for workload in (replace(self.workload(),version='v2'),replace(self.workload(),name='other'),
                         replace(self.workload(),metric_scope='different')):
            with self.assertRaisesRegex(ValueError,'identity'):rec.checked_configuration(workload)
        for now in (rec.created_at-1,rec.expires_at,rec.expires_at+1):
            with self.assertRaisesRegex(ValueError,'expired'):rec.checked_configuration(self.workload(),now=now)
        with patch('intelligent_futures.evaluation._package_version',return_value='changed'):
            with self.assertRaisesRegex(ValueError,'runtime'):rec.checked_configuration(self.workload())

    def test_failed_training_denominator_matches_reserved_budget(self):
        report=self.run_evaluation(self.workload(lambda *a:Measurement(1,1,False)),
            plan=EvaluationPlan(max_evaluations=54))
        self.assertEqual(report.expected_evaluations,54)
        self.assertEqual(report.attempted_evaluations,6)
        self.assertEqual(report.skipped_evaluations,48)

    def test_exact_null_rejection_probability_is_bounded(self):
        # Enumerate the binomial null, not a Monte Carlo performance experiment.
        n=24
        _, k=_median_lower(list(range(n)),.025)
        probability=sum(math.comb(n,j) for j in range(n-k+1,n+1))/2**n
        self.assertLessEqual(probability,.025)

    def test_recommendation_rechecks_adapter_declarations(self):
        measure=self.workload().measure
        measure._evaluation_max_pending=8
        workload=self.workload(measure)
        rec=self.run_evaluation(workload).recommendation
        measure._evaluation_max_pending=4
        with self.assertRaisesRegex(ValueError,'window changed'):
            rec.checked_configuration(workload)
        measure._evaluation_max_pending=8
        measure._evaluation_process_safe=True
        process=Config('process',2)
        rec=self.run_evaluation(workload,configs=(process,),allow_process=True).recommendation
        measure._evaluation_process_safe=False
        with self.assertRaisesRegex(ValueError,'safety changed'):
            rec.checked_configuration(workload,allow_process=True)

    def test_pool_lifecycle_adapter_cannot_claim_warm_timing(self):
        w=replace(self.workload(),metric_scope='pool_lifecycle')
        with self.assertRaisesRegex(ValueError,'cold_seconds'):
            self.run_evaluation(w,plan=EvaluationPlan(metric='warm_seconds'))
        def measure(*a):return Measurement(1,1,True)
        measure._evaluation_max_pending=4
        with self.assertRaisesRegex(ValueError,'submission window'):
            self.run_evaluation(self.workload(measure))


if __name__=='__main__':unittest.main()
