"""Small synthetic reports only; no workload, pool, media or network execution."""
import copy
from dataclasses import asdict
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from intelligent_futures.tuning import Configuration
from benchmarks.real_world.optimizer_metrics import GateBudgets
from tools.analyze_optimizer_rankings import audit, summary


A = asdict(Configuration('thread', 1, 1))
B = asdict(Configuration('thread', 8, 16))
SEEDS = [21, 22, 23]


def row(config, seed, cold, warm, engine='intelligent', role=None):
    def phase(seconds):
        return {'correct': True, 'seconds': seconds, 'tasks': 24,
                'throughput_per_second': 24 / seconds, 'error_rate': 0,
                'errors': [], 'batch_completion_latency_p95_seconds': .5}
    result = {'engine': engine, 'configuration': config, 'seed': seed,
              'cold': phase(cold), 'warm': phase(warm),
              'cold_lifecycle_seconds': cold,
              'sampled_tree_peak_rss_bytes': 100, 'lifecycle_cpu_seconds': 2.}
    if role is not None:
        result['baseline_role'] = role
    return result


def raw_fixture():
    raw = {'status': 'complete', 'failures': [],
           'protocol': {'workload': 'http', 'grid': [A, B],
                        'gates': asdict(GateBudgets()), 'count_per_pass': 24,
                        'training_fixture_seeds': [11, 12], 'heldout_seeds': SEEDS},
           'selections': {'budgeted': {'selected': A, 'virtual_callback_seconds': 20.},
                          'exhaustive': {'selected': A}},
           'standard_library_fixed_configuration': B,
           'manual_proxy_selection': {'selected': B, 'virtual_callback_seconds': 30.},
           'training': [row(config, seed, 4., 2.) for config in (A, B) for seed in (11, 12)],
           'heldout': [], 'baseline_heldout': []}
    for index, seed in enumerate(SEEDS):
        cold, warm = 4. + 2 * index, 2. + index
        raw['heldout'].extend([row(A, seed, cold, warm),
                               row(B, seed, 2. + index, 1. + index)])
        for role, factor, config, engine in (
            ('standard_library_fixed', 2., B, 'stdlib'),
            ('standard_library_manual_tuning_proxy', 1.5, B, 'stdlib'),
            ('direct_python', 3., None, 'sequential'),
        ):
            raw['baseline_heldout'].append(row(config, seed, cold * factor, warm * factor, engine, role))
    return raw


class RankingAuditTests(unittest.TestCase):
    def test_all_phases_absolute_times_ratios_and_descriptive_uncertainty(self):
        report = audit(raw_fixture())
        fixed = report['baselines']['standard_library_fixed']
        self.assertEqual(set(fixed['phases']), {'cold', 'warm', 'combined'})
        self.assertEqual(fixed['automatic_choice']['mean'], 9.)
        self.assertEqual(fixed['baseline']['mean'], 18.)
        self.assertEqual(fixed['automatic_choice']['median'], 9.)
        self.assertEqual(fixed['automatic_choice']['mad'], 3.)
        self.assertEqual((fixed['automatic_choice']['minimum'], fixed['automatic_choice']['maximum']), (6., 12.))
        for phase in fixed['phases'].values():
            self.assertEqual(phase['expected_seeds'], 3)
            self.assertTrue(phase['complete_pairs'])
            self.assertEqual(phase['baseline_mean_over_automatic_choice_mean'], 2.)
            self.assertEqual(phase['automatic_choice_mean_over_baseline_mean'], .5)
            self.assertEqual(phase['per_seed_baseline_over_selected_ratios']['mad'], 0.)
            self.assertEqual([slot['seed'] for slot in phase['seed_slots']], SEEDS)
        self.assertIn('not confidence intervals', fixed['descriptive_uncertainty_scope'])
        json.dumps(report, allow_nan=False)

    def test_effective_batch_and_worker_ceilings_are_not_active_worker_claims(self):
        report = audit(raw_fixture())
        capacity = report['baselines']['standard_library_fixed']['capacity']
        self.assertEqual(capacity['configured_workers'], 8)
        self.assertEqual(capacity['runnable_batches_per_pass'], 2)
        self.assertEqual(capacity['maximum_simultaneously_runnable_batches'], 2)
        self.assertEqual(capacity['effective_batch_size_ceiling'], 16)
        self.assertEqual(capacity['final_batch_size'], 8)
        self.assertIn('not measured active workers', capacity['scope'])
        self.assertIsNone(report['baselines']['direct_python']['capacity'])

    def test_fixed_and_direct_amortization_only_combined_positive_savings(self):
        report = audit(raw_fixture())
        for label, saving, cycles in (('standard_library_fixed', 9., 3), ('direct_python', 18., 2)):
            estimate = report['baselines'][label]['estimated_amortization']
            self.assertEqual(estimate['mean_execution_saving_seconds'], saving)
            self.assertEqual(estimate['primary_virtual_callback_seconds'], 20.)
            self.assertEqual(estimate['baseline_virtual_callback_seconds'], 0.)
            self.assertEqual(estimate['minimum_nonnegative_extra_cost_repayment_cycles'], cycles)
            self.assertIn('not live end-to-end', estimate['scope'])

    def test_manual_proxy_reports_both_costs_and_nonnegative_incremental_cycles(self):
        raw = raw_fixture()
        estimate = audit(raw)['baselines']['standard_library_manual_tuning_proxy']['estimated_amortization']
        self.assertEqual(estimate['primary_minus_baseline_discovery_seconds'], -10.)
        self.assertEqual(estimate['nonnegative_extra_discovery_seconds'], 0.)
        self.assertEqual(estimate['baseline_virtual_callback_seconds'], 30.)
        self.assertEqual(estimate['minimum_nonnegative_extra_cost_repayment_cycles'], 0)
        raw['manual_proxy_selection']['virtual_callback_seconds'] = 5.
        estimate = audit(raw)['baselines']['standard_library_manual_tuning_proxy']['estimated_amortization']
        self.assertEqual(estimate['primary_minus_baseline_discovery_seconds'], 15.)
        self.assertEqual(estimate['minimum_nonnegative_extra_cost_repayment_cycles'], 4)

    def test_slower_or_equal_execution_never_has_break_even_even_with_cheaper_search(self):
        for factor in (.5, 1.):
            raw = raw_fixture()
            for baseline in raw['baseline_heldout']:
                if baseline['baseline_role'] == 'standard_library_manual_tuning_proxy':
                    chosen = next(r for r in raw['heldout'] if r['seed'] == baseline['seed'] and r['configuration'] == A)
                    baseline['cold_lifecycle_seconds'] = chosen['cold_lifecycle_seconds'] * factor
                    baseline['warm']['seconds'] = chosen['warm']['seconds'] * factor
            result = audit(raw)['baselines']['standard_library_manual_tuning_proxy']['estimated_amortization']
            self.assertEqual(result['status'], 'no_positive_execution_saving')
            self.assertIsNone(result['minimum_nonnegative_extra_cost_repayment_cycles'])
            self.assertEqual(result['primary_minus_baseline_discovery_seconds'], -10.)

    def test_missing_duplicate_or_failed_baseline_preserves_slots_and_suppresses_aggregates(self):
        for mode in ('missing', 'duplicate', 'failed', 'wrong_engine', 'wrong_configuration', 'wrong_count'):
            raw = raw_fixture()
            target = next(r for r in raw['baseline_heldout'] if r['baseline_role'] == 'standard_library_fixed')
            if mode == 'missing':
                raw['baseline_heldout'].remove(target)
            elif mode == 'duplicate':
                raw['baseline_heldout'].append(copy.deepcopy(target))
            elif mode == 'failed':
                target['cold']['correct'] = False
            elif mode == 'wrong_engine':
                target['engine'] = 'intelligent'
            elif mode == 'wrong_configuration':
                target['configuration'] = A
            else:
                target['cold']['tasks'] = target['warm']['tasks'] = 23
            report = audit(raw)['baselines']['standard_library_fixed']
            with self.subTest(mode=mode):
                for phase in report['phases'].values():
                    self.assertEqual(len(phase['seed_slots']), 3)
                    self.assertEqual(phase['valid_seeds'], 2)
                    self.assertIsNone(phase['baseline_mean_over_automatic_choice_mean'])
                    self.assertIsNone(phase['per_seed_baseline_over_selected_ratios']['mean'])
                    self.assertEqual(phase['per_seed_baseline_over_selected_ratios']['count'], 2)
                    self.assertFalse(phase['seed_slots'][0]['valid'])
                self.assertIsNone(report['estimated_amortization']['minimum_nonnegative_extra_cost_repayment_cycles'])

    def test_failed_selected_slot_keeps_independent_baseline_time_without_success_only_ratio(self):
        raw = raw_fixture()
        raw['heldout'][0]['failure'] = 'synthetic failure'
        report = audit(raw)
        for label in report['baselines']:
            comparison = report['baselines'][label]
            self.assertEqual(comparison['valid_seeds'], 2)
            self.assertIsNone(comparison['automatic_choice']['mean'])
            self.assertIsNotNone(comparison['baseline']['mean'])
            self.assertIsNone(comparison['baseline_mean_over_automatic_choice_mean'])
        self.assertEqual(report['choices']['selected']['observations']['heldout']['combined']['count'], 2)
        self.assertIsNone(report['choices']['selected']['observations']['heldout']['combined']['mean'])

    def test_oracle_is_recomputed_per_phase_and_never_amortized(self):
        raw = raw_fixture()
        for value in raw['heldout']:
            if value['configuration'] == B:
                value['warm']['seconds'] = 20.
        raw['summaries'] = {'budgeted': {'combined': {'oracle_configuration': B}}}  # stale on purpose
        oracles = audit(raw)['conditional_candidate_oracles']
        self.assertEqual(oracles['cold']['oracle_configuration'], B)
        self.assertEqual(oracles['warm']['oracle_configuration'], A)
        self.assertEqual(oracles['combined']['oracle_configuration'], A)
        for result in oracles.values():
            self.assertTrue(result['oracle_valid'])
            self.assertIsNone(result['amortization'])
            self.assertIn('hindsight', result['scope'])

    def test_missing_other_grid_candidate_invalidates_oracle_not_external_baselines(self):
        raw = raw_fixture()
        raw['heldout'].remove(raw['heldout'][1])
        report = audit(raw)
        for result in report['conditional_candidate_oracles'].values():
            self.assertFalse(result['oracle_valid'])
            self.assertEqual(result['valid_seeds'], 0)
            self.assertEqual(len(result['seed_slots']), 3)
            self.assertIsNone(result['baseline_mean_over_automatic_choice_mean'])
        self.assertTrue(report['baselines']['standard_library_fixed']['complete_pairs'])
        self.assertIsNone(report['baselines']['standard_library_fixed']['matching_intelligent_mean_over_stdlib_mean'])

    def test_missing_cost_does_not_become_zero(self):
        raw = raw_fixture()
        del raw['manual_proxy_selection']['virtual_callback_seconds']
        result = audit(raw)['baselines']['standard_library_manual_tuning_proxy']['estimated_amortization']
        self.assertEqual(result['status'], 'discovery_cost_unavailable')
        self.assertIsNone(result['baseline_virtual_callback_seconds'])
        self.assertIsNone(result['minimum_nonnegative_extra_cost_repayment_cycles'])

    def test_invalid_costs_fail_closed_and_output_stays_strict_json(self):
        for cost in (None, True, -1, float('nan'), float('inf'), 10**1000):
            raw = raw_fixture()
            raw['selections']['budgeted']['virtual_callback_seconds'] = cost
            result = audit(raw)
            with self.subTest(cost_type=type(cost).__name__):
                self.assertIsNone(result['primary_virtual_callback_seconds'])
                self.assertEqual(result['baselines']['direct_python']['estimated_amortization']['status'],
                                 'discovery_cost_unavailable')
                json.dumps(result, allow_nan=False)

    def test_duplicate_selected_seed_and_failed_matching_config_remain_visible(self):
        raw = raw_fixture()
        raw['heldout'].append(copy.deepcopy(raw['heldout'][0]))
        result = audit(raw)
        self.assertEqual(result['baselines']['direct_python']['valid_seeds'], 2)
        self.assertIsNone(result['baselines']['direct_python']['baseline_mean_over_automatic_choice_mean'])
        self.assertFalse(result['conditional_candidate_oracles']['combined']['oracle_valid'])
        raw = raw_fixture()
        raw['heldout'][1]['failure'] = 'synthetic matching-config failure'
        comparison = audit(raw)['baselines']['standard_library_fixed']
        self.assertTrue(comparison['complete_pairs'])
        self.assertEqual(comparison['baseline_mean_over_automatic_choice_mean'], 2.)
        self.assertIsNone(comparison['matching_intelligent_mean_over_stdlib_mean'])
        self.assertIn('trial_failure', comparison['seed_slots'][0]['matching_config_failures'])

    def test_no_selected_configuration_fails_closed(self):
        raw = raw_fixture()
        raw['selections']['budgeted']['selected'] = None
        report = audit(raw)
        self.assertIsNone(report['choices']['selected']['capacity'])
        for comparison in report['baselines'].values():
            self.assertEqual(comparison['valid_seeds'], 0)
            self.assertEqual(len(comparison['seed_slots']), 3)
            self.assertIsNone(comparison['baseline_mean_over_automatic_choice_mean'])

    def test_undeclared_seed_does_not_silently_disappear(self):
        raw = raw_fixture()
        extra = copy.deepcopy(raw['baseline_heldout'][0])
        extra['seed'] = 999
        raw['baseline_heldout'].append(extra)
        result = audit(raw)['baselines']['standard_library_fixed']
        self.assertEqual(result['valid_seeds'], 0)
        self.assertIn('baseline:undeclared_seed_rows', result['seed_slots'][0]['failures'])

    def test_input_is_unchanged_and_legacy_combined_fields_remain(self):
        raw = raw_fixture()
        before = copy.deepcopy(raw)
        result = audit(raw)
        self.assertEqual(raw, before)
        self.assertIn('paired_observations', result)
        self.assertIn('aggregate_heldout_oracle', result['choices'])
        self.assertEqual(result['baselines']['direct_python']['baseline'],
                         result['baselines']['direct_python']['phases']['combined']['baseline'])

    def test_summary_preserves_running_missing_workloads_and_known_seed_slots(self):
        with tempfile.TemporaryDirectory() as directory, patch('tools.analyze_optimizer_rankings.NAMES', ('http', 'video', 'ocr')):
            root = Path(directory)
            (root / 'http.json').write_text(json.dumps(raw_fixture()))
            running = {'status': 'running', 'protocol': {'heldout_seeds': SEEDS}, 'failures': ['pending']}
            (root / 'video.json').write_text(json.dumps(running))
            result = summary(root)
            self.assertEqual(result['status'], 'partial')
            self.assertEqual((result['workloads_planned'], result['workloads_completed']), (3, 1))
            self.assertEqual(result['missing_or_running_workloads'], ['video', 'ocr'])
            self.assertEqual([slot['seed'] for slot in result['incomplete_workload_records'][0]['seed_slots']], SEEDS)
            self.assertFalse(any(slot['valid'] for slot in result['incomplete_workload_records'][0]['seed_slots']))
            self.assertIsNone(result['incomplete_workload_records'][1]['seed_slots'])


if __name__ == '__main__':
    unittest.main()
