"""Portable mocked affinity-constraint checks; no workload or child jobs run."""
from contextlib import ExitStack, redirect_stdout
from dataclasses import asdict, replace
import io
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from intelligent_futures.tuning import Configuration, Measurement, fixture_seeds
from benchmarks.real_world import optimizer
from tools import probe_affinity_constraints as probe


SOURCES = {probe.TOOL_PATH: 'a' * 64, 'src/intelligent_futures/tuning.py': 'b' * 64}


def baseline_report(name='images', *, saved=True):
    plan = optimizer.plan(name, 8)
    sha = optimizer.digest(plan)
    selections = {'budgeted': {'saved_configuration': {
        'workload_key': f'optimizer-evidence:v1:{name}:representative:24:{sha}'} if saved else None}}
    return {'status': 'complete', 'protocol': plan, 'protocol_sha256': sha,
        'selections': selections, 'selection_frozen_sha256': optimizer.digest(selections)}


class AffinityProtocolTests(unittest.TestCase):
    def test_four_predeclared_disjoint_namespaces_and_exact_grids(self):
        probe.validate_namespaces()
        seen = set()
        for name in probe.NAMES:
            for cap in probe.CAPS:
                base = {'file_sha256': 'c' * 64, 'saved_configuration': None}
                plan = probe.protocol(name, cap, sources=SOURCES, baseline=base,
                    available_cpu_ids=range(8), constrained_cpu_ids=range(cap))
                self.assertEqual(len(plan['grid']), 6 * cap)
                self.assertEqual({c['workers'] for c in plan['grid']}, set(range(1, cap + 1)))
                self.assertEqual({c['batch_size'] for c in plan['grid']}, {1, 4, 16})
                self.assertEqual({c['backend'] for c in plan['grid']}, {'thread', 'process'})
                self.assertEqual(plan['count_per_pass'], 24)
                self.assertEqual(plan['passes_per_trial'], 2)
                self.assertEqual(plan['gates'], {'max_p95_seconds': 60., 'max_rss_bytes': 1024**3, 'max_cpu_seconds': 300.})
                self.assertNotIn('training_gates', plan)
                self.assertEqual(len(plan['baselines']), 3)
                seeds = {plan['training_seed'], *plan['training_fixture_seeds'], *plan['heldout_seeds']}
                self.assertFalse(seeds & seen)
                seen.update(seeds)
                planned = plan['experiment']['planned_counts']
                self.assertEqual(planned['primary_replay_callback_limit'], 18 * cap)
                self.assertEqual(planned['total_callback_slots'], 66 * cap + 9)
                self.assertEqual(plan['experiment']['runner_source_sha256'], SOURCES[probe.TOOL_PATH])
                self.assertIn('No new-hardware', plan['experiment']['scope'])

    def test_baseline_requires_matching_complete_raw_primary_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'base.json'
            valid = baseline_report()
            path.write_text(json.dumps(valid))
            parsed = probe.read_baseline(path, 'images')
            self.assertEqual(parsed['saved_configuration']['workload_key'], valid['selections']['budgeted']['saved_configuration']['workload_key'])
            with self.assertRaisesRegex(ValueError, 'same workload'):
                probe.read_baseline(path, 'geospatial')
            corrupt = json.loads(json.dumps(valid))
            corrupt['selections']['budgeted']['saved_configuration']['workload_key'] = 'changed'
            path.write_text(json.dumps(corrupt))
            with self.assertRaisesRegex(ValueError, 'selection hash'):
                probe.read_baseline(path, 'images')
            corrupt['selection_frozen_sha256'] = optimizer.digest(corrupt['selections'])
            path.write_text(json.dumps(corrupt))
            with self.assertRaisesRegex(ValueError, 'original exact workload key'):
                probe.read_baseline(path, 'images')

    def test_real_profile_loader_rejects_changed_mock_affinity_without_callbacks(self):
        # Actual persistence implementation; only CPU affinity and callback data are mocked.
        with patch.object(probe.os, 'sched_getaffinity', return_value=set(range(8)), create=True):
            chosen = optimizer.calibrate([Configuration('thread', 1, 1)],
                lambda config, seed: Measurement(1., 1., correct=True, cpu_seconds=1., peak_rss_bytes=100,
                                                batch_p95_seconds=1.), repeats=1, seed=12)
            saved = json.loads(optimizer.export_configuration(chosen, workload_key='exact-test-key'))
        with patch.object(probe.os, 'sched_getaffinity', return_value={0}, create=True):
            result = probe.check_profile_reuse({'saved_configuration': saved}, 1)
        self.assertTrue(result['passed'])
        self.assertEqual(result['reason'], 'hardware fingerprint mismatch')
        self.assertEqual(result['workload_key'], 'exact-test-key')
        self.assertEqual(result['calibration_callbacks'], 0)

    def test_other_profile_errors_are_not_misreported_as_hardware_rejection(self):
        base = {'saved_configuration': {'workload_key': 'exact-key'}}
        with patch.object(probe, 'load_configuration', side_effect=ValueError('Python runtime mismatch')):
            result = probe.check_profile_reuse(base, 2)
        self.assertFalse(result['passed'])
        self.assertEqual(result['reason'], 'Python runtime mismatch')
        with patch.object(probe, 'load_configuration', return_value=object()):
            self.assertEqual(probe.check_profile_reuse(base, 1)['status'], 'unexpectedly_accepted')
        self.assertEqual(probe.check_profile_reuse({'saved_configuration': None}, 1)['status'], 'unavailable')

    def test_unsupported_platform_and_output_protection_precede_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'report.json'
            with patch.object(probe.os, 'sched_getaffinity', None, create=True), \
                    patch.object(optimizer, 'run') as run:
                with self.assertRaisesRegex(RuntimeError, 'Linux CPU-affinity APIs'):
                    probe.run('images', output, 1, baseline=Path(tmp) / 'missing.json')
                run.assert_not_called()
            output.write_text('preserve me')
            with self.assertRaises(FileExistsError):
                probe.run('images', output, 1, baseline=Path(tmp) / 'missing.json')
            self.assertEqual(output.read_text(), 'preserve me')

    def test_cli_stop_marker_and_bounded_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / '.stop-before-next-workload').touch()
            with patch.object(probe, 'run') as run:
                self.assertEqual(probe.main(['--workload', 'images', '--affinity', '1',
                    '--output', str(path / 'r.json'), '--baseline', str(path / 'b.json')]), 75)
                run.assert_not_called()
        for name, cap in (('ocr', 1), ('images', 8), ('geospatial', True)):
            with self.assertRaises(ValueError):
                probe.protocol(name, cap, sources=SOURCES, baseline={},
                    available_cpu_ids=range(8), constrained_cpu_ids=range(1))


class AffinityFlowTests(unittest.TestCase):
    def mock_flow(self, name='images', cap=1, *, fail_trial=False, fail_fixture=False,
                  no_secondary=False, no_manual=False, missing_profile=False, changed_sources=False):
        mask = [set(range(8))]
        observed = []
        training = fixture_seeds(seed=probe.TRAINING_SEEDS[(name, cap)], repeats=4)
        heldout = probe.HELDOUT_SEEDS[(name, cap)]

        def prepare(workload, root, count, seed, scale):
            self.assertEqual((workload, count, scale), (name, 24, 'representative'))
            self.assertEqual(len(mask[0]), cap)
            if seed in heldout:
                frozen = json.loads(output.read_text())
                self.assertEqual(optimizer.digest(frozen['selections']), frozen['selection_frozen_sha256'])
                self.assertEqual(optimizer.digest(frozen['protocol']), frozen['protocol_sha256'])
                self.assertEqual(len(frozen['training']), 24 * cap)
                self.assertEqual(len(frozen['manual_training']), 24 * cap)
                self.assertEqual(frozen['protocol']['experiment']['source_sha256'], SOURCES)
            if fail_fixture and seed == training[0]:
                raise RuntimeError('planned fixture failure')
            return [{'seed': seed, 'index': i} for i in range(count)]

        def trial(workload, engine, config, count, seed, root, prepared):
            observed.append((engine, seed))
            self.assertEqual(len(mask[0]), cap)
            self.assertEqual(len(prepared['tasks']), 24)
            if fail_trial and len(observed) == 1:
                raise RuntimeError('planned callback failure')
            seconds = float(config.workers + config.batch_size)
            phase = {'seconds': seconds, 'correct': True, 'errors': [], 'error_rate': 0.,
                'batch_completion_latency_p95_seconds': seconds, 'tasks': count,
                'throughput_per_second': count / seconds, 'result_sha256': prepared['reference_sha256']}
            return {'engine': engine, 'configuration': asdict(config), 'seed': seed,
                'cold': dict(phase), 'warm': dict(phase), 'cold_lifecycle_seconds': seconds,
                'lifecycle_cpu_seconds': 1., 'sampled_tree_peak_rss_bytes': 1000}

        def direct(workload, prepared, seed):
            result = trial(workload, 'sequential', Configuration('thread', 1, 1), 24, seed, None, prepared)
            result['configuration'] = None
            return result

        original_tune, original_calibrate = optimizer.tune, optimizer.calibrate
        calls = []
        def tune(*args, **kwargs):
            result = original_tune(*args, **kwargs)
            return replace(result, selected=None) if no_secondary and kwargs['min_repeats'] == 1 else result
        def calibrate(*args, **kwargs):
            result = original_calibrate(*args, **kwargs)
            calls.append(1)
            return replace(result, selected=None) if no_manual and len(calls) == 2 else result

        with tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
            root = Path(tmp)
            output, baseline = root / 'output.json', root / 'baseline.json'
            baseline.write_text(json.dumps(baseline_report(name, saved=not missing_profile)))
            stack.enter_context(patch.object(probe.os, 'sched_getaffinity', side_effect=lambda pid: mask[0], create=True))
            stack.enter_context(patch.object(probe.os, 'sched_setaffinity', side_effect=lambda pid, cpus: mask.__setitem__(0, set(cpus)), create=True))
            stack.enter_context(patch.object(probe, 'source_hashes', side_effect=[SOURCES, dict(SOURCES, changed='c')] if changed_sources else None, return_value=SOURCES))
            stack.enter_context(patch.object(probe, 'load_configuration', side_effect=ValueError('hardware fingerprint mismatch')))
            stack.enter_context(patch.object(optimizer, 'module_for', return_value=types.SimpleNamespace(prepare=prepare)))
            stack.enter_context(patch.object(optimizer, 'execute_batch', side_effect=lambda payload:
                [{'value': (t['seed'], t['index']), 'error': None} for t in payload[1]]))
            stack.enter_context(patch.object(optimizer, 'trial', side_effect=trial))
            stack.enter_context(patch.object(optimizer, 'direct_trial', side_effect=direct))
            stack.enter_context(patch.object(optimizer, 'workload_environment', return_value={'source_sha256': dict(SOURCES)}))
            stack.enter_context(patch.object(optimizer, 'tune', side_effect=tune))
            stack.enter_context(patch.object(optimizer, 'calibrate', side_effect=calibrate))
            stack.enter_context(redirect_stdout(io.StringIO()))
            report = probe.run(name, output, cap, baseline=baseline)
            self.assertEqual(json.loads(output.read_text()), json.loads(json.dumps(report)))
        self.assertEqual(mask[0], set(range(8)))
        self.assertEqual(report['status'], 'complete')
        self.assertEqual(report['protocol']['training_seed'], probe.TRAINING_SEEDS[(name, cap)])
        self.assertEqual(len(report['training']), 24 * cap)
        self.assertEqual(len(report['manual_training']), 24 * cap)
        self.assertEqual(len(report['heldout']), 18 * cap)
        self.assertEqual(report['constraint_accounting']['expected_callback_slots'], 66 * cap + 9)
        self.assertEqual(report['constraint_accounting']['actual_callback_seconds'], sum(
            r.get('measured_callback_seconds') or 0. for phase in probe.PHASES for r in report[phase]))
        self.assertEqual(optimizer.digest(report['selections']), report['selection_frozen_sha256'])
        self.assertEqual(optimizer.digest(report['protocol']), report['protocol_sha256'])
        self.assertEqual([seed in heldout for _, seed in observed], sorted(seed in heldout for _, seed in observed))
        return report

    def test_all_four_full_mocked_matrices_and_replay_budgets(self):
        for name in probe.NAMES:
            for cap in probe.CAPS:
                with self.subTest(name=name, affinity=cap):
                    report = self.mock_flow(name, cap)
                    self.assertTrue(report['accepted'])
                    self.assertEqual(report['selections']['budgeted']['evaluations'], 18 * cap)
                    self.assertEqual(report['selections']['exhaustive']['evaluations'], 24 * cap)
                    self.assertEqual(report['observed_trials'], 66 * cap + 9)
                    self.assertEqual(report['executed_task_observations'], (66 * cap + 9) * 48)
                    self.assertEqual(report['constraint_accounting']['successful_reference_task_observations'], 168)
                    self.assertTrue(report['primary_profile_reuse_check']['passed'])
                    self.assertEqual(report['environment']['source_sha256'][probe.TOOL_PATH], SOURCES[probe.TOOL_PATH])

    def test_failed_callbacks_remain_costed(self):
        report = self.mock_flow(fail_trial=True)
        phases = report['constraint_accounting']['phases']
        self.assertEqual(sum(p['failure_slots'] for p in phases.values()), 1)
        self.assertGreater(report['constraint_accounting']['failed_callback_seconds'], 0.)
        self.assertEqual(report['constraint_accounting']['actual_executed_callbacks'], 75)

    def test_fixture_failures_preserve_unexecuted_slots_and_other_observations(self):
        report = self.mock_flow(fail_fixture=True)
        phases = report['constraint_accounting']['phases']
        self.assertEqual(phases['training']['unexecuted_slots'], 6)
        self.assertEqual(phases['manual_training']['unexecuted_slots'], 6)
        self.assertTrue(all(r['measured_callback_seconds'] is None for r in report['training'] if r.get('trial_executed') is False))
        self.assertGreater(report['constraint_accounting']['failed_fixture_or_reference_seconds'], 0.)
        self.assertEqual(len(report['baseline_heldout']), 6)
        self.assertEqual(phases['baseline_heldout']['missing_slots'], 3)
        self.assertFalse(report['accepted'])

    def test_no_secondary_selection_does_not_suppress_primary_or_full_oracle(self):
        report = self.mock_flow(no_secondary=True)
        self.assertIsNone(report['selections']['budgeted_lowcost']['selected'])
        self.assertTrue(report['summaries']['budgeted']['combined']['quality_gate_eligible'])
        self.assertFalse(report['accepted'])

    def test_missing_manual_selection_is_visible_and_other_baselines_run(self):
        report = self.mock_flow(no_manual=True)
        self.assertEqual(len(report['baseline_heldout']), 6)
        self.assertEqual(report['constraint_accounting']['phases']['baseline_heldout']['missing_slots'], 3)
        self.assertFalse(report['accepted'])

    def test_profile_check_unavailable_does_not_suppress_matrix(self):
        report = self.mock_flow(missing_profile=True)
        self.assertTrue(report['matrix_accepted'])
        self.assertFalse(report['accepted'])
        self.assertEqual(report['primary_profile_reuse_check']['status'], 'unavailable')

    def test_source_changes_leave_completed_matrix_visible(self):
        report = self.mock_flow(changed_sources=True)
        self.assertFalse(report['source_unchanged_at_end'])
        self.assertTrue(report['matrix_accepted'])
        self.assertFalse(report['accepted'])

    def test_optimizer_exception_checkpoints_and_restores_affinity(self):
        mask = [set(range(8))]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline, output = root / 'base.json', root / 'output.json'
            baseline.write_text(json.dumps(baseline_report()))
            with patch.object(probe.os, 'sched_getaffinity', side_effect=lambda pid: mask[0], create=True), \
                    patch.object(probe.os, 'sched_setaffinity', side_effect=lambda pid, cpus: mask.__setitem__(0, set(cpus)), create=True), \
                    patch.object(probe, 'source_hashes', return_value=SOURCES), \
                    patch.object(probe, 'load_configuration', side_effect=ValueError('hardware fingerprint mismatch')), \
                    patch.object(optimizer, 'run', side_effect=RuntimeError('planned outer failure')):
                with self.assertRaisesRegex(RuntimeError, 'planned outer failure'):
                    probe.run('images', output, 1, baseline=baseline)
            self.assertEqual(mask[0], set(range(8)))
            report = json.loads(output.read_text())
            self.assertEqual(report['status'], 'failed')
            self.assertEqual(report['constraint_accounting']['observed_callback_slots'], 0)
            self.assertTrue(report['primary_profile_reuse_check']['passed'])


if __name__ == '__main__':
    unittest.main()
