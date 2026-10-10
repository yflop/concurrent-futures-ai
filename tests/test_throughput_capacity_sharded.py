"""Mock-only protocol/flow tests: no workloads, executors, or subprocesses run."""
from __future__ import annotations

from contextlib import ExitStack, redirect_stdout
from dataclasses import asdict, replace
import io
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from tools import probe_throughput_capacity_sharded as probe


class CapacityProtocolTests(unittest.TestCase):
    def test_fixture_preflight_checks_files_without_algorithms_or_executors(self):
        def prepare(name, root, count, seed, *, scale):
            self.assertLessEqual(count, 64)
            root.mkdir(parents=True)
            tasks = []
            for index in range(count):
                path = root / f'{index}.jsonl'
                path.write_bytes(b'{}\n' * 128)
                tasks.append({'id': index, 'path': str(path)})
            return tasks
        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(probe, 'module_for', return_value=types.SimpleNamespace(prepare=prepare)), \
                patch.object(probe, 'execute_batch') as execute, patch.object(probe, 'trial') as trial, \
                patch.object(probe, 'tune') as tune, patch.object(probe, 'calibrate') as calibrate:
            output = Path(tmp) / 'preflight.json'
            report = probe.fixture_preflight(output)
            self.assertTrue(report['accepted'])
            self.assertEqual(report['prepared_task_count'], 128)
            self.assertEqual(report['observed_pages_per_pass'], 16384)
            self.assertEqual(report['started_trials'], 0)
            self.assertEqual(report['executed_task_observations'], 0)
            self.assertEqual(report['reference_task_observations'], 0)
            self.assertEqual(report['protocol_sha256'], probe.digest(probe.plan()))
            self.assertEqual(report['dataset_seed'], probe.fixture_seeds(probe.TRAIN_SEED)[0])
            for mocked in (execute, trial, tune, calibrate):
                mocked.assert_not_called()
            with self.assertRaises(FileExistsError):
                probe.fixture_preflight(output)

    def test_actual_frozen_adapter_rejects_original_invalid_count_without_io(self):
        from benchmarks.real_world import data
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(ValueError, 'representative count must be at most 64'):
                data.prepare('pipeline', root, 128, 1, scale='representative')
            self.assertEqual(list(root.iterdir()), [])

    def test_bounded_shard_composition_preserves_original_task_dictionaries(self):
        calls, originals = [], []
        def prepare(name, root, count, seed, *, scale):
            if count > 64:
                raise ValueError('representative count must be at most 64')
            calls.append((root, count, seed))
            tasks = [{'id': i, 'path': str(root / str(i)), 'unchanged': object()} for i in range(count)]
            originals.extend(tasks)
            return tasks
        metadata = {}
        tasks = probe.prepare_sharded(types.SimpleNamespace(prepare=prepare), Path('/unused'),
            probe.fixture_seeds(probe.TRAIN_SEED)[0], metadata=metadata)
        self.assertEqual(len(calls), 2)
        self.assertEqual([count for _, count, _ in calls], [64, 64])
        self.assertNotEqual(calls[0][0], calls[1][0])
        self.assertNotEqual(calls[0][2], calls[1][2])
        self.assertTrue(all(a is b for a, b in zip(tasks, originals)))
        self.assertEqual([t['id'] for t in tasks], list(range(64)) * 2)
        self.assertEqual(metadata['declared_pages_per_task'], 128)
        self.assertEqual(metadata['declared_pages_per_pass'], 16384)

    def test_new_logical_and_shard_namespaces_keep_failed_original_separate(self):
        plan = probe.plan()
        composition = plan['fixture_composition']
        datasets = composition['datasets']
        self.assertEqual(len(datasets), 7)
        seeds = [s['fixture_seed'] for d in datasets for s in d['shards']]
        self.assertEqual(len(set(seeds)), 14)
        forbidden = {761213}
        for generator, heldout in probe.EARLIER_NAMESPACES:
            forbidden.update((generator, *probe.fixture_seeds(generator), *heldout))
        logical = {probe.TRAIN_SEED, probe.SHARD_SEED_NAMESPACE,
            *probe.fixture_seeds(probe.TRAIN_SEED), *probe.HOLDOUT_SEEDS}
        self.assertFalse(set(seeds) & logical)
        self.assertFalse((set(seeds) | logical) & forbidden)
        self.assertTrue(plan['prior_failed_scenario']['retained_as_failure'])
        self.assertEqual(plan['prior_failed_scenario']['started_trials'], 0)
        self.assertNotEqual(probe.digest(plan), plan['prior_failed_scenario']['protocol_sha256'])

    def test_predeclared_grid_seeds_costs_and_parallelism(self):
        plan = probe.plan()
        self.assertEqual(plan['count_per_pass'], 128)
        self.assertEqual(plan['passes_per_trial'], 2)
        self.assertEqual(len(plan['grid']), 8)
        self.assertEqual({c['batch_size'] for c in plan['grid']}, {16})
        self.assertEqual({(c['backend'], c['workers']) for c in plan['grid']},
            {(b, w) for b in ('thread', 'process') for w in (1, 2, 4, 8)})
        excluded = {1001207, 1010231, 1020241, 1030249, 881237, 970171, 980183, 990191,
            *probe.fixture_seeds(1001207), *probe.fixture_seeds(881237)}
        seeds = {plan['training_seed'], *plan['training_fixture_seeds'], *plan['heldout_seeds']}
        self.assertEqual(len(seeds), 8)
        self.assertFalse(seeds & excluded)
        self.assertEqual(plan['planned_costs']['training_callbacks'], 32)
        self.assertEqual(plan['planned_costs']['heldout_oracle_callbacks'], 24)
        self.assertEqual(plan['planned_costs']['total_callbacks'], 62)
        self.assertEqual(plan['runnable_batches_per_pass'], 8)
        self.assertIn('128 pages per task', plan['fixture_size_caveat'])
        self.assertIn('16384 pages per pass', plan['fixture_size_caveat'])
        self.assertIn('not a pure task-count scaling experiment', plan['fixture_size_caveat'])
        for config, ceiling in zip(probe.CANDIDATES, plan['parallelism_ceilings']):
            self.assertEqual(ceiling['runnable_batches_per_pass'], 8)
            self.assertEqual(ceiling['affinity_bounded_parallelism_ceiling'], config.workers)
            self.assertIn('not observed concurrency or CPU utilization', ceiling['scope'])
        self.assertIn('conditional on batch_size=16', plan['oracle'])

    def test_requires_exact_affinity_and_never_silently_shrinks_grid(self):
        for affinity in (None, 1, 4, 16, True, 8.):
            with self.subTest(affinity=affinity), self.assertRaises(ValueError):
                probe.plan(affinity)
        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(probe.os, 'sched_getaffinity', return_value=set(range(4)), create=True), \
                patch.object(probe.os, 'sched_setaffinity', create=True), \
                patch.object(probe, 'module_for') as module:
            with self.assertRaisesRegex(ValueError, 'unavailable'):
                probe.run(Path(tmp) / 'report.json')
            module.assert_not_called()

    def test_existing_evidence_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'report.json'
            output.write_text('prior evidence')
            with patch.object(probe, 'module_for') as module:
                with self.assertRaises(FileExistsError):
                    probe.run(output)
                module.assert_not_called()
            self.assertEqual(output.read_text(), 'prior evidence')

    def test_cli_stop_marker_prevents_any_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / '.stop-before-next-workload').touch()
            with patch.object(probe, 'run') as run:
                self.assertEqual(probe.main(['--output', str(root / 'report.json')]), 75)
                run.assert_not_called()


class CapacityFlowTests(unittest.TestCase):
    def fake_run(self, *, change_row=None, prepare_failure=None, reference_failure=None,
                 selection_patch=None, affinity_failure=False, environment=None):
        events, preparations, references, checkpoints = [], [], [], []
        affinity = [set(range(12))]
        logical_for_shard = {shard: seed for seed in (*probe.fixture_seeds(probe.TRAIN_SEED), *probe.HOLDOUT_SEEDS) for shard in probe.shard_seeds(seed)}

        def set_affinity(pid, cpus):
            affinity[0] = set(cpus) if not affinity_failure else set(range(12))

        def frozen_before_holdout(seed):
            if seed in probe.HOLDOUT_SEEDS:
                saved = json.loads(output.read_text())
                self.assertEqual(len(saved['training']), 32)
                self.assertEqual(set(saved['selections']), set(probe.METHODS))
                self.assertEqual(probe.digest(saved['selections']), saved['selection_frozen_sha256'])
                self.assertEqual(probe.digest(saved['protocol']), saved['protocol_sha256'])
                self.assertEqual(saved['heldout_freeze']['prepared_heldout_datasets'], 0)
                self.assertEqual(saved['heldout_freeze']['observed_heldout_rows'], 0)
                self.assertIn(probe.TOOL_PATH, saved['environment']['source_sha256'])

        def prepare(name, root, count, seed, *, scale):
            if count > 64:
                raise ValueError('representative count must be at most 64 to bound fixture resources')
            self.assertEqual((name, count, scale), ('pipeline', 64, 'representative'))
            logical_seed = logical_for_shard[seed]
            frozen_before_holdout(logical_seed)
            preparations.append(seed)
            if prepare_failure and prepare_failure(logical_seed):
                raise RuntimeError('planned fixture failure')
            return [{'id': i, 'seed': seed, 'path': str(root / f'pages-{i:05d}.jsonl')} for i in range(count)]

        def execute(payload):
            name, tasks = payload
            self.assertEqual(name, 'pipeline')
            self.assertEqual(len(tasks), 128)
            references.append(logical_for_shard[tasks[0]['seed']])
            rows = [{'value': {'id': t['id'], 'seed': t['seed'], 'pages': 128}, 'error': None} for t in tasks]
            if reference_failure and reference_failure(logical_for_shard[tasks[0]['seed']]):
                rows[0]['error'] = 'planned reference error'
            return rows

        def trial(name, engine, config, count, seed, root, prepared):
            frozen_before_holdout(seed)
            events.append((engine, config, seed))
            self.assertEqual(count, 128)
            self.assertEqual(len(prepared['tasks']), 128)
            self.assertEqual(prepared['reference_sha256'], probe.digest([
                {'id': i, 'seed': shard, 'pages': 128} for shard in probe.shard_seeds(seed) for i in range(64)]))
            self.assertEqual([t['id'] for t in prepared['tasks']], list(range(64)) * 2)
            self.assertEqual(len({t['path'] for t in prepared['tasks']}), 128)
            seconds = (9. - config.workers) if config else 10.
            phase = {'seconds': seconds, 'correct': True, 'errors': [], 'error_rate': 0.,
                'batch_completion_latency_p95_seconds': seconds, 'tasks': count,
                'throughput_per_second': count / seconds, 'result_sha256': prepared['reference_sha256'],
                'completion_trace': [{'test': 'trim this'}]}
            row = {'engine': engine, 'configuration': asdict(config) if config else None,
                'seed': seed, 'cold': dict(phase), 'warm': dict(phase),
                'cold_lifecycle_seconds': seconds, 'lifecycle_cpu_seconds': 1.,
                'sampled_tree_peak_rss_bytes': 1000, 'rss_collection': {'complete': True},
                'profiles_after_two_batches': ['trim this'], 'optional_physical_snapshot': {'peak': 1}}
            if change_row:
                change_row(row, len(events))
            return row

        def direct(name, prepared, seed):
            return trial(name, 'sequential', None, len(prepared['tasks']), seed, None, prepared)

        original_write = probe.write

        def write(path, report):
            checkpoints.append(tuple(len(report[p]) for p in probe.PHASES))
            original_write(path, report)

        module = types.SimpleNamespace(prepare=prepare)
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'report.json'
            with ExitStack() as stack:
                stack.enter_context(patch.object(probe, 'module_for', return_value=module))
                stack.enter_context(patch.object(probe, 'execute_batch', side_effect=execute))
                stack.enter_context(patch.object(probe, 'trial', side_effect=trial))
                stack.enter_context(patch.object(probe, 'direct_trial', side_effect=direct))
                stack.enter_context(patch.object(probe, 'workload_environment', return_value=environment or {'mock_only': True}))
                stack.enter_context(patch.object(probe, 'write', side_effect=write))
                stack.enter_context(patch.object(probe.os, 'sched_getaffinity', side_effect=lambda pid: affinity[0], create=True))
                stack.enter_context(patch.object(probe.os, 'sched_setaffinity', side_effect=set_affinity, create=True))
                stack.enter_context(redirect_stdout(io.StringIO()))
                if selection_patch:
                    stack.enter_context(selection_patch)
                try:
                    report = probe.run(output)
                finally:
                    self.last_checkpoint = json.loads(output.read_text())
                    self.last_affinity = affinity[0]
                self.assertEqual(json.loads(output.read_text()), json.loads(json.dumps(report)))
        self.assertEqual(affinity[0], set(range(12)))
        self.assertEqual(len(preparations), sum(len(d['shards']) for d in report['datasets']))
        self.assertEqual(len(set(preparations)), len(preparations))
        self.assertEqual(len(report['datasets']), 7)
        self.assertEqual(report['status'], 'complete')
        self.assertEqual([len(report[p]) for p in probe.PHASES], [32, 24, 6])
        self.assertEqual(report['scheduled_trial_slots'], 62)
        self.assertEqual(report['observed_trials'], len(events))
        self.assertEqual(report['unexecuted_trial_slots'], 62 - len(events))
        self.assertEqual(report['selections']['exhaustive']['evaluations'],
            0 if report['selections']['exhaustive']['selection_status'] == 'failed' else 32)
        self.assertEqual(set((probe.config_key(r['configuration']), r['seed']) for r in report['heldout']),
            {(probe.config_key(c), seed) for c in probe.CANDIDATES for seed in probe.HOLDOUT_SEEDS})
        self.assertTrue(all(probe.COUNT == r[p]['tasks'] for r in report['heldout']
            for p in ('cold', 'warm') if p in r and not r.get('failure')))
        self.assertEqual([seed in probe.HOLDOUT_SEEDS for _, _, seed in events],
            sorted(seed in probe.HOLDOUT_SEEDS for _, _, seed in events))
        # Every appended observation gets an immediate persisted checkpoint.
        self.assertEqual(len(set(checkpoints)), 63)
        self.assertEqual(report['costs']['observed_callback_seconds'], sum(
            r['measured_callback_seconds'] for phase in probe.PHASES for r in report[phase]))
        self.assertEqual(report['costs']['fixture_preparation_seconds'], sum(
            d['preparation_seconds'] for d in report['datasets']))
        self.assertEqual(report['costs']['reference_seconds'], sum(d['reference_seconds'] for d in report['datasets']))
        self.assertGreaterEqual(report['elapsed_seconds'], report['costs']['observed_callback_seconds'])
        return report

    def test_full_mock_flow_freeze_real_counts_replay_budget_and_references(self):
        report = self.fake_run()
        self.assertTrue(report['accepted'])
        self.assertEqual(report['executed_task_observations'], 62 * 128 * 2)
        self.assertEqual(report['costs']['reference_task_observations'], 7 * 128)
        self.assertEqual(report['selections']['budgeted']['evaluations'], 24)
        self.assertEqual(report['selections']['exhaustive_median']['evaluations'], 32)
        for method, selection in report['selections'].items():
            audit = selection['cost_audit']
            self.assertEqual(audit['exhaustive_callback_count'], 32)
            self.assertEqual(audit['sample_reduction_fraction'], .25 if method == 'budgeted' else 0.)
            self.assertEqual(audit['recorded_requested_callback_seconds'], selection['virtual_callback_seconds'])
            self.assertEqual(audit['recorded_exhaustive_callback_seconds'], report['costs']['by_phase']['training']['callback_seconds'])
            for summary in report['summaries'][method].values():
                self.assertTrue(summary['oracle_valid'])
                self.assertTrue(summary['quality_gate_eligible'])
                self.assertEqual(summary['coverage_denominator'], 3)
                self.assertEqual(summary['aggregate_relative_regret'], 0.)
                self.assertIn('conditional on batch_size=16', summary['oracle_scope'])
            self.assertEqual(report['baseline_comparisons'][method]['valid_comparison_slots'], 18)
        for phase in probe.PHASES:
            for row in report[phase]:
                self.assertNotIn('completion_trace', row['cold'])
                self.assertNotIn('profiles_after_two_batches', row)
                self.assertEqual(row['optional_physical_snapshot'], {'peak': 1})

    def test_failed_training_callback_remains_in_matrix_and_cost_denominator(self):
        def fail_first(row, index):
            if index == 1:
                raise RuntimeError('planned callback failure')
        report = self.fake_run(change_row=fail_first)
        row = report['training'][0]
        self.assertIn('planned callback failure', row['failure'])
        self.assertGreater(row['measured_callback_seconds'], 0.)
        self.assertEqual(report['costs']['by_phase']['training']['failed_slots'], 1)
        self.assertEqual(report['costs']['by_phase']['training']['failed_callback_seconds'], row['measured_callback_seconds'])
        self.assertEqual(report['selections']['exhaustive']['virtual_failed_callback_seconds'], row['measured_callback_seconds'])
        self.assertTrue(any(r['measurement'] is None for r in report['selections']['exhaustive']['report']['records']))

    def test_no_selection_does_not_suppress_other_methods_or_heldout(self):
        original = probe.tune
        def no_budgeted(*args, **kwargs):
            result = original(*args, **kwargs)
            return replace(result, selected=None) if kwargs['min_repeats'] == 2 else result
        report = self.fake_run(selection_patch=patch.object(probe, 'tune', side_effect=no_budgeted))
        self.assertEqual(report['selections']['budgeted']['selection_status'], 'no_eligible_configuration')
        self.assertFalse(report['accepted'])
        self.assertTrue(report['summaries']['budgeted']['combined']['oracle_valid'])
        self.assertEqual(report['summaries']['budgeted']['combined']['within_10_percent_count'], 0)
        self.assertTrue(report['summaries']['exhaustive']['combined']['quality_gate_eligible'])
        self.assertEqual(report['baseline_comparisons']['budgeted']['valid_comparison_slots'], 0)

    def test_selector_exception_is_retained_and_other_methods_continue(self):
        report = self.fake_run(selection_patch=patch.object(probe, 'calibrate', side_effect=RuntimeError('planned selector error')))
        self.assertEqual(report['selections']['exhaustive']['selection_status'], 'failed')
        self.assertIn('planned selector error', report['selections']['exhaustive']['failure'])
        self.assertTrue(report['summaries']['budgeted']['combined']['quality_gate_eligible'])
        self.assertFalse(report['accepted'])

    def test_failed_fixture_is_costed_cached_and_other_seeds_continue(self):
        seed = probe.fixture_seeds(probe.TRAIN_SEED)[0]
        report = self.fake_run(prepare_failure=lambda candidate: candidate == seed)
        self.assertEqual(report['costs']['by_phase']['training']['started_callbacks'], 24)
        self.assertEqual(report['costs']['by_phase']['training']['failed_slots'], 8)
        self.assertEqual(report['costs']['by_phase']['training']['unexecuted_slots'], 8)
        self.assertEqual(report['observed_trials'], 54)
        self.assertEqual(report['scheduled_trial_slots'], 62)
        self.assertEqual(report['unexecuted_trial_slots'], 8)
        self.assertTrue(all(not r['execution_attempted'] for r in report['training'] if r['seed'] == seed))
        self.assertTrue(all(r['measured_callback_seconds'] == 0. for r in report['training'] if r['seed'] == seed))
        self.assertGreater(report['costs']['failed_dataset_preparation_and_reference_seconds'], 0.)
        self.assertTrue(report['summaries']['budgeted']['combined']['oracle_valid'])
        self.assertFalse(report['accepted'])

    def test_all_fixture_failures_count_zero_trials_without_dropping_quality_slots(self):
        report = self.fake_run(prepare_failure=lambda seed: True)
        self.assertEqual(report['scheduled_trial_slots'], 62)
        self.assertEqual(report['observed_trials'], 0)
        self.assertEqual(report['unexecuted_trial_slots'], 62)
        self.assertEqual(report['executed_task_observations'], 0)
        self.assertEqual(report['costs']['observed_callback_seconds'], 0.)
        self.assertFalse(report['accepted'])
        for summaries in report['summaries'].values():
            self.assertEqual(summaries['combined']['coverage_denominator'], 3)
            self.assertFalse(summaries['combined']['oracle_valid'])
            self.assertEqual(summaries['combined']['grid_audit']['observed_row_count'], 24)

    def test_reference_error_is_preserved_and_never_runs_invalid_fixture(self):
        seed = probe.fixture_seeds(probe.TRAIN_SEED)[0]
        report = self.fake_run(reference_failure=lambda candidate: candidate == seed)
        self.assertEqual(report['datasets'][0]['reference_errors'], ['planned reference error'])
        self.assertEqual(report['costs']['by_phase']['training']['started_callbacks'], 24)
        self.assertFalse(report['accepted'])

    def test_gate_failures_and_bad_hashes_fail_closed_without_erasing_measurements(self):
        for label in ('rss', 'cpu', 'p95', 'errors', 'rss_incomplete', 'hash', 'count'):
            def corrupt(row, index, label=label):
                if row['seed'] not in probe.HOLDOUT_SEEDS or row['engine'] != 'intelligent':
                    return
                if label == 'rss': row['sampled_tree_peak_rss_bytes'] = 1024**3 + 1
                elif label == 'cpu': row['lifecycle_cpu_seconds'] = 300.1
                elif label == 'p95': row['warm']['batch_completion_latency_p95_seconds'] = 60.1
                elif label == 'errors': row['cold']['errors'] = ['bad task']
                elif label == 'rss_incomplete': row['rss_collection']['complete'] = False
                elif label == 'hash': row['warm']['result_sha256'] = 'wrong'
                elif label == 'count': row['warm']['tasks'] = 127
            with self.subTest(label=label):
                report = self.fake_run(change_row=corrupt)
                self.assertFalse(report['accepted'])
                self.assertFalse(report['summaries']['budgeted']['combined']['oracle_valid'])
                self.assertTrue(all(r['gate_failures'] for r in report['heldout']))
                self.assertEqual(report['costs']['by_phase']['heldout']['gate_failed_slots'], 24)

    def test_interrupted_callback_is_costed_before_propagating(self):
        def interrupt(row, index):
            if index == 1:
                raise KeyboardInterrupt('planned interruption')
        with self.assertRaises(KeyboardInterrupt):
            self.fake_run(change_row=interrupt)
        report = self.last_checkpoint
        self.assertEqual(report['status'], 'interrupted')
        self.assertEqual(len(report['training']), 1)
        row = report['training'][0]
        self.assertIn('KeyboardInterrupt', row['failure'])
        self.assertTrue(row['execution_attempted'])
        self.assertGreater(row['measured_callback_seconds'], 0.)
        self.assertEqual(report['costs']['by_phase']['training']['failed_callback_seconds'], row['measured_callback_seconds'])
        self.assertEqual(self.last_affinity, set(range(12)))

    def test_baseline_failure_is_retained_and_invalidates_only_its_slots(self):
        def fail_direct(row, index):
            if row['engine'] == 'sequential':
                raise RuntimeError('planned direct-reference failure')
        report = self.fake_run(change_row=fail_direct)
        self.assertFalse(report['accepted'])
        self.assertTrue(report['summaries']['budgeted']['combined']['quality_gate_eligible'])
        self.assertEqual(report['baseline_comparisons']['budgeted']['valid_comparison_slots'], 9)
        self.assertEqual(report['costs']['by_phase']['baseline_heldout']['failed_slots'], 3)

    def test_affinity_verification_failure_checkpoints_and_restores(self):
        with self.assertRaisesRegex(RuntimeError, 'exactly eight'):
            self.fake_run(affinity_failure=True)
        self.assertEqual(self.last_checkpoint['status'], 'failed')
        self.assertEqual(self.last_checkpoint['training'], [])
        self.assertEqual(self.last_affinity, set(range(12)))

    def test_source_change_is_reported_without_losing_completed_evidence(self):
        report = self.fake_run(environment={'source_sha256': {probe.TOOL_PATH: 'replaced-by-actual-hash', 'not-a-source.py': 'bad'}})
        self.assertEqual(report['source_changes_during_run'], ['not-a-source.py'])
        self.assertFalse(report['accepted'])
        self.assertTrue(report['summaries']['budgeted']['combined']['quality_gate_eligible'])


if __name__ == '__main__':
    unittest.main()
