"""Mocked media-envelope protocols and accounting; no media programs execute."""
import contextlib
from dataclasses import asdict
import io
import json
from pathlib import Path
import tempfile
import types
import unittest
import subprocess
from unittest.mock import Mock, patch

from intelligent_futures.tuning import Configuration, fixture_seeds
from benchmarks.real_world import optimizer
from tools import probe_media_envelope as probe


SOURCES = {'tools/probe_media_envelope.py': 'a' * 64,
           'src/intelligent_futures/tuning.py': 'b' * 64}
FONT = {'font_name': 'DejaVuSans-Bold.ttf', 'font_size': 42,
        'font_file_name': 'DejaVuSans-Bold.ttf', 'font_sha256': 'c' * 64,
        'fixture_revision': 'bold42-exact-text-v2'}
TRAINEDDATA = {'language': 'eng', 'file_name': 'eng.traineddata', 'sha256': 'f' * 64}


def reference_rows(args):
    name, tasks = args
    return [{'value': {'seed': task['seed'], 'index': task['index']}, 'error': None}
            for task in tasks]


def prepare(name, root, count, seed, scale):
    return [{'seed': seed, 'index': index} for index in range(count)]


def row(config, seed, *, engine='intelligent', count=24, rss=500, parent=300):
    seconds = float(config.workers + config.batch_size)
    phase = {'seconds': seconds, 'correct': True, 'errors': [], 'error_rate': 0.,
             'batch_completion_latency_p95_seconds': seconds,
             'tasks': count, 'throughput_per_second': count / seconds, 'completion_trace': []}
    sample = {'tree_rss_bytes': rss, 'parent_rss_bytes': parent,
              'descendant_rss_bytes': rss - parent, 'observed_descendant_count': 1}
    return {'configuration': asdict(config), 'seed': seed, 'engine': engine,
            'cold': dict(phase), 'warm': dict(phase), 'cold_lifecycle_seconds': seconds,
            'lifecycle_cpu_seconds': 1., 'sampled_tree_peak_rss_bytes': rss,
            'rss_collection': {'complete': True, 'first_complete_sample': dict(sample),
                               'last_complete_sample': dict(sample), 'peak_complete_sample': dict(sample)}}


class MediaEnvelopeTests(unittest.TestCase):
    def test_original_protocol_defaults_keep_original_shape_and_seed_sequence(self):
        original = optimizer.plan('video', 8)
        self.assertEqual(original['training_seed'], optimizer.TRAIN_SEED)
        self.assertEqual(original['training_fixture_seeds'], fixture_seeds(seed=optimizer.TRAIN_SEED, repeats=4))
        self.assertEqual(original['heldout_seeds'], optimizer.HOLDOUT_SEEDS)
        self.assertEqual(len(original['baselines']), 3)
        self.assertNotIn('training_gates', original)
        self.assertNotIn('experiment', original)
        self.assertNotIn('fixture_failure_handling', original)
        explicit = optimizer.plan('video', 8, training_seed=optimizer.TRAIN_SEED,
            heldout_seeds=optimizer.HOLDOUT_SEEDS, include_baselines=True,
            experiment_metadata=None, record_fixture_failures=False)
        self.assertEqual(optimizer.digest(original), optimizer.digest(explicit))

    def test_video_protocol_declares_fresh_seeds_reserve_grid_and_original_denominator(self):
        declared = probe.protocol('video', sources=SOURCES)
        self.assertEqual(len(declared['grid']), 24)
        self.assertEqual(declared['training_seed'], 20261013)
        self.assertEqual(declared['heldout_seeds'], (20262011, 20262017, 20262023))
        self.assertEqual(declared['training_gates']['max_rss_bytes'], 715827882)
        self.assertEqual(declared['gates']['max_rss_bytes'], 1024**3)
        self.assertEqual(declared['baselines'], {})
        self.assertEqual(declared['count_per_pass'], 24)
        self.assertEqual(declared['passes_per_trial'], 2)
        metadata = declared['experiment']
        self.assertEqual(metadata['fresh_context']['planned_trials'], 3)
        self.assertEqual(metadata['original_primary_strata'], 11)
        self.assertNotIn('original_primary_accepted_strata', metadata)
        self.assertFalse(metadata['replaces_primary_evidence'])
        self.assertIn('Experimental', metadata['calibration_memory_reserve']['status'])
        self.assertEqual(metadata['source_snapshot_sha256'], optimizer.digest(SOURCES))
        self.assertTrue(set(declared['training_fixture_seeds']).isdisjoint(declared['heldout_seeds']))

    def test_ocr_has_separate_seeds_full_baselines_and_frozen_actual_font_identity(self):
        with patch.object(probe, 'ocr_font_identity', return_value=FONT), \
             patch.object(probe, 'ocr_traineddata_identity', return_value=TRAINEDDATA):
            declared = probe.protocol('ocr', sources=SOURCES)
        video = probe.protocol('video', sources=SOURCES)
        self.assertNotEqual(declared['training_seed'], video['training_seed'])
        self.assertTrue(set(declared['heldout_seeds']).isdisjoint(video['heldout_seeds']))
        self.assertEqual(len(declared['baselines']), 3)
        self.assertEqual(declared['training_gates']['max_rss_bytes'], 1024**3)
        self.assertEqual(declared['gates']['max_rss_bytes'], 1024**3)
        self.assertEqual(declared['experiment']['fixture_font'], FONT)
        self.assertEqual(declared['experiment']['traineddata'], TRAINEDDATA)
        self.assertFalse(declared['experiment']['fresh_context']['enabled'])

    def test_actual_traineddata_resolution_retains_only_filename_and_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'eng.traineddata'
            path.write_bytes(b'test-only-traineddata')
            listed = f'List of available languages in "{directory}/" (1):\neng\n'
            with patch.object(probe.subprocess, 'check_output', return_value=listed):
                identity = probe.ocr_traineddata_identity()
            self.assertEqual(identity['file_name'], 'eng.traineddata')
            self.assertEqual(identity['sha256'], probe.hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertNotIn(directory, json.dumps(identity))

    def test_reject_invalid_parameters_without_work(self):
        for affinity in (True, 0, 7, 8.0):
            with self.subTest(affinity=affinity), self.assertRaises(ValueError):
                probe.protocol('video', affinity, sources=SOURCES)
        for options in ({'include_baselines': 1}, {'record_fixture_failures': 1},
                        {'training_max_rss_bytes': True}, {'training_max_rss_bytes': 0},
                        {'training_seed': True}, {'experiment_metadata': []}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                optimizer.plan('video', 8, **options)

    def test_full_video_flow_freezes_choices_and_runs_exactly_171_trials(self):
        heldout = probe.ENVELOPES['video']['heldout_seeds']
        calls, fresh_requests = [], []
        module = types.SimpleNamespace(prepare=prepare)
        def trial(name, engine, config, count, seed, root, prepared):
            if seed in heldout:
                saved = json.loads(output.read_text())
                self.assertEqual(len(saved['training']), 96)
                self.assertEqual(len(saved['manual_training']), 0)
                self.assertEqual(saved['selection_frozen_sha256'], optimizer.digest(saved['selections']))
                self.assertEqual(saved['selections']['budgeted']['evaluations'], 72)
            calls.append((config, seed))
            return row(config, seed, engine=engine, count=count)
        def fresh(request):
            saved = json.loads(output.read_text())
            self.assertEqual(len(saved['heldout']), 72)
            self.assertEqual(request['selection_frozen_sha256'], saved['selection_frozen_sha256'])
            self.assertEqual(request['configuration'], saved['selections']['budgeted']['selected'])
            self.assertEqual(request['source_sha256'], SOURCES)
            fresh_requests.append(request)
            result = row(Configuration(**request['configuration']), request['seed'], rss=400, parent=200)
            result.update(trial_executed=True, measured_callback_seconds=.01,
                          reference_matches_reused_parent=True)
            return result
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'follow-on.json'
            with patch.object(probe, 'source_hashes', return_value=SOURCES), \
                 patch.object(optimizer, 'module_for', return_value=module), \
                 patch.object(optimizer, 'execute_batch', side_effect=reference_rows), \
                 patch.object(optimizer, 'trial', side_effect=trial), \
                 patch.object(optimizer, 'workload_environment', return_value={'test_only': True}), \
                 patch.object(probe, 'run_fresh_process', side_effect=fresh), \
                 patch('os.sched_getaffinity', return_value=set(range(8)), create=True), \
                 patch('os.sched_setaffinity', create=True), contextlib.redirect_stdout(io.StringIO()):
                report = probe.run('video', output)
        self.assertEqual(len(calls), 168)
        self.assertEqual(len(fresh_requests), 3)
        self.assertEqual(report['observed_trials'], 171)
        self.assertEqual(report['scheduled_trial_slots'], 171)
        self.assertEqual(report['expected_trial_slots'], 171)
        self.assertEqual(report['executed_task_observations'], 171 * 48)
        self.assertEqual(report['baseline_comparisons'], {})
        self.assertEqual(report['baseline_heldout'], [])
        self.assertEqual(report['unknown_trial_execution_slots'], 0)
        self.assertTrue(report['source_unchanged_at_end'])
        self.assertTrue(report['accepted'])
        for comparison in report['retention_comparison']['comparisons']:
            self.assertEqual(comparison['reused_minus_fresh_tree_peak_bytes'], 100)
            self.assertEqual(comparison['fresh_parent_gates'], [])

    def run_small_matrix(self, output, *, prepare_mock=None, reference_mock=None, **options):
        configs = (Configuration('thread', 1, 1), Configuration('thread', 2, 1))
        prepare_mock = prepare_mock or Mock(side_effect=prepare)
        reference_mock = reference_mock or Mock(side_effect=reference_rows)
        def trial(name, engine, config, count, seed, root, prepared):
            # The empirically fastest candidate is below the evaluation cap but
            # above the experimental calibration reserve.
            rss = 800_000_000 if config.workers == 1 else 500_000_000
            return row(config, seed, engine=engine, count=count, rss=rss)
        defaults = dict(training_seed=20261013, heldout_seeds=(20262011, 20262017, 20262023),
                        training_max_rss_bytes=probe.VIDEO_TRAINING_RSS_BYTES,
                        include_baselines=False, record_fixture_failures=True)
        defaults.update(options)
        with patch.object(optimizer, 'CANDIDATES', configs), \
             patch.object(optimizer, 'COUNT', 2), \
             patch.object(optimizer, 'module_for', return_value=types.SimpleNamespace(prepare=prepare_mock)), \
             patch.object(optimizer, 'execute_batch', reference_mock), \
             patch.object(optimizer, 'trial', side_effect=trial) as trial_mock, \
             patch.object(optimizer, 'workload_environment', return_value={}), \
             contextlib.redirect_stdout(io.StringIO()):
            result = optimizer.run('video', output, **defaults)
        return result, prepare_mock, reference_mock, trial_mock

    def test_separate_calibration_cap_rejects_fast_candidate_that_passes_evaluation_cap(self):
        with tempfile.TemporaryDirectory() as directory:
            report, _, _, calls = self.run_small_matrix(Path(directory) / 'result.json')
        self.assertEqual(report['selections']['budgeted']['selected']['workers'], 2)
        self.assertEqual(report['selections']['budgeted']['report']['objective']['max_peak_rss_bytes'], 715827882)
        audit = report['summaries']['budgeted']['combined']['grid_audit']
        self.assertEqual(audit['eligible_candidate_count'], 2)
        self.assertEqual(audit['budgets']['max_rss_bytes'], 1024**3)
        self.assertEqual(calls.call_count, 14)
        self.assertEqual(report['baseline_comparisons'], {})

    def test_fixture_failure_is_cached_once_per_seed_with_zero_fabricated_execution(self):
        for phase in ('prepare', 'reference'):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as directory:
                failed_prepare = Mock(side_effect=RuntimeError('bad fixture')) if phase == 'prepare' else None
                failed_reference = Mock(return_value=[{'value': None, 'error': 'wrong exact truth'}]) if phase == 'reference' else None
                report, prepares, references, trials = self.run_small_matrix(
                    Path(directory) / 'result.json', prepare_mock=failed_prepare, reference_mock=failed_reference)
                self.assertEqual(prepares.call_count, 7)
                self.assertEqual(references.call_count, 0 if phase == 'prepare' else 7)
                trials.assert_not_called()
                self.assertEqual(len(report['datasets']), 7)
                self.assertEqual(len(report['training']), 8)
                self.assertEqual(len(report['heldout']), 6)
                self.assertEqual(report['scheduled_trial_slots'], 14)
                self.assertEqual(report['observed_trials'], 0)
                self.assertEqual(report['executed_task_observations'], 0)
                self.assertFalse(report['accepted'])
                for result in report['training'] + report['heldout']:
                    self.assertIs(result['trial_executed'], False)
                    self.assertIsNone(result['measured_callback_seconds'])
                    self.assertNotIn('cold', result)
                self.assertEqual(report['selections']['exhaustive']['unexecuted_training_slots_requested'], 8)
                audit = report['summaries']['budgeted']['combined']['grid_audit']
                self.assertTrue(audit['grid_complete'])
                self.assertEqual(audit['eligible_candidate_count'], 0)

    def test_legacy_fixture_failure_still_aborts_when_option_is_off(self):
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(RuntimeError, 'bad fixture'):
            self.run_small_matrix(Path(directory) / 'result.json',
                prepare_mock=Mock(side_effect=RuntimeError('bad fixture')), record_fixture_failures=False)

    def fresh_request(self):
        seed = probe.ENVELOPES['video']['heldout_seeds'][0]
        values = [r['value'] for r in reference_rows(('video', prepare('video', None, 24, seed, 'representative')))]
        return {'workload': 'video', 'seed': seed, 'configuration': asdict(Configuration('thread', 1, 1)),
                'affinity': 8, 'source_sha256': SOURCES, 'reference_sha256': optimizer.digest(values),
                'protocol_sha256': 'd' * 64, 'selection_frozen_sha256': 'e' * 64}

    def invoke_fresh(self, request):
        with patch.object(probe, 'source_hashes', return_value=SOURCES), \
             patch.object(probe, 'module_for', return_value=types.SimpleNamespace(prepare=prepare)), \
             patch.object(probe, 'execute_batch', side_effect=reference_rows), \
             patch.object(probe, 'trial', side_effect=lambda name, engine, config, count, seed, root, prepared: row(config, seed)) as trial_mock, \
             patch.object(probe, 'workload_environment', return_value={'test_only': True}), \
             patch('os.sched_getaffinity', return_value=set(range(8)), create=True), patch('os.sched_setaffinity', create=True):
            result = probe.fresh_trial(request)
        return result, trial_mock

    def test_fresh_trial_regenerates_reference_and_runs_only_the_frozen_configuration(self):
        request = self.fresh_request()
        result, trial_mock = self.invoke_fresh(request)
        self.assertEqual(trial_mock.call_count, 1)
        self.assertEqual(result['configuration'], request['configuration'])
        self.assertTrue(result['reference_matches_reused_parent'])
        self.assertTrue(result['trial_executed'])
        self.assertEqual(result['dataset']['count'], 24)
        self.assertEqual(result['fresh_context']['source_sha256'], SOURCES)
        self.assertEqual(result['fresh_context']['selection_frozen_sha256'], request['selection_frozen_sha256'])

    def test_fresh_reference_mismatch_fails_before_trial(self):
        request = dict(self.fresh_request(), reference_sha256='f' * 64)
        result, trial_mock = self.invoke_fresh(request)
        trial_mock.assert_not_called()
        self.assertFalse(result['trial_executed'])
        self.assertIsNone(result['measured_callback_seconds'])
        self.assertFalse(result['reference_matches_reused_parent'])
        self.assertIn('reference digests differ', result['failure'])

    def test_changed_fresh_source_is_rejected_before_fixture_preparation(self):
        request = dict(self.fresh_request(), source_sha256={})
        with self.assertRaisesRegex(ValueError, 'source snapshot changed'):
            self.invoke_fresh(request)

    def test_existing_output_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'original.json'
            output.write_text('original evidence')
            with patch.object(optimizer, 'run') as work, self.assertRaisesRegex(ValueError, 'already exists'):
                probe.run('video', output)
            work.assert_not_called()
            self.assertEqual(output.read_text(), 'original evidence')

    def mock_matrix(self, *, selected=None):
        declaration = probe.protocol('video', sources=SOURCES)
        selections = {'budgeted': {'selected': selected}}
        return {'protocol': declaration, 'protocol_sha256': optimizer.digest(declaration),
                'selections': selections, 'selection_frozen_sha256': optimizer.digest(selections),
                'status': 'complete', 'accepted': False, 'observed_trials': 0,
                'scheduled_trial_slots': 168, 'executed_task_observations': 0,
                'environment': {}, 'heldout': [], 'failures': ['mock matrix failure'],
                'datasets': [{'seed': seed, 'reference_sha256': '1' * 64}
                             for seed in probe.ENVELOPES['video']['heldout_seeds']]}

    def test_no_selection_keeps_three_explicit_unexecuted_fresh_slots(self):
        matrix = self.mock_matrix()
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(probe, 'source_hashes', return_value=SOURCES), \
             patch.object(optimizer, 'run', return_value=matrix), \
             patch.object(probe, 'run_fresh_process') as fresh:
            result = probe.run('video', Path(directory) / 'result.json')
        fresh.assert_not_called()
        self.assertEqual(result['expected_trial_slots'], 171)
        self.assertEqual(result['scheduled_trial_slots'], 171)
        self.assertEqual(result['observed_trials'], 0)
        self.assertEqual(len(result['fresh_selected']), 3)
        self.assertTrue(all(r['trial_executed'] is False and r['measured_callback_seconds'] is None
                            for r in result['fresh_selected']))

    def test_source_change_prevents_fresh_work_without_dropping_slots(self):
        matrix = self.mock_matrix(selected=asdict(Configuration('thread', 1, 1)))
        changed = dict(SOURCES, changed='changed')
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(probe, 'source_hashes', side_effect=[SOURCES, changed, changed]), \
             patch.object(optimizer, 'run', return_value=matrix), \
             patch.object(probe, 'run_fresh_process') as fresh:
            result = probe.run('video', Path(directory) / 'result.json')
        fresh.assert_not_called()
        self.assertFalse(result['source_unchanged_at_end'])
        self.assertEqual(len(result['fresh_selected']), 3)
        self.assertFalse(result['accepted'])

    def test_fresh_orchestration_failures_are_retained_with_unknown_execution(self):
        matrix = self.mock_matrix(selected=asdict(Configuration('thread', 1, 1)))
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(probe, 'source_hashes', return_value=SOURCES), \
             patch.object(optimizer, 'run', return_value=matrix), \
             patch.object(probe, 'run_fresh_process', side_effect=OSError('mock launch failure')) as fresh:
            result = probe.run('video', Path(directory) / 'result.json')
        self.assertEqual(fresh.call_count, 3)
        self.assertEqual(result['unknown_trial_execution_slots'], 3)
        self.assertEqual(result['observed_trials'], 0)
        self.assertTrue(all(r['measured_callback_seconds'] is None for r in result['fresh_selected']))

    def test_invalid_child_protocol_identity_is_not_accepted_as_measured_result(self):
        request = self.fresh_request()
        def popen(command, **kwargs):
            output = Path(command[command.index('--output') + 1])
            result = row(Configuration(**request['configuration']), request['seed'])
            result['fresh_context'] = {'source_sha256': SOURCES, 'protocol_sha256': 'wrong',
                                       'selection_frozen_sha256': request['selection_frozen_sha256']}
            output.write_text(json.dumps(result))
            return types.SimpleNamespace(returncode=0, wait=lambda timeout: 0)
        with patch.object(probe.subprocess, 'Popen', side_effect=popen):
            result = probe.run_fresh_process(request)
        self.assertIn('unavailable', result['failure'])
        self.assertIsNone(result['trial_executed'])
        self.assertIsNone(result['measured_callback_seconds'])
        self.assertNotIn('cold', result)

    def test_fresh_timeout_terminates_only_created_process_group_and_keeps_unknown_slot(self):
        process = Mock(pid=123456, returncode=-15)
        process.wait.side_effect = [subprocess.TimeoutExpired('mock', 900), None]
        with patch.object(probe.subprocess, 'Popen', return_value=process), \
             patch.object(probe.os, 'killpg', create=True) as kill_group:
            result = probe.run_fresh_process(self.fresh_request())
        kill_group.assert_called_once_with(123456, probe.signal.SIGTERM)
        self.assertIsNone(result['trial_executed'])
        self.assertIn('timed out', result['failure'])


if __name__ == '__main__':
    unittest.main()
