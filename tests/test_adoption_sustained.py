"""Deterministic invariants for the standalone sustained experiment."""
from concurrent.futures import Future, CancelledError
from copy import deepcopy
from dataclasses import asdict
import math
import json
import importlib.util
import os
import subprocess
import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from benchmarks.real_world import adoption_sustained as sustained
from intelligent_futures.evaluation import EvaluationPlan, _median_lower


def row(seed, seconds=1., latency=.2):
    return {'seed': seed, 'correct': True, 'exclusions': [],
            'lifecycle_cpu_seconds': 2., 'peak_rss_bytes': 1024,
            'lifecycle_seconds': 2., 'warm': {'seconds': seconds,
                'latency': {name: {'p95': latency} for name in ('all', 'geospatial', 'http')}}}


class ComparisonTests(unittest.TestCase):
    def test_exact_sign_boundary_and_builtin_interval_parity(self):
        seeds = list(range(24))
        baseline = [row(seed, 1.1 if seed < 18 else 1.04) for seed in seeds]
        candidate = [row(seed) for seed in seeds]
        result = sustained.compare('static', baseline, candidate, seeds)
        self.assertTrue(result['passed'])
        self.assertEqual(result['wins_above_margin'], 18)
        self.assertEqual(result['lower_order_statistic'], 7)
        lower, k = _median_lower([math.log(r['warm']['seconds']) for r in baseline], .0125)
        self.assertAlmostEqual(result['lower_speedup'], math.expm1(lower))
        self.assertEqual(k, 7)
        baseline[17]['warm']['seconds'] = 1.04
        self.assertFalse(sustained.compare('static', baseline, candidate, seeds)['passed'])

    def test_margin_ties_and_rounding_never_pass(self):
        seeds = list(range(24))
        for gain in (1.05, math.nextafter(1.05, math.inf)):
            result = sustained.compare('fixed', [row(s, gain) for s in seeds], [row(s) for s in seeds], seeds)
            self.assertFalse(result['passed'])
            self.assertEqual(result['wins_above_margin'], 0)

    def test_missing_duplicate_and_reordered_pairs_fail(self):
        seeds = list(range(24))
        baseline = [row(s, 1.2) for s in seeds]
        candidate = [row(s) for s in seeds]
        for bad in (baseline[:-1], baseline[::-1], baseline[:-1] + [baseline[0]]):
            self.assertEqual(sustained.compare('fixed', bad, candidate, seeds)['reasons'], ['incomplete_or_failed_heldout'])

    def test_numeric_failures_and_baseline_exclusions_fail_closed(self):
        seeds = list(range(24))
        for value in (None, 0., -1., float('nan'), float('inf'), True, '1'):
            baseline = [row(s, 1.2) for s in seeds]
            candidate = [row(s) for s in seeds]
            candidate[3]['warm']['seconds'] = value
            self.assertFalse(sustained.compare('fixed', baseline, candidate, seeds)['passed'])
        baseline = [row(s, 1.2) for s in seeds]
        baseline[5]['peak_rss_bytes'] = None
        self.assertFalse(sustained.compare('fixed', baseline, [row(s) for s in seeds], seeds)['passed'])
        baseline[5] = row(5, 1.2)
        baseline[5]['exclusions'] = ['rss_cap']
        self.assertFalse(sustained.compare('fixed', baseline, [row(s) for s in seeds], seeds)['passed'])

    def test_extreme_timing_ratios_fail_without_overflow(self):
        seeds = list(range(24))
        for b, c in ((1e308, 1e-308), (1e-308, 1e308)):
            result = sustained.compare('fixed', [row(s, b) for s in seeds], [row(s, c) for s in seeds], seeds)
            self.assertFalse(result['passed'])

    def test_latency_gate_and_aliases_keep_alpha(self):
        seeds = list(range(24))
        baseline = [row(s, 1.2) for s in seeds]
        candidate = [row(s, 1., .3) for s in seeds]
        for role in ('fixed', 'single', 'static', 'overflow'):
            result = sustained.compare(role, baseline, candidate, seeds)
            self.assertEqual(result['comparison_alpha'], .0125)
            self.assertFalse(result['passed'])
            self.assertIn('http:observed_p95_regression', result['reasons'])

    def test_quantile_weights_each_logical_item(self):
        self.assertEqual(sustained.percentile(list(range(1, 21)), .95), 19)
        self.assertEqual(sustained.percentile([1, 1, 1, 10], .95), 10)


class FixturePlanTests(unittest.TestCase):
    @staticmethod
    def jobs(number):
        return {'geospatial': [{'id': f'g{i}', 'family': 'geospatial', 'key': f'g{i % 3}', 'task': i} for i in range(number)],
                'http': [{'id': f'h{i}', 'family': 'http', 'key': f'h{i % 6}', 'task': i} for i in range(number)]}

    def test_arrival_is_reproducible_complete_and_epoch_mix_frozen(self):
        jobs = self.jobs(128)
        counts = ((48, 16), (16, 48), (48, 16), (16, 48))
        stream = sustained.arrival_stream(jobs, counts, 22)
        self.assertEqual(stream, sustained.arrival_stream(jobs, counts, 22))
        self.assertEqual(len({j['id'] for j in stream}), 256)
        for index, (graphs, _) in enumerate(counts):
            self.assertEqual(sum(j['family'] == 'geospatial' for j in stream[index * 64:(index + 1) * 64]), graphs)
        for size in (1, 4):
            batches = sustained.group_batches(stream, size)
            self.assertEqual(tuple(j for batch in batches for j in batch), stream)
            self.assertTrue(all(1 <= len(batch) <= size for batch in batches))
            self.assertTrue(all(len({(j['key'], j['family']) for j in batch}) == 1 for batch in batches))

    def test_seed_sets_and_equal_worker_grid_budgets(self):
        manifest = sustained.seed_manifest()
        values = manifest['development'] + manifest['training'] + manifest['heldout']
        self.assertEqual(len(values), len(set(values)))
        grid = sustained.grids()
        self.assertEqual([len(grid[k]) for k in ('single', 'static', 'overflow', 'intelligent')], [8] * 4)
        for c in grid['intelligent']:
            self.assertIn(sustained.Configuration('stdlib', 'static', c.threads, c.processes, c.batch), grid['static'])
            self.assertIn(sustained.Configuration('stdlib', 'overflow', c.threads, c.processes, c.batch), grid['overflow'])
            for route in ('thread', 'process'):
                workers = c.threads + c.processes
                equivalent = sustained.Configuration('stdlib', route, workers if route == 'thread' else 0,
                                                       workers if route == 'process' else 0, c.batch)
                self.assertIn(equivalent, grid['single'])

    def test_no_fixture_work_without_both_consents(self):
        config = sustained.grids()['intelligent'][0]
        with patch.object(sustained, 'server_process') as server:
            for a, b in ((False, False), (True, False), (False, True)):
                with self.assertRaises(ValueError):
                    sustained.observe(config, 1, allow_repeatable=a, allow_process=b)
            server.assert_not_called()

    def test_window_bounded_completion_and_result_oracle(self):
        stream = sustained.arrival_stream(self.jobs(40), ((40, 40),), 123)
        class Pool:
            def submit(self, batch):
                future = Future()
                future.set_result({'values': tuple((j['id'], j['task']) for j in batch),
                                   'pid': 1, 'thread_id': 2, 'service_seconds': .01})
                return future
        for batch in (1, 4):
            result = sustained.execute_stream(Pool(), sustained.Configuration('stdlib', 'thread', 4, 0, batch), stream)
            self.assertEqual(result['logical_items'], 80)
            self.assertLessEqual(result['peak_logical_pending'], 32)
            self.assertEqual(result['digest'], sustained.digest(tuple((j['id'], j['task']) for j in stream)))
            self.assertEqual(len(result['trace']), 80)

    def test_task_cancellation_propagates(self):
        stream = sustained.arrival_stream(self.jobs(1), ((1, 1),), 123)
        class Pool:
            def submit(self, batch):
                future = Future()
                future.set_exception(CancelledError())
                return future
        with self.assertRaises(CancelledError):
            sustained.execute_stream(Pool(), sustained.Configuration('stdlib', 'thread', 4, 0, 1), stream)


class FailureAccountingTests(unittest.TestCase):
    def test_resource_and_training_time_validation(self):
        for field in ('lifecycle_cpu_seconds', 'peak_rss_bytes', 'lifecycle_seconds'):
            for value in (None, -1, float('nan'), float('inf'), True, 'bad'):
                observation = row(1)
                observation[field] = value
                self.assertTrue(sustained.exclusions(observation))
        for value in (0., .0005, None):
            observation = row(1, value)
            self.assertTrue(sustained.exclusions(observation))
        observation = row(1)
        observation['warm']['latency']['http']['p95'] = 5.1
        self.assertIn('absolute_p95_cap', sustained.exclusions(observation))

    def test_actual_input_digest_ignores_paths_but_detects_content_and_parameters(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            a, b = root / 'a.json', root / 'b.json'
            a.write_text('{"graph": 1}')
            b.write_text(a.read_text())
            base = [{'id': 'g', 'family': 'geospatial', 'key': 'g1',
                     'task': {'path': str(a), 'origins': ['a'], 'destinations': ['b']}}]
            copy = deepcopy(base)
            copy[0]['task']['path'] = str(b)
            self.assertEqual(sustained.fixture_digest(base), sustained.fixture_digest(copy))
            b.write_text('{"graph": 2}')
            self.assertNotEqual(sustained.fixture_digest(base), sustained.fixture_digest(copy))
            b.write_text(a.read_text())
            copy[0]['task']['origins'] = ['c']
            self.assertNotEqual(sustained.fixture_digest(base), sustained.fixture_digest(copy))

    def test_interrupted_launched_slot_is_retained_and_counted(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'campaign'
            with patch.object(sustained, 'source_identity', return_value={'test': 1}), \
                 patch.object(sustained, 'workload_environment', return_value={}), \
                 patch.object(sustained, 'supervise_observation', side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    sustained.run_campaign(target, allow_repeatable=True, allow_process=True, development=True)
            report = json.loads((target / 'report.json').read_text())
            self.assertEqual(report['status'], 'interrupted_or_failed')
            self.assertEqual(report['attempted_observations'], 1)
            self.assertEqual(report['settled_observations'], 1)
            self.assertEqual(report['failed_observations'], 1)
            self.assertEqual(report['skipped_observations'], 7)
            self.assertEqual(len((target / 'records.jsonl').read_text().splitlines()), 1)
            self.assertEqual(len((target / 'slot-ledger.jsonl').read_text().splitlines()), 2)

    @unittest.skipUnless(hasattr(os, 'killpg') and importlib.util.find_spec('psutil') is not None, 'POSIX watchdog requires optional psutil')
    def test_state_write_failure_after_launch_does_not_strand_group(self):
        original_popen, original_write = subprocess.Popen, sustained._write
        launched = []
        def launch(*args, **kwargs):
            process = original_popen([sys.executable, '-c', 'import time; time.sleep(10)'], **kwargs)
            launched.append(process)
            return process
        def fail_state(path, value):
            if str(path).endswith('-supervisor-state.json'):
                raise OSError('injected disk failure')
            return original_write(path, value)
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(sustained.subprocess, 'Popen', side_effect=launch), \
                 patch.object(sustained, '_write', side_effect=fail_state):
                with self.assertRaises(OSError):
                    sustained.supervise_observation(sustained.grids()['single'][0], 1, directory, 'disk-failure')
            self.assertIsNotNone(launched[0].poll())
            evidence = json.loads((Path(directory) / 'disk-failure-exception.json').read_text())
            self.assertTrue(evidence['cleanup'][0]['post_cleanup_verified'])
            with self.assertRaises(ProcessLookupError):
                os.killpg(launched[0].pid, 0)

    @unittest.skipUnless(hasattr(os, 'killpg') and importlib.util.find_spec('psutil') is not None, 'POSIX watchdog requires optional psutil')
    def test_timeout_preserves_partial_child_file_and_verifies_cleanup(self):
        original_popen = subprocess.Popen
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / 'timeout.json'
            def launch(*args, **kwargs):
                command = [sys.executable, '-c',
                    'from pathlib import Path; import time; '
                    + 'Path(' + repr(str(target)) + ').write_text("partial child output"); time.sleep(10)']
                return original_popen(command, **kwargs)
            with patch.object(sustained.subprocess, 'Popen', side_effect=launch):
                result = sustained.supervise_observation(sustained.grids()['single'][0], 1, root,
                                                         'timeout', timeout=.25)
            self.assertFalse(result['correct'])
            self.assertIn('supervisor_observation_timeout', result['exclusions'])
            self.assertEqual(target.read_text(), 'partial child output')
            self.assertTrue(result['supervisor']['post_cleanup_verified'])
            self.assertEqual(result['supervisor']['survivors'], [])
            self.assertTrue((root / 'timeout-supervisor.json').exists())


if __name__ == '__main__':
    unittest.main()
