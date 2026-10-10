"""Mocked corpus/CLI checks: no PDF generation, OCR, pools or native tools run."""
from contextlib import redirect_stderr, redirect_stdout
import hashlib
import io
import json
from pathlib import Path
import random
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from tools import qualify_ocr_typography as qualifier


class FakePool:
    workers = []

    def __init__(self, max_workers):
        self.workers.append(max_workers)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def map(self, function, tasks):
        return map(function, tasks)


class QualificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.font_bytes = self.root / 'mock-font.ttf'
        self.font_bytes.write_bytes(b'synthetic font bytes for mocked tests only')
        self.font_hash = hashlib.sha256(self.font_bytes.read_bytes()).hexdigest()
        self.font = SimpleNamespace(truetype=Mock(return_value=SimpleNamespace(path=str(self.font_bytes))))
        self.media = SimpleNamespace(random=SimpleNamespace(Random=random.Random),
                                     OCR_PROFILES=qualifier.PROFILES,
                                     __file__=str(self.font_bytes))
        self.media.prepare = Mock(side_effect=self.prepare)
        self.media.run_task = Mock(side_effect=lambda task: {
            'text': task['expected_text'],
            'sha256': hashlib.sha256(task['expected_text'].encode()).hexdigest(),
        })
        FakePool.workers = []

    def prepare(self, name, workspace, count, seed, *, scale):
        self.assertEqual((name, count, scale), ('ocr', 120, 'representative'))
        profile = self.media.OCR_PROFILES[0]
        self.font.truetype('original-font.ttf', 42)
        tasks = []
        for index in range(count):
            rng = self.media.random.Random(f'media-v1:{seed}:{index}')
            pages = ['LOCAL DOCUMENT ' + ' '.join(rng.sample(qualifier.WORDS, 3))
                     + ' THE QUICK BROWN FOX READ EVERY WORD MOCK BODY'
                     for _ in range(profile[2])]
            tasks.append({'name': 'ocr', 'index': index, 'expected_pages': pages,
                          'expected_text': ' '.join(pages), 'page_count': profile[2],
                          'source_size': list(profile[:2]), 'font_name': qualifier.FONT,
                          'font_size': 42, 'font_sha256': self.font_hash})
        return tasks

    def run_mocked(self):
        with patch.object(qualifier, '_dependencies', return_value=(self.media, self.font)), \
                patch.object(qualifier.concurrent.futures, 'ThreadPoolExecutor', FakePool):
            return qualifier.qualify(self.root / 'new-workspace', self.root / 'new-report.json')

    def test_corrected_rng_uses_document_index_and_retains_heading_across_pages(self):
        constructor = qualifier.headline_random(random.Random)
        orders = []
        for index in range(120):
            rng = constructor(f'media-v1:6101317:{index}')
            values = [tuple(rng.sample(qualifier.WORDS, 3)) for _ in range(3)]
            self.assertEqual(values, [qualifier.ORDERS[index]] * 3)
            orders.append(values[0])
        self.assertEqual(len(set(orders)), 120)
        rng = constructor('media-v1:6101317:11')
        ordinary = random.Random('media-v1:6101317:11')
        self.assertEqual(rng.sample(['a', 'b', 'c'], 2), ordinary.sample(['a', 'b', 'c'], 2))

    def test_bad_document_indices_or_heading_request_fail(self):
        constructor = qualifier.headline_random(random.Random)
        for seed in ('media-v1:6101317:-1', 'media-v1:6101317:120', 5, 'unversioned:3'):
            with self.subTest(seed=seed), self.assertRaises(ValueError):
                constructor(seed)
        with self.assertRaises(ValueError):
            constructor('media-v1:6101317:0').sample(qualifier.WORDS, 2)

    def test_mocked_full_480_coverage_and_exact_truth(self):
        report = self.run_mocked()
        self.assertTrue(report['accepted'])
        self.assertEqual(report['status'], 'complete')
        self.assertEqual(len(report['rows']), 480)
        self.assertEqual(self.media.run_task.call_count, 480)
        self.assertEqual(FakePool.workers, [4, 4, 4, 4])
        self.assertEqual([p['unique_orders'] for p in report['verified_profile_orders']], [120] * 4)
        self.assertEqual(len({(row['profile_index'], row['index']) for row in report['rows']}), 480)
        self.assertEqual(report['plan']['font_sha256'], self.font_hash)
        self.assertEqual(self.media.random.Random, random.Random)
        self.assertEqual(self.media.OCR_PROFILES, qualifier.PROFILES)
        self.assertEqual(json.loads((self.root / 'new-report.json').read_text())['status'], 'complete')
        self.assertIn('not confirmatory performance', report['plan']['scope'])
        self.assertIn('No CPU/RSS measurement', report['resource_scope'])

    def test_v1_repeated_first_heading_is_rejected_before_any_ocr(self):
        def repeated(*args, **kwargs):
            tasks = self.prepare(*args, **kwargs)
            for task in tasks:
                task['expected_pages'] = [tasks[0]['expected_pages'][0]] * task['page_count']
                task['expected_text'] = ' '.join(task['expected_pages'])
            return tasks
        self.media.prepare.side_effect = repeated
        with self.assertRaises(AssertionError):
            self.run_mocked()
        self.media.run_task.assert_not_called()
        self.assertEqual(FakePool.workers, [])
        self.assertEqual(json.loads((self.root / 'new-report.json').read_text())['status'], 'failed')

    def test_all_profiles_are_verified_before_first_worker_execution(self):
        def late_failure(*args, **kwargs):
            tasks = self.prepare(*args, **kwargs)
            if self.media.OCR_PROFILES[0] == qualifier.PROFILES[-1]:
                tasks.pop()
            return tasks
        self.media.prepare.side_effect = late_failure
        with self.assertRaises(AssertionError):
            self.run_mocked()
        self.assertEqual(self.media.prepare.call_count, 4)
        self.media.run_task.assert_not_called()
        self.assertEqual(FakePool.workers, [])

    def test_metadata_or_page_truth_mismatch_is_rejected_before_execution(self):
        for field, value in (('expected_text', 'wrong'), ('page_count', 99),
                             ('source_size', [1, 1]), ('font_sha256', '0' * 64),
                             ('font_name', 'other.ttf'), ('index', 119)):
            with self.subTest(field=field):
                self.media.OCR_PROFILES = (qualifier.PROFILES[0],)
                with patch.object(self.media.random, 'Random', qualifier.headline_random(random.Random)):
                    tasks = self.prepare('ocr', self.root, 120, qualifier.SEED, scale='representative')
                tasks[0][field] = value
                with self.assertRaises(AssertionError):
                    qualifier.verify_profile(tasks, 0, qualifier.PROFILES[0], self.font_hash)
                self.media.OCR_PROFILES = qualifier.PROFILES
        self.media.run_task.assert_not_called()

    def test_ordinary_ocr_errors_are_retained_and_acceptance_fails(self):
        self.media.run_task.side_effect = RuntimeError('synthetic OCR failure')
        report = self.run_mocked()
        self.assertFalse(report['accepted'])
        self.assertEqual(report['status'], 'complete')
        self.assertEqual(len(report['rows']), 480)
        self.assertTrue(all(row['error'] == 'RuntimeError: synthetic OCR failure' for row in report['rows']))
        self.assertTrue(all(row['sha256'] is None for row in report['rows']))

    def test_result_text_cannot_silently_change_truth(self):
        self.media.run_task.side_effect = None
        self.media.run_task.return_value = {'text': 'DELIA', 'sha256': '0' * 64}
        report = self.run_mocked()
        self.assertFalse(report['accepted'])
        self.assertTrue(all(row['error'].startswith('ValueError: OCR result differs') for row in report['rows']))

    def test_generation_failure_restores_all_monkeypatches(self):
        original_font = self.font.truetype
        self.media.prepare.side_effect = RuntimeError('synthetic preparation failure')
        with self.assertRaises(RuntimeError):
            self.run_mocked()
        self.assertIs(self.media.random.Random, random.Random)
        self.assertEqual(self.media.OCR_PROFILES, qualifier.PROFILES)
        self.assertIs(self.font.truetype, original_font)
        self.media.run_task.assert_not_called()

    def test_new_workspace_output_and_distinct_destinations_are_required(self):
        existing = self.root / 'existing'
        existing.mkdir()
        output = self.root / 'existing.json'
        output.write_text('KEEP')
        with patch.object(qualifier, '_dependencies') as deps:
            for workspace, report, exception in ((existing, self.root / 'other.json', FileExistsError),
                                                 (self.root / 'new', output, FileExistsError),
                                                 (self.root / 'same', self.root / 'same', ValueError)):
                with self.subTest(workspace=workspace), self.assertRaises(exception):
                    qualifier.qualify(workspace, report)
            deps.assert_not_called()
        self.assertEqual(output.read_text(), 'KEEP')

    def test_cli_requires_explicit_paths_and_does_not_run_during_help(self):
        for argv in ([], ['--workspace', 'new'], ['--output', 'new.json']):
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
                qualifier.argument_parser().parse_args(argv)
            self.assertEqual(caught.exception.code, 2)
        with patch.object(qualifier, '_dependencies') as deps, redirect_stdout(io.StringIO()), \
                self.assertRaises(SystemExit) as caught:
            qualifier.main(['--help'])
        self.assertEqual(caught.exception.code, 0)
        deps.assert_not_called()
        with patch.object(qualifier, 'qualify', return_value={
                'status': 'complete', 'accepted': False, 'rows': [], 'plan_sha256': 'x'}) as run, \
                redirect_stdout(io.StringIO()):
            self.assertEqual(qualifier.main(['--workspace', 'new', '--output', 'new.json']), 1)
        run.assert_called_once_with(Path('new'), Path('new.json'))


if __name__ == '__main__':
    unittest.main()
