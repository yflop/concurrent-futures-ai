"""The CI evidence gate must never promote an incomplete experiment."""
import copy
import importlib.util
import signal
import subprocess
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, MagicMock

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('ci_endurance', ROOT / 'tools/ci_endurance.py')
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)
PLAN = json.loads((ROOT / 'benchmarks/results/operational-endurance-github-v1-plan.json').read_text())


class CiEnduranceTests(unittest.TestCase):
    def values(self):
        summary = {'status': 'passed', 'mode': 'endurance', 'required_workload_seconds': 14400,
                   'workload_seconds': 14400.1, 'diagnostics': [{'status': 'passed'} for _ in range(16)],
                   'identity': {'runtime_sha256': PLAN['runtime_files'],
                                'harness_sha256': PLAN['frozen_files']['tools/operational_endurance.py'],
                                'protocol_sha256': PLAN['frozen_files']['docs/OPERATIONAL-ENDURANCE-PROTOCOL.md']}}
        watchdog = {'status': 'passed', 'returncode': 0, 'stop_reason': None, 'remaining_owned_processes': [],
                    'errors': [], 'wall_limit_seconds': 15000, 'sampled_rss_limit_bytes': 1024**3,
                    'progress_limit_seconds': 90}
        return summary, watchdog

    def test_frozen_source_identity(self):
        self.assertEqual(ci.source_identity(PLAN)['runtime_manifest_sha256'], PLAN['runtime_manifest_sha256'])

    def test_complete_native_reports_pass(self):
        ci.validate_terminal(*self.values(), PLAN)

    def test_incomplete_duration_or_diagnostics_fail(self):
        for field, value in [('status', 'failed'), ('mode', 'smoke'), ('workload_seconds', 14399),
                             ('required_workload_seconds', 60), ('diagnostics', [{'status': 'passed'}] * 15),
                             ('diagnostics', [{'status': 'failed'}] * 16)]:
            with self.subTest(field=field, value=value):
                summary, watchdog = self.values()
                summary[field] = value
                with self.assertRaises(RuntimeError):
                    ci.validate_terminal(summary, watchdog, PLAN)

    def test_bad_identity_or_cleanup_fails(self):
        for field, value in [('status', 'failed'), ('returncode', 1), ('stop_reason', 'wall_limit'),
                             ('remaining_owned_processes', [{'pid': 123}]), ('errors', ['error']),
                             ('wall_limit_seconds', 16000), ('sampled_rss_limit_bytes', 2 * 1024**3),
                             ('progress_limit_seconds', 120)]:
            with self.subTest(field=field):
                summary, watchdog = self.values()
                watchdog[field] = value
                with self.assertRaises(RuntimeError):
                    ci.validate_terminal(summary, watchdog, PLAN)
        summary, watchdog = self.values()
        summary['identity'] = copy.deepcopy(summary['identity'])
        summary['identity']['runtime_sha256']['executor.py'] = 'different'
        with self.assertRaises(RuntimeError):
            ci.validate_terminal(summary, watchdog, PLAN)

    def test_missing_terminal_report_is_retained_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            with patch.dict(os.environ, {'ENDURANCE_OUTCOME': 'success', 'PREFLIGHT_OUTCOME': 'success',
                                         'TESTS_OUTCOME': 'success'}):
                self.assertEqual(ci.finalize(target, PLAN), 1)
            result = json.loads((target / 'ci-terminal.json').read_text())
            self.assertEqual(result['status'], 'failed')
            self.assertFalse(result['cleanup_verified_for_this_run'])

    def test_wrong_runtime_digest_fails(self):
        plan = copy.deepcopy(PLAN)
        plan['runtime_manifest_sha256'] = 'different'
        with self.assertRaises(RuntimeError):
            ci.source_identity(plan)


@unittest.skipUnless(importlib.util.find_spec('psutil') is not None, 'optional psutil required')
class ObserverFailureTests(unittest.TestCase):
    def child(self):
        child = MagicMock()
        child.pid = os.getpid()
        child.poll.return_value = None
        return child

    def test_progress_logging_error_cleans_and_fails(self):
        child = self.child()
        child.wait.side_effect = subprocess.TimeoutExpired('fake', 60)
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(ci.subprocess, 'Popen', return_value=child), \
                 patch.object(ci, 'report_progress', side_effect=BrokenPipeError('injected log error')), \
                 patch.object(ci, 'stop_supervisor', return_value={'synthetic': True}) as stop:
                self.assertEqual(ci.observe(Path(directory), PLAN), 1)
                stop.assert_called_once()
            result = json.loads((Path(directory) / 'observer-failure.json').read_text())
            self.assertEqual(result['failure']['error_type'], 'BrokenPipeError')

    def test_wait_error_cleans_and_fails(self):
        child = self.child()
        child.wait.side_effect = OSError('injected wait error')
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(ci.subprocess, 'Popen', return_value=child), \
                 patch.object(ci, 'stop_supervisor', return_value={'synthetic': True}) as stop:
                self.assertEqual(ci.observe(Path(directory), PLAN), 1)
                stop.assert_called_once()
            self.assertTrue((Path(directory) / 'observer-failure.json').is_file())

    def test_interruption_cannot_pass_when_child_exits_zero(self):
        child = self.child()
        handlers = {}
        def install(signum, handler):
            handlers[signum] = handler
            return signal.SIG_DFL
        def interrupted_wait(timeout):
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            return 0
        child.wait.side_effect = interrupted_wait
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(ci.subprocess, 'Popen', return_value=child), \
                 patch.object(ci.signal, 'signal', side_effect=install), \
                 patch.object(ci, 'stop_supervisor', return_value={'synthetic': True}) as stop:
                self.assertEqual(ci.observe(Path(directory), PLAN), 1)
                stop.assert_called_once()
            result = json.loads((Path(directory) / 'observer-failure.json').read_text())
            self.assertEqual(result['interrupt_signals'], [signal.SIGTERM])


if __name__ == '__main__':
    unittest.main()
