import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
try:
    from tools import supervise_sustained as supervisor
except ModuleNotFoundError as error:
    if error.name != 'psutil':
        raise
    supervisor = None


@unittest.skipUnless(hasattr(os, 'killpg') and supervisor is not None, 'POSIX supervisor requires optional psutil')
class SupervisorTests(unittest.TestCase):
    def run_case(self, code, *, seconds=1):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            checkpoint = root / 'checkpoint'
            checkpoint.touch()
            return supervisor.supervise([sys.executable, '-c', code], seconds=seconds,
                rss_bytes=1024**3, checkpoint=checkpoint, report=root / 'report.json', stall_seconds=5)

    def test_normal_zero_exit_passes(self):
        result = self.run_case('pass')
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(result['remaining_live_owned_processes'], [])

    def test_nested_session_term_resistant_child_is_cleaned(self):
        code = ('import subprocess, sys, time; '
                'subprocess.Popen([sys.executable,"-c",'
                '"import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(60)"],start_new_session=True); '
                'time.sleep(60)')
        result = self.run_case(code, seconds=.5)
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['stop_reason'], 'wall_limit')
        self.assertGreaterEqual(len(result['observed_groups']), 2)
        self.assertEqual(result['remaining_live_owned_processes'], [])

    def test_timeout_stays_failed_after_zero_term_handler(self):
        code = 'import signal,time,sys; signal.signal(signal.SIGTERM,lambda *a:sys.exit(0)); time.sleep(60)'
        result = self.run_case(code, seconds=.5)
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['stop_reason'], 'wall_limit')
        self.assertEqual(result['returncode'], 0)

    def test_sampling_failure_cleans_spawned_runner(self):
        original = supervisor.psutil.Process.memory_info
        with patch.object(supervisor.psutil.Process, 'memory_info', side_effect=PermissionError):
            result = self.run_case('import time; time.sleep(60)')
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['remaining_live_owned_processes'], [])

    def test_census_and_signal_errors_do_not_skip_failure_report(self):
        original_killpg = supervisor.os.killpg
        def kill_then_error(group, sig):
            original_killpg(group, sig)
            raise PermissionError('injected post-signal error')
        with patch.object(supervisor.psutil, 'process_iter', side_effect=RuntimeError), \
             patch.object(supervisor.psutil.Process, 'kill', side_effect=supervisor.psutil.AccessDenied), \
             patch.object(supervisor.os, 'killpg', side_effect=kill_then_error):
            result = self.run_case('import time; time.sleep(60)')
        self.assertEqual(result['status'], 'failed')
        self.assertIsNone(result['remaining_live_owned_processes'])
        self.assertFalse(result['cleanup_census_complete'])
        self.assertTrue(any('identity_kill:' in error for error in result['errors']))
        self.assertTrue(any('group_kill:' in error for error in result['errors']))


if __name__ == '__main__':
    unittest.main()
