"""Watchdog and resource-trend checks; full endurance is a separate campaign."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]

def load(name):
    spec=importlib.util.spec_from_file_location(name,ROOT/'tools'/(name+'.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module

@unittest.skipUnless(os.name=='posix' and importlib.util.find_spec('psutil') is not None,
                     'Linux/POSIX watchdog tooling requires optional psutil')
class OperationalToolTests(unittest.TestCase):
    def supervise(self,code,*,seconds=5,stall=4):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            command=[sys.executable,str(ROOT/'tools/supervise_endurance.py'),
                '--seconds',str(seconds),'--stall-seconds',str(stall),
                '--checkpoint',str(root/'progress'),'--report',str(root/'report.json'),
                '--',sys.executable,'-c',code]
            completed=subprocess.run(command,text=True,capture_output=True,timeout=25)
            return completed,json.loads((root/'report.json').read_text())

    def test_clean_completion_and_census(self):
        completed,result=self.supervise('print("done")')
        self.assertEqual(completed.returncode,0,completed.stderr)
        self.assertEqual(result['status'],'passed')
        self.assertEqual(result['remaining_owned_processes'],[])

    def test_nonzero_child_is_failed(self):
        completed,result=self.supervise('raise SystemExit(7)')
        self.assertNotEqual(completed.returncode,0)
        self.assertEqual(result['status'],'failed')
        self.assertEqual(result['returncode'],7)

    @unittest.skipUnless(os.name=='posix','POSIX owned process-group watchdog')
    def test_wall_stop_remains_failure_even_if_term_handler_exits_zero(self):
        completed,result=self.supervise(
            'import signal,time,sys;signal.signal(signal.SIGTERM,lambda *a:sys.exit(0));time.sleep(20)',
            seconds=1,stall=4)
        self.assertNotEqual(completed.returncode,0)
        self.assertEqual(result['returncode'],0)
        self.assertEqual(result['status'],'failed')
        self.assertEqual(result['stop_reason'],'wall_limit')
        self.assertEqual(result['remaining_owned_processes'],[])

    def test_stale_progress_is_failure(self):
        completed,result=self.supervise('import time;time.sleep(20)',seconds=5,stall=0.5)
        self.assertNotEqual(completed.returncode,0)
        self.assertEqual(result['stop_reason'],'checkpoint_stall')

    @unittest.skipUnless(os.name=='posix','POSIX owned process-group watchdog')
    def test_timeout_cleans_child_that_survives_its_leader(self):
        import psutil
        with tempfile.TemporaryDirectory() as directory:
            pidfile=Path(directory)/'descendant.pid'
            descendant='import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(60)'
            code=('import subprocess,sys,time;'
                  f'child=subprocess.Popen([sys.executable,"-c",{descendant!r}]);'
                  f'open({str(pidfile)!r},"w").write(str(child.pid));time.sleep(20)')
            completed,result=self.supervise(code,seconds=1,stall=4)
            self.assertNotEqual(completed.returncode,0)
            self.assertEqual(result['stop_reason'],'wall_limit')
            pid=int(pidfile.read_text())
            try: remaining=psutil.Process(pid)
            except psutil.NoSuchProcess: pass
            else:
                # An orphan zombie has no executing code; the supervisor must
                # still disclose it rather than call the census empty.
                self.assertEqual(remaining.status(),psutil.STATUS_ZOMBIE)
                self.assertTrue(any(p['pid']==pid for p in result['remaining_owned_processes']))

    @unittest.skipUnless(os.name=='posix','POSIX supervisor interruption')
    def test_supervisor_interrupt_cleans_and_persists_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);marker=root/'started'
            code=f'import pathlib,time;pathlib.Path({str(marker)!r}).touch();time.sleep(20)'
            command=[sys.executable,str(ROOT/'tools/supervise_endurance.py'),
                '--seconds','10','--stall-seconds','9','--checkpoint',str(root/'progress'),
                '--report',str(root/'report.json'),'--',sys.executable,'-c',code]
            process=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            try:
                deadline=time.monotonic()+5
                while not marker.exists() and time.monotonic()<deadline:time.sleep(0.01)
                self.assertTrue(marker.exists())
                process.send_signal(signal.SIGINT)
                stdout,stderr=process.communicate(timeout=20)
                result=json.loads((root/'report.json').read_text())
                self.assertNotEqual(process.returncode,0,stdout+stderr)
                self.assertEqual(result['stop_reason'],'supervisor_error:KeyboardInterrupt')
                self.assertEqual(result['remaining_owned_processes'],[])
            finally:
                if process.poll() is None:
                    process.kill();process.wait()

    def test_optimized_python_is_refused_before_creating_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            target=Path(directory)/'evidence'
            result=subprocess.run([sys.executable,'-O',str(ROOT/'tools/operational_endurance.py'),
                '--output',str(target),'--mode','smoke','--seconds','1'],
                text=True,capture_output=True,timeout=10)
            self.assertNotEqual(result.returncode,0)
            self.assertIn('optimized Python disables required assertions',result.stderr)
            self.assertFalse(target.exists())

    def test_resource_heartbeat_does_not_refresh_productive_checkpoint(self):
        module=load('operational_endurance')
        with tempfile.TemporaryDirectory() as directory:
            evidence=module.Evidence(Path(directory)/'evidence')
            try:
                evidence.emit('workload_progress',sequence=1)
                before=(evidence.directory/'checkpoint.json').read_bytes()
                evidence.emit('active_sample',sample=2)
                self.assertEqual((evidence.directory/'checkpoint.json').read_bytes(),before)
            finally:evidence.stream.close()

    def test_group_census_does_not_hide_access_denied(self):
        module=load('supervise_endurance')
        import psutil
        with patch.object(module.psutil,'process_iter',side_effect=psutil.AccessDenied(123)):
            with self.assertRaises(psutil.AccessDenied):module.group_members(123)

    def test_linear_resource_slope(self):
        module=load('operational_endurance')
        rows=[{'elapsed_seconds':i*3600,'census':{'rss':100+4*i}} for i in range(4)]
        self.assertEqual(module.slope(rows,'rss'),4)
        self.assertIsNone(module.slope(rows[:1],'rss'))

if __name__=='__main__':unittest.main()
