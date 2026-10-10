"""Serial process-group watchdog for the explicitly bounded optimizer experiment."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import psutil
from . import NAMES


def supervise(name, output, *, affinity=8, wall_limit=21600., stall_limit=600., rss_limit=2*1024**3):
    output.mkdir(parents=True, exist_ok=True)
    result = output / (name+'.json')
    log = output / (name+'.log')
    status = {'workload':name, 'limits':{'wall_seconds':wall_limit, 'checkpoint_stall_seconds':stall_limit,
        'emergency_process_tree_rss_bytes':rss_limit, 'cpu_affinity_count':affinity},
        'started_unix':time.time(), 'status':'running'}
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[2]/'src'),
        OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1')
    command = [sys.executable,'-m','benchmarks.real_world.optimizer','--workload',name,
        '--output',str(result),'--affinity',str(affinity)]
    with log.open('w') as stream:
        process = subprocess.Popen(command,stdout=stream,stderr=subprocess.STDOUT,
                                   env=env,start_new_session=True)
        started=time.monotonic()
        peak=0
        reason=None
        while process.poll() is None:
            now=time.monotonic()
            try:
                parent=psutil.Process(process.pid)
                rss=0
                for child in [parent,*parent.children(recursive=True)]:
                    try:
                        rss += child.memory_info().rss
                    except psutil.NoSuchProcess:
                        pass
                    except psutil.AccessDenied:
                        reason='supervisor_rss_access_denied'
                        break
                peak=max(peak,rss)
                if rss > rss_limit:
                    reason='emergency_rss_limit_exceeded'
            except psutil.NoSuchProcess:
                pass
            except Exception as exc:
                reason='supervisor_monitor_error:'+type(exc).__name__
            if now-started>wall_limit:
                reason='workload_wall_limit_exceeded'
            checkpoint_age=time.time()-result.stat().st_mtime if result.exists() else now-started
            if checkpoint_age>stall_limit:
                reason='checkpoint_stall_limit_exceeded'
            if reason:
                os.killpg(process.pid,signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid,signal.SIGKILL)
                    process.wait()
                break
            time.sleep(1)
        status.update(status='complete' if process.returncode==0 else 'failed',
            returncode=process.returncode, stop_reason=reason,
            elapsed_seconds=time.monotonic()-started, sampled_peak_tree_rss_bytes=peak)
    (output/(name+'-supervisor.json')).write_text(json.dumps(status,indent=2)+'\n')
    return status


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workloads',nargs='+',choices=NAMES,default=list(NAMES))
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--affinity',type=int,default=8)
    args=parser.parse_args()
    results=[]
    for name in args.workloads:
        print('START',name,flush=True)
        result=supervise(name,args.output,affinity=args.affinity)
        results.append(result)
        print(json.dumps(result),flush=True)
        (args.output/'supervisor.json').write_text(json.dumps(results,indent=2)+'\n')
        if result['status']!='complete':
            return 1
    return 0

if __name__=='__main__':
    raise SystemExit(main())
