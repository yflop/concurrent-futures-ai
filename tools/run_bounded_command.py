"""Serial child-process wall/RSS/checkpoint watchdog for owned local experiments."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import psutil


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds',type=float,required=True)
    parser.add_argument('--max-rss-mib',type=int,default=2048)
    parser.add_argument('--checkpoint',type=Path)
    parser.add_argument('--stall-seconds',type=float,default=180)
    parser.add_argument('--report',type=Path,required=True)
    parser.add_argument('command',nargs=argparse.REMAINDER)
    args=parser.parse_args();command=args.command
    if command and command[0]=='--':command=command[1:]
    if not command or not 1<=args.seconds<=20000 or not 64<=args.max_rss_mib<=4096:
        parser.error('bounded command/time/memory required')
    start=time.monotonic();peak=0;reason=None
    process=subprocess.Popen(command,start_new_session=True)
    while process.poll() is None:
        elapsed=time.monotonic()-start
        try:
            parent=psutil.Process(process.pid);rss=0
            for item in [parent,*parent.children(recursive=True)]:
                try:rss+=item.memory_info().rss
                except psutil.NoSuchProcess:pass
            peak=max(peak,rss)
            if rss>args.max_rss_mib*1024**2:reason='sampled_emergency_rss_limit'
        except psutil.NoSuchProcess:pass
        except Exception as exc:reason='resource_monitor_error:'+type(exc).__name__
        if elapsed>args.seconds:reason='wall_limit'
        if args.checkpoint:
            age=time.time()-args.checkpoint.stat().st_mtime if args.checkpoint.exists() else elapsed
            if age>args.stall_seconds:reason='checkpoint_stall'
        if reason:
            try:os.killpg(process.pid,signal.SIGTERM)
            except ProcessLookupError:pass
            try:process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid,signal.SIGKILL);process.wait()
            break
        time.sleep(.25)
    report={'status':'complete' if process.returncode==0 and reason is None else 'failed',
        'returncode':process.returncode,'stop_reason':reason,'elapsed_seconds':time.monotonic()-start,
        'sampled_peak_process_tree_rss_bytes':peak,'wall_limit_seconds':args.seconds,
        'emergency_sampled_rss_limit_bytes':args.max_rss_mib*1024**2,
        'scope':'supervisory sampled limits, not a hard OS memory cap'}
    args.report.parent.mkdir(parents=True,exist_ok=True);args.report.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report),flush=True)
    return 0 if report['status']=='complete' else 1

if __name__=='__main__':raise SystemExit(main())
