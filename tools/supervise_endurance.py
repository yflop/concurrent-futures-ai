"""Independent failure-inclusive wall/RSS/progress/process-group watchdog."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import traceback
import psutil


def group_members(group):
    """Fail closed on census permissions/errors; only disappearance is normal."""
    rows=[]
    for process in psutil.process_iter(['pid','create_time','status']):
        try:
            if os.getpgid(process.pid)==group:
                rows.append((process,process.create_time()))
        except (ProcessLookupError,psutil.NoSuchProcess):
            pass
    return rows


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--seconds',type=float,default=15000)
    p.add_argument('--rss-mib',type=int,default=1024)
    p.add_argument('--stall-seconds',type=float,default=90)
    p.add_argument('--checkpoint',type=Path,required=True)
    p.add_argument('--report',type=Path,required=True)
    p.add_argument('command',nargs=argparse.REMAINDER)
    args=p.parse_args();command=args.command
    if command and command[0]=='--':command=command[1:]
    if not command or not 1<=args.seconds<=30000 or not 64<=args.rss_mib<=4096 or not 0.1<=args.stall_seconds<=300:
        p.error('invalid envelope')
    start=time.monotonic();peak=0;reason=None;observed={};samples=0
    child=None;group=None;output=None;remaining=[];errors=[]
    def stop_owned(sig):
        # start_new_session makes the returned child PID the owned process group.
        if group is not None:
            try:os.killpg(group,sig)
            except ProcessLookupError:pass
    try:
        args.report.parent.mkdir(parents=True,exist_ok=True)
        output=args.report.with_suffix('.samples.jsonl').open('x')
        child=subprocess.Popen(command,start_new_session=True)
        group=child.pid
        while child.poll() is None:
            elapsed=time.monotonic()-start
            members=group_members(group);rss=0
            for member,created in members:
                observed[(member.pid,created)]=True
                try:rss+=member.memory_info().rss
                except psutil.NoSuchProcess:pass
            peak=max(peak,rss);samples+=1
            output.write(json.dumps({'elapsed_seconds':elapsed,'rss_bytes':rss,
                'members':[{'pid':m.pid,'created':c} for m,c in members]})+'\n');output.flush()
            if rss>args.rss_mib*1024**2:reason='sampled_emergency_rss_limit'
            if elapsed>args.seconds:reason=reason or 'wall_limit'
            try:age=time.time()-args.checkpoint.stat().st_mtime
            except FileNotFoundError:age=elapsed
            if age>args.stall_seconds:reason=reason or 'checkpoint_stall'
            if reason:break
            time.sleep(0.5)
    except BaseException as error:
        reason=reason or 'supervisor_error:'+type(error).__name__
        errors.append(traceback.format_exc())
    finally:
        # Cleanup runs even after interruption, failed sampling/stat, or disk I/O.
        if child is not None:
            try:
                if reason or child.poll() is None:
                    stop_owned(signal.SIGTERM)
                    try:child.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        stop_owned(signal.SIGKILL);child.wait(timeout=10)
                else:
                    child.wait(timeout=10)
                # Normal resource-tracker exit gets a small finite grace period.
                deadline=time.monotonic()+2
                survivors=group_members(group)
                while survivors and time.monotonic()<deadline:
                    for member,created in survivors:observed[(member.pid,created)]=True
                    time.sleep(0.1);survivors=group_members(group)
                if survivors:
                    reason=reason or 'owned_descendants_survived_runner_exit'
                    for member,created in survivors:observed[(member.pid,created)]=True
                    stop_owned(signal.SIGTERM)
                    time.sleep(0.5)
                    survivors=group_members(group)
                    if survivors:stop_owned(signal.SIGKILL)
                    psutil.wait_procs([m for m,c in survivors],timeout=5)
                for (pid,created) in observed:
                    try:
                        item=psutil.Process(pid)
                        if item.create_time()==created:
                            remaining.append({'pid':pid,'created':created,'status':item.status()})
                    except psutil.NoSuchProcess:pass
                if remaining:reason=reason or 'owned_process_census_not_empty'
            except BaseException as error:
                reason=reason or 'cleanup_error:'+type(error).__name__
                errors.append(traceback.format_exc())
                # If census itself failed, do not claim absence: still kill the
                # known-owned group and try to reap the known leader.
                try:stop_owned(signal.SIGKILL)
                except BaseException as cleanup_error:errors.append(repr(cleanup_error))
                try:child.wait(timeout=10)
                except BaseException as cleanup_error:errors.append(repr(cleanup_error))
        if output is not None:
            try:output.close()
            except BaseException as error:
                reason=reason or 'sample_file_close_error';errors.append(repr(error))
        result={'status':'passed' if child is not None and child.returncode==0 and reason is None else 'failed',
            'returncode':None if child is None else child.returncode,'stop_reason':reason,
            'elapsed_seconds':time.monotonic()-start,'sampled_peak_tree_rss_bytes':peak,
            'samples':samples,'remaining_owned_processes':remaining,'errors':errors,
            'observed_processes':len(observed),'wall_limit_seconds':args.seconds,
            'sampled_rss_limit_bytes':args.rss_mib*1024**2,'progress_limit_seconds':args.stall_seconds,
            'limits':'Independent sampled watchdog; not a hard OS resource quota.'}
        try:
            with args.report.open('x') as report:
                report.write(json.dumps(result,indent=2)+'\n')
        except BaseException as error:
            result['status']='failed';result['stop_reason']=result['stop_reason'] or 'report_persistence_error'
            result['errors'].append(repr(error))
        print(json.dumps(result),flush=True)
    return 0 if result['status']=='passed' else 1


if __name__=='__main__':raise SystemExit(main())
