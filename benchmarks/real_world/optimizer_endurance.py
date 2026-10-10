"""Twenty-minute real-workload lifecycle envelope; bounded local evidence only."""
from __future__ import annotations
import argparse
from concurrent.futures import CancelledError
from contextlib import ExitStack
from dataclasses import asdict
import gc
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import psutil
from intelligent_futures import IntelligentExecutor, TaskHints
from intelligent_futures.tuning import Configuration
from . import NAMES, module_for, execute_batch
from .run import make_executor, measure_pool, workload_environment, cpu_seconds
from .soak import MemorySampler


def identity(value):
    return value


def planned_failure():
    raise ValueError('planned endurance fault')


def stable_digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def census():
    process=psutil.Process()
    return {'threads':sorted(t.name for t in threading.enumerate()),
        'children':sorted((p.pid,p.name()) for p in process.children(recursive=True)),
        'fds':process.num_fds(), 'rss_bytes':process.memory_info().rss}


def settled(snapshot, max_pending):
    counts=snapshot['counts']
    physical=snapshot.get('physical',{})
    return (snapshot['closed'] and snapshot['pending']==0 and counts['peak_pending']<=max_pending
        and counts['submitted']==sum(counts[k] for k in ('completed','failed','cancelled'))
        and physical.get('retained')==0 and physical.get('peak_retained',max_pending+1)<=max_pending
        and all(value==0 for value in physical.get('queued',{}).values())
        and all(value==0 for value in physical.get('in_flight',{}).values())
        and all(value<=physical['dispatch_limits'][backend]
                for backend,value in physical.get('peak_in_flight',{}).items()))


def fault_probe():
    ready=threading.Event(); release=threading.Event(); callbacks=[]
    def gate():
        ready.set()
        if not release.wait(10):
            raise AssertionError('planned gate timed out')
        return 'released'
    pool=IntelligentExecutor(max_workers=1,max_pending=4,policy='hints')
    try:
        running=pool.submit(gate)
        if not ready.wait(10):
            raise AssertionError('worker did not start')
        cancelled=pool.submit(identity,1)
        failed=pool.submit(planned_failure)
        for f in (running,cancelled,failed):
            f.add_done_callback(lambda _:callbacks.append(1))
        if not cancelled.cancel():
            raise AssertionError('queued task cancellation failed')
        release.set()
        if running.result(10)!='released':
            raise AssertionError('wrong gate result')
        try:
            failed.result(10)
        except ValueError as exc:
            if str(exc)!='planned endurance fault':
                raise
        else:
            raise AssertionError('missing planned failure')
        try:
            cancelled.result(10)
        except CancelledError:
            pass
        else:
            raise AssertionError('cancelled result did not raise')
    finally:
        release.set(); pool.shutdown(wait=True,cancel_futures=True)
    snapshot=pool.snapshot()
    if not settled(snapshot,4) or len(callbacks)!=3 or snapshot['counts']['failed']!=1 or snapshot['counts']['cancelled']!=1:
        raise AssertionError('fault accounting/callback mismatch')
    return {'expected_failures':1,'expected_cancellations':1,'callbacks':len(callbacks),'snapshot':snapshot}


def run(output,epoch_seconds=55.,epochs=22):
    if not 1<=epoch_seconds<=120 or not 1<=epochs<=44:
        raise ValueError('bounded duration required')
    available=sorted(os.sched_getaffinity(0)); os.sched_setaffinity(0,available[:8])
    report={'schema_version':1,'status':'running','environment':workload_environment(),
        'method':{'epoch_seconds':epoch_seconds,'epochs':epochs,'workloads':NAMES,
            'configuration':'two workers, max_pending four; thread then spawn-process epochs; alternating batch sizes 1 and 4',
            'fixtures':'six distinct representative-profile inputs per workload, exact independent result digests',
            'memory':'10ms sampled aggregate process RSS, not allocator/leak proof; absolute peak budget 1GiB, parent final growth budget 128MiB relative to warmed fixture/library baseline',
            'scope':'single host bounded endurance with real libraries; no production uptime or leak-free proof'},
        'epochs':[],'fault_probes':[],'failures':[]}
    def save():
        output.parent.mkdir(parents=True,exist_ok=True)
        output.write_text(json.dumps(report,indent=2)+'\n')
    # Spawn establishes Python's persistent resource tracker before baseline census.
    warm=make_executor('intelligent',Configuration('process',1,1))
    warm.submit_task(identity,1,hints=TaskHints(kind='cpu',process_safe=True)).result(20)
    warm.shutdown(wait=True)
    gc.collect(); baseline=census(); report['baseline']=baseline
    started=time.perf_counter(); before_cpu=cpu_seconds()
    with tempfile.TemporaryDirectory(prefix='optimizer-endurance-') as tmp, ExitStack() as stack:
        root=Path(tmp); datasets={}
        for name in NAMES:
            module=module_for(name); path=root/name; path.mkdir()
            if hasattr(module,'fixture'):
                stack.enter_context(module.fixture(path))
            tasks=module.prepare(name,path/'data',6,761213,scale='representative')
            rows=execute_batch((name,tasks))
            if any(r['error'] for r in rows):
                raise RuntimeError('Reference failed: '+repr(rows))
            datasets[name]=(tasks,stable_digest([r['value'] for r in rows]))
        ready_census=census(); report['after_fixture_setup']=ready_census; save()
        for epoch in range(epochs):
            name=NAMES[epoch%len(NAMES)]
            config=Configuration('thread' if (epoch//len(NAMES))%2==0 else 'process',2,1 if epoch%2==0 else 4)
            tasks,expected=datasets[name]
            pool=make_executor('intelligent',config)
            epoch_start=time.perf_counter(); passes=errors=mismatches=observations=0
            p95=[]; callback_start=0
            with MemorySampler() as memory:
                try:
                    while time.perf_counter()-epoch_start<epoch_seconds:
                        row=measure_pool(pool,'intelligent',config,name,tasks)
                        passes+=1; observations+=row['tasks']; errors+=len(row['errors'])
                        mismatches+=int(row['result_sha256']!=expected)
                        p95.append(row['batch_completion_latency_p95_seconds'])
                        if observations>100000:
                            raise RuntimeError('per-epoch safety task cap reached')
                finally:
                    pool.shutdown(wait=True)
            snapshot=pool.snapshot(); resources=census()
            failures=[]
            if errors or mismatches:
                failures.append('unexpected task errors or mismatched results')
            if not settled(snapshot,4):
                failures.append('executor accounting/pending bound failed')
            if memory.peak is None or memory.peak>1024**3 or memory.errors:
                failures.append('sampled memory gate failed')
            if resources['threads']!=ready_census['threads']:
                failures.append('live thread census changed after pool teardown')
            if resources['children']!=ready_census['children']:
                failures.append('live child census changed after pool teardown')
            if resources['fds']>ready_census['fds']+4:
                failures.append('file descriptor growth exceeded temporary allowance')
            record={'epoch':epoch,'workload':name,'configuration':asdict(config),
                'elapsed_seconds':time.perf_counter()-epoch_start,'fixture_passes':passes,
                'task_observations':observations,'unexpected_errors':errors,'digest_mismatches':mismatches,
                'max_observed_batch_p95_seconds':max(p95),'snapshot':snapshot,
                'memory':memory.report(),'resources_after_shutdown':resources,'failures':failures}
            report['epochs'].append(record)
            report['fault_probes'].append(fault_probe())
            report['failures'].extend(f'epoch {epoch}: {f}' for f in failures)
            save(); print(name,epoch,observations,failures,flush=True)
    gc.collect(); final=census(); report['final']=final
    if final['threads']!=baseline['threads'] or final['children']!=baseline['children']:
        report['failures'].append('final live thread/child census mismatch')
    if final['fds']>baseline['fds']+2:
        report['failures'].append('final file descriptors exceed baseline+2')
    report['parent_rss_growth_after_fixture_warmup_bytes']=final['rss_bytes']-ready_census['rss_bytes']
    report['parent_rss_growth_since_import_bytes']=final['rss_bytes']-baseline['rss_bytes']
    if final['rss_bytes']-ready_census['rss_bytes']>128*1024**2:
        report['failures'].append('parent RSS growth exceeds128MiB')
    report.update(status='complete',elapsed_seconds=time.perf_counter()-started,
        cpu_seconds=cpu_seconds()-before_cpu,total_task_observations=sum(e['task_observations'] for e in report['epochs']),
        accepted=not report['failures'])
    save(); return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--epoch-seconds',type=float,default=55.)
    parser.add_argument('--epochs',type=int,default=22)
    args=parser.parse_args(); report=run(args.output,args.epoch_seconds,args.epochs)
    return 0 if report['accepted'] else 1

if __name__=='__main__':
    raise SystemExit(main())
