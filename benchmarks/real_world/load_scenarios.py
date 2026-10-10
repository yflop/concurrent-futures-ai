"""Controlled load transitions executing real fixture work; no speedup claims.

Gates create known queue states only. They are not substitutes for pipeline/image
algorithms, and gated elapsed time is deliberately not a performance result.
"""
from __future__ import annotations
import argparse
from dataclasses import asdict
from contextlib import nullcontext
import json
from pathlib import Path
import tempfile
import threading
import time
from intelligent_futures import IntelligentExecutor,TaskHints,AdaptivePolicy,ExecutionObjective
from . import execute_batch,module_for
from .features import batch_features
from .optimizer import digest,write
from .run import workload_environment


class FileGate:
    """Bounded test orchestration without a manager/socket service."""
    def __init__(self,path):self.path=Path(path)
    def set(self):self.path.write_text('released')
    def wait(self,seconds):
        deadline=time.monotonic()+seconds;pause=threading.Event()
        while not self.path.exists():
            if time.monotonic()>=deadline:return False
            pause.wait(.001)
        return True


def gated_real_work(event,payload):
    if not event.wait(30):
        raise RuntimeError('controlled load gate timed out')
    return execute_batch(payload)


def verify(rows,expected):
    if any(row['error'] for row in rows) or digest([r['value'] for r in rows])!=expected:
        raise AssertionError('actual workload output mismatch')


def drained(pool):
    deadline=time.monotonic()+10
    while pool.snapshot()['physical']['retained']:
        if time.monotonic()>deadline:
            raise RuntimeError('physical bookkeeping did not settle')
        threading.Event().wait(.001)


def physical_ok(snapshot):
    p=snapshot['physical']
    return (p['peak_retained']<=p['max_retained'] and p['retained']==0
        and all(value<=p['dispatch_limits'][name] for name,value in p['peak_in_flight'].items()))


def run(output):
    report={'schema_version':1,'scope':'deterministic queued/occupied-slot transitions with real pipeline and image tasks; gates are orchestration, elapsed time is not speedup evidence',
        'environment':workload_environment(),'transitions':[],'predictive_epochs':[],'failures':[]}
    with tempfile.TemporaryDirectory(prefix='load-transitions-') as temp:
        root=Path(temp);module=module_for('pipeline')
        tasks=module.prepare('pipeline',root/'pipeline',2,771337,scale='representative')
        reference=execute_batch(('pipeline',tasks));expected=digest([r['value'] for r in reference])
        if any(r['error'] for r in reference):raise RuntimeError('reference failed')
        with nullcontext():
            process_gate=FileGate(root/'process-release');thread_gate=threading.Event()
            pool=IntelligentExecutor(max_workers=2,process_workers=2,max_pending=12,
                policy=AdaptivePolicy(exploration_interval=128,minimum_samples=32))
            try:
                cpu=TaskHints(kind='cpu',key='actual-pipeline',process_safe=True)
                blocked=[pool.submit_task(gated_real_work,process_gate,('pipeline',tasks),hints=cpu) for _ in range(4)]
                before=pool.snapshot()
                selected=pool.submit_task(execute_batch,('pipeline',tasks),hints=cpu)
                after=pool.snapshot();verify(selected.result(30),expected)
                if after['policy']['last_decision']['backend']!='thread' or after['policy']['last_decision']['reason']!='queue-pressure':
                    raise AssertionError('process backlog did not trigger measured queue-pressure diversion')
                report['transitions'].append({'label':'process backlog diverts CPU-safe work to thread','before':before,'after':after})
                process_gate.set()
                for future in blocked:verify(future.result(30),expected)
                drained(pool)
                selected=pool.submit_task(execute_batch,('pipeline',tasks),hints=cpu)
                restored=pool.snapshot();verify(selected.result(30),expected);drained(pool)
                if restored['policy']['last_decision']['backend']!='process':raise AssertionError('drained backlog did not restore CPU preference')
                report['transitions'].append({'label':'drained backlog restores CPU process preference','after':restored})
                io=TaskHints(kind='io',key='controlled-thread-gate',process_safe=False)
                blocked=[pool.submit_task(gated_real_work,thread_gate,('pipeline',tasks),hints=io) for _ in range(4)]
                before=pool.snapshot();selected=pool.submit_task(execute_batch,('pipeline',tasks),hints=cpu)
                after=pool.snapshot();verify(selected.result(30),expected)
                if after['policy']['last_decision']['backend']!='process':raise AssertionError('thread backlog blocked independent process progress')
                report['transitions'].append({'label':'thread backlog allows independent CPU process progress','before':before,'after':after})
                thread_gate.set()
                for future in blocked:verify(future.result(30),expected)
            finally:
                process_gate.set();thread_gate.set();pool.shutdown(wait=True,cancel_futures=True)
            report['routing_final_snapshot']=pool.snapshot()
            if not physical_ok(report['routing_final_snapshot']):raise AssertionError('routing physical bounds failed')
        image_module=module_for('images');images=image_module.prepare('images',root/'images',6,781349,scale='representative')
        ordered=sorted(images,key=lambda t:t['source_size'][0]*t['source_size'][1])
        expected={task['index']:digest([r['value'] for r in execute_batch(('images',[task]))]) for task in images}
        with IntelligentExecutor(max_workers=4,max_pending=8,policy='hints',predictive=True,
                profile_sample_interval=1,objective=ExecutionObjective(mode='throughput',max_concurrency=4)) as pool:
            for label,work in [('small sparse',ordered[:2]),('large burst',ordered[-2:]*8),('small recovery',ordered[:2]*2)]:
                futures=[];snapshots=[]
                for task in work:
                    future=pool.submit_profiled(execute_batch,('images',[task]),
                        hints=TaskHints(kind='io',key='changing-image-cost'),features=batch_features('images',[task]))
                    futures.append((task,future));snapshots.append(pool.snapshot())
                for task,future in futures:verify(future.result(30),expected[task['index']])
                drained(pool)
                report['predictive_epochs'].append({'label':label,'tasks':len(work),
                    'source_pixel_counts':[t['source_size'][0]*t['source_size'][1] for t in work],
                    'observed_limits':[s['predictive']['concurrency_limit'] for s in snapshots],
                    'observed_loads':[s['load'] for s in snapshots],
                    'prediction_sources':[s['predictive']['last_prediction'] for s in snapshots],
                    'completion_snapshot':pool.snapshot()})
            profiles=pool.export_profiles()
            report['profile_count']=len(profiles)
        report['predictive_final_snapshot']=pool.snapshot()
        if not physical_ok(report['predictive_final_snapshot']):raise AssertionError('predictive physical bounds failed')
    report['accepted']=True;report['status']='complete';write(output,report);return report


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();run(args.output)

if __name__=='__main__':main()
