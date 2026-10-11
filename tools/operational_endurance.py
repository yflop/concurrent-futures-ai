"""Four-hour, bounded-state executor endurance; see declared protocol first."""
from __future__ import annotations
import argparse
from collections import Counter, deque
from concurrent.futures import as_completed, wait
from concurrent.futures.process import BrokenProcessPool
import gc
import hashlib
import json
import os
from pathlib import Path
import pickle
import platform
import sys
import threading
import time
import traceback
import weakref

import psutil
import intelligent_futures
import intelligent_futures.executor as loaded_executor
from intelligent_futures import (BackpressureError, ExecutionObjective,
                                 IntelligentExecutor, TaskHints)


def identity(value):
    return value


def workload(kind, sequence, payload):
    if kind == 'blocking':
        time.sleep(0.004)
        return sequence ^ 12345
    if kind == 'cpu':
        return sum((i * i + sequence) % 997 for i in range(3000))
    if kind == 'payload':
        return payload
    raise ValueError('intentional endurance task failure')


def crash_worker():
    time.sleep(0.02)
    os._exit(17)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def manifest():
    root = Path(__file__).resolve().parents[1]
    package = Path(loaded_executor.__file__).resolve().parent
    return {'runtime_variant': 'unreleased-0.5.1-reentrant-guard',
        'version': intelligent_futures.__version__, 'published_base_commit':
        '7c95c6fac134e2aaf53245dc20ac0e195bf5e6ed',
        'loaded_package_directory': str(package),
        'runtime_sha256': {str(p.relative_to(package)): sha(p) for p in sorted(package.rglob('*'))
                           if p.is_file() and '__pycache__' not in p.parts},
        'harness_sha256': sha(__file__),
        'protocol_sha256': sha(root/'docs/OPERATIONAL-ENDURANCE-PROTOCOL.md'),
        'python': sys.version, 'python_optimization_level':sys.flags.optimize,
        'platform': platform.platform(),
        'psutil': psutil.__version__, 'affinity': psutil.Process().cpu_affinity()
            if hasattr(psutil.Process(), 'cpu_affinity') else None}


def census():
    parent = psutil.Process()
    children = []
    for child in parent.children(recursive=True):
        try:
            children.append({'pid': child.pid, 'created': child.create_time(),
                'rss': child.memory_info().rss, 'fds': child.num_fds(),
                'threads': child.num_threads(), 'status': child.status(),
                'resource_tracker': 'multiprocessing.resource_tracker' in ' '.join(child.cmdline())})
        except psutil.NoSuchProcess:
            pass
    return {'parent_rss': parent.memory_info().rss, 'parent_fds': parent.num_fds(),
        'parent_native_threads': parent.num_threads(),
        'python_threads': len(threading.enumerate()), 'children': children,
        'task_children': sum(not c['resource_tracker'] for c in children),
        'tree_rss': parent.memory_info().rss + sum(c['rss'] for c in children)}


def until(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError('bounded operational condition failed')
        time.sleep(0.002)


class Evidence:
    def __init__(self, directory):
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=False)
        self.stream = (directory/'events.jsonl').open('x')
        self.started = time.monotonic()
        self.peaks = Counter()
        self.lock = threading.Lock()

    def emit(self, event, **values):
        row = {'event': event, 'elapsed_seconds': time.monotonic()-self.started,
               'unix_time': time.time(), **values}
        line = json.dumps(row, allow_nan=False, sort_keys=True)
        with self.lock:
            self.stream.write(line+'\n'); self.stream.flush()
            if event != 'active_sample':
                temp = self.directory/'checkpoint.tmp'
                temp.write_text(line+'\n')
                temp.replace(self.directory/'checkpoint.json')
        return row

    def sample(self, event, pool=None, **values):
        state = census()
        with self.lock:
            for key in ('parent_rss','tree_rss','parent_fds','python_threads','task_children',
                        'parent_native_threads'):
                self.peaks[key] = max(self.peaks[key], state[key])
        snapshot = None if pool is None else pool.snapshot()
        if snapshot is not None:
            assert 0 <= snapshot['pending'] <= 32
            assert 0 <= snapshot['physical']['retained'] <= 32
            assert snapshot['physical']['peak_retained'] <= 32
            for backend, limit in (('thread',2), ('process',2)):
                assert snapshot['physical']['in_flight'][backend] <= limit
                assert snapshot['physical']['peak_in_flight'][backend] <= limit
            assert len(snapshot['profiles']) <= 16
            assert snapshot['policy']['tracked_keys'] <= 16
            assert snapshot['counts']['peak_pending'] <= 32
            assert len(pool.routing_trace()) <= 64
            assert pool.routing_counters()['retained'] <= 64
            c = snapshot['counts']
            assert c['completed']+c['failed']+c['cancelled'] <= c['submitted']
            # Fixed-size view, rather than storing all profile values per sample.
            snapshot = {k:snapshot[k] for k in ('counts','pending','physical','load')}
            snapshot['profile_count'] = len(pool.snapshot()['profiles'])
            snapshot['adaptive_tracked_keys'] = pool.snapshot()['policy']['tracked_keys']
            snapshot['routing'] = pool.routing_counters()
        return self.emit(event, census=state, snapshot=snapshot, **values)


class Payload:
    def __init__(self):
        self.data = bytes(65536)


def diagnostic_checkpoint(evidence, cycle):
    row = {'cycle': cycle, 'status': 'running', 'accepted': 0, 'rejected': 0,
           'cancelled': 0, 'expected_failures': 0, 'successes': 0}
    evidence.emit('diagnostic_start', **row)
    start=time.monotonic()
    try:
        return _diagnostic_checkpoint(evidence,cycle,row,start)
    except BaseException as error:
        row.update(status='failed',error_type=type(error).__name__,error=str(error),
                   elapsed_seconds=time.monotonic()-start)
        evidence.emit('diagnostic_failure',**row)
        raise


def _diagnostic_checkpoint(evidence,cycle,row,start):
    gate = threading.Event()
    started = threading.Event()
    pool = IntelligentExecutor(max_workers=1, max_pending=8, policy='threads', submit_timeout=0)
    def blocked():
        started.set()
        if not gate.wait(10): raise TimeoutError('diagnostic gate')
        return 3
    try:
        first = pool.submit(blocked); row['accepted'] += 1
        assert started.wait(10)
        futures, refs = [], []
        for _ in range(7):
            payload = Payload(); refs.append(weakref.ref(payload))
            futures.append(pool.submit(lambda p: len(p.data), payload)); row['accepted'] += 1
        del payload
        try: pool.submit(identity, 7)
        except BackpressureError: row['rejected'] += 1
        else: raise AssertionError('saturated admission was not rejected')
        assert all(f.cancel() for f in futures)
        row['cancelled'] += len(futures)
        assert not wait(futures, timeout=10).not_done
        assert len(list(as_completed(futures, timeout=10))) == 7
        gc.collect(); assert all(ref() is None for ref in refs)
        pool.shutdown(wait=False)
        pool.shutdown(wait=False, cancel_futures=True)
        gate.set(); pool.shutdown(wait=True)
        assert first.result() == 3; row['successes'] += 1
        assert pool.snapshot()['pending'] == pool.snapshot()['physical']['retained'] == 0
    finally:
        gate.set(); pool.shutdown(wait=True, cancel_futures=True)
    guard = IntelligentExecutor(max_workers=2, max_pending=1, policy='threads')
    def nested():
        try: guard.submit(identity, 1)
        except BackpressureError: return 'guarded'
        raise AssertionError('nested admission did not fail fast')
    try:
        assert guard.submit(nested).result(10) == 'guarded'
        row['accepted'] += 1; row['successes'] += 1; row['rejected'] += 1
        assert guard.snapshot()['counts']['reentrant_rejected'] == 1
        callback_gate=threading.Event(); callback_done=threading.Event(); callback_errors=[];callback_children=[]
        def guarded_callback(_):
            try:
                callback_children.append(guard.submit(lambda: callback_gate.wait(10)))
                row['accepted']+=1
                try: guard.submit(identity, 2)
                except BackpressureError: row['rejected']+=1
                else: raise AssertionError('callback admission did not fail fast')
            except BaseException as error: callback_errors.append(type(error).__name__)
            finally: callback_done.set()
        ready=threading.Event(); body_gate=threading.Event()
        def callback_body():
            ready.set()
            if not body_gate.wait(10): raise TimeoutError('callback body gate')
            return 4
        first_callback=guard.submit(callback_body);row['accepted']+=1
        assert ready.wait(10)
        first_callback.add_done_callback(guarded_callback); body_gate.set()
        try:
            assert callback_done.wait(10)
            assert not callback_errors,callback_errors
        finally: callback_gate.set()
        assert first_callback.result(10)==4
        assert len(callback_children)==1 and callback_children[0].result(10) is True
        row['successes']+=2
        guard.shutdown()
        guard_snapshot=guard.snapshot()
        assert guard_snapshot['counts']['reentrant_rejected']==2
        assert guard_snapshot['counts']['completed']==3
        assert guard_snapshot['pending']==guard_snapshot['physical']['retained']==0
    finally: guard.shutdown()
    hints = TaskHints(kind='cpu', process_safe=True)
    broken = IntelligentExecutor(max_workers=1, process_workers=1, max_pending=8, policy='process')
    try:
        futures = [broken.submit_task(crash_worker, hints=hints)]; row['accepted'] += 1
        for i in range(7):
            futures.append(broken.submit_task(identity,i,hints=hints));row['accepted'] += 1
        for future in futures:
            try: future.result(10)
            except BrokenProcessPool: row['expected_failures'] += 1
            else: raise AssertionError('crash-affected task unexpectedly succeeded')
        try: broken.submit_task(identity, 1, hints=hints)
        except BrokenProcessPool: row['rejected'] += 1
        else: raise AssertionError('broken process pool accepted later process work')
        assert broken.snapshot()['counts']['thread_submitted'] == 0
    finally: broken.shutdown()
    recovery = IntelligentExecutor(max_workers=1, process_workers=1, max_pending=4, policy='process')
    try:
        assert recovery.submit_task(identity, 19, hints=hints).result(10) == 19
        row['accepted'] += 1; row['successes'] += 1
        unpickleable = recovery.submit_task(lambda: 9, hints=hints); row['accepted'] += 1
        try: unpickleable.result(10)
        except (AttributeError, TypeError, pickle.PicklingError): row['expected_failures'] += 1
        else: raise AssertionError('serialization failure did not surface')
        assert recovery.submit_task(identity, 23, hints=hints).result(10) == 23
        row['accepted'] += 1; row['successes'] += 1
    finally: recovery.shutdown()
    predictive=IntelligentExecutor(max_workers=2,process_workers=1,max_pending=8,
        predictive=True,objective=ExecutionObjective(max_concurrency=1),
        max_profile_keys=4,trace_capacity=8)
    predictive_started=time.monotonic()
    try:
        for i in range(16):
            future=predictive.submit_profiled(identity,i,features={'size':float(i%3)},
                hints=TaskHints(key='predictive-%d'%(i%8),process_safe=True))
            row['accepted']+=1
            assert future.result(10)==i;row['successes']+=1
        predictive.shutdown(wait=False);predictive.shutdown(wait=True,cancel_futures=True)
        state=predictive.snapshot()
        assert state['pending']==state['physical']['retained']==0
        assert len(predictive.export_profiles())<=4
        assert state['counts']['completed']==16
        row['predictive_probe']={'seconds':time.monotonic()-predictive_started,
            'tasks':16,'counts':state['counts'],'routing':predictive.routing_counters(),
            'profile_count':len(predictive.export_profiles()),
            'scope':'short lifecycle probe, not four-hour predictive endurance'}
    finally:predictive.shutdown()
    assert row['accepted'] == row['successes']+row['expected_failures']+row['cancelled']
    row.update(status='passed', elapsed_seconds=time.monotonic()-start,
               recovery='new executor only; serialization error did not break new pool')
    evidence.emit('diagnostic_end', **row)
    return row


def slope(rows, field):
    if len(rows)<2: return None
    xs = [r['elapsed_seconds']/3600 for r in rows]
    ys = [r['census'][field] for r in rows]
    xm, ym = sum(xs)/len(xs), sum(ys)/len(ys)
    denominator = sum((x-xm)**2 for x in xs)
    return None if not denominator else sum((x-xm)*(y-ym) for x,y in zip(xs,ys))/denominator


def run(args):
    evidence = Evidence(args.output)
    identity_record = manifest()
    evidence.emit('start', identity=identity_record, arguments={
        'seconds':args.seconds,'warmup':args.warmup,'cycle_seconds':args.cycle_seconds,
        'sample_seconds':args.sample_seconds,'mode':args.mode})
    before = census()
    pool = IntelligentExecutor(max_workers=2, process_workers=2, max_pending=32,
        policy='adaptive', submit_timeout=10, trace_capacity=64, max_profile_keys=16)
    count_keys=('attempted','accepted','rejected','success','expected_failure','unexpected_failure',
                'cancelled','cancel_attempted','cancel_true','cancel_false')
    counts = Counter(dict.fromkeys(count_keys,0))
    by_kind = {kind:Counter(dict.fromkeys(count_keys,0)) for kind in ('blocking','cpu','payload','failure')}
    callbacks = Counter(); callback_lock = threading.Lock()
    retained = deque(maxlen=64)
    quiescent = []
    diagnostics = []
    sequence = 0; batches = 0; workload_elapsed = 0.0
    baseline = None; measured_start = None; failure = None
    last_phase = None; next_boundary=args.cycle_seconds;last_progress=time.monotonic()
    monitor_stop=threading.Event();monitor_errors=[]
    def monitor():
        while not monitor_stop.wait(args.sample_seconds):
            try: evidence.sample('active_sample',pool)
            except BaseException as error:
                monitor_errors.append(type(error).__name__+': '+str(error))
                evidence.emit('monitor_failure',error=monitor_errors[0])
                return
    monitor_thread=threading.Thread(target=monitor,name='endurance-resource-monitor')
    monitor_thread.start()
    warm_deadline = time.monotonic()+args.warmup
    def finished(future, slow):
        if slow: time.sleep(0.002)
        with callback_lock: callbacks['total'] += 1
    def settled():
        s = pool.snapshot()
        with callback_lock: callback_total=callbacks['total']
        return (s['pending']==0 and s['physical']['retained']==0
                and callback_total==counts['accepted'])
    def verify_outcomes():
        until(settled)
        s=pool.snapshot(); c=s['counts']
        assert c['submitted']==counts['accepted']
        assert c['completed']==counts['success']
        assert c['failed']==counts['expected_failure']
        assert c['cancelled']==counts['cancelled']
        assert c['submitted']==c['completed']+c['failed']+c['cancelled']
        trace=pool.routing_counters()
        assert trace['recorded']==c['submitted']
        assert trace['retained']<=64 and trace['dropped']==trace['recorded']-trace['retained']
        if c['submitted']>64:assert trace['dropped']>0
    def quiescent_sample(label):
        retained.clear(); gc.collect(); verify_outcomes()
        current=evidence.sample(label,pool,counts=dict(counts),by_kind=by_kind,
            workload_seconds=workload_elapsed, primary_executor_age=time.monotonic()-evidence.started)
        if baseline is not None:
            b=baseline['census']; c=current['census']
            for field,limit in [('parent_rss',128*1024**2),('tree_rss',256*1024**2),
                                ('parent_fds',4),('python_threads',2)]:
                assert c[field]-b[field]<=limit,(field,c[field]-b[field],limit)
            assert c['task_children']==b['task_children']==2
            identities=lambda state:sorted((child['pid'],child['created']) for child in state['children']
                                             if not child['resource_tracker'])
            assert identities(c)==identities(b),'primary worker identities changed'
        quiescent.append(current)
        return current
    try:
        # Warm all process workers and all task kinds using the same bounded stream.
        while True:
            measured = baseline is not None
            if monitor_errors: raise AssertionError(monitor_errors[0])
            if measured and workload_elapsed >= args.seconds: break
            batch_started=time.monotonic()
            phase_time=workload_elapsed % args.cycle_seconds
            phase=('steady' if phase_time<args.cycle_seconds*2/3 else
                   'burst' if phase_time<args.cycle_seconds*13/15 else 'retention') if measured else 'warmup'
            if phase != last_phase:
                retained.clear()
                evidence.emit('phase',phase=phase,workload_seconds=workload_elapsed,counts=dict(counts))
                last_phase=phase
            pairs=[]
            for _ in range(32):
                slot=sequence%20
                kind='blocking' if slot<8 else 'cpu' if slot<15 else 'payload' if slot<19 else 'failure'
                payload=bytes([sequence%251])*65536 if kind=='payload' else b''
                hints=TaskHints(kind='io' if kind=='blocking' else 'cpu' if kind=='payload' else 'unknown',
                                process_safe=True,key='key-%d'%(sequence%64))
                counts['attempted']+=1; by_kind[kind]['attempted']+=1
                try: future=pool.submit_task(workload,kind,sequence,payload,hints=hints)
                except BackpressureError:
                    counts['rejected']+=1; by_kind[kind]['rejected']+=1
                    raise
                counts['accepted']+=1; by_kind[kind]['accepted']+=1
                future.add_done_callback(lambda f,slow=(batches%20==0 and sequence%32==0):finished(f,slow))
                if sequence%7==0:
                    counts['cancel_attempted']+=1; by_kind[kind]['cancel_attempted']+=1
                    cancelled=future.cancel()
                    counts['cancel_true' if cancelled else 'cancel_false']+=1
                    by_kind[kind]['cancel_true' if cancelled else 'cancel_false']+=1
                pairs.append((future,kind,sequence,payload)); sequence+=1
            for future,kind,n,payload in (reversed(pairs) if batches%2 else pairs):
                if future.cancelled(): outcome='cancelled'
                else:
                    try: result=future.result(10)
                    except ValueError:
                        if kind!='failure':
                            counts['unexpected_failure']+=1;by_kind[kind]['unexpected_failure']+=1
                            raise
                        outcome='expected_failure'
                    except BaseException:
                        counts['unexpected_failure']+=1;by_kind[kind]['unexpected_failure']+=1
                        raise
                    else:
                        assert kind!='failure'
                        expected=(n^12345 if kind=='blocking' else
                                  sum((i*i+n)%997 for i in range(3000)) if kind=='cpu' else payload)
                        assert result==expected,(kind,n)
                        outcome='success'
                counts[outcome]+=1; by_kind[kind][outcome]+=1
                if phase=='retention':retained.append(future)
            pairs.clear(); del future,payload
            batches+=1
            if phase in ('steady','warmup'):
                time.sleep(max(0,0.2-(time.monotonic()-batch_started)))
            if measured:workload_elapsed+=time.monotonic()-batch_started
            if not measured and time.monotonic()>=warm_deadline:
                verify_outcomes()
                baseline=quiescent_sample('warm_baseline')
                measured_start=time.monotonic()
                evidence.emit('measurement_start',workload_seconds=0,counts=dict(counts))
            elif measured and workload_elapsed>=next_boundary:
                verify_outcomes()
                cycle=len(diagnostics)+1
                diagnostic=diagnostic_checkpoint(evidence,cycle);diagnostics.append(diagnostic)
                until(lambda:census()['task_children']==2,timeout=2)
                health=pool.submit(identity,cycle)
                health.add_done_callback(lambda f:finished(f,False))
                assert health.result(10)==cycle
                # The health probe participates in a separate explicit denominator.
                counts['attempted']+=1;counts['accepted']+=1;counts['success']+=1
                for key in ('attempted','accepted','success'):by_kind.setdefault('health',Counter())[key]+=1
                quiescent_sample('cycle_quiescent')
                next_boundary+=args.cycle_seconds
            if time.monotonic()-last_progress>=args.sample_seconds:
                with callback_lock:callback_total=callbacks['total']
                evidence.emit('workload_progress',workload_seconds=workload_elapsed,
                    counts=dict(counts),by_kind=by_kind,callbacks=callback_total,held_futures=len(retained))
                last_progress=time.monotonic()
        verify_outcomes()
        quiescent_sample('final_quiescent')
        pool.shutdown(wait=False)
        pool.shutdown(wait=True)
        monitor_stop.set();monitor_thread.join(10)
        assert not monitor_thread.is_alive()
        assert not monitor_errors,monitor_errors
        retained.clear();gc.collect()
        until(lambda:census()['task_children']==0,timeout=2)
        after=census()
        assert after['python_threads']<=before['python_threads']
        assert after['parent_fds']<=before['parent_fds']+4
        assert len(after['children'])<=len(before['children'])+1
        assert all(c['resource_tracker'] for c in after['children'])
        assert after['parent_rss']-before['parent_rss']<=128*1024**2
        final_identity=manifest()
        for identity_key in ('runtime_sha256','harness_sha256','protocol_sha256'):
            assert identity_record[identity_key]==final_identity[identity_key],identity_key
        result={'status':'passed','mode':args.mode,'identity':identity_record,
            'workload_seconds':workload_elapsed,'required_workload_seconds':args.seconds,
            'elapsed_seconds':time.monotonic()-evidence.started,'batches':batches,
            'counts':dict(counts),'warmup_counts':baseline['counts'],
            'measured_counts':{k:counts[k]-baseline['counts'].get(k,0) for k in counts},
            'by_kind':by_kind,'callbacks':dict(callbacks),
            'diagnostics':diagnostics,'before':before,'after':after,'peaks':dict(evidence.peaks),
            'quiescent_samples':len(quiescent),'quiescent_resource_trends':{
                k:{'first':quiescent[0]['census'][k],'last':quiescent[-1]['census'][k],
                   'delta':quiescent[-1]['census'][k]-quiescent[0]['census'][k],
                   'ols_per_hour':slope(quiescent,k)} for k in ('parent_rss','tree_rss','parent_fds','python_threads','task_children')},
            'scope':'One host/interpreter and declared workload only; no production certification or performance claim.'}
        evidence.emit('complete',result=result)
        (args.output/'summary.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
        return 0
    except BaseException as error:
        failure={'status':'failed','error_type':type(error).__name__,'error':str(error),
            'traceback':traceback.format_exc(),'workload_seconds':workload_elapsed,
            'counts':dict(counts),'by_kind':by_kind,
            'accepted_without_observed_terminal_outcome':counts['accepted']-sum(counts[k] for k in
                ('success','expected_failure','unexpected_failure','cancelled')),
            'identity':identity_record,
            'census':census(),'snapshot':pool.snapshot()}
        evidence.emit('failure',result=failure)
        (args.output/'summary.json').write_text(json.dumps(failure,indent=2,allow_nan=False)+'\n')
        pool.shutdown(wait=False,cancel_futures=True)
        return 1
    finally:
        monitor_stop.set();monitor_thread.join(10)
        evidence.stream.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--mode',choices=('smoke','endurance'),required=True)
    parser.add_argument('--seconds',type=float,default=14400)
    parser.add_argument('--warmup',type=float,default=60)
    parser.add_argument('--cycle-seconds',type=float,default=900)
    parser.add_argument('--sample-seconds',type=float,default=10)
    args=parser.parse_args()
    if not __debug__ or sys.flags.optimize:
        parser.error('optimized Python disables required assertions; run without -O/-OO')
    if not (1<=args.seconds<=21600 and 0<=args.warmup<=300 and
            1<=args.cycle_seconds<=900 and 0.1<=args.sample_seconds<=30):parser.error('invalid bounded duration')
    if args.mode=='smoke' and args.seconds>300:parser.error('smoke duration exceeds five minutes')
    if args.mode=='endurance' and (args.seconds!=14400 or args.warmup!=60 or args.cycle_seconds!=900 or args.sample_seconds!=10):
        parser.error('endurance must match the predeclared four-hour protocol')
    return run(args)


if __name__=='__main__':raise SystemExit(main())
