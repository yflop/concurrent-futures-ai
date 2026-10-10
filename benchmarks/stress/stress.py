"""Independent bounded subprocess stress harness; does not change tested sources."""
import argparse, concurrent.futures as cf, gc, hashlib, json, os, pathlib, random, resource, subprocess, sys, threading, time, traceback, tracemalloc
from collections import Counter
from intelligent_futures import IntelligentExecutor, TaskHints, BackpressureError
from intelligent_futures.framework import TaskRegistry, TaskService, TaskServiceError

def identity(x): return x

def crash(): os._exit(17)

def terminal(s, ident, timeout=8):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        r=s.get(ident)
        if r['status'] in ('completed','failed','cancelled'): return r
        time.sleep(.0002)
    raise AssertionError('task failed to become terminal')

def invariant(s):
    c=s['counts']; assert c['submitted']==sum(c[k] for k in ('completed','failed','cancelled'))+s['pending'], s

def producers():
    ex=IntelligentExecutor(max_workers=8,max_pending=48,max_profile_keys=32,submit_timeout=5)
    lat=[]; lock=threading.Lock(); start=time.perf_counter()
    def produce(p):
        local=[]
        for i in range(700):
            t=time.perf_counter(); f=ex.submit_task(identity,(p,i),hints=TaskHints(key=f'key-{p}-{i%80}'))
            assert f.result(5)==(p,i); local.append(time.perf_counter()-t)
        with lock: lat.extend(local)
    with cf.ThreadPoolExecutor(24) as pool: list(pool.map(produce,range(24)))
    ex.shutdown(); snap=ex.snapshot(); invariant(snap); assert snap['pending']==0 and len(snap['profiles'])<=32
    lat.sort(); return dict(tasks=len(lat),seconds=time.perf_counter()-start,p50_ms=lat[len(lat)//2]*1000,p99_ms=lat[int(len(lat)*.99)]*1000,snapshot=snap)

def service_churn():
    registry=TaskRegistry(); registry.register('id',identity)
    s=TaskService(registry,max_workers=8,max_pending=48,max_tasks=96); counts=Counter(); lock=threading.Lock(); start=time.perf_counter()
    def producer(p):
        local=Counter()
        for i in range(350):
            while True:
                try: r=s.submit('id',{'x':[p,i]}); break
                except TaskServiceError as e:
                    assert e.code in ('busy','task_capacity'); local[e.code]+=1; time.sleep(.0005)
            if i%3==0: s.cancel(r['task_id'])
            out=terminal(s,r['task_id']); local[out['status']]+=1
            if out['status']=='completed': assert out['result']==[p,i]
            s.forget(r['task_id'])
        with lock: counts.update(local)
    with cf.ThreadPoolExecutor(20) as pool: list(pool.map(producer,range(20)))
    s.close(); snap=s.snapshot(); assert snap['retained']==snap['pending']==0; assert sum(snap['counts'][k] for k in ('completed','failed','cancelled'))==7000
    return dict(tasks=7000,seconds=time.perf_counter()-start,outcomes=dict(counts),snapshot=snap)

def physical_bound():
    gate=threading.Event(); reg=TaskRegistry(); reg.register('hold',lambda:gate.wait(5)); reg.register('id',identity)
    s=TaskService(reg,max_workers=1,max_pending=16,max_tasks=16)
    first=s.submit('hold',{}); end=time.monotonic()+3
    while s.get(first['task_id'])['status']!='running':
        assert time.monotonic()<end; time.sleep(.001)
    accepted=0; rejected=0
    for i in range(5000):
        try:
            r=s.submit('id',{'x':'x'*1000}); accepted+=1; assert s.cancel(r['task_id'])['cancel_accepted']; s.forget(r['task_id'])
        except TaskServiceError as e: assert e.code=='busy'; rejected+=1
    snap=s.snapshot(); assert accepted==15 and snap['pending']==16; gate.set(); s.close()
    return dict(accepted=accepted,rejected=rejected,physical_pending_before_release=snap['pending'],final=s.snapshot())

def shutdown_races():
    for iteration in range(60):
        ex=IntelligentExecutor(max_workers=3,max_pending=8,submit_timeout=.2); barrier=threading.Barrier(13); accepted=[]; lock=threading.Lock()
        def submitter():
            barrier.wait()
            for n in range(20):
                try: f=ex.submit(time.sleep,.0004)
                except (RuntimeError,BackpressureError): break
                with lock: accepted.append(f)
        threads=[threading.Thread(target=submitter) for _ in range(12)]
        for t in threads:t.start()
        barrier.wait(); time.sleep(.0005); ex.shutdown(wait=False,cancel_futures=bool(iteration%2)); ex.shutdown(wait=True,cancel_futures=True)
        for t in threads:t.join(3); assert not t.is_alive()
        assert all(f.done() for f in accepted); invariant(ex.snapshot()); assert ex.snapshot()['pending']==0
    return dict(iterations=60)

def service_shutdown_races():
    for iteration in range(50):
        reg=TaskRegistry(); reg.register('id',identity); s=TaskService(reg,max_workers=4,max_pending=16,max_tasks=100)
        barrier=threading.Barrier(9); errs=[]
        def produce():
            barrier.wait()
            for i in range(30):
                try:s.submit('id',{'x':i})
                except TaskServiceError as e:
                    if e.code not in ('busy','task_capacity','service_closed'): errs.append(e.code)
        ts=[threading.Thread(target=produce) for _ in range(8)]
        for t in ts:t.start()
        barrier.wait(); time.sleep(.0005); s.close(wait=False); s.close()
        for t in ts:t.join(3); assert not t.is_alive()
        snap=s.snapshot(); assert not errs and snap['pending']==0; assert snap['counts']['submitted']==sum(snap['counts'][k] for k in ('completed','failed','cancelled'))
    return dict(iterations=50)

def process_failures():
    ex=IntelligentExecutor(max_workers=2,process_workers=2,max_pending=12,policy='hints',submit_timeout=1)
    fs=[ex.submit_task(identity,i,hints=TaskHints(kind='cpu',process_safe=True)) for i in range(30)]
    assert [f.result(8) for f in fs]==list(range(30))
    bad=ex.submit_task(crash,hints=TaskHints(kind='cpu',process_safe=True))
    try:bad.result(8)
    except cf.process.BrokenProcessPool:pass
    else:raise AssertionError('process crash did not fail')
    try:ex.submit_task(identity,1,hints=TaskHints(kind='cpu',process_safe=True))
    except cf.process.BrokenProcessPool:pass
    else:raise AssertionError('broken pool unexpectedly accepted')
    ex.shutdown(wait=False); ex.shutdown(wait=True); invariant(ex.snapshot())
    return ex.snapshot()

def nested():
    ex=IntelligentExecutor(max_workers=4,max_pending=8,submit_timeout=.05)
    def outer():return ex.submit(identity,42).result(2)
    assert ex.submit(outer).result(3)==42
    # A done callback can submit safely when capacity is available.
    done=threading.Event(); values=[]
    def callback(f):values.append(ex.submit(identity,73)); done.set()
    f=ex.submit(identity,1); f.add_done_callback(callback); assert done.wait(3); assert values[0].result(3)==73; ex.shutdown(); invariant(ex.snapshot())
    # Saturated nested admission is bounded by explicitly configured timeout.
    ex=IntelligentExecutor(max_workers=1,max_pending=1,submit_timeout=.02); gate=threading.Event()
    def recursive():
        gate.wait(2)
        try:ex.submit(identity,1)
        except BackpressureError:return 'rejected'
    f=ex.submit(recursive); gate.set(); assert f.result(2)=='rejected'; ex.shutdown()
    return dict(safe_nested=True,callback_submit=True,saturated_finite_timeout=True)

def limits():
    reg=TaskRegistry(); reg.register('id',identity); s=TaskService(reg,max_input_bytes=128,max_result_bytes=64,max_tasks=8)
    cyc=[];cyc.append(cyc); deep=0
    for _ in range(70):deep=[deep]
    invalid=[float('nan'),float('inf'),'\ud800',{1:'x'},(1,2),object(),cyc,deep,'x'*200,10**500]
    for x in invalid:
        try:s.submit('id',{'x':x})
        except TaskServiceError as e:assert e.code=='invalid_arguments'
        else:raise AssertionError('invalid input accepted')
    r=s.submit('id',{'x':'x'*80}); assert terminal(s,r['task_id'])['error']['code']=='invalid_result'
    r=s.submit('id',{'x':'x'*62}); assert terminal(s,r['task_id'])['status']=='completed'
    r=s.submit('id',{'x':'x'*63}); assert terminal(s,r['task_id'])['error']['code']=='invalid_result'
    s.close(); return dict(invalid_inputs=len(invalid),result_boundary_bytes=64,snapshot=s.snapshot())

def retention():
    reg=TaskRegistry();reg.register('blob',lambda:'z'*65500);s=TaskService(reg,max_workers=4,max_pending=8,max_tasks=64,max_result_bytes=65536)
    tracemalloc.start(); base=tracemalloc.get_traced_memory()[0]; peaks=[]; retained=[]
    for cycle in range(5):
        ids=[]
        for i in range(64):
            r=s.submit('blob',{});terminal(s,r['task_id']);ids.append(r['task_id'])
        try:s.submit('blob',{})
        except TaskServiceError as e:assert e.code=='task_capacity'
        else:raise AssertionError('retention limit bypassed')
        peaks.append(tracemalloc.get_traced_memory()[0]-base)
        for ident in ids:s.forget(ident)
        gc.collect();retained.append(tracemalloc.get_traced_memory()[0]-base)
    s.close(); current,peak=tracemalloc.get_traced_memory();tracemalloc.stop()
    assert retained[-1]<1000000 and max(peaks)<8000000,(peaks,retained)
    return dict(cycles=5,bytes_at_capacity=peaks,bytes_after_forget=retained,tracemalloc_peak=peak,rss_max_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

def bounded_map():
    ex=IntelligentExecutor(max_workers=8,max_pending=16,submit_timeout=2)
    out=list(ex.map_bounded(identity,range(5000),buffersize=32));assert out==list(range(5000))
    def failing_source():
        yield from range(25)
        raise ValueError('source failed')
    iterator=ex.map_bounded(identity,failing_source(),buffersize=16)
    try:list(iterator)
    except ValueError:pass
    else:raise AssertionError('source exception swallowed')
    iterator=ex.map_bounded(identity,range(1000),buffersize=16);assert next(iterator)==0;iterator.close()
    gate=threading.Event()
    def wait_task(i):gate.wait(3);return i
    iterator=ex.map_bounded(wait_task,range(8),buffersize=8,timeout=.01)
    try:next(iterator)
    except TimeoutError:pass
    else:raise AssertionError('map deadline ignored')
    gate.set();ex.shutdown();invariant(ex.snapshot());assert ex.snapshot()['pending']==0
    return dict(ordered_results=5000,source_error_propagated=True,abandoned_iterator_closed=True,deadline_enforced=True,snapshot=ex.snapshot())

CASES={f.__name__:f for f in (bounded_map,producers,service_churn,physical_bound,shutdown_races,service_shutdown_races,process_failures,nested,limits,retention)}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--case',choices=CASES);p.add_argument('--output',default='stress-results.json');a=p.parse_args()
    if a.case:
        result=CASES[a.case]();print(json.dumps(result));sys.exit()
    results={};start=time.time()
    for name in CASES:
        t=time.monotonic()
        try:
            r=subprocess.run([sys.executable,__file__,'--case',name],capture_output=True,text=True,timeout=90)
            results[name]={'ok':r.returncode==0,'seconds':time.monotonic()-t,'returncode':r.returncode,'result':json.loads(r.stdout) if r.returncode==0 else None,'stderr':r.stderr}
        except subprocess.TimeoutExpired as e:results[name]={'ok':False,'timeout':90,'stdout':str(e.stdout),'stderr':str(e.stderr)}
        print(name,results[name]['ok'],flush=True)
        pathlib.Path(a.output).write_text(json.dumps({'python':sys.version,'source_path':os.environ.get('PYTHONPATH'),'started_epoch':start,'results':results},indent=2))
    sys.exit(not all(r['ok'] for r in results.values()))
