"""Fresh-process diagnostic only; no change to the predeclared endurance gate."""
import argparse
from contextlib import ExitStack
import ctypes
import gc
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import tracemalloc
from benchmarks.real_world import NAMES,module_for,execute_batch
from benchmarks.real_world.optimizer_endurance import census,stable_digest,identity
from benchmarks.real_world.run import make_executor,measure_pool
from intelligent_futures import TaskHints
from intelligent_futures.tuning import Configuration


def objects():
    counts={}
    for value in gc.get_objects():
        cls=type(value); module=getattr(cls,'__module__',None); label=getattr(cls,'__name__',None)
        if not isinstance(module,str) or not isinstance(label,str):continue
        name=module+'.'+label
        if name.startswith(('intelligent_futures.','concurrent.futures.')):
            counts[name]=counts.get(name,0)+1
    return counts


def run(engine,output,seconds):
    os.sched_setaffinity(0,sorted(os.sched_getaffinity(0))[:8])
    report={'engine':engine,'epoch_seconds':seconds,'scope':'short fresh-process allocation diagnosis; not a repeat or replacement of 20-minute endurance','epochs':[]}
    def save():output.write_text(json.dumps(report,indent=2)+'\n')
    warm=make_executor('intelligent',Configuration('process',1,1))
    warm.submit_task(identity,1,hints=TaskHints(kind='cpu',process_safe=True)).result(20);warm.shutdown();del warm
    with tempfile.TemporaryDirectory(prefix='memory-diagnostic-') as tmp,ExitStack() as stack:
        root=Path(tmp);datasets={}
        for name in NAMES:
            module=module_for(name);path=root/name;path.mkdir()
            if hasattr(module,'fixture'):stack.enter_context(module.fixture(path))
            tasks=module.prepare(name,path/'data',6,761213,scale='representative')
            rows=execute_batch((name,tasks))
            if any(r['error'] for r in rows):raise AssertionError(name)
            datasets[name]=(tasks,stable_digest([r['value'] for r in rows]))
        del rows
        gc.collect();report['baseline']=census();report['baseline_objects']=objects()
        tracemalloc.start(5);baseline=tracemalloc.take_snapshot()
        for epoch in range(22):
            name=NAMES[epoch%len(NAMES)];backend='thread' if epoch<len(NAMES) else 'process'
            config=Configuration(backend,2,1 if epoch%2==0 else 4)
            tasks,expected=datasets[name];pool=None
            if engine!='direct':pool=make_executor(engine,config)
            started=time.perf_counter();passes=errors=mismatches=0
            try:
                while time.perf_counter()-started<seconds:
                    if engine=='direct':
                        result=execute_batch((name,tasks))
                        errors+=sum(r['error'] is not None for r in result)
                        mismatches+=stable_digest([r['value'] for r in result])!=expected
                        del result
                    else:
                        row=measure_pool(pool,engine,config,name,tasks)
                        errors+=len(row['errors']);mismatches+=row['result_sha256']!=expected
                        del row
                    passes+=1
            finally:
                if pool is not None:pool.shutdown(wait=True)
            del pool
            current,peak=tracemalloc.get_traced_memory()
            record={'epoch':epoch,'workload':name,'backend':backend if engine!='direct' else 'direct','passes':passes,'errors':errors,'mismatches':mismatches,'before_gc':census(),'traced_current_bytes':current,'traced_peak_bytes':peak,'tracemalloc_overhead_bytes':tracemalloc.get_tracemalloc_memory()}
            record['gc_collected']=gc.collect();record['after_gc']=census();record['live_scheduler_objects']=objects()
            snapshot=tracemalloc.take_snapshot()
            record['top_python_live_growth']=[{'traceback':str(x.traceback),'size_diff':x.size_diff,'count_diff':x.count_diff} for x in snapshot.compare_to(baseline,'lineno')[:12]]
            del snapshot
            report['epochs'].append(record);save();print(engine,epoch,name,record['after_gc']['rss_bytes'],current,flush=True)
    gc.collect();report['final_before_trim']=census();report['final_objects']=objects()
    report['final_traced_bytes']=tracemalloc.get_traced_memory();report['tracemalloc_overhead_bytes']=tracemalloc.get_tracemalloc_memory()
    del baseline;tracemalloc.stop();gc.collect();report['after_tracemalloc_stop']=census()
    try:
        trim=ctypes.CDLL(None).malloc_trim;trim.argtypes=[ctypes.c_size_t];trim.restype=ctypes.c_int
        report['diagnostic_malloc_trim_return']=trim(0)
    except (AttributeError,OSError) as exc:report['diagnostic_malloc_trim_unavailable']=type(exc).__name__
    report['after_diagnostic_trim']=census();report['status']='complete';save()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--engine',choices=['intelligent','stdlib','direct'],required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--seconds',type=float,default=5.);a=p.parse_args();run(a.engine,a.output,a.seconds)
