"""Repeated bounded trace/cancellation/shutdown stress; no performance claim.

Run serially with other experiments. Process trials use spawn and explicit hints.
"""
from __future__ import annotations
import argparse
from concurrent.futures import CancelledError, ThreadPoolExecutor, wait
import gc
import hashlib
import json
import multiprocessing
from pathlib import Path
import threading
import time

from intelligent_futures import IntelligentExecutor, TaskHints


def task(value):
    return value * value


def state():
    try:
        import psutil
        process=psutil.Process()
        return {'rss_bytes':process.memory_info().rss,
                'descendants':len(process.children(recursive=True)),
                'fds':process.num_fds() if hasattr(process,'num_fds') else None,
                'threads':len(threading.enumerate())}
    except ImportError:
        return {'rss_bytes':None,'descendants':len(multiprocessing.active_children()),
                'fds':None,'threads':len(threading.enumerate())}


def run(cycles, process_cycles):
    before=state();started=time.perf_counter()
    accepted=completed=cancelled=trace_evictions=0
    peaks={'retained':0,'pending':0,'trace':0}
    for cycle in range(cycles):
        pool=IntelligentExecutor(max_workers=2,max_pending=8,policy='threads',
                                 submit_timeout=10,trace_capacity=16)
        def producer(offset):
            rows=[]
            for value in range(offset,offset+64):
                future=pool.submit(task,value)
                if value%5==0:future.cancel()
                rows.append((value,future))
                assert len(pool.routing_trace())<=16
            return rows
        with ThreadPoolExecutor(max_workers=4) as producers:
            groups=[producers.submit(producer,i*64) for i in range(4)]
            pairs=[pair for group in groups for pair in group.result()]
        # Exercise repeated non-waiting then waiting shutdown and queue cancel.
        pool.shutdown(wait=False,cancel_futures=cycle%2==0)
        pool.shutdown(wait=True)
        wait([future for _,future in pairs])
        for value,future in pairs:
            accepted+=1
            if future.cancelled():cancelled+=1
            else:
                assert future.result()==value*value
                completed+=1
        snapshot=pool.snapshot();trace=pool.routing_counters()
        assert snapshot['pending']==0
        assert trace['recorded']==len(pairs)
        assert trace['retained']<=16
        trace_evictions+=trace['dropped']
        # Public scheduler snapshots are exact item counts, not memory budgets.
        peaks['trace']=max(peaks['trace'],trace['retained'])
        peaks['pending']=max(peaks['pending'],snapshot['counts']['peak_pending'])
        peaks['retained']=max(peaks['retained'],snapshot['physical']['peak_retained'])
        assert snapshot['counts']['peak_pending']<=8 and snapshot['physical']['peak_retained']<=8
        del pool,pairs,groups
    process_completed=0
    for cycle in range(process_cycles):
        with IntelligentExecutor(max_workers=1,process_workers=2,max_pending=4,
                                 policy='process',trace_capacity=4) as pool:
            futures=[pool.submit_task(task,value,hints=TaskHints(process_safe=True)) for value in range(8)]
            assert [future.result() for future in futures]==[value*value for value in range(8)]
            assert all(row.backend=='process' for row in pool.routing_trace())
            process_completed+=len(futures)
        del pool,futures
    gc.collect();after=state()
    assert after['threads']<=before['threads']
    # multiprocessing may retain a single resource-tracker child for process
    # lifetime. It is support state rather than a worker pool leak.
    assert after['descendants']<=before['descendants']+1
    growth=None if before['rss_bytes'] is None else after['rss_bytes']-before['rss_bytes']
    assert growth is None or growth<=128*1024**2
    if after['fds'] is not None:assert after['fds']<=before['fds']+4
    root=Path(__file__).resolve().parents[1]
    hashes={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (root/'src/intelligent_futures').glob('*.py')}
    return {'status':'passed','cycles':cycles,'process_cycles':process_cycles,
        'accepted_thread_tasks':accepted,'completed_thread_tasks':completed,
        'cancelled_thread_tasks':cancelled,'completed_process_tasks':process_completed,
        'trace_evictions':trace_evictions,'observed_item_peaks':peaks,
        'before':before,'after':after,'rss_growth_bytes':growth,
        'rss_growth_gate_bytes':128*1024**2,'elapsed_seconds':time.perf_counter()-started,
        'source_sha256':hashes,'limitations':'Observed test gate, not hard RSS enforcement or universal lifecycle proof.'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cycles',type=int,default=128)
    parser.add_argument('--process-cycles',type=int,default=8)
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    if not 1<=args.cycles<=10000 or not 0<=args.process_cycles<=1000:parser.error('invalid cycles')
    result=run(args.cycles,args.process_cycles)
    Path(args.output).write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
