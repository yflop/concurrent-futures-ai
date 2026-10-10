"""Regression for historical cancelled-queue payload retention (see 0.4 history).

Retain every cancelled public Future deliberately. Owned queued payloads must
still be released while the sole real worker remains blocked. Caller-held Future
objects consume memory; this is an item/payload-lifetime check, not a byte cap.
"""
from __future__ import annotations
import argparse
import gc
import json
from pathlib import Path
import threading
import tracemalloc
import weakref
from intelligent_futures import IntelligentExecutor

class Payload:
    def __init__(self,size):self.data=bytes(size)

def consume(payload):return len(payload.data)

def run(count=10000,payload_bytes=4096):
    gate=threading.Event();ready=threading.Event()
    def blocked():
        ready.set()
        if not gate.wait(60):raise RuntimeError('churn gate timed out')
    executor=IntelligentExecutor(max_workers=1,max_pending=2,submit_timeout=0)
    current=executor.submit(blocked)
    if not ready.wait(10):raise RuntimeError('churn worker did not start')
    futures=[];references=[];tracemalloc.start();base=tracemalloc.get_traced_memory()[0]
    try:
        for _ in range(count):
            payload=Payload(payload_bytes);references.append(weakref.ref(payload))
            future=executor.submit(consume,payload);del payload
            if not future.cancel():raise AssertionError('owned queued cancellation failed')
            futures.append(future)
        gc.collect();held=tracemalloc.get_traced_memory()[0]-base;snapshot=executor.snapshot()
        retained_payloads=sum(ref() is not None for ref in references)
        if retained_payloads:raise AssertionError('cancelled Future retained queued payload')
        if snapshot['pending']!=1 or snapshot['physical']['retained']!=1 or snapshot['physical']['peak_retained']>2:
            raise AssertionError('physical/logical admission bound failed')
        if snapshot['physical']['peak_in_flight']['thread']!=1:
            raise AssertionError('backend dispatch budget exceeded')
        gate.set();executor.shutdown(wait=True);current.result();gc.collect()
        final=executor.snapshot();drained=tracemalloc.get_traced_memory()[0]-base
        if final['pending'] or final['physical']['retained']:
            raise AssertionError('final drain did not release scheduler items')
        return {'classification':'bounded cancellation payload-retention regression',
            'cancelled':count,'payload_bytes_each':payload_bytes,'cancelled_futures_deliberately_retained':len(futures),
            'live_cancelled_payloads_before_drain':retained_payloads,
            'traced_bytes_held_including_caller_retained_futures':held,
            'traced_bytes_after_drain_including_caller_retained_futures':drained,
            'before_drain':snapshot,'after_drain':final,'accepted':True,
            'limits':'Explicit shutdown; physical item count is not an RSS/byte cap; no task preemption or callback/deadlock guarantee.'}
    finally:
        gate.set();executor.shutdown(wait=True);tracemalloc.stop()

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path)
    parser.add_argument('--count',type=int,default=10000);args=parser.parse_args()
    if not 1<=args.count<=100000:parser.error('count must be1..100000')
    report=run(args.count);text=json.dumps(report,indent=2)+'\n'
    if args.output:args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(text)
    else:print(text)

if __name__=='__main__':main()
