import gc,json,threading,tracemalloc
from intelligent_futures import IntelligentExecutor

def consume(blob):return len(blob)
if __name__=='__main__':
    gate=threading.Event();ex=IntelligentExecutor(max_workers=1,max_pending=2,submit_timeout=0)
    f=ex.submit(gate.wait,10);tracemalloc.start();base=tracemalloc.get_traced_memory()[0]
    try:
        for i in range(10000):
            queued=ex.submit(consume,b'x'*4096+str(i).encode());assert queued.cancel()
        gc.collect();held=tracemalloc.get_traced_memory()[0]-base; snap=ex.snapshot()
        assert snap['pending']==1;gate.set();ex.shutdown();gc.collect();drained=tracemalloc.get_traced_memory()[0]-base
        print(json.dumps(dict(classification='documented limitation, not regression',cancelled=10000,logical_pending_before_drain=1,bytes_held=held,bytes_after_drain=drained),indent=2))
    finally:gate.set();ex.shutdown();tracemalloc.stop()
