import concurrent.futures as cf,gc,json,threading,time,tracemalloc
from intelligent_futures import IntelligentExecutor,TaskHints
from intelligent_futures.framework import TaskRegistry,TaskService,TaskServiceError

def run(seconds=20):
    reg=TaskRegistry();reg.register('echo',lambda x:x)
    s=TaskService(reg,max_workers=8,max_pending=32,max_tasks=96)
    end=time.monotonic()+seconds;counts={'completed':0,'cancelled':0,'busy':0,'task_capacity':0}; lock=threading.Lock(); errors=[];samples=[]
    def producer(k):
        local=dict.fromkeys(counts,0); n=0
        try:
            while time.monotonic()<end:
                try:r=s.submit('echo',{'x':[k,n,'x'*1024]})
                except TaskServiceError as e:local[e.code]+=1;time.sleep(.0005);continue
                ident=r['task_id'];n+=1
                if n%2:s.cancel(ident)
                while True:
                    r=s.get(ident)
                    if r['status'] in ('completed','cancelled','failed'):break
                    time.sleep(.0005)
                assert r['status']!='failed'
                if r['status']=='completed':assert r['result']==[k,n-1,'x'*1024]
                local[r['status']]+=1;s.forget(ident)
        except BaseException as e:errors.append(repr(e))
        with lock:
            for key,value in local.items():counts[key]+=value
    tracemalloc.start();start=time.monotonic()
    with cf.ThreadPoolExecutor(32) as pool:
        futures=[pool.submit(producer,k) for k in range(32)]
        while time.monotonic()<end:
            snap=s.snapshot();assert snap['pending']<=32 and snap['retained']<=96
            current,peak=tracemalloc.get_traced_memory();samples.append({'seconds':time.monotonic()-start,'memory_current':current,'memory_peak':peak,'pending':snap['pending'],'retained':snap['retained']});time.sleep(1)
        for f in futures:f.result(5)
    s.close();gc.collect();current,peak=tracemalloc.get_traced_memory();tracemalloc.stop()
    assert not errors,errors
    assert s.snapshot()['pending']==s.snapshot()['retained']==0
    return dict(seconds=time.monotonic()-start,counts=counts,memory_final=current,memory_peak=peak,samples=samples,snapshot=s.snapshot())
if __name__=='__main__':print(json.dumps(run(),indent=2))
