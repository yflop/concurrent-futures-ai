"""Real deterministic JSON/gzip transformation comparison, equal 4-worker budgets."""
import concurrent.futures as cf,gzip,hashlib,json,multiprocessing,pathlib,random,statistics,time
from intelligent_futures import IntelligentExecutor,TaskHints

def transform(payload):
    rows=json.loads(payload)
    selected=[dict(id=r['id'],total=sum(r['values']),name=r['name'].upper()) for r in rows if r['id']%3]
    selected.sort(key=lambda r:(r['total'],r['id']))
    encoded=json.dumps(selected,separators=(',',':')).encode()
    return hashlib.sha256(gzip.compress(encoded,mtime=0)).hexdigest()

def main():
    payloads=[json.dumps([{'id':i,'name':f'record-{i}','values':[(i*j+k)%997 for j in range(24)]} for i in range(1200)]).encode() for k in range(16)]
    expected=[transform(p) for p in payloads];samples={};rng=random.Random(42)
    for rep in range(3):
        engines=['sequential','thread4','process4','intelligent-thread4','intelligent-process4'];rng.shuffle(engines)
        for engine in engines:
            start=time.perf_counter()
            if engine=='sequential':out=list(map(transform,payloads))
            else:
                if engine=='thread4':ex=cf.ThreadPoolExecutor(4)
                elif engine=='process4':ex=cf.ProcessPoolExecutor(4,mp_context=multiprocessing.get_context('spawn'))
                elif engine=='intelligent-thread4':ex=IntelligentExecutor(max_workers=4,max_pending=8,policy='threads')
                else:ex=IntelligentExecutor(max_workers=1,process_workers=4,max_pending=8,policy='hints')
                with ex:
                    if engine=='intelligent-process4':fs=[ex.submit_task(transform,p,hints=TaskHints(key='json-gzip',kind='cpu',process_safe=True)) for p in payloads]
                    else:fs=[ex.submit(transform,p) for p in payloads]
                    out=[f.result(20) for f in fs]
            duration=time.perf_counter()-start;assert out==expected;samples.setdefault(engine,[]).append(duration)
    return dict(workload='parse/filter/sort/aggregate 1,200 JSON records and gzip + SHA256 per task',tasks=16,payload_bytes=[len(p) for p in payloads],timing='cold end-to-end including pool creation/spawn/shutdown',repeats=3,raw_seconds=samples,median_seconds={k:statistics.median(v) for k,v in samples.items()},cgroup_cpu_max=(pathlib.Path('/sys/fs/cgroup/cpu.max').read_text().strip() if pathlib.Path('/sys/fs/cgroup/cpu.max').exists() else 'unavailable'))
if __name__=='__main__':print(json.dumps(main(),indent=2))
