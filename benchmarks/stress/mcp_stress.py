import asyncio,json,os,pathlib,sys,time
from mcp import Client
from mcp.client.stdio import StdioServerParameters
ROOT=pathlib.Path(os.environ['TEST_ROOT'])

def payload(r):
    p=json.loads(r.content[0].text);assert p==r.structured_content;return p

async def main():
    counts={'calls':0,'rounds':0,'rejected':0};started=time.perf_counter()
    parameters=StdioServerParameters(command=sys.executable,args=[str(ROOT/'tests/mcp_fixture.py')],env={'PYTHONPATH':str(ROOT/'src')})
    for connection in range(8):
        async with Client(parameters,read_timeout_seconds=8,cache=None) as c:
            for round in range(8):
                rs=await asyncio.gather(*(c.call_tool('submit_task',{'name':'add','arguments':{'left':i}}) for i in range(32)))
                counts['calls']+=32;ids=[]
                for r in rs:
                    d=payload(r)
                    if r.is_error:assert d['error']['code'] in ('busy','task_capacity');counts['rejected']+=1
                    else:ids.append(d['task_id'])
                async def collect(ident):
                    for n in range(100):
                        r=payload(await c.call_tool('get_task',{'task_id':ident}));counts['calls']+=1
                        if r['status'] in ('completed','failed','cancelled'):break
                        await asyncio.sleep(.002)
                    assert r['status']=='completed';assert isinstance(r['result'],int)
                    assert payload(await c.call_tool('forget_task',{'task_id':ident}))['forgotten'];counts['calls']+=1
                await asyncio.gather(*(collect(i) for i in ids))
                invalid=[('list_tasks',{'limit':v}) for v in (None,True,1.2,'1',0,101,-1,[],{})]
                invalid += [('submit_task',{'name':'add','arguments':{'left':v}}) for v in (None,True,1.2,'1',[],{},'x'*10000)]
                invalid += [('get_task',{'task_id':v}) for v in (None,True,1.2,[],{},'missing')]
                rr=await asyncio.gather(*(c.call_tool(n,a) for n,a in invalid))
                for r in rr:assert r.is_error;assert 'error' in payload(r)
                counts['calls']+=len(invalid);counts['rounds']+=1
            m=payload(await c.call_tool('executor_metrics',{}));assert m['retained']==m['pending']==0
    counts['seconds']=time.perf_counter()-started;return counts

if __name__=='__main__':print(json.dumps(asyncio.run(asyncio.wait_for(main(),120)),indent=2))
