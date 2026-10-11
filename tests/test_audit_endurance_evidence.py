"""Synthetic consistency tests only. These fixtures are not empirical evidence."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('audit_endurance_evidence',ROOT/'tools/audit_endurance_evidence.py')
audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)
COMMIT='synthetic-commit-for-unit-tests-only'
RUN_ID='synthetic-run-for-unit-tests-only'


def outcome(accepted,success,fail,cancel,cancel_false=0):
    return dict(accepted=accepted,attempted=accepted,rejected=0,success=success,
        expected_failure=fail,unexpected_failure=0,cancelled=cancel,
        cancel_true=cancel,cancel_false=cancel_false,cancel_attempted=cancel+cancel_false)


def kinds(cycle):
    b=cycle+1
    return {'blocking':outcome(40*b,36*b,0,4*b),
        'cpu':outcome(35*b,32*b,0,3*b,b),
        'payload':outcome(20*b,18*b,0,2*b),
        'failure':outcome(5*b,0,4*b,b),
        'health':{'accepted':cycle,'attempted':cycle,'success':cycle}}


def counts(cycle):
    return {key:sum(kind.get(key,0) for kind in kinds(cycle).values()) for key in audit.COUNT_KEYS}


def census():
    return {'parent_rss':20*1024**2,'tree_rss':60*1024**2,'parent_fds':16,
        'python_threads':6,'parent_native_threads':6,'task_children':2,
        'children':[{'pid':101,'created':1000.0,'resource_tracker':False},
                    {'pid':102,'created':1000.1,'resource_tracker':False},
                    {'pid':103,'created':999.0,'resource_tracker':True}]}


def snapshot(cycle):
    c=counts(cycle)
    return {'pending':0,'profile_count':16,'adaptive_tracked_keys':16,
        'counts':{'submitted':c['accepted'],'completed':c['success'],
                  'failed':c['expected_failure'],'cancelled':c['cancelled'],'peak_pending':32},
        'physical':{'retained':0,'peak_retained':32,
                    'in_flight':{'thread':0,'process':0},'peak_in_flight':{'thread':2,'process':2}},
        'routing':{'recorded':c['accepted'],'retained':64,'dropped':c['accepted']-64}}


def fixture():
    """Build a full, plainly synthetic successful-schema fixture in memory."""
    docs={'OPERATIONAL-ENDURANCE-PROTOCOL.md':b'SYNTHETIC PROTOCOL FOR UNIT TESTS\n',
          'OPERATIONAL-ENDURANCE-RESTART.md':b'SYNTHETIC ADDENDUM FOR UNIT TESTS\n'}
    sha=lambda b:hashlib.sha256(b).hexdigest()
    runtime={'__init__.py':sha(b'synthetic runtime')}
    frozen={'tools/operational_endurance.py':sha(b'synthetic harness'),
        'tools/supervise_endurance.py':sha(b'synthetic supervisor'),
        'docs/OPERATIONAL-ENDURANCE-PROTOCOL.md':sha(docs['OPERATIONAL-ENDURANCE-PROTOCOL.md'])}
    digest=sha(json.dumps(runtime,sort_keys=True,separators=(',',':')).encode())
    plan={'runtime_files':runtime,'runtime_manifest_sha256':digest,'frozen_files':frozen,
          'python_version':'3.12.14','psutil_version':'7.2.2','measured_seconds':14400,
          'warmup_seconds':60,'cycle_seconds':900,'sample_seconds':10,'mode':'endurance',
          'minimum_available_cpus':4,'minimum_memory_bytes':8*1024**3,'minimum_free_disk_bytes':2*1024**3}
    source_identity={key:plan[key] for key in ('runtime_files','runtime_manifest_sha256','frozen_files')}
    identity={'version':'0.5.1','python_optimization_level':0,'runtime_sha256':runtime,
        'harness_sha256':frozen['tools/operational_endurance.py'],
        'protocol_sha256':frozen['docs/OPERATIONAL-ENDURANCE-PROTOCOL.md'],
        'python':'3.12.14 SYNTHETIC TEST INTERPRETER','psutil':'7.2.2',
        'platform':'Linux-SYNTHETIC-TEST','affinity':[0,1,2,3]}
    events=[{'event':'start','identity':identity,'elapsed_seconds':0,'unix_time':1000,
        'arguments':{'seconds':14400,'warmup':60,'cycle_seconds':900,'sample_seconds':10,'mode':'endurance'}}]
    rows=[]
    for cycle in range(17):
        row={'event':'warm_baseline' if cycle==0 else 'cycle_quiescent',
             'census':census(),'snapshot':snapshot(cycle),'counts':counts(cycle),
             'by_kind':kinds(cycle),'elapsed_seconds':60+cycle*901,
             'workload_seconds':cycle*900,'unix_time':1060+cycle*901}
        rows.append(row);events.append(row)
    last=copy.deepcopy(rows[-1]);last['event']='final_quiescent';last['elapsed_seconds']+=1
    last['workload_seconds']=14400.2
    rows.append(last);events.append(last)
    diagnostics=[]
    for cycle in range(1,17):
        row={'cycle':cycle,'status':'passed','accepted':38,'successes':22,
             'expected_failures':9,'cancelled':7,'rejected':4,'elapsed_seconds':0.5,
             'predictive_probe':{'tasks':16,'counts':{'completed':16},'profile_count':4}}
        diagnostics.append(row)
        events.append({'event':'diagnostic_end','unix_time':1000+cycle*901,**row})
    before={'parent_rss':19*1024**2,'python_threads':1,'parent_fds':5}
    after={'parent_rss':20*1024**2,'python_threads':1,'parent_fds':6,'task_children':0,
           'children':[{'pid':103,'created':999.0,'resource_tracker':True}]}
    trends={field:{'first':rows[0]['census'][field],'last':rows[-1]['census'][field],
        'delta':0,'ols_per_hour':audit.ols(rows,field)} for field in audit.TREND_KEYS}
    summary={'status':'passed','mode':'endurance','identity':identity,'workload_seconds':14400.2,
        'required_workload_seconds':14400,'counts':counts(16),'warmup_counts':counts(0),
        'measured_counts':{key:counts(16)[key]-counts(0)[key] for key in audit.COUNT_KEYS},
        'by_kind':kinds(16),'callbacks':{'total':counts(16)['accepted']},'diagnostics':diagnostics,
        'quiescent_samples':18,'quiescent_resource_trends':trends,'before':before,'after':after,
        'peaks':{key:census()[key] for key in audit.TREND_KEYS|{'parent_native_threads'}}}
    events.append({'event':'complete','result':summary,'elapsed_seconds':14478,'unix_time':15478})
    watchdog={'status':'passed','returncode':0,'stop_reason':None,'errors':[],
        'remaining_owned_processes':[],'wall_limit_seconds':15000,
        'sampled_rss_limit_bytes':1024**3,'progress_limit_seconds':90,'samples':2,
        'sampled_peak_tree_rss_bytes':60*1024**2}
    terminal={'status':'passed','commit':COMMIT,'run_id':RUN_ID,'run_attempt':'1',
        'identity_after':source_identity,'endurance_step_outcome':'success',
        'preflight_step_outcome':'success','tests_step_outcome':'success',
        'cleanup_verified_for_this_run':True,'workload_seconds':summary['workload_seconds']}
    source={'commit':COMMIT,'files':{**{'src/intelligent_futures/'+k:v for k,v in runtime.items()},
        **frozen,'docs/OPERATIONAL-ENDURANCE-RESTART.md':sha(docs['OPERATIONAL-ENDURANCE-RESTART.md'])}}
    context={'GITHUB_SHA':COMMIT,'GITHUB_RUN_ID':RUN_ID,'GITHUB_RUN_ATTEMPT':'1'}
    preflight={'status':'passed','host_check_status':'passed','identity':source_identity,
        'python':identity['python'],'platform':identity['platform'],'cpu_affinity':identity['affinity'],
        'python_optimization_level':0,'psutil_version':'7.2.2','machine':'x86_64',
        'memory_bytes':16*1024**3,'free_disk_bytes':14*1024**3}
    observed={k:v for k,v in preflight.items() if k!='host_check_status'};observed['status']='checking'
    values={'endurance/summary.json':summary,'watchdog.json':watchdog,'ci-terminal.json':terminal,
        'preflight.json':preflight,'host-observed.json':observed,
        'endurance/checkpoint.json':events[-1],
        'run-context.json':context,'source-manifest.json':source,
        'operational-endurance-github-v1-plan.json':plan}
    return {'plan':plan,'values':values,'events':events,'docs':docs,
        'watchdog_samples':[{'elapsed_seconds':0.1,'rss_bytes':20*1024**2},
                            {'elapsed_seconds':14460,'rss_bytes':60*1024**2}]}


def write_fixture(root,data):
    root.mkdir(exist_ok=True)
    for relative,value in data['values'].items():
        if relative=='ci-terminal.json':continue
        path=root/relative;path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text(json.dumps(value,allow_nan=False)+'\n')
    (root/'endurance/events.jsonl').write_text(''.join(json.dumps(row,allow_nan=False)+'\n' for row in data['events']))
    (root/'watchdog.samples.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in data['watchdog_samples']))
    for relative,value in data['docs'].items():(root/relative).write_bytes(value)
    terminal=copy.deepcopy(data['values']['ci-terminal.json'])
    terminal['evidence_files']={p.relative_to(root).as_posix():{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),
        'bytes':p.stat().st_size} for p in root.rglob('*') if p.is_file() and p.name!='ci-terminal.json'}
    (root/'ci-terminal.json').write_text(json.dumps(terminal,allow_nan=False)+'\n')


class AuditTests(unittest.TestCase):
    def audit(self,root,plan,**kwargs):
        return audit.audit(root,plan,expected_commit=kwargs.pop('expected_commit',COMMIT),
            expected_run_id=kwargs.pop('expected_run_id',RUN_ID),**kwargs)
    def reject(self,mutate,message):
        data=fixture();mutate(data)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);write_fixture(root,data)
            with self.assertRaisesRegex(ValueError,message):self.audit(root,data['plan'])
    def test_full_synthetic_success(self):
        data=fixture()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);write_fixture(root,data)
            result=self.audit(root,data['plan'],expected_source_files=data['values']['source-manifest.json']['files'])
        self.assertEqual(result['status'],'passed')
        self.assertEqual(result['counts']['accepted'],1716)
        self.assertEqual(result['measured_counts']['accepted'],1616)
        self.assertEqual(result['diagnostic_checkpoints'],16)
    def test_start_arguments_match_plan(self):
        self.reject(lambda d:d['events'][0]['arguments'].update(warmup=1),'start arguments')
    def test_measured_host_matches_preflight(self):
        def mutate(d):d['values']['endurance/summary.json']['identity']['affinity']=[0]
        self.reject(mutate,'host mismatch')
    def test_per_kind_defect_cannot_hide_in_aggregate(self):
        def mutate(d):
            by_kind=d['values']['endurance/summary.json']['by_kind']
            by_kind['blocking']['success']-=1;by_kind['cpu']['success']+=1
        self.reject(mutate,'terminal outcomes')
    def test_watchdog_peak_recomputed(self):
        self.reject(lambda d:d['values']['watchdog.json'].update(sampled_peak_tree_rss_bytes=1),'watchdog sampled peak')
    def test_active_peak_recomputed(self):
        self.reject(lambda d:d['values']['endurance/summary.json']['peaks'].update(parent_rss=1),'resource peak mismatch')
    def test_denominators_reconcile(self):audit.reconciled(outcome(100,80,5,15,5))
    def test_missing_failure_is_not_dropped(self):
        c=outcome(100,80,5,15);c['expected_failure']=4
        with self.assertRaisesRegex(ValueError,'terminal outcomes'):audit.reconciled(c)
    def test_unexpected_failure_rejects(self):
        c=outcome(100,80,5,15);c['success']-=1;c['unexpected_failure']=1
        with self.assertRaisesRegex(ValueError,'unexpected'):audit.reconciled(c)
    def test_boolean_is_not_integer_count(self):
        c=outcome(100,80,5,15);c['rejected']=True
        with self.assertRaisesRegex(ValueError,'count type'):audit.reconciled(c)
    def test_cancel_failure_is_retained(self):
        c=outcome(100,80,5,15,5);c['cancel_false']=0
        with self.assertRaisesRegex(ValueError,'cancel attempts'):audit.reconciled(c)
    def test_ols_uses_recorded_elapsed_hours(self):
        rows=[{'elapsed_seconds':i*1800,'census':{'rss':100+i*4}} for i in range(5)]
        self.assertEqual(audit.ols(rows,'rss'),8)
    def test_smoke_cannot_pass(self):
        self.reject(lambda d:d['values']['endurance/summary.json'].update(mode='smoke'),'smoke')
    def test_missing_terminal_report_is_not_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileNotFoundError):self.audit(directory,{})
    def test_missing_complete(self):
        self.reject(lambda d:d['events'].pop(),'complete event')
    def test_complete_does_not_match(self):
        def mutate(d):d['events'][-1]['result']=dict(d['values']['endurance/summary.json'],workload_seconds=0)
        self.reject(mutate,'complete-event result')
    def test_diagnostics_do_not_match(self):
        def mutate(d):d['values']['endurance/summary.json']['diagnostics'][0]=dict(d['values']['endurance/summary.json']['diagnostics'][0],elapsed_seconds=2)
        self.reject(mutate,'diagnostic rows differ')
    def test_missing_measured_key(self):
        self.reject(lambda d:d['values']['endurance/summary.json']['measured_counts'].pop('expected_failure'),'count keys')
    def test_missing_trend_key(self):
        self.reject(lambda d:d['values']['endurance/summary.json']['quiescent_resource_trends'].pop('parent_rss'),'trend keys')
    def test_wrong_commit(self):
        self.reject(lambda d:d['values']['ci-terminal.json'].update(commit='wrong'),'commit mismatch')
    def test_wrong_run(self):
        self.reject(lambda d:d['values']['run-context.json'].update(GITHUB_RUN_ID='wrong'),'run ID mismatch')
    def test_rerun_not_original_attempt(self):
        self.reject(lambda d:d['values']['run-context.json'].update(GITHUB_RUN_ATTEMPT='2'),'run attempt mismatch')
    def test_frozen_source_mismatch(self):
        self.reject(lambda d:d['values']['source-manifest.json']['files'].update({'tools/supervise_endurance.py':'wrong'}),'tracked source')
    def test_terminal_identity_mismatch(self):
        def mutate(d):d['values']['ci-terminal.json']['identity_after']=dict(d['values']['ci-terminal.json']['identity_after'],frozen_files={})
        self.reject(mutate,'source identity mismatch')
    def test_nonfinite_json_and_jsonl(self):
        for text in ('NaN','Infinity','-Infinity','1e400'):
            with self.subTest(text=text),self.assertRaisesRegex(ValueError,'nonfinite'):audit.loads(text)
        data=fixture()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);write_fixture(root,data)
            with (root/'endurance/events.jsonl').open('a') as stream:stream.write('{"event":"bad","value":NaN}\n')
            with self.assertRaisesRegex(ValueError,'nonfinite'):self.audit(root,data['plan'])
    def test_unlisted_artifact_rejects(self):
        data=fixture()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);write_fixture(root,data);(root/'unlisted.txt').write_text('not inventoried')
            with self.assertRaisesRegex(ValueError,'inventory incomplete'):self.audit(root,data['plan'])
    def test_inventory_cannot_omit_present_file(self):
        data=fixture()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);write_fixture(root,data)
            terminal=audit.load(root/'ci-terminal.json');terminal['evidence_files'].pop('watchdog.json')
            (root/'ci-terminal.json').write_text(json.dumps(terminal))
            with self.assertRaisesRegex(ValueError,'inventory incomplete'):self.audit(root,data['plan'])
    def test_artifact_hash_mismatch(self):
        data=fixture()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);write_fixture(root,data)
            with (root/'watchdog.json').open('a') as stream:stream.write(' ')
            with self.assertRaisesRegex(ValueError,'artifact identity mismatch'):self.audit(root,data['plan'])
    def test_full_source_manifest_mismatch(self):
        data=fixture()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);write_fixture(root,data)
            with self.assertRaisesRegex(ValueError,'full launched source'):self.audit(root,data['plan'],expected_source_files={})
    def test_worker_identity_replacement(self):
        def mutate(d):d['events'][2]['census']['children'][0]['created']+=1
        self.reject(mutate,'process identity changed')
    def test_raw_queue_violation(self):
        def mutate(d):d['events'][2]['snapshot']['physical']['peak_retained']=33
        self.reject(mutate,'historical queue bound')
    def test_trend_recomputed(self):
        def mutate(d):d['values']['endurance/summary.json']['quiescent_resource_trends']['parent_rss']['ols_per_hour']=1
        self.reject(mutate,'resource slope mismatch')

if __name__=='__main__':unittest.main()
