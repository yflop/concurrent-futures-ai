"""Reconcile a completed frozen endurance run against raw events and CI records.

This independently checks supplied evidence. It does not rerun workloads or
establish a production guarantee, and it cannot certify an interrupted attempt.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import math
from pathlib import Path


def check(condition,message):
    if not condition:raise ValueError(message)


COUNT_KEYS = frozenset(('accepted','attempted','rejected','success','expected_failure',
    'unexpected_failure','cancelled','cancel_true','cancel_false','cancel_attempted'))
TREND_KEYS = frozenset(('parent_rss','tree_rss','parent_fds','python_threads','task_children'))


def loads(value):
    def bad(value):raise ValueError('nonfinite JSON: '+value)
    def finite_float(value):
        result=float(value)
        if not math.isfinite(result):bad(value)
        return result
    return json.loads(value,parse_constant=bad,parse_float=finite_float)


def load(path):
    return loads(Path(path).read_bytes())


def reconciled(counts):
    check(set(counts)==COUNT_KEYS,'incomplete or unknown count keys')
    for key in COUNT_KEYS:
        check(type(counts.get(key)) is int and counts[key]>=0,'invalid count type/value: '+key)
    terminal=sum(counts.get(k,0) for k in ('success','expected_failure','unexpected_failure','cancelled'))
    check(counts['accepted']==terminal,'accepted denominator does not equal terminal outcomes')
    check(counts['attempted']==counts['accepted']+counts['rejected'],'attempt denominator mismatch')
    check(counts['cancel_true']==counts['cancelled'],'successful cancellation count mismatch')
    check(counts['cancel_attempted']==counts['cancel_true']+counts['cancel_false'],'cancel attempts mismatch')
    check(counts['unexpected_failure']==0,'unexpected task failure')


def ols(rows,field):
    xs=[row['elapsed_seconds']/3600 for row in rows]
    ys=[row['census'][field] for row in rows]
    xm,ym=sum(xs)/len(xs),sum(ys)/len(ys)
    denominator=sum((x-xm)**2 for x in xs)
    return None if not denominator else sum((x-xm)*(y-ym) for x,y in zip(xs,ys))/denominator


def audit(directory,plan,*,expected_commit,expected_run_id,expected_run_attempt=1,expected_source_files=None):
    directory=Path(directory)
    summary=load(directory/'endurance/summary.json')
    watchdog=load(directory/'watchdog.json')
    terminal=load(directory/'ci-terminal.json')
    preflight=load(directory/'preflight.json')
    with (directory/'endurance/events.jsonl').open() as stream:
        events=[loads(line) for line in stream if line.strip()]
    check(summary['status']==watchdog['status']==terminal['status']==preflight['status']=='passed','all terminal/preflight layers must pass')
    check(summary['mode']=='endurance','a smoke test cannot establish endurance')
    check(summary['required_workload_seconds']==plan['measured_seconds']==14400,'declared duration changed')
    check(summary['workload_seconds']>=14400,'four measured hours incomplete')
    check(watchdog['returncode']==0 and watchdog['stop_reason'] is None,'supervisor stop/failure')
    check(watchdog['errors']==[] and watchdog['remaining_owned_processes']==[],'cleanup not verified')
    for key,expected in [('wall_limit_seconds',15000),('sampled_rss_limit_bytes',1024**3),('progress_limit_seconds',90)]:
        check(watchdog[key]==expected,'watchdog gate changed: '+key)
    context=load(directory/'run-context.json')
    source=load(directory/'source-manifest.json')
    check(terminal['commit']==context['GITHUB_SHA']==source['commit']==expected_commit,'launched commit mismatch')
    check(str(terminal['run_id'])==str(context['GITHUB_RUN_ID'])==str(expected_run_id),'launched run ID mismatch')
    check(str(terminal['run_attempt'])==str(context['GITHUB_RUN_ATTEMPT'])==str(expected_run_attempt),'run attempt mismatch')
    check(all(terminal[field]=='success' for field in ('endurance_step_outcome','preflight_step_outcome','tests_step_outcome')),'CI step outcome mismatch')
    check(terminal['cleanup_verified_for_this_run'] is True,'terminal cleanup not verified')
    check(terminal['workload_seconds']==summary['workload_seconds'],'terminal duration mismatch')
    check(load(directory/'operational-endurance-github-v1-plan.json')==plan,'copied plan mismatch')
    expected_identity={key:plan[key] for key in ('runtime_files','runtime_manifest_sha256','frozen_files')}
    check(preflight['identity']==terminal['identity_after']==expected_identity,'preflight/terminal source identity mismatch')
    actual_digest=hashlib.sha256(json.dumps(plan['runtime_files'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
    check(actual_digest==plan['runtime_manifest_sha256'],'plan runtime digest mismatch')
    for relative,digest in {**{'src/intelligent_futures/'+k:v for k,v in plan['runtime_files'].items()},**plan['frozen_files']}.items():
        check(source['files'].get(relative)==digest,'tracked source identity mismatch: '+relative)
    if expected_source_files is not None:
        check(source['files']==expected_source_files,'full launched source manifest mismatch')
    for name in ('OPERATIONAL-ENDURANCE-PROTOCOL.md','OPERATIONAL-ENDURANCE-RESTART.md'):
        check(hashlib.sha256((directory/name).read_bytes()).hexdigest()==source['files']['docs/'+name],'copied document mismatch: '+name)
    identity=summary['identity']
    check(identity['version']=='0.5.1' and identity['python_optimization_level']==0,'runtime version/optimization mismatch')
    check(identity['runtime_sha256']==plan['runtime_files'],'runtime source mismatch')
    check(identity['python'].split()[0]==plan['python_version'] and identity['psutil']==plan['psutil_version'],'interpreter/dependency mismatch')
    for measured,host in [('python','python'),('platform','platform'),('affinity','cpu_affinity'),
                          ('python_optimization_level','python_optimization_level'),('psutil','psutil_version')]:
        check(identity[measured]==preflight[host],'measured/preflight host mismatch: '+measured)
    check(preflight['machine']=='x86_64' and preflight['platform'].startswith('Linux-'),'host platform mismatch')
    check(len(preflight['cpu_affinity'])>=plan['minimum_available_cpus'],'host CPU budget insufficient')
    check(preflight['memory_bytes']>=plan['minimum_memory_bytes'],'host memory insufficient')
    check(preflight['free_disk_bytes']>=plan['minimum_free_disk_bytes'],'host free disk insufficient')
    observed_host=load(directory/'host-observed.json')
    check({k:v for k,v in observed_host.items() if k!='status'}==
          {k:v for k,v in preflight.items() if k not in ('status','host_check_status')},'observed/preflight host mismatch')
    check(preflight['identity']['runtime_files']==plan['runtime_files'],'preflight runtime mismatch')
    check(terminal['identity_after']['runtime_files']==plan['runtime_files'],'terminal runtime mismatch')
    start=[row for row in events if row['event']=='start']
    check(len(start)==1 and start[0]['identity']==identity,'start/summary identity mismatch')
    expected_arguments={'seconds':plan['measured_seconds'],'warmup':plan['warmup_seconds'],
        'cycle_seconds':plan['cycle_seconds'],'sample_seconds':plan['sample_seconds'],'mode':plan['mode']}
    check(start[0]['arguments']==expected_arguments,'measured start arguments differ from plan')
    check(identity['harness_sha256']==plan['frozen_files']['tools/operational_endurance.py'],'harness mismatch')
    check(identity['protocol_sha256']==plan['frozen_files']['docs/OPERATIONAL-ENDURANCE-PROTOCOL.md'],'protocol mismatch')
    counts=summary['counts'];reconciled(counts)
    reconciled(summary['warmup_counts']);reconciled(summary['measured_counts'])
    check(set(summary['quiescent_resource_trends'])==TREND_KEYS,'incomplete or unknown resource trend keys')
    check(summary['callbacks']['total']==counts['accepted'],'callback count mismatch')
    for key,value in summary['measured_counts'].items():
        check(value==counts[key]-summary['warmup_counts'].get(key,0),'warm-up subtraction mismatch: '+key)
    def reconcile_kinds(by_kind,totals):
        check(set(by_kind)<=set(('blocking','cpu','payload','failure','health')),'unknown task kind')
        for kind in by_kind.values():
            check(set(kind)<=COUNT_KEYS,'unknown task-kind count key')
            reconciled({key:kind.get(key,0) for key in COUNT_KEYS})
        for key in COUNT_KEYS:
            check(sum(kind.get(key,0) for kind in by_kind.values())==totals[key],'task-kind denominator mismatch: '+key)
    reconcile_kinds(summary['by_kind'],counts)
    warm=[row for row in events if row['event']=='warm_baseline']
    cycles=[row for row in events if row['event']=='cycle_quiescent']
    final=[row for row in events if row['event']=='final_quiescent']
    diagnostics=[row for row in events if row['event']=='diagnostic_end']
    check(len(warm)==1 and len(cycles)==16 and len(final)==1,'quiescent checkpoint count mismatch')
    check(len(diagnostics)==16 and [row['cycle'] for row in diagnostics]==list(range(1,17)),'diagnostic sequence incomplete')
    check(not any(row['event'] in ('failure','diagnostic_failure','monitor_failure') for row in events),'raw event contains failure')
    complete=[row for row in events if row['event']=='complete']
    check(len(complete)==1,'missing or duplicate complete event')
    check(complete[0]['result']==summary,'complete-event result differs from summary')
    raw_diagnostics=[{key:value for key,value in row.items() if key not in ('event','unix_time')} for row in diagnostics]
    check(summary['diagnostics']==raw_diagnostics,'summary/raw diagnostic rows differ')
    for row in diagnostics:
        check(row['status']=='passed','diagnostic failure')
        check(row['accepted']==38 and row['successes']==22 and row['expected_failures']==9 and row['cancelled']==7 and row['rejected']==4,'diagnostic outcome denominator mismatch')
        probe=row['predictive_probe']
        check(probe['tasks']==16 and probe['counts']['completed']==16 and probe['profile_count']<=4,'short predictive probe mismatch')
    rows=warm+cycles+final
    check(summary['quiescent_samples']==len(rows),'summary/raw quiescent count mismatch')
    check(summary['warmup_counts']==warm[0]['counts'],'summary/raw warm-up counts mismatch')
    check(counts==final[0]['counts'],'summary/raw final outcomes mismatch')
    check(summary['by_kind']==final[0]['by_kind'],'summary/raw task kinds mismatch')
    check(summary['workload_seconds']==final[0]['workload_seconds'],'summary/raw measured duration mismatch')
    check(load(directory/'endurance/checkpoint.json')==complete[0],'last productive checkpoint is not complete event')
    baseline=warm[0]['census']
    ids=lambda state:sorted((child['pid'],child['created']) for child in state['children'] if not child['resource_tracker'])
    for row in rows:
        state=row['census'];snapshot=row['snapshot'];reconciled(row['counts'])
        reconcile_kinds(row['by_kind'],row['counts'])
        check(state['task_children']==2 and ids(state)==ids(baseline),'primary process identity changed')
        check(snapshot['pending']==snapshot['physical']['retained']==0,'non-quiescent checkpoint')
        check(snapshot['counts']['submitted']==row['counts']['accepted'],'snapshot acceptance mismatch')
        check(snapshot['counts']['completed']==row['counts']['success'],'snapshot success mismatch')
        check(snapshot['counts']['failed']==row['counts']['expected_failure'],'snapshot failure mismatch')
        check(snapshot['counts']['cancelled']==row['counts']['cancelled'],'snapshot cancellation mismatch')
        route=snapshot['routing'];check(route['recorded']==row['counts']['accepted'],'route denominator mismatch')
        check(route['dropped']==route['recorded']-route['retained'] and route['dropped']>0,'trace eviction reconciliation failed')
        for field,limit in [('parent_rss',128*1024**2),('tree_rss',256*1024**2),('parent_fds',4),('python_threads',2)]:
            check(state[field]-baseline[field]<=limit,'quiescent growth limit exceeded: '+field)
    sampled=[row for row in events if row.get('snapshot') is not None]
    for row in sampled:
        s=row['snapshot'];physical=s['physical']
        check(0<=s['pending']<=32 and 0<=physical['retained']<=32,'active admission/retention bound exceeded')
        check(physical['peak_retained']<=32 and s['counts']['peak_pending']<=32,'historical queue bound exceeded')
        check(all(physical['in_flight'][b]<=2 and physical['peak_in_flight'][b]<=2 for b in ('thread','process')),'dispatch worker bound exceeded')
        check(s['profile_count']<=16 and s['adaptive_tracked_keys']<=16 and s['routing']['retained']<=64,'bounded observation state exceeded')
    for field,reported in summary['quiescent_resource_trends'].items():
        first,last=rows[0]['census'][field],rows[-1]['census'][field]
        check(reported['first']==first and reported['last']==last and reported['delta']==last-first,'resource endpoint mismatch: '+field)
        calculated=ols(rows,field)
        check(calculated is not None and math.isclose(reported['ols_per_hour'],calculated,rel_tol=1e-10,abs_tol=1e-8),'resource slope mismatch: '+field)
    raw_censuses=[row['census'] for row in events if 'census' in row]
    peak_keys=TREND_KEYS|{'parent_native_threads'}
    check(set(summary['peaks'])==peak_keys,'resource peak keys incomplete')
    for key in peak_keys:
        check(summary['peaks'][key]==max(state[key] for state in raw_censuses),'sampled resource peak mismatch: '+key)
    with (directory/'watchdog.samples.jsonl').open() as stream:
        watchdog_samples=[loads(line) for line in stream if line.strip()]
    check(len(watchdog_samples)==watchdog['samples'] and watchdog_samples,'watchdog sample count mismatch')
    check(max(row['rss_bytes'] for row in watchdog_samples)==watchdog['sampled_peak_tree_rss_bytes'],'watchdog sampled peak mismatch')
    check(watchdog['sampled_peak_tree_rss_bytes']<=1024**3,'watchdog RSS limit exceeded')
    check(all(0<=row['elapsed_seconds']<=15000 for row in watchdog_samples),'watchdog wall gate exceeded')
    check(all(b['elapsed_seconds']>=a['elapsed_seconds'] for a,b in zip(watchdog_samples,watchdog_samples[1:])),'watchdog samples out of order')
    before,after=summary['before'],summary['after']
    check(after['task_children']==0 and all(child['resource_tracker'] for child in after['children']),'worker survived final shutdown')
    check(after['python_threads']<=before['python_threads'] and after['parent_fds']<=before['parent_fds']+4,'final thread/FD census failed')
    check(after['parent_rss']-before['parent_rss']<=128*1024**2,'final RSS gate exceeded')
    actual_files=set()
    for path in directory.rglob('*'):
        check(not path.is_symlink(),'symlink in artifact tree')
        if path.is_file():actual_files.add(path.relative_to(directory).as_posix())
    check(set(terminal['evidence_files'])==actual_files-{'ci-terminal.json'},'CI artifact inventory incomplete or unexpected')
    for relative,description in terminal['evidence_files'].items():
        path=directory/relative
        check(not Path(relative).is_absolute() and '..' not in Path(relative).parts
              and path.resolve().is_relative_to(directory.resolve()) and not path.is_symlink(),
              'unsafe artifact path: '+relative)
        check(path.is_file(),'CI evidence file missing: '+relative)
        check(path.stat().st_size==description['bytes'] and hashlib.sha256(path.read_bytes()).hexdigest()==description['sha256'],'CI artifact identity mismatch: '+relative)
    return {'status':'passed','scope':'Independent raw/summary/provenance consistency audit; not workload reexecution or production certification.',
        'source_commit':terminal['commit'],'run_id':terminal['run_id'],'workload_seconds':summary['workload_seconds'],
        'raw_events':len(events),'quiescent_checkpoints':len(rows),'diagnostic_checkpoints':len(diagnostics),
        'counts':counts,'measured_counts':summary['measured_counts'],'quiescent_resource_trends':summary['quiescent_resource_trends'],
        'cleanup_verified_for_this_run':True,'previous_attempt_cleanup':'Unknown; no elapsed time or counts carried forward.'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence',type=Path,required=True)
    parser.add_argument('--plan',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--expected-commit',required=True)
    parser.add_argument('--expected-run-id',required=True)
    parser.add_argument('--expected-run-attempt',type=int,default=1)
    parser.add_argument('--expected-source-files',type=Path,help='Independent path-to-SHA256 source manifest JSON')
    args=parser.parse_args()
    check(not args.output.resolve().is_relative_to(args.evidence.resolve()),'write audit result outside original evidence directory')
    result=audit(args.evidence,load(args.plan),expected_commit=args.expected_commit,
                 expected_run_id=args.expected_run_id,expected_run_attempt=args.expected_run_attempt,
                 expected_source_files=load(args.expected_source_files) if args.expected_source_files else None)
    with args.output.open('x') as stream:stream.write(json.dumps(result,indent=2,allow_nan=False)+'\n')
    print(json.dumps(result,indent=2))

if __name__=='__main__':main()
