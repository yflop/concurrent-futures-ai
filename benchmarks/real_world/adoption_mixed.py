"""Separate fixed experiment: real graph routing plus loopback HTTP with hints.

The comparator class is tuned single-backend stdlib pools, not a hand-built
expert hybrid. This uses the caller-measured API because the convenience
callable adapter intentionally does not classify mixed per-task workload keys.
"""
from __future__ import annotations
import argparse
from concurrent.futures import CancelledError
from dataclasses import asdict
import json
from pathlib import Path
import tempfile
import time

from intelligent_futures import TaskHints
from intelligent_futures.evaluation import (EvaluationConfiguration, EvaluationPlan,
    RepeatableWorkload, evaluate_workload)
from intelligent_futures.evaluation_workloads import make_executor
from intelligent_futures.tuning import Measurement
from . import module_for
from .adoption import _digest, run_item
from .resource_sampling import RSSSampler
from .run import cpu_seconds, workload_environment


def run(output):
    output=Path(output)
    if output.exists():raise ValueError('use a new report path; retain earlier experiments')
    output.parent.mkdir(parents=True,exist_ok=True)
    plan=EvaluationPlan(training_blocks=3,heldout_blocks=24,seed=505110,
        max_workers=4,max_pending=8,max_evaluations=128,expected_reuses=10000,
        max_cpu_seconds=120,max_peak_rss_bytes=1024**3,max_observed_seconds=60)
    standard=tuple(EvaluationConfiguration(backend,workers) for backend in ('thread','process') for workers in (2,4))
    hybrid=EvaluationConfiguration('adaptive',4,process_workers=2)
    reference_digests={};details=[]
    network=module_for('http');graph=module_for('geospatial')
    def tasks(root,seed):
        cpu=graph.prepare('geospatial',root/'graph',4,seed,scale='representative')
        io=network.prepare('http',root/'http',4,seed,scale='representative')
        return tuple(item for pair in zip((('geospatial',v) for v in cpu),(('http',v) for v in io)) for item in pair)
    def measure(engine,config,seed):
        with tempfile.TemporaryDirectory(prefix='intelligent-mixed-adoption-') as directory:
            root=Path(directory)
            with network.fixture(root):
                work=tasks(root/'measured',seed)
                if seed not in reference_digests:
                    reference_digests[seed]=_digest(tuple(run_item(v) for v in tasks(root/'oracle',seed)))
                before=cpu_seconds();elapsed=1e-12;correct=False;errors=0;counts=None
                with RSSSampler() as rss:
                    start=time.perf_counter()
                    try:
                        with make_executor(engine,config,max_pending=8,allow_process=True) as pool:
                            futures=[]
                            # Exactly eight tasks and a common eight-item window.
                            # Both engines receive the same interleaved task order.
                            for value in work:
                                if engine=='intelligent':
                                    hints=TaskHints(kind='cpu' if value[0]=='geospatial' else 'io',
                                        key='mixed-'+value[0],process_safe=True)
                                    futures.append(pool.submit_task(run_item,value,hints=hints))
                                else:
                                    futures.append(pool.submit(run_item,value))
                            values=tuple(f.result() for f in futures)
                        elapsed=max(1e-12,time.perf_counter()-start)
                        if engine=='intelligent':
                            snapshot=pool.snapshot()['counts']
                            counts={k:snapshot[k] for k in ('submitted','thread_submitted','process_submitted','failed','cancelled')}
                        correct=_digest(values)==reference_digests[seed]
                    except CancelledError:
                        raise
                    except Exception:
                        elapsed=max(1e-12,time.perf_counter()-start);errors=1
                after=cpu_seconds()
                measured_cpu=None if before is None or after is None else max(0.,after-before)
                details.append({'engine':engine,'configuration':asdict(config),'seed':seed,
                    'routing_counts':counts,'rss_collection':rss.report()})
                return Measurement(elapsed,elapsed,correct,error_count=errors,
                    cpu_seconds=measured_cpu,peak_rss_bytes=rss.peak)
    measure._evaluation_max_pending=8
    measure._evaluation_process_safe=True
    workload=RepeatableWorkload('mixed-geospatial-http',
        'v1:4graphs+4http:interleaved:representative:keys-per-family:explicit-cpu-io-hints:empty-policy-state',
        measure,True,True,'cold_pool_lifecycle;resources_include_validation;fresh_fixture_paths;os_cache_uncontrolled')
    environment=workload_environment()
    environment['command']='python -m benchmarks.real_world.adoption_mixed --output [new report path]'
    result={'schema_version':1,'purpose':'separate real heterogeneous hinted adaptive adoption experiment',
        'environment':environment,'predeclared_plan':{'plan':asdict(plan),'standard_grid':[asdict(c) for c in standard],
            'intelligent_grid':[asdict(hybrid)],'fixed_baseline':asdict(EvaluationConfiguration('thread',4)),
            'comparator_scope':'tuned single-backend stdlib, not expert hybrid pipelines',
            'fixture':'4 real geospatial graph jobs + 4 loopback HTTP jobs, interleaved, representative recipe',
            'process_consent':'every fixture task explicitly process-safe; task keys and cpu/io hints declared',
            'state':'fresh pools and empty policy state each measurement; no predictive sampling',
            'family_alpha_scope':'two baselines in this one separate fixed report, no family claim across experiments'},
        'workloads':[],'status':'running'}
    output.write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    started=time.perf_counter()
    report=evaluate_workload(workload,stdlib_configurations=standard,intelligent_configurations=(hybrid,),
        fixed_baseline=EvaluationConfiguration('thread',4),plan=plan,allow_process=True)
    result['workloads']=[{'name':'mixed-geospatial-http','scale':'representative','logical_items_per_fixture':8,
        'measured_logical_items_attempted_upper_bound':8*report.attempted_evaluations,
        'sequential_oracle_fixtures':len(reference_digests),'sequential_oracle_logical_items':8*len(reference_digests),
        'report':report.to_json(),'resource_collection':details}]
    result.update(status='complete',elapsed_seconds=time.perf_counter()-started)
    output.write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    print(report.status,report.reasons,flush=True)
    print('Calls:',report.attempted_evaluations,'failed:',report.failed_evaluations,flush=True)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True)
    run(parser.parse_args().output)


if __name__=='__main__':main()
