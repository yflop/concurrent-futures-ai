"""Separate live calibration and predeclared input-shift checks.

Run serially, after the primary matrix. These are new actual executions, never
replay timings. The shift is explicit heavy-profile reweighting on the same host.
"""
from __future__ import annotations
import argparse
from contextlib import nullcontext
from dataclasses import asdict
import json
import os
from pathlib import Path
import random
import tempfile
import time
from intelligent_futures.tuning import (Configuration, CalibrationObjective, tune,
    export_configuration, load_configuration)
from . import execute_batch, module_for
from .optimizer import CANDIDATES, canonical, digest, measurement, write
from .optimizer_metrics import GateBudgets, summarize_phases, validate_split
from .run import trial, workload_environment

SEED=881237
HELDOUT=(970171,980183,990191)
COUNT=24


def reweight_heavy(name,tasks):
    if name=='images':
        weight=lambda task:task['source_size'][0]*task['source_size'][1]
    elif name=='geospatial':
        weight=lambda task:len(task['origins'])*len(task['destinations'])
    else:
        raise ValueError('heavy-profile shift supported only for images/geospatial')
    ordered=sorted(tasks,key=weight)
    light=ordered[:6];heavy=ordered[-6:]
    result=heavy*3+light
    return result,{'rule':'largest six declared cost-proxy inputs repeated three times, followed by smallest six; 24 tasks',
        'original_weights':[weight(t) for t in tasks], 'shifted_weights':[weight(t) for t in result],
        'duplicates_intentional':True, 'not_novel_upstream_inputs':True}


def run(baseline_path,output,mode):
    baseline=json.loads(baseline_path.read_text()); name=baseline['protocol']['workload']
    available=sorted(os.sched_getaffinity(0));os.sched_setaffinity(0,available[:8])
    rng=random.Random(SEED);training=tuple(rng.randrange(2**63) for _ in range(4))
    validate_split(training,HELDOUT)
    plan={'mode':mode,'workload':name,'count':COUNT,'training_seeds':training,'heldout_seeds':HELDOUT,
        'candidates':[asdict(c) for c in CANDIDATES],
        'selection':'live tune min2/max4/budget72; actual callback executions, no recorded-timing replay',
        'shift':'same-generator holdout' if mode=='live' else 'declared heavy-profile reweighting, not new hardware or upstream distribution',
        'baseline_selection_sha256':baseline['selection_frozen_sha256'],
        'gates':{'max_p95_seconds':60.,'max_rss_bytes':1024**3,'max_cpu_seconds':300.}}
    report={'schema_version':1,'status':'running','plan':plan,'plan_sha256':digest(plan),
        'environment':workload_environment(),'training':[],'heldout':[],'datasets':[],'failures':[]}
    output.parent.mkdir(parents=True,exist_ok=True);write(output,report)
    started=time.perf_counter();module=module_for(name)
    with tempfile.TemporaryDirectory(prefix='optimizer-variant-') as temp:
        root=Path(temp); fixture=module.fixture(root) if hasattr(module,'fixture') else nullcontext()
        with fixture:
            cache={}
            def dataset(seed):
                if seed not in cache:
                    t=time.perf_counter();tasks=module.prepare(name,root/f'data-{seed}',COUNT,seed,scale='representative')
                    metadata={}
                    if mode=='shift':
                        tasks,metadata=reweight_heavy(name,tasks)
                    prep=time.perf_counter()-t;t=time.perf_counter()
                    rows=execute_batch((name,tasks))
                    if any(r['error'] for r in rows):
                        raise RuntimeError('Executor-independent reference failed')
                    data={'tasks':tasks,'reference_sha256':digest([r['value'] for r in rows])}
                    cache[seed]=data;report['datasets'].append({'seed':seed,'preparation_seconds':prep,
                        'reference_seconds':time.perf_counter()-t,'shift':metadata})
                return cache[seed]
            # Prepare references outside the measured live-search interval, explicitly costed.
            for seed in training:
                dataset(seed)
            def observe(config,seed,phase):
                data=dataset(seed);t=time.perf_counter()
                try:
                    row=trial(name,'intelligent',config,COUNT,seed,root/f'{phase}-{len(report[phase])}',prepared=data)
                except Exception as exc:
                    row={'engine':'intelligent','configuration':asdict(config),'seed':seed,
                        'failure':type(exc).__name__+': '+str(exc)}
                row['measured_callback_seconds']=time.perf_counter()-t
                for p in ('cold','warm'):
                    if p in row:
                        row[p].pop('completion_trace',None)
                row.pop('profiles_after_two_batches',None)
                report[phase].append(row);write(output,report)
                print(name,mode,phase,len(report[phase]),asdict(config),flush=True)
                return row
            def evaluate(config,seed):
                row=observe(config,seed,'training')
                if row.get('failure'):
                    raise RuntimeError(row['failure'])
                return measurement(row)
            objective=CalibrationObjective(elapsed='combined',max_cpu_seconds=300.,
                max_peak_rss_bytes=1024**3,max_batch_p95_seconds=60.)
            t=time.perf_counter()
            selected=tune(CANDIDATES,evaluate,max_evaluations=72,min_repeats=2,max_repeats=4,
                seed=SEED,objective=objective,allow_process=True)
            report['live_calibration_seconds_including_checkpoint_io']=time.perf_counter()-t
            report['live_callback_seconds']=sum(r['measured_callback_seconds'] for r in report['training'])
            report['tuning_report']=selected.to_dict()
            write(output,report)
            if selected.selected is None:
                raise RuntimeError('Live tuning has no eligible selection')
            key=f'optimizer-variant:v1:{name}:{mode}:{report["plan_sha256"]}'
            before=len(report['training']);t=time.perf_counter()
            saved=export_configuration(selected,workload_key=key)
            reused=load_configuration(saved,workload_key=key,allow_process=True)
            report['reuse_export_load_seconds']=time.perf_counter()-t
            report['reuse_calibration_calls']=len(report['training'])-before
            # Check an ACTUAL changed affinity identity is rejected, then restore.
            original=os.sched_getaffinity(0)
            try:
                os.sched_setaffinity(0,[min(original)])
                try:
                    load_configuration(saved,workload_key=key,allow_process=True)
                except ValueError as exc:
                    report['changed_affinity_profile_rejected']='hardware fingerprint mismatch' in str(exc)
                else:
                    report['changed_affinity_profile_rejected']=False
            finally:
                os.sched_setaffinity(0,original)
            report['selections']={'live_budgeted':asdict(reused.configuration),
                'frozen_primary_budgeted':baseline['selections']['budgeted']['selected']}
            report['tuning_report']=selected.to_dict()
            report['frozen_selection_sha256']=digest(report['selections']);write(output,report)
            for seed in HELDOUT:
                order=list(CANDIDATES);random.Random(seed+123).shuffle(order)
                for config in order:
                    observe(config,seed,'heldout')
            if digest(report['selections'])!=report['frozen_selection_sha256']:
                raise RuntimeError('Selection changed during heldout')
            gates=GateBudgets(**plan['gates'])
            report['summaries']={key:summarize_phases(config,report['heldout'],candidates=CANDIDATES,
                heldout_seeds=HELDOUT,budgets=gates) for key,config in report['selections'].items()}
    report['accepted']=all(s['combined']['quality_gate_eligible'] for s in report['summaries'].values()) and report['changed_affinity_profile_rejected']
    report.update(status='complete',elapsed_seconds=time.perf_counter()-started)
    write(output,report);return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--mode',choices=('live','shift'),required=True)
    args=parser.parse_args()
    try:
        report=run(args.baseline,args.output,args.mode)
    except Exception as exc:
        if args.output.exists():
            report=json.loads(args.output.read_text());report.update(status='failed',accepted=False)
            report['failures'].append(type(exc).__name__+': '+str(exc));write(args.output,report)
        raise
    return 0 if report['accepted'] else 1

if __name__=='__main__':
    raise SystemExit(main())
