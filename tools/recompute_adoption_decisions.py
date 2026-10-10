"""Recompute rejected adoption decisions from stored scalars, without task replay.

Limited to complete, successful experiments already rejected by a comparison
(non-cost) gate. Thus fresh analysis overhead cannot change amortization decisions.
Exact derived replay needs a matching math runtime; report bytes/hashes stay exact.
"""
from __future__ import annotations
import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from intelligent_futures.evaluation import (EVALUATION_REVISION, LOG_MARGIN_GUARD,
    EvaluationConfiguration, EvaluationPlan, RepeatableWorkload, evaluate_workload)
from intelligent_futures.tuning import Measurement
from tools.report_io import read_report
from tools.summarize_adoption import summarize


def recompute(payload):
    summarize(payload)  # Validate every slot, seed and exact paired comparison.
    output=[]
    for family in payload['workloads']:
        row=family['report']
        if (row['failed_evaluations'] or row['skipped_evaluations'] or row['recommendation']
                or not row['comparisons'] or all(c['passed'] for c in row['comparisons'])):
            raise ValueError('only complete comparison-rejected decisions can be recomputed')
        configurations={'stdlib':set(),'intelligent':set()};stored={}
        for trial in row['records']:
            config=EvaluationConfiguration(**trial['configuration'])
            key=(trial['engine'],config,trial['seed'])
            if key in stored:raise ValueError('duplicate scalar observation')
            stored[key]=Measurement(**trial['measurement'])
            if trial['phase']=='training':configurations[trial['engine']].add(config)
        used=[]
        def scalar_observation(engine,config,seed):
            key=(engine,config,seed);used.append(key)
            return stored[key]
        # This callback retrieves numeric records only. No fixture, executor or
        # task module is imported, started, replayed or used for timing evidence.
        workload=RepeatableWorkload(row['workload_name'],row['workload_version'],
            scalar_observation,repeatable=True,independent=True,metric_scope=row['metric_scope'])
        fresh=evaluate_workload(workload,
            stdlib_configurations=configurations['stdlib'],
            intelligent_configurations=configurations['intelligent'],
            fixed_baseline=EvaluationConfiguration(**row['fixed_baseline']),
            plan=EvaluationPlan(**row['plan']),allow_process=True).to_json()
        fresh=json.loads(json.dumps(fresh,allow_nan=False))
        fields=('status','reasons','selected_intelligent','selected_stdlib','fixed_baseline',
            'comparisons','training_seeds','heldout_seeds','expected_evaluations',
            'attempted_evaluations','successful_evaluations','failed_evaluations','skipped_evaluations',
            'empirical_break_even_reuses','empirical_net_seconds','recommendation')
        for field in fields:
            if fresh[field]!=row[field]:raise ValueError('decision changed: '+family['name']+':'+field)
        if len(used)!=len(stored) or set(used)!=set(stored):raise ValueError('scalar accounting mismatch')
        output.append({'name':family['name'],'status':fresh['status'],'reasons':fresh['reasons'],
            'comparisons':len(fresh['comparisons']),'scalar_observations':len(used),
            'exact_decision_and_selection_match':True})
    return {'evaluation_revision':EVALUATION_REVISION,'positive_log_margin_guard':LOG_MARGIN_GUARD,
        'workload_count':len(output),'comparison_count':sum(r['comparisons'] for r in output),
        'scalar_observations':sum(r['scalar_observations'] for r in output),
        'task_executions':0,'workloads':output,
        'scope':'Numeric decision recomputation only; original measurements, timing costs, assumptions and source provenance remain in unchanged raw reports. Matching math runtime required for exact derived floating-field equality.'}


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('report',type=Path)
    args=parser.parse_args();result=recompute(read_report(args.report))
    result['archive_sha256']=hashlib.sha256(args.report.read_bytes()).hexdigest()
    print(json.dumps(result,indent=2,allow_nan=False))


if __name__=='__main__':main()
