"""Verify accounting and recompute paired gates from raw adoption evidence.

This reads evidence only. It does not import tasks, recreate fixtures or deploy
configurations. Accepts JSON or the repository's gzip report format.
"""
from __future__ import annotations
import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from intelligent_futures.evaluation import (EvaluationConfiguration, EvaluationPlan,
                                           TrialRecord, _compare)
from intelligent_futures.tuning import Measurement
from tools.report_io import read_report


def summarize(payload):
    if payload.get('status')!='complete':
        raise ValueError('experiment is not complete')
    summaries=[]
    for family in payload['workloads']:
        row=family['report'];plan=EvaluationPlan(**row['plan'])
        records=[]
        for raw in row['records']:
            raw=dict(raw)
            raw['configuration']=EvaluationConfiguration(**raw['configuration'])
            raw['measurement']=None if raw['measurement'] is None else Measurement(**raw['measurement'])
            records.append(TrialRecord(**raw))
        if len(records)!=row['attempted_evaluations']:raise ValueError('attempt count mismatch')
        failed=sum(r.exclusion is not None for r in records)
        if failed!=row['failed_evaluations']:raise ValueError('failure count mismatch')
        if row['successful_evaluations']+failed!=len(records):raise ValueError('success count mismatch')
        if row['expected_evaluations']!=len(records)+row['skipped_evaluations']:raise ValueError('slot count mismatch')
        if set(row['training_seeds']) & set(row['heldout_seeds']):raise ValueError('leaked holdout seed')
        for phase,seeds in (('training',row['training_seeds']),('heldout',row['heldout_seeds'])):
            for record in records:
                if record.phase==phase and record.seed!=seeds[record.block]:raise ValueError('seed/block mismatch')
        summaries.append({'name':family['name'],'status':row['status'],
            'attempted_evaluations':len(records),'failed_evaluations':failed,
            'discovery_seconds':row['discovery_seconds'],'comparisons':[]})
        candidate=[r for r in records if r.phase=='heldout' and r.engine=='intelligent']
        for comparison in row['comparisons']:
            config=EvaluationConfiguration(**comparison['configuration'])
            base=[r for r in records if r.phase=='heldout' and r.engine=='stdlib' and r.configuration==config]
            actual=asdict(_compare(comparison['role'],config,base,candidate,plan))
            # JSON normalization converts tuples consistently without coercing
            # scalar measurements or rounding the statistical calculations.
            actual=json.loads(json.dumps(actual,allow_nan=False))
            if actual!=comparison:raise ValueError('paired gate recomputation mismatch')
            summaries[-1]['comparisons'].append({k:comparison[k] for k in (
                'role','median_speedup','lower_speedup','aggregate_speedup','passed','reasons')})
        if row['status']=='recommend_intelligent' and (
            not row['comparisons'] or not all(c['passed'] for c in row['comparisons'])
            or row['recommendation'] is None or row['reasons']):
            raise ValueError('unsupported recommendation')
    return {'workload_count':len(summaries),'attempted_evaluations':sum(r['attempted_evaluations'] for r in summaries),
        'failed_evaluations':sum(r['failed_evaluations'] for r in summaries),'workloads':summaries}


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('report',type=Path)
    args=parser.parse_args();print(json.dumps(summarize(read_report(args.report)),indent=2,allow_nan=False))


if __name__=='__main__':main()
