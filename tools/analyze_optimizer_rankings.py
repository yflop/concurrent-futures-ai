"""Descriptive train/holdout ranking audit; never reselects or edits raw evidence."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import statistics
from benchmarks.real_world import NAMES
from benchmarks.real_world.optimizer_metrics import GateBudgets,gate_failures,phase_seconds
from tools.summarize_optimizer import execution_capacity


def describe(values):
    if not values:return {'count':0,'mean':None,'median':None,'mad':None,'minimum':None,'maximum':None}
    middle=statistics.median(values)
    return {'count':len(values),'mean':statistics.mean(values),'median':middle,
        'mad':statistics.median(abs(v-middle) for v in values),'minimum':min(values),'maximum':max(values)}


def audit(raw):
    gates=GateBudgets(**raw['protocol']['gates'])
    selected=raw['selections']['budgeted']['selected']
    oracle=raw['summaries']['budgeted']['combined']['oracle_configuration']
    choices={};paired=[]
    for label,config in [('selected',selected),('aggregate_heldout_oracle',oracle)]:
        phases={}
        for group in ('training','heldout'):
            rows=[r for r in raw[group] if r['configuration']==config and not gate_failures(r,gates)] if config else []
            phases[group]={phase:describe([phase_seconds(r,phase) for r in rows]) for phase in ('cold','warm','combined')}
        choices[label]={'configuration':config,'capacity':execution_capacity(config,raw['protocol']['count_per_pass']),'observations':phases}
    for group,seeds in [('training',raw['protocol']['training_fixture_seeds']),('heldout',raw['protocol']['heldout_seeds'])]:
        for seed in seeds:
            first=[r for r in raw[group] if r['configuration']==selected and r['seed']==seed]
            second=[r for r in raw[group] if r['configuration']==oracle and r['seed']==seed]
            good=len(first)==len(second)==1 and not gate_failures(first[0],gates) and not gate_failures(second[0],gates)
            paired.append({'split':group,'seed':seed,'valid':good,'phases':{
                phase:{'selected_seconds':phase_seconds(first[0],phase) if good else None,
                       'fixed_aggregate_oracle_seconds':phase_seconds(second[0],phase) if good else None,
                       'selected_over_oracle':phase_seconds(first[0],phase)/phase_seconds(second[0],phase) if good else None}
                for phase in ('cold','warm','combined')}})
    baselines={}
    for label in ('standard_library_fixed','standard_library_manual_tuning_proxy','direct_python'):
        rows=[r for r in raw['baseline_comparisons']['budgeted']['comparisons'] if r['baseline_role']==label and r['phase']=='combined']
        good=[r for r in rows if r['passes_correctness_resource_gates']]
        baseline=describe([r['baseline_seconds'] for r in good]);choice=describe([r['selected_seconds'] for r in good])
        matching=describe([r['matching_config_intelligent_seconds'] for r in good if r['matching_config_intelligent_seconds'] is not None])
        full=len(good)==len(raw['protocol']['heldout_seeds'])
        baselines[label]={'expected_seeds':len(raw['protocol']['heldout_seeds']),'valid_seeds':len(good),
            'baseline':baseline,'automatic_choice':choice,'matching_config_intelligent':matching,
            'baseline_mean_over_automatic_choice_mean':baseline['mean']/choice['mean'] if full else None,
            'matching_intelligent_mean_over_stdlib_mean':matching['mean']/baseline['mean'] if full and matching['count']==len(good) else None}
    return {'workload':raw['protocol']['workload'],'choices':choices,'paired_observations':paired,'baselines':baselines,
        'primary_matches_exhaustive_choice':selected==raw['selections']['exhaustive']['selected'],
        'cache_scope':'cached_http creates a fresh per-task requests-cache database; warm means executor reuse, not a prior task cache' if raw['protocol']['workload']=='cached_http' else None}


def main():
    p=argparse.ArgumentParser();p.add_argument('--input',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    workloads=[]
    for name in NAMES:
        source=a.input/(name+'.json')
        if source.exists():
            raw=json.loads(source.read_text())
            if raw.get('status')=='complete':workloads.append(audit(raw))
    report={'scope':'Descriptive frozen-choice ranking audit only; mean/median/MAD/range are not confidence intervals. No heldout observation changes selection. Small correlated seeded repeats cannot establish deployment success probability.','workloads':workloads}
    a.output.write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')

if __name__=='__main__':main()
