"""Audit completed primary grids and report configuration quality, not engine parity."""
from __future__ import annotations
import argparse
from tools.report_io import read_report, resolve_report
from dataclasses import asdict
import json
import hashlib
from pathlib import Path
import statistics
from intelligent_futures.tuning import Configuration
from benchmarks.real_world import NAMES
from benchmarks.real_world.optimizer_metrics import GateBudgets, summarize_selection


def execution_capacity(configuration, count):
    if configuration is None:
        return None
    batches=(count+configuration['batch_size']-1)//configuration['batch_size']
    return {'items_per_pass':count,'configured_workers':configuration['workers'],
        'runnable_batches_per_pass':batches,
        'maximum_simultaneously_runnable_batches':min(configuration['workers'],batches),
        'scope':'computed work-availability ceiling, not measured active workers, CPU utilisation or proof of performance equivalence'}


def summary(root):
    workloads=[];missing=[];failed=[]
    for name in NAMES:
        path=resolve_report(root/(name+'.json'))
        if not path.exists():
            missing.append(name);continue
        raw=read_report(path)
        if raw['status']!='complete':
            missing.append(name)
            failed.append({'name':name,'status':raw['status'],'failures':raw.get('failures',[]),
                'recorded_trials':sum(len(raw.get(k,[])) for k in ('training','manual_training','heldout','baseline_heldout')),
                'recorded_callback_seconds':sum(r.get('measured_callback_seconds',0.) for k in ('training','manual_training','heldout','baseline_heldout') for r in raw.get(k,[])),
                'recorded_task_observations':sum(r.get(p,{}).get('tasks',0) for k in ('training','manual_training','heldout','baseline_heldout') for r in raw.get(k,[]) for p in ('cold','warm')),
                'selection_costs':{m:{k:v.get(k) for k in ('evaluations','virtual_callback_seconds','selection_status')} for m,v in raw.get('selections',{}).items()}})
            continue
        candidates=raw['protocol']['grid']; seeds=raw['protocol']['heldout_seeds']; gates=GateBudgets(**raw['protocol']['gates'])
        outcomes={}
        for method,selection in raw['selections'].items():
            phases=raw['summaries'][method]
            outcomes[method]={'configuration':selection['selected'],
                'execution_capacity':execution_capacity(selection['selected'],raw['protocol']['count_per_pass']),'phases':{phase:{
                key: values[key] for key in ('aggregate_relative_regret','aggregate_within_10_percent',
                    'within_10_percent_count','coverage_denominator','within_10_percent_coverage',
                    'selected_mean_seconds','oracle_mean_seconds','quality_gate_eligible')}
                for phase,values in phases.items()},
                'evaluations':selection['evaluations'],'virtual_callback_seconds':selection['virtual_callback_seconds'],
                'reuse_export_load_seconds':selection['configuration_export_load_seconds'],
                'reuse_calibration_calls':selection['reuse_calibration_calls']}
        trace=[]
        for point in raw['selections']['budgeted']['report']['trace']:
            measured=summarize_selection(point['best_configuration'],raw['heldout'],candidates=candidates,
                heldout_seeds=seeds,budgets=gates)
            trace.append({'evaluations':point['evaluations'],'leader':point['best_configuration'],
                'heldout_relative_regret_descriptive_only':measured['aggregate_relative_regret'],
                'heldout_within_10_percent':measured['aggregate_within_10_percent']})
        stable=next((p['evaluations'] for i,p in enumerate(trace)
            if all(q['heldout_within_10_percent'] for q in trace[i:])),None)
        baselines={}
        chosen=outcomes['budgeted']['phases']['combined']['selected_mean_seconds']
        for label,config in [('thread_one_worker_batch_one',Configuration('thread',1,1)),
                             ('thread_four_workers_batch_one',Configuration('thread',4,1))]:
            if asdict(config) not in candidates:continue
            measured=summarize_selection(config,raw['heldout'],candidates=candidates,heldout_seeds=seeds,budgets=gates)
            elapsed=measured['selected_mean_seconds']
            baselines[label]={'mean_seconds':elapsed,'baseline_time_over_selected_time':elapsed/chosen if elapsed and chosen else None,
                'scope':'illustrative same-engine fixed configuration; not the user original code or a best manual solution'}
        pareto=[]
        audit=raw['summaries']['budgeted']['combined']['grid_audit']
        for item in audit['candidates']:
            if not item['eligible']:continue
            rows=[r for r in raw['heldout'] if r['configuration']==item['configuration']]
            pareto.append({'configuration':item['configuration'],
                'mean_combined_seconds':statistics.mean(r['cold_lifecycle_seconds']+r['warm']['seconds'] for r in rows),
                'mean_cpu_seconds':statistics.mean(r['lifecycle_cpu_seconds'] for r in rows),
                'max_sampled_tree_rss_bytes':max(r['sampled_tree_peak_rss_bytes'] for r in rows),
                'max_batch_p95_seconds':max(r[p]['batch_completion_latency_p95_seconds'] for r in rows for p in ('cold','warm'))})
        dimensions=('mean_combined_seconds','mean_cpu_seconds','max_sampled_tree_rss_bytes','max_batch_p95_seconds')
        frontier=[p for p in pareto if not any(all(q[k]<=p[k] for k in dimensions) and any(q[k]<p[k] for k in dimensions) for q in pareto)]
        groups={}
        for configuration in candidates:
            capacity=execution_capacity(configuration,raw['protocol']['count_per_pass'])
            key=(configuration['backend'],configuration['batch_size'],capacity['maximum_simultaneously_runnable_batches'])
            groups.setdefault(key,[]).append(configuration)
        workloads.append({'name':name,
            'work_availability_equivalence_groups':[{'configurations':configs,'execution_capacity':execution_capacity(configs[0],raw['protocol']['count_per_pass'])} for configs in groups.values() if len(configs)>1],
            'equivalence_scope':'Same backend, batch size and work-availability ceiling; configured limits/startup effects may still differ. Exact observed oracle advantages can include timing variation.','accepted_correctness_resource_gates':raw['accepted'],
            'trials':raw['observed_trials'],'task_observations':raw['executed_task_observations'],
            'failed_callback_trials':sum(bool(r.get('failure')) for group in ('training','manual_training','heldout','baseline_heldout') for r in raw.get(group,[])),
            'unexpected_task_errors':sum(len(r.get(p,{}).get('errors',[])) for group in ('training','manual_training','heldout','baseline_heldout') for r in raw.get(group,[]) for p in ('cold','warm')),
            'eligible_candidates':audit['eligible_candidate_count'],'outcomes':outcomes,'illustrative_baselines':baselines,
            'measured_baseline_comparisons':raw.get('baseline_comparisons',{}).get('budgeted'),
            'manual_proxy_search':{k:raw.get('manual_proxy_selection',{}).get(k) for k in ('selected','evaluations','virtual_callback_seconds','scope')},
            'budgeted_trace_hindsight_audit':trace,'evaluations_after_which_observed_leaders_remain_within10':stable,
            'frontier_scope':'descriptive heldout Pareto frontier, not used for selection; sampled memory, meanCPU and maxbatchp95 have distinct scopes',
            'heldout_pareto_frontier':frontier,
            'source_manifest_sha256':hashlib.sha256(json.dumps(raw['environment']['source_sha256'],sort_keys=True,separators=(',',':')).encode()).hexdigest(),
            'source_tuning_sha256':raw['environment']['source_sha256']['src/intelligent_futures/tuning.py']})
    report={'schema_version':1,'status':'complete' if not missing else 'partial','missing_or_running_workloads':missing,
        'scope':'Single host; full recorded grid only; near-best is not a global optimum or deployment success probability.',
        'workloads':workloads,'workloads_completed':len(workloads),
        'workloads_planned':len(NAMES),'incomplete_workload_records':failed,
        'coverage_scope':'All planned workloads and heldout seeds remain in coverage denominators; missing/running/failed results count as not demonstrated, not successful. Regret statistics use only defined observations and report their count.',
        'trials':sum(w['trials'] for w in workloads)+sum(w['recorded_trials'] for w in failed),
        'task_observations':sum(w['task_observations'] for w in workloads)+sum(w['recorded_task_observations'] for w in failed),
        'incomplete_recorded_callback_seconds':sum(w['recorded_callback_seconds'] for w in failed),
        'source_manifest_hashes':sorted({w['source_manifest_sha256'] for w in workloads}),
        'source_tuning_hashes':sorted({w['source_tuning_sha256'] for w in workloads})}
    report['manual_proxy_total_virtual_callback_seconds']=sum(w['manual_proxy_search'].get('virtual_callback_seconds') or 0 for w in workloads)
    report['manual_proxy_total_evaluations']=sum(w['manual_proxy_search'].get('evaluations') or 0 for w in workloads)
    for method in ('exhaustive','exhaustive_median','budgeted','budgeted_lowcost'):
        valid=[w['outcomes'][method]['phases']['combined'] for w in workloads]
        regrets=[v['aggregate_relative_regret'] for v in valid if v['aggregate_relative_regret'] is not None]
        report[method]={'cost_scope':'selector cost totals below cover completed workload reports; partially recorded selector costs are retained in incomplete_workload_records',
            'aggregate_workload_within10_count':sum(v['aggregate_within_10_percent'] for v in valid),
            'aggregate_workload_denominator':len(NAMES),'defined_regret_workload_count':len(regrets),'median_workload_relative_regret':statistics.median(regrets) if regrets else None,
            'max_workload_relative_regret':max(regrets) if regrets else None,
            'combined_seed_within10_count':sum(v['within_10_percent_count'] for v in valid),
            'combined_seed_denominator':len(NAMES)*3,
            'cold_warm_seed_within10_count':sum(w['outcomes'][method]['phases'][p]['within_10_percent_count'] for w in workloads for p in ('cold','warm')),
            'cold_warm_seed_denominator':len(NAMES)*3*2,
            'virtual_callback_seconds':sum(w['outcomes'][method]['virtual_callback_seconds'] for w in workloads),
            'evaluations':sum(w['outcomes'][method]['evaluations'] for w in workloads)}
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();result=summary(args.input)
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k!='workloads'},indent=2))

if __name__=='__main__':main()
