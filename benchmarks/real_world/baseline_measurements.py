"""Explicit direct-Python and standard-library comparison scopes."""
from __future__ import annotations
import hashlib
import json
import time
from . import execute_batch
from .run import cpu_seconds, percentile
from .resource_sampling import RSSSampler
from .optimizer_metrics import gate_failures, phase_seconds


def direct_trial(name,prepared,seed):
    """Two ordinary direct-call passes with no executor or concurrent batching."""
    tasks=prepared['tasks'];reference=prepared['reference_sha256'];phases={}
    before=cpu_seconds()
    with RSSSampler() as rss:
        for phase in ('cold','warm'):
            started=time.perf_counter();rows=execute_batch((name,tasks));elapsed=time.perf_counter()-started
            errors=[r['error'] for r in rows if r['error'] is not None]
            actual=hashlib.sha256(json.dumps([r['value'] for r in rows],sort_keys=True,separators=(',',':')).encode()).hexdigest()
            service=[r['service_seconds'] for r in rows if r['service_seconds'] is not None]
            phases[phase]={'seconds':elapsed,'tasks':len(rows),'throughput_per_second':len(rows)/elapsed,
                'errors':errors,'error_rate':len(errors)/len(rows),'result_sha256':actual,
                'correct':not errors and actual==reference,
                'batch_completion_latency_p95_seconds':percentile(service,.95),
                'latency_scope':'individual direct-call service time; no submission queue or batching'}
    after=cpu_seconds()
    return {'engine':'sequential','configuration':None,'seed':seed,**phases,
        'cold_lifecycle_seconds':phases['cold']['seconds'],
        'lifecycle_cpu_seconds':None if before is None or after is None else max(0.,after-before),
        'sampled_tree_peak_rss_bytes':rss.peak,'rss_samples':rss.samples,'rss_collection':rss.report(),
        'scope':'standard Python direct execute_batch loop, same per-item workload algorithm, no concurrency'}


def compare_baselines(selected,heldout,baselines,*,seeds,budgets):
    comparisons=[];failures=[]
    seeds=tuple(seeds)
    for seed in seeds:
        selected_rows=[r for r in heldout if r['seed']==seed and r['configuration']==selected]
        chosen=selected_rows[0] if len(selected_rows)==1 else None
        if chosen is None:
            failures.append(f'selected seed {seed}: need exactly one row')
        for label in ('standard_library_fixed','standard_library_manual_tuning_proxy','direct_python'):
            rows=[r for r in baselines if r['seed']==seed and r.get('baseline_role')==label]
            row=rows[0] if len(rows)==1 else None
            errors=[]
            if row is None:
                errors.append('baseline_missing_or_duplicate')
            else:
                errors.extend(gate_failures(row,budgets))
            if chosen is None:
                errors.append('selected_missing_or_duplicate')
            else:
                errors.extend(gate_failures(chosen,budgets))
            if errors:failures.append(f'{label} seed {seed}: '+','.join(errors))
            for phase in ('cold','warm','combined'):
                standard=phase_seconds(row,phase) if row and not gate_failures(row,budgets) else None
                selected_time=phase_seconds(chosen,phase) if chosen and not gate_failures(chosen,budgets) else None
                same=[r for r in heldout if row and r['seed']==seed and r['configuration']==row.get('configuration')]
                matching=phase_seconds(same[0],phase) if len(same)==1 and not gate_failures(same[0],budgets) else None
                comparisons.append({'seed':seed,'baseline_role':label,'phase':phase,
                    'baseline_seconds':standard,'selected_seconds':selected_time,
                    'baseline_time_over_selected_time':standard/selected_time if not errors else None,
                    'matching_config_intelligent_seconds':matching,
                    'stdlib_time_over_matching_config_intelligent_time':standard/matching if not errors and matching else None,
                    'observed_baseline_rows':len(rows), 'observed_selected_rows':len(selected_rows),
                    'failures':errors,'passes_correctness_resource_gates':not errors})
    return {'comparisons':comparisons,'failures':failures,
        'expected_comparison_slots':len(seeds)*9,
        'observed_unique_baseline_slots':sum(r['observed_baseline_rows']==1 for r in comparisons),
        'valid_comparison_slots':sum(r['passes_correctness_resource_gates'] for r in comparisons),
        'manual_proxy_scope':'exhaustive mean calibration of stdlib configurations on training-only inputs; reproducible manual-tuning proxy, not claimed human expertise',
        'matching_scope':'engine overhead at matching explicit configuration is separately labelled; not configuration-selection benefit'}
