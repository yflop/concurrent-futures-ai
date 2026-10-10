"""Frozen train/held-out exhaustive-grid audit of configuration selection.

Timings are actual workload execution. Search comparison replays only the frozen
training matrix; virtual callback costs are recorded actual execution costs, not
standalone live-search wall time. No held-out row is available during selection.
"""
from __future__ import annotations

import argparse
from contextlib import nullcontext
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import random
import shlex
import sys
import tempfile
import time

from intelligent_futures.tuning import (Configuration, Measurement, CalibrationObjective,
    calibrate, tune, export_configuration, load_configuration, fixture_seeds)
from . import NAMES, module_for, execute_batch
from .run import trial, workload_environment
from .optimizer_metrics import GateBudgets, validate_split, summarize_phases
from .baseline_measurements import direct_trial, compare_baselines

TRAIN_SEED = 1001207
HOLDOUT_SEEDS = (1010231, 1020241, 1030249)
TRAIN_REPEATS = 4
COUNT = 24
CANDIDATES = tuple(Configuration(backend, workers, batch)
    for backend in ('thread', 'process') for workers in (1, 2, 4, 8)
    for batch in (1, 4, 16))
BUDGET = 72


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def config_key(value):
    return canonical(asdict(value) if isinstance(value, Configuration) else value)


def write(path, report):
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
    tmp.replace(path)


def measurement(row):
    return Measurement(cold_seconds=row['cold_lifecycle_seconds'],
        warm_seconds=row['warm']['seconds'],
        correct=all(row[p]['correct'] for p in ('cold', 'warm')),
        error_count=sum(len(row[p]['errors']) for p in ('cold', 'warm')),
        cpu_seconds=row['lifecycle_cpu_seconds'],
        peak_rss_bytes=row['sampled_tree_peak_rss_bytes'],
        batch_p95_seconds=max(row[p]['batch_completion_latency_p95_seconds'] for p in ('cold', 'warm')))


def plan(name, affinity, candidates=CANDIDATES, *, training_seed=TRAIN_SEED,
         heldout_seeds=HOLDOUT_SEEDS, training_max_rss_bytes=None,
         include_baselines=True, experiment_metadata=None, record_fixture_failures=False):
    """Declare the original protocol, or an explicitly versioned follow-on.

    Defaults preserve the original plan exactly. A separate calibration RSS cap
    is an observed selection filter, never a resource reservation or runtime cap.
    """
    training_seeds = fixture_seeds(seed=training_seed, repeats=TRAIN_REPEATS)
    heldout_seeds = tuple(heldout_seeds)
    validate_split(training_seeds, heldout_seeds)
    if type(include_baselines) is not bool:
        raise ValueError('include_baselines must be bool')
    if type(record_fixture_failures) is not bool:
        raise ValueError('record_fixture_failures must be bool')
    if training_max_rss_bytes is not None:
        GateBudgets(max_rss_bytes=training_max_rss_bytes)
    if experiment_metadata is not None and type(experiment_metadata) is not dict:
        raise ValueError('experiment_metadata must be a JSON object')
    result = {'workload': name, 'grid': [asdict(c) for c in candidates],
        'training_seed': training_seed, 'training_fixture_seeds': training_seeds,
        'heldout_seeds': heldout_seeds, 'training_repeats': TRAIN_REPEATS,
        'count_per_pass': COUNT, 'passes_per_trial': 2, 'scale': 'representative', 'holdout_distribution': 'same-generator seeded mixed-input holdout; no distribution-shift claim',
        'objective': 'combined cold executor lifecycle plus warm pass elapsed',
        'selection': {'exhaustive': 'arithmetic mean of four paired training repeats',
            'exhaustive_median': 'median of four paired repeats, statistic-matched control for budgeted search',
            'budgeted': f'PRIMARY: successive halving, median score, minimum two paired repeats, maximum four, {3*len(candidates)} callback limit',
            'budgeted_lowcost': f'SECONDARY: successive halving, median score, minimum one paired repeat, maximum four, {2*len(candidates)} callback limit'},
        'selection_cost_scope': 'measured-matrix replay; virtual callback seconds summed from requested real training rows; fixture/reference costs separate; replay wall is not live search cost',
        'gates': {'max_p95_seconds': 60., 'max_rss_bytes': 1024**3, 'max_cpu_seconds': 300.},
        'affinity_requested': affinity,
        'baselines': {'standard_library_fixed': 'ThreadPoolExecutor at declared CPU-affinity worker cap, batch one',
            'manual_tuning_proxy': 'exhaustive mean-ranked training-only standard-library grid, same candidates/repeats/objective/gates; no claimed human expert',
            'direct_python': 'two direct sequential passes of the same workload algorithm, no executor'},
        'oracle': 'all declared candidate configurations on every independent heldout seed; aggregate oracle requires all gates on all seeds',
        'limits': 'one actual host, generated local mixed-input workloads, fixed explicit process safety; no upstream service compatibility, global optimum, cross-hardware generalization or deployment certification'}
    if training_max_rss_bytes is not None:
        result['training_gates'] = dict(result['gates'], max_rss_bytes=training_max_rss_bytes)
    if not include_baselines:
        result['baselines'] = {}
        result['baseline_scope'] = 'Intentionally omitted in this separate follow-on; no baseline comparison claim or denominator.'
    if experiment_metadata is not None:
        result['experiment'] = json.loads(canonical(experiment_metadata))
    if record_fixture_failures:
        result['fixture_failure_handling'] = 'Cache one preparation/reference failure per seed; retain explicit unexecuted grid slots, with no fabricated callback cost or tasks.'
    return result


def run(name, output, affinity=None, *, training_seed=TRAIN_SEED,
        heldout_seeds=HOLDOUT_SEEDS, training_max_rss_bytes=None,
        include_baselines=True, experiment_metadata=None, record_fixture_failures=False):
    if affinity is not None and (type(affinity) is not int or affinity < 1):
        raise ValueError("affinity must be a positive integer")
    if affinity:
        available = sorted(os.sched_getaffinity(0))
        if affinity > len(available):
            raise ValueError('requested affinity exceeds available CPUs')
        os.sched_setaffinity(0, available[:affinity])
    candidates = tuple(c for c in CANDIDATES if affinity is None or c.workers <= affinity)
    protocol = plan(name, affinity, candidates, training_seed=training_seed,
        heldout_seeds=heldout_seeds, training_max_rss_bytes=training_max_rss_bytes,
        include_baselines=include_baselines, experiment_metadata=experiment_metadata,
        record_fixture_failures=record_fixture_failures)
    heldout_seeds = tuple(protocol['heldout_seeds'])
    report = {'schema_version': 1, 'status': 'running', 'protocol': protocol,
        'protocol_sha256': digest(protocol), 'environment': workload_environment(),
        'training': [], 'manual_training': [], 'heldout': [], 'baseline_heldout': [],
        'datasets': [], 'selections': {}, 'failures': []}
    command_module = (getattr(getattr(sys.modules.get('__main__'), '__spec__', None), 'name', None)
                      if experiment_metadata is not None else None) or 'benchmarks.real_world.optimizer'
    report['environment']['command'] = shlex.join([sys.executable, '-m', command_module, *sys.argv[1:]])
    output.parent.mkdir(parents=True, exist_ok=True)
    write(output, report)
    started = time.perf_counter()
    module = module_for(name)
    with tempfile.TemporaryDirectory(prefix='optimizer-'+name+'-') as tmp:
        root = Path(tmp)
        fixture = module.fixture(root) if hasattr(module, 'fixture') else nullcontext()
        with fixture:
            datasets = {}
            def get_dataset(seed):
                if seed in datasets:
                    return datasets[seed]
                t = time.perf_counter()
                prep = None
                stage = 'fixture_preparation'
                try:
                    tasks = module.prepare(name, root / f'dataset-{seed}', COUNT, seed, scale='representative')
                    prep = time.perf_counter()-t
                    t = time.perf_counter()
                    stage = 'reference_validation'
                    rows = execute_batch((name, tasks))
                    if any(row['error'] for row in rows):
                        raise RuntimeError('Executor-independent same-algorithm reference failed: '+repr(rows))
                    reference = digest([row['value'] for row in rows])
                    data = {'tasks': tasks, 'reference_sha256': reference, 'seed': seed,
                        'count': COUNT, 'preparation_seconds': prep,
                        'reference_seconds': time.perf_counter()-t}
                except Exception as exc:
                    if not record_fixture_failures:
                        raise
                    duration = time.perf_counter()-t
                    data = {'seed': seed, 'count': COUNT, 'failure_stage': stage,
                        'failure': type(exc).__name__ + ': ' + str(exc)[:4000],
                        'preparation_seconds': duration if prep is None else prep,
                        'reference_seconds': duration if stage == 'reference_validation' else None,
                        'preparation_attempts': 1, 'reference_attempts': int(stage == 'reference_validation')}
                datasets[seed] = data
                report['datasets'].append({k:v for k,v in data.items() if k != 'tasks'})
                write(output, report)
                return data
            def observe(config, seed, phase, engine="intelligent", baseline_role=None):
                data = get_dataset(seed)
                if data.get('failure'):
                    row = {'engine': engine, 'configuration': asdict(config) if config else None, 'seed': seed,
                        'failure': 'fixture unavailable: ' + data['failure'],
                        'failure_stage': data['failure_stage'], 'trial_executed': False,
                        'measured_callback_seconds': None}
                else:
                    t = time.perf_counter()
                    try:
                        if engine == 'sequential':
                            row = direct_trial(name, data, seed)
                        else:
                            row = trial(name, engine, config, COUNT, seed,
                                root / f'{phase}-{len(report[phase])}', prepared=data)
                    except Exception as exc:
                        row = {'engine': engine, 'configuration': asdict(config) if config else None, 'seed': seed,
                            'failure': type(exc).__name__ + ': ' + str(exc)}
                    row['measured_callback_seconds'] = time.perf_counter()-t
                    if record_fixture_failures:
                        row['trial_executed'] = True
                if baseline_role:
                    row['baseline_role'] = baseline_role
                # Store bounded sufficient statistics; exact task result digest retained.
                for p in ('cold', 'warm'):
                    if p in row:
                        row[p].pop('completion_trace', None)
                row.pop('profiles_after_two_batches', None)
                report[phase].append(row)
                report['elapsed_seconds'] = time.perf_counter()-started
                write(output, report)
                cost = row['measured_callback_seconds']
                cost_text = f'{cost:.3f}' if cost is not None else 'not_executed'
                print(f'{name} {phase} {len(report[phase])}: {config} seed={seed} seconds={cost_text}', flush=True)
                return row
            for index, seed in enumerate(protocol['training_fixture_seeds']):
                order = list(candidates)
                random.Random(training_seed + index + 700).shuffle(order)
                for config in order:
                    engines = ['intelligent','stdlib'] if include_baselines else ['intelligent']
                    random.Random(f'{training_seed}:{index}:{config}').shuffle(engines)
                    for engine in engines:
                        observe(config, seed, 'training' if engine == 'intelligent' else 'manual_training', engine)
            matrix = {(config_key(r['configuration']), r['seed']): r for r in report['training']}
            training_gates = protocol.get('training_gates', protocol['gates'])
            objective = CalibrationObjective(elapsed='combined', max_cpu_seconds=training_gates['max_cpu_seconds'],
                max_peak_rss_bytes=training_gates['max_rss_bytes'], max_batch_p95_seconds=training_gates['max_p95_seconds'])
            for method in ('exhaustive', 'exhaustive_median', 'budgeted', 'budgeted_lowcost'):
                requested = []
                def replay(config, seed):
                    row = matrix[(config_key(config), seed)]
                    requested.append(row)
                    if row.get('failure'):
                        raise RuntimeError(row['failure'])
                    return measurement(row)
                t = time.perf_counter()
                if method == 'exhaustive':
                    chosen = calibrate(candidates, replay, repeats=4, seed=training_seed,
                        objective=objective, allow_process=True)
                elif method == 'exhaustive_median':
                    chosen = tune(candidates, replay, max_evaluations=4*len(candidates), min_repeats=4,
                        max_repeats=4, seed=training_seed, objective=objective, allow_process=True)
                elif method == 'budgeted_lowcost':
                    chosen = tune(candidates, replay, max_evaluations=2*len(candidates), min_repeats=1,
                        max_repeats=4, seed=training_seed, objective=objective, allow_process=True)
                else:
                    chosen = tune(candidates, replay, max_evaluations=3*len(candidates), min_repeats=2,
                        max_repeats=4, seed=training_seed, objective=objective, allow_process=True)
                replay_wall = time.perf_counter()-t
                validate_split(protocol['training_fixture_seeds'], heldout_seeds,
                    selection_fixture_seeds=[r['seed'] for r in requested])
                before_reuse = len(requested)
                saved = None
                reuse_seconds = None
                if chosen.selected is not None:
                    key = f'optimizer-evidence:v1:{name}:representative:24:{report["protocol_sha256"]}'
                    t = time.perf_counter()
                    saved = export_configuration(chosen, workload_key=key,
                        measured_scope='Explicit real fixture whole executor lifecycle and second warm pass')
                    restored = load_configuration(saved, workload_key=key, allow_process=True)
                    reuse_seconds = time.perf_counter()-t
                    if restored.configuration != chosen.selected:
                        raise RuntimeError('Saved configuration round trip changed selection')
                report['selections'][method] = {'selected': asdict(chosen.selected) if chosen.selected else None,
                    'selection_status': 'selected' if chosen.selected else 'no_eligible_configuration',
                    'report': chosen.to_dict(), 'evaluations': len(requested),
                    'virtual_callback_seconds': sum(r['measured_callback_seconds'] for r in requested if r['measured_callback_seconds'] is not None),
                    'unexecuted_training_slots_requested': sum(r.get('trial_executed') is False for r in requested),
                    'replay_wall_seconds': replay_wall, 'configuration_export_load_seconds': reuse_seconds,
                    'saved_configuration': json.loads(saved) if saved else None,
                    'reuse_calibration_calls': len(requested)-before_reuse,
                    'training_request_digest': digest([(r['configuration'],r['seed']) for r in requested])}
            if include_baselines:
                manual_matrix = {(config_key(r['configuration']),r['seed']):r for r in report['manual_training']}
                manual_requests = []
                def manual_replay(config,seed):
                    row = manual_matrix[(config_key(config),seed)]; manual_requests.append(row)
                    if row.get('failure'):
                        raise RuntimeError(row['failure'])
                    return measurement(row)
                manual = calibrate(candidates,manual_replay,repeats=4,seed=training_seed,
                    objective=objective,allow_process=True)
                report['manual_proxy_selection'] = {'selected':asdict(manual.selected) if manual.selected else None,
                    'selection_status': 'selected' if manual.selected else 'no_eligible_configuration', 'report':manual.to_dict(),
                    'evaluations':len(manual_requests),
                    'virtual_callback_seconds':sum(r['measured_callback_seconds'] for r in manual_requests if r['measured_callback_seconds'] is not None),
                    'unexecuted_training_slots_requested':sum(r.get('trial_executed') is False for r in manual_requests),
                    'scope':'training-only exhaustive grid, reproducible manual-tuning proxy, not human expertise'}
                report['manual_selection_frozen_sha256'] = digest(report['manual_proxy_selection'])
                fixed = Configuration('thread',max(c.workers for c in candidates),1)
                report['standard_library_fixed_configuration'] = asdict(fixed)
            report['selection_frozen_sha256'] = digest(report['selections'])
            write(output, report)
            print(f'{name}: selections FROZEN {report["selection_frozen_sha256"]}', flush=True)
            for index, seed in enumerate(heldout_seeds):
                order = list(candidates)
                random.Random(seed + 1300).shuffle(order)
                jobs = [('intelligent',config,None) for config in order]
                if include_baselines:
                    jobs.extend([('stdlib',fixed,'standard_library_fixed'),
                                 ('sequential',None,'direct_python')])
                    if manual.selected is not None:
                        jobs.append(('stdlib',manual.selected,'standard_library_manual_tuning_proxy'))
                random.Random(seed+2200).shuffle(jobs)
                for engine,config,role in jobs:
                    observe(config,seed,'heldout' if role is None else 'baseline_heldout',engine,role)
            if digest(report['selections']) != report['selection_frozen_sha256']:
                raise RuntimeError('Selection changed during heldout observation')
            if include_baselines and digest(report['manual_proxy_selection']) != report['manual_selection_frozen_sha256']:
                raise RuntimeError('Manual proxy changed during heldout')
            gates = GateBudgets(**protocol['gates'])
            report['summaries'] = {method: summarize_phases(value['selected'], report['heldout'],
                candidates=candidates, heldout_seeds=heldout_seeds, budgets=gates)
                for method,value in report['selections'].items()}
            report['baseline_comparisons'] = {method:compare_baselines(value['selected'],report['heldout'],
                report['baseline_heldout'],seeds=heldout_seeds,budgets=gates)
                for method,value in report['selections'].items()} if include_baselines else {}
    report['elapsed_seconds'] = time.perf_counter()-started
    report['status'] = 'complete'
    for method, phases in report['summaries'].items():
        if not phases['combined']['oracle_valid']:
            report['failures'].append(method + ': incomplete or no eligible exhaustive heldout oracle')
        if not phases['combined']['quality_gate_eligible']:
            report['failures'].append(method + ': selected configuration failed heldout correctness/resource gates')
    if include_baselines:
        report['failures'].extend(report['baseline_comparisons']['budgeted']['failures'])
    report['accepted'] = not report['failures']
    report['scheduled_trial_slots'] = sum(len(report[p]) for p in ('training','manual_training','heldout','baseline_heldout'))
    report['observed_trials'] = sum(r.get('trial_executed', True) is True for p in ('training','manual_training','heldout','baseline_heldout') for r in report[p])
    report['executed_task_observations'] = sum(r.get(p,{}).get('tasks',0) for phase in ('training','manual_training','heldout','baseline_heldout') for r in report[phase] for p in ('cold','warm'))
    write(output, report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workload', choices=NAMES, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--affinity', type=int)
    args = parser.parse_args()
    if (args.output.parent / ".stop-before-next-workload").exists():
        raise SystemExit(75)  # Cooperative stop before any fixture or measurement.
    try:
        result = run(args.workload, args.output, args.affinity)
    except Exception as exc:
        if args.output.exists():
            report = json.loads(args.output.read_text())
            report.update(status='failed', accepted=False)
            report['failures'].append(type(exc).__name__ + ': ' + str(exc))
            write(args.output, report)
        raise
    # Completed evidence remains usable even when a selector fails its quality gates.
    # Such failures are explicit in accepted/failures and must not stop independent workloads.
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
