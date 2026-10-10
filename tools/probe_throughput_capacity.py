"""Predeclared pipeline capacity follow-on, run serially after the primary matrix.

This is an actual 128-item, fixed-batch-16 workload experiment. Selection replays
only its fresh training matrix; recorded callback costs are virtual search costs,
never live-search timings. The heldout oracle is conditional on this eight-member
batch-16 grid. Eight runnable batches are a capacity ceiling, not CPU utilization.

Run from the repository with PYTHONPATH=src:
python -m tools.probe_throughput_capacity --output PATH --affinity 8
"""
from __future__ import annotations

import argparse
from contextlib import nullcontext
from dataclasses import asdict
import hashlib
import os
from pathlib import Path
import random
import shlex
import sys
import tempfile
import time

from intelligent_futures.tuning import Configuration, CalibrationObjective, calibrate, tune
from benchmarks.real_world import module_for, execute_batch
from benchmarks.real_world.baseline_measurements import direct_trial
from benchmarks.real_world.optimizer import config_key, digest, measurement, write
from benchmarks.real_world.optimizer_metrics import (
    GateBudgets, gate_failures, phase_seconds, search_cost_summary,
    summarize_phases, validate_split,
)
from benchmarks.real_world.run import trial, workload_environment


NAME = 'pipeline'
COUNT = 128
BATCH_SIZE = 16
AFFINITY = 8
TRAIN_SEED = 1281607
TRAIN_REPEATS = 4
HOLDOUT_SEEDS = (1282311, 1282417, 1282519)
CANDIDATES = tuple(Configuration(backend, workers, BATCH_SIZE)
    for backend in ('thread', 'process') for workers in (1, 2, 4, 8))
BUDGET = 24
FIXED = Configuration('thread', 8, BATCH_SIZE)
METHODS = ('exhaustive', 'exhaustive_median', 'budgeted')
PHASES = ('training', 'heldout', 'baseline_heldout')
BASELINE_ROLES = ('standard_library_fixed', 'direct_python')
REPO_ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = 'tools/probe_throughput_capacity.py'


def fixture_seeds(seed):
    rng = random.Random(seed)
    return tuple(rng.randrange(2**63) for _ in range(TRAIN_REPEATS))


def parallelism(config, affinity=AFFINITY):
    """Static runnable-batch/worker ceiling; this does not measure scheduling."""
    batches = (COUNT + config.batch_size - 1) // config.batch_size
    return {'runnable_batches_per_pass': batches,
        'worker_batch_parallelism_ceiling': min(batches, config.workers),
        'affinity_bounded_parallelism_ceiling': min(batches, config.workers, affinity),
        'scope': 'computed maximum concurrent batches, not observed concurrency or CPU utilization'}


def plan(affinity=AFFINITY):
    if type(affinity) is not int or affinity != AFFINITY:
        raise ValueError('this predeclared probe requires exactly eight affinity CPUs')
    training = fixture_seeds(TRAIN_SEED)
    # Include both seed-generator identities and their generated training seeds.
    earlier = {1001207, 1010231, 1020241, 1030249,
        881237, 970171, 980183, 990191,
        *fixture_seeds(1001207), *fixture_seeds(881237)}
    if earlier.intersection((TRAIN_SEED, *training, *HOLDOUT_SEEDS)):
        raise ValueError('follow-on seeds overlap an earlier declared experiment')
    split = validate_split(training, HOLDOUT_SEEDS)
    return {'workload': NAME, 'experiment': 'fixed-batch-16 throughput capacity follow-on',
        'grid': [asdict(c) for c in CANDIDATES], 'count_per_pass': COUNT,
        'batch_size_fixed': BATCH_SIZE, 'passes_per_trial': 2, 'scale': 'representative',
        'training_seed': TRAIN_SEED, 'training_fixture_seeds': training,
        'training_repeats': TRAIN_REPEATS, 'heldout_seeds': HOLDOUT_SEEDS,
        'seed_split': split, 'earlier_seed_overlap': False,
        'holdout_distribution': 'fresh same-generator mixed-input seeds; no distribution-shift claim',
        'fixture_size_caveat': 'The frozen generator uses min((128,256,512)[i%3],8192//count) pages per task. At count 128 all 128 tasks have 64 pages, totaling 8192 pages per pass. Individual tasks are smaller than at primary count 24; this is not a pure task-count scaling experiment.',
        'objective': 'combined cold executor lifecycle plus warm pass elapsed',
        'selection': {'budgeted': 'PRIMARY: median successive halving, minimum 2 / maximum 4 repeats, callback limit 24',
            'exhaustive': 'four paired training repeats, arithmetic mean, 32 callbacks',
            'exhaustive_median': 'four paired repeats, median statistic-matched control, callback limit 32; failed candidates may stop early'},
        'selection_cost_scope': 'Recorded-training replay only. Virtual callback seconds sum requested measured rows, including failed rows. Replay wall seconds are selector overhead, not live calibration.',
        'gates': {'max_p95_seconds': 60., 'max_rss_bytes': 1024**3, 'max_cpu_seconds': 300.},
        'correctness': 'zero errors, exactly 128 results per pass, same-algorithm reference SHA256 match',
        'gate_scope': 'observed acceptance gates, not hard runtime resource limits',
        'affinity_requested': affinity, 'runnable_batches_per_pass': COUNT // BATCH_SIZE,
        'parallelism_ceilings': [{'configuration': asdict(c), **parallelism(c, affinity)} for c in CANDIDATES],
        'baseline_configurations': {'standard_library_fixed': asdict(FIXED), 'direct_python': None},
        'baseline_scope': 'three heldout repeats each; fixed stdlib with 8 threads and batch size 16, and direct sequential same-algorithm calls; no stdlib training search',
        'oracle': 'all eight configurations on three heldout seeds; conditional on batch_size=16 and this host/fixture/affinity, never a global optimum',
        'planned_costs': {'training_callbacks': 32, 'heldout_oracle_callbacks': 24,
            'baseline_callbacks': 6, 'total_callbacks': 62, 'reference_passes': 7,
            'callback_task_observations': 62 * COUNT * 2, 'reference_task_observations': 7 * COUNT},
        'ordering': 'paired common-seed training repeats, reproducibly shuffled candidates; heldout candidates and baselines shuffled together after selection/protocol/source freeze',
        'limits': 'One local host, generated fixtures, no actual CPU utilization measurement, no inference about all batch sizes, upstream inputs, other hardware, or production deployment.'}


def costs(report):
    by_phase = {}
    for phase in PHASES:
        rows = report[phase]
        by_phase[phase] = {'observed_slots': len(rows),
            'started_callbacks': sum(r['execution_attempted'] for r in rows),
            'failed_slots': sum(bool(r.get('failure')) for r in rows),
            'gate_failed_slots': sum(bool(r['gate_failures']) for r in rows),
            'callback_seconds': sum(r['measured_callback_seconds'] for r in rows),
            'failed_callback_seconds': sum(r['measured_callback_seconds'] for r in rows if r.get('failure')),
            'gate_failed_callback_seconds': sum(r['measured_callback_seconds'] for r in rows if r['gate_failures']),
            'task_observations': sum(r.get(p, {}).get('tasks', 0) for r in rows for p in ('cold', 'warm'))}
    datasets = report['datasets']
    callback = sum(p['callback_seconds'] for p in by_phase.values())
    preparation = sum(d['preparation_seconds'] for d in datasets)
    reference = sum(d['reference_seconds'] for d in datasets)
    replay = sum(s['replay_wall_seconds'] for s in report['selections'].values())
    elapsed = report.get('elapsed_seconds', 0.)
    return {'by_phase': by_phase, 'observed_callback_seconds': callback,
        'fixture_preparation_seconds': preparation, 'reference_seconds': reference,
        'failed_dataset_preparation_and_reference_seconds': sum(
            d['preparation_seconds'] + d['reference_seconds'] for d in datasets if d.get('failure')),
        'reference_task_observations': sum(d.get('reference_result_count', 0) for d in datasets),
        'selection_replay_wall_seconds': replay, 'actual_elapsed_seconds': elapsed,
        'other_wall_seconds_including_checkpoints_fixture_lifecycle_and_environment': max(0., elapsed - callback - preparation - reference - replay),
        'scope': 'Actual serial experiment cost, including attempted failures. Shared preparation/reference costs are separate and counted once per seed. Replay virtual costs overlap the already executed training matrix and are not added again.'}


def compare_references(selected, heldout, baselines, budgets):
    """Only the two declared baselines; do not invent missing manual-search slots."""
    comparisons = []
    for seed in HOLDOUT_SEEDS:
        selected_rows = [r for r in heldout if r['seed'] == seed and r['configuration'] == selected]
        chosen = selected_rows[0] if len(selected_rows) == 1 else None
        for role in BASELINE_ROLES:
            matches = [r for r in baselines if r['seed'] == seed and r['baseline_role'] == role]
            baseline = matches[0] if len(matches) == 1 else None
            reasons = (['selected_missing_or_duplicate'] if chosen is None else gate_failures(chosen, budgets))
            reasons += (['baseline_missing_or_duplicate'] if baseline is None else gate_failures(baseline, budgets))
            for phase in ('cold', 'warm', 'combined'):
                a = phase_seconds(chosen, phase) if chosen and not gate_failures(chosen, budgets) else None
                b = phase_seconds(baseline, phase) if baseline and not gate_failures(baseline, budgets) else None
                comparisons.append({'seed': seed, 'baseline_role': role, 'phase': phase,
                    'selected_seconds': a, 'baseline_seconds': b,
                    'baseline_time_over_selected_time': b / a if not reasons else None,
                    'failures': reasons, 'passes_correctness_resource_gates': not reasons})
    return {'comparisons': comparisons, 'expected_comparison_slots': 18,
        'valid_comparison_slots': sum(r['passes_correctness_resource_gates'] for r in comparisons),
        'scope': 'Fixed stdlib and direct-Python reference ratios; includes engine and configuration differences, not a pure configuration-selection speedup. Direct p95 measures task service time, executor p95 measures batch completion latency.'}


def run(output, affinity=AFFINITY):
    output = Path(output)
    protocol = plan(affinity)
    if output.exists():
        raise FileExistsError('refusing to overwrite existing experiment evidence: ' + str(output))
    available = set(os.sched_getaffinity(0))
    if len(available) < affinity:
        raise ValueError('predeclared eight-CPU affinity is unavailable; do not shrink this candidate grid')
    started = time.perf_counter()
    report = {'schema_version': 1, 'status': 'running', 'protocol': protocol,
        'protocol_sha256': digest(protocol), 'training': [], 'heldout': [],
        'baseline_heldout': [], 'datasets': [], 'selections': {}, 'failures': []}
    output.parent.mkdir(parents=True, exist_ok=True)
    write(output, report)

    def checkpoint():
        report['elapsed_seconds'] = time.perf_counter() - started
        report['costs'] = costs(report)
        write(output, report)

    try:
        os.sched_setaffinity(0, sorted(available)[:affinity])
        actual = sorted(os.sched_getaffinity(0))
        if len(actual) != affinity:
            raise RuntimeError('failed to establish exactly eight affinity CPUs')
        report['environment'] = workload_environment()
        report['environment']['command'] = shlex.join([sys.executable, '-m', 'tools.probe_throughput_capacity', *sys.argv[1:]])
        report['environment']['probe_affinity_cpu_ids'] = actual
        report['environment'].setdefault('source_sha256', {})[TOOL_PATH] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        report['source_manifest_sha256'] = digest(report['environment']['source_sha256'])
        checkpoint()
        budgets = GateBudgets(**protocol['gates'])
        module = module_for(NAME)
        with tempfile.TemporaryDirectory(prefix='pipeline-capacity-') as temp:
            root = Path(temp)
            fixture = module.fixture(root) if hasattr(module, 'fixture') else nullcontext()
            with fixture:
                datasets = {}

                def dataset(seed):
                    if seed in datasets:
                        data = datasets[seed]
                        if data.get('failure'):
                            raise RuntimeError(data['failure'])
                        return data
                    meta = {'seed': seed, 'count': COUNT, 'preparation_seconds': 0., 'reference_seconds': 0.}
                    report['datasets'].append(meta)
                    data = dict(meta)
                    datasets[seed] = data
                    try:
                        t = time.perf_counter()
                        try:
                            tasks = module.prepare(NAME, root / f'dataset-{seed}', COUNT, seed, scale='representative')
                        finally:
                            meta['preparation_seconds'] = time.perf_counter() - t
                        meta['prepared_task_count'] = len(tasks)
                        if len(tasks) != COUNT:
                            raise ValueError('fixture did not prepare exactly 128 tasks')
                        t = time.perf_counter()
                        try:
                            rows = execute_batch((NAME, tasks))
                        finally:
                            meta['reference_seconds'] = time.perf_counter() - t
                        meta['reference_result_count'] = len(rows)
                        meta['reference_errors'] = [r['error'] for r in rows if r['error'] is not None]
                        if len(rows) != COUNT or meta['reference_errors']:
                            raise RuntimeError('executor-independent reference failed count/error validation')
                        meta['reference_sha256'] = digest([r['value'] for r in rows])
                        data.update(meta, tasks=tasks)
                    except BaseException as exc:
                        meta['failure'] = type(exc).__name__ + ': ' + str(exc)
                        data.update(meta)
                        raise
                    finally:
                        checkpoint()
                    return data

                def observe(config, seed, phase, engine='intelligent', role=None):
                    row = {'engine': engine, 'configuration': asdict(config) if config else None, 'seed': seed}
                    callback_started = None
                    interruption = None
                    try:
                        data = dataset(seed)
                        callback_started = time.perf_counter()
                        if engine == 'sequential':
                            observed = direct_trial(NAME, data, seed)
                        else:
                            observed = trial(NAME, engine, config, COUNT, seed,
                                root / f'{phase}-{len(report[phase])}', prepared=data)
                        row.update(observed)
                        if (row['engine'] != engine or row['configuration'] != (asdict(config) if config else None)
                                or row['seed'] != seed):
                            raise ValueError('trial identity does not match its declared slot')
                        if any(row[p]['tasks'] != COUNT or row[p]['result_sha256'] != data['reference_sha256']
                                for p in ('cold', 'warm')):
                            raise ValueError('trial count/reference hash does not match the prepared 128-item fixture')
                    except BaseException as exc:
                        row['failure'] = type(exc).__name__ + ': ' + str(exc)
                        # Keep failed observations associated with their requested slot.
                        row.update(engine=engine, configuration=asdict(config) if config else None, seed=seed)
                        if not isinstance(exc, Exception):
                            interruption = exc
                    row['execution_attempted'] = callback_started is not None
                    row['measured_callback_seconds'] = 0. if callback_started is None else time.perf_counter() - callback_started
                    if role:
                        row['baseline_role'] = role
                    if config:
                        row['parallelism_ceiling'] = parallelism(config, affinity)
                    for p in ('cold', 'warm'):
                        if p in row:
                            row[p].pop('completion_trace', None)
                    row.pop('profiles_after_two_batches', None)
                    row['gate_failures'] = gate_failures(row, budgets)
                    report[phase].append(row)
                    checkpoint()
                    print(f'{NAME} capacity {phase} {len(report[phase])}: {config} seed={seed} '
                          f'seconds={row["measured_callback_seconds"]:.3f} gates={row["gate_failures"]}', flush=True)
                    if interruption is not None:
                        raise interruption

                for index, seed in enumerate(protocol['training_fixture_seeds']):
                    order = list(CANDIDATES)
                    random.Random(TRAIN_SEED + index + 700).shuffle(order)
                    for config in order:
                        observe(config, seed, 'training')
                matrix = {(config_key(r['configuration']), r['seed']): r for r in report['training']}
                objective = CalibrationObjective(elapsed='combined', max_cpu_seconds=300.,
                    max_peak_rss_bytes=1024**3, max_batch_p95_seconds=60.)
                reference_costs = [{**r, 'callback_seconds': r['measured_callback_seconds']} for r in report['training']]
                for method in METHODS:
                    requested = []

                    def replay(config, seed):
                        row = matrix[(config_key(config), seed)]
                        requested.append(row)
                        if row['gate_failures']:
                            raise RuntimeError('; '.join(row['gate_failures']) + ': ' + row.get('failure', ''))
                        return measurement(row)

                    t = time.perf_counter()
                    chosen = None
                    selection_failure = None
                    try:
                        if method == 'exhaustive':
                            chosen = calibrate(CANDIDATES, replay, repeats=4, seed=TRAIN_SEED,
                                objective=objective, allow_process=True)
                        else:
                            chosen = tune(CANDIDATES, replay, max_evaluations=BUDGET if method == 'budgeted' else 32,
                                min_repeats=2 if method == 'budgeted' else 4, max_repeats=4,
                                seed=TRAIN_SEED, objective=objective, allow_process=True)
                    except Exception as exc:
                        selection_failure = type(exc).__name__ + ': ' + str(exc)
                    wall = time.perf_counter() - t
                    validate_split(protocol['training_fixture_seeds'], HOLDOUT_SEEDS,
                        selection_fixture_seeds=[r['seed'] for r in requested])
                    selected = chosen.selected if chosen is not None else None
                    report['selections'][method] = {'selected': asdict(selected) if selected else None,
                        'selection_status': 'failed' if selection_failure else 'selected' if selected else 'no_eligible_configuration',
                        'failure': selection_failure, 'report': chosen.to_dict() if chosen else None,
                        'evaluations': len(requested), 'replay_wall_seconds': wall,
                        'virtual_callback_seconds': sum(r['measured_callback_seconds'] for r in requested),
                        'virtual_failed_callback_seconds': sum(r['measured_callback_seconds'] for r in requested if r.get('failure')),
                        'training_request_digest': digest([(r['configuration'], r['seed']) for r in requested]),
                        'cost_audit': search_cost_summary(requested, reference_costs, replay=True,
                            actual_wall_seconds=wall, candidates=CANDIDATES,
                            training_seeds=protocol['training_fixture_seeds'])}
                    checkpoint()
                report['selection_frozen_sha256'] = digest(report['selections'])
                frozen = {'protocol_sha256': report['protocol_sha256'],
                    'selection_frozen_sha256': report['selection_frozen_sha256'],
                    'source_manifest_sha256': report['source_manifest_sha256']}
                report['heldout_freeze'] = {**frozen, 'sha256': digest(frozen),
                    'observed_heldout_rows': 0, 'prepared_heldout_datasets': 0}
                checkpoint()
                print(f'{NAME} capacity: selections and protocol FROZEN {report["heldout_freeze"]["sha256"]}', flush=True)
                for seed in HOLDOUT_SEEDS:
                    jobs = [('intelligent', c, None) for c in CANDIDATES] + [
                        ('stdlib', FIXED, 'standard_library_fixed'), ('sequential', None, 'direct_python')]
                    random.Random(seed + 2200).shuffle(jobs)
                    for engine, config, role in jobs:
                        observe(config, seed, 'baseline_heldout' if role else 'heldout', engine, role)
                if digest(report['selections']) != report['selection_frozen_sha256']:
                    raise RuntimeError('selection changed during heldout observation')
                if digest(report['protocol']) != report['protocol_sha256']:
                    raise RuntimeError('protocol changed during heldout observation')
                report['summaries'] = {method: summarize_phases(selection['selected'], report['heldout'],
                    candidates=CANDIDATES, heldout_seeds=HOLDOUT_SEEDS, budgets=budgets)
                    for method, selection in report['selections'].items()}
                for phases in report['summaries'].values():
                    for summary in phases.values():
                        summary['oracle_scope'] = protocol['oracle']
                report['baseline_comparisons'] = {method: compare_references(selection['selected'],
                    report['heldout'], report['baseline_heldout'], budgets)
                    for method, selection in report['selections'].items()}
        report['source_changes_during_run'] = [name for name, sha in report['environment']['source_sha256'].items()
            if not (REPO_ROOT / name).is_file() or hashlib.sha256((REPO_ROOT / name).read_bytes()).hexdigest() != sha]
        if report['source_changes_during_run']:
            report['failures'].append('source files changed during the experiment')
        for method, phases in report['summaries'].items():
            if not phases['combined']['oracle_valid']:
                report['failures'].append(method + ': incomplete or no eligible conditional batch-16 oracle')
            if not phases['combined']['quality_gate_eligible']:
                report['failures'].append(method + ': no eligible heldout selection')
            if report['baseline_comparisons'][method]['valid_comparison_slots'] != 18:
                report['failures'].append(method + ': incomplete or failed baseline comparisons')
        report['accepted'] = not report['failures']
        report['status'] = 'complete'
        report['observed_trials'] = sum(len(report[p]) for p in PHASES)
        report['executed_task_observations'] = sum(r.get(p, {}).get('tasks', 0)
            for phase in PHASES for r in report[phase] for p in ('cold', 'warm'))
        checkpoint()
        return report
    except BaseException as exc:
        report.update(status='interrupted' if isinstance(exc, (KeyboardInterrupt, SystemExit)) else 'failed', accepted=False)
        report['failures'].append(type(exc).__name__ + ': ' + str(exc))
        checkpoint()
        raise
    finally:
        os.sched_setaffinity(0, available)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--affinity', type=int, choices=(AFFINITY,), default=AFFINITY)
    args = parser.parse_args(argv)
    if (args.output.parent / '.stop-before-next-workload').exists():
        return 75
    run(args.output, args.affinity)
    # Rejected selection evidence must not halt independently scheduled experiments.
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
