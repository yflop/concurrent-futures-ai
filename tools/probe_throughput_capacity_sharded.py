"""Separately versioned sharded pipeline capacity correction, run serially.

Two bounded 64-item fixtures compose an actual 128-item, fixed-batch-16 experiment.
This changes the declared work to 16,384 pages per pass. The original failed
128-at-once scenario remains failed evidence, with no reclassification or overwrite. Selection replays
only its fresh training matrix; recorded callback costs are virtual search costs,
never live-search timings. The heldout oracle is conditional on this eight-member
batch-16 grid. Eight runnable batches are a capacity ceiling, not CPU utilization.

Run from the repository with PYTHONPATH=src:
python -m tools.probe_throughput_capacity_sharded --output PATH --affinity 8
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

from intelligent_futures.tuning import Configuration, CalibrationObjective, calibrate, tune
from benchmarks.real_world import module_for, execute_batch
from benchmarks.real_world import run as real_world_run
from benchmarks.real_world.baseline_measurements import direct_trial
from benchmarks.real_world.optimizer import config_key, digest, measurement, write
from benchmarks.real_world.optimizer_metrics import (
    GateBudgets, gate_failures, phase_seconds, search_cost_summary,
    summarize_phases, validate_split,
)
from benchmarks.real_world.run import trial, workload_environment


NAME = 'pipeline'
COUNT = 128
SHARD_COUNT = 64
SHARDS = 2
PAGES_PER_TASK = 128
PAGES_PER_PASS = 16384
SHARD_SEED_NAMESPACE = 64016431
BATCH_SIZE = 16
AFFINITY = 8
TRAIN_SEED = 64012817
TRAIN_REPEATS = 4
HOLDOUT_SEEDS = (64013819, 64014823, 64015829)
CANDIDATES = tuple(Configuration(backend, workers, BATCH_SIZE)
    for backend in ('thread', 'process') for workers in (1, 2, 4, 8))
BUDGET = 24
FIXED = Configuration('thread', 8, BATCH_SIZE)
METHODS = ('exhaustive', 'exhaustive_median', 'budgeted')
PHASES = ('training', 'heldout', 'baseline_heldout')
BASELINE_ROLES = ('standard_library_fixed', 'direct_python')
REPO_ROOT = Path(real_world_run.__file__).resolve().parents[2]
TOOL_PATH = 'tools/probe_throughput_capacity_sharded.py'
PRIOR_FAILED_SCENARIO = {
    'experiment': 'original unsharded pipeline capacity attempt',
    'protocol_sha256': '1af7bed87cc720eedbfb54414ce9595dd0d347d4c8520b274c7e89ae39124090',
    'raw_report_sha256': 'b536d0af1bc82d03946f51ca0c5d02093f9bb36578f5615ca1ff2840e96ec18b',
    'tool_sha256': '214e1b71a22d0445a8fa29fec046cbafea4fde8819867cba407dec13356b2659',
    'recorded_slots': 62, 'started_trials': 0, 'task_observations': 0,
    'reason': 'Representative prepare rejects count above 64. The original observed_trials=62 field counted recorded unexecuted slots, not executed trials.',
    'retained_as_failure': True,
    'scope': 'Separate corrected follow-on; never replace or reclassify the original failed scenario or remove it from existing quality denominators.',
}
EARLIER_NAMESPACES = (
    (1001207, (1010231, 1020241, 1030249)),
    (881237, (970171, 980183, 990191)),
    (1281607, (1282311, 1282417, 1282519)),
    (20261013, (20262011, 20262017, 20262023)),
    (20271013, (20272011, 20272017, 20272023)),
    (410101, (411101, 412101, 413101)),
    (410102, (411102, 412102, 413102)),
    (420101, (421101, 422101, 423101)),
    (420102, (421102, 422102, 423102)),
)


def fixture_seeds(seed):
    rng = random.Random(seed)
    return tuple(rng.randrange(2**63) for _ in range(TRAIN_REPEATS))


def source_file(name):
    return Path(__file__).resolve() if name == TOOL_PATH else REPO_ROOT / name


def shard_seeds(dataset_seed):
    """Deterministic seeds declared before preparing any dataset or running trials."""
    return tuple(int(digest({'version': 'pipeline-capacity-sharded-v2',
        'namespace': SHARD_SEED_NAMESPACE, 'dataset_seed': dataset_seed,
        'shard_index': index})[:16], 16) % (2**63) for index in range(SHARDS))


def composition_plan(training, heldout):
    return {'shards_per_dataset': SHARDS, 'tasks_per_shard': SHARD_COUNT,
        'tasks_per_pass': COUNT, 'generator_representative_count_limit': 64,
        'declared_pages_per_task': PAGES_PER_TASK, 'declared_pages_per_pass': PAGES_PER_PASS,
        'shard_seed_namespace': SHARD_SEED_NAMESPACE,
        'datasets': [{'dataset_seed': seed, 'phase': phase,
            'shards': [{'index': index, 'fixture_seed': shard_seed, 'count': SHARD_COUNT,
                       'directory': f'shard-{index}-{shard_seed}'}
                      for index, shard_seed in enumerate(shard_seeds(seed))]}
            for phase, seeds in (('training', training), ('heldout', heldout)) for seed in seeds],
        'task_identity': 'Original task dictionaries and per-shard IDs 0..63 are unchanged. Ordered (shard index, original task ID) identifies each task; task paths are disjoint.',
        'composition': 'Concatenate shard 0 then shard 1 without modifying, repeating, truncating or relabeling tasks. All candidates and the direct reference receive the identical ordered list.'}


def prepare_sharded(module, root, seed, *, metadata):
    tasks = []
    metadata['shards'] = []
    metadata['declared_pages_per_task'] = PAGES_PER_TASK
    metadata['declared_pages_per_pass'] = PAGES_PER_PASS
    for index, shard_seed in enumerate(shard_seeds(seed)):
        relative = f'shard-{index}-{shard_seed}'
        shard = {'index': index, 'fixture_seed': shard_seed, 'directory': relative,
                 'requested_count': SHARD_COUNT, 'status': 'preparing'}
        metadata['shards'].append(shard)
        started = time.perf_counter()
        try:
            prepared = module.prepare(NAME, root / relative, SHARD_COUNT, shard_seed,
                                      scale='representative')
            shard['prepared_task_count'] = len(prepared)
            if len(prepared) != SHARD_COUNT:
                raise ValueError('each bounded fixture shard must contain exactly 64 tasks')
            tasks.extend(prepared)
            shard['status'] = 'prepared'
        except BaseException as exc:
            shard['status'] = 'failed'
            shard['failure'] = type(exc).__name__ + ': ' + str(exc)
            raise
        finally:
            shard['preparation_seconds'] = time.perf_counter() - started
    if len({task['path'] for task in tasks}) != COUNT:
        raise ValueError('composed shard task paths must be distinct')
    return tasks


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
    earlier = {761213}
    for generator, heldout in EARLIER_NAMESPACES:
        earlier.update((generator, *fixture_seeds(generator), *heldout))
    logical = (TRAIN_SEED, SHARD_SEED_NAMESPACE, *training, *HOLDOUT_SEEDS)
    shards = tuple(s for seed in (*training, *HOLDOUT_SEEDS) for s in shard_seeds(seed))
    if len(set((*logical, *shards))) != len(logical) + len(shards):
        raise ValueError('all new dataset, generator and shard seeds must be distinct')
    if earlier.intersection((*logical, *shards)):
        raise ValueError('follow-on seeds overlap an earlier declared experiment')
    split = validate_split(training, HOLDOUT_SEEDS)
    return {'workload': NAME, 'experiment': 'pipeline-capacity-sharded-v2',
        'grid': [asdict(c) for c in CANDIDATES], 'count_per_pass': COUNT,
        'fixture_composition': composition_plan(training, HOLDOUT_SEEDS),
        'prior_failed_scenario': dict(PRIOR_FAILED_SCENARIO),
        'batch_size_fixed': BATCH_SIZE, 'passes_per_trial': 2, 'scale': 'representative',
        'training_seed': TRAIN_SEED, 'training_fixture_seeds': training,
        'training_repeats': TRAIN_REPEATS, 'heldout_seeds': HOLDOUT_SEEDS,
        'seed_split': split, 'earlier_seed_overlap': False, 'shard_seeds_disjoint_checked': True,
        'holdout_distribution': 'fresh same-generator mixed-input seeds; no distribution-shift claim',
        'fixture_size_caveat': 'The frozen representative generator permits at most 64 tasks per prepare call. Two independent 64-item shards each produce 128 pages per task via min((128,256,512)[i%3],8192//64), giving 16384 pages per pass. This doubles the unrealizable 8192-page work declaration in the original failed 128-at-once plan; this is not a pure task-count scaling experiment.',
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
            'callback_task_observations': 62 * COUNT * 2, 'reference_task_observations': 7 * COUNT,
            'callback_page_observations': 62 * PAGES_PER_PASS * 2,
            'reference_page_observations': 7 * PAGES_PER_PASS},
        'ordering': 'paired common-seed training repeats, reproducibly shuffled candidates; heldout candidates and baselines shuffled together after selection/protocol/source freeze',
        'limits': 'One local host, generated fixtures, no actual CPU utilization measurement, no inference about all batch sizes, upstream inputs, other hardware, or production deployment.'}


def costs(report):
    by_phase = {}
    for phase in PHASES:
        rows = report[phase]
        by_phase[phase] = {'observed_slots': len(rows),
            'started_callbacks': sum(r['execution_attempted'] for r in rows),
            'unexecuted_slots': sum(not r['execution_attempted'] for r in rows),
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
        'validated_reference_page_observations': sum(d.get('observed_reference_pages_per_pass', 0)
            for d in datasets if not d.get('failure')),
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


def fixture_preflight(output):
    """One training fixture only; never run task algorithms, pools, or selectors.

    Schedule separately only when the serial workload CPU slot is free. This
    verifies the real adapter boundary, distinct input files, and declared pages.
    It is preparation validation, not performance or configuration-quality data.
    """
    output = Path(output)
    if output.exists():
        raise FileExistsError('refusing to overwrite existing fixture preflight evidence')
    protocol = plan()
    seed = protocol['training_fixture_seeds'][0]
    report = {'schema_version': 1, 'mode': 'fixture_only_preflight', 'status': 'running',
        'protocol': protocol, 'protocol_sha256': digest(protocol), 'dataset_seed': seed,
        'source_sha256': {TOOL_PATH: hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'benchmarks/real_world/data.py': hashlib.sha256((REPO_ROOT / 'benchmarks/real_world/data.py').read_bytes()).hexdigest()},
        'started_trials': 0, 'executed_task_observations': 0, 'reference_task_observations': 0,
        'scope': 'One real composed training fixture and read-only file/page/content validation. No pipeline transformations, correctness-reference execution, executor, training selection, or heldout preparation.',
        'dataset': {'seed': seed}, 'failures': []}
    output.parent.mkdir(parents=True, exist_ok=True)
    write(output, report)
    started = time.perf_counter()
    try:
        with tempfile.TemporaryDirectory(prefix='pipeline-shard-preflight-') as temp:
            tasks = prepare_sharded(module_for(NAME), Path(temp), seed, metadata=report['dataset'])
            pages, identities = [], []
            for index, task in enumerate(tasks):
                content = hashlib.sha256()
                count = 0
                with Path(task['path']).open('rb') as handle:
                    for line in handle:
                        content.update(line)
                        count += 1
                pages.append(count)
                identities.append({'shard_index': index // SHARD_COUNT, 'original_task_id': task['id'],
                                   'pages': count, 'content_sha256': content.hexdigest()})
            if len(tasks) != COUNT or any(count != PAGES_PER_TASK for count in pages):
                raise ValueError('real composed fixture does not match its declared count/page workload')
            report.update(prepared_task_count=len(tasks), observed_pages_per_pass=sum(pages),
                observed_pages_per_task_min=min(pages), observed_pages_per_task_max=max(pages),
                ordered_input_content_sha256=digest(identities), input_content_identities=identities,
                status='complete', accepted=True)
    except BaseException as exc:
        report.update(status='failed', accepted=False)
        report['failures'].append(type(exc).__name__ + ': ' + str(exc))
        raise
    finally:
        report['elapsed_seconds'] = time.perf_counter() - started
        write(output, report)
    return report


def run(output, affinity=AFFINITY):
    output = Path(output)
    protocol = plan(affinity)
    if output.exists():
        raise FileExistsError('refusing to overwrite existing experiment evidence: ' + str(output))
    if not callable(getattr(os, 'sched_getaffinity', None)) or not callable(getattr(os, 'sched_setaffinity', None)):
        raise RuntimeError('the measured capacity probe requires Linux CPU-affinity APIs')
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
        report['scheduled_trial_slots'] = sum(len(report[p]) for p in PHASES)
        report['observed_trials'] = sum(r['execution_attempted'] for p in PHASES for r in report[p])
        report['unexecuted_trial_slots'] = report['scheduled_trial_slots'] - report['observed_trials']
        report['costs'] = costs(report)
        write(output, report)

    try:
        os.sched_setaffinity(0, sorted(available)[:affinity])
        actual = sorted(os.sched_getaffinity(0))
        if len(actual) != affinity:
            raise RuntimeError('failed to establish exactly eight affinity CPUs')
        report['environment'] = workload_environment()
        report['environment']['command'] = shlex.join([sys.executable, '-m', 'tools.probe_throughput_capacity_sharded', *sys.argv[1:]])
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
                            tasks = prepare_sharded(module, root / f'dataset-{seed}', seed, metadata=meta)
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
                        pages = [row['value']['pages'] for row in rows]
                        meta['observed_reference_pages_per_task_min'] = min(pages)
                        meta['observed_reference_pages_per_task_max'] = max(pages)
                        meta['observed_reference_pages_per_pass'] = sum(pages)
                        if any(pages_per_task != PAGES_PER_TASK for pages_per_task in pages):
                            raise RuntimeError('pipeline reference did not observe the declared 128 pages per task')
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
            if not source_file(name).is_file() or hashlib.sha256(source_file(name).read_bytes()).hexdigest() != sha]
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
        report['scheduled_trial_slots'] = sum(len(report[p]) for p in PHASES)
        report['observed_trials'] = sum(r['execution_attempted'] for p in PHASES for r in report[p])
        report['unexecuted_trial_slots'] = report['scheduled_trial_slots'] - report['observed_trials']
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
    parser.add_argument('--fixture-preflight', action='store_true',
        help='Prepare and validate one real training fixture only; run separately when the CPU slot is free.')
    args = parser.parse_args(argv)
    if (args.output.parent / '.stop-before-next-workload').exists():
        return 75
    if args.fixture_preflight:
        fixture_preflight(args.output)
    else:
        run(args.output, args.affinity)
    # Rejected selection evidence must not halt independently scheduled experiments.
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
