"""Predeclared, serial one-/two-CPU affinity follow-ons on the same Linux host.

This wrapper calls the frozen real-world optimizer with its original 24-item,
two-pass workload and unchanged acceptance gates. It does not simulate hardware,
measure CPU utilization, retune from heldout data, or launch nested experiments.
Run each invocation under tools/run_bounded_command.py, serially after primary
measurements. Supply the raw completed eight-CPU primary report as --baseline.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import sys
import time

from intelligent_futures.tuning import fixture_seeds, load_configuration
from benchmarks.real_world import optimizer
from benchmarks.real_world.optimizer_metrics import validate_split


ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = 'tools/probe_affinity_constraints.py'
NAMES = ('images', 'geospatial')
CAPS = (1, 2)
TRAINING_SEEDS = {('images', 1): 410101, ('images', 2): 410102,
                  ('geospatial', 1): 420101, ('geospatial', 2): 420102}
HELDOUT_SEEDS = {key: tuple(seed + offset for offset in (1000, 2000, 3000))
                 for key, seed in TRAINING_SEEDS.items()}
EARLIER_NAMESPACES = (
    (1001207, (1010231, 1020241, 1030249)),
    (881237, (970171, 980183, 990191)),
    (20261013, (20262011, 20262017, 20262023)),
    (20271013, (20272011, 20272017, 20272023)),
    (1281607, (1282311, 1282417, 1282519)),
)
PHASES = ('training', 'manual_training', 'heldout', 'baseline_heldout')


def source_hashes():
    files = sorted([*(ROOT / 'src' / 'intelligent_futures').rglob('*.py'),
                    *(ROOT / 'benchmarks' / 'real_world').rglob('*.py'),
                    Path(__file__).resolve()])
    return {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in files}


def validate_namespaces():
    """Check generator IDs and generated fixture seeds across all four runs."""
    used = {761213}  # Existing endurance fixture seed, also kept separate.
    for training, heldout in EARLIER_NAMESPACES:
        used.update((training, *fixture_seeds(seed=training, repeats=4), *heldout))
    for key, training in TRAINING_SEEDS.items():
        generated = fixture_seeds(seed=training, repeats=4)
        heldout = HELDOUT_SEEDS[key]
        validate_split(generated, heldout)
        namespace = {training, *generated, *heldout}
        if len(namespace) != 8 or namespace.intersection(used):
            raise ValueError('affinity follow-on seed namespaces must be independent and fresh')
        used.update(namespace)


def read_baseline(path, name):
    payload = Path(path).read_bytes()
    baseline = json.loads(payload)
    protocol = baseline.get('protocol', {})
    if (baseline.get('status') != 'complete' or protocol.get('workload') != name
            or protocol.get('affinity_requested') != 8
            or protocol.get('count_per_pass') != 24
            or protocol.get('training_seed') != optimizer.TRAIN_SEED):
        raise ValueError('baseline must be the raw completed eight-CPU primary report for the same workload')
    if optimizer.digest(protocol) != baseline.get('protocol_sha256'):
        raise ValueError('baseline protocol hash is invalid')
    if optimizer.digest(baseline.get('selections', {})) != baseline.get('selection_frozen_sha256'):
        raise ValueError('baseline selection hash is invalid; use raw evidence, not a redacted projection')
    saved = baseline.get('selections', {}).get('budgeted', {}).get('saved_configuration')
    expected_key = f'optimizer-evidence:v1:{name}:representative:24:{baseline["protocol_sha256"]}'
    if saved is not None and (not isinstance(saved, dict) or saved.get('workload_key') != expected_key):
        raise ValueError('baseline saved profile does not have its original exact workload key')
    return {'file_sha256': hashlib.sha256(payload).hexdigest(),
        'protocol_sha256': baseline['protocol_sha256'],
        'selection_frozen_sha256': baseline['selection_frozen_sha256'],
        'workload': name, 'affinity_requested': 8,
        'saved_configuration': saved}


def protocol(name, affinity, *, sources, baseline, available_cpu_ids, constrained_cpu_ids):
    if (name, affinity) not in TRAINING_SEEDS or type(affinity) is not int:
        raise ValueError('only images/geospatial at affinity 1 or 2 are predeclared')
    validate_namespaces()
    candidates = tuple(c for c in optimizer.CANDIDATES if c.workers <= affinity)
    count = len(candidates)
    metadata = {'version': 'same-host-affinity-constraints-v1',
        'scope': 'A separate single-host operating-system CPU-affinity constraint experiment. No new-hardware, simulated-hardware, cross-host generalization, or measured CPU-utilization claim.',
        'replaces_primary_evidence': False, 'predeclared_run_count': 4,
        'seed_namespaces_disjoint_checked': True,
        'source_sha256': sources, 'source_manifest_sha256': optimizer.digest(sources),
        'runner_source_sha256': sources[TOOL_PATH],
        'available_affinity_cpu_ids_before': sorted(available_cpu_ids),
        'constrained_affinity_cpu_ids': sorted(constrained_cpu_ids),
        'primary_baseline': {k: v for k, v in baseline.items() if k != 'saved_configuration'},
        'profile_reuse_check': 'Before fresh calibration, load the primary budgeted saved profile under the real narrowed affinity with its exact stored workload key; expect hardware fingerprint mismatch. This profile is never used for selection or execution.',
        'execution': 'One serial optimizer.run invocation. External tools/run_bounded_command.py supervision is required operationally; this wrapper creates no nested measurement jobs.',
        'planned_counts': {'candidates': count, 'training_callbacks': 4 * count,
            'manual_training_callbacks': 4 * count, 'heldout_oracle_callbacks': 3 * count,
            'baseline_callbacks': 9, 'total_callback_slots': 11 * count + 9,
            'primary_replay_callback_limit': 3 * count, 'exhaustive_replay_callbacks': 4 * count,
            'reference_passes': 7, 'reference_task_observations': 7 * 24,
            'callback_task_observations': (11 * count + 9) * 24 * 2},
        'baseline_missing_selection_scope': 'If manual training has no eligible configuration, its three heldout baseline slots remain visibly missing; all other methods and oracle rows still run.',
        'oracle_scope': 'Best all-repeat-admissible observed candidate in this workload and affinity-specific 6- or 12-member grid; conditional on the declared host, fixture generator, gates and candidate universe, not a global optimum.'}
    return optimizer.plan(name, affinity, candidates,
        training_seed=TRAINING_SEEDS[(name, affinity)], heldout_seeds=HELDOUT_SEEDS[(name, affinity)],
        include_baselines=True, experiment_metadata=metadata, record_fixture_failures=True)


def check_profile_reuse(baseline, affinity):
    saved = baseline['saved_configuration']
    result = {'primary_affinity': 8, 'current_affinity': affinity,
        'expected_rejection': 'hardware fingerprint mismatch', 'calibration_callbacks': 0,
        'passed': False, 'scope': 'Strict persisted-profile compatibility check only; no profile is adopted.'}
    if saved is None:
        return dict(result, status='unavailable', reason='primary budgeted method had no saved configuration')
    result['workload_key'] = saved['workload_key']
    started = time.perf_counter()
    try:
        load_configuration(optimizer.canonical(saved), workload_key=saved['workload_key'], allow_process=True)
    except ValueError as exc:
        result.update(status='rejected', reason=str(exc), passed=str(exc) == 'hardware fingerprint mismatch')
    except Exception as exc:
        result.update(status='failed', reason=type(exc).__name__ + ': ' + str(exc))
    else:
        result.update(status='unexpectedly_accepted', reason='primary profile was accepted under changed real affinity')
    result['elapsed_seconds'] = time.perf_counter() - started
    return result


def accounting(report):
    planned = report['protocol']['experiment']['planned_counts']
    expected = dict(zip(PHASES, (planned['training_callbacks'], planned['manual_training_callbacks'],
                               planned['heldout_oracle_callbacks'], planned['baseline_callbacks'])))
    phases = {}
    for phase in PHASES:
        rows = report.get(phase, [])
        phases[phase] = {'expected_slots': expected[phase], 'observed_slots': len(rows),
            'missing_slots': max(0, expected[phase] - len(rows)),
            'executed_callbacks': sum(r.get('trial_executed', True) is True for r in rows),
            'unexecuted_slots': sum(r.get('trial_executed') is False for r in rows),
            'failure_slots': sum(r.get('failure') is not None for r in rows),
            'callback_seconds': sum(r.get('measured_callback_seconds') or 0. for r in rows),
            'failed_callback_seconds': sum(r.get('measured_callback_seconds') or 0. for r in rows if r.get('failure')),
            'executed_callbacks_with_unknown_cost': sum(r.get('trial_executed', True) is True
                and r.get('measured_callback_seconds') is None for r in rows),
            'returned_task_observations': sum(r.get(p, {}).get('tasks', 0) for r in rows for p in ('cold', 'warm'))}
    datasets = report.get('datasets', [])
    return {'phases': phases, 'expected_callback_slots': planned['total_callback_slots'],
        'observed_callback_slots': sum(v['observed_slots'] for v in phases.values()),
        'actual_executed_callbacks': sum(v['executed_callbacks'] for v in phases.values()),
        'actual_callback_seconds': sum(v['callback_seconds'] for v in phases.values()),
        'failed_callback_seconds': sum(v['failed_callback_seconds'] for v in phases.values()),
        'fixture_preparation_seconds': sum(d.get('preparation_seconds') or 0. for d in datasets),
        'reference_seconds': sum(d.get('reference_seconds') or 0. for d in datasets),
        'failed_fixture_or_reference_seconds': sum((d.get('preparation_seconds') or 0.) +
            (d.get('reference_seconds') or 0.) for d in datasets if d.get('failure')),
        'successful_reference_task_observations': sum(d.get('count', 0) for d in datasets if not d.get('failure')),
        'selection_replay': {method: {'evaluations': value['evaluations'],
            'virtual_callback_seconds': value['virtual_callback_seconds'],
            'replay_wall_seconds': value['replay_wall_seconds'],
            'unexecuted_training_slots_requested': value.get('unexecuted_training_slots_requested', 0)}
            for method, value in report.get('selections', {}).items()},
        'manual_selection_virtual_callback_seconds': report.get('manual_proxy_selection', {}).get('virtual_callback_seconds'),
        'matrix_elapsed_seconds': report.get('elapsed_seconds'),
        'wrapper_elapsed_seconds': report.get('constraint_elapsed_seconds'),
        'scope': 'Actual callback costs retain failures; unexecuted fixture-failure slots have null callback cost. Shared fixtures/references are charged once. Recorded virtual selector costs overlap the measured training matrix and are not added to actual cost. Matrix/wrapper wall includes remaining orchestration and checkpoint overhead.'}


def run(name, output, affinity, *, baseline):
    output = Path(output)
    if output.exists():
        raise FileExistsError('output already exists; preserve evidence and choose a new path')
    if (name, affinity) not in TRAINING_SEEDS or type(affinity) is not int:
        raise ValueError('only images/geospatial at affinity 1 or 2 are predeclared')
    if not callable(getattr(os, 'sched_getaffinity', None)) or not callable(getattr(os, 'sched_setaffinity', None)):
        raise RuntimeError('this same-host constraint experiment requires Linux CPU-affinity APIs')
    started = time.perf_counter()
    original = set(os.sched_getaffinity(0))
    if len(original) < affinity:
        raise ValueError('requested real affinity exceeds the available CPU mask')
    narrowed = set(sorted(original)[:affinity])
    primary = read_baseline(baseline, name)
    sources = source_hashes()
    declaration = protocol(name, affinity, sources=sources, baseline=primary,
        available_cpu_ids=original, constrained_cpu_ids=narrowed)
    report = {'schema_version': 1, 'status': 'preflight', 'protocol': declaration,
        'protocol_sha256': optimizer.digest(declaration), 'failures': [], 'selections': {},
        **{phase: [] for phase in PHASES}, 'datasets': []}
    output.parent.mkdir(parents=True, exist_ok=True)
    optimizer.write(output, report)
    reuse = None
    try:
        os.sched_setaffinity(0, narrowed)
        if set(os.sched_getaffinity(0)) != narrowed:
            raise RuntimeError('operating system did not establish the exact declared affinity mask')
        reuse = check_profile_reuse(primary, affinity)
        report['primary_profile_reuse_check'] = reuse
        optimizer.write(output, report)
        report = optimizer.run(name, output, affinity,
            training_seed=TRAINING_SEEDS[(name, affinity)], heldout_seeds=HELDOUT_SEEDS[(name, affinity)],
            include_baselines=True, experiment_metadata=declaration['experiment'], record_fixture_failures=True)
        # Annotation is post-hoc only; the frozen selector and protocol are untouched.
        if report['protocol_sha256'] != optimizer.digest(declaration):
            raise RuntimeError('executed protocol does not match its predeclared constraint experiment')
        if optimizer.digest(report['selections']) != report['selection_frozen_sha256']:
            raise RuntimeError('returned selections do not match their frozen hash')
        report['matrix_status'], report['matrix_accepted'] = report['status'], report['accepted']
        report['primary_profile_reuse_check'] = reuse
        report['constraint_source_sha256'] = sources
        report['source_unchanged_at_end'] = source_hashes() == sources
        report['affinity_matches_at_end'] = set(os.sched_getaffinity(0)) == narrowed
        report['environment']['source_sha256'][TOOL_PATH] = sources[TOOL_PATH]
        report['environment']['command'] = shlex.join([sys.executable, '-m', 'tools.probe_affinity_constraints',
            '--workload', name, '--output', str(output), '--affinity', str(affinity), '--baseline', str(baseline)])
        if not report['source_unchanged_at_end']:
            report['failures'].append('source snapshot changed during the constraint experiment')
        if not report['affinity_matches_at_end']:
            report['failures'].append('CPU affinity at completion does not match the declared mask')
        if not reuse['passed']:
            report['failures'].append('primary profile compatibility check did not establish expected hardware rejection')
        report['constraint_elapsed_seconds'] = time.perf_counter() - started
        report['constraint_accounting'] = accounting(report)
        report['accepted'] = not report['failures']
        optimizer.write(output, report)
        return report
    except BaseException as exc:
        if output.exists():
            report = json.loads(output.read_text())
        report.update(status='interrupted' if isinstance(exc, (KeyboardInterrupt, SystemExit)) else 'failed', accepted=False)
        report.setdefault('failures', []).append(type(exc).__name__ + ': ' + str(exc))
        report['primary_profile_reuse_check'] = reuse
        report['constraint_elapsed_seconds'] = time.perf_counter() - started
        report['constraint_accounting'] = accounting(report)
        optimizer.write(output, report)
        raise
    finally:
        os.sched_setaffinity(0, original)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workload', choices=NAMES, required=True)
    parser.add_argument('--affinity', type=int, choices=CAPS, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--baseline', type=Path, required=True)
    args = parser.parse_args(argv)
    if (args.output.parent / '.stop-before-next-workload').exists():
        return 75
    run(args.workload, args.output, args.affinity, baseline=args.baseline)
    # Completed rejected evidence remains useful and must not suppress other runs.
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
