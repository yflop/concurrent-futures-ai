"""Separately versioned post-failure media envelopes, never replacement evidence.

Video measures the complete 96-row training and 72-row heldout intelligent grid,
then three frozen-choice trials in fresh interpreters. The 72-call primary search
is a retrospective training-matrix replay; its cost is not live-search wall time.
OCR retains the original independent standard-library and direct baselines.
Use --plan to inspect the declaration without creating fixtures or running work.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import time

from intelligent_futures.tuning import Configuration
from benchmarks.real_world import execute_batch, module_for
from benchmarks.real_world import optimizer
from benchmarks.real_world.optimizer_metrics import GateBudgets, gate_failures
from benchmarks.real_world.run import trial, workload_environment


ROOT = Path(__file__).resolve().parents[1]
EVALUATION_RSS_BYTES = 1024**3
VIDEO_TRAINING_RSS_BYTES = 715827882  # floor(1 GiB / 1.5), an experimental filter.
FRESH_TIMEOUT_SECONDS = 900
ENVELOPES = {
    'video': {'version': 'video-streaming-reserve-v1', 'training_seed': 20261013,
              'heldout_seeds': (20262011, 20262017, 20262023),
              'training_max_rss_bytes': VIDEO_TRAINING_RSS_BYTES,
              'include_baselines': False, 'fresh_selected': True,
              'adapter': 'bounded streaming decoded video frames; unchanged exact frame truth'},
    'ocr': {'version': 'ocr-bold42-v1', 'training_seed': 20271013,
            'heldout_seeds': (20272011, 20272017, 20272023),
            'training_max_rss_bytes': EVALUATION_RSS_BYTES,
            'include_baselines': True, 'fresh_selected': False,
            'adapter': 'candidate Bold42 generated typography revision; exact text truth unchanged'},
}


def source_hashes():
    files = sorted([*(ROOT / 'src' / 'intelligent_futures').rglob('*.py'),
                    *(ROOT / 'benchmarks' / 'real_world').rglob('*.py'),
                    Path(__file__).resolve()])
    return {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in files}


def envelope_options(name, *, fresh_selected=None):
    if name not in ENVELOPES:
        raise ValueError('workload must be video or ocr')
    options = dict(ENVELOPES[name])
    if fresh_selected is not None:
        if type(fresh_selected) is not bool:
            raise ValueError('fresh_selected must be bool or None')
        options['fresh_selected'] = fresh_selected
    return options


def ocr_font_identity():
    """Resolve and hash the actual font used by the revised generator."""
    from PIL import ImageFont
    from benchmarks.real_world import media
    font = ImageFont.truetype(media.OCR_FONT_NAME, 42)
    path = Path(font.path)
    return {'font_name': media.OCR_FONT_NAME, 'font_size': 42,
            'font_file_name': path.name, 'font_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'fixture_revision': media.OCR_FIXTURE_REVISION}


def ocr_traineddata_identity():
    """Resolve Tesseract's actual English data directory, retaining no host path."""
    output = subprocess.check_output(['tesseract', '--list-langs'],
        stderr=subprocess.STDOUT, text=True, timeout=10)
    match = re.search(r'List of available languages in "([^"]+)"', output)
    if match is None or 'eng' not in output.splitlines():
        raise ValueError('cannot resolve the installed Tesseract English traineddata')
    path = Path(match.group(1)) / 'eng.traineddata'
    return {'language': 'eng', 'file_name': path.name,
            'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def protocol(name, affinity=8, *, fresh_selected=None, sources=None):
    """Declare seeds, budget, source identity, and failure accounting before work."""
    if type(affinity) is not int or affinity < 8:
        raise ValueError('this 24-candidate envelope requires affinity of at least 8 CPUs')
    options = envelope_options(name, fresh_selected=fresh_selected)
    sources = source_hashes() if sources is None else sources
    metadata = {
        'envelope_version': options['version'], 'required_adapter': options['adapter'],
        'scope': 'Separate post-primary development experiment; the original 11-stratum evidence universe and every recorded failure remain unchanged. Acceptance definitions stay with the original reports and analysis.',
        'original_primary_strata': 11,
        'replaces_primary_evidence': False,
        'source_snapshot_sha256': optimizer.digest(sources),
        'runner_source_sha256': sources['tools/probe_media_envelope.py'],
        'calibration_memory_reserve': {
            'training_filter_bytes': options['training_max_rss_bytes'],
            'evaluation_gate_bytes': EVALUATION_RSS_BYTES,
            'factor': 1.5 if name == 'video' else 1.0,
            'status': 'Experimental post-failure choice; no reservation, enforcement, safety guarantee, or validated universal factor.',
        },
        'fresh_context': {
            'enabled': options['fresh_selected'],
            'planned_trials': len(options['heldout_seeds']) if options['fresh_selected'] else 0,
            'selection': 'Primary budgeted choice frozen before any heldout fixture is prepared; no heldout reselection.',
            'fixture_scope': 'Regenerate the same declared seed/profile and independent reference in each fresh interpreter; require exact reference digest match.',
            'retention_scope': 'Fresh selected context versus reused-parent selected rows; descriptive, order-confounded retention comparison, not causal isolation.',
            'timeout_seconds': FRESH_TIMEOUT_SECONDS,
        },
        'resource_attribution': '10 ms process-tree sampling; start/end and parent/descendant RSS at the observed tree peak use non-atomic sweeps and may miss spikes or double-count shared pages.',
    }
    if name == 'ocr':
        metadata['fixture_font'] = ocr_font_identity()
        metadata['traineddata'] = ocr_traineddata_identity()
    return optimizer.plan(name, affinity, training_seed=options['training_seed'],
        heldout_seeds=options['heldout_seeds'], training_max_rss_bytes=options['training_max_rss_bytes'],
        include_baselines=options['include_baselines'], experiment_metadata=metadata,
        record_fixture_failures=True)


def _failure(config, seed, reason, *, executed=False):
    return {'engine': 'intelligent', 'configuration': config, 'seed': seed,
            'failure': reason, 'trial_executed': executed,
            'measured_callback_seconds': None}


def fresh_trial(request):
    """One new-interpreter trial; never runs an optimizer or accesses heldout peers."""
    name, seed = request['workload'], request['seed']
    options = envelope_options(name)
    config = Configuration(**request['configuration'])
    if config not in optimizer.CANDIDATES or seed not in options['heldout_seeds']:
        raise ValueError('fresh request is outside the declared configuration/seed universe')
    if type(request['affinity']) is not int or request['affinity'] < 8:
        raise ValueError('invalid fresh-context CPU affinity')
    available = sorted(os.sched_getaffinity(0))
    if len(available) < request['affinity']:
        raise ValueError('fresh process has fewer CPUs than the declared affinity')
    os.sched_setaffinity(0, available[:request['affinity']])
    initial_sources = source_hashes()
    if initial_sources != request['source_sha256']:
        raise ValueError('source snapshot changed before fresh-context execution')
    if name == 'ocr' and ocr_font_identity() != request['fixture_font']:
        raise ValueError('actual OCR font changed before fresh-context execution')
    if name == 'ocr' and ocr_traineddata_identity() != request['traineddata']:
        raise ValueError('actual OCR traineddata changed before fresh-context execution')
    environment = workload_environment()
    dataset = {'seed': seed, 'count': optimizer.COUNT}
    row = None
    started = time.perf_counter()
    module = module_for(name)
    with tempfile.TemporaryDirectory(prefix='media-fresh-' + name + '-') as directory:
        root = Path(directory)
        stage = 'fixture_preparation'
        t = time.perf_counter()
        try:
            tasks = module.prepare(name, root / 'dataset', optimizer.COUNT, seed, scale='representative')
            dataset['preparation_seconds'] = time.perf_counter() - t
            stage = 'reference_validation'
            t = time.perf_counter()
            reference_rows = execute_batch((name, tasks))
            dataset['reference_seconds'] = time.perf_counter() - t
            if len(reference_rows) != optimizer.COUNT or any(r['error'] for r in reference_rows):
                raise RuntimeError('fresh-context exact-truth reference failed')
            reference = optimizer.digest([r['value'] for r in reference_rows])
            dataset['reference_sha256'] = reference
            if reference != request['reference_sha256']:
                raise RuntimeError('fresh and reused-parent reference digests differ')
            stage = 'trial'
            prepared = dict(dataset, tasks=tasks)
            t = time.perf_counter()
            row = trial(name, 'intelligent', config, optimizer.COUNT, seed,
                        root / 'trial', prepared=prepared)
            row['trial_executed'] = True
            row['measured_callback_seconds'] = time.perf_counter() - t
        except Exception as exc:
            duration = time.perf_counter() - t
            if stage == 'fixture_preparation':
                dataset['preparation_seconds'] = duration
            elif stage == 'reference_validation':
                dataset.setdefault('reference_seconds', duration)
            row = _failure(asdict(config), seed, type(exc).__name__ + ': ' + str(exc)[:4000],
                           executed=stage == 'trial')
            row['failure_stage'] = stage
            if stage == 'trial':
                row['measured_callback_seconds'] = duration
    for phase in ('cold', 'warm'):
        if phase in row:
            row[phase].pop('completion_trace', None)
    row.pop('profiles_after_two_batches', None)
    row['dataset'] = dataset
    row['fresh_context'] = {'fresh_interpreter': True,
        'fixture_and_reference_regenerated': True, 'elapsed_seconds': time.perf_counter() - started,
        'protocol_sha256': request['protocol_sha256'],
        'selection_frozen_sha256': request['selection_frozen_sha256'],
        'source_sha256': initial_sources, 'environment': environment}
    row['reference_matches_reused_parent'] = dataset.get('reference_sha256') == request['reference_sha256']
    if source_hashes() != initial_sources:
        row['failure'] = 'source snapshot changed during fresh-context execution'
    return row


def _terminate(process):
    """Bound timeout cleanup to this explicitly created subprocess group."""
    if os.name == 'posix':
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    else:
        process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        if os.name == 'posix':
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        else:
            process.kill()
        process.wait()


def run_fresh_process(request):
    with tempfile.TemporaryDirectory(prefix='media-fresh-request-') as directory:
        root = Path(directory)
        request_path, output_path, log_path = root / 'request.json', root / 'result.json', root / 'run.log'
        optimizer.write(request_path, request)
        command = [sys.executable, '-m', 'tools.probe_media_envelope',
                   '--fresh-request', str(request_path), '--output', str(output_path)]
        started = time.perf_counter()
        with log_path.open('wb') as log:
            process = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=os.name == 'posix')
            try:
                process.wait(timeout=FRESH_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                _terminate(process)
                row = _failure(request['configuration'], request['seed'],
                               'fresh interpreter timed out; trial execution status is unknown')
                row['trial_executed'] = None
            else:
                try:
                    row = json.loads(output_path.read_text())
                    if process.returncode or row['configuration'] != request['configuration'] or row['seed'] != request['seed']:
                        raise ValueError('child failed or returned a different configuration/seed')
                    context = row['fresh_context']
                    if (context['source_sha256'] != request['source_sha256']
                            or context['protocol_sha256'] != request['protocol_sha256']
                            or context['selection_frozen_sha256'] != request['selection_frozen_sha256']):
                        raise ValueError('child source/protocol/frozen-selection identity mismatch')
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    row = _failure(request['configuration'], request['seed'],
                                   'fresh subprocess result unavailable: ' + type(exc).__name__)
                    row['trial_executed'] = None
        row['subprocess_seconds'] = time.perf_counter() - started
        row['subprocess_returncode'] = process.returncode
        if row.get('failure'):
            with log_path.open('rb') as log:
                log.seek(max(0, log_path.stat().st_size - 4000))
                row['subprocess_log_tail'] = log.read().decode(errors='replace')
        return row


def retention_comparisons(report, fresh_rows):
    selected = report['selections']['budgeted']['selected']
    gates = GateBudgets(**report['protocol']['gates'])
    comparisons = []
    for seed in report['protocol']['heldout_seeds']:
        reused = [r for r in report['heldout'] if r['configuration'] == selected and r['seed'] == seed]
        fresh = [r for r in fresh_rows if r['seed'] == seed]
        left, right = (reused[0] if len(reused) == 1 else {}), (fresh[0] if len(fresh) == 1 else {})
        left_peak, right_peak = left.get('sampled_tree_peak_rss_bytes'), right.get('sampled_tree_peak_rss_bytes')
        valid_peaks = all(type(value) is int and value >= 0 for value in (left_peak, right_peak))
        comparisons.append({'seed': seed, 'configuration': selected,
            'reused_parent_gates': gate_failures(left, gates), 'fresh_parent_gates': gate_failures(right, gates),
            'reference_matches': right.get('reference_matches_reused_parent') is True,
            'reused_parent_rss_collection': left.get('rss_collection'),
            'fresh_parent_rss_collection': right.get('rss_collection'),
            'reused_tree_peak_rss_bytes': left_peak, 'fresh_tree_peak_rss_bytes': right_peak,
            'reused_minus_fresh_tree_peak_bytes': left_peak - right_peak if valid_peaks else None})
    return {'scope': 'Same frozen choice and seeded exact-reference workload, different parent histories; descriptive and order-confounded, not a causal memory-leak or retention estimate.',
            'expected_comparisons': len(report['protocol']['heldout_seeds']), 'comparisons': comparisons}


def run(name, output, affinity=8, *, fresh_selected=None):
    output = Path(output)
    if output.exists():
        raise ValueError('output already exists; preserve prior evidence and choose a new path')
    sources = source_hashes()
    declaration = protocol(name, affinity, fresh_selected=fresh_selected, sources=sources)
    options = envelope_options(name, fresh_selected=fresh_selected)
    report = optimizer.run(name, output, affinity, training_seed=options['training_seed'],
        heldout_seeds=options['heldout_seeds'], training_max_rss_bytes=options['training_max_rss_bytes'],
        include_baselines=options['include_baselines'], experiment_metadata=declaration['experiment'],
        record_fixture_failures=True)
    if report['protocol_sha256'] != optimizer.digest(declaration):
        raise RuntimeError('executed protocol does not match the predeclared envelope')
    report['envelope_source_sha256'] = sources
    report['matrix_status'], report['matrix_accepted'] = report['status'], report['accepted']
    report['matrix_observed_trials'] = report['observed_trials']
    report['fresh_selected'] = []
    report['status'] = 'running_fresh_context' if options['fresh_selected'] else 'complete'
    report['environment']['command'] = shlex.join([sys.executable, '-m', 'tools.probe_media_envelope',
        '--workload', name, '--output', str(output), '--affinity', str(affinity),
        '--fresh-selected' if options['fresh_selected'] else '--no-fresh-selected'])
    selected = report['selections']['budgeted']['selected']
    frozen_hash = report['selection_frozen_sha256']
    source_unchanged = source_hashes() == sources
    if not source_unchanged:
        report['failures'].append('source snapshot changed during matrix execution')
    optimizer.write(output, report)
    if options['fresh_selected']:
        for seed in options['heldout_seeds']:
            data = next((d for d in report['datasets'] if d['seed'] == seed), {})
            if selected is None or not data.get('reference_sha256') or not source_unchanged:
                row = _failure(selected, seed, 'fresh selected trial not executed: no selection, exact reference, or unchanged source snapshot')
            else:
                request = {'workload': name, 'configuration': selected, 'seed': seed,
                    'affinity': affinity, 'reference_sha256': data['reference_sha256'],
                    'source_sha256': sources, 'protocol_sha256': report['protocol_sha256'],
                    'selection_frozen_sha256': frozen_hash}
                if name == 'ocr':
                    request['fixture_font'] = declaration['experiment']['fixture_font']
                    request['traineddata'] = declaration['experiment']['traineddata']
                try:
                    row = run_fresh_process(request)
                except Exception as exc:
                    row = _failure(selected, seed, 'fresh process orchestration failed: ' + type(exc).__name__)
                    row['trial_executed'] = None
            report['fresh_selected'].append(row)
            optimizer.write(output, report)
        report['retention_comparison'] = retention_comparisons(report, report['fresh_selected'])
        for comparison in report['retention_comparison']['comparisons']:
            if comparison['fresh_parent_gates'] or not comparison['reference_matches']:
                report['failures'].append(f"fresh selected seed={comparison['seed']}: resource/correctness/reference check failed")
    if optimizer.digest(report['selections']) != frozen_hash:
        raise RuntimeError('frozen selection changed during fresh-context observations')
    report['source_unchanged_at_end'] = source_hashes() == sources
    if not report['source_unchanged_at_end']:
        report['failures'].append('source snapshot changed before envelope completion')
    report['fixture_dependencies_unchanged_at_end'] = True
    if name == 'ocr':
        report['fixture_dependencies_unchanged_at_end'] = (
            ocr_font_identity() == declaration['experiment']['fixture_font']
            and ocr_traineddata_identity() == declaration['experiment']['traineddata'])
        if not report['fixture_dependencies_unchanged_at_end']:
            report['failures'].append('actual OCR font or traineddata changed during the envelope')
    report['expected_trial_slots'] = (273 if options['include_baselines'] else 168) + (3 if options['fresh_selected'] else 0)
    report['scheduled_trial_slots'] += len(report['fresh_selected'])
    report['observed_trials'] += sum(r.get('trial_executed') is True for r in report['fresh_selected'])
    report['unknown_trial_execution_slots'] = sum(r.get('trial_executed') is None for r in report['fresh_selected'])
    report['executed_task_observations'] += sum(r.get(p, {}).get('tasks', 0) for r in report['fresh_selected'] for p in ('cold', 'warm'))
    report['status'], report['accepted'] = 'complete', not report['failures']
    optimizer.write(output, report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workload', choices=tuple(ENVELOPES))
    parser.add_argument('--output', type=Path)
    parser.add_argument('--affinity', type=int, default=8)
    parser.add_argument('--fresh-selected', action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument('--plan', action='store_true')
    parser.add_argument('--fresh-request', type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.fresh_request:
        if args.output is None or args.output.exists():
            parser.error('fresh output must be a new file')
        optimizer.write(args.output, fresh_trial(json.loads(args.fresh_request.read_text())))
        return 0
    if args.workload is None:
        parser.error('--workload is required')
    if args.plan:
        declaration = protocol(args.workload, args.affinity, fresh_selected=args.fresh_selected)
        print(json.dumps({'protocol': declaration, 'protocol_sha256': optimizer.digest(declaration)}, indent=2))
        return 0
    if args.output is None:
        parser.error('--output is required unless --plan is used')
    if (args.output.parent / '.stop-before-next-workload').exists():
        return 75
    existed = args.output.exists()
    try:
        run(args.workload, args.output, args.affinity, fresh_selected=args.fresh_selected)
    except Exception as exc:
        if not existed and args.output.exists():
            report = json.loads(args.output.read_text())
            report.update(status='failed', accepted=False)
            report.setdefault('failures', []).append(type(exc).__name__ + ': ' + str(exc)[:4000])
            optimizer.write(args.output, report)
        raise
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
