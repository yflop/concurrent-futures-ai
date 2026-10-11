"""Fixed-preconditioned heterogeneous execution against tuned stdlib hybrids.

This is a separate experiment, not a new evaluation adapter or a deployable
AdoptionRecommendation. All work is generated, repeatable, and loopback-only.
Run only with explicit --allow-repeatable --allow-process and no competing soak.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import (CancelledError, FIRST_COMPLETED, ProcessPoolExecutor,
                                ThreadPoolExecutor, wait)
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import random
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time

from intelligent_futures import IntelligentExecutor, TaskHints
from . import data, network
from .resource_sampling import RSSSampler
from .run import cpu_seconds, workload_environment

REVISION = 'sustained-fixed-preconditioned-v1'
LOGICAL_WINDOW = 32
TRAINING_SEEDS = (602201, 602202, 602203)
DEVELOPMENT_SEEDS = (601101, 601102, 601103, 601104)
ALPHA = .05 / 4
MARGIN = .05
WARMUP_ITEMS = 32
MEASURED_ITEMS = 256


@dataclass(frozen=True, order=True)
class Configuration:
    engine: str
    route: str
    threads: int
    processes: int
    batch: int

    def __post_init__(self):
        if self.engine not in ('stdlib', 'intelligent'):
            raise ValueError('invalid engine')
        if self.route not in ('thread', 'process', 'static', 'overflow', 'adaptive'):
            raise ValueError('invalid route')
        if any(type(n) is not int or n < 0 for n in (self.threads, self.processes)):
            raise ValueError('invalid worker count')
        if not 1 <= self.threads + self.processes <= 8 or self.batch not in (1, 4):
            raise ValueError('worker/batch budget')
        if self.engine == 'intelligent' and self.route != 'adaptive':
            raise ValueError('only the predeclared adaptive candidate is supported')
        if self.engine == 'stdlib' and self.route == 'adaptive':
            raise ValueError('adaptive is not a stdlib comparator')
        if self.route == 'thread' and (not self.threads or self.processes):
            raise ValueError('thread mode shape')
        if self.route == 'process' and (self.threads or not self.processes):
            raise ValueError('process mode shape')
        if self.route in ('static', 'overflow', 'adaptive') and not (self.threads and self.processes):
            raise ValueError('hybrid mode shape')

    @property
    def key(self):
        return ':'.join(map(str, (self.engine, self.route, self.threads, self.processes, self.batch)))


def grids():
    single = tuple(Configuration('stdlib', route, n if route == 'thread' else 0,
                                 n if route == 'process' else 0, b)
                   for route in ('thread', 'process') for n in (4, 8) for b in (1, 4))
    splits = ((2, 2), (4, 4), (2, 6), (6, 2))
    def hybrid(engine, route):
        return tuple(Configuration(engine, route, t, p, b) for t, p in splits for b in (1, 4))
    return {'single': single, 'static': hybrid('stdlib', 'static'),
            'overflow': hybrid('stdlib', 'overflow'),
            'intelligent': hybrid('intelligent', 'adaptive')}


def seed_manifest():
    rng = random.Random(603301)
    heldout = []
    while len(heldout) < 24:
        seed = rng.randrange(2**63)
        if seed not in (*TRAINING_SEEDS, *DEVELOPMENT_SEEDS, *heldout):
            heldout.append(seed)
    return {'development': list(DEVELOPMENT_SEEDS), 'training': list(TRAINING_SEEDS),
            'heldout': heldout, 'order_seed': 603302}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def percentile(values, probability):
    """Nearest-rank empirical quantile, with one entry per logical job."""
    if not values:
        raise ValueError('empty quantile')
    return sorted(values)[max(0, math.ceil(probability * len(values)) - 1)]


def _write(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')
    temporary.replace(path)


def source_identity():
    root = Path(__file__).resolve().parents[2]
    import intelligent_futures.executor as runtime
    files = sorted((root / 'src' / 'intelligent_futures').rglob('*.py'))
    files += sorted((root / 'benchmarks' / 'real_world').glob('*.py'))
    return {'runtime_import_path': str(Path(runtime.__file__).resolve()),
            'start_method': 'spawn', 'harness_revision': REVISION,
            'imported_runtime_sha256': {str(p.relative_to(Path(runtime.__file__).parent)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(Path(runtime.__file__).parent.rglob('*')) if p.is_file() and '__pycache__' not in p.parts},
            'source_sha256': {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                              for p in files}}


def _server_main(root, pipe):
    """Separate interpreter/GIL; aggregate resource usage remains in the tree."""
    try:
        with network.fixture(Path(root)) as server:
            lock = threading.Lock()
            active = 0
            peak = 0
            original = server.process_request_thread
            def tracked(*args):
                nonlocal active, peak
                with lock:
                    active += 1
                    peak = max(peak, active)
                try:
                    return original(*args)
                finally:
                    with lock:
                        active -= 1
            server.process_request_thread = tracked
            pipe.send({'ready': True, 'pid': os.getpid(),
                       'affinity': sorted(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else None})
            while True:
                command = pipe.recv()
                if command == 'stop':
                    break
                with lock:
                    observed_peak, observed_active = peak, active
                    if command == 'begin':
                        peak = active
                with server.counter_lock:
                    requests = sum(server.requests.values())
                pipe.send({'cpu_seconds': cpu_seconds(), 'requests': requests,
                           'active_handlers': observed_active, 'peak_handlers': observed_peak})
    except BaseException as exc:
        try:
            pipe.send({'server_error': type(exc).__name__})
        except (BrokenPipeError, EOFError, OSError):
            pass
        raise
    finally:
        pipe.close()


@contextmanager
def server_process(root):
    parent, child = multiprocessing.get_context('spawn').Pipe()
    process = multiprocessing.get_context('spawn').Process(target=_server_main, args=(str(root), child))
    process.start()
    child.close()
    def rpc(command):
        parent.send(command)
        if not parent.poll(15):
            raise TimeoutError('loopback server control deadline')
        result = parent.recv()
        if 'server_error' in result:
            raise RuntimeError('loopback server failed')
        return result
    try:
        if not parent.poll(15):
            raise TimeoutError('loopback server startup deadline')
        info = parent.recv()
        if not info.get('ready'):
            raise RuntimeError('loopback server startup failed')
        yield info, rpc
    finally:
        if process.is_alive():
            try:
                parent.send('stop')
            except (BrokenPipeError, EOFError, OSError):
                pass
        process.join(10)
        if process.is_alive():
            process.terminate()
            process.join(5)
            if process.is_alive():
                process.kill()
                process.join()
            raise RuntimeError('loopback server required forced cleanup')
        parent.close()
        if process.exitcode:
            raise RuntimeError('loopback server exited unsuccessfully')


def _jobs(root, count, seed, prefix):
    """Independent local recipe inputs, each original recipe call <=64 items."""
    result = {'geospatial': [], 'http': []}
    for family, module in (('geospatial', data), ('http', network)):
        for chunk in range((count + 63) // 64):
            size = min(64, count - 64 * chunk)
            tasks = module.prepare(family, root / (prefix + '-' + family + '-' + str(chunk)),
                                   size, seed + chunk * 101, scale='representative')
            for index, task in enumerate(tasks):
                key = (f'graph-{len(task["origins"])}-{len(task["destinations"])}'
                       if family == 'geospatial' else f'http-{task["size"]}')
                result[family].append({'id': f'{prefix}-{family}-{chunk * 64 + index}',
                                       'family': family, 'key': key, 'task': task})
    return result


def arrival_stream(jobs, counts, seed):
    """Take each family's next jobs, key-sort within epoch, shuffle <=4-job packets."""
    rng = random.Random(seed)
    offsets = {'geospatial': 0, 'http': 0}
    output = []
    for graph_count, http_count in counts:
        packets = []
        for family, number in (('geospatial', graph_count), ('http', http_count)):
            start = offsets[family]
            subset = jobs[family][start:start + number]
            if len(subset) != number:
                raise ValueError('arrival schedule exceeds prepared jobs')
            offsets[family] += number
            subset = sorted(subset, key=lambda job: (job['key'], job['id']))
            packets.extend(group_batches(subset, 4))
        rng.shuffle(packets)
        output.extend(job for packet in packets for job in packet)
    if offsets != {family: len(values) for family, values in jobs.items()}:
        raise ValueError('arrival schedule omitted prepared jobs')
    return tuple(output)


def group_batches(jobs, size):
    result = []
    for job in jobs:
        if (not result or len(result[-1]) == size
                or (result[-1][0]['family'], result[-1][0]['key']) != (job['family'], job['key'])):
            result.append([])
        result[-1].append(job)
    return tuple(tuple(batch) for batch in result)


def fixture_digest(jobs):
    """Bind actual generated input bytes/metadata without paths or server ports."""
    graph_files = {}
    manifest = []
    for job in jobs:
        task = job['task']
        stable = {key: value for key, value in task.items() if key not in ('path', 'url', 'work')}
        if job['family'] == 'geospatial':
            path = task['path']
            if path not in graph_files:
                graph_files[path] = hashlib.sha256(Path(path).read_bytes()).hexdigest()
            stable['graph_content_sha256'] = graph_files[path]
        else:
            # The prepare() manifest holds the SHA256 of the exact response bytes.
            from urllib.parse import urlsplit
            stable['request_query'] = urlsplit(task['url']).query
        manifest.append({'id': job['id'], 'family': job['family'], 'key': job['key'], 'input': stable})
    return digest(manifest)


def execute_batch(jobs):
    """Identical spawn-picklable kernel; timing metadata never enters the oracle."""
    started = time.perf_counter()
    values = tuple((job['id'], (data if job['family'] == 'geospatial' else network).run_task(job['task']))
                   for job in jobs)
    return {'values': values, 'pid': os.getpid(), 'thread_id': threading.get_ident(),
            'service_seconds': time.perf_counter() - started}


class ManualPool:
    """Straight stdlib hybrid, with accepted-not-terminal counters under a lock.

    Overflow is expressed in BATCH slots, just as the candidate scheduler:
    Qp=max(0,Op-P); spill iff Qp>=P and Op/P-Ot/T >=1.1.
    Those are declared capacity proxies, not observations of OS worker activity.
    """
    def __init__(self, config):
        self.config = config
        self.lock = threading.RLock()
        self.outstanding = {'thread': 0, 'process': 0}
        self.submitted = {'thread': 0, 'process': 0}
        self.peak_outstanding = {'thread': 0, 'process': 0}
        self.pools = {}
        if config.threads:
            self.pools['thread'] = ThreadPoolExecutor(max_workers=config.threads)
        if config.processes:
            self.pools['process'] = ProcessPoolExecutor(max_workers=config.processes,
                mp_context=multiprocessing.get_context('spawn'))

    def submit(self, batch):
        c = self.config
        with self.lock:
            if c.route in ('thread', 'process'):
                backend = c.route
            else:
                backend = 'process' if batch[0]['family'] == 'geospatial' else 'thread'
                if backend == 'process' and c.route == 'overflow':
                    p, t = self.outstanding['process'], self.outstanding['thread']
                    if max(0, p - c.processes) >= c.processes and p / c.processes - t / c.threads >= 1.1:
                        backend = 'thread'
            self.outstanding[backend] += 1
            try:
                future = self.pools[backend].submit(execute_batch, batch)
            except BaseException:
                self.outstanding[backend] -= 1
                raise
            self.submitted[backend] += 1
            self.peak_outstanding[backend] = max(self.peak_outstanding[backend], self.outstanding[backend])
            def finished(_):
                with self.lock:
                    self.outstanding[backend] -= 1
            future.add_done_callback(finished)
            return future

    def snapshot(self):
        with self.lock:
            return {'outstanding': dict(self.outstanding), 'submitted': dict(self.submitted),
                    'peak_outstanding': dict(self.peak_outstanding)}

    def shutdown(self):
        try:
            if 'thread' in self.pools:
                self.pools['thread'].shutdown(wait=True, cancel_futures=True)
        finally:
            if 'process' in self.pools:
                self.pools['process'].shutdown(wait=True, cancel_futures=True)


def _make_pool(config):
    if config.engine == 'stdlib':
        return ManualPool(config)
    return IntelligentExecutor(max_workers=config.threads, process_workers=config.processes,
        max_pending=LOGICAL_WINDOW, policy='adaptive', predictive=False,
        mp_context=multiprocessing.get_context('spawn'))


def execute_stream(pool, config, jobs):
    batches = group_batches(jobs, config.batch)
    pending = {}
    values = {}
    trace = []
    next_batch = 0
    logical_pending = 0
    peak_logical = 0
    started = time.perf_counter()
    try:
        while next_batch < len(batches) or pending:
            while next_batch < len(batches) and logical_pending + len(batches[next_batch]) <= LOGICAL_WINDOW:
                batch = batches[next_batch]
                before = time.perf_counter()
                if config.engine == 'intelligent':
                    future = pool.submit_task(execute_batch, batch, hints=TaskHints(
                        kind='cpu' if batch[0]['family'] == 'geospatial' else 'io',
                        key=batch[0]['key'], process_safe=True))
                else:
                    future = pool.submit(batch)
                submitted = time.perf_counter()
                completed = []
                callback_done = threading.Event()
                def done(_, box=completed, event=callback_done):
                    box.append(time.perf_counter())
                    event.set()
                future.add_done_callback(done)
                pending[future] = (next_batch, batch, before, submitted, completed, callback_done)
                logical_pending += len(batch)
                peak_logical = max(peak_logical, logical_pending)
                if peak_logical > LOGICAL_WINDOW:
                    raise AssertionError('logical admission bound violated')
                next_batch += 1
            done_set, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in done_set:
                index, batch, before, submitted, completed, callback_done = pending.pop(future)
                result = future.result()
                if not callback_done.wait(10):
                    raise RuntimeError('completion timestamp callback missing')
                logical_pending -= len(batch)
                if tuple(row[0] for row in result['values']) != tuple(job['id'] for job in batch):
                    raise AssertionError('batch identity/order mismatch')
                values[index] = result['values']
                for job in batch:
                    trace.append({'id': job['id'], 'family': job['family'], 'key': job['key'],
                        'requested_seconds': before - started, 'submitted_seconds': submitted - started,
                        'completed_seconds': completed[0] - started,
                        'latency_seconds': completed[0] - before, 'admission_seconds': submitted - before,
                        'pid': result['pid'], 'thread_id': result['thread_id']})
        elapsed = max(1e-12, time.perf_counter() - started)
    finally:
        for future in pending:
            future.cancel()
    ordered = tuple(row for index in sorted(values) for row in values[index])
    if len(ordered) != len(jobs) or len({row[0] for row in ordered}) != len(jobs):
        raise AssertionError('missing/duplicate logical results')
    latency = {}
    for label in ('all', 'geospatial', 'http'):
        samples = [row['latency_seconds'] for row in trace if label == 'all' or row['family'] == label]
        latency[label] = {f'p{int(q * 100)}': percentile(samples, q) for q in (.50, .95, .99)}
    return {'seconds': elapsed, 'digest': digest(ordered), 'logical_items': len(ordered),
            'batches': len(batches), 'peak_logical_pending': peak_logical,
            'latency': latency, 'trace': trace,
            'workers_seen': sorted({(row['pid'], row['thread_id']) for row in trace})}


def exclusions(result):
    reasons = []
    if not result.get('correct'):
        reasons.append('incorrect_or_incomplete')
    if result.get('error'):
        reasons.append('task_or_harness_error')
    for field, cap, label in (('lifecycle_cpu_seconds', 120, 'cpu'),
                              ('peak_rss_bytes', 1024**3, 'rss'),
                              ('lifecycle_seconds', 60, 'elapsed')):
        value = result.get(field)
        if not _valid_number(value):
            reasons.append(label + '_unavailable')
        elif value > cap:
            reasons.append(label + '_cap')
    warm = result.get('warm')
    if warm:
        duration = warm.get('seconds')
        if not _valid_number(duration, positive=True):
            reasons.append('warm_timing_unavailable')
        elif duration < .001:
            reasons.append('timing_below_resolution_floor')
        p95s = [warm.get('latency', {}).get(family, {}).get('p95') for family in ('all', 'geospatial', 'http')]
        if not all(_valid_number(value, positive=True) for value in p95s):
            reasons.append('p95_unavailable')
        elif any(value > 5 for value in p95s):
            reasons.append('absolute_p95_cap')
    else:
        reasons.append('warm_measurement_unavailable')
    return reasons


def observe(config, seed, *, allow_repeatable=False, allow_process=False):
    """One fresh interpreter session is created by supervise_observation."""
    if not allow_repeatable or not allow_process:
        raise ValueError('explicit fixture repetition and process consent required')
    result = {'configuration': asdict(config), 'seed': seed, 'correct': False,
              'source_identity': source_identity(), 'pid': os.getpid(), 'started_unix': time.time(),
              'affinity': sorted(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else None,
              'host_loadavg_start': list(os.getloadavg()) if hasattr(os, 'getloadavg') else None}
    total_started = time.perf_counter()
    try:
        with tempfile.TemporaryDirectory(prefix='intelligent-sustained-') as directory:
            root = Path(directory)
            with server_process(root) as (server_info, rpc):
                result['server'] = server_info
                warmup = arrival_stream(_jobs(root, 16, seed, 'precondition'), ((16, 16),), seed ^ 0x11)
                measured = arrival_stream(_jobs(root, 128, seed ^ 0x772, 'measured'),
                    ((48, 16), (16, 48), (48, 16), (16, 48)), seed ^ 0x33)
                result['fixture_manifest_digest'] = fixture_digest((*warmup, *measured))
                oracle_started = time.perf_counter()
                expected_warmup = digest(tuple(row for job in warmup for row in execute_batch((job,))['values']))
                expected_measured = digest(tuple(row for job in measured for row in execute_batch((job,))['values']))
                result['oracle_seconds'] = time.perf_counter() - oracle_started
                before_cpu = cpu_seconds()
                server_before = rpc('begin')
                with RSSSampler() as rss:
                    lifecycle_started = time.perf_counter()
                    pool = _make_pool(config)
                    constructed = time.perf_counter()
                    try:
                        first = execute_stream(pool, config, warmup)
                        result['precondition'] = first
                        result['snapshot_after_precondition'] = pool.snapshot()
                        result['warm'] = execute_stream(pool, config, measured)
                    finally:
                        shutdown_started = time.perf_counter()
                        pool.shutdown()
                        shutdown_finished = time.perf_counter()
                    result['lifecycle_seconds'] = shutdown_finished - lifecycle_started
                    result['construction_seconds'] = constructed - lifecycle_started
                    result['shutdown_seconds'] = shutdown_finished - shutdown_started
                    result['final_snapshot'] = pool.snapshot()
                    result['correct'] = (first['digest'] == expected_warmup
                        and result['warm']['digest'] == expected_measured
                        and first['logical_items'] == WARMUP_ITEMS
                        and result['warm']['logical_items'] == MEASURED_ITEMS)
                    if config.engine == 'intelligent':
                        snapshot = result['final_snapshot']
                        physical = snapshot['physical']
                        result['correct'] &= (snapshot['closed'] and snapshot['pending'] == 0
                            and physical['retained'] == 0 and physical['peak_retained'] <= LOGICAL_WINDOW
                            and snapshot['counts']['submitted'] == first['batches'] + result['warm']['batches']
                            and snapshot['counts']['failed'] == snapshot['counts']['cancelled'] == 0
                            and snapshot['counts']['peak_pending'] <= LOGICAL_WINDOW
                            and snapshot['counts']['submitted'] == sum(snapshot['counts'][name] for name in ('completed', 'failed', 'cancelled'))
                            and snapshot['counts']['submitted'] == snapshot['counts']['thread_submitted'] + snapshot['counts']['process_submitted']
                            and all(value <= physical['dispatch_limits'][name]
                                    for name, value in physical['peak_in_flight'].items()))
                    else:
                        result['correct'] &= (not any(result['final_snapshot']['outstanding'].values())
                            and sum(result['final_snapshot']['submitted'].values()) == first['batches'] + result['warm']['batches'])
                after_cpu = cpu_seconds()
                server_after = rpc('snapshot')
                server_cpu = (None if server_before['cpu_seconds'] is None or server_after['cpu_seconds'] is None
                              else server_after['cpu_seconds'] - server_before['cpu_seconds'])
                result['lifecycle_cpu_seconds'] = (None if before_cpu is None or after_cpu is None or server_cpu is None
                                                   else after_cpu - before_cpu + server_cpu)
                result['server_lifecycle'] = {'cpu_seconds': server_cpu,
                    'requests': server_after['requests'] - server_before['requests'],
                    'peak_handlers': server_after['peak_handlers'], 'active_handlers_at_end': server_after['active_handlers']}
                result['correct'] &= result['server_lifecycle']['requests'] == (WARMUP_ITEMS + MEASURED_ITEMS) // 2
                result['peak_rss_bytes'] = rss.peak
                result['rss_collection'] = rss.report()
                result['new_workers_in_measured_stream'] = sorted(set(map(tuple, result['warm']['workers_seen']))
                    - set(map(tuple, first['workers_seen'])))
    except CancelledError:
        raise
    except Exception as exc:
        result['error'] = type(exc).__name__
        result['correct'] = False
    result['total_observation_seconds'] = time.perf_counter() - total_started
    result['ended_unix'] = time.time()
    result['host_loadavg_end'] = list(os.getloadavg()) if hasattr(os, 'getloadavg') else None
    result['exclusions'] = exclusions(result)
    return result


def _valid_number(value, *, positive=False):
    return type(value) in (int, float) and math.isfinite(value) and (value > 0 if positive else value >= 0)


def compare(role, baseline, candidate, expected_seeds):
    """Strict paired v2-style sign bound, with four predeclared comparisons."""
    output = {'role': role, 'comparison_alpha': ALPHA, 'pairs': len(candidate), 'passed': False}
    expected_seeds = list(expected_seeds)
    if (len(expected_seeds) != 24 or len(set(expected_seeds)) != 24
            or [row.get('seed') for row in baseline] != expected_seeds
            or [row.get('seed') for row in candidate] != expected_seeds
            or any(row.get('exclusions') != [] or row.get('correct') is not True or exclusions(row)
                   for row in (*baseline, *candidate))):
        return dict(output, reasons=['incomplete_or_failed_heldout'])
    b, c = [], []
    for rows, values in ((baseline, b), (candidate, c)):
        for row in rows:
            value = row.get('warm', {}).get('seconds')
            if not _valid_number(value, positive=True):
                return dict(output, reasons=['invalid_warm_timing'])
            values.append(value)
            for family in ('all', 'geospatial', 'http'):
                p95 = row.get('warm', {}).get('latency', {}).get(family, {}).get('p95')
                if not _valid_number(p95, positive=True):
                    return dict(output, reasons=['invalid_latency_measurement'])
    logs = [math.log(x) - math.log(y) for x, y in zip(b, c)]
    wins = sum(value > math.log1p(MARGIN) + 1e-12 for value in logs)
    p_value = sum(math.comb(24, j) for j in range(wins, 25)) / 2**24
    tail, k = 0, 0
    for j in range(24):
        tail += math.comb(24, j)
        if tail / 2**24 <= ALPHA:
            k = j + 1
        else:
            break
    lower = sorted(logs)[k - 1]
    median = statistics.median(logs)
    mad = statistics.median(abs(value - median) for value in logs)
    scale = max(*b, *c)
    denominator = math.fsum(v / scale for v in c)
    if not denominator:
        return dict(output, reasons=['numeric_overflow'])
    aggregate = math.fsum(v / scale for v in b) / denominator - 1
    if not math.isfinite(aggregate):
        return dict(output, reasons=['numeric_overflow'])
    try:
        worst = math.expm1(max(-value for value in logs))
        median_gain, lower_gain = math.expm1(median), math.expm1(lower)
    except OverflowError:
        return dict(output, reasons=['numeric_overflow'])
    reasons = []
    if not lower > math.log1p(MARGIN) + 1e-12 or p_value > ALPHA:
        reasons.append('practical_improvement_not_established')
    if min(*b, *c) < .001:
        reasons.append('timing_below_resolution_floor')
    if mad > .10:
        reasons.append('paired_noise_exceeds_threshold')
    if not aggregate > 0:
        reasons.append('aggregate_regression_or_tie')
    if worst > .25:
        reasons.append('observed_tail_regression')
    latency_ratios = {}
    for family in ('all', 'geospatial', 'http'):
        ratios = [y['warm']['latency'][family]['p95'] / x['warm']['latency'][family]['p95']
                  for x, y in zip(baseline, candidate)]
        latency_ratios[family] = max(ratios)
        if max(ratios) > 1.25:
            reasons.append(family + ':observed_p95_regression')
    savings = [x - y for x, y in zip(b, c)]
    return dict(output, wins_above_margin=wins, sign_p_value=p_value,
        lower_order_statistic=k, median_speedup=median_gain, lower_speedup=lower_gain,
        log_speedup_mad=mad, aggregate_speedup=aggregate, worst_pair_slowdown=worst,
        minimum_observed_saving_seconds=min(savings), mean_observed_saving_seconds=statistics.fmean(savings),
        maximum_p95_ratios=latency_ratios, reasons=reasons, passed=not reasons)


def _terminate_group(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
    # The direct child can exit before its grandchildren. Kill any owned group
    # still alive, then the supervisor's process-tree census checks survivors.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _supervise_observation(config, seed, directory, slot, *, affinity=8, timeout=90, ownership):
    import psutil
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f'{slot}.json'
    log = directory / f'{slot}.log'
    request = directory / f'{slot}-request.json'
    if any(path.exists() for path in (target, log, request)):
        raise ValueError('observation paths already exist; no overwrite or silent retry')
    _write(request, {'configuration': asdict(config), 'seed': seed, 'output': str(target.resolve()),
                     'allow_repeatable': True, 'allow_process': True, 'affinity': affinity})
    root = Path(__file__).resolve().parents[2]
    env = dict(os.environ, PYTHONPATH=str(root / 'src'), OMP_NUM_THREADS='1',
               OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1')
    command = [sys.executable, '-m', 'benchmarks.real_world.adoption_sustained',
               'worker', '--request', str(request.resolve())]
    started = time.perf_counter()
    reason = None
    peak = 0
    identities = {}
    with log.open('x') as stream:
        process = subprocess.Popen(command, cwd=root, env=env, stdout=stream,
            stderr=subprocess.STDOUT, start_new_session=True)
        ownership.append(process)
        _write(directory / f'{slot}-supervisor-state.json', {'status': 'launched', 'pid': process.pid,
            'started_unix': time.time(), 'configuration': asdict(config), 'seed': seed})
        try:
            while process.poll() is None:
                try:
                    parent = psutil.Process(process.pid)
                    tree = [parent, *parent.children(recursive=True)]
                    rss = 0
                    for item in tree:
                        try:
                            identities[item.pid] = item.create_time()
                            rss += item.memory_info().rss
                        except psutil.NoSuchProcess:
                            pass
                    peak = max(peak, rss)
                    if rss > 1536 * 1024**2:
                        reason = 'supervisor_emergency_rss_limit'
                except psutil.NoSuchProcess:
                    pass
                except Exception as exc:
                    reason = 'supervisor_sampling_error:' + type(exc).__name__
                if time.perf_counter() - started > timeout:
                    reason = 'supervisor_observation_timeout'
                if reason:
                    _terminate_group(process)
                    break
                time.sleep(.1)
        except BaseException as exc:
            _terminate_group(process)
            _write(directory / f'{slot}-supervisor-state.json', {'status': 'interrupted', 'pid': process.pid,
                'abort_class': type(exc).__name__, 'post_cleanup_census': 'not reached; parent records interrupted slot'})
            raise
    def owned_survivors():
        living = []
        for item in psutil.process_iter(['pid', 'create_time', 'status']):
            try:
                pid = item.pid
                exact = identities.get(pid) == item.info['create_time']
                # The newly created session/group is exclusive to this trial.
                # Scan the group too: a child can be orphaned between tree sweeps.
                in_group = os.getpgid(pid) == process.pid and os.getsid(pid) == process.pid
                if (exact or in_group) and item.info['status'] != psutil.STATUS_ZOMBIE:
                    identities[pid] = item.info['create_time']
                    living.append(pid)
            except (psutil.NoSuchProcess, ProcessLookupError):
                pass
        return living
    survivors = owned_survivors()
    grace_deadline = time.monotonic() + 2
    while survivors and time.monotonic() < grace_deadline:
        time.sleep(.05)
        survivors = owned_survivors()
    cleanup_forced = bool(survivors)
    if survivors:
        reason = reason or 'owned_process_cleanup_required'
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        cleanup_deadline = time.monotonic() + 5
        while survivors and time.monotonic() < cleanup_deadline:
            time.sleep(.05)
            survivors = owned_survivors()
        if survivors:
            reason = 'surviving_owned_processes_after_cleanup'
    try:
        result = json.loads(target.read_text()) if target.exists() else {}
    except (OSError, ValueError):
        result = {}
        reason = reason or 'invalid_child_report'
    result.setdefault('configuration', asdict(config))
    result.setdefault('seed', seed)
    if reason or process.returncode or not result:
        result['correct'] = False
        result.setdefault('exclusions', []).append(reason or 'child_exit_or_missing_report')
    if 'exclusions' not in result:
        result.update(correct=False, exclusions=['missing_child_report'])
    result['supervisor'] = {'returncode': process.returncode, 'stop_reason': reason,
        'elapsed_seconds': time.perf_counter() - started, 'sampled_peak_tree_rss_bytes': peak,
        'emergency_rss_bytes': 1536 * 1024**2, 'observation_timeout_seconds': timeout,
        'observed_process_identities': sorted(identities.items()), 'survivors': survivors,
        'cleanup_forced': cleanup_forced, 'post_cleanup_verified': not survivors}
    _write(directory / f'{slot}-supervisor.json', result)
    return result


def supervise_observation(config, seed, directory, slot, *, affinity=8, timeout=90):
    """Contain every exceptional exit after Popen, including disk/census failures."""
    ownership = []
    try:
        return _supervise_observation(config, seed, directory, slot, affinity=affinity,
                                      timeout=timeout, ownership=ownership)
    except BaseException as exc:
        cleanup = []
        for process in ownership:
            info = {'pid': process.pid, 'post_cleanup_verified': False}
            try:
                _terminate_group(process)
                # killpg(0) after reaping detects surviving owned group members.
                # Zombies may keep a group visible; distinguish them explicitly.
                import psutil
                deadline = time.monotonic() + 5
                while True:
                    live = []
                    for item in psutil.process_iter(['pid', 'status']):
                        try:
                            if os.getpgid(item.pid) == process.pid and os.getsid(item.pid) == process.pid and item.info['status'] != psutil.STATUS_ZOMBIE:
                                live.append(item.pid)
                        except (psutil.NoSuchProcess, ProcessLookupError):
                            pass
                    if not live or time.monotonic() >= deadline:
                        break
                    time.sleep(.05)
                info.update(post_cleanup_verified=not live, survivors=live)
            except BaseException as cleanup_error:
                info['cleanup_error'] = type(cleanup_error).__name__
            cleanup.append(info)
        evidence = {'status': 'exceptional_supervisor_exit', 'exception_class': type(exc).__name__,
                    'configuration': asdict(config), 'seed': seed, 'cleanup': cleanup}
        try:
            _write(Path(directory) / f'{slot}-exception.json', evidence)
        except BaseException:
            print(json.dumps(evidence), file=sys.stderr, flush=True)
        raise


def predeclared_plan():
    return {'revision': REVISION, 'seeds': seed_manifest(),
        'grids': {label: [asdict(c) for c in values] for label, values in grids().items()},
        'fixed': asdict(Configuration('stdlib', 'thread', 8, 0, 1)),
        'logical_window': LOGICAL_WINDOW, 'precondition_items': WARMUP_ITEMS,
        'measured_items': MEASURED_ITEMS, 'alpha': .05, 'comparison_alpha': ALPHA,
        'minimum_speedup': MARGIN, 'max_log_ratio_mad': .1, 'max_pair_slowdown': .25,
        'min_duration_seconds': .001, 'max_relative_p95': 1.25, 'max_absolute_p95_seconds': 5,
        'resource_caps': {'lifecycle_cpu_seconds': 120, 'sampled_tree_rss_bytes': 1024**3,
                          'lifecycle_seconds': 60, 'supervisor_observation_seconds': 90,
                          'supervisor_sampled_emergency_rss_bytes': 1536 * 1024**2},
        'scope': 'fixed-preconditioned persistent sessions, separate-process loopback HTTP server; executor worker budget excludes server/support threads; eight-CPU shared affinity',
        'arrival': 'four 64-job epochs with graph/http counts 48/16,16/48,48/16,16/48; each family next slice key/id sorted, split into at-most-four homogeneous packets, seeded packet shuffle per epoch; batch grouping never crosses family/key',
        'manual_overflow': 'batch-slot counts O accepted-not-terminal; Qp=max(0,Op-P); CPU spills iff Qp>=P and Op/P-Ot/T>=1.1; IO thread-only',
        'latency': 'nearest-rank p50,p95,p99, one observation per logical item, containing-batch submission request to done callback; saturated closed-loop, not end-user response latency',
        'cold_scope': 'construction + fixed precondition + measured warm stream + shutdown; not equal-sized cold/warm fixtures',
        'resource_scope': 'execution lifecycle includes client and waited-worker CPU plus concurrent separately sampled server CPU; tree RSS includes server; fixture and oracle setup excluded but included in discovery',
        'warmth': 'fixed precondition; actual worker identities and policy profiles observed, no guarantee every lazy worker starts or policy converges',
        'cache_scope': 'fresh application paths/server per observation; OS/page caches uncontrolled; oracle touches source inputs',
        'independence': 'fresh child interpreter and fixture/pools per observation; 24 common-seed paired blocks; random order cannot establish independent temporal host noise',
        'amortization': 'no actual user reuse horizon supplied; conditional break-even only, never a stock adoption recommendation'}


def _append_record(path, row):
    with Path(path).open('a') as stream:
        stream.write(json.dumps(row, allow_nan=False) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


def run_campaign(output, *, allow_repeatable=False, allow_process=False, affinity=8,
                 development=False, development_report=None):
    if not allow_repeatable or not allow_process:
        raise ValueError('explicit fixture repetition and process consent required')
    if not 1 <= affinity <= 8:
        raise ValueError('affinity must be in [1,8]')
    output = Path(output)
    if output.exists():
        raise ValueError('new campaign directory required; all earlier results are retained')
    output.mkdir(parents=True)
    started = time.perf_counter()
    development_cost = None
    development_identity = None
    if development_report is not None:
        development_path = Path(development_report)
        development_data = json.loads(development_path.read_text())
        development_cost = development_data.get('elapsed_seconds')
        if development_data.get('phase') != 'development' or not _valid_number(development_cost, positive=True):
            raise ValueError('development report must identify valid retained development cost')
        development_identity = {'path': str(development_path.resolve()), 'sha256': hashlib.sha256(development_path.read_bytes()).hexdigest(),
            'elapsed_seconds': development_cost, 'source_identity': development_data.get('source_identity')}
    plan = predeclared_plan()
    identity = source_identity()
    report = {'schema_version': 1, 'status': 'running', 'phase': 'development' if development else 'confirmation',
        'plan': plan, 'plan_sha256': digest(plan), 'source_identity': identity,
        'environment': workload_environment(), 'started_unix': time.time(), 'records': [],
        'comparisons': [], 'recommendation': None, 'development_evidence': development_identity,
        'slots': [], 'planned_observations': 8 if development else 216,
        'requested_affinity_count': affinity,
        'expected_affinity': sorted(os.sched_getaffinity(0))[:affinity] if hasattr(os, 'sched_getaffinity') else None}
    _write(output / 'preregistration.json', {key: value for key, value in report.items() if key != 'records'})
    _write(output / 'report.json', report)
    def measure(config, seed, phase, block, ordinal):
        if source_identity() != identity:
            raise RuntimeError('source changed after preregistration; stop campaign')
        slot = f'{phase}-{block:02d}-{ordinal:02d}'
        slot_state = {'slot': slot, 'phase': phase, 'block': block, 'order': ordinal, 'seed': seed,
                      'configuration': asdict(config), 'status': 'launched'}
        report['slots'].append(slot_state)
        _append_record(output / 'slot-ledger.jsonl', slot_state)
        _write(output / 'report.json', report)
        try:
            row = supervise_observation(config, seed, output / 'observations', slot, affinity=affinity)
        except BaseException as exc:
            row = {'configuration': asdict(config), 'seed': seed, 'correct': False,
                   'exclusions': ['supervisor_interrupted_or_failed'], 'error': type(exc).__name__,
                   'phase': phase, 'block': block, 'order': ordinal, 'configuration_key': config.key}
            slot_state['status'] = 'interrupted_or_failed'
            report['records'].append(row)
            _append_record(output / 'records.jsonl', row)
            _append_record(output / 'slot-ledger.jsonl', slot_state)
            raise
        row.update(phase=phase, block=block, order=ordinal, configuration_key=config.key)
        if row.get('source_identity') != identity:
            row['correct'] = False
            row['exclusions'].append('child_runtime_identity_mismatch')
        if row.get('affinity') != report['expected_affinity'] or row.get('requested_affinity_count') != affinity:
            row['correct'] = False
            row['exclusions'].append('affinity_scope_mismatch')
        previous = [r for r in report['records'] if r['seed'] == seed and r.get('fixture_manifest_digest')]
        if row.get('fixture_manifest_digest') and any(r['fixture_manifest_digest'] != row['fixture_manifest_digest'] for r in previous):
            row['correct'] = False
            row['exclusions'].append('paired_fixture_content_mismatch')
        slot_state['status'] = 'settled'
        _append_record(output / 'slot-ledger.jsonl', slot_state)
        _append_record(output / 'records.jsonl', row)
        report['records'].append(row)
        report['elapsed_seconds'] = time.perf_counter() - started
        _write(output / 'report.json', report)
        print(slot, config.key, row.get('warm', {}).get('seconds'), row['exclusions'], flush=True)
        return row
    try:
        grid = grids()
        if development:
            # Fixed feasibility set; no post-timing cherry-picking. Eight calls.
            modes = (Configuration('stdlib', 'thread', 8, 0, 1),
                     Configuration('stdlib', 'process', 0, 8, 4),
                     Configuration('stdlib', 'static', 4, 4, 4),
                     Configuration('stdlib', 'overflow', 4, 4, 1),
                     Configuration('intelligent', 'adaptive', 4, 4, 1),
                     Configuration('intelligent', 'adaptive', 4, 4, 4),
                     Configuration('stdlib', 'static', 2, 6, 1),
                     Configuration('intelligent', 'adaptive', 2, 6, 1))
            for index, config in enumerate(modes):
                measure(config, DEVELOPMENT_SEEDS[index // 2], 'development', index // 2, index % 2)
            report['status'] = 'development_complete'
        else:
            rng = random.Random(603302)
            all_modes = tuple(c for values in grid.values() for c in values)
            for block, seed in enumerate(TRAINING_SEEDS):
                order = list(all_modes)
                rng.shuffle(order)
                for ordinal, config in enumerate(order):
                    measure(config, seed, 'training', block, ordinal)
            selected = {}
            for label, modes in grid.items():
                eligible = []
                for config in modes:
                    rows = [r for r in report['records'] if r['configuration_key'] == config.key]
                    if len(rows) == 3 and all(r['exclusions'] == [] for r in rows):
                        eligible.append((statistics.median(r['warm']['seconds'] for r in rows), config))
                selected[label] = min(eligible)[1] if eligible else None
            fixed = Configuration('stdlib', 'thread', 8, 0, 1)
            fixed_ok = all(r['exclusions'] == [] for r in report['records'] if r['configuration_key'] == fixed.key)
            report['selected'] = {label: asdict(c) if c else None for label, c in selected.items()}
            frozen = {'selected': report['selected'], 'training_record_digest': digest(report['records']),
                      'source_identity': identity, 'frozen_before_heldout_unix': time.time()}
            _write(output / 'frozen-selection.json', frozen)
            if not all(selected.values()) or not fixed_ok:
                report['status'] = 'inconclusive'
                report['reasons'] = ['no_eligible_training_finalist_or_fixed_baseline']
                report['skipped_heldout_slots_upper_bound'] = 24 * 5
            else:
                roles = {'fixed': fixed, 'single': selected['single'], 'static': selected['static'],
                         'overflow': selected['overflow']}
                hold_modes = tuple(dict.fromkeys((*roles.values(), selected['intelligent'])))
                report['planned_observations'] = 96 + 24 * len(hold_modes)
                heldout = plan['seeds']['heldout']
                for block, seed in enumerate(heldout):
                    order = list(hold_modes)
                    rng.shuffle(order)
                    for ordinal, config in enumerate(order):
                        measure(config, seed, 'heldout', block, ordinal)
                def records(config):
                    return [r for r in report['records'] if r['phase'] == 'heldout'
                            and r['configuration_key'] == config.key]
                candidate = records(selected['intelligent'])
                report['comparisons'] = [compare(role, records(config), candidate, heldout)
                                         for role, config in roles.items()]
                report['status'] = ('warm_execution_supported_adoption_horizon_unknown'
                                    if all(c['passed'] for c in report['comparisons']) else 'no_full_execution_advantage')
                report['reasons'] = [comparison['role'] + ':' + reason
                                     for comparison in report['comparisons'] for reason in comparison['reasons']]
                full_session = {}
                for role, config in roles.items():
                    bases = records(config)
                    if (len(bases) != 24 or len(candidate) != 24
                            or any(r.get('exclusions') != [] or not _valid_number(r.get('lifecycle_seconds'), positive=True)
                                   for r in (*bases, *candidate))):
                        full_session[role] = {'available': False, 'reason': 'incomplete_or_failed_paired_lifecycle'}
                    else:
                        savings = [b['lifecycle_seconds'] - c['lifecycle_seconds'] for b, c in zip(bases, candidate)]
                        full_session[role] = {'available': True, 'minimum_observed_saving_seconds': min(savings),
                                             'mean_observed_saving_seconds': statistics.fmean(savings)}
                report['full_session_savings_descriptive'] = full_session
        if source_identity() != identity:
            raise RuntimeError('source changed during campaign')
    except BaseException as exc:
        report.update(status='interrupted_or_failed', abort_class=type(exc).__name__,
            elapsed_seconds=time.perf_counter() - started,
            attempted_observations=len(report['slots']), settled_observations=len(report['records']),
            failed_observations=sum(bool(r['exclusions']) for r in report['records']),
            successful_observations=sum(not r['exclusions'] for r in report['records']),
            skipped_observations=report['planned_observations'] - len(report['slots']))
        _write(output / 'report.json', report)
        raise
    report.update(elapsed_seconds=time.perf_counter() - started, ended_unix=time.time(),
                  attempted_observations=len(report['slots']), settled_observations=len(report['records']),
                  successful_observations=sum(not row['exclusions'] for row in report['records']),
                  skipped_observations=report['planned_observations'] - len(report['slots']),
                  failed_observations=sum(bool(row['exclusions']) for row in report['records']))
    report['discovery_cost'] = {
        'campaign_wall_seconds_including_internal_analysis': report['elapsed_seconds'],
        'prior_development_wall_seconds': development_cost,
        'total_known_wall_seconds': report['elapsed_seconds'] + (development_cost or 0),
        'scope': 'Development plus this campaign fixture preparation, oracles, training, held-out validation and internal analysis when development is linked; excludes prior historical campaigns and later human/external review time.'}
    if report['comparisons'] and all(c['passed'] for c in report['comparisons']):
        saving = min(c['minimum_observed_saving_seconds'] for c in report['comparisons'])
        full = report.get('full_session_savings_descriptive', {})
        lifecycle_saving = (min(v['minimum_observed_saving_seconds'] for v in full.values())
                            if full and all(v['available'] for v in full.values()) else None)
        numerator = report['discovery_cost']['total_known_wall_seconds']
        report['conditional_break_even'] = {
            'numerator_seconds': numerator,
            'cost_scope_complete_through_internal_analysis': development_cost is not None,
            'warm_256_job_fixtures': math.ceil(numerator / saving) if saving > 0 else None,
            'full_sessions': math.ceil(numerator / lifecycle_saving) if lifecycle_saving is not None and lifecycle_saving > 0 else None,
            'none_meaning': 'Unavailable or no positive minimum observed saving; no amortization claim',
            'actual_reuse_horizon': None}
    _write(output / 'report.json', report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    worker = sub.add_parser('worker')
    worker.add_argument('--request', type=Path, required=True)
    for name in ('development', 'run'):
        entry = sub.add_parser(name)
        entry.add_argument('--output', type=Path, required=True)
        entry.add_argument('--allow-repeatable', action='store_true')
        entry.add_argument('--allow-process', action='store_true')
        entry.add_argument('--affinity', type=int, default=8)
        entry.add_argument('--development-report', type=Path)
    args = parser.parse_args()
    if args.command == 'worker':
        request = json.loads(args.request.read_text())
        if not request.get('allow_repeatable') or not request.get('allow_process'):
            raise ValueError('missing worker consent')
        count = request['affinity']
        if type(count) is not int or not 1 <= count <= 8:
            raise ValueError('affinity budget')
        if hasattr(os, 'sched_setaffinity'):
            os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:count])
        result = observe(Configuration(**request['configuration']), request['seed'],
                         allow_repeatable=True, allow_process=True)
        result['requested_affinity_count'] = count
        _write(request['output'], result)
    else:
        run_campaign(args.output, allow_repeatable=args.allow_repeatable,
            allow_process=args.allow_process, affinity=args.affinity, development=args.command == 'development',
            development_report=args.development_report)


if __name__ == '__main__':
    main()
