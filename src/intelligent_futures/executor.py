"""Bounded, instrumented scheduling using genuine standard-library Futures."""
from collections import OrderedDict, deque
from concurrent.futures import Executor, ThreadPoolExecutor, ProcessPoolExecutor
from dataclasses import asdict
import math
import multiprocessing
import threading
import time
from types import MappingProxyType

from .policies import TaskHints, Profile, PolicyRegistry
from .prediction import (HardwareCapabilities, ExecutionObjective, Predictor,
                         RuntimeSample, normalize_features)


class BackpressureError(TimeoutError):
    """Admission did not become available within submit_timeout."""


class IntelligentExecutor(Executor):
    """Executor-compatible prototype with explicit process consent.

    max_pending bounds all accepted unfinished tasks, running plus queued.
    submit_timeout controls admission only, never execution deadlines.
    process_workers=0 disables processes. Spawn is the portable default.
    Callbacks are instrumentation, not scheduling worker threads.
    """
    def __init__(self, max_workers=4, *, process_workers=0, max_pending=64,
                 policy="adaptive", submit_timeout=None, ewma_alpha=0.25,
                 max_profile_keys=256, mp_context=None, predictive=False,
                 hardware=None, objective=None, workload_profiles=(), profile_sample_interval=1):
        for name, value, minimum in (("max_workers", max_workers, 1),
                                     ("process_workers", process_workers, 0),
                                     ("max_pending", max_pending, 1),
                                     ("max_profile_keys", max_profile_keys, 1),
                                     ("profile_sample_interval", profile_sample_interval, 1)):
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError(f"{name} must be an integer >= {minimum}")
        if submit_timeout is not None and (submit_timeout < 0 or not math.isfinite(submit_timeout)):
            raise ValueError("submit_timeout must be finite and nonnegative or None")
        if not 0 < ewma_alpha <= 1:
            raise ValueError("ewma_alpha must be in (0, 1]")
        self._policy = PolicyRegistry().create(policy) if isinstance(policy, str) else policy
        if not callable(getattr(self._policy, "choose", None)):
            raise TypeError("policy must implement choose")
        if not isinstance(predictive, bool):
            raise TypeError("predictive must be bool")
        if not predictive and (hardware is not None or objective is not None or workload_profiles):
            raise ValueError("hardware, objective and workload_profiles require predictive=True")
        self._predictive_routing = isinstance(policy, str) and policy == "adaptive"
        self._predictor = None
        if predictive:
            self._predictor = Predictor(hardware=hardware, objective=objective,
                                        profiles=workload_profiles, max_profiles=max_profile_keys)
            if self._predictor.objective.min_concurrency > min(max_pending, max_workers, process_workers or max_workers):
                raise ValueError("min_concurrency exceeds the available worker or pending budget")
        self._worker_limits = {"thread": max_workers, "process": process_workers}
        self._concurrency_limit = max_pending
        self._prediction = None
        self._running_threads = 0
        self._profiling_errors = 0
        self._sample_interval = profile_sample_interval
        self._sample_counts = OrderedDict()
        self._threads = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="intelligent")
        self._processes = None
        if process_workers:
            self._processes = ProcessPoolExecutor(max_workers=process_workers,
                mp_context=mp_context or multiprocessing.get_context("spawn"))
        self._condition = threading.Condition(threading.RLock())
        self._closed = False
        self._cancel_requested = False
        self._pending = 0
        self._max_pending = max_pending
        self._submit_timeout = submit_timeout
        self._alpha = ewma_alpha
        self._max_profile_keys = max_profile_keys
        self._profiles = OrderedDict()
        self._counts = dict(submitted=0, completed=0, failed=0, cancelled=0,
                            rejected=0, thread_submitted=0, process_submitted=0,
                            peak_pending=0, profiles_evicted=0)

    def submit(self, fn, /, *args, **kwargs):
        """Standard submit: kwargs always belong to fn, even 'hints'."""
        return self._submit(fn, args, kwargs, TaskHints())

    def submit_task(self, fn, /, *args, hints=None, **kwargs):
        """Opt-in routing API; hints is reserved here, unlike submit."""
        return self._submit(fn, args, kwargs, TaskHints() if hints is None else hints)

    def submit_profiled(self, fn, /, *args, hints=None, features=(), **kwargs):
        """Explicit opt-in numeric feature summaries; never inspect raw inputs.

        features must contain only caller-approved, non-sensitive numeric summaries.
        No callable source, arguments, results, or exception messages are exported.
        """
        if self._predictor is None:
            raise ValueError("submit_profiled requires predictive=True")
        return self._submit(fn, args, kwargs, TaskHints() if hints is None else hints,
                            normalize_features(features))

    def export_profiles(self):
        """Immutable, bounded snapshots; serialize each with WorkloadProfile.to_json()."""
        with self._condition:
            return () if self._predictor is None else self._predictor.profiles

    def _measure_thread(self, fn, args, kwargs, hints, features):
        with self._condition:
            self._running_threads += 1
            concurrency = self._running_threads
            sample_key = (hints.key, features)
            sequence = self._sample_counts.get(sample_key, 0)
            self._sample_counts[sample_key] = (sequence + 1) % self._sample_interval
            self._sample_counts.move_to_end(sample_key)
            if len(self._sample_counts) > self._max_profile_keys:
                self._sample_counts.popitem(last=False)
            measure = sequence == 0
        if not measure:
            try:
                return fn(*args, **kwargs)
            finally:
                with self._condition:
                    self._running_threads -= 1
        wall = time.perf_counter()
        cpu_clock = getattr(time, "thread_time", None)
        cpu = cpu_clock() if cpu_clock is not None else None
        succeeded = False
        try:
            result = fn(*args, **kwargs)
            succeeded = True
            return result
        finally:
            wall_elapsed = max(0.0, time.perf_counter() - wall)
            cpu_elapsed = None if cpu is None else max(0.0, cpu_clock() - cpu)
            with self._condition:
                self._running_threads -= 1
                if succeeded:
                    try:
                        self._predictor.record(hints.key, RuntimeSample(
                            wall_seconds=wall_elapsed, thread_cpu_seconds=cpu_elapsed,
                            backend="thread", concurrency=concurrency), features=features)
                    except Exception:
                        # Observability must never replace the user's result.
                        self._profiling_errors += 1

    def _profile_view(self):
        return MappingProxyType({key: MappingProxyType(dict(value)) for key, value in self._profiles.items()})

    def _submit(self, fn, args, kwargs, hints, features=()):
        if not callable(fn):
            raise TypeError("fn must be callable")
        if not isinstance(hints, TaskHints):
            raise TypeError("hints must be TaskHints")
        deadline = None if self._submit_timeout is None else time.monotonic() + self._submit_timeout
        chosen_backend = None
        with self._condition:
            while not self._closed:
                if self._predictor is not None:
                    self._prediction = self._predictor.recommend(
                        hints.key, features=features, kind=hints.kind,
                        process_safe=hints.process_safe,
                        process_available=self._processes is not None)
                    if self._predictive_routing:
                        chosen_backend = self._prediction.backend
                    elif chosen_backend is None:
                        chosen_backend = self._policy.choose(hints, self._profile_view(), self._processes is not None)
                    if chosen_backend not in {"thread", "process"}:
                        raise ValueError("policy must return thread or process")
                    if chosen_backend == "process" and (not hints.process_safe or self._processes is None):
                        raise ValueError("process routing requires process_safe=True and process_workers > 0")
                    # Admission limits accepted unfinished work, without changing
                    # the fixed capacity or private internals of a stdlib pool.
                    self._concurrency_limit = min(self._max_pending,
                        self._worker_limits[chosen_backend], self._prediction.concurrency)
                if self._pending < self._concurrency_limit:
                    break
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    self._counts["rejected"] += 1
                    raise BackpressureError("executor admission limit reached")
                self._condition.wait(remaining)
            if self._closed:
                raise RuntimeError("cannot schedule new futures after shutdown")
            backend = (chosen_backend if self._predictor is not None else
                       self._policy.choose(hints, self._profile_view(), self._processes is not None))
            if backend not in {"thread", "process"}:
                raise ValueError("policy must return thread or process")
            # Policy plugins cannot bypass the caller's process-safety boundary.
            if backend == "process" and (not hints.process_safe or self._processes is None):
                raise ValueError("process routing requires process_safe=True and process_workers > 0")
            pool = self._processes if backend == "process" else self._threads
            started = time.perf_counter()
            if self._predictor is not None and backend == "thread":
                future = pool.submit(self._measure_thread, fn, args, kwargs, hints, features)
            else:
                future = pool.submit(fn, *args, **kwargs)
            self._pending += 1
            self._counts["submitted"] += 1
            self._counts[backend + "_submitted"] += 1
            self._counts["peak_pending"] = max(self._counts["peak_pending"], self._pending)
            # Register under the RLock so shutdown cannot overtake bookkeeping.
            # An already-finished Future invokes this immediately and reentrantly.
            future.add_done_callback(lambda done: self._record(done, hints.key, backend, started))
        return future

    def _record(self, future, key, backend, started):
        duration = time.perf_counter() - started
        with self._condition:
            self._pending -= 1
            if future.cancelled():
                self._counts["cancelled"] += 1
            else:
                failed = future.exception() is not None
                self._counts["failed" if failed else "completed"] += 1
                bucket = self._profiles.setdefault(key, {})
                self._profiles.move_to_end(key)
                previous = bucket.get(backend, Profile())
                # Failed observations are counted, but never train latency choice.
                if failed:
                    bucket[backend] = Profile(previous.samples, previous.ewma_seconds,
                                              previous.total_seconds, previous.failures + 1)
                else:
                    ewma = duration if not previous.samples else self._alpha * duration + (1-self._alpha) * previous.ewma_seconds
                    bucket[backend] = Profile(previous.samples + 1, ewma,
                                              previous.total_seconds + duration, previous.failures)
                if len(self._profiles) > self._max_profile_keys:
                    self._profiles.popitem(last=False)
                    self._counts["profiles_evicted"] += 1
            self._condition.notify_all()

    def snapshot(self):
        """JSON-safe copy. Latency excludes admission wait, includes pool queueing.

        Future.result may return just before its bookkeeping callback runs.
        shutdown(wait=True) joins workers and establishes final metrics.
        """
        with self._condition:
            return {"counts": dict(self._counts), "pending": self._pending,
                    "predictive": None if self._predictor is None else {
                        "hardware": {**asdict(self._predictor.hardware),
                                     "usable_cpus": self._predictor.hardware.usable_cpus,
                                     "fingerprint": self._predictor.hardware.fingerprint},
                        "objective": asdict(self._predictor.objective),
                        "concurrency_limit": self._concurrency_limit,
                        "last_prediction": None if self._prediction is None else asdict(self._prediction),
                        "profile_count": len(self._predictor.profiles),
                        "profiling_errors": self._profiling_errors,
                        "profile_sample_interval": self._sample_interval,
                        "process_runtime_metrics": "unavailable",
                        "task_memory_metrics": "unavailable"},
                    "closed": self._closed, "max_pending": self._max_pending,
                    "profiles": {k: {b: asdict(p) for b, p in v.items()} for k, v in self._profiles.items()}}

    def map_bounded(self, fn, iterable, *, buffersize=16, timeout=None, hints=None):
        """Ordered single-iterable streaming map with bounded submitted results.

        timeout is measured from this call, as for Executor.map. Admission may
        block independently; use finite submit_timeout when deadlines matter.
        Closing/exhausting the iterator cancels unstarted buffered tasks.
        """
        if isinstance(buffersize, bool) or not isinstance(buffersize, int) or buffersize < 1:
            raise ValueError("buffersize must be a positive integer")
        deadline = None if timeout is None else time.monotonic() + timeout
        def results():
            source = iter(iterable)
            pending = deque()
            exhausted = False
            try:
                while True:
                    while not exhausted and len(pending) < buffersize:
                        try:
                            item = next(source)
                        except StopIteration:
                            exhausted = True
                            break
                        pending.append(self.submit_task(fn, item, hints=hints))
                    if not pending:
                        break
                    future = pending.popleft()
                    try:
                        yield future.result(timeout=None if deadline is None else max(0, deadline-time.monotonic()))
                    finally:
                        future.cancel()
            finally:
                for future in pending:
                    future.cancel()
        return results()

    def map_adaptive(self, fn, iterable, *, min_batch_size=1, max_batch_size=64,
                     target_batch_seconds=0.05, hints=None):
        """Ordered adaptive submission windows with bounded memory and no retries.

        Every input is a separate Future/call, not a vectorized or process chunk.
        Window size adapts to callback-observed completion time (including admission
        and earlier completion callbacks), excluding consumer pauses;
        each next window is at most twice the previous one. Closing the iterator
        cancels unstarted buffered work. No speculative duplicate execution.
        """
        for name, value in (("min_batch_size", min_batch_size), ("max_batch_size", max_batch_size)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if min_batch_size > max_batch_size:
            raise ValueError("min_batch_size must not exceed max_batch_size")
        if (isinstance(target_batch_seconds, bool) or not isinstance(target_batch_seconds, (int, float))
                or not math.isfinite(target_batch_seconds) or target_batch_seconds <= 0):
            raise ValueError("target_batch_seconds must be finite and positive")
        if hints is not None and not isinstance(hints, TaskHints):
            raise TypeError("hints must be TaskHints")
        if not callable(fn):
            raise TypeError("fn must be callable")

        def results():
            source = iter(iterable)
            batch_size = min_batch_size
            pending = []
            exhausted = False
            try:
                while not exhausted:
                    pending = []
                    started = time.monotonic()
                    completion = [started]
                    completion_lock = threading.Lock()
                    completion_events = []

                    for _ in range(batch_size):
                        try:
                            item = next(source)
                        except StopIteration:
                            exhausted = True
                            break
                        future = self.submit_task(fn, item, hints=hints)
                        pending.append(future)
                        ready = threading.Event()
                        completion_events.append(ready)

                        def finished(_future, stamp=completion, lock=completion_lock, ready=ready):
                            with lock:
                                stamp[0] = max(stamp[0], time.monotonic())
                            ready.set()

                        future.add_done_callback(finished)
                    for future in pending:
                        yield future.result()
                    for ready in completion_events:
                        ready.wait()
                    with completion_lock:
                        elapsed = max(1e-9, completion[0] - started)
                    if pending:
                        proposed = int(min(batch_size * 2, len(pending) * target_batch_seconds / elapsed))
                        batch_size = max(min_batch_size, min(max_batch_size,
                            max(1, batch_size // 2, min(batch_size * 2, proposed))))
            finally:
                for future in pending:
                    future.cancel()
        return results()

    def map_batches(self, batch_fn, iterable, *, buffersize=2, min_batch_size=1,
                    max_batch_size=64, target_batch_seconds=0.05, hints=None):
        """Execute actual adaptive tuple chunks, yielding one result per batch.

        batch_fn(tuple_of_inputs) is invoked exactly once per accepted chunk.
        It defines its own result shape and any within-batch processing. A failed
        chunk is never retried; earlier side effects are not rolled back. Running
        chunks cannot be partially cancelled. At most buffersize chunks are
        retained, each with at most max_batch_size inputs. Chunk size targets
        measured admission-to-completion-callback latency, including admission wait,
        queueing, serialization, and preceding completion callbacks.
        """
        for name, value in (("buffersize", buffersize), ("min_batch_size", min_batch_size),
                            ("max_batch_size", max_batch_size)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if min_batch_size > max_batch_size:
            raise ValueError("min_batch_size must not exceed max_batch_size")
        if (isinstance(target_batch_seconds, bool) or not isinstance(target_batch_seconds, (int, float))
                or not math.isfinite(target_batch_seconds) or target_batch_seconds <= 0):
            raise ValueError("target_batch_seconds must be finite and positive")
        if not callable(batch_fn):
            raise TypeError("batch_fn must be callable")
        if hints is not None and not isinstance(hints, TaskHints):
            raise TypeError("hints must be TaskHints")

        def results():
            source = iter(iterable)
            batch_size = min_batch_size
            pending = deque()
            exhausted = False
            try:
                while True:
                    while not exhausted and len(pending) < buffersize:
                        batch = []
                        for _ in range(batch_size):
                            try:
                                batch.append(next(source))
                            except StopIteration:
                                exhausted = True
                                break
                        if not batch:
                            break
                        started = time.monotonic()
                        measured = threading.Event()
                        elapsed = []

                        def finished(_future, start=started, timing=elapsed, ready=measured):
                            timing.append(max(1e-9, time.monotonic() - start))
                            ready.set()

                        future = self.submit_task(batch_fn, tuple(batch), hints=hints)
                        pending.append((future, len(batch), measured, elapsed))
                        future.add_done_callback(finished)
                    if not pending:
                        break
                    future, count, measured, elapsed = pending.popleft()
                    try:
                        result = future.result()
                        # result() can return just before completion callbacks.
                        measured.wait()
                        proposed = int(min(batch_size * 2, count * target_batch_seconds / elapsed[0]))
                        batch_size = max(min_batch_size, min(max_batch_size,
                            max(1, batch_size // 2, proposed)))
                        yield result
                    finally:
                        future.cancel()
            finally:
                for future, _, _, _ in pending:
                    future.cancel()
        return results()

    def shutdown(self, wait=True, *, cancel_futures=False):
        with self._condition:
            self._closed = True
            self._cancel_requested = self._cancel_requested or cancel_futures
            cancel_futures = self._cancel_requested
            self._condition.notify_all()
        # ThreadPool retains joinable worker references after wait=False, whereas
        # ProcessPool does not retain its manager reference. Call the process
        # shutdown once with the requested wait mode, then join threads.
        self._threads.shutdown(wait=False, cancel_futures=cancel_futures)
        if self._processes is not None:
            self._processes.shutdown(wait=wait, cancel_futures=cancel_futures)
        if wait:
            self._threads.shutdown(wait=True, cancel_futures=cancel_futures)
            # Also establishes final metrics after an earlier wait=False call.
            with self._condition:
                while self._pending:
                    self._condition.wait()
