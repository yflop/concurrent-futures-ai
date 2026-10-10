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
                 max_profile_keys=256, mp_context=None):
        for name, value, minimum in (("max_workers", max_workers, 1),
                                     ("process_workers", process_workers, 0),
                                     ("max_pending", max_pending, 1),
                                     ("max_profile_keys", max_profile_keys, 1)):
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError(f"{name} must be an integer >= {minimum}")
        if submit_timeout is not None and (submit_timeout < 0 or not math.isfinite(submit_timeout)):
            raise ValueError("submit_timeout must be finite and nonnegative or None")
        if not 0 < ewma_alpha <= 1:
            raise ValueError("ewma_alpha must be in (0, 1]")
        self._policy = PolicyRegistry().create(policy) if isinstance(policy, str) else policy
        if not callable(getattr(self._policy, "choose", None)):
            raise TypeError("policy must implement choose")
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

    def _profile_view(self):
        return MappingProxyType({key: MappingProxyType(dict(value)) for key, value in self._profiles.items()})

    def _submit(self, fn, args, kwargs, hints):
        if not callable(fn):
            raise TypeError("fn must be callable")
        if not isinstance(hints, TaskHints):
            raise TypeError("hints must be TaskHints")
        deadline = None if self._submit_timeout is None else time.monotonic() + self._submit_timeout
        with self._condition:
            while not self._closed and self._pending >= self._max_pending:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    self._counts["rejected"] += 1
                    raise BackpressureError("executor admission limit reached")
                self._condition.wait(remaining)
            if self._closed:
                raise RuntimeError("cannot schedule new futures after shutdown")
            backend = self._policy.choose(hints, self._profile_view(), self._processes is not None)
            if backend not in {"thread", "process"}:
                raise ValueError("policy must return thread or process")
            # Policy plugins cannot bypass the caller's process-safety boundary.
            if backend == "process" and (not hints.process_safe or self._processes is None):
                raise ValueError("process routing requires process_safe=True and process_workers > 0")
            pool = self._processes if backend == "process" else self._threads
            started = time.perf_counter()
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
