"""Bounded, instrumented scheduling using genuine standard-library Futures."""
from collections import OrderedDict, deque
from concurrent.futures import (CancelledError, Executor, Future, ThreadPoolExecutor,
                                ProcessPoolExecutor)
from dataclasses import asdict, dataclass
import math
import logging
import multiprocessing
from queue import SimpleQueue
import threading
import time
from types import MappingProxyType

from .policies import (TaskHints, Profile, PolicyRegistry, BackendLoad, SchedulingLoad,
                       AdaptivePolicy, HintPolicy, ProcessPolicy, ThreadPolicy)
from .prediction import (HardwareCapabilities, ExecutionObjective, Predictor,
                         RuntimeSample, normalize_features)


class BackpressureError(TimeoutError):
    """Admission did not become available within submit_timeout."""


@dataclass(frozen=True)
class RoutingDecision:
    """Sanitized evidence for one accepted submission, without task identity.

    load and pending describe scheduler state immediately before admission.
    A policy may have chosen its fixed route before an admission wait; its
    reason/exploration evidence is preserved even if that load later changes.
    admission_limit bounds unfinished accepted tasks; load.max_pending also
    bounds physically retained items, and load.*.workers cap dispatch slots.
    These are exact scheduler limits/counts, not sampled CPU/RSS measurements
    or a claim that operating-system resource use is capped.
    """
    sequence: int
    backend: str
    reason: str
    exploration: bool
    load: SchedulingLoad
    pending: int
    admission_limit: int


# Counter cardinality is independent of keys, input data and custom policies.
_ROUTING_REASONS = (
    "thread-policy", "process-policy", "cpu-hint", "io-hint", "default-thread",
    "process-unavailable", "process-consent-required", "task-hint",
    "queue-pressure", "uncontended-load-proxy", "completion-ewma", "adaptive-policy",
    "predictive-heuristic", "predictive-measurement-guided", "predictive-empirical",
    "predictive-transfer-prior", "custom-policy",
)
_ADAPTIVE_REASONS = {value: value for value in (
    "task-hint", "queue-pressure", "uncontended-load-proxy", "completion-ewma")}
_PREDICTIVE_REASONS = {value: "predictive-" + value for value in (
    "heuristic", "measurement-guided", "empirical", "transfer-prior")}


class _WorkItem:
    """One removable payload; backend queues hold only bounded dispatches."""
    __slots__ = ("future", "fn", "args", "kwargs", "hints", "features", "backend",
                 "started", "uncontended", "cancel_notified")

    def __init__(self, future, fn, args, kwargs, hints, features, backend, uncontended):
        self.future = future
        self.fn, self.args, self.kwargs = fn, args, kwargs
        self.hints, self.features = hints, features
        self.backend = backend
        self.started = time.perf_counter()
        self.uncontended = uncontended
        self.cancel_notified = False

    def clear_payload(self):
        # The caller must retain this tuple until outside the scheduler lock:
        # dropping the final reference can invoke arbitrary user __del__ code.
        released = self.fn, self.args, self.kwargs
        self.fn = self.args = self.kwargs = None
        return released


class _RecordCallback:
    """Retain live bookkeeping, then detach without editing Future internals.

    Future retains its callback list after completion. Leaving the WorkItem
    here would create Future -> callback -> WorkItem -> Future result cycles and
    keep the executor alive for as long as a completed Future is retained.
    """
    __slots__ = ("owner", "item")

    def __init__(self, owner, item):
        self.owner, self.item = owner, item

    def __call__(self, done):
        owner, item = self.owner, self.item
        if owner is None:
            return
        try:
            owner._record(done, item)
        finally:
            # Keep strong locals until cancellation accounting has finished,
            # even if a concurrent dispatcher already removed the work item.
            self.owner = self.item = None


class IntelligentExecutor(Executor):
    """Bounded runtime with stdlib Futures and explicit process consent.

    max_pending bounds accepted unfinished tasks AND retained scheduling items.
    Owned queued payloads are removed on cancellation. A cancelled dispatched
    thread item keeps its physical slot until its backend acknowledges it.
    Each backend has at most its worker count of outstanding submissions.
    submit_timeout controls admission only, never execution deadlines.
    process_workers=0 disables processes. Spawn is the portable default.
    trace_capacity=0 disables routing traces; positive values retain only the
    newest bounded sanitized routing decisions, with cumulative route counters.
    Explicit shutdown (or a context manager) drains and joins the scheduler.
    """
    def __init__(self, max_workers=4, *, process_workers=0, max_pending=64,
                 policy="adaptive", submit_timeout=None, ewma_alpha=0.25,
                 max_profile_keys=256, mp_context=None, predictive=False,
                 hardware=None, objective=None, workload_profiles=(), profile_sample_interval=1,
                 trace_capacity=0):
        for name, value, minimum in (("max_workers", max_workers, 1),
                                     ("process_workers", process_workers, 0),
                                     ("max_pending", max_pending, 1),
                                     ("max_profile_keys", max_profile_keys, 1),
                                     ("profile_sample_interval", profile_sample_interval, 1),
                                     ("trace_capacity", trace_capacity, 0)):
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError(f"{name} must be an integer >= {minimum}")
        if submit_timeout is not None and (submit_timeout < 0 or not math.isfinite(submit_timeout)):
            raise ValueError("submit_timeout must be finite and nonnegative or None")
        if not 0 < ewma_alpha <= 1:
            raise ValueError("ewma_alpha must be in (0, 1]")
        self._policy = PolicyRegistry().create(policy) if isinstance(policy, str) else policy
        session = getattr(self._policy, "for_executor", None)
        if callable(session):
            self._policy = session(max_keys=max_profile_keys)
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
        self._queued = {"thread": OrderedDict(), "process": OrderedDict()}
        self._in_flight = {"thread": {}, "process": {}}
        self._peak_queued = {"thread": 0, "process": 0}
        self._peak_in_flight = {"thread": 0, "process": 0}
        self._peak_retained = 0
        self._callback_context = threading.local()
        self._completions = SimpleQueue()
        self._completion_active = False
        self._completion_threads = set()
        self._shutdown_thread = None
        self._shutdown_error = None
        self._pending = 0
        self._max_pending = max_pending
        self._submit_timeout = submit_timeout
        self._alpha = ewma_alpha
        self._max_profile_keys = max_profile_keys
        self._profiles = OrderedDict()
        self._trace_capacity = trace_capacity
        self._routing_trace = deque(maxlen=trace_capacity)
        self._routing_recorded = 0
        self._routing_by_backend = {"thread": 0, "process": 0}
        self._routing_by_reason = dict.fromkeys(_ROUTING_REASONS, 0)
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

    def routing_trace(self):
        """Immutable, bounded accepted-routing snapshot in admission order.

        Rejections and failed synchronous dispatches are not accepted routes.
        Trace records never include callables, inputs, results, exceptions,
        workload keys, features, policy names, or custom-policy explanation text.
        Use dataclasses.asdict on a record for a JSON-safe detached copy.
        """
        with self._condition:
            return tuple(self._routing_trace)

    def routing_counters(self):
        """Detached cumulative counters with fixed category cardinality.

        dropped counts accepted records evicted from the bounded trace. With
        trace_capacity=0 both trace recording and these counters are disabled.
        Concurrent calls to the two snapshot methods are individually atomic,
        but need not represent the same instant.
        """
        with self._condition:
            retained = len(self._routing_trace)
            return {"enabled": bool(self._trace_capacity), "capacity": self._trace_capacity,
                    "recorded": self._routing_recorded, "retained": retained,
                    "dropped": self._routing_recorded - retained,
                    "by_backend": dict(self._routing_by_backend),
                    "by_reason": dict(self._routing_by_reason)}

    def _routing_reason(self, hints):
        # Never ask a custom policy for telemetry or retain its strings. Exact
        # built-in types avoid accidentally invoking a subclass's hooks here.
        policy_type = type(self._policy)
        if self._predictor is not None and self._predictive_routing:
            return _PREDICTIVE_REASONS[self._prediction.source], False
        if policy_type is ThreadPolicy:
            return "thread-policy", False
        if policy_type is ProcessPolicy:
            return "process-policy", False
        if policy_type not in (HintPolicy, AdaptivePolicy):
            return "custom-policy", False
        if policy_type is AdaptivePolicy or hints.kind == "cpu":
            if self._processes is None:
                return "process-unavailable", False
            if not hints.process_safe:
                return "process-consent-required", False
        if hints.kind == "io":
            return "io-hint", False
        if policy_type is HintPolicy:
            return ("cpu-hint" if hints.kind == "cpu" else "default-thread"), False
        decision = self._policy.snapshot()["last_decision"]
        reason = None if decision is None else decision["reason"]
        # Canonical constants, rather than even a string subclass returned by
        # external mutation of a built-in instance, enter the long-lived trace.
        reason = _ADAPTIVE_REASONS.get(reason, "adaptive-policy") if type(reason) is str else "adaptive-policy"
        return reason, decision is not None and decision["exploration"] is True

    def _record_routing_locked(self, backend, hints, load, pending, decision):
        reason, exploration = self._routing_reason(hints) if decision is None else decision
        backend = "thread" if backend == "thread" else "process"
        self._routing_recorded += 1
        self._routing_by_backend[backend] += 1
        self._routing_by_reason[reason] += 1
        self._routing_trace.append(RoutingDecision(self._routing_recorded, backend,
            reason, exploration, load, pending, self._concurrency_limit))

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

    def _load_view(self):
        return SchedulingLoad(
            thread=BackendLoad(self._worker_limits["thread"],
                len(self._queued["thread"]), len(self._in_flight["thread"])),
            process=BackendLoad(self._worker_limits["process"],
                len(self._queued["process"]), len(self._in_flight["process"])),
            max_pending=self._max_pending)

    def _choose(self, hints, load):
        choose = getattr(self._policy, "choose_with_load", None)
        if callable(choose):
            return choose(hints, self._profile_view(), self._processes is not None, load)
        return self._policy.choose(hints, self._profile_view(), self._processes is not None)

    def _retained(self):
        return sum(len(self._queued[b]) + len(self._in_flight[b]) for b in self._queued)

    def _submit(self, fn, args, kwargs, hints, features=()):
        if not callable(fn):
            raise TypeError("fn must be callable")
        if not isinstance(hints, TaskHints):
            raise TypeError("hints must be TaskHints")
        deadline = None if self._submit_timeout is None else time.monotonic() + self._submit_timeout
        chosen_backend = None
        routing_decision = None
        with self._condition:
            if not self._closed and type(self._policy) is ProcessPolicy:
                # Invalid deterministic-process submissions must fail clearly,
                # even when valid work has saturated the admission budget.
                self._validate_backend("process", hints)
            while not self._closed:
                load = self._load_view()
                backend = None
                if self._predictor is not None:
                    if not self._predictive_routing and chosen_backend is None:
                        chosen_backend = self._choose(hints, load)
                        self._validate_backend(chosen_backend, hints)
                        if self._trace_capacity:
                            # This fixed route survives admission waits. Other
                            # producers may advance built-in policy state while
                            # the condition is released, so capture its reason now.
                            routing_decision = self._routing_reason(hints)
                    self._prediction = self._predictor.recommend(
                        hints.key, features=features, kind=hints.kind,
                        process_safe=hints.process_safe,
                        process_available=self._processes is not None, load=load,
                        backend=chosen_backend)
                    backend = self._prediction.backend
                    self._validate_backend(backend, hints)
                    self._concurrency_limit = min(self._max_pending,
                        self._worker_limits[backend], self._prediction.concurrency)
                if self._pending < self._concurrency_limit and self._retained() < self._max_pending:
                    break
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    self._counts["rejected"] += 1
                    raise BackpressureError("executor admission limit reached")
                self._condition.wait(remaining)
            if self._closed:
                raise RuntimeError("cannot schedule new futures after shutdown")
            if backend is None:
                backend = self._choose(hints, load)
            self._validate_backend(backend, hints)
            self._ensure_completion_pump_locked()
            future = Future()
            item = _WorkItem(future, fn, args, kwargs, hints, features, backend,
                            not self._queued[backend] and not self._in_flight[backend])
            # Immediate backend failure preserves submit's synchronous failure
            # contract. Deferred dispatch failure instead belongs to the already
            # accepted Future. No failing callable is ever replayed.
            actions = []
            if not self._queued[backend] and len(self._in_flight[backend]) < self._worker_limits[backend]:
                raw, released = self._start_locked(item)
                actions.append(("watch", raw, item))
                actions.append(("release", released, None))
            else:
                self._queued[backend][future] = item
                self._peak_queued[backend] = max(self._peak_queued[backend], len(self._queued[backend]))
            self._pending += 1
            self._counts["submitted"] += 1
            self._counts[backend + "_submitted"] += 1
            self._counts["peak_pending"] = max(self._counts["peak_pending"], self._pending)
            self._peak_retained = max(self._peak_retained, self._retained())
            if self._trace_capacity:
                self._record_routing_locked(backend, hints, load, self._pending - 1, routing_decision)
            future.add_done_callback(_RecordCallback(self, item))
        self._run_actions(actions)
        return future

    def _ensure_completion_pump_locked(self):
        if self._completion_active:
            return
        pump = threading.Thread(target=self._pump_completions,
                                name="intelligent-completion", daemon=False)
        self._completion_active = True
        self._completion_threads.add(pump)
        try:
            pump.start()
        except BaseException:
            self._completion_threads.remove(pump)
            self._completion_active = False
            raise

    def _pump_completions(self):
        self._callback_context.active = True
        try:
            while True:
                with self._condition:
                    if not self._pending and not self._retained():
                        self._completion_active = False
                        return
                completion = self._completions.get()
                if completion is None:
                    continue
                raw, item = completion
                del completion
                try:
                    self._backend_done(raw, item)
                except BaseException:
                    # Future callback BaseExceptions have undefined stdlib
                    # semantics. Keep the owned pump alive so they cannot strand
                    # unrelated accepted work; cancellation-caller callbacks
                    # still propagate normally from Future.cancel().
                    logging.getLogger(__name__).exception("exception in executor completion callback")
                finally:
                    # Do not retain a completed result/exception while waiting
                    # for an unrelated slow task's next completion notification.
                    del raw, item
        finally:
            with self._condition:
                self._completion_threads.discard(threading.current_thread())
                self._condition.notify_all()

    def _validate_backend(self, backend, hints):
        if backend not in {"thread", "process"}:
            raise ValueError("policy must return thread or process")
        if backend == "process" and (not hints.process_safe or self._processes is None):
            raise ValueError("process routing requires process_safe=True and process_workers > 0")

    def _notify_cancel_locked(self, item):
        # Future.cancel() does not itself notify wait()/as_completed() waiters.
        # The executor owns this acknowledgement, exactly once, even if a
        # worker observed cancellation before the cancel callback got our lock.
        if not item.cancel_notified:
            item.future.set_running_or_notify_cancel()
            item.cancel_notified = True

    def _start_locked(self, item):
        backend = item.backend
        released = None
        if backend == "process":
            if not item.future.set_running_or_notify_cancel():
                item.cancel_notified = True
                return None, item.clear_payload()
            # Process transport cannot carry the public Future. Committing to
            # dispatch is the truthful process cancellation boundary.
            raw = self._processes.submit(item.fn, *item.args, **item.kwargs)
            released = item.clear_payload()
        else:
            raw = self._threads.submit(self._run_thread, item)
        self._in_flight[backend][item.future] = item
        self._peak_in_flight[backend] = max(self._peak_in_flight[backend], len(self._in_flight[backend]))
        return raw, released

    def _run_thread(self, item):
        previous = getattr(self._callback_context, "active", False)
        self._callback_context.active = True
        released = fn = args = kwargs = None
        try:
            with self._condition:
                if item.future.cancelled():
                    self._notify_cancel_locked(item)
                    released = item.clear_payload()
                    return None
                if not item.future.set_running_or_notify_cancel():
                    item.cancel_notified = True
                    released = item.clear_payload()
                    return None
                fn, args, kwargs = item.fn, item.args, item.kwargs
                item.clear_payload()
            if self._predictor is not None:
                return self._measure_thread(fn, args, kwargs, item.hints, item.features)
            return fn(*args, **kwargs)
        finally:
            # Finalizers also execute in this worker. Drop its payload outside
            # the lock but before allowing a waiting self-shutdown again.
            released = fn = args = kwargs = None
            self._callback_context.active = previous

    def _dispatch_locked(self):
        actions = []
        if self._cancel_requested:
            return actions
        for backend, queue in self._queued.items():
            while queue and len(self._in_flight[backend]) < self._worker_limits[backend]:
                _, item = queue.popitem(last=False)
                if item.future.cancelled():
                    self._notify_cancel_locked(item)
                    actions.append(("release", item.clear_payload(), None))
                    continue
                try:
                    raw, released = self._start_locked(item)
                except BaseException as error:
                    actions.append(("release", item.clear_payload(), None))
                    actions.append(("error", error, item))
                else:
                    actions.append(("release", released, None))
                    if raw is not None:
                        actions.append(("watch", raw, item))
        return actions

    def _backend_done(self, raw, item):
        with self._condition:
            self._in_flight[item.backend].pop(item.future)
            released = item.clear_payload()
            actions = self._dispatch_locked()
            self._condition.notify_all()
        # Never invoke application callbacks under the scheduler lock.
        actions.append(("publish", raw, item))
        self._run_actions(actions)

    def _run_actions(self, actions):
        if not actions:
            return
        # A Future can already be complete when add_done_callback is called.
        # Use an iterative per-thread pump, not recursive dispatch completion.
        active = getattr(self._callback_context, "actions", None)
        if active is not None:
            active.extend(actions)
            actions.clear()
            return
        active = deque(actions)
        actions.clear()
        self._callback_context.actions = active
        previous = getattr(self._callback_context, "active", False)
        self._callback_context.active = True
        first_error = None
        try:
            while active:
                kind, value, item = active.popleft()
                try:
                    if kind == "release":
                        continue
                    if kind == "watch":
                        # ProcessPool invokes failure callbacks while holding
                        # its own shutdown lock. This callback must never enter
                        # our scheduling lock, submit, shut down, or invoke user
                        # code. At most the bounded in-flight set can enqueue.
                        value.add_done_callback(lambda raw, item=item, queue=self._completions:
                                                queue.put((raw, item)))
                    else:
                        with self._condition:
                            if item.future.cancelled():
                                self._notify_cancel_locked(item)
                                continue
                            if not item.future.running():
                                if not item.future.set_running_or_notify_cancel():
                                    item.cancel_notified = True
                                    continue
                        if kind == "error":
                            item.future.set_exception(value)
                        elif value.cancelled():
                            if not item.future.cancel():
                                item.future.set_exception(CancelledError("backend cancelled committed work"))
                        elif value.exception() is not None:
                            item.future.set_exception(value.exception())
                        else:
                            item.future.set_result(value.result())
                except BaseException as error:
                    # BaseException from a user callback is undefined in the
                    # stdlib contract, but must not strand other accepted work.
                    if first_error is None:
                        first_error = error
            if first_error is not None:
                raise first_error
        finally:
            del self._callback_context.actions
            self._callback_context.active = previous

    def _record(self, future, item):
        duration = time.perf_counter() - item.started
        key, backend = item.hints.key, item.backend
        with self._condition:
            self._pending -= 1
            if future.cancelled():
                self._queued[backend].pop(future, None)
                released = item.clear_payload()
                self._notify_cancel_locked(item)
                self._counts["cancelled"] += 1
                # Dispatch may remove a cancelled item before this callback
                # obtains our lock. If that leaves the pump waiting for a raw
                # completion that will never exist, wake its idle-exit check.
                if not self._pending and not self._retained() and self._completion_active:
                    self._completions.put(None)
            else:
                failed = future.exception() is not None
                self._counts["failed" if failed else "completed"] += 1
                bucket = self._profiles.setdefault(key, {})
                self._profiles.move_to_end(key)
                previous = bucket.get(backend, Profile())
                if failed:
                    bucket[backend] = Profile(previous.samples, previous.ewma_seconds,
                        previous.total_seconds, previous.failures + 1,
                        previous.uncontended_samples, previous.uncontended_ewma_seconds,
                        previous.uncontended_total_seconds)
                else:
                    ewma = duration if not previous.samples else self._alpha * duration + (1-self._alpha) * previous.ewma_seconds
                    samples = previous.uncontended_samples
                    baseline = previous.uncontended_ewma_seconds
                    total = previous.uncontended_total_seconds
                    if item.uncontended:
                        baseline = duration if not samples else self._alpha * duration + (1-self._alpha) * baseline
                        samples += 1
                        total += duration
                    bucket[backend] = Profile(previous.samples + 1, ewma,
                        previous.total_seconds + duration, previous.failures,
                        samples, baseline, total)
                if len(self._profiles) > self._max_profile_keys:
                    self._profiles.popitem(last=False)
                    self._counts["profiles_evicted"] += 1
            self._condition.notify_all()

    def snapshot(self):
        """JSON-safe copy. Latency excludes admission wait, includes pool queueing.

        Future.result may return just before its bookkeeping callback runs.
        shutdown(wait=True) joins workers and establishes final metrics.
        physical.retained counts queued plus dispatched items (including a
        cancelled dispatch until backend acknowledgement), never payload bytes.
        """
        with self._condition:
            return {"counts": dict(self._counts), "pending": self._pending,
                    "load": asdict(self._load_view()),
                    "policy": self._policy.snapshot() if type(self._policy) is AdaptivePolicy else None,
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
                    "physical": {
                        "queued": {b: len(q) for b, q in self._queued.items()},
                        "in_flight": {b: len(q) for b, q in self._in_flight.items()},
                        "retained": self._retained(), "max_retained": self._max_pending,
                        "dispatch_limits": dict(self._worker_limits),
                        "peak_queued": dict(self._peak_queued),
                        "peak_in_flight": dict(self._peak_in_flight),
                        "peak_retained": self._peak_retained},
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
        """Close admission, optionally cancel uncommitted work, and drain pools.

        wait=False still drains accepted queued work unless cancel_futures=True.
        Later calls can escalate cancellation and wait for the same coordinator.
        Running calls are never killed. A waiting shutdown from one of this
        executor's tasks/completion callbacks raises rather than self-deadlocks.
        """
        if wait and getattr(self._callback_context, "active", False):
            raise RuntimeError("shutdown(wait=True) cannot run in this executor's worker or completion callback")
        with self._condition:
            self._closed = True
            self._cancel_requested = self._cancel_requested or cancel_futures
            candidates = []
            if self._cancel_requested:
                for backend in self._queued:
                    candidates.extend(self._queued[backend])
                    candidates.extend(self._in_flight[backend])
            if self._shutdown_thread is None:
                self._shutdown_thread = threading.Thread(target=self._join_pools,
                    name="intelligent-shutdown", daemon=False)
                self._shutdown_thread.start()
            self._condition.notify_all()
            coordinator = self._shutdown_thread
        first_error = None
        for future in candidates:
            try:
                future.cancel()
            except BaseException as error:
                if first_error is None:
                    first_error = error
        if wait:
            coordinator.join()
            if self._shutdown_error is not None:
                raise self._shutdown_error
        if first_error is not None:
            raise first_error

    def _join_pools(self):
        try:
            # Keep both pools open until accepted owned queues have drained.
            # Exactly one waiting close retains ProcessPool's join boundary,
            # including after any earlier nonblocking shutdown request.
            with self._condition:
                while self._retained() or self._pending:
                    self._condition.wait()
                completion_threads = tuple(self._completion_threads)
            # Public callbacks run on the completion pump, not backend workers;
            # joining only the pools would return before those callbacks finish.
            for pump in completion_threads:
                pump.join()
            try:
                self._threads.shutdown(wait=True)
            finally:
                if self._processes is not None:
                    self._processes.shutdown(wait=True)
        except BaseException as error:
            self._shutdown_error = error
