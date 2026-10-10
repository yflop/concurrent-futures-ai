"""Small auditable policies; no introspection of user code or remote inference."""
from collections import OrderedDict
from dataclasses import dataclass
import math
from threading import RLock
from typing import Mapping, Protocol


def _count(value, name, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


@dataclass(frozen=True)
class TaskHints:
    """Explicit routing consent. process_safe includes pickling and semantic safety.

    key groups comparable work; never put secrets or unbounded identifiers in it.

    >>> TaskHints(kind="io", key="http").process_safe
    False
    >>> HintPolicy().choose(TaskHints(kind="cpu", process_safe=True), {}, True)
    'process'
    """
    kind: str = "unknown"
    key: str = "default"
    process_safe: bool = False

    def __post_init__(self):
        if self.kind not in {"unknown", "io", "cpu"}:
            raise ValueError("kind must be unknown, io, or cpu")
        if not isinstance(self.key, str) or not self.key or len(self.key) > 256:
            raise ValueError("key must be a nonempty string of at most 256 characters")
        if not isinstance(self.process_safe, bool):
            raise TypeError("process_safe must be bool")


@dataclass(frozen=True)
class Profile:
    samples: int = 0
    ewma_seconds: float = 0.0
    total_seconds: float = 0.0
    failures: int = 0
    # Completion measurements admitted while this backend had no outstanding
    # work. They still include cold start, dispatch and serialization overhead.
    uncontended_samples: int = 0
    uncontended_ewma_seconds: float = 0.0
    uncontended_total_seconds: float = 0.0


@dataclass(frozen=True)
class BackendLoad:
    """Owned queued work and occupied dispatch slots, not CPU utilisation.

    in_flight includes a cancelled dispatch until the backend acknowledges it.
    Process worker activity and OS scheduling are not inferred from these counts.
    """
    workers: int
    queued: int = 0
    in_flight: int = 0

    def __post_init__(self):
        for name in ("workers", "queued", "in_flight"):
            _count(getattr(self, name), name)
        if self.in_flight > self.workers:
            raise ValueError("in_flight must not exceed workers")
        if not self.workers and self.queued:
            raise ValueError("a disabled backend cannot have queued work")

    @property
    def outstanding(self):
        return self.queued + self.in_flight

    @property
    def pressure(self):
        return self.outstanding / self.workers if self.workers else 0.0

    @property
    def slot_utilisation(self):
        return self.in_flight / self.workers if self.workers else 0.0


@dataclass(frozen=True)
class SchedulingLoad:
    """Immutable point-in-time scheduler counters captured under its lock."""
    thread: BackendLoad
    process: BackendLoad
    max_pending: int

    def __post_init__(self):
        if not isinstance(self.thread, BackendLoad) or not isinstance(self.process, BackendLoad):
            raise TypeError("thread and process must be BackendLoad instances")
        _count(self.max_pending, "max_pending", 1)
        if not self.thread.workers:
            raise ValueError("at least one thread worker is required")
        if self.thread.outstanding + self.process.outstanding > self.max_pending:
            raise ValueError("outstanding work exceeds max_pending")

    def for_backend(self, backend):
        if backend not in ("thread", "process"):
            raise ValueError("backend must be thread or process")
        return getattr(self, backend)


Profiles = Mapping[str, Mapping[str, Profile]]


class Policy(Protocol):
    def choose(self, hints: TaskHints, profiles: Profiles, process_available: bool) -> str:
        """Return thread or process. Called under the executor's scheduling lock.

        Custom policies retain this three-argument interface. A custom instance
        shared across executors must synchronize its own state, or supply an
        explicit for_executor() factory returning independent scheduling state.
        choose_with_load(..., load) is an optional immutable-load-aware hook.
        """
        ...


class ThreadPolicy:
    def choose(self, hints, profiles, process_available):
        return "thread"


class HintPolicy:
    def choose(self, hints, profiles, process_available):
        return "process" if hints.kind == "cpu" and hints.process_safe and process_available else "thread"


class AdaptivePolicy:
    """Experimental bounded learner with deterministic, per-workload exploration.

    choose() retains the historical completion-EWMA interface. choose_with_load()
    instead uses uncontended completion observations, plus live dispatch-slot and
    queue pressure. Those observations include startup/serialization; multiplying
    them by pressure is a heuristic, never an intrinsic service-time estimate.
    Keys and explicit task kinds must group comparable work. No task is replayed.

    Built-in instances supplied to an executor are configuration templates:
    for_executor() starts a fresh exploration/deadband session, not a transfer of
    earlier decisions. Measurements remain executor-owned. Subclasses retain
    their state unless they explicitly override that hook.
    """
    def __init__(self, exploration_interval=8, minimum_samples=2, *,
                 switch_margin=0.1, max_keys=256):
        _count(exploration_interval, "exploration_interval", 2)
        _count(minimum_samples, "minimum_samples", 1)
        _count(max_keys, "max_keys", 1)
        if isinstance(switch_margin, bool) or not isinstance(switch_margin, (int, float)):
            raise ValueError("switch_margin must be finite and in [0, 1)")
        try:
            switch_margin = float(switch_margin)
        except OverflowError:
            raise ValueError("switch_margin must be finite and in [0, 1)") from None
        if not math.isfinite(switch_margin) or not 0 <= switch_margin < 1:
            raise ValueError("switch_margin must be finite and in [0, 1)")
        self.exploration_interval = exploration_interval
        self.minimum_samples = minimum_samples
        self.switch_margin = float(switch_margin)
        self.max_keys = max_keys
        self._lock = RLock()
        self._states = OrderedDict()
        self._last_decision = None

    def for_executor(self, *, max_keys=None):
        # Do not silently erase a subclass's explicitly supplied custom state.
        if type(self) is not AdaptivePolicy:
            return self
        return AdaptivePolicy(self.exploration_interval, self.minimum_samples,
                              switch_margin=self.switch_margin,
                              max_keys=self.max_keys if max_keys is None else min(self.max_keys, max_keys))

    def snapshot(self):
        """A detached bounded summary, safe alongside concurrent decisions."""
        with self._lock:
            return {"tracked_keys": len(self._states), "max_keys": self.max_keys,
                    "last_decision": None if self._last_decision is None else dict(self._last_decision)}

    def choose(self, hints, profiles, process_available):
        return self._choose(hints, profiles, process_available, None)

    def choose_with_load(self, hints, profiles, process_available, load):
        if not isinstance(load, SchedulingLoad):
            raise TypeError("load must be SchedulingLoad")
        return self._choose(hints, profiles, process_available, load)

    def _choose(self, hints, profiles, process_available, load):
        with self._lock:
            if (not process_available or not hints.process_safe or hints.kind == "io"
                    or (load is not None and not load.process.workers)):
                self._last_decision = {"backend": "thread", "reason": "process-ineligible",
                                       "exploration": False}
                return "thread"
            state_key = (hints.key, hints.kind)
            sequence, incumbent = self._states.get(state_key, (0, None))
            sequence = (sequence + 1) % self.exploration_interval
            bucket = profiles.get(hints.key, {})
            observations = {name: bucket.get(name, Profile()) for name in ("thread", "process")}
            counts = {name: getattr(value, "samples" if load is None else "uncontended_samples")
                      for name, value in observations.items()}
            times = {name: getattr(value, "ewma_seconds" if load is None else "uncontended_ewma_seconds")
                     for name, value in observations.items()}
            default = "process" if hints.kind == "cpu" else "thread"
            reason = "task-hint"
            if min(counts.values()) >= self.minimum_samples:
                scores = {name: seconds * (1 if load is None else 1 + load.for_backend(name).pressure)
                          for name, seconds in times.items()}
                preferred = min(scores, key=lambda name: (scores[name], name != "thread"))
                # A 10% deadband suppresses switches driven by small score noise.
                if (incumbent is not None and scores[preferred] >=
                        scores[incumbent] * (1 - self.switch_margin)):
                    preferred = incumbent
                reason = "completion-ewma" if load is None else "uncontended-load-proxy"
            else:
                preferred = default
                if load is not None:
                    other = "process" if default == "thread" else "thread"
                    primary, alternate = load.for_backend(default), load.for_backend(other)
                    # Without comparable measurements, divert only an established
                    # backlog with a material capacity-normalized pressure gap.
                    if (primary.queued >= primary.workers and
                            primary.pressure - alternate.pressure >= 1 + self.switch_margin):
                        preferred, reason = other, "queue-pressure"
            chosen = preferred
            if sequence == 0:
                if counts["thread"] < self.minimum_samples:
                    candidate = "thread"
                elif counts["process"] < self.minimum_samples:
                    candidate = "process"
                else:
                    candidate = "process" if preferred == "thread" else "thread"
                # Cold exploration should produce an uncontended observation;
                # never add exploratory work to an already queued backend.
                if (load is None or (load.for_backend(candidate).queued == 0 and
                        (min(counts.values()) >= self.minimum_samples or
                         load.for_backend(candidate).outstanding == 0))):
                    chosen = candidate
            self._states[state_key] = (sequence, preferred)
            self._states.move_to_end(state_key)
            if len(self._states) > self.max_keys:
                self._states.popitem(last=False)
            self._last_decision = {"backend": chosen, "reason": reason,
                                   "exploration": chosen != preferred}
            return chosen


class PolicyRegistry:
    """Thread-safe, per-instance registry. Register factories, not shared state."""
    def __init__(self):
        self._factories = {"threads": ThreadPolicy, "hints": HintPolicy, "adaptive": AdaptivePolicy}
        self._lock = RLock()

    def register(self, name, factory):
        if not isinstance(name, str) or not name:
            raise ValueError("policy name must be new and nonempty")
        if not callable(factory):
            raise TypeError("factory must be callable")
        with self._lock:
            if name in self._factories:
                raise ValueError("policy name must be new and nonempty")
            self._factories[name] = factory

    def create(self, name):
        with self._lock:
            try:
                factory = self._factories[name]
            except KeyError:
                raise ValueError(f"unknown policy: {name}") from None
        # User factories run outside the registry lock, with no exception rewriting.
        return factory()

    def names(self):
        with self._lock:
            return tuple(sorted(self._factories))
