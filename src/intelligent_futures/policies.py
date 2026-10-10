"""Small auditable policies; no introspection of user code or remote inference."""
from dataclasses import dataclass
from typing import Mapping, Protocol


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


Profiles = Mapping[str, Mapping[str, Profile]]


class Policy(Protocol):
    def choose(self, hints: TaskHints, profiles: Profiles, process_available: bool) -> str:
        """Return thread or process. Called under the executor's scheduling lock."""
        ...


class ThreadPolicy:
    def choose(self, hints, profiles, process_available):
        return "thread"


class HintPolicy:
    def choose(self, hints, profiles, process_available):
        return "process" if hints.kind == "cpu" and hints.process_safe and process_available else "thread"


class AdaptivePolicy:
    """Experimental EWMA learner with deterministic exploration.

    Learns completion latency including queueing/serialization, not intrinsic CPU
    cost. Nonstationary loads and noncomparable keys can mislead it. No task is
    replayed; exploration routes future, explicitly process-safe submissions.
    """
    def __init__(self, exploration_interval=8, minimum_samples=2):
        if (isinstance(exploration_interval, bool) or not isinstance(exploration_interval, int)
                or isinstance(minimum_samples, bool) or not isinstance(minimum_samples, int)
                or exploration_interval < 2 or minimum_samples < 1):
            raise ValueError("exploration_interval >= 2 and minimum_samples >= 1 required")
        self.exploration_interval = exploration_interval
        self.minimum_samples = minimum_samples
        self._decisions = 0

    def choose(self, hints, profiles, process_available):
        if not process_available or not hints.process_safe or hints.kind == "io":
            return "thread"
        self._decisions += 1
        bucket = profiles.get(hints.key, {})
        thread = bucket.get("thread", Profile())
        process = bucket.get("process", Profile())
        default = "process" if hints.kind == "cpu" else "thread"
        # Only explore every N eligible decisions to avoid overwhelming cold starts.
        if self._decisions % self.exploration_interval == 0:
            if thread.samples < self.minimum_samples:
                return "thread"
            if process.samples < self.minimum_samples:
                return "process"
            best = "thread" if thread.ewma_seconds <= process.ewma_seconds else "process"
            return "process" if best == "thread" else "thread"
        if min(thread.samples, process.samples) < self.minimum_samples:
            return default
        return "thread" if thread.ewma_seconds <= process.ewma_seconds else "process"


class PolicyRegistry:
    """Per-instance extensible registry. Register factories, not shared state."""
    def __init__(self):
        self._factories = {"threads": ThreadPolicy, "hints": HintPolicy, "adaptive": AdaptivePolicy}

    def register(self, name, factory):
        if not isinstance(name, str) or not name or name in self._factories:
            raise ValueError("policy name must be new and nonempty")
        if not callable(factory):
            raise TypeError("factory must be callable")
        self._factories[name] = factory

    def create(self, name):
        try:
            factory = self._factories[name]
        except KeyError:
            raise ValueError(f"unknown policy: {name}") from None
        return factory()

    def names(self):
        return tuple(sorted(self._factories))
