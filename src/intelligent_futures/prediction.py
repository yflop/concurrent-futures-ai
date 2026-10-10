"""Opt-in, bounded predictive foundation using explicitly supplied task metadata.

Predictions are conservative heuristics until matching local measurements exist.
A hardware fingerprint is a compatibility hint, never proof of transferability.
No callable introspection, raw inputs, per-task RSS, or remote inference is used.
"""
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from functools import cached_property
import hashlib
import json
import math
import os
from pathlib import Path
import platform
from statistics import mean
from threading import RLock

from .policies import SchedulingLoad

MAX_SAMPLES = 128
MAX_FEATURES = 32
MAX_JSON_BYTES = 262144
MAX_PROFILES = 256


def _integer(value, name, minimum=1, maximum=1048576):
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer in [{minimum}, {maximum}]")
    return value


def _number(value, name, minimum=0.0, maximum=1e15):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite number")
    try:
        value = float(value)
    except (OverflowError, ValueError):
        raise ValueError(f"{name} must be a finite number") from None
    if not math.isfinite(value) or not minimum <= value <= maximum:
        raise ValueError(f"{name} is outside the supported finite range")
    return value


def _key(value, name='key', limit=256):
    if not isinstance(value, str) or not value or len(value) > limit or any(ord(c) < 32 for c in value):
        raise ValueError(f"{name} must be nonempty text of at most {limit} characters without controls")
    return value


def _features(values):
    # A mapping or pairs are accepted, but stored as immutable sorted pairs.
    if hasattr(values, 'items'):
        values = values.items()
    if not hasattr(values, '__len__') or len(values) > MAX_FEATURES:
        raise ValueError(f"features must contain at most {MAX_FEATURES} numeric summaries")
    result = []
    for pair in values:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            raise ValueError('each feature must be a name/value pair')
        result.append((_key(pair[0], 'feature name', 64), _number(pair[1], 'feature value', -1e15)))
    if len({key for key, _ in result}) != len(result):
        raise ValueError('duplicate feature names')
    return tuple(sorted(result))


def normalize_features(features=()):
    """Copy explicit numeric summaries into a bounded immutable representation."""
    return _features(features)


def _read(path):
    try:
        with open(path, encoding='ascii') as stream:
            return stream.read(65536)
    except (OSError, UnicodeError):
        return ''


def _quota():
    """Read accessible cgroup v2/v1 limits, including current-group ancestors.

    Mount namespaces may hide limits; unavailable constraints remain unknown.
    """
    roots = [(Path('/sys/fs/cgroup'), 'v2'),
             (Path('/sys/fs/cgroup/cpu'), 'v1'),
             (Path('/sys/fs/cgroup/cpu,cpuacct'), 'v1')]
    entries = _read('/proc/self/cgroup').splitlines()
    paths = list(roots)
    for entry in entries:
        pieces = entry.split(':', 2)
        if len(pieces) != 3:
            continue
        _, controllers, relative = pieces
        # Reject traversal in kernel-file fixtures as well as unusual inputs.
        relative_path = Path(relative.lstrip('/'))
        if '..' in relative_path.parts:
            continue
        for root, version in roots:
            if (version == 'v2' and controllers == '') or (version == 'v1' and 'cpu' in controllers.split(',')):
                current = root / relative_path
                while current != root and root in current.parents:
                    paths.append((current, version))
                    current = current.parent
    quotas = []
    for path, version in paths:
        try:
            if version == 'v2':
                parts = _read(path / 'cpu.max').split()
                if len(parts) != 2 or parts[0] == 'max':
                    continue
                quota, period = map(int, parts)
            else:
                quota = int(_read(path / 'cpu.cfs_quota_us').strip())
                period = int(_read(path / 'cpu.cfs_period_us').strip())
            if quota > 0 and period > 0:
                limit = quota / period
                if math.isfinite(limit) and limit > 0:
                    quotas.append(limit)
        except (ValueError, OverflowError):
            continue
    return min(quotas) if quotas else None


@dataclass(frozen=True)
class HardwareCapabilities:
    logical_cpus: int = 1
    affinity_cpus: int | None = None
    quota_cpus: float | None = None
    machine: str = field(default_factory=platform.machine)
    system: str = field(default_factory=platform.system)
    cpu_model: str = ''

    def __post_init__(self):
        _integer(self.logical_cpus, 'logical_cpus')
        if self.affinity_cpus is not None:
            _integer(self.affinity_cpus, 'affinity_cpus')
        if self.quota_cpus is not None:
            object.__setattr__(self, 'quota_cpus', _number(self.quota_cpus, 'quota_cpus', 1e-12, 1048576))
        for name in ('machine', 'system', 'cpu_model'):
            value = getattr(self, name)
            if not isinstance(value, str) or len(value) > 256:
                raise ValueError(f'{name} must be bounded text')

    @cached_property
    def usable_cpus(self):
        constraints = [self.logical_cpus]
        if self.affinity_cpus is not None:
            constraints.append(self.affinity_cpus)
        if self.quota_cpus is not None:
            constraints.append(max(1, math.floor(self.quota_cpus)))
        return max(1, min(constraints))

    @cached_property
    def fingerprint(self):
        payload = (self.logical_cpus, self.affinity_cpus, self.quota_cpus, self.machine, self.system, self.cpu_model)
        return hashlib.sha256(json.dumps(payload).encode()).hexdigest()

    @classmethod
    def detect(cls):
        logical = os.cpu_count() or 1
        try:
            affinity = len(os.sched_getaffinity(0)) or None
        except (AttributeError, OSError, NotImplementedError):
            affinity = None
        model = ''
        for line in _read('/proc/cpuinfo').splitlines():
            name, separator, value = line.partition(':')
            if separator and name.strip().lower() == 'model name':
                model = value.strip()
                break
        if not model:
            model = platform.processor() or ''
        # Never collect serial numbers, network addresses, or other device IDs.
        model = ''.join(c for c in model if c.isprintable())[:256]
        return cls(logical_cpus=logical, affinity_cpus=affinity, quota_cpus=_quota(), cpu_model=model)


@dataclass(frozen=True)
class ExecutionObjective:
    mode: str = 'balanced'
    min_concurrency: int = 1
    max_concurrency: int | None = None
    memory_limit_bytes: int | None = None

    def __post_init__(self):
        if self.mode not in ('throughput', 'latency', 'balanced'):
            raise ValueError('mode must be throughput, latency, or balanced')
        _integer(self.min_concurrency, 'min_concurrency')
        if self.max_concurrency is not None:
            _integer(self.max_concurrency, 'max_concurrency', self.min_concurrency)
        if self.memory_limit_bytes is not None:
            raise ValueError('memory limits are unsupported: per-task RSS is not measured')

    def clamp(self, concurrency):
        return max(self.min_concurrency, min(concurrency, self.max_concurrency or 1048576))


@dataclass(frozen=True)
class RuntimeSample:
    wall_seconds: float
    thread_cpu_seconds: float | None = None
    backend: str = 'thread'
    concurrency: int = 1

    def __post_init__(self):
        object.__setattr__(self, 'wall_seconds', _number(self.wall_seconds, 'wall_seconds', 0, 1e9))
        if self.thread_cpu_seconds is not None:
            object.__setattr__(self, 'thread_cpu_seconds', _number(self.thread_cpu_seconds, 'thread_cpu_seconds', 0, 1e9))
        if self.backend not in ('thread', 'process'):
            raise ValueError('backend must be thread or process')
        _integer(self.concurrency, 'concurrency')


@dataclass(frozen=True)
class ScalingBucket:
    backend: str
    concurrency: int
    samples: int
    mean_wall_seconds: float
    mean_thread_cpu_seconds: float | None
    provenance: str = field(default='observational-concurrency-bucket', init=False)

    @property
    def estimated_throughput(self):
        """Concurrency / mean service time proxy, not measured batch throughput."""
        return self.concurrency / self.mean_wall_seconds if self.mean_wall_seconds > 0 else 0.0


@dataclass(frozen=True)
class WorkloadProfile:
    key: str
    features: tuple = ()
    samples: tuple = ()
    hardware_fingerprint: str | None = None
    version: int = 1
    hardware: HardwareCapabilities | None = None
    _stats: tuple = field(default=(), init=False, repr=False, compare=False)
    _curve: tuple = field(default=(), init=False, repr=False, compare=False)

    def __post_init__(self):
        _key(self.key)
        object.__setattr__(self, 'features', _features(self.features))
        if not isinstance(self.samples, (tuple, list)) or len(self.samples) > MAX_SAMPLES:
            raise ValueError(f'at most {MAX_SAMPLES} samples are supported')
        if any(not isinstance(s, RuntimeSample) for s in self.samples):
            raise ValueError('samples must be RuntimeSample instances')
        object.__setattr__(self, 'samples', tuple(self.samples))
        if self.hardware_fingerprint is not None:
            _key(self.hardware_fingerprint, 'hardware_fingerprint', 256)
        if self.hardware is not None:
            if not isinstance(self.hardware, HardwareCapabilities):
                raise ValueError('hardware must be HardwareCapabilities or None')
            if self.hardware_fingerprint is None:
                object.__setattr__(self, 'hardware_fingerprint', self.hardware.fingerprint)
            elif self.hardware_fingerprint != self.hardware.fingerprint:
                raise ValueError('hardware fingerprint does not match source hardware')
        if type(self.version) is not int or self.version != 1:
            raise ValueError('unsupported profile version')
        stats = {}
        for sample in self.samples:
            self._add_stats(stats, sample, 1)
        self._set_stats(stats)

    @staticmethod
    def _add_stats(stats, sample, direction):
        key = (sample.backend, sample.concurrency)
        count, wall, cpu, cpu_count = stats.get(key, (0, 0.0, 0.0, 0))
        count += direction
        wall += direction * sample.wall_seconds
        if sample.thread_cpu_seconds is not None:
            cpu += direction * sample.thread_cpu_seconds
            cpu_count += direction
        if count:
            stats[key] = (count, max(0.0, wall), max(0.0, cpu), cpu_count)
        else:
            stats.pop(key, None)

    def _set_stats(self, stats):
        values = tuple(sorted(stats.items()))
        object.__setattr__(self, '_stats', values)
        object.__setattr__(self, '_curve', tuple(
            ScalingBucket(backend, concurrency, count, wall / count,
                          cpu / cpu_count if cpu_count else None)
            for (backend, concurrency), (count, wall, cpu, cpu_count) in values))

    def with_sample(self, sample):
        if not isinstance(sample, RuntimeSample):
            raise ValueError('sample must be a RuntimeSample')
        # Existing state was already validated and is immutable. Copy it without
        # revalidating every retained sample, and update bounded cached aggregates.
        result = object.__new__(WorkloadProfile)
        for name in ('key', 'features', 'hardware_fingerprint', 'version', 'hardware'):
            object.__setattr__(result, name, getattr(self, name))
        object.__setattr__(result, 'samples', (self.samples + (sample,))[-MAX_SAMPLES:])
        stats = dict(self._stats)
        if len(self.samples) == MAX_SAMPLES:
            evicted = self.samples[0]
            evicted_key = (evicted.backend, evicted.concurrency)
            _, old_wall, old_cpu, _ = stats[evicted_key]
            self._add_stats(stats, evicted, -1)
            # Subtracting a dominant observation can erase tiny retained values.
            # Rebase that bucket with fsum; ordinary steady-state updates stay O(1).
            if ((old_wall > 0 and evicted.wall_seconds >= .5 * old_wall) or
                    (old_cpu > 0 and evicted.thread_cpu_seconds is not None and
                     evicted.thread_cpu_seconds >= .5 * old_cpu)):
                retained = [s for s in self.samples[1:]
                            if (s.backend, s.concurrency) == evicted_key]
                if retained:
                    cpu_values = [s.thread_cpu_seconds for s in retained
                                  if s.thread_cpu_seconds is not None]
                    stats[evicted_key] = (len(retained), math.fsum(s.wall_seconds for s in retained),
                                          math.fsum(cpu_values), len(cpu_values))
        self._add_stats(stats, sample, 1)
        result._set_stats(stats)
        return result

    def scaling_curve(self):
        """Cached observed active-concurrency buckets, not controlled scaling trials."""
        return self._curve

    def to_json(self):
        data = {'version': self.version, 'key': self.key, 'features': self.features,
                'hardware_fingerprint': self.hardware_fingerprint,
                'hardware': asdict(self.hardware) if self.hardware is not None else None,
                'samples': [{'wall_seconds': s.wall_seconds, 'thread_cpu_seconds': s.thread_cpu_seconds,
                             'backend': s.backend, 'concurrency': s.concurrency} for s in self.samples]}
        result = json.dumps(data, allow_nan=False, separators=(',', ':'), ensure_ascii=True)
        if len(result.encode('utf-8')) > MAX_JSON_BYTES:
            raise ValueError('profile exceeds JSON size limit')
        return result

    @classmethod
    def from_json(cls, payload):
        if not isinstance(payload, (str, bytes)) or len(payload) > MAX_JSON_BYTES:
            raise ValueError('profile JSON must be bounded text or bytes')
        if isinstance(payload, str):
            try:
                encoded_size = len(payload.encode('utf-8'))
            except UnicodeError as exc:
                raise ValueError('invalid Unicode in profile JSON') from exc
            if encoded_size > MAX_JSON_BYTES:
                raise ValueError('profile exceeds JSON size limit')
        def unique(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError('duplicate JSON object key')
                result[key] = value
            return result
        def reject_constant(value):
            raise ValueError('nonfinite JSON number')
        try:
            data = json.loads(payload, object_pairs_hook=unique, parse_constant=reject_constant)
            expected = {'version', 'key', 'features', 'hardware_fingerprint', 'hardware', 'samples'}
            if not isinstance(data, dict) or set(data) != expected:
                raise ValueError('invalid profile schema')
            if not isinstance(data['samples'], list) or len(data['samples']) > MAX_SAMPLES:
                raise ValueError('invalid samples')
            hardware = data['hardware']
            if hardware is not None:
                if not isinstance(hardware, dict) or set(hardware) != {'logical_cpus', 'affinity_cpus', 'quota_cpus', 'machine', 'system', 'cpu_model'}:
                    raise ValueError('invalid source hardware schema')
                hardware = HardwareCapabilities(**hardware)
            samples = []
            for item in data['samples']:
                if not isinstance(item, dict) or set(item) != {'wall_seconds', 'thread_cpu_seconds', 'backend', 'concurrency'}:
                    raise ValueError('invalid sample schema')
                samples.append(RuntimeSample(**item))
            return cls(key=data['key'], features=data['features'], samples=tuple(samples),
                       hardware_fingerprint=data['hardware_fingerprint'], version=data['version'], hardware=hardware)
        except (TypeError, UnicodeError, RecursionError, OverflowError) as exc:
            raise ValueError('invalid profile JSON') from exc


@dataclass(frozen=True)
class Prediction:
    backend: str
    concurrency: int
    source: str
    transfer: str = 'unknown'
    expected_wall_seconds: float | None = None
    confidence: str = field(init=False)
    load_adjusted: bool = False
    stabilized: bool = False

    def __post_init__(self):
        # Qualitative evidence labels, not calibrated statistical confidence.
        labels = {'heuristic': 'heuristic', 'measurement-guided': 'heuristic',
                  'empirical': 'observational', 'transfer-prior': 'unvalidated'}
        if self.source not in labels:
            raise ValueError('unknown prediction source')
        object.__setattr__(self, 'confidence', labels[self.source])


class Predictor:
    """Bounded nearest-*exact*-profile selector; never claims unmeasured transfer.

    Single-concurrency CPU/wall ratios may guide heuristic budgets; they do not
    identify the GIL, native code, or predict parallel speedup. At least two
    observed concurrency buckets are needed for empirical non-latency ranking.
    Objectives rank observed service-time buckets. Throughput uses the labeled
    concurrency/service-time proxy. This is not an optimizer or speedup promise.
    Foreign profiles with source capabilities supply unvalidated conservative
    priors, never transferred latency or speedup claims. Local evidence supersedes
    those priors. Imported profiles are advice, not execution authority.
    """
    def __init__(self, hardware=None, objective=None, profiles=(), max_profiles=MAX_PROFILES, *,
                 switch_margin=0.1):
        self.hardware = hardware if hardware is not None else HardwareCapabilities.detect()
        self.objective = objective if objective is not None else ExecutionObjective()
        if not isinstance(self.hardware, HardwareCapabilities) or not isinstance(self.objective, ExecutionObjective):
            raise ValueError('hardware and objective must use their validated types')
        self.max_profiles = _integer(max_profiles, 'max_profiles', 1, MAX_PROFILES)
        self.switch_margin = _number(switch_margin, 'switch_margin', 0, .99)
        self._profiles = OrderedDict()
        self._choices = OrderedDict()
        self._lock = RLock()
        if not isinstance(profiles, (list, tuple)) or len(profiles) > self.max_profiles:
            raise ValueError('profiles must be a bounded list or tuple')
        for profile in profiles:
            if not isinstance(profile, WorkloadProfile):
                raise ValueError('profiles must be WorkloadProfile instances')
            self._profiles[(profile.key, profile.features, profile.hardware_fingerprint)] = profile

    @property
    def profiles(self):
        with self._lock:
            return tuple(self._profiles.values())

    def record(self, key, sample, features=()):
        profile_key = (_key(key), _features(features), self.hardware.fingerprint)
        with self._lock:
            profile = self._profiles.get(profile_key)
            if profile is None:
                profile = WorkloadProfile(key, profile_key[1], hardware=self.hardware)
            profile = profile.with_sample(sample)
            if profile_key not in self._profiles and len(self._profiles) >= self.max_profiles:
                self._profiles.popitem(last=False)
            self._profiles[profile_key] = profile
            self._profiles.move_to_end(profile_key)
            return profile

    def recommend(self, key, features=(), kind='unknown', process_safe=False, process_available=False, *, load=None, backend=None):
        """Select observed service-time evidence with optional live demand controls.

        Load is dispatch-slot occupancy, not measured CPU utilisation. Queue-scaled
        ranking remains a heuristic; expected_wall_seconds, when supplied, is the
        observed service time only, never a forecast of queueing or batch speedup.
        A bounded per-workload deadband suppresses small load-aware ranking flips.
        backend optionally fixes the route chosen by an explicit executor policy;
        evidence, demand and capacity then all refer to that actual backend.
        This override never grants process consent or backend availability.
        """
        if load is not None and not isinstance(load, SchedulingLoad):
            raise TypeError('load must be SchedulingLoad or None')
        if backend not in (None, 'thread', 'process'):
            raise ValueError('backend must be thread, process or None')
        key, features = _key(key), _features(features)
        with self._lock:
            result = self._recommend(key, features, kind, process_safe, process_available, load, backend)
            if load is None:
                return result
            backend_load = load.for_backend(result.backend)
            capacity = min(backend_load.workers, load.max_pending)
            if capacity < self.objective.min_concurrency:
                raise ValueError('load capacity is below min_concurrency')
            # A sparse workload need not reserve its full observed budget. The
            # next submission sees updated demand and can grow the limit again.
            concurrency = min(result.concurrency, capacity,
                              max(self.objective.min_concurrency, backend_load.outstanding + 1))
            changed = concurrency != result.concurrency
            source = 'measurement-guided' if changed and result.source == 'empirical' else result.source
            return Prediction(result.backend, concurrency, source, result.transfer,
                              None if changed else result.expected_wall_seconds,
                              load_adjusted=True, stabilized=result.stabilized)

    def _recommend(self, key, features, kind, process_safe, process_available, load, backend):
        if kind not in ('unknown', 'io', 'cpu'):
            raise ValueError('kind must be unknown, io, or cpu')
        if type(process_safe) is not bool or type(process_available) is not bool:
            raise ValueError('process safety and availability must be booleans')
        process_allowed = (process_safe and process_available and (kind != 'io' or backend == 'process')
                           and (load is None or load.process.workers > 0))
        if backend == 'process' and not process_allowed:
            raise ValueError('process override requires process consent and availability')
        default_backend = backend or ('process' if kind == 'cpu' and process_allowed else 'thread')
        default_concurrency = self.objective.clamp(self.hardware.usable_cpus if kind == 'cpu' else min(32, self.hardware.usable_cpus + 4))
        with self._lock:
            profile_key = (key, features, self.hardware.fingerprint)
            profile = self._profiles.get(profile_key)
            if profile is not None:
                self._profiles.move_to_end(profile_key)
            foreign = tuple(p for p in self._profiles.values()
                            if p.key == key and p.features == features
                            and p.hardware is not None
                            and p.hardware_fingerprint != self.hardware.fingerprint)
        buckets = [] if profile is None else [b for b in profile.scaling_curve()
            if b.samples >= 2 and b.mean_wall_seconds > 0
            and (b.backend == 'thread' or process_allowed)
            and (backend is None or b.backend == backend)
            and self.objective.clamp(b.concurrency) == b.concurrency]
        if not buckets:
            # A single local CPU/wall observation is weak but more relevant than
            # foreign measurements; keep its extrapolation explicitly heuristic.
            local = [] if profile is None else [s for s in profile.samples
                if s.backend == 'thread' and s.wall_seconds > 0 and s.thread_cpu_seconds is not None]
            if local:
                ratio = sum(s.thread_cpu_seconds for s in local) / sum(s.wall_seconds for s in local)
                budget = self.hardware.usable_cpus if ratio >= .8 else default_concurrency
                if ratio <= .2:
                    budget = min(32, self.hardware.usable_cpus + 4)
                if self.objective.mode == 'latency':
                    budget = 1
                return Prediction(default_backend, self.objective.clamp(budget),
                                  'measurement-guided', 'fingerprint-match')
            if profile is None or not profile.samples:
                prior = self._foreign_prior(foreign, default_backend)
                if prior is not None:
                    return prior
            return Prediction(default_backend, default_concurrency, 'heuristic')
        if self.objective.mode != 'latency' and len({b.concurrency for b in buckets}) < 2:
            # Do not permanently cap future bursts at one after sequential work.
            # The ratio measures this thread's CPU usage, not GIL/native behavior.
            measured = [b for b in buckets if b.mean_thread_cpu_seconds is not None]
            if measured:
                observed = min(measured, key=lambda b: (b.mean_wall_seconds, b.backend))
                ratio = observed.mean_thread_cpu_seconds / observed.mean_wall_seconds
                if ratio >= .8:
                    budget = self.hardware.usable_cpus
                elif ratio <= .2:
                    budget = min(32, self.hardware.usable_cpus + 4)
                else:
                    budget = default_concurrency
                return Prediction(observed.backend, self.objective.clamp(budget),
                                  'measurement-guided', 'fingerprint-match')
            return Prediction(default_backend, default_concurrency, 'heuristic')
        fastest = min(buckets, key=lambda b: (b.mean_wall_seconds, b.concurrency, b.backend))
        candidates = buckets
        if self.objective.mode == 'balanced':
            # Balance throughput against service latency, excluding >25% slower
            # observations. Queue pressure never becomes a stored runtime sample.
            candidates = [b for b in buckets if b.mean_wall_seconds <= 1.25 * fastest.mean_wall_seconds]

        def score(bucket):
            if self.objective.mode == 'latency':
                pressure = 0 if load is None else load.for_backend(bucket.backend).pressure
                return 1 / (bucket.mean_wall_seconds * (1 + pressure))
            concurrency = bucket.concurrency
            if load is not None:
                backend_load = load.for_backend(bucket.backend)
                concurrency = min(concurrency, backend_load.workers, backend_load.outstanding + 1)
            return concurrency / bucket.mean_wall_seconds

        best = max(candidates, key=lambda b: (score(b), -b.concurrency, b.backend == 'thread'))
        stabilized = False
        if load is not None:
            choice_key = (key, features, kind, process_allowed, backend)
            previous = self._choices.get(choice_key)
            incumbent = next((b for b in candidates if (b.backend, b.concurrency) == previous), None)
            if incumbent is not None and score(best) <= score(incumbent) * (1 + self.switch_margin):
                stabilized = best != incumbent
                best = incumbent
            self._choices[choice_key] = (best.backend, best.concurrency)
            self._choices.move_to_end(choice_key)
            if len(self._choices) > self.max_profiles:
                self._choices.popitem(last=False)
        return Prediction(best.backend, best.concurrency, 'empirical', 'fingerprint-match',
                          best.mean_wall_seconds, stabilized=stabilized)

    def _foreign_prior(self, profiles, backend):
        """A bounded, unvalidated budget prior; source times are never predicted."""
        candidates = []
        for profile in profiles:
            samples = [s for s in profile.samples if s.backend == 'thread'
                       and s.wall_seconds > 0 and s.thread_cpu_seconds is not None]
            if len(samples) >= 2:
                candidates.append((len(samples), profile.hardware_fingerprint, profile, samples))
        if not candidates:
            return None
        _, _, profile, samples = max(candidates, key=lambda item: item[:2])
        ratio = sum(s.thread_cpu_seconds for s in samples) / sum(s.wall_seconds for s in samples)
        source_concurrency = max(1, math.floor(mean(s.concurrency for s in samples)))
        scale = min(1.0, self.hardware.usable_cpus / profile.hardware.usable_cpus)
        observed_budget = max(1, math.floor(source_concurrency * scale))
        if ratio >= .8:
            budget = min(self.hardware.usable_cpus, observed_budget)
        elif ratio <= .2:
            waiting_budget = math.ceil(1.0 / max(.1, ratio))
            budget = min(32, self.hardware.usable_cpus + 4, observed_budget, waiting_budget)
        else:
            budget = min(self.hardware.usable_cpus, observed_budget)
        if self.objective.mode == 'latency':
            budget = 1
        return Prediction(backend, self.objective.clamp(budget), 'transfer-prior', 'unvalidated')
