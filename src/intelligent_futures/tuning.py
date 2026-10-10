"""Explicit, bounded offline calibration. Never discovers or replays user tasks."""
from __future__ import annotations

from concurrent.futures import CancelledError
from dataclasses import asdict, dataclass, field
import math
import random
import time
from typing import Callable, Iterable

from .prediction import ExecutionObjective, HardwareCapabilities

MAX_CANDIDATES = 128
MAX_REPEATS = 32

# Compatibility identities are independent of the package release number. Bump
# the scheduler revision for material queue, dispatch, cancellation, routing, or
# admission changes; bump the strategy revision when Configuration's constructor
# mapping or batching/profiling assumptions change. Never reuse a retired token.
SCHEDULER_REVISION = "owned-removable-queue-physical-dispatch-v1"
STRATEGY_REVISION = "configuration-executor-kwargs-v1"
SAVED_CONFIGURATION_SCHEMA_VERSION = 2


def _package_version():
    # Read this installed/source package, not distribution metadata that could
    # belong to a different checkout. This import is never derived from payloads.
    from . import __version__
    return __version__


def _integer(value, name, minimum=1, maximum=1_000_000):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer in [{minimum}, {maximum}]")


def _number(value, name, *, positive=False):
    try:
        valid = type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        valid = False
    if not valid or value < 0 or (positive and value == 0):
        raise ValueError(f"{name} must be a finite {'positive' if positive else 'nonnegative'} number")


@dataclass(frozen=True, order=True)
class Configuration:
    backend: str
    workers: int
    batch_size: int
    profile_sample_interval: int = 0

    def __post_init__(self):
        if self.backend not in ("thread", "process"):
            raise ValueError("backend must be thread or process")
        _integer(self.workers, "workers", maximum=4096)
        _integer(self.batch_size, "batch_size")
        _integer(self.profile_sample_interval, "profile_sample_interval", minimum=0)

    def executor_kwargs(self, *, allow_process=False):
        """Constructor options only; batching and process-safe hints remain explicit."""
        _consent((self,), allow_process)
        return dict(
            max_workers=self.workers,
            process_workers=self.workers if self.backend == "process" else 0,
            max_pending=2 * self.workers, policy="hints",
            predictive=self.profile_sample_interval > 0,
            profile_sample_interval=max(1, self.profile_sample_interval),
            objective=ExecutionObjective(mode="throughput", max_concurrency=self.workers)
            if self.profile_sample_interval else None,
        )


@dataclass(frozen=True)
class Measurement:
    """Caller-measured whole-fixture timings, including exactly the documented scope.

    Cold includes pool/setup costs; warm excludes setup. Resource values and the
    optional caller-measured batch p95 must use documented, consistent fixture
    scopes across candidates. Missing resources/latency never pass their caps.
    """
    cold_seconds: float
    warm_seconds: float
    correct: bool
    error_count: int = 0
    cpu_seconds: float | None = None
    peak_rss_bytes: int | None = None
    batch_p95_seconds: float | None = None

    def __post_init__(self):
        _number(self.cold_seconds, "cold_seconds", positive=True)
        _number(self.warm_seconds, "warm_seconds", positive=True)
        if type(self.correct) is not bool:
            raise ValueError("correct must be bool")
        _integer(self.error_count, "error_count", minimum=0)
        if self.cpu_seconds is not None:
            _number(self.cpu_seconds, "cpu_seconds")
        if self.peak_rss_bytes is not None:
            _integer(self.peak_rss_bytes, "peak_rss_bytes", minimum=0, maximum=2**63 - 1)
        if self.batch_p95_seconds is not None:
            _number(self.batch_p95_seconds, "batch_p95_seconds")


@dataclass(frozen=True)
class CalibrationObjective:
    elapsed: str = "cold"
    max_cpu_seconds: float | None = None
    max_peak_rss_bytes: int | None = None
    max_batch_p95_seconds: float | None = None

    def __post_init__(self):
        if self.elapsed not in ("cold", "warm", "combined"):
            raise ValueError("elapsed must be cold, warm, or combined")
        if self.max_cpu_seconds is not None:
            _number(self.max_cpu_seconds, "max_cpu_seconds")
        if self.max_peak_rss_bytes is not None:
            _integer(self.max_peak_rss_bytes, "max_peak_rss_bytes", minimum=0, maximum=2**63 - 1)
        if self.max_batch_p95_seconds is not None:
            _number(self.max_batch_p95_seconds, "max_batch_p95_seconds")

    def score(self, measurement):
        if self.elapsed == "combined":
            return measurement.cold_seconds + measurement.warm_seconds
        return getattr(measurement, self.elapsed + "_seconds")

    def exclusion(self, measurement):
        if not measurement.correct:
            return "incorrect"
        if measurement.error_count:
            return "task_errors"
        if self.max_cpu_seconds is not None:
            if measurement.cpu_seconds is None:
                return "cpu_unavailable"
            if measurement.cpu_seconds > self.max_cpu_seconds:
                return "cpu_cap"
        if self.max_peak_rss_bytes is not None:
            if measurement.peak_rss_bytes is None:
                return "rss_unavailable"
            if measurement.peak_rss_bytes > self.max_peak_rss_bytes:
                return "rss_cap"
        if self.max_batch_p95_seconds is not None:
            if measurement.batch_p95_seconds is None:
                return "batch_p95_unavailable"
            if measurement.batch_p95_seconds > self.max_batch_p95_seconds:
                return "batch_p95_cap"
        if not math.isfinite(self.score(measurement)):
            return "elapsed_overflow"
        return None


@dataclass(frozen=True)
class EvaluationRecord:
    configuration: Configuration
    repeat: int
    seed: int
    measurement: Measurement | None
    exclusion: str | None
    callback_seconds: float


@dataclass(frozen=True)
class CandidateResult:
    configuration: Configuration
    score_seconds: float | None
    callback_seconds: float


@dataclass(frozen=True)
class ConvergencePoint:
    evaluations: int
    best_configuration: Configuration | None
    best_score_seconds: float | None
    relative_gap_to_best_observed: float | None
    within_ten_percent: bool


@dataclass(frozen=True)
class CalibrationReport:
    selected: Configuration | None
    objective: CalibrationObjective
    records: tuple[EvaluationRecord, ...]
    candidates: tuple[CandidateResult, ...]
    trace: tuple[ConvergencePoint, ...]
    seed: int
    repeats: int
    allow_process: bool
    hardware: HardwareCapabilities
    elapsed_seconds: float
    evaluations_to_stable_near_best: int | None
    provenance: str = "caller-owned-offline-calibration-v1"
    scheduler_revision: str = field(default_factory=lambda: SCHEDULER_REVISION)
    strategy_revision: str = field(default_factory=lambda: STRATEGY_REVISION)
    package_version: str = field(default_factory=_package_version)

    def to_dict(self):
        """Independent JSON-compatible report; no executable profile loading."""
        result = asdict(self)
        result["hardware_fingerprint"] = self.hardware.fingerprint
        result["convergence_reference"] = "best eligible mean observed in this calibration only"
        return result


Evaluator = Callable[[Configuration, int], Measurement]


def fixture_seeds(*, seed: int, repeats: int) -> tuple[int, ...]:
    """Deterministic common fixture seeds, independent of candidate order.

    Both calibration APIs use this exact sequence. It is also useful for an
    explicitly requested replay of a previously measured fixture matrix. It does
    not run callbacks, establish independence, or authorize any task replay.
    """
    _integer(seed, "seed", minimum=0, maximum=2**63 - 1)
    _integer(repeats, "repeats", maximum=MAX_REPEATS)
    rng = random.Random(seed)
    return tuple(rng.randrange(2**63) for _ in range(repeats))


def _consent(configurations, allow_process):
    if type(allow_process) is not bool:
        raise ValueError("allow_process must be bool")
    if not allow_process and any(c.backend == "process" for c in configurations):
        raise ValueError("process candidates require allow_process=True")


def _record(evaluate, config, repeat, seed, objective):
    started = time.perf_counter()
    measurement = None
    try:
        measurement = evaluate(config, seed)
        if type(measurement) is not Measurement:
            raise ValueError("evaluate must return Measurement")
        # Revalidate even if a caller bypassed the frozen dataclass constructor.
        measurement.__post_init__()
        exclusion = objective.exclusion(measurement)
    except CancelledError:
        raise  # Cancellation/interrupts abort; never become a ranking observation.
    except Exception:
        measurement = None
        exclusion = "callback_error_or_invalid_measurement"
    return EvaluationRecord(config, repeat, seed, measurement, exclusion,
                            time.perf_counter() - started)


def _records(evaluate, config, repeats, seeds, objective):
    for repeat in range(repeats):
        yield _record(evaluate, config, repeat, seeds[repeat], objective)


def calibrate(
    candidates: Iterable[Configuration], evaluate: Evaluator, *,
    repeats: int, seed: int, objective: CalibrationObjective | None = None,
    allow_process: bool = False,
) -> CalibrationReport:
    """Evaluate only the explicitly supplied callback, once per candidate/repeat.

    Callers own idempotence, fixture reset, process safety, and measurements. All
    repeats must pass correctness/resource checks for a candidate to be eligible.
    Rank by mean elapsed; ties use Configuration ordering, never evaluation order.
    Resource caps filter observations; they do not enforce runtime limits.
    """
    started = time.perf_counter()
    _integer(repeats, "repeats", maximum=MAX_REPEATS)
    _integer(seed, "seed", minimum=0, maximum=2**63 - 1)
    if not callable(evaluate):
        raise ValueError("evaluate must be callable")
    if objective is None:
        objective = CalibrationObjective()
    if type(objective) is not CalibrationObjective:
        raise ValueError("objective must be CalibrationObjective")
    objective.__post_init__()
    configurations = []
    for candidate in candidates:
        if len(configurations) == MAX_CANDIDATES:
            raise ValueError(f"at most {MAX_CANDIDATES} candidates")
        if type(candidate) is not Configuration:
            raise ValueError("candidates must contain Configuration instances")
        candidate.__post_init__()
        if candidate in configurations:
            raise ValueError("duplicate candidate")
        configurations.append(candidate)
    if not configurations:
        raise ValueError("at least one candidate is required")
    _consent(configurations, allow_process)
    hardware = HardwareCapabilities.detect()
    rng = random.Random(seed)
    configurations.sort()
    rng.shuffle(configurations)
    # Equal fixture seeds per repeat across configurations; separate from shuffle.
    seeds = fixture_seeds(seed=seed, repeats=repeats)
    records, results, prefixes = [], [], []
    best = None
    for config in configurations:
        observations = tuple(_records(evaluate, config, repeats, seeds, objective))
        records.extend(observations)
        score = None
        if all(record.exclusion is None for record in observations):
            values = [objective.score(record.measurement) for record in observations]
            # Scale first to avoid overflow and subnormal underflow.
            scale = max(values)
            score = scale * (math.fsum(value / scale for value in values) / repeats)
        results.append(CandidateResult(config, score,
                                       sum(record.callback_seconds for record in observations)))
        if score is not None and (best is None or (score, config) < best):
            best = (score, config)
        prefixes.append((len(records), best))
    trace = tuple(ConvergencePoint(
        count, prefix[1] if prefix else None, prefix[0] if prefix else None,
        _gap(prefix[0], best[0]) if prefix and best else None,
        bool(prefix and best and prefix[0] / best[0] <= 1.1),
    ) for count, prefix in prefixes)
    near = next((point.evaluations for point in trace if point.within_ten_percent), None)
    return CalibrationReport(
        best[1] if best else None, objective, tuple(records), tuple(results), trace,
        seed, repeats, allow_process, hardware, time.perf_counter() - started, near,
    )


@dataclass(frozen=True)
class TuningCandidateResult:
    """An observed score is not eligibility for final selection.

    ``evaluated`` candidates form the final comparable cohort. ``eliminated``
    candidates passed their observed gates but lost a halving round; their fewer
    observations cannot be compared fairly with the final cohort. ``excluded``
    candidates failed a correctness, error, measurement, or resource gate.
    """
    configuration: Configuration
    status: str
    evaluations: int
    score_seconds: float | None
    mad_seconds: float | None
    minimum_seconds: float | None
    maximum_seconds: float | None
    callback_seconds: float
    exclusion_reasons: tuple[str, ...]
    eliminated_after_repeat: int | None


@dataclass(frozen=True)
class TuningConvergencePoint:
    """Observed active-cohort leader after a complete common-seed repeat.

    Scores may rise as evidence accumulates. Stable leaders are descriptive,
    not proof of convergence to an optimum.
    """
    evaluations: int
    repeats: int
    active_candidates: tuple[Configuration, ...]
    eliminated: tuple[Configuration, ...]
    excluded: tuple[Configuration, ...]
    best_configuration: Configuration | None
    best_score_seconds: float | None


@dataclass(frozen=True)
class TuningUncertainty:
    """Descriptive comparisons only; no confidence level or success probability."""
    runner_up: Configuration | None
    relative_margin: float | None
    paired_repeats: int
    selected_faster_repeats: int
    tied_repeats: int
    observed_ranges_overlap: bool | None
    lower_scoring_eliminated: tuple[Configuration, ...]
    notes: tuple[str, ...]


@dataclass(frozen=True)
class TuningReport:
    selected: Configuration | None
    objective: CalibrationObjective
    records: tuple[EvaluationRecord, ...]
    candidates: tuple[TuningCandidateResult, ...]
    trace: tuple[TuningConvergencePoint, ...]
    uncertainty: TuningUncertainty
    seed: int
    min_repeats: int
    max_repeats: int
    reduction_factor: int
    max_evaluations: int
    allow_process: bool
    hardware: HardwareCapabilities
    elapsed_seconds: float
    stopping_reason: str
    provenance: str = "caller-owned-budgeted-tuning-v1"
    scheduler_revision: str = field(default_factory=lambda: SCHEDULER_REVISION)
    strategy_revision: str = field(default_factory=lambda: STRATEGY_REVISION)
    package_version: str = field(default_factory=_package_version)

    @property
    def evaluations(self):
        return len(self.records)

    @property
    def unused_evaluations(self):
        return self.max_evaluations - self.evaluations

    @property
    def callback_seconds(self):
        return sum(record.callback_seconds for record in self.records)

    @property
    def selection_changes(self):
        return sum(a.best_configuration != b.best_configuration
                   for a, b in zip(self.trace, self.trace[1:]))

    @property
    def stable_selection_rounds(self):
        if self.selected is None:
            return 0
        count = 0
        for point in reversed(self.trace):
            if point.best_configuration != self.selected:
                break
            count += 1
        return count

    def to_dict(self):
        result = asdict(self)
        result.update(
            hardware_fingerprint=self.hardware.fingerprint,
            evaluations=self.evaluations,
            unused_evaluations=self.unused_evaluations,
            callback_seconds=self.callback_seconds,
            selection_changes=self.selection_changes,
            stable_selection_rounds=self.stable_selection_rounds,
            algorithm="successive-halving-median-v1",
            ranking_statistic="median elapsed seconds",
            selection_scope="final active cohort with equal common-seed repeat counts",
            budget_scope="evaluator calls; not a wall-clock or resource limit",
            convergence_reference="observed active-cohort leaders only; no optimum guarantee",
        )
        return result


def _median(values):
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    # Avoid overflowing the sum of two large finite nonnegative values.
    low, high = ordered[middle - 1:middle + 1]
    return low + (high - low) / 2


def _tuning_result(config, observations, objective, eliminated):
    reasons = tuple(sorted({r.exclusion for r in observations if r.exclusion is not None}))
    values = [objective.score(r.measurement) for r in observations if r.exclusion is None]
    score = _median(values) if values and not reasons else None
    return TuningCandidateResult(
        config, "excluded" if reasons else "eliminated" if config in eliminated else "evaluated",
        len(observations), score,
        _median([abs(value - score) for value in values]) if score is not None else None,
        min(values) if score is not None else None,
        max(values) if score is not None else None,
        sum(r.callback_seconds for r in observations), reasons, eliminated.get(config),
    )


def _tuning_uncertainty(results, observations, objective):
    finalists = sorted((r for r in results if r.status == "evaluated"),
                       key=lambda r: (r.score_seconds, r.configuration))
    notes = ["Descriptive observations only; no statistical confidence or global-optimum guarantee."]
    if any(r.status == "eliminated" for r in results):
        notes.append("Early elimination may discard the best configuration; eliminated candidates were not reraced.")
    if not finalists:
        notes.append("No eligible candidate remains in the final cohort.")
        return TuningUncertainty(None, None, 0, 0, 0, None, (), tuple(notes))
    selected = finalists[0]
    lower = tuple(r.configuration for r in results
                  if r.status == "eliminated" and r.score_seconds < selected.score_seconds)
    if lower:
        notes.append("Some eliminated candidates have lower partial medians; unequal repeat counts prevent final comparison.")
    if selected.evaluations < 3:
        notes.append("Fewer than three repeats provide little information about timing variability.")
    if len(finalists) == 1:
        notes.append("Only one final candidate remains; there is no final-cohort runner-up comparison.")
        return TuningUncertainty(None, None, 0, 0, 0, None, lower, tuple(notes))
    runner = finalists[1]
    left = [objective.score(r.measurement) for r in observations[selected.configuration]]
    right = [objective.score(r.measurement) for r in observations[runner.configuration]]
    overlap = max(min(left), min(right)) <= min(max(left), max(right))
    if overlap:
        notes.append("The selected and runner-up observed timing ranges overlap.")
    if selected.score_seconds == runner.score_seconds:
        notes.append("Median scores tie; Configuration ordering breaks the tie deterministically.")
    return TuningUncertainty(
        runner.configuration, _gap(runner.score_seconds, selected.score_seconds),
        len(left), sum(a < b for a, b in zip(left, right)),
        sum(a == b for a, b in zip(left, right)), overlap, lower, tuple(notes),
    )


def tune(
    candidates: Iterable[Configuration], evaluate: Evaluator, *,
    max_evaluations: int, seed: int, min_repeats: int = 3,
    max_repeats: int = MAX_REPEATS, reduction_factor: int = 2,
    objective: CalibrationObjective | None = None, allow_process: bool = False,
) -> TuningReport:
    """Select a tested configuration by budgeted, median-ranked successive halving.

    Only the explicitly supplied evaluator runs. Callers own fixture reset,
    idempotence, process safety, and consistently scoped measurements. The budget
    limits callback invocations, not their duration; resource/latency caps filter
    measurements and do not enforce runtime limits. Exceptions/invalid results
    exclude a candidate permanently; cancellation and interrupts propagate.

    The budget must cover ``len(candidates) * min_repeats``. Every surviving
    candidate receives common fixture seeds in complete, shuffled-order repeats.
    After min_repeats, retain ceil(active/reduction_factor) candidates and multiply
    the target repeat count by reduction_factor. Continue until the call budget
    cannot fund another complete repeat or max_repeats is reached. Leftover calls
    are deliberately unused rather than introducing unequal final sample counts.

    Rank the final comparable active cohort by median elapsed, then Configuration
    ordering. Eliminated candidates remain visible but cannot win, even if their
    partial medians look better. This is a heuristic search of the explicit set,
    not a guarantee of the global optimum, a speedup, or statistical confidence.
    Check a frozen selection on independently held-out fixtures before adopting it.
    """
    started = time.perf_counter()
    _integer(max_evaluations, "max_evaluations", maximum=MAX_CANDIDATES * MAX_REPEATS)
    _integer(seed, "seed", minimum=0, maximum=2**63 - 1)
    _integer(min_repeats, "min_repeats", maximum=MAX_REPEATS)
    _integer(max_repeats, "max_repeats", maximum=MAX_REPEATS)
    _integer(reduction_factor, "reduction_factor", minimum=2, maximum=MAX_CANDIDATES)
    if min_repeats > max_repeats:
        raise ValueError("min_repeats cannot exceed max_repeats")
    if not callable(evaluate):
        raise ValueError("evaluate must be callable")
    if objective is None:
        objective = CalibrationObjective()
    if type(objective) is not CalibrationObjective:
        raise ValueError("objective must be CalibrationObjective")
    objective.__post_init__()
    configurations = []
    for candidate in candidates:
        if len(configurations) == MAX_CANDIDATES:
            raise ValueError(f"at most {MAX_CANDIDATES} candidates")
        if type(candidate) is not Configuration:
            raise ValueError("candidates must contain Configuration instances")
        candidate.__post_init__()
        if candidate in configurations:
            raise ValueError("duplicate candidate")
        configurations.append(candidate)
    if not configurations:
        raise ValueError("at least one candidate is required")
    _consent(configurations, allow_process)
    if max_evaluations < len(configurations) * min_repeats:
        raise ValueError("max_evaluations must cover min_repeats for every candidate")
    configurations.sort()
    hardware = HardwareCapabilities.detect()
    seeds = fixture_seeds(seed=seed, repeats=max_repeats)
    order_rng = random.Random(seed)
    observations = {config: [] for config in configurations}
    active = list(configurations)
    eliminated = {}
    records, trace = [], []
    completed_repeats, target_repeats = 0, min_repeats
    stopping_reason = "repeat_limit"
    while active and completed_repeats < max_repeats:
        remaining = max_evaluations - len(records)
        if remaining < len(active):
            stopping_reason = "budget_exhausted" if remaining == 0 else "incomplete_round_budget"
            break
        order = sorted(active)
        order_rng.shuffle(order)
        excluded_this_round = []
        for config in order:
            record = _record(evaluate, config, completed_repeats,
                             seeds[completed_repeats], objective)
            records.append(record)
            observations[config].append(record)
            if record.exclusion is not None:
                excluded_this_round.append(config)
        active = [config for config in active if config not in excluded_this_round]
        completed_repeats += 1
        ranked = sorted(active, key=lambda config: (
            _median([objective.score(r.measurement) for r in observations[config]]), config,
        ))
        eliminated_this_round = []
        if completed_repeats == target_repeats and completed_repeats < max_repeats:
            keep = (len(active) + reduction_factor - 1) // reduction_factor
            # Do not eliminate candidates if there is no budget to test survivors.
            if len(active) > 1 and max_evaluations - len(records) >= keep:
                eliminated_this_round = ranked[keep:]
                for config in eliminated_this_round:
                    eliminated[config] = completed_repeats
                active = ranked[:keep]
            target_repeats = min(max_repeats, target_repeats * reduction_factor)
        leader = ranked[0] if ranked else None
        trace.append(TuningConvergencePoint(
            len(records), completed_repeats, tuple(sorted(active)),
            tuple(sorted(eliminated_this_round)), tuple(sorted(excluded_this_round)), leader,
            _median([objective.score(r.measurement) for r in observations[leader]]) if leader else None,
        ))
    if not active:
        stopping_reason = "no_eligible_finalists"
    elif len(records) == max_evaluations:
        stopping_reason = "budget_exhausted"
    results = tuple(_tuning_result(config, observations[config], objective, eliminated)
                    for config in configurations)
    finalists = [result for result in results if result.status == "evaluated"]
    best = min(finalists, key=lambda result: (result.score_seconds, result.configuration), default=None)
    return TuningReport(
        best.configuration if best else None, objective, tuple(records), results, tuple(trace),
        _tuning_uncertainty(results, observations, objective), seed, min_repeats, max_repeats,
        reduction_factor, max_evaluations, allow_process, hardware,
        time.perf_counter() - started, stopping_reason,
    )


def _gap(value, best):
    gap = max(0.0, value / best - 1)
    return gap if math.isfinite(gap) else None


def inspect_holdout(
    selection: CalibrationReport | TuningReport, evaluate: Evaluator, *, repeats: int, seed: int,
    allow_process: bool = False,
) -> tuple[EvaluationRecord, ...]:
    """Measure the frozen selection only. Never select again or alter calibration.

    The caller must supply independently held-out fixtures and seeds. This API
    cannot establish dataset independence or prevent manual tuning leakage.
    """
    if type(selection) not in (CalibrationReport, TuningReport) or selection.selected is None:
        raise ValueError("a calibration or tuning report with an eligible selection is required")
    if type(selection) is TuningReport and not any(
        result.configuration == selection.selected and result.status == "evaluated"
        and result.score_seconds is not None for result in selection.candidates
    ):
        raise ValueError("tuning selection must belong to the final eligible cohort")
    _integer(repeats, "repeats", maximum=MAX_REPEATS)
    _integer(seed, "seed", minimum=0, maximum=2**63 - 1)
    if not callable(evaluate):
        raise ValueError("evaluate must be callable")
    _consent((selection.selected,), allow_process)
    seeds = fixture_seeds(seed=seed, repeats=repeats)
    return tuple(_records(evaluate, selection.selected, repeats, seeds, selection.objective))


# Configuration-only interchange; deliberately independent of report serialization.
MAX_SAVED_CONFIGURATION_BYTES = 16_384


def _saved_text(value, name, maximum):
    if (type(value) is not str or not value or len(value) > maximum
            or not all(char.isprintable() for char in value)):
        raise ValueError(f"{name} must be nonempty printable text of at most {maximum} characters")


def _runtime_identity():
    import sys
    # Include patch/build identity and ABI flags, not just major/minor version.
    return "|".join((sys.implementation.name, sys.implementation.cache_tag or "",
                     sys.version.replace("\n", " "), getattr(sys, "abiflags", "")))


def _validated_hardware_fingerprint(hardware):
    if type(hardware) is not HardwareCapabilities:
        raise ValueError("hardware must be HardwareCapabilities")
    # Reconstruct to validate fields and avoid trusting a stale cached fingerprint.
    return HardwareCapabilities(**{
        name: getattr(hardware, name) for name in (
            "logical_cpus", "affinity_cpus", "quota_cpus", "machine", "system", "cpu_model"
        )
    }).fingerprint


def _saved_configuration(value):
    if type(value) is not Configuration or type(value.backend) is not str:
        raise ValueError("configuration must be Configuration")
    return Configuration(value.backend, value.workers, value.batch_size,
                         value.profile_sample_interval)


def _compatible_revisions(scheduler_revision, strategy_revision):
    if scheduler_revision != SCHEDULER_REVISION:
        raise ValueError("scheduler revision mismatch; recalibrate with the current scheduler")
    if strategy_revision != STRATEGY_REVISION:
        raise ValueError("strategy revision mismatch; recalibrate with the current strategy")


@dataclass(frozen=True)
class SavedConfiguration:
    """Validated, configuration-only data; no fixtures, callbacks, or executable state.

    Use load_configuration to check compatibility before reuse. Direct construction
    validates data but does not establish compatibility or measurement authenticity.
    """
    configuration: Configuration
    workload_key: str
    hardware_fingerprint: str
    python_runtime: str
    provenance: str
    measured_scope: str | None = None
    scheduler_revision: str = field(default_factory=lambda: SCHEDULER_REVISION)
    strategy_revision: str = field(default_factory=lambda: STRATEGY_REVISION)
    package_version: str = field(default_factory=_package_version)

    def __post_init__(self):
        _saved_configuration(self.configuration)
        _saved_text(self.workload_key, "workload_key", 512)
        if (type(self.hardware_fingerprint) is not str
                or len(self.hardware_fingerprint) != 64
                or any(c not in "0123456789abcdef" for c in self.hardware_fingerprint)):
            raise ValueError("hardware_fingerprint must be a lowercase SHA-256 hex digest")
        _saved_text(self.python_runtime, "python_runtime", 2048)
        _saved_text(self.provenance, "provenance", 256)
        _saved_text(self.scheduler_revision, "scheduler_revision", 256)
        _saved_text(self.strategy_revision, "strategy_revision", 256)
        _saved_text(self.package_version, "package_version", 128)
        if self.measured_scope is not None:
            _saved_text(self.measured_scope, "measured_scope", 2048)

    def to_json(self):
        """Return bounded schema-v2 JSON. File I/O remains caller-owned."""
        import json
        self.__post_init__()
        payload = {"schema_version": SAVED_CONFIGURATION_SCHEMA_VERSION, **asdict(self)}
        result = json.dumps(payload, ensure_ascii=True, allow_nan=False, sort_keys=True)
        if len(result.encode("utf-8")) > MAX_SAVED_CONFIGURATION_BYTES:
            raise ValueError("saved configuration exceeds byte limit")
        return result

    def executor_kwargs(self, *, allow_process=False):
        """Reuse constructor options; process consent is never saved or inherited."""
        self.__post_init__()
        _compatible_revisions(self.scheduler_revision, self.strategy_revision)
        return self.configuration.executor_kwargs(allow_process=allow_process)


def export_configuration(report, *, workload_key, measured_scope=None):
    """Export a successful calibration/tuning selection without running callbacks.

    workload_key is the caller's versioned fixture/algorithm/dependency identity.
    Hardware records the calibration machine; runtime records the exporting Python.
    Reports retain the scheduler/strategy revisions and package version from
    calibration. Incompatible revisions require recalibration. Package version
    alone is informational. Export in the same Python runtime as calibration.
    """
    if type(report) not in (CalibrationReport, TuningReport) or report.selected is None:
        raise ValueError("a successful CalibrationReport or TuningReport is required")
    selected = _saved_configuration(report.selected)
    if type(report.objective) is not CalibrationObjective:
        raise ValueError("report objective must be CalibrationObjective")
    report.objective.__post_init__()
    eligible = [c for c in report.candidates if c.configuration == selected]
    if len(eligible) != 1 or eligible[0].score_seconds is None:
        raise ValueError("selected configuration must have an eligible measured score")
    if type(report) is TuningReport and eligible[0].status != "evaluated":
        raise ValueError("tuning selection must belong to the final eligible cohort")
    _number(eligible[0].score_seconds, "selected score", positive=True)
    observations = [r for r in report.records if r.configuration == selected]
    repeats = eligible[0].evaluations if type(report) is TuningReport else report.repeats
    _integer(repeats, "report repeats", maximum=MAX_REPEATS)
    if len(observations) != repeats:
        raise ValueError("selected configuration must have all repeat measurements")
    for record in observations:
        if record.exclusion is not None or type(record.measurement) is not Measurement:
            raise ValueError("selected configuration has an excluded measurement")
        record.measurement.__post_init__()
        if report.objective.exclusion(record.measurement) is not None:
            raise ValueError("selected configuration has an ineligible measurement")
    saved = SavedConfiguration(
        selected, workload_key, _validated_hardware_fingerprint(report.hardware),
        _runtime_identity(), report.provenance, measured_scope,
        report.scheduler_revision, report.strategy_revision, report.package_version,
    )
    _compatible_revisions(saved.scheduler_revision, saved.strategy_revision)
    return saved.to_json()


def load_configuration(payload, *, workload_key, hardware=None, allow_process=False):
    """Load strict JSON, rejecting foreign workload/hardware/Python/revision identities.

    No file access, imports from the payload, task execution, or recalibration.
    hardware may be a validated explicit snapshot for controlled environments/tests;
    omit it to detect the current hardware. A match is not a performance guarantee.
    Package version is informational; scheduler and strategy revisions must match.
    Legacy schema-v1 files lack these revisions and require recalibration.
    """
    import json
    _saved_text(workload_key, "workload_key", 512)
    if type(allow_process) is not bool:
        raise ValueError("allow_process must be bool")
    if type(payload) is not str:
        raise ValueError("payload must be JSON text")
    if len(payload) > MAX_SAVED_CONFIGURATION_BYTES:
        raise ValueError("saved configuration exceeds byte limit")
    try:
        if len(payload.encode("utf-8")) > MAX_SAVED_CONFIGURATION_BYTES:
            raise ValueError("saved configuration exceeds byte limit")
    except UnicodeError as exc:
        raise ValueError("payload must be valid UTF-8 text") from exc

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def reject_constant(value):
        raise ValueError("nonfinite JSON number")

    try:
        data = json.loads(payload, object_pairs_hook=unique_object,
                          parse_constant=reject_constant)
    except (ValueError, RecursionError) as exc:
        raise ValueError("invalid saved configuration JSON") from exc
    if type(data) is not dict:
        raise ValueError("saved configuration fields must match schema v2 exactly")
    schema_version = data.get("schema_version")
    if type(schema_version) is int and schema_version == 1:
        raise ValueError("legacy saved configuration schema v1 lacks scheduler/strategy "
                         "identity; recalibrate and export a schema v2 configuration")
    if type(schema_version) is not int or schema_version != SAVED_CONFIGURATION_SCHEMA_VERSION:
        raise ValueError("unsupported saved configuration schema; recalibrate and export "
                         "a schema v2 configuration")
    required = {"schema_version", "configuration", "workload_key", "hardware_fingerprint",
                "python_runtime", "provenance", "measured_scope", "scheduler_revision",
                "strategy_revision", "package_version"}
    if set(data) != required:
        raise ValueError("saved configuration fields must match schema v2 exactly")
    config = data["configuration"]
    if type(config) is not dict or set(config) != {
        "backend", "workers", "batch_size", "profile_sample_interval"
    }:
        raise ValueError("configuration fields must match schema v2 exactly")
    if type(config["backend"]) is not str:
        raise ValueError("backend must be text")
    saved = SavedConfiguration(Configuration(**config), data["workload_key"],
                               data["hardware_fingerprint"], data["python_runtime"],
                               data["provenance"], data["measured_scope"],
                               data["scheduler_revision"], data["strategy_revision"],
                               data["package_version"])
    _compatible_revisions(saved.scheduler_revision, saved.strategy_revision)
    if saved.workload_key != workload_key:
        raise ValueError("workload_key mismatch")
    if saved.python_runtime != _runtime_identity():
        raise ValueError("Python runtime mismatch")
    current = HardwareCapabilities.detect() if hardware is None else hardware
    if saved.hardware_fingerprint != _validated_hardware_fingerprint(current):
        raise ValueError("hardware fingerprint mismatch")
    _consent((saved.configuration,), allow_process)
    return saved
