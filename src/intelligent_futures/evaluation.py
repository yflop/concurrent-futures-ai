"""Opt-in, evidence-gated adoption. Never observes or replays ordinary submissions.

Only caller-declared repeatable fixtures are executed. Candidate selection uses
training blocks; an untouched, fixed-size held-out experiment can recommend the
exact measured configuration, retain stdlib, or be inconclusive. Core is stdlib
only. The sign interval concerns typical paired log-speedup, not mean throughput.
"""
from __future__ import annotations

from concurrent.futures import CancelledError
from dataclasses import asdict, dataclass, field
import math
import random
import statistics
import time
from typing import Callable, Iterable

from .prediction import HardwareCapabilities
from .tuning import Measurement, SCHEDULER_REVISION, _integer, _number, _package_version, _median, _runtime_identity

EVALUATION_REVISION = "paired-heldout-sign-v1"
MAX_CONFIGURATIONS = 32
MAX_BLOCKS = 128


def _text(value, name, limit=256):
    if type(value) is not str or not value or len(value) > limit or not value.isprintable():
        raise ValueError(f"{name} must be nonempty text of at most {limit} characters")


def _flag(value, name):
    if type(value) is not bool:
        raise ValueError(f"{name} must be bool")


@dataclass(frozen=True, order=True)
class EvaluationConfiguration:
    """Exact executor mode; workers is the active backend slot budget.

    Adaptive splits that budget between threads and processes. Deterministic
    process mode creates an unused thread executor shell but dispatches only to
    its process workers. batch_size is identical in both executor families.
    Prediction always starts with empty profiles for each measurement.
    """
    backend: str = "thread"
    workers: int = 4
    batch_size: int = 1
    process_workers: int = 0
    predictive: bool = False
    profile_sample_interval: int = 1

    def __post_init__(self):
        if self.backend not in ("thread", "process", "adaptive"):
            raise ValueError("backend must be thread, process, or adaptive")
        _integer(self.workers, "workers", maximum=4096)
        _integer(self.batch_size, "batch_size", maximum=1_000_000)
        _integer(self.process_workers, "process_workers", minimum=0, maximum=4096)
        _integer(self.profile_sample_interval, "profile_sample_interval", maximum=1_000_000)
        _flag(self.predictive, "predictive")
        if self.backend == "adaptive":
            if not 0 < self.process_workers < self.workers:
                raise ValueError("adaptive requires 0 < process_workers < workers")
        elif self.process_workers:
            raise ValueError("process_workers is only used for adaptive's split budget")
        if not self.predictive and self.profile_sample_interval != 1:
            raise ValueError("profile_sample_interval requires predictive=True")

    @property
    def uses_process(self):
        return self.backend in ("process", "adaptive")

    def standard_equivalents(self):
        """The same worker/batch budgets must be available to the stdlib search."""
        backends = ("thread", "process") if self.backend == "adaptive" else (self.backend,)
        return tuple(EvaluationConfiguration(b, self.workers, self.batch_size) for b in backends)


@dataclass(frozen=True)
class RepeatableWorkload:
    """Explicit evaluation consent, never permission to replay live user work.

    measure(engine, configuration, seed) must reset the same fixture, implement
    equivalent timing/queue/cache/batch scopes, and return a validated Measurement.
    independent declares that held-out seeds represent independent comparable
    blocks. It is a caller assumption, not something a random seed proves.
    name/version/metric_scope must be non-sensitive; version binds code, data,
    dependencies and relevant cache/environment assumptions for evidence reuse.
    """
    name: str
    version: str
    measure: Callable[[str, EvaluationConfiguration, int], Measurement] = field(repr=False, compare=False)
    repeatable: bool = False
    independent: bool = False
    metric_scope: str = "caller_defined"

    def __post_init__(self):
        for name in ("name", "version", "metric_scope"):
            _text(getattr(self, name), name)
        if not callable(self.measure):
            raise ValueError("measure must be callable")
        _flag(self.repeatable, "repeatable")
        _flag(self.independent, "independent")


@dataclass(frozen=True)
class EvaluationPlan:
    """Predeclared fixed experiment and observational acceptance gates.

    max_pending is an item-count admission/window bound, not an RSS cap.
    max_evaluations limits measurement calls, never task duration or preemption.
    CPU/RSS/p95/elapsed caps filter observations and fail closed if unavailable.
    expected_reuses counts future fixtures of the measured size and scope.
    """
    training_blocks: int = 3
    heldout_blocks: int = 24
    seed: int = 0
    metric: str = "cold_seconds"
    minimum_speedup: float = 0.05
    family_alpha: float = 0.05
    max_pair_slowdown: float = 0.25
    max_log_speedup_mad: float = 0.10
    minimum_measured_seconds: float = 0.001
    max_workers: int = 4
    max_pending: int = 8
    max_evaluations: int = 512
    max_cpu_seconds: float | None = None
    max_peak_rss_bytes: int | None = None
    max_batch_p95_seconds: float | None = None
    max_observed_seconds: float | None = None
    expected_reuses: int | None = None
    evidence_ttl_seconds: float = 86400.0

    def __post_init__(self):
        _integer(self.training_blocks, "training_blocks", maximum=MAX_BLOCKS)
        _integer(self.heldout_blocks, "heldout_blocks", maximum=MAX_BLOCKS)
        _integer(self.seed, "seed", minimum=0, maximum=2**63 - 1)
        _integer(self.max_workers, "max_workers", maximum=4096)
        _integer(self.max_pending, "max_pending", maximum=1_000_000)
        _integer(self.max_evaluations, "max_evaluations", maximum=16384)
        if self.metric not in ("cold_seconds", "warm_seconds"):
            raise ValueError("metric must be cold_seconds or warm_seconds")
        for name in ("minimum_speedup", "max_pair_slowdown", "max_log_speedup_mad", "minimum_measured_seconds"):
            _number(getattr(self, name), name)
        _number(self.family_alpha, "family_alpha", positive=True)
        if self.family_alpha >= 0.5:
            raise ValueError("family_alpha must be below 0.5")
        for name in ("max_cpu_seconds", "max_batch_p95_seconds", "max_observed_seconds"):
            if getattr(self, name) is not None:
                _number(getattr(self, name), name)
        if self.max_peak_rss_bytes is not None:
            _integer(self.max_peak_rss_bytes, "max_peak_rss_bytes", minimum=0, maximum=2**63-1)
        if self.expected_reuses is not None:
            _integer(self.expected_reuses, "expected_reuses", minimum=0, maximum=2**63-1)
        _number(self.evidence_ttl_seconds, "evidence_ttl_seconds", positive=True)
        if self.evidence_ttl_seconds > 365 * 86400:
            raise ValueError("evidence_ttl_seconds must not exceed one year")

    def exclusion(self, measurement):
        if not measurement.correct:
            return "incorrect"
        if measurement.error_count:
            return "task_errors"
        for field_name, cap_name, label in (
            ("cpu_seconds", "max_cpu_seconds", "cpu"),
            ("peak_rss_bytes", "max_peak_rss_bytes", "rss"),
            ("batch_p95_seconds", "max_batch_p95_seconds", "batch_p95"),
        ):
            cap = getattr(self, cap_name)
            if cap is not None:
                value = getattr(measurement, field_name)
                if value is None:
                    return label + "_unavailable"
                if value > cap:
                    return label + "_cap"
        if self.max_observed_seconds is not None and getattr(measurement, self.metric) > self.max_observed_seconds:
            return "observed_elapsed_cap"
        return None


@dataclass(frozen=True)
class TrialRecord:
    phase: str
    block: int
    order: int
    seed: int
    engine: str
    configuration: EvaluationConfiguration
    measurement: Measurement | None
    exclusion: str | None
    callback_seconds: float


@dataclass(frozen=True)
class BaselineComparison:
    role: str
    configuration: EvaluationConfiguration
    pairs: int
    wins_above_margin: int
    sign_p_value: float | None
    comparison_alpha: float
    median_speedup: float | None
    median_log_speedup_lower: float | None
    lower_speedup: float | None
    lower_order_statistic: int | None
    log_speedup_mad: float | None
    aggregate_speedup: float | None
    worst_pair_slowdown: float | None
    minimum_observed_saving_seconds: float | None
    mean_observed_saving_seconds: float | None
    passed: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class AdoptionRecommendation:
    """Exact measured mode, locally guarded; not an executable serialized profile.

    A matching hardware fingerprint is only a compatibility hint. Callers must
    revise workload_version when code, data, dependencies or deployment changes.
    """
    configuration: EvaluationConfiguration
    workload_name: str
    workload_version: str
    metric_scope: str
    max_pending: int
    hardware_fingerprint: str
    runtime_identity: str
    package_version: str
    scheduler_revision: str
    evaluation_revision: str
    created_at: float
    expires_at: float

    def checked_configuration(self, workload, *, allow_process=False, now=None):
        _flag(allow_process, "allow_process")
        if type(workload) is not RepeatableWorkload:
            raise ValueError("workload must be RepeatableWorkload")
        workload.__post_init__()
        self.configuration.__post_init__()
        if (workload.name, workload.version, workload.metric_scope) != (self.workload_name, self.workload_version, self.metric_scope):
            raise ValueError("workload identity or measurement scope changed; reevaluate")
        if not workload.repeatable or not workload.independent:
            raise ValueError("repeatability/independence declarations changed; reevaluate")
        if (self.package_version != _package_version() or self.runtime_identity != _runtime_identity()
                or self.scheduler_revision != SCHEDULER_REVISION or self.evaluation_revision != EVALUATION_REVISION
                or self.hardware_fingerprint != HardwareCapabilities.detect().fingerprint):
            raise ValueError("runtime or hardware changed; reevaluate")
        moment = time.time() if now is None else now
        _number(moment, "now")
        if not self.created_at <= moment < self.expires_at:
            raise ValueError("evidence expired or clock moved backwards; reevaluate")
        declared_window = getattr(workload.measure, "_evaluation_max_pending", None)
        if declared_window is not None and declared_window != self.max_pending:
            raise ValueError("fixture submission window changed; reevaluate")
        if self.configuration.uses_process and getattr(workload.measure, "_evaluation_process_safe", None) is False:
            raise ValueError("fixture process safety changed; reevaluate")
        if self.configuration.uses_process and not allow_process:
            raise ValueError("process recommendation requires renewed allow_process=True")
        return self.configuration


@dataclass(frozen=True)
class EvaluationReport:
    status: str
    reasons: tuple[str, ...]
    workload_name: str
    workload_version: str
    metric_scope: str
    plan: EvaluationPlan
    selected_intelligent: EvaluationConfiguration | None
    selected_stdlib: EvaluationConfiguration | None
    fixed_baseline: EvaluationConfiguration
    records: tuple[TrialRecord, ...]
    comparisons: tuple[BaselineComparison, ...]
    training_seeds: tuple[int, ...]
    heldout_seeds: tuple[int, ...]
    expected_evaluations: int
    attempted_evaluations: int
    successful_evaluations: int
    failed_evaluations: int
    skipped_evaluations: int
    discovery_seconds: float
    training_seconds: float
    validation_seconds: float
    empirical_break_even_reuses: int | None
    empirical_net_seconds: float | None
    recommendation: AdoptionRecommendation | None
    hardware: HardwareCapabilities
    created_at: float
    package_version: str
    evaluation_revision: str = EVALUATION_REVISION

    def to_json(self):
        """Detached JSON-compatible data; never includes callback, inputs or results.

        No from_json/deployment loader is supplied: this is evidence, not a source
        of authority to run code or reactivate process execution.
        """
        return asdict(self)


def _record(workload, engine, configuration, seed, phase, block, order, plan):
    start = time.perf_counter()
    measurement = None
    try:
        measurement = workload.measure(engine, configuration, seed)
        if type(measurement) is not Measurement:
            raise ValueError("measure must return Measurement")
        measurement.__post_init__()
        exclusion = plan.exclusion(measurement)
    except CancelledError:
        raise
    except Exception:
        measurement = None
        exclusion = "callback_error_or_invalid_measurement"
    return TrialRecord(phase, block, order, seed, engine, configuration,
                       measurement, exclusion, max(0.0, time.perf_counter() - start))


def _median_lower(values, alpha):
    """Distribution-free one-sided median order-statistic interval.

    Ties are conservative; the median is that of independent paired log ratios.
    Return None when the fixed number of observations cannot support the level.
    """
    n = len(values)
    if not n:
        return None, None
    tail = 0
    k = 0
    for j in range(n):
        tail += math.comb(n, j)
        if tail / 2**n <= alpha:
            k = j + 1
        else:
            break
    return (sorted(values)[k - 1], k) if k else (None, None)


def _safe_expm1(value):
    # Reports must remain valid strict JSON even for extreme valid timings.
    try:
        result = math.expm1(value)
    except OverflowError:
        return None
    return result if math.isfinite(result) else None


def _compare(role, baseline, base_records, candidate_records, plan):
    n = len(candidate_records)
    alpha = plan.family_alpha / 2  # Fixed + tuned, even when their modes coincide.
    reasons = []
    if (n != plan.heldout_blocks or len(base_records) != n
            or any(r.exclusion is not None for r in (*base_records, *candidate_records))
            or any(a.seed != b.seed for a, b in zip(base_records, candidate_records))):
        return BaselineComparison(role, baseline, n, 0, None, alpha, None, None,
                                  None, None, None, None, None, None, None, False,
                                  ("incomplete_or_failed_heldout",))
    b = [getattr(r.measurement, plan.metric) for r in base_records]
    c = [getattr(r.measurement, plan.metric) for r in candidate_records]
    logs = [math.log(x) - math.log(y) for x, y in zip(b, c)]
    margin = math.log1p(plan.minimum_speedup)
    wins = sum(value > margin for value in logs)
    p = sum(math.comb(n, j) for j in range(wins, n + 1)) / 2**n
    lower, k = _median_lower(logs, alpha)
    median = statistics.median(logs)
    mad = statistics.median(abs(v - median) for v in logs)
    # Normalise sums to avoid overflow; both retain all fixed held-out slots.
    scale = max(*b, *c)
    sum_b, sum_c = math.fsum(v / scale for v in b), math.fsum(v / scale for v in c)
    aggregate = sum_b / sum_c - 1 if sum_c else None
    if aggregate is not None and not math.isfinite(aggregate):
        aggregate = None
    worst = _safe_expm1(max(-v for v in logs))
    savings = [x - y for x, y in zip(b, c)]
    mean_saving = statistics.fmean(v / scale for v in savings) * scale
    if lower is None:
        reasons.append("insufficient_fixed_sample")
    elif not lower > margin or p > alpha:
        reasons.append("practical_improvement_not_established")
    if min(*b, *c) < plan.minimum_measured_seconds:
        reasons.append("timing_below_resolution_floor")
    if mad > plan.max_log_speedup_mad:
        reasons.append("paired_noise_exceeds_threshold")
    if aggregate is None:
        reasons.append("comparison_numeric_overflow")
    elif aggregate <= 0:
        reasons.append("aggregate_regression_or_tie")
    if worst is None or worst > plan.max_pair_slowdown:
        reasons.append("observed_tail_regression")
    numeric = (_safe_expm1(median), _safe_expm1(lower) if lower is not None else None)
    if numeric[0] is None or (lower is not None and numeric[1] is None) or not math.isfinite(mean_saving):
        reasons.append("comparison_numeric_overflow")
    return BaselineComparison(role, baseline, n, wins, p, alpha, numeric[0], lower,
                              numeric[1], k, mad, aggregate, worst, min(savings),
                              mean_saving if math.isfinite(mean_saving) else None,
                              not reasons, tuple(reasons))


def _configurations(values, name, *, stdlib, plan, allow_process):
    result = []
    for value in values:
        if len(result) == MAX_CONFIGURATIONS:
            raise ValueError(f"at most {MAX_CONFIGURATIONS} {name}")
        if type(value) is not EvaluationConfiguration:
            raise ValueError(f"{name} must contain EvaluationConfiguration")
        value.__post_init__()
        if value in result:
            raise ValueError(f"duplicate {name}")
        if stdlib and (value.backend == "adaptive" or value.predictive):
            raise ValueError("stdlib configurations must use deterministic unsampled modes")
        if value.workers > plan.max_workers:
            raise ValueError("configuration exceeds max_workers")
        if value.uses_process and not allow_process:
            raise ValueError("process candidates require allow_process=True")
        result.append(value)
    if not result:
        raise ValueError(f"at least one {name} required")
    return tuple(sorted(result))


def evaluate_workload(workload: RepeatableWorkload, *,
                      stdlib_configurations: Iterable[EvaluationConfiguration],
                      intelligent_configurations: Iterable[EvaluationConfiguration],
                      fixed_baseline: EvaluationConfiguration,
                      plan: EvaluationPlan | None = None, allow_process=False) -> EvaluationReport:
    """Run a predeclared bounded search, then a fixed, untouched paired experiment.

    Recommendation needs BOTH baseline comparisons, all correctness/resource
    gates, declared independent fixtures, and empirical cost amortization within
    expected_reuses. It never changes an application executor or runs live tasks.
    Exceptions are sanitized failed observations; interruption aborts the run.
    There are no automatic retries, early-success stops or held-out reselections.
    """
    start = time.perf_counter()
    created = time.time()
    if type(workload) is not RepeatableWorkload:
        raise ValueError("workload must be RepeatableWorkload")
    workload.__post_init__()
    if not workload.repeatable:
        raise ValueError("evaluation requires explicit repeatable=True fixture consent")
    _flag(allow_process, "allow_process")
    plan = EvaluationPlan() if plan is None else plan
    if type(plan) is not EvaluationPlan:
        raise ValueError("plan must be EvaluationPlan")
    plan.__post_init__()
    declared_window = getattr(workload.measure, "_evaluation_max_pending", None)
    if declared_window is not None and declared_window != plan.max_pending:
        raise ValueError("fixture submission window does not match plan.max_pending")
    if workload.metric_scope == "pool_lifecycle" and plan.metric != "cold_seconds":
        raise ValueError("pool_lifecycle fixture adapter supports cold_seconds only")
    stdlib = _configurations(stdlib_configurations, "stdlib configurations", stdlib=True,
                             plan=plan, allow_process=allow_process)
    intelligent = _configurations(intelligent_configurations, "intelligent configurations", stdlib=False,
                                  plan=plan, allow_process=allow_process)
    if type(fixed_baseline) is not EvaluationConfiguration or fixed_baseline not in stdlib:
        raise ValueError("fixed_baseline must be a predeclared stdlib configuration")
    if any(e not in stdlib for c in intelligent for e in c.standard_equivalents()):
        raise ValueError("stdlib search must include equivalent backend/worker/batch budgets")
    process_safe = getattr(workload.measure, "_evaluation_process_safe", None)
    if process_safe is False and any(c.uses_process for c in (*stdlib, *intelligent)):
        raise ValueError("fixture adapter requires explicit process_safe=True")
    # Reserve worst-case three measured modes per holdout block before any work.
    reserved_holdout_modes = 2 if len(stdlib) == 1 else 3
    reservation = (len(stdlib) + len(intelligent)) * plan.training_blocks + reserved_holdout_modes * plan.heldout_blocks
    if reservation > plan.max_evaluations:
        raise ValueError("fixed experiment exceeds max_evaluations; reduce the declared plan")
    hardware = HardwareCapabilities.detect()
    seed_rng = random.Random(plan.seed)
    seeds = []
    while len(seeds) < plan.training_blocks + plan.heldout_blocks:
        seed = seed_rng.randrange(2**63)
        if seed not in seeds:
            seeds.append(seed)
    train_seeds = tuple(seeds[:plan.training_blocks])
    heldout_seeds = tuple(seeds[plan.training_blocks:])
    order_rng = random.Random(plan.seed ^ 0x5A17A1)
    records = []
    candidates = tuple(("stdlib", c) for c in stdlib) + tuple(("intelligent", c) for c in intelligent)
    train_started = time.perf_counter()
    for block, seed in enumerate(train_seeds):
        order = list(candidates)
        order_rng.shuffle(order)
        for ordinal, (engine, config) in enumerate(order):
            records.append(_record(workload, engine, config, seed, "training", block, ordinal, plan))
    training_seconds = time.perf_counter() - train_started

    def select(engine, configs):
        eligible = []
        for config in configs:
            trials = [r for r in records if r.engine == engine and r.configuration == config]
            if all(r.exclusion is None for r in trials):
                eligible.append((_median(getattr(r.measurement, plan.metric) for r in trials), config))
        return min(eligible)[1] if eligible else None

    chosen_stdlib, chosen_intelligent = select("stdlib", stdlib), select("intelligent", intelligent)
    reasons = []
    fixed_training_failed = any(r.engine == "stdlib" and r.configuration == fixed_baseline
                                and r.exclusion is not None for r in records)
    if chosen_stdlib is None:
        reasons.append("no_eligible_stdlib_training_candidate")
    if chosen_intelligent is None:
        reasons.append("no_eligible_intelligent_training_candidate")
    if fixed_training_failed:
        reasons.append("fixed_baseline_failed_training")
    comparisons = ()
    hold_modes = ()
    validation_started = time.perf_counter()
    if not reasons:
        hold_modes = tuple(dict.fromkeys((("stdlib", fixed_baseline), ("stdlib", chosen_stdlib),
                                          ("intelligent", chosen_intelligent))))
        for block, seed in enumerate(heldout_seeds):
            order = list(hold_modes)
            order_rng.shuffle(order)
            for ordinal, (engine, config) in enumerate(order):
                records.append(_record(workload, engine, config, seed, "heldout", block, ordinal, plan))
        selected_records = [r for r in records if r.phase == "heldout" and r.engine == "intelligent"]
        comparisons = tuple(_compare(role, config,
            [r for r in records if r.phase == "heldout" and r.engine == "stdlib" and r.configuration == config],
            selected_records, plan) for role, config in (("fixed", fixed_baseline), ("tuned", chosen_stdlib)))
        for comparison in comparisons:
            reasons.extend(comparison.role + ":" + r for r in comparison.reasons)
    validation_seconds = time.perf_counter() - validation_started
    if not workload.independent:
        reasons.append("independent_representative_blocks_not_declared")
    discovery_seconds = time.perf_counter() - start
    break_even = net = None
    if comparisons and all(c.passed for c in comparisons):
        saving = min(c.minimum_observed_saving_seconds for c in comparisons)
        if saving > 0:
            ratio = discovery_seconds / saving
            if math.isfinite(ratio):
                break_even = max(1, math.ceil(ratio))
            if plan.expected_reuses is not None:
                net_value = saving * plan.expected_reuses - discovery_seconds
                net = net_value if math.isfinite(net_value) else None
        if plan.expected_reuses is None:
            reasons.append("amortization_horizon_unknown")
        elif break_even is None or plan.expected_reuses < break_even:
            reasons.append("evaluation_cost_not_amortized_in_declared_horizon")
    # Retaining stdlib is a positive outcome when observations show no gain, a
    # regression, or excessive measured adoption cost. Failures/noise stay unknown.
    adverse = {"aggregate_regression_or_tie", "observed_tail_regression"}
    retain = any(set(c.reasons) & adverse for c in comparisons)
    if comparisons and any(c.median_speedup is not None and c.median_speedup <= plan.minimum_speedup for c in comparisons):
        retain = True
    if "evaluation_cost_not_amortized_in_declared_horizon" in reasons:
        retain = True
    status = "recommend_intelligent" if not reasons else "retain_stdlib" if retain else "inconclusive"
    recommendation = None
    if status == "recommend_intelligent":
        recommendation = AdoptionRecommendation(chosen_intelligent, workload.name, workload.version,
            workload.metric_scope, plan.max_pending, hardware.fingerprint, _runtime_identity(),
            _package_version(), SCHEDULER_REVISION, EVALUATION_REVISION, created, created + plan.evidence_ttl_seconds)
    # Distinct baseline aliases share an identical measurement rather than being
    # rerun; count that planned experiment precisely. A failed train leaves all
    # reserved holdout slots per block visibly skipped.
    expected = (len(stdlib) + len(intelligent)) * plan.training_blocks + (len(hold_modes) if hold_modes else reserved_holdout_modes) * plan.heldout_blocks
    successful = sum(r.exclusion is None for r in records)
    return EvaluationReport(status, tuple(reasons), workload.name, workload.version, workload.metric_scope,
        plan, chosen_intelligent, chosen_stdlib, fixed_baseline, tuple(records), comparisons,
        train_seeds, heldout_seeds, expected, len(records), successful, len(records)-successful,
        expected-len(records), discovery_seconds, training_seconds, validation_seconds,
        break_even, net, recommendation, hardware, created, _package_version())
