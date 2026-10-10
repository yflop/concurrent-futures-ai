"""Fail-closed metrics for frozen selection versus an exhaustive held-out grid.

This module only audits measurements. It never runs workloads or changes a
selection. ``rows`` use ``run.trial``'s schema and must cover one engine, workload,
input stratum and host-cap setting at a time. Every declared candidate/seed pair
must have one row, including explicit failure rows. Missing rows invalidate an
exhaustive-oracle claim rather than silently shrinking the candidate universe.

An oracle candidate must pass every declared repeat and every gate in BOTH pool
phases. Thus a fast survivor cannot hide an unsuccessful repeat. The mean oracle
is the lowest mean phase time among those candidates. Per-seed hindsight oracles
are additionally reported, labelled separately; neither oracle is fed to tuning.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import asdict, dataclass
import math
from statistics import median

from intelligent_futures.tuning import Configuration


PHASES = ("cold", "warm", "combined")


def _number(value, *, positive=False):
    try:
        return (type(value) in (int, float) and math.isfinite(value)
                and (value > 0 if positive else value >= 0))
    except OverflowError:
        return False


def _mean(values):
    # Inputs are positive; scaling avoids overflow while preserving tiny values.
    scale = max(values)
    return scale * (math.fsum(value / scale for value in values) / len(values))


@dataclass(frozen=True)
class GateBudgets:
    """Observed acceptance limits, not hard runtime resource enforcement."""
    max_p95_seconds: float = 30.0
    max_rss_bytes: int = 1024**3
    max_cpu_seconds: float = 300.0

    def __post_init__(self):
        for name in ("max_p95_seconds", "max_cpu_seconds"):
            if not _number(getattr(self, name), positive=True):
                raise ValueError(f"{name} must be finite and positive")
        if type(self.max_rss_bytes) is not int or not 0 < self.max_rss_bytes <= 2**63 - 1:
            raise ValueError("max_rss_bytes must be a positive integer below 2**63")


def candidate_key(candidate):
    """Canonical validated configuration; reject unknown fields instead of dropping them."""
    if isinstance(candidate, Mapping):
        try:
            candidate = Configuration(**dict(candidate))
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid candidate configuration") from exc
    if type(candidate) is not Configuration:
        raise ValueError("candidate must be Configuration or a configuration mapping")
    candidate.__post_init__()
    return candidate


def _seeds(values, name):
    result = tuple(values)
    if (not result or any(type(seed) is not int or not 0 <= seed < 2**63 for seed in result)
            or len(set(result)) != len(result)):
        raise ValueError(f"{name} must contain distinct nonnegative integer fixture seeds below 2**63")
    return result


def validate_split(training_seeds, heldout_seeds, *, training_fixture_ids=None,
                   heldout_fixture_ids=None, selection_fixture_seeds=None):
    """Validate declared seed/content disjointness and the selector's actual inputs.

    Fixture IDs, when available, must identify input CONTENT, not paths or output
    digests. Seed checks alone cannot establish independent data distributions.
    ``selection_fixture_seeds`` may repeat: multiple configurations use matched
    training inputs. It must describe every fixture observed by selection.
    """
    training = _seeds(training_seeds, "training_seeds")
    heldout = _seeds(heldout_seeds, "heldout_seeds")
    if set(training).intersection(heldout):
        raise ValueError("training and heldout fixture seeds overlap")
    if (training_fixture_ids is None) != (heldout_fixture_ids is None):
        raise ValueError("provide both training and heldout fixture IDs, or neither")
    content_checked = training_fixture_ids is not None
    if content_checked:
        training_ids, heldout_ids = tuple(training_fixture_ids), tuple(heldout_fixture_ids)
        if len(training_ids) != len(training) or len(heldout_ids) != len(heldout):
            raise ValueError("one fixture content ID is required for every declared seed")
        if any(type(item) is not str or not item for item in (*training_ids, *heldout_ids)):
            raise ValueError("fixture content IDs must be nonempty strings")
        if set(training_ids).intersection(heldout_ids):
            raise ValueError("training and heldout fixture content overlaps")
    selection_checked = selection_fixture_seeds is not None
    if selection_checked:
        observed = tuple(selection_fixture_seeds)
        if any(type(seed) is not int or seed not in training for seed in observed):
            raise ValueError("selection observed a fixture outside the declared training split")
    return {"training_seeds": list(training), "heldout_seeds": list(heldout),
            "seeds_disjoint": True, "fixture_content_disjoint_checked": content_checked,
            "selection_inputs_checked": selection_checked,
            "independence_limit": "Disjoint declared inputs do not establish distributional independence or prevent manual tuning after holdout inspection."}


def phase_seconds(row, phase):
    """Read exact lifecycle-cold/warm scope; never substitute missing cold timings."""
    if phase not in PHASES:
        raise ValueError("phase must be cold, warm or combined")
    if not isinstance(row, Mapping):
        return None
    cold = row.get("cold_lifecycle_seconds")
    warm_phase = row.get("warm")
    warm = warm_phase.get("seconds") if isinstance(warm_phase, Mapping) else None
    if phase == "cold":
        value = cold
    elif phase == "warm":
        value = warm
    else:
        value = cold + warm if _number(cold, positive=True) and _number(warm, positive=True) else None
    return value if _number(value, positive=True) else None


def gate_failures(row, budgets):
    """Every required metric fails closed; correctness and resources stay distinct."""
    if type(budgets) is not GateBudgets:
        raise ValueError("budgets must be GateBudgets")
    budgets.__post_init__()
    if not isinstance(row, Mapping):
        return ["invalid_row"]
    failures = []
    if row.get("error") is not None or row.get("failure") is not None:
        failures.append("trial_failure")
    for phase in ("cold", "warm"):
        measured = row.get(phase)
        if not isinstance(measured, Mapping):
            failures.append(phase + ":missing_phase")
            continue
        if measured.get("correct") is not True:
            failures.append(phase + ":incorrect")
        errors = measured.get("errors")
        rate = measured.get("error_rate")
        if not isinstance(errors, (list, tuple)) or errors or not _number(rate) or rate != 0:
            failures.append(phase + ":task_errors_or_invalid_error_metric")
        tasks = measured.get("tasks")
        if type(tasks) is not int or tasks < 1:
            failures.append(phase + ":invalid_task_count")
        if not _number(measured.get("seconds"), positive=True):
            failures.append(phase + ":invalid_elapsed")
        if not _number(measured.get("throughput_per_second"), positive=True):
            failures.append(phase + ":invalid_throughput")
        p95 = measured.get("batch_completion_latency_p95_seconds")
        if not _number(p95):
            failures.append(phase + ":latency_unavailable")
        elif p95 > budgets.max_p95_seconds:
            failures.append(phase + ":latency_cap")
    if any(phase_seconds(row, phase) is None for phase in PHASES):
        failures.append("invalid_phase_timing")
    for field, limit, label in (("sampled_tree_peak_rss_bytes", budgets.max_rss_bytes, "rss"),
                                 ("lifecycle_cpu_seconds", budgets.max_cpu_seconds, "cpu")):
        value = row.get(field)
        if not _number(value):
            failures.append(label + "_unavailable")
        elif value > limit:
            failures.append(label + "_cap")
    if "rss_collection" in row and (not isinstance(row["rss_collection"], Mapping)
            or row["rss_collection"].get("complete") is not True):
        failures.append("rss_collection_incomplete")
    cold, warm = row.get("cold"), row.get("warm")
    if isinstance(cold, Mapping) and isinstance(warm, Mapping) and cold.get("tasks") != warm.get("tasks"):
        failures.append("phase_task_counts_differ")
    return failures


def _grid(rows, candidates, heldout_seeds, budgets):
    configs = tuple(candidate_key(candidate) for candidate in candidates)
    if not configs or len(set(configs)) != len(configs):
        raise ValueError("declare a nonempty distinct candidate universe")
    configs = tuple(sorted(configs))
    seeds = _seeds(heldout_seeds, "heldout_seeds")
    if type(budgets) is not GateBudgets:
        raise ValueError("budgets must be GateBudgets")
    budgets.__post_init__()
    indexed = defaultdict(list)
    structural = []
    count = 0
    for count, row in enumerate(rows, 1):
        try:
            config = candidate_key(row["configuration"])
            seed = row["seed"]
        except (KeyError, TypeError, ValueError):
            structural.append(f"row {count}: invalid configuration or missing seed")
            continue
        if config not in configs or type(seed) is not int or seed not in seeds:
            structural.append(f"row {count}: undeclared candidate or heldout seed")
            continue
        indexed[(config, seed)].append(row)
    audits = []
    eligible = []
    for config in configs:
        failures = []
        for seed in seeds:
            entries = indexed[(config, seed)]
            if len(entries) != 1:
                reason = "missing_row" if not entries else "duplicate_rows"
                structural.append(f"{config!r} seed={seed}: {reason}")
                failures.append({"seed": seed, "reasons": [reason]})
            else:
                reasons = gate_failures(entries[0], budgets)
                if reasons:
                    failures.append({"seed": seed, "reasons": reasons})
        if not failures:
            eligible.append(config)
        audits.append({"configuration": asdict(config), "eligible": not failures,
                       "observed_rows": sum(len(indexed[(config, seed)]) for seed in seeds),
                       "failures": failures})
    # Mixed engine inputs must not turn into another apparently exhaustive grid.
    engines = [row.get("engine") for entries in indexed.values() for row in entries]
    if any(engine is not None and type(engine) is not str for engine in engines):
        structural.append("invalid engine label")
    elif len(set(engines)) > 1:
        structural.append("rows mix engines; audit each engine separately")
    for seed in seeds:
        task_counts = {row[phase]["tasks"] for (config, seen_seed), entries in indexed.items()
                       if seen_seed == seed for row in entries for phase in ("cold", "warm")
                       if isinstance(row.get(phase), Mapping)
                       and type(row[phase].get("tasks")) is int and row[phase]["tasks"] > 0}
        if len(task_counts) > 1:
            structural.append(f"seed={seed}: candidates measured different task counts")
    report = {"grid_complete": not structural, "structural_failures": structural,
              "declared_candidate_count": len(configs), "declared_seed_count": len(seeds),
              "expected_row_count": len(configs) * len(seeds), "observed_row_count": count,
              "eligible_candidate_count": len(eligible), "candidates": audits,
              "budgets": asdict(budgets),
              "eligibility": "Every planned seed must pass correctness, errors, timings, throughput, p95, CPU and RSS in both phases."}
    return report, configs, seeds, indexed, tuple(eligible)


def audit_grid(rows, *, candidates, heldout_seeds, budgets):
    """Return JSON-safe completeness and all-repeat candidate admissibility audit."""
    return _grid(rows, candidates, heldout_seeds, budgets)[0]


def _relative_regret(value, oracle):
    gap = max(0.0, value / oracle - 1.0)
    return gap if math.isfinite(gap) else None


def summarize_selection(selected, rows, *, candidates, heldout_seeds, budgets,
                        phase="combined"):
    """Report frozen-choice quality, retaining all declared seeds in denominators.

    A missing/duplicate grid pair makes oracle metrics unavailable. A completely
    observed failed candidate is excluded, not a missing observation. If the
    selected candidate fails any repeat, its aggregate regret is unavailable and
    acceptance fails; individual valid repeat comparisons remain visible.
    """
    if phase not in PHASES:
        raise ValueError("phase must be cold, warm or combined")
    audit, configs, seeds, indexed, eligible = _grid(rows, candidates, heldout_seeds, budgets)
    choice = candidate_key(selected) if selected is not None else None
    if choice is not None and choice not in configs:
        raise ValueError("selected configuration is outside the declared candidate universe")
    oracle_valid = audit["grid_complete"] and bool(eligible)
    means = {config: _mean([phase_seconds(indexed[(config, seed)][0], phase) for seed in seeds])
             for config in eligible} if oracle_valid else {}
    best = min(means, key=lambda config: (means[config], config)) if means else None
    best_mean = means.get(best)
    selected_mean = means.get(choice)
    comparisons = []
    for seed in seeds:
        entries = indexed.get((choice, seed), [])
        selection_valid = len(entries) == 1 and not gate_failures(entries[0], budgets)
        selected_seconds = phase_seconds(entries[0], phase) if selection_valid else None
        per_seed = (min(eligible, key=lambda config: (phase_seconds(indexed[(config, seed)][0], phase), config))
                    if oracle_valid else None)
        oracle_seconds = phase_seconds(indexed[(per_seed, seed)][0], phase) if per_seed else None
        regret = (_relative_regret(selected_seconds, oracle_seconds)
                  if selected_seconds is not None and oracle_seconds is not None else None)
        # A safe repeat from an overall inadmissible selected candidate can be
        # faster than the admissible oracle; clipping is explicit in the method.
        within = bool(selection_valid and oracle_valid and selected_seconds / oracle_seconds <= 1.1)
        comparisons.append({"seed": seed, "selected_passes_gates": selection_valid,
                            "selected_seconds": selected_seconds,
                            "per_seed_oracle_configuration": asdict(per_seed) if per_seed else None,
                            "per_seed_oracle_seconds": oracle_seconds,
                            "relative_regret": regret, "within_10_percent": within})
    covered = sum(row["within_10_percent"] for row in comparisons)
    regrets = [row["relative_regret"] for row in comparisons if row["relative_regret"] is not None]
    valid = oracle_valid and choice in eligible
    return {"phase": phase, "selected_configuration": asdict(choice) if choice else None,
            "selected_eligible": choice in eligible, "quality_gate_eligible": valid,
            "oracle_valid": oracle_valid, "grid_audit": audit,
            "oracle_configuration": asdict(best) if best else None,
            "oracle_mean_seconds": best_mean, "selected_mean_seconds": selected_mean,
            "aggregate_relative_regret": (_relative_regret(selected_mean, best_mean) if valid else None),
            "aggregate_within_10_percent": bool(valid and selected_mean / best_mean <= 1.1),
            "within_10_percent_count": covered, "coverage_denominator": len(seeds),
            "within_10_percent_coverage": covered / len(seeds),
            "per_seed_regret_median": median(regrets) if regrets else None,
            "per_seed_regret_max": max(regrets) if regrets else None,
            "per_seed_regret_observation_count": len(regrets), "comparisons": comparisons,
            "oracle_scope": "Best observed heldout value among the entire declared grid's all-repeat admissible candidates, not a global optimum.",
            "regret_method": "max(0, selected/oracle - 1); aggregate uses candidate mean, per-seed uses a separate hindsight minimum; undefined values remain null."}


def summarize_phases(selected, rows, *, candidates, heldout_seeds, budgets):
    """Cold lifecycle, reused-pool warm and combined quality kept separate."""
    # Materialize once so callers can safely pass generators.
    rows, candidates, heldout_seeds = tuple(rows), tuple(candidates), tuple(heldout_seeds)
    return {phase: summarize_selection(selected, rows, candidates=candidates,
                                      heldout_seeds=heldout_seeds, budgets=budgets, phase=phase)
            for phase in PHASES}


def search_cost_summary(requested_records, reference_records, *, replay,
                        actual_wall_seconds=None, candidates=None, training_seeds=None):
    """Audit selector queries against a complete *training* measurement matrix.

    Keys are (configuration, fixture_seed), with ``seed`` accepted for trial rows.
    Reference records must carry genuine ``callback_seconds`` measured around
    workload evaluation. For replay this is a *virtual* standalone execution
    cost: the sum of prerecorded costs of queried pairs, never replay wall time.
    Pass ``candidates`` and ``training_seeds`` together to verify the denominator
    against the predeclared exhaustive universe. Otherwise only completeness of
    the *observed* rectangular training matrix can be checked. The caller also
    validates train/heldout separation with ``validate_split``.
    """
    if type(replay) is not bool:
        raise ValueError("replay must be bool")
    if actual_wall_seconds is not None and not _number(actual_wall_seconds):
        raise ValueError("actual_wall_seconds must be finite and nonnegative")
    def index(records, name):
        result = {}
        for record in records:
            if not isinstance(record, Mapping):
                raise ValueError(f"{name} records must be mappings")
            config = candidate_key(record.get("configuration"))
            seed = record.get("fixture_seed", record.get("seed"))
            if type(seed) is not int or not 0 <= seed < 2**63:
                raise ValueError(f"{name} records require valid fixture seeds")
            key = (config, seed)
            if key in result:
                raise ValueError(f"{name} has duplicate callback query")
            result[key] = record
        return result
    requested, reference = index(requested_records, "requested"), index(reference_records, "reference")
    if not reference:
        raise ValueError("reference must contain exhaustive training measurements")
    if not requested.keys() <= reference.keys():
        raise ValueError("selector queried outside the reference training matrix")
    if (candidates is None) != (training_seeds is None):
        raise ValueError("provide both candidates and training_seeds, or neither")
    declared_checked = candidates is not None
    if declared_checked:
        configs = tuple(candidate_key(config) for config in candidates)
        seeds = _seeds(training_seeds, "training_seeds")
        if not configs or len(set(configs)) != len(configs):
            raise ValueError("declare distinct nonempty training candidates")
    else:
        configs = {key[0] for key in reference}
        seeds = {key[1] for key in reference}
    if set(reference) != {(config, seed) for config in configs for seed in seeds}:
        raise ValueError("reference training matrix is not complete for every candidate/seed pair")
    for record in reference.values():
        if not _number(record.get("callback_seconds")):
            raise ValueError("reference callback_seconds must be genuine finite nonnegative measurements")
    try:
        total = math.fsum(row["callback_seconds"] for row in reference.values())
        used = math.fsum(reference[key]["callback_seconds"] for key in requested)
    except OverflowError as exc:
        raise ValueError("callback cost sum is not finite") from exc
    sample_fraction = len(requested) / len(reference)
    return {"callback_count": len(requested), "exhaustive_callback_count": len(reference),
            "declared_reference_universe_checked": declared_checked,
            "sample_fraction": sample_fraction, "sample_reduction_fraction": 1 - sample_fraction,
            "recorded_requested_callback_seconds": used, "recorded_exhaustive_callback_seconds": total,
            "recorded_execution_cost_fraction": used / total if total > 0 else None,
            "actual_search_wall_seconds": actual_wall_seconds, "replay": replay,
            "cost_scope": ("Virtual execution cost from requested prerecorded training callbacks; actual wall is replay overhead."
                           if replay else "Observed callback execution cost; actual wall additionally includes selector overhead."),
            "excluded_costs": ["shared fixture generation", "sequential correctness references", "heldout oracle evaluation", "operational adoption"]}
