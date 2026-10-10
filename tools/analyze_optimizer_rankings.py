"""Read-only descriptive ranking/cost audit; never reselects or runs workloads."""
from __future__ import annotations

import argparse
from fractions import Fraction
import json
import math
from pathlib import Path
import statistics

from benchmarks.real_world import NAMES
from benchmarks.real_world.optimizer_metrics import (
    GateBudgets, gate_failures, phase_seconds, summarize_selection,
)
from tools.summarize_optimizer import execution_capacity


PHASES = ('cold', 'warm', 'combined')
BASELINES = ('standard_library_fixed', 'standard_library_manual_tuning_proxy', 'direct_python')
UNCERTAINTY_SCOPE = (
    'Mean/median/MAD/range describe the declared correlated seed observations, not '
    'confidence intervals or deployment success probabilities. Aggregate statistics '
    'and ratios require every expected slot; failed/missing/duplicate slots are retained.'
)
AMORTIZATION_SCOPE = (
    'Conditional component estimate for repeated combined two-pass lifecycles '
    '(cold executor lifecycle plus one warm pass), assuming unchanged observed mean '
    'execution savings. Discovery costs are virtual sums of requested real training '
    'callback durations, not live end-to-end search wall time. Fixture/reference '
    'preparation, holdout/oracle measurement, integration and adoption costs are excluded. '
    'Fixed/direct baselines have zero discovery cost by design. This is not a future '
    'break-even guarantee; no estimate is made without positive execution savings.'
)


def _number(value, *, positive=False):
    try:
        return (type(value) in (int, float) and math.isfinite(value)
                and (value > 0 if positive else value >= 0))
    except OverflowError:
        return False


def describe(values):
    if not values:
        return {'count': 0, 'mean': None, 'median': None, 'mad': None,
                'minimum': None, 'maximum': None}
    middle = statistics.median(values)
    return {'count': len(values), 'mean': statistics.mean(values), 'median': middle,
            'mad': statistics.median(abs(value - middle) for value in values),
            'minimum': min(values), 'maximum': max(values)}


def _complete_description(values):
    """Keep expected slots; never summarize just the successful ratio subset."""
    valid = [value for value in values if value is not None]
    complete = bool(values) and len(valid) == len(values)
    result = describe(valid if complete else [])
    result.update(count=len(valid), expected_count=len(values), complete=complete,
                  missing_or_invalid_count=len(values) - len(valid))
    return result


def _ratio(numerator, denominator):
    if not _number(numerator, positive=True) or not _number(denominator, positive=True):
        return None
    value = numerator / denominator
    return value if math.isfinite(value) else None


def _capacity(configuration, count):
    result = execution_capacity(configuration, count)
    if result is not None:
        result.update(configured_batch_size=configuration['batch_size'],
                      effective_batch_size_ceiling=min(count, configuration['batch_size']),
                      final_batch_size=count % configuration['batch_size']
                      or min(count, configuration['batch_size']))
    return result


def _observation(rows, seed, gates, count, *, configuration, engine,
                 configuration_required=True, structural_failures=()):
    failures = list(structural_failures)
    if configuration_required and configuration is None:
        failures.append('configuration_unavailable')
    matches = [row for row in rows if row.get('seed') == seed]
    if len(matches) != 1:
        failures.append('missing_row' if not matches else 'duplicate_rows')
    row = matches[0] if len(matches) == 1 else None
    if row is not None:
        failures.extend(gate_failures(row, gates))
        if row.get('configuration') != configuration:
            failures.append('configuration_mismatch')
        if row.get('engine') != engine:
            failures.append('engine_mismatch')
        if any(row.get(phase, {}).get('tasks') != count for phase in ('cold', 'warm')
               if isinstance(row.get(phase), dict)):
            failures.append('declared_task_count_mismatch')
    return {'seed': seed, 'valid': not failures, 'observed_rows': len(matches),
            'failures': failures, 'phases': {
                phase: phase_seconds(row, phase) if not failures else None for phase in PHASES}}


def _series(rows, seeds, gates, count, *, configuration, engine,
            configuration_required=True):
    unexpected = [row.get('seed') for row in rows if row.get('seed') not in seeds]
    structural = ['undeclared_seed_rows'] if unexpected else []
    return [_observation(rows, seed, gates, count, configuration=configuration,
                         engine=engine, configuration_required=configuration_required,
                         structural_failures=structural) for seed in seeds]


def _configuration_series(raw, group, seeds, config, gates, count):
    rows = [row for row in raw.get(group, []) if config is not None
            and row.get('configuration') == config]
    return _series(rows, seeds, gates, count, configuration=config, engine='intelligent')


def _phase_comparison(selected, baseline, phase, matching=None, *, failures=()):
    slots = []
    for index, (left, right) in enumerate(zip(selected, baseline)):
        common_failures = list(failures)
        common_failures.extend('selected:' + value for value in left['failures'])
        common_failures.extend('baseline:' + value for value in right['failures'])
        chosen_time, baseline_time = left['phases'][phase], right['phases'][phase]
        valid = not common_failures
        matched = matching[index] if matching is not None else None
        matching_time = matched['phases'][phase] if matched is not None else None
        slots.append({
            'seed': left['seed'], 'valid': valid, 'failures': common_failures,
            'selected_valid': left['valid'], 'baseline_valid': right['valid'],
            'observed_selected_rows': left['observed_rows'],
            'observed_baseline_rows': right['observed_rows'],
            'selected_seconds': chosen_time, 'baseline_seconds': baseline_time,
            'baseline_time_over_selected_time': _ratio(baseline_time, chosen_time) if valid else None,
            'selected_time_over_baseline_time': _ratio(chosen_time, baseline_time) if valid else None,
            'execution_saving_seconds': baseline_time - chosen_time if valid else None,
            'matching_config_intelligent_seconds': matching_time,
            'matching_config_failures': matched['failures'] if matched is not None else [],
            'stdlib_time_over_matching_config_intelligent_time':
                _ratio(baseline_time, matching_time) if valid else None,
        })
    full = bool(slots) and all(slot['valid'] for slot in slots)
    baseline_stats = _complete_description([slot['baseline_seconds'] for slot in slots])
    selected_stats = _complete_description([slot['selected_seconds'] for slot in slots])
    matching_stats = _complete_description([slot['matching_config_intelligent_seconds'] for slot in slots])
    return {
        'expected_seeds': len(slots), 'valid_seeds': sum(slot['valid'] for slot in slots),
        'complete_pairs': full, 'seed_slots': slots, 'baseline': baseline_stats,
        'automatic_choice': selected_stats, 'matching_config_intelligent': matching_stats,
        'baseline_mean_over_automatic_choice_mean':
            _ratio(baseline_stats['mean'], selected_stats['mean']) if full else None,
        'automatic_choice_mean_over_baseline_mean':
            _ratio(selected_stats['mean'], baseline_stats['mean']) if full else None,
        'matching_intelligent_mean_over_stdlib_mean':
            _ratio(matching_stats['mean'], baseline_stats['mean']) if full else None,
        'mean_execution_saving_seconds':
            baseline_stats['mean'] - selected_stats['mean'] if full else None,
        'per_seed_baseline_over_selected_ratios':
            _complete_description([slot['baseline_time_over_selected_time'] for slot in slots]),
        'per_seed_selected_over_baseline_ratios':
            _complete_description([slot['selected_time_over_baseline_time'] for slot in slots]),
        'per_seed_execution_savings_seconds':
            _complete_description([slot['execution_saving_seconds'] for slot in slots]),
        'descriptive_uncertainty_scope': UNCERTAINTY_SCOPE,
    }


def _amortization(comparison, primary_cost, baseline_cost):
    costs_valid = _number(primary_cost) and _number(baseline_cost)
    difference = primary_cost - baseline_cost if costs_valid else None
    saving = comparison['mean_execution_saving_seconds']
    cycles = None
    if not comparison['complete_pairs']:
        status = 'incomplete_or_failed_execution_pairs'
    elif saving is None or saving <= 0:
        status = 'no_positive_execution_saving'
    elif not costs_valid:
        status = 'discovery_cost_unavailable'
    else:
        # Exact arithmetic on the measured finite values avoids overflow of a
        # very large cost / very small saving and never rounds a ratio to infinity.
        cycles = math.ceil(Fraction(max(0., difference)) / Fraction(saving))
        status = 'conditional_component_estimate'
    return {
        'scope': AMORTIZATION_SCOPE, 'status': status,
        'execution_unit': 'combined cold lifecycle plus one warm pass',
        'primary_virtual_callback_seconds': primary_cost if _number(primary_cost) else None,
        'baseline_virtual_callback_seconds': baseline_cost if _number(baseline_cost) else None,
        'primary_minus_baseline_discovery_seconds': difference,
        'nonnegative_extra_discovery_seconds': max(0., difference) if costs_valid else None,
        'mean_execution_saving_seconds': saving,
        'minimum_nonnegative_extra_cost_repayment_cycles': cycles,
        'formula': 'ceil(max(0, primary_virtual_callback_seconds - baseline_virtual_callback_seconds) / positive_mean_combined_execution_saving_seconds)',
    }


def audit(raw):
    protocol = raw['protocol']
    gates = GateBudgets(**protocol['gates'])
    seeds, count = protocol['heldout_seeds'], protocol['count_per_pass']
    selected = raw['selections']['budgeted']['selected']
    selected_series = _configuration_series(raw, 'heldout', seeds, selected, gates, count)
    # Re-audit the current immutable raw grid rather than trust possibly stale summaries.
    oracle_summaries = {phase: summarize_selection(
        selected, raw.get('heldout', []), candidates=protocol['grid'],
        heldout_seeds=seeds, budgets=gates, phase=phase) for phase in PHASES}
    oracle = oracle_summaries['combined']['oracle_configuration']
    choices, paired = {}, []
    splits = (('training', protocol['training_fixture_seeds']), ('heldout', seeds))
    for label, config in (('selected', selected), ('aggregate_heldout_oracle', oracle)):
        observations, slots = {}, {}
        for group, expected in splits:
            slots[group] = _configuration_series(raw, group, expected, config, gates, count)
            observations[group] = {phase: _complete_description(
                [slot['phases'][phase] for slot in slots[group]]) for phase in PHASES}
        choices[label] = {'configuration': config, 'capacity': _capacity(config, count),
                          'observations': observations, 'seed_slots': slots}
    # Compatibility: this comparison fixes the COMBINED oracle config for all phases.
    for group, expected in splits:
        left = choices['selected']['seed_slots'][group]
        right = choices['aggregate_heldout_oracle']['seed_slots'][group]
        for first, second in zip(left, right):
            valid = first['valid'] and second['valid']
            paired.append({'split': group, 'seed': first['seed'], 'valid': valid,
                           'failures': first['failures'] + second['failures'], 'phases': {
                               phase: {'selected_seconds': first['phases'][phase],
                                       'fixed_aggregate_oracle_seconds': second['phases'][phase],
                                       'selected_over_oracle': _ratio(first['phases'][phase], second['phases'][phase]) if valid else None}
                               for phase in PHASES}})
    primary_cost = raw['selections']['budgeted'].get('virtual_callback_seconds')
    manual = raw.get('manual_proxy_selection', {})
    manual_cost = manual.get('virtual_callback_seconds')
    baseline_configs = {
        'standard_library_fixed': raw.get('standard_library_fixed_configuration'),
        'standard_library_manual_tuning_proxy': manual.get('selected'),
        'direct_python': None,
    }
    baselines = {}
    for label in BASELINES:
        config = baseline_configs[label]
        rows = [row for row in raw.get('baseline_heldout', []) if row.get('baseline_role') == label]
        baseline_series = _series(rows, seeds, gates, count, configuration=config,
                                  engine='sequential' if label == 'direct_python' else 'stdlib',
                                  configuration_required=label != 'direct_python')
        matching = (_configuration_series(raw, 'heldout', seeds, config, gates, count)
                    if config is not None else None)
        phases = {phase: _phase_comparison(selected_series, baseline_series, phase, matching)
                  for phase in PHASES}
        discovery_cost = manual_cost if label == 'standard_library_manual_tuning_proxy' else 0.
        # Preserve combined-only keys used by the previous version, now fail-closed.
        baselines[label] = {**phases['combined'], 'configuration': config,
                            'capacity': _capacity(config, count), 'phases': phases,
                            'comparison_scope': {
                                'standard_library_fixed': 'Predeclared fixed standard-library configuration; same workload algorithm; zero discovery cost.',
                                'standard_library_manual_tuning_proxy': 'Independent training-only exhaustive arithmetic-mean standard-library grid selection; reproducible manual-tuning proxy, not claimed human expertise.',
                                'direct_python': 'Executor-independent direct sequential execution of the same per-item workload algorithm; no concurrent batching or discovery.',
                            }[label],
                            'estimated_amortization': _amortization(phases['combined'], primary_cost, discovery_cost)}
    oracles = {}
    for phase, result in oracle_summaries.items():
        config = result['oracle_configuration']
        oracle_series = _configuration_series(raw, 'heldout', seeds, config, gates, count)
        comparison = _phase_comparison(selected_series, oracle_series, phase,
                                       failures=() if result['oracle_valid'] else ('conditional_oracle_unavailable',))
        oracles[phase] = {**comparison, 'oracle_configuration': config,
                          'capacity': _capacity(config, count),
                          'oracle_valid': result['oracle_valid'], 'grid_audit': result['grid_audit'],
                          'scope': 'Phase-specific lowest heldout mean among candidates passing every declared seed/gate in a complete grid; hindsight conditional candidate oracle, not a deployable baseline.',
                          'amortization': None,
                          'amortization_scope': 'Not estimated: the heldout oracle is hindsight, not a free deployable choice.'}
    return {
        'schema_version': 2, 'workload': protocol['workload'], 'raw_status': raw.get('status'),
        'recorded_failures': raw.get('failures', []), 'expected_heldout_seeds': seeds,
        'choices': choices, 'paired_observations': paired, 'baselines': baselines,
        'conditional_candidate_oracles': oracles,
        'primary_virtual_callback_seconds': primary_cost if _number(primary_cost) else None,
        'manual_proxy_virtual_callback_seconds': manual_cost if _number(manual_cost) else None,
        'primary_matches_exhaustive_choice': selected == raw['selections']['exhaustive']['selected'],
        'cache_scope': ('cached_http creates a fresh per-task requests-cache database; warm means executor reuse, not a prior task cache'
                        if protocol['workload'] == 'cached_http' else None),
    }


def summary(root):
    workloads, incomplete = [], []
    for name in NAMES:
        source = root / (name + '.json')
        if not source.exists():
            incomplete.append({'workload': name, 'status': 'missing', 'expected_heldout_seeds': None,
                               'seed_slots': None, 'reason': 'No raw protocol available; workload remains in planned denominator.'})
            continue
        raw = json.loads(source.read_text())
        if raw.get('status') == 'complete':
            workloads.append(audit(raw))
        else:
            seeds = raw.get('protocol', {}).get('heldout_seeds', [])
            incomplete.append({'workload': name, 'status': raw.get('status'),
                               'expected_heldout_seeds': seeds,
                               'seed_slots': [{'seed': seed, 'valid': False,
                                               'reason': 'workload_not_complete'} for seed in seeds],
                               'recorded_failures': raw.get('failures', [])})
    return {
        'schema_version': 2, 'status': 'complete' if not incomplete else 'partial',
        'scope': 'Descriptive frozen-choice ranking audit only. No heldout observation changes selection. ' + UNCERTAINTY_SCOPE,
        'workloads_planned': len(NAMES), 'workloads_completed': len(workloads),
        'missing_or_running_workloads': [item['workload'] for item in incomplete],
        'incomplete_workload_records': incomplete, 'workloads': workloads,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(json.dumps(summary(args.input), indent=2, allow_nan=False) + '\n')


if __name__ == '__main__':
    main()
