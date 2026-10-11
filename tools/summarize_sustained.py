"""Read-only slot/selection/comparison audit; executes no workload or pool."""
from __future__ import annotations
import argparse
from dataclasses import asdict
import gzip
import json
import math
from pathlib import Path
import random
import statistics
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmarks.real_world.adoption_sustained import Configuration, compare, digest
from intelligent_futures.evaluation import EvaluationConfiguration, EvaluationPlan, TrialRecord, _compare
from intelligent_futures.tuning import Measurement


def load(path):
    opener = gzip.open if str(path).endswith('.gz') else open
    with opener(path, 'rb') as stream:
        raw = stream.read(128 * 1024**2 + 1)
    if len(raw) > 128 * 1024**2:
        raise ValueError('report exceeds bounded reader')
    return json.loads(raw)


def audit(report):
    if report.get('phase') != 'confirmation' or report.get('status') not in (
            'warm_execution_supported_adoption_horizon_unknown', 'no_full_execution_advantage'):
        raise ValueError('a complete confirmation experiment is required')
    plan = report['plan']
    if digest(plan) != report['plan_sha256']:
        raise AssertionError('preregistered plan digest mismatch')
    grid = {label: tuple(Configuration(**row) for row in values) for label, values in plan['grids'].items()}
    records = report['records']
    if (len(records) != report['attempted_observations'] or len(records) != report['settled_observations']
            or len(records) != report['planned_observations'] or report['skipped_observations']):
        raise AssertionError('observation accounting mismatch')
    if report['failed_observations'] != sum(bool(row['exclusions']) for row in records):
        raise AssertionError('failure accounting mismatch')
    for row in records:
        if row['source_identity'] != report['source_identity'] or row['affinity'] != report['expected_affinity']:
            raise AssertionError('runtime or affinity identity mismatch')
    train = [r for r in records if r['phase'] == 'training']
    held = [r for r in records if r['phase'] == 'heldout']
    selected = {}
    for label, configurations in grid.items():
        eligible = []
        for config in configurations:
            observations = [r for r in train if r['configuration_key'] == config.key]
            if [r['seed'] for r in observations] != plan['seeds']['training']:
                raise AssertionError('missing/duplicate/reordered training slots')
            if all(not r['exclusions'] for r in observations):
                eligible.append((statistics.median(r['warm']['seconds'] for r in observations), config))
        selected[label] = min(eligible)[1] if eligible else None
    if {label: asdict(c) if c else None for label, c in selected.items()} != report['selected']:
        raise AssertionError('training selection replay mismatch')
    fixed = Configuration(**plan['fixed'])
    roles = {'fixed': fixed, 'single': selected['single'], 'static': selected['static'], 'overflow': selected['overflow']}
    modes = tuple(dict.fromkeys((*roles.values(), selected['intelligent'])))
    if len(train) != sum(len(v) for v in grid.values()) * len(plan['seeds']['training']) or len(held) != 24 * len(modes):
        raise AssertionError('planned grid/heldout slot count mismatch')
    rng = random.Random(plan['seeds']['order_seed'])
    for phase, seeds, configs in (('training', plan['seeds']['training'], tuple(c for v in grid.values() for c in v)),
                                  ('heldout', plan['seeds']['heldout'], modes)):
        for block, seed in enumerate(seeds):
            order = list(configs)
            rng.shuffle(order)
            rows = [r for r in records if r['phase'] == phase and r['block'] == block]
            if [(r['seed'], r['order'], r['configuration_key']) for r in rows] != [(seed, i, c.key) for i, c in enumerate(order)]:
                raise AssertionError('predeclared randomized order mismatch')
            if len({r['fixture_manifest_digest'] for r in rows}) != 1:
                raise AssertionError('paired actual input content mismatch')
    def rows(config):
        return [r for r in held if r['configuration_key'] == config.key]
    candidate = rows(selected['intelligent'])
    recomputed = [compare(role, rows(config), candidate, plan['seeds']['heldout']) for role, config in roles.items()]
    if recomputed != report['comparisons']:
        raise AssertionError('stored comparison differs from exact scalar replay')
    # Independent stock-evaluator parity for the common v2 scalar gates. The
    # stock adapter cannot run hybrids; this only checks stored numeric evidence.
    parity = []
    dummy = EvaluationConfiguration('thread', 1)
    def converted(values):
        return [TrialRecord('heldout', index, 0, row['seed'], 'stdlib', dummy,
            Measurement(row['lifecycle_seconds'], row['warm']['seconds'], row['correct'],
                        error_count=0, cpu_seconds=row['lifecycle_cpu_seconds'],
                        peak_rss_bytes=row['peak_rss_bytes'],
                        batch_p95_seconds=max(v['p95'] for v in row['warm']['latency'].values())),
            None if not row['exclusions'] else '|'.join(row['exclusions']), 0.)
                for index, row in enumerate(values)]
    scalar_plan = EvaluationPlan(heldout_blocks=24, family_alpha=.025, metric='warm_seconds')
    fields = ('wins_above_margin', 'sign_p_value', 'comparison_alpha', 'median_speedup',
              'lower_speedup', 'lower_order_statistic', 'log_speedup_mad', 'aggregate_speedup',
              'worst_pair_slowdown', 'minimum_observed_saving_seconds', 'mean_observed_saving_seconds')
    for comparison in recomputed:
        reference = _compare(comparison['role'], dummy, converted(rows(roles[comparison['role']])), converted(candidate), scalar_plan)
        values = asdict(reference)
        for field in fields:
            if field == 'mean_observed_saving_seconds' and values[field] is not None and comparison.get(field) is not None:
                # The stock evaluator normalizes its mean to avoid overflow;
                # this runner's finite local-scale fmean can differ by a ULP.
                if math.isclose(values[field], comparison[field], rel_tol=1e-12, abs_tol=1e-12):
                    continue
            if values[field] != comparison.get(field):
                raise AssertionError('stock evaluator scalar mismatch: ' + comparison['role'] + ':' + field)
        expected_reasons = set(reference.reasons)
        observed_reasons = {r for r in comparison['reasons'] if not r.endswith(':observed_p95_regression')}
        if expected_reasons != observed_reasons:
            raise AssertionError('stock evaluator rejection-gate mismatch')
        parity.append(comparison['role'])
    return {'verified': True, 'attempted_observations': len(records), 'training_observations': len(train),
            'heldout_observations': len(held), 'failed_observations': report['failed_observations'],
            'training_selections_recomputed': True, 'randomized_order_recomputed': True,
            'paired_actual_content_digests_verified': True, 'comparison_exact_replay': True,
            'stock_evaluator_scalar_parity': parity, 'mean_saving_parity_tolerance': {'relative': 1e-12, 'absolute_seconds': 1e-12}, 'status': report['status'],
            'selected': report['selected'], 'comparisons': recomputed,
            'scope': 'Read-only replay of a stored complete report; no new workload measurement, adoption permission, or independent replication.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = audit(load(args.report))
    rendered = json.dumps(result, indent=2, allow_nan=False) + '\n'
    if args.output:
        with args.output.open('x') as stream:
            stream.write(rendered)
    print(rendered)


if __name__ == '__main__':
    main()
