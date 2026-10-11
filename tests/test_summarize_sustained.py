from copy import deepcopy
from dataclasses import asdict
import random
import statistics
import unittest

from benchmarks.real_world import adoption_sustained as experiment
from tools.summarize_sustained import audit


def fixture():
    plan = experiment.predeclared_plan()
    grid = experiment.grids()
    chosen = {'single': experiment.Configuration('stdlib', 'process', 0, 8, 4),
              'static': experiment.Configuration('stdlib', 'static', 2, 6, 1),
              'overflow': experiment.Configuration('stdlib', 'overflow', 2, 6, 4),
              'intelligent': experiment.Configuration('intelligent', 'adaptive', 2, 6, 4)}
    warm = {chosen['single']: 1.2, chosen['static']: 1.3,
            chosen['overflow']: 1.4, chosen['intelligent']: 1.0}
    fixed = experiment.Configuration(**plan['fixed'])
    roles = {'fixed': fixed, 'single': chosen['single'], 'static': chosen['static'], 'overflow': chosen['overflow']}
    modes = tuple(dict.fromkeys((*roles.values(), chosen['intelligent'])))
    rng = random.Random(plan['seeds']['order_seed'])
    records = []
    for phase, seeds, configs in (('training', plan['seeds']['training'], tuple(c for v in grid.values() for c in v)),
                                  ('heldout', plan['seeds']['heldout'], modes)):
        for block, seed in enumerate(seeds):
            order = list(configs)
            rng.shuffle(order)
            for ordinal, config in enumerate(order):
                seconds = warm.get(config, 2.)
                records.append({'seed': seed, 'phase': phase, 'block': block, 'order': ordinal,
                    'configuration_key': config.key, 'correct': True, 'exclusions': [],
                    'lifecycle_seconds': seconds + .1, 'lifecycle_cpu_seconds': .1, 'peak_rss_bytes': 1024,
                    'fixture_manifest_digest': str(seed), 'source_identity': {'test': 1}, 'affinity': [0, 1],
                    'warm': {'seconds': seconds, 'latency': {family: {'p95': .2}
                                                          for family in ('all', 'geospatial', 'http')}}})
    def rows(config):
        return [r for r in records if r['phase'] == 'heldout' and r['configuration_key'] == config.key]
    comparisons = [experiment.compare(role, rows(config), rows(chosen['intelligent']), plan['seeds']['heldout'])
                   for role, config in roles.items()]
    return {'phase': 'confirmation', 'status': 'warm_execution_supported_adoption_horizon_unknown',
        'plan': plan, 'plan_sha256': experiment.digest(plan), 'records': records,
        'source_identity': {'test': 1}, 'expected_affinity': [0, 1],
        'planned_observations': len(records), 'attempted_observations': len(records),
        'settled_observations': len(records), 'skipped_observations': 0, 'failed_observations': 0,
        'selected': {label: asdict(c) for label, c in chosen.items()}, 'comparisons': comparisons}


class AuditTests(unittest.TestCase):
    def test_complete_synthetic_report_replays_against_stock_evaluator(self):
        result = audit(fixture())
        self.assertTrue(result['verified'])
        self.assertEqual(result['attempted_observations'], 216)
        self.assertEqual(result['stock_evaluator_scalar_parity'], ['fixed', 'single', 'static', 'overflow'])

    def test_modified_timing_and_missing_slot_do_not_replay(self):
        report = fixture()
        report['records'][-1]['warm']['seconds'] *= 2
        with self.assertRaises(AssertionError):
            audit(report)
        report = fixture()
        report['records'].pop()
        with self.assertRaises(AssertionError):
            audit(report)

    def test_order_input_digest_and_selection_are_checked(self):
        for modification in ('order', 'fixture_manifest_digest', 'selection'):
            report = fixture()
            if modification == 'selection':
                report['selected']['single']['batch'] = 1
            else:
                report['records'][0][modification] = 'changed'
            with self.assertRaises(AssertionError):
                audit(report)


if __name__ == '__main__':
    unittest.main()
