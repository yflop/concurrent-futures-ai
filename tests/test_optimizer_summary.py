import json
from pathlib import Path
import tempfile
import unittest
from tools.summarize_optimizer import summary, execution_capacity

class SummaryFailureTests(unittest.TestCase):
    def test_failed_and_missing_workloads_stay_in_planned_denominators(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            (root/'images.json').write_text(json.dumps({'status':'failed','failures':['test interruption'],'training':[{}]}))
            report=summary(root)
        self.assertEqual(report['workloads_completed'],0)
        self.assertEqual(report['workloads_planned'],11)
        self.assertEqual(report['incomplete_workload_records'][0]['recorded_trials'],1)
        for method in ('exhaustive','exhaustive_median','budgeted','budgeted_lowcost'):
            self.assertEqual(report[method]['aggregate_workload_denominator'],11)
            self.assertEqual(report[method]['combined_seed_denominator'],33)
            self.assertEqual(report[method]['cold_warm_seed_denominator'],66)
            self.assertEqual(report[method]['aggregate_workload_within10_count'],0)
            self.assertEqual(report[method]['defined_regret_workload_count'],0)

class ExecutionCapacityTests(unittest.TestCase):
    def test_small_fixture_cannot_exercise_nominal_workers(self):
        for workers in (2,4,8):
            result=execution_capacity({'backend':'thread','workers':workers,'batch_size':16},24)
            self.assertEqual(result['runnable_batches_per_pass'],2)
            self.assertEqual(result['maximum_simultaneously_runnable_batches'],2)
        self.assertEqual(execution_capacity({'backend':'process','workers':8,'batch_size':16},128)['maximum_simultaneously_runnable_batches'],8)
        self.assertIsNone(execution_capacity(None,24))
