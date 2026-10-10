"""Synthetic protocol accounting; these timings are not workload evidence."""
import copy
import json
import unittest
from intelligent_futures.evaluation import (EvaluationConfiguration, EvaluationPlan,
    RepeatableWorkload, evaluate_workload)
from intelligent_futures.tuning import Measurement
from tools.summarize_adoption import summarize
from tools.recompute_adoption_decisions import recompute


class AdoptionSummaryTests(unittest.TestCase):
    def payload(self):
        config=EvaluationConfiguration('thread',1)
        workload=RepeatableWorkload('synthetic','v1',lambda engine,c,s:Measurement(1,1,True),True,True)
        report=evaluate_workload(workload,stdlib_configurations=(config,),intelligent_configurations=(config,),
            fixed_baseline=config,plan=EvaluationPlan(training_blocks=1,heldout_blocks=6))
        return json.loads(json.dumps({'status':'complete','workloads':[{'name':'test','report':report.to_json()}]}))

    def test_exact_scalar_recomputation(self):
        summary=summarize(self.payload())
        self.assertEqual(summary['attempted_evaluations'],14)
        self.assertEqual(summary['failed_evaluations'],0)
        self.assertEqual(summary['workloads'][0]['status'],'retain_stdlib')

    def test_scalar_decision_recomputation_runs_no_tasks_and_rejects_status_change(self):
        proof=recompute(self.payload())
        self.assertEqual(proof['task_executions'],0)
        self.assertEqual(proof['scalar_observations'],14)
        self.assertEqual(proof['comparison_count'],2)
        self.assertTrue(proof['workloads'][0]['exact_decision_and_selection_match'])
        payload=self.payload();payload['workloads'][0]['report']['status']='inconclusive'
        with self.assertRaisesRegex(ValueError,'decision changed'):recompute(payload)

    def test_incomplete_and_modified_gate_rejected(self):
        payload=self.payload();payload['status']='running'
        with self.assertRaisesRegex(ValueError,'not complete'):summarize(payload)
        payload=self.payload();payload['workloads'][0]['report']['comparisons'][0]['passed']=True
        with self.assertRaisesRegex(ValueError,'recomputation'):summarize(payload)

    def test_missing_or_miscounted_slots_rejected(self):
        for field in ('attempted_evaluations','failed_evaluations','successful_evaluations','expected_evaluations'):
            payload=self.payload();payload['workloads'][0]['report'][field]+=1
            with self.subTest(field=field),self.assertRaises(ValueError):summarize(payload)


if __name__=='__main__':unittest.main()
