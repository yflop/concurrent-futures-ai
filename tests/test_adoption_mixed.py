"""The mixed benchmark plan is fixed before any performance measurements."""
from pathlib import Path
from concurrent.futures import CancelledError
from contextlib import nullcontext
import json
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from benchmarks.real_world import adoption_mixed


class MixedAdoptionPlanTests(unittest.TestCase):
    def test_task_cancellation_aborts_instead_of_becoming_measurement(self):
        module=SimpleNamespace(prepare=lambda *a,**k:list(range(4)),fixture=lambda root:nullcontext())
        class Future:
            def result(self):raise CancelledError('not report data')
        class Pool:
            def __enter__(self):return self
            def __exit__(self,*a):return False
            def submit(self,*a,**k):return Future()
        class Sampler:
            peak=1000
            def __enter__(self):return self
            def __exit__(self,*a):return False
            def report(self):return {}
        def evaluate(workload,**kwargs):
            return workload.measure('stdlib',kwargs['fixed_baseline'],123)
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'interrupted.json'
            with patch.object(adoption_mixed,'workload_environment',return_value={}), \
                 patch.object(adoption_mixed,'module_for',return_value=module), \
                 patch.object(adoption_mixed,'run_item',return_value=1), \
                 patch.object(adoption_mixed,'make_executor',return_value=Pool()), \
                 patch.object(adoption_mixed,'RSSSampler',Sampler), \
                 patch.object(adoption_mixed,'evaluate_workload',side_effect=evaluate):
                with self.assertRaises(CancelledError):adoption_mixed.run(path)
            self.assertEqual(json.loads(path.read_text())['status'],'running')

    def test_fixed_grid_scope_and_no_overwrite(self):
        seen=[]
        def evaluate(workload,**kwargs):
            seen.append(workload)
            self.assertTrue(workload.repeatable and workload.independent)
            self.assertEqual(workload.measure._evaluation_max_pending,8)
            self.assertTrue(workload.measure._evaluation_process_safe)
            plan=kwargs['plan']
            self.assertEqual((plan.training_blocks,plan.heldout_blocks,plan.max_workers),(3,24,4))
            self.assertEqual(plan.minimum_speedup,.05)
            self.assertEqual(len(kwargs['stdlib_configurations']),4)
            hybrid=kwargs['intelligent_configurations'][0]
            self.assertEqual((hybrid.backend,hybrid.workers,hybrid.process_workers),('adaptive',4,2))
            return SimpleNamespace(attempted_evaluations=0,failed_evaluations=0,status='inconclusive',
                reasons=('test_only',),to_json=lambda:{'status':'test_fixture_only'})
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'report.json'
            with patch.object(adoption_mixed,'workload_environment',return_value={'test':True}),\
                 patch.object(adoption_mixed,'evaluate_workload',side_effect=evaluate):
                result=adoption_mixed.run(path)
                with self.assertRaisesRegex(ValueError,'new report path'):adoption_mixed.run(path)
        self.assertEqual(len(seen),1)
        self.assertEqual(result['predeclared_plan']['comparator_scope'],
            'tuned single-backend stdlib, not expert hybrid pipelines')
        self.assertEqual(result['workloads'][0]['logical_items_per_fixture'],8)


if __name__=='__main__':unittest.main()
