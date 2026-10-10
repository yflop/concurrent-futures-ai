"""Small deterministic checks of the serial evidence harness, not benchmarks."""
from contextlib import nullcontext
from dataclasses import asdict
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from intelligent_futures.evaluation import EvaluationPlan
from benchmarks.real_world import adoption


class AdoptionRunnerTests(unittest.TestCase):
    def test_digest_is_ordered_and_stable(self):
        self.assertEqual(adoption._digest([{'a':1,'b':2}]),adoption._digest([{'b':2,'a':1}]))
        self.assertNotEqual(adoption._digest([1,2]),adoption._digest([2,1]))

    def test_counted_fixture_oracles_and_grid(self):
        prepared=[]
        module=SimpleNamespace(
            prepare=lambda name,path,count,seed,scale:prepared.append((str(path),seed)) or list(range(count)),
            run_task=lambda task:task*task)
        class Sampler:
            peak=1234
            def __enter__(self):return self
            def __exit__(self,*args):return False
            def report(self):return {'samples':2,'peak':self.peak}
        # Measure once per engine, sharing seed for oracle reuse. The evaluator
        # itself is tested independently; this verifies the actual harness scope.
        def evaluate(workload,**kwargs):
            self.assertEqual(len(kwargs['stdlib_configurations']),4)
            self.assertEqual(len(kwargs['intelligent_configurations']),5)
            self.assertEqual(kwargs['fixed_baseline'].workers,2)
            self.assertEqual(kwargs['plan'].max_pending,4)
            for engine,expected_cpu in (('stdlib',.25),('intelligent',.5)):
                m=workload.measure(engine,kwargs['fixed_baseline'],7)
                self.assertTrue(m.correct)
                self.assertEqual(m.peak_rss_bytes,1234)
                self.assertEqual(m.cpu_seconds,expected_cpu)
            return SimpleNamespace(attempted_evaluations=2,to_json=lambda:{'status':'test'})
        with patch.object(adoption,'module_for',return_value=module),patch.object(adoption,'RSSSampler',Sampler),\
             patch.object(adoption,'cpu_seconds',side_effect=(10.,10.25,20.,20.5)),\
             patch.object(adoption,'evaluate_workload',side_effect=evaluate):
            row=adoption.evaluate_family('test',count=3,scale='representative',
                                        plan=EvaluationPlan(max_workers=2,max_pending=4))
        self.assertEqual(row['sequential_oracle_fixtures'],1)
        self.assertEqual(row['sequential_oracle_logical_items'],3)
        self.assertEqual(row['measured_logical_items_attempted_upper_bound'],6)
        self.assertEqual(len(prepared),3)
        self.assertEqual(len(row['resource_collection']),2)
        self.assertNotEqual(prepared[0][0],prepared[-1][0])

    def test_unavailable_cpu_snapshots_remain_missing(self):
        module=SimpleNamespace(
            prepare=lambda name,path,count,seed,scale:list(range(count)),
            run_task=lambda task:task*task)
        sampler=SimpleNamespace(peak=1234,report=lambda:{'samples':2,'peak':1234})
        def evaluate(workload,**kwargs):
            for engine in ('stdlib','intelligent'):
                measurement=workload.measure(engine,kwargs['fixed_baseline'],7)
                self.assertTrue(measurement.correct)
                self.assertEqual(measurement.peak_rss_bytes,1234)
                self.assertIsNone(measurement.cpu_seconds)
            return SimpleNamespace(attempted_evaluations=2,to_json=lambda:{'status':'test'})
        # Platforms without resource, or an unavailable endpoint, must not
        # fabricate a zero-CPU observation that could pass a resource gate.
        for snapshots in ((None,None),(None,10.),(10.,None)):
            with self.subTest(snapshots=snapshots),\
                 patch.object(adoption,'module_for',return_value=module),\
                 patch.object(adoption,'RSSSampler',side_effect=lambda:nullcontext(sampler)),\
                 patch.object(adoption,'cpu_seconds',side_effect=snapshots*2),\
                 patch.object(adoption,'evaluate_workload',side_effect=evaluate):
                adoption.evaluate_family('test',count=1,scale='representative',
                    plan=EvaluationPlan(max_workers=2,max_pending=4))


if __name__=='__main__':unittest.main()
