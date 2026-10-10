from dataclasses import replace
import contextlib
import io
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch
from benchmarks.real_world import optimizer


class OptimizerRunnerTests(unittest.TestCase):
    def test_plan_declares_complete_grid_and_disjoint_fixture_seeds(self):
        plan=optimizer.plan('images',8)
        self.assertEqual(len(plan['grid']),24)
        self.assertEqual(len(plan['training_fixture_seeds']),4)
        self.assertTrue(set(plan['training_fixture_seeds']).isdisjoint(plan['heldout_seeds']))
        self.assertEqual(plan['count_per_pass'],24)
        self.assertEqual(optimizer.digest(plan),optimizer.digest(optimizer.plan('images',8)))

    def test_full_flow_freezes_selection_before_all_heldout_candidates(self):
        module=types.SimpleNamespace(prepare=lambda name,root,count,seed,scale: [{'seed':seed}])
        events=[]
        def trial(name,engine,config,count,seed,root,prepared):
            heldout=seed in optimizer.HOLDOUT_SEEDS
            if heldout:
                saved=json.loads(output.read_text())
                self.assertEqual(len(saved['training']),96)
                self.assertEqual(len(saved['manual_training']),96)
                self.assertEqual(set(saved['selections']),{'exhaustive','exhaustive_median','budgeted','budgeted_lowcost'})
                self.assertEqual(optimizer.digest(saved['selections']),saved['selection_frozen_sha256'])
            events.append((heldout,config,seed))
            seconds=float(config.workers+config.batch_size)
            phase={'seconds':seconds,'correct':True,'errors':[], 'error_rate':0.,
                'batch_completion_latency_p95_seconds':seconds,
                'tasks':count,'throughput_per_second':count/seconds,'completion_trace':[]}
            return {'configuration':optimizer.asdict(config),'seed':seed,'engine':engine,
                'cold':dict(phase),'warm':dict(phase),'cold_lifecycle_seconds':seconds,
                'lifecycle_cpu_seconds':1.,'sampled_tree_peak_rss_bytes':1000}
        def direct(name,prepared,seed):
            row=trial(name,'sequential',optimizer.Configuration('thread',1,1),24,seed,None,prepared)
            row['configuration']=None
            return row
        with tempfile.TemporaryDirectory() as temp:
            output=Path(temp)/'result.json'
            with patch.object(optimizer,'module_for',return_value=module), \
                 patch.object(optimizer,'execute_batch',return_value=[{'value':1,'error':None}]), \
                 patch.object(optimizer,'trial',side_effect=trial), \
                 patch.object(optimizer,'direct_trial',side_effect=direct), \
                 patch.object(optimizer,'workload_environment',return_value={'test_only':True}), \
                 contextlib.redirect_stdout(io.StringIO()):
                report=optimizer.run('images',output)
        self.assertEqual(report['status'],'complete')
        self.assertEqual(len(report['training']),96)
        self.assertEqual(len(report['heldout']),72)
        self.assertEqual(len(report['manual_training']),96)
        manual_missing=getattr(self,'manual_missing',False)
        self.assertEqual(len(report['baseline_heldout']),6 if manual_missing else 9)
        self.assertEqual(report['observed_trials'],270 if manual_missing else 273)
        self.assertEqual(report['executed_task_observations'],12960 if manual_missing else 13104)
        self.assertEqual([held for held,_,_ in events],[False]*192+[True]*(78 if manual_missing else 81))
        self.assertEqual(report['selections']['exhaustive']['evaluations'],96)
        self.assertEqual(report['selections']['budgeted']['evaluations'],72)
        self.assertEqual(report['selections']['budgeted_lowcost']['evaluations'],48)
        self.assertEqual(report['selections']['budgeted']['reuse_calibration_calls'],0)
        self.last_report=report
        for method in ('exhaustive','budgeted'):
            self.assertTrue(report['summaries'][method]['combined']['oracle_valid'])
            self.assertEqual(report['summaries'][method]['combined']['aggregate_relative_regret'],0.)


    def test_secondary_no_selection_preserves_primary_and_full_oracle(self):
        original=optimizer.tune
        def no_secondary(*args,**kwargs):
            report=original(*args,**kwargs)
            return replace(report,selected=None) if kwargs.get('min_repeats')==1 else report
        with patch.object(optimizer,'tune',side_effect=no_secondary):
            self.test_full_flow_freezes_selection_before_all_heldout_candidates()
        report=self.last_report
        self.assertIsNone(report['selections']['budgeted_lowcost']['selected'])
        self.assertEqual(report['selections']['budgeted_lowcost']['selection_status'],'no_eligible_configuration')
        self.assertEqual(report['summaries']['budgeted_lowcost']['combined']['coverage_denominator'],3)
        self.assertEqual(report['summaries']['budgeted_lowcost']['combined']['within_10_percent_count'],0)
        self.assertTrue(report['summaries']['budgeted']['combined']['quality_gate_eligible'])
        self.assertFalse(report['accepted'])

    def test_manual_no_selection_retains_other_baselines_and_missing_slots(self):
        original=optimizer.calibrate;calls=[]
        def no_manual(*args,**kwargs):
            report=original(*args,**kwargs);calls.append(True)
            return replace(report,selected=None) if len(calls)==2 else report
        self.manual_missing=True
        with patch.object(optimizer,'calibrate',side_effect=no_manual):
            self.test_full_flow_freezes_selection_before_all_heldout_candidates()
        report=self.last_report
        self.assertIsNone(report['manual_proxy_selection']['selected'])
        comparisons=report['baseline_comparisons']['budgeted']
        self.assertEqual(len(comparisons['comparisons']),27)
        self.assertEqual(comparisons['expected_comparison_slots'],27)
        self.assertEqual(comparisons['valid_comparison_slots'],18)
        missing=[r for r in comparisons['comparisons'] if r['baseline_role']=='standard_library_manual_tuning_proxy']
        self.assertEqual(len(missing),9)
        self.assertTrue(all(r['baseline_time_over_selected_time'] is None for r in missing))
        self.assertFalse(report['accepted'])


if __name__=='__main__':
    unittest.main()
