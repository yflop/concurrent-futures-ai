import unittest
from benchmarks.real_world.optimizer_variants import reweight_heavy

class OptimizerVariantTests(unittest.TestCase):
    def test_heavy_image_mix_is_predeclared_and_inputs_unchanged(self):
        tasks=[{'source_size':(i+1,10),'id':i} for i in range(24)]
        result,meta=reweight_heavy('images',tasks)
        self.assertEqual(len(result),24)
        self.assertEqual([r['id'] for r in result],list(range(18,24))*3+list(range(6)))
        self.assertEqual([r['id'] for r in tasks],list(range(24)))
        self.assertGreater(sum(meta['shifted_weights']),sum(meta['original_weights']))
        self.assertTrue(meta['duplicates_intentional'])

    def test_route_pairs_define_geospatial_shift(self):
        tasks=[{'origins':list(range(i+1)),'destinations':[1,2]} for i in range(24)]
        _,meta=reweight_heavy('geospatial',tasks)
        self.assertEqual(meta['shifted_weights'][:6],[38,40,42,44,46,48])

    def test_unsupported_shift_never_silently_runs_unshifted(self):
        with self.assertRaises(ValueError):
            reweight_heavy('ocr',[])


class LiveFailureAccountingTests(unittest.TestCase):
    def test_failed_trial_is_retained_and_costed_with_tuner_records(self):
        import contextlib
        import io
        import json
        from pathlib import Path
        import tempfile
        import types
        from unittest.mock import patch
        from benchmarks.real_world import optimizer_variants as variant
        from intelligent_futures.tuning import load_configuration
        calls=[]
        def fake_trial(name,engine,config,count,seed,root,prepared):
            calls.append((config,seed))
            if len(calls)==1:
                raise RuntimeError('planned trial failure')
            seconds=float(config.workers+config.batch_size)
            phase={'seconds':seconds,'correct':True,'errors':[],'error_rate':0.,
                'batch_completion_latency_p95_seconds':seconds,'tasks':count,
                'throughput_per_second':count/seconds}
            return {'configuration':variant.asdict(config),'seed':seed,'engine':engine,
                'cold':dict(phase),'warm':dict(phase),'cold_lifecycle_seconds':seconds,
                'lifecycle_cpu_seconds':1.,'sampled_tree_peak_rss_bytes':1000}
        loads=[]
        def fake_load(*args,**kwargs):
            loads.append(1)
            if len(loads)==2:
                raise ValueError('hardware fingerprint mismatch')
            return load_configuration(*args,**kwargs)
        module=types.SimpleNamespace(prepare=lambda *args,**kwargs:[{}])
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);baseline=root/'base.json';output=root/'output.json'
            baseline.write_text(json.dumps({'protocol':{'workload':'http'},'selection_frozen_sha256':'test',
                'selections':{'budgeted':{'selected':variant.asdict(variant.CANDIDATES[0])}}}))
            with patch.object(variant,'module_for',return_value=module), \
                 patch.object(variant,'execute_batch',return_value=[{'value':1,'error':None}]), \
                 patch.object(variant,'trial',side_effect=fake_trial), \
                 patch.object(variant,'workload_environment',return_value={'synthetic_test':True}), \
                 patch.object(variant,'load_configuration',side_effect=fake_load), \
                 patch.object(variant.os,'sched_getaffinity',return_value=set(range(8)),create=True), \
                 patch.object(variant.os,'sched_setaffinity',create=True), contextlib.redirect_stdout(io.StringIO()):
                report=variant.run(baseline,output,'live')
        self.assertIn('failure',report['training'][0])
        self.assertGreaterEqual(report['training'][0]['measured_callback_seconds'],0)
        self.assertEqual(len(report['training']),len(report['tuning_report']['records']))
        self.assertEqual(report['live_callback_seconds'],sum(r['measured_callback_seconds'] for r in report['training']))
        self.assertEqual(sum('failure' in r for r in report['training']),1)
        self.assertIsNone(report['tuning_report']['records'][0]['measurement'])

if __name__=="__main__":unittest.main()
