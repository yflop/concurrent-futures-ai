import types
import unittest
from unittest.mock import patch
from intelligent_futures.tuning import Measurement, CalibrationObjective
from benchmarks.real_world.resource_sampling import RSSSampler

class AccessDenied(Exception):pass
class NoSuchProcess(Exception):pass

class Process:
    def __init__(self,rss=100,children=(),failure=None):
        self.rss=rss;self.descendants=children;self.failure=failure;self.enumeration_failure=None
    def children(self,recursive=True):
        if self.enumeration_failure:raise self.enumeration_failure
        return self.descendants
    def memory_info(self):
        if self.failure:raise self.failure
        return types.SimpleNamespace(rss=self.rss)

def sampler(process):
    module=types.SimpleNamespace(Process=lambda:process,NoSuchProcess=NoSuchProcess)
    result=RSSSampler(interval=60,psutil_module=module);result._process=process
    return result

class ResourceSamplingTests(unittest.TestCase):
    def test_attribution_tracks_first_last_and_aggregate_peak(self):
        child=Process(50);parent=Process(100,[child]);s=sampler(parent)
        s.sample();parent.rss=120;child.rss=200;s.sample()
        parent.rss=140;child.rss=10;s.sample();report=s.report()
        self.assertEqual(report['first_complete_sample']['tree_rss_bytes'],150)
        self.assertEqual(report['last_complete_sample']['tree_rss_bytes'],150)
        self.assertEqual(report['peak_complete_sample'],{'tree_rss_bytes':320,
            'parent_rss_bytes':120,'descendant_rss_bytes':200,'observed_descendant_count':1})
        report['peak_complete_sample']['parent_rss_bytes']=0
        self.assertEqual(s.report()['peak_complete_sample']['parent_rss_bytes'],120)

    def test_attribution_does_not_restore_failed_collection_eligibility(self):
        parent=Process(100);s=sampler(parent);s.sample()
        parent.enumeration_failure=AccessDenied();s.sample()
        self.assertIsNone(s.report()['peak_rss_bytes'])
        self.assertFalse(s.report()['complete'])
        self.assertEqual(s.report()['peak_complete_sample']['parent_rss_bytes'],100)

    def test_complete_tree_samples(self):
        s=sampler(Process(100,[Process(50)]))
        with s:pass
        self.assertEqual(s.peak,150)
        self.assertEqual(s.samples,2)
        self.assertTrue(s.report()['complete'])

    def test_access_denied_child_never_accepts_partial_parent_rss(self):
        s=sampler(Process(100,[Process(failure=AccessDenied())]))
        with s:pass
        self.assertIsNone(s.peak)
        self.assertFalse(s.report()['complete'])
        self.assertIn('process_rss:AccessDenied',s.errors)

    def test_enumeration_error_invalidates_previous_peak(self):
        process=Process(100);s=sampler(process);s.sample()
        self.assertEqual(s.peak,100)
        process.enumeration_failure=AccessDenied();s.sample()
        process.enumeration_failure=None;s.sample()
        self.assertIsNone(s.peak)
        self.assertFalse(s.report()['complete'])

    def test_child_exit_race_is_counted_and_not_permission_failure(self):
        s=sampler(Process(100,[Process(failure=NoSuchProcess())]));s.sample()
        self.assertEqual(s.peak,100)
        self.assertEqual(s.exited_process_races,1)
        self.assertTrue(s.report()['complete'])

    def test_own_process_missing_is_invalid(self):
        s=sampler(Process(failure=NoSuchProcess()));s.sample()
        self.assertIsNone(s.peak)
        self.assertFalse(s.report()['complete'])

    def test_bad_metric_is_invalid_and_error_list_bounded(self):
        s=sampler(Process(rss=float('nan')))
        for _ in range(20):s.sample()
        self.assertIsNone(s.peak)
        self.assertEqual(len(s.errors),8)
        self.assertEqual(s.error_count,20)


    def test_thread_start_failure_invalidates_initial_sample(self):
        s=sampler(Process())
        with patch('threading.Thread.start',side_effect=RuntimeError('test failure')):
            with s:pass
        self.assertIsNone(s.peak)
        self.assertIn('thread_start:RuntimeError',s.errors)

    def test_final_sampling_failure_does_not_mask_workload_error(self):
        p=Process();s=sampler(p)
        with self.assertRaisesRegex(ValueError,'workload'):
            with s:
                p.enumeration_failure=RuntimeError('sampling')
                raise ValueError('workload')
        self.assertIsNone(s.peak)

    def test_missing_rss_is_ineligible_under_required_cap(self):
        s=sampler(Process(100,[Process(failure=AccessDenied())]))
        with s:pass
        metric=Measurement(1,1,True,peak_rss_bytes=s.peak)
        self.assertEqual(CalibrationObjective(max_peak_rss_bytes=1000).exclusion(metric),'rss_unavailable')

if __name__=='__main__':unittest.main()
