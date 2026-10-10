import dataclasses
import json
import unittest
from unittest.mock import patch

from intelligent_futures.prediction import (
    ExecutionObjective, HardwareCapabilities, MAX_JSON_BYTES, MAX_SAMPLES,
    Predictor, RuntimeSample, WorkloadProfile, normalize_features, _quota,
)


class HardwareTests(unittest.TestCase):
    def test_conservative_constraints(self):
        h = HardwareCapabilities(16, 8, 2.8)
        self.assertEqual(h.usable_cpus, 2)
        self.assertEqual(HardwareCapabilities(16, 8, .25).usable_cpus, 1)
        self.assertEqual(HardwareCapabilities(2, 8, 16).usable_cpus, 2)
        self.assertEqual(len(h.fingerprint), 64)
        self.assertEqual(json.loads(json.dumps(dataclasses.asdict(h)))['logical_cpus'], 16)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            h.logical_cpus = 12

    def test_invalid(self):
        for kwargs in ({'logical_cpus': True}, {'logical_cpus': 0}, {'affinity_cpus': 0},
                       {'quota_cpus': float('nan')}, {'quota_cpus': -1}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                HardwareCapabilities(**kwargs)

    def test_detect_available_and_missing(self):
        with patch('os.cpu_count', return_value=16), patch('os.sched_getaffinity', return_value={1, 2}, create=True), patch('intelligent_futures.prediction._quota', return_value=1.5):
            self.assertEqual(HardwareCapabilities.detect().usable_cpus, 1)
        with patch('os.cpu_count', return_value=None), patch('os.sched_getaffinity', side_effect=OSError, create=True), patch('intelligent_futures.prediction._quota', return_value=None):
            self.assertEqual(HardwareCapabilities.detect().usable_cpus, 1)

    def test_cpu_model_is_bounded_nonidentifying_and_affects_fingerprint(self):
        with patch('intelligent_futures.prediction._read', return_value='serial: secret-id\nmodel name: Example CPU\n'), patch('intelligent_futures.prediction._quota', return_value=None):
            hardware = HardwareCapabilities.detect()
            self.assertEqual(hardware.cpu_model, 'Example CPU')
            self.assertNotIn('secret-id', repr(hardware))
        self.assertNotEqual(HardwareCapabilities(cpu_model='A').fingerprint,
                            HardwareCapabilities(cpu_model='B').fingerprint)
        with self.assertRaises(ValueError):
            HardwareCapabilities(cpu_model='x' * 257)
        self.assertNotIn('fingerprint', dataclasses.asdict(hardware))

    def test_cgroups_v2_nested_and_v1(self):
        files = {'/proc/self/cgroup': '0::/team/job', '/sys/fs/cgroup/cpu.max': 'max 100000',
                 '/sys/fs/cgroup/team/cpu.max': '150000 100000',
                 '/sys/fs/cgroup/team/job/cpu.max': '300000 100000'}
        with patch('intelligent_futures.prediction._read', side_effect=lambda p: files.get(str(p), '')):
            self.assertEqual(_quota(), 1.5)
        files = {'/proc/self/cgroup': '2:cpu,cpuacct:/job',
                 '/sys/fs/cgroup/cpu/job/cpu.cfs_quota_us': '50000',
                 '/sys/fs/cgroup/cpu/job/cpu.cfs_period_us': '100000'}
        with patch('intelligent_futures.prediction._read', side_effect=lambda p: files.get(str(p), '')):
            self.assertEqual(_quota(), .5)
        with patch('intelligent_futures.prediction._read', return_value='nonsense'):
            self.assertIsNone(_quota())


class ProfileTests(unittest.TestCase):
    def test_roundtrip_immutable_features_and_buckets(self):
        features = {'n': 100, 'bytes': 4096}
        p = WorkloadProfile('job', features, [RuntimeSample(2, .5), RuntimeSample(4, 1)])
        features['n'] = 200
        self.assertEqual(dict(p.features)['n'], 100)
        self.assertEqual(WorkloadProfile.from_json(p.to_json()), p)
        bucket, = p.scaling_curve()
        self.assertEqual(bucket.samples, 2)
        self.assertEqual(bucket.provenance, 'observational-concurrency-bucket')
        self.assertEqual(bucket.mean_wall_seconds, 3)
        self.assertEqual(bucket.mean_thread_cpu_seconds, .75)
        self.assertEqual(normalize_features({'b': 2, 'a': 1}), (('a', 1.0), ('b', 2.0)))

    def test_ring_bound(self):
        p = WorkloadProfile('job')
        for i in range(MAX_SAMPLES + 20):
            p = p.with_sample(RuntimeSample(i))
        self.assertEqual(len(p.samples), MAX_SAMPLES)
        self.assertEqual(p.samples[0].wall_seconds, 20)

    def test_cached_curve_tracks_ring_eviction_and_preserves_old_snapshot(self):
        profile = WorkloadProfile('cached')
        for i in range(300):
            previous = profile
            profile = profile.with_sample(RuntimeSample((i % 7 + 1) / 10,
                (i % 5) / 100 if i % 3 else None, concurrency=(i % 4) + 1))
            self.assertIs(previous.scaling_curve(), previous.scaling_curve())
            reconstructed = WorkloadProfile('cached', samples=profile.samples)
            for actual, expected in zip(profile.scaling_curve(), reconstructed.scaling_curve()):
                self.assertEqual((actual.backend, actual.concurrency, actual.samples),
                                 (expected.backend, expected.concurrency, expected.samples))
                self.assertAlmostEqual(actual.mean_wall_seconds, expected.mean_wall_seconds)
                self.assertAlmostEqual(actual.mean_thread_cpu_seconds, expected.mean_thread_cpu_seconds)
        self.assertEqual(len(profile.samples), MAX_SAMPLES)

    def test_dominant_eviction_retains_tiny_measurements(self):
        for tiny in (1e-12, 1e-6):
            profile = WorkloadProfile('dynamic', samples=(RuntimeSample(1e9, 1e9),) +
                                      (RuntimeSample(tiny, tiny),) * (MAX_SAMPLES - 1))
            updated = profile.with_sample(RuntimeSample(tiny, tiny))
            bucket, = updated.scaling_curve()
            self.assertAlmostEqual(bucket.mean_wall_seconds / tiny, 1)
            self.assertAlmostEqual(bucket.mean_thread_cpu_seconds / tiny, 1)
            restored, = WorkloadProfile.from_json(updated.to_json()).scaling_curve()
            self.assertAlmostEqual(bucket.mean_wall_seconds / restored.mean_wall_seconds, 1)

    def test_reject_untrusted_json(self):
        valid = json.loads(WorkloadProfile('job').to_json())
        bad = ['null', '[]', '{"version":1,"version":1}', '[' * 2000 + ']' * 2000,
               ' ' * (MAX_JSON_BYTES + 1), '\ud800', b'\xff']
        for field, value in [('version', True), ('version', 2), ('key', []),
                             ('features', [['x', float('nan')]]), ('features', [['x', 'raw secret']]),
                             ('features', [['x', 1], ['x', 2]]), ('samples', [{}]),
                             ('samples', [{}] * (MAX_SAMPLES + 1))]:
            bad.append(json.dumps(dict(valid, **{field: value})))
        bad.append(json.dumps(dict(valid, unexpected='raw input')))
        for payload in bad:
            with self.subTest(payload=str(payload)[:60]), self.assertRaises(ValueError):
                WorkloadProfile.from_json(payload)

    def test_invalid_measurements_and_features(self):
        for value in [True, -1, float('inf'), float('nan'), 10**1000]:
            with self.assertRaises(ValueError):
                RuntimeSample(value)
        for features in [[('a', True)], [('a', object())], [('x', 1)] * 33, [('x', 1, 2)]]:
            with self.assertRaises(ValueError):
                normalize_features(features)


class PredictorTests(unittest.TestCase):
    def setUp(self):
        self.hw = HardwareCapabilities(4, 2, 1.8, 'test', 'test')

    def test_objectives_and_constraints(self):
        for kwargs in [{'mode': 'magic'}, {'min_concurrency': 0}, {'max_concurrency': True},
                       {'min_concurrency': 4, 'max_concurrency': 2}, {'memory_limit_bytes': 100}]:
            with self.assertRaises(ValueError):
                ExecutionObjective(**kwargs)
        p = Predictor(self.hw, ExecutionObjective(min_concurrency=2, max_concurrency=3))
        result = p.recommend('job', kind='cpu', process_safe=True, process_available=True)
        self.assertEqual((result.backend, result.concurrency, result.source), ('process', 2, 'heuristic'))
        self.assertEqual(p.recommend('job', kind='cpu', process_available=True).backend, 'thread')

    def test_empirical_exact_match_and_transfer_unknown(self):
        p = Predictor(self.hw)
        for _ in range(2):
            p.record('job', RuntimeSample(.1, .05, concurrency=2), {'n': 5})
            p.record('job', RuntimeSample(.1, .05, concurrency=1), {'n': 5})
        result = p.recommend('job', {'n': 5})
        self.assertEqual((result.source, result.concurrency), ('empirical', 2))
        self.assertEqual(result.transfer, 'fingerprint-match')
        self.assertEqual(result.confidence, 'observational')
        self.assertEqual(p.recommend('job', {'n': 6}).source, 'heuristic')
        other = Predictor(HardwareCapabilities(8), profiles=p.profiles)
        self.assertEqual(other.recommend('job', {'n': 5}).source, 'transfer-prior')
        self.assertEqual(other.recommend('job', {'n': 5}).transfer, 'unvalidated')

    def test_profile_never_grants_process_consent(self):
        p = Predictor(self.hw, ExecutionObjective('latency'))
        for _ in range(2):
            p.record('job', RuntimeSample(.01, backend='process'))
        self.assertEqual(p.recommend('job').backend, 'thread')
        self.assertEqual(p.recommend('job', process_available=True).backend, 'thread')
        self.assertEqual(p.recommend('job', kind='io', process_safe=True, process_available=True).backend, 'thread')
        self.assertEqual(p.recommend('job', process_safe=True, process_available=True).source, 'empirical')

    def test_observed_objective_ranking_and_hard_bounds(self):
        p = Predictor(self.hw)
        for _ in range(2):
            p.record('job', RuntimeSample(1, concurrency=1))
            p.record('job', RuntimeSample(2, concurrency=4))
        latency = Predictor(self.hw, ExecutionObjective('latency'), p.profiles)
        throughput = Predictor(self.hw, ExecutionObjective('throughput'), p.profiles)
        bounded = Predictor(self.hw, ExecutionObjective('throughput', max_concurrency=2), p.profiles)
        self.assertEqual(latency.recommend('job').concurrency, 1)
        self.assertEqual(throughput.recommend('job').concurrency, 4)
        self.assertLessEqual(bounded.recommend('job').concurrency, 2)
        self.assertEqual(p.recommend('job').concurrency, 1)

    def test_single_bucket_does_not_freeze_bursts(self):
        p = Predictor(self.hw)
        for _ in range(2):
            p.record('io', RuntimeSample(1, .01))
            p.record('cpu', RuntimeSample(1, .99))
        self.assertEqual(p.recommend('io').source, 'measurement-guided')
        self.assertEqual(p.recommend('io').concurrency, 5)
        self.assertEqual(p.recommend('cpu').concurrency, 1)
        self.assertIsNone(p.recommend('io').expected_wall_seconds)

    def test_foreign_cpu_prior_scales_down_but_not_speculatively_up(self):
        source = HardwareCapabilities(8, machine='source', system='test')
        profile = WorkloadProfile('cpu', {'n': 5},
            (RuntimeSample(1, .9, concurrency=8),) * 3, hardware=source)
        self.assertEqual(WorkloadProfile.from_json(profile.to_json()), profile)
        for cpus, expected in [(2, 2), (16, 8)]:
            predictor = Predictor(HardwareCapabilities(cpus), profiles=(profile,))
            result = predictor.recommend('cpu', {'n': 5})
            self.assertEqual(result.source, 'transfer-prior')
            self.assertEqual(result.transfer, 'unvalidated')
            self.assertEqual(result.confidence, 'unvalidated')
            self.assertEqual(result.concurrency, expected)
            self.assertEqual(result.backend, 'thread')
            self.assertIsNone(result.expected_wall_seconds)
            self.assertEqual(predictor.recommend('cpu', {'n': 6}).source, 'heuristic')

    def test_foreign_wait_prior_and_local_override(self):
        source = HardwareCapabilities(8, machine='source', system='test')
        profile = WorkloadProfile('io', samples=(RuntimeSample(1, .01, concurrency=8),) * 3,
                                  hardware=source)
        target = Predictor(HardwareCapabilities(4), profiles=(profile,))
        prior = target.recommend('io')
        self.assertEqual(prior.concurrency, 4)
        self.assertEqual(prior.source, 'transfer-prior')
        target.record('io', RuntimeSample(1, .99))
        local = target.recommend('io')
        self.assertEqual(local.source, 'measurement-guided')
        self.assertEqual(local.transfer, 'fingerprint-match')
        self.assertIsNone(local.expected_wall_seconds)

    def test_foreign_prior_requires_source_and_cpu_provenance(self):
        source = HardwareCapabilities(8)
        profiles = [
            WorkloadProfile('job', samples=(RuntimeSample(1, .5),) * 2),
            WorkloadProfile('job', samples=(RuntimeSample(1),) * 2, hardware=source),
            WorkloadProfile('job', samples=(RuntimeSample(1, .5, backend='process'),) * 2, hardware=source),
            WorkloadProfile('job', samples=(RuntimeSample(1, .5),), hardware=source),
        ]
        for profile in profiles:
            result = Predictor(HardwareCapabilities(2), profiles=(profile,)).recommend('job')
            self.assertEqual(result.source, 'heuristic')
            self.assertEqual(result.backend, 'thread')
        with self.assertRaises(ValueError):
            WorkloadProfile('job', hardware=source, hardware_fingerprint='false')
        data = json.loads(WorkloadProfile('job', hardware=source).to_json())
        data['hardware']['logical_cpus'] = True
        with self.assertRaises(ValueError):
            WorkloadProfile.from_json(json.dumps(data))

    def test_foreign_prior_preserves_process_consent_and_bounds(self):
        profile = WorkloadProfile('job', samples=(RuntimeSample(1, .99, concurrency=8),) * 2,
                                  hardware=HardwareCapabilities(8))
        target = Predictor(HardwareCapabilities(4), ExecutionObjective(max_concurrency=2), (profile,))
        self.assertEqual(target.recommend('job', kind='cpu').backend, 'thread')
        self.assertEqual(target.recommend('job', kind='cpu', process_available=True).backend, 'thread')
        approved = target.recommend('job', kind='cpu', process_safe=True, process_available=True)
        self.assertEqual(approved.backend, 'process')
        self.assertLessEqual(approved.concurrency, 2)
        self.assertIsNone(approved.expected_wall_seconds)

    def test_lru_bounds(self):
        p = Predictor(self.hw, max_profiles=2)
        p.record('one', RuntimeSample(.1))
        p.record('two', RuntimeSample(.1))
        p.recommend('one')
        p.record('three', RuntimeSample(.1))
        self.assertEqual({v.key for v in p.profiles}, {'one', 'three'})
        with self.assertRaises(ValueError):
            Predictor(max_profiles=0)


if __name__ == '__main__':
    unittest.main()
