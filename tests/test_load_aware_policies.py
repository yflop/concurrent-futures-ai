"""Deterministic scheduling controls; no timing-based optimality assertions."""
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import FrozenInstanceError
import threading
import unittest
from unittest.mock import patch

from intelligent_futures import AdaptivePolicy, IntelligentExecutor, PolicyRegistry, Profile, TaskHints
from intelligent_futures.policies import BackendLoad, SchedulingLoad
from intelligent_futures.prediction import ExecutionObjective, HardwareCapabilities, Predictor, RuntimeSample


def load(*, thread_workers=2, thread_queued=0, thread_in_flight=0,
         process_workers=1, process_queued=0, process_in_flight=0):
    return SchedulingLoad(BackendLoad(thread_workers, thread_queued, thread_in_flight),
                          BackendLoad(process_workers, process_queued, process_in_flight), 64)


def baseline(seconds, *, legacy_seconds=None):
    return Profile(samples=20, ewma_seconds=seconds if legacy_seconds is None else legacy_seconds,
                   uncontended_samples=2, uncontended_ewma_seconds=seconds)


class LoadSnapshotTests(unittest.TestCase):
    def test_immutable_validated_dispatch_slot_snapshot(self):
        value = load(thread_in_flight=1, thread_queued=3)
        self.assertEqual(value.thread.outstanding, 4)
        self.assertEqual(value.thread.pressure, 2)
        self.assertEqual(value.thread.slot_utilisation, .5)
        with self.assertRaises(FrozenInstanceError):
            value.thread.queued = 9
        for kwargs in ({'workers': True}, {'workers': -1}, {'workers': 0, 'queued': 1},
                       {'workers': 1, 'queued': -1}, {'workers': 1, 'in_flight': 2}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                BackendLoad(**kwargs)
        with self.assertRaises(ValueError):
            SchedulingLoad(BackendLoad(1, queued=2), BackendLoad(0), 1)
        with self.assertRaises(TypeError):
            SchedulingLoad({}, BackendLoad(0), 1)


class AdaptiveLoadTests(unittest.TestCase):
    def setUp(self):
        self.hints = TaskHints(kind='cpu', key='job', process_safe=True)

    def test_established_backlog_changes_routing_and_recovers(self):
        policy = AdaptivePolicy(exploration_interval=100)
        process_busy = load(process_in_flight=1, process_queued=2)
        self.assertEqual(policy.choose_with_load(self.hints, {}, True, process_busy), 'thread')
        self.assertEqual(policy.snapshot()['last_decision']['reason'], 'queue-pressure')
        thread_busy = load(thread_in_flight=2, thread_queued=4)
        self.assertEqual(policy.choose_with_load(self.hints, {}, True, thread_busy), 'process')
        self.assertEqual(policy.choose_with_load(self.hints, {}, True, load()), 'process')
        # Occupancy alone is not evidence that the other execution model is faster.
        self.assertEqual(policy.choose_with_load(self.hints, {}, True,
                                                load(process_in_flight=1)), 'process')

    def test_pressure_never_grants_process_safety(self):
        pressure = load(thread_in_flight=2, thread_queued=10)
        policy = AdaptivePolicy(exploration_interval=2)
        for hints, available in ((TaskHints(kind='cpu'), True),
                                 (TaskHints(kind='io', process_safe=True), True),
                                 (self.hints, False)):
            self.assertEqual(policy.choose_with_load(hints, {}, available, pressure), 'thread')
        self.assertEqual(policy.choose_with_load(self.hints, {}, True,
                                                load(process_workers=0)), 'thread')

    def test_queued_completion_history_cannot_train_load_proxy(self):
        policy = AdaptivePolicy(exploration_interval=100)
        profiles = {'job': {'thread': baseline(.2, legacy_seconds=100),
                            'process': baseline(1, legacy_seconds=.001)}}
        self.assertEqual(policy.choose_with_load(self.hints, profiles, True, load()), 'thread')
        # Profiles without uncontended provenance remain useful legacy metrics,
        # but cannot masquerade as a service-cost baseline in live load routing.
        profiles = {'job': {'thread': Profile(samples=100, ewma_seconds=.001),
                            'process': Profile(samples=100, ewma_seconds=100)}}
        self.assertEqual(policy.choose_with_load(self.hints, profiles, True, load()), 'process')

    def test_deadband_ignores_small_changes_but_tracks_material_changes(self):
        policy = AdaptivePolicy(exploration_interval=100, switch_margin=.1)
        profiles = {'job': {'thread': baseline(1), 'process': baseline(1.05)}}
        self.assertEqual(policy.choose_with_load(self.hints, profiles, True, load()), 'thread')
        profiles['job'] = {'thread': baseline(1.02), 'process': baseline(1)}
        self.assertEqual(policy.choose_with_load(self.hints, profiles, True, load()), 'thread')
        profiles['job']['process'] = baseline(.5)
        self.assertEqual(policy.choose_with_load(self.hints, profiles, True, load()), 'process')
        # A sustained load shift may outweigh the same measured baseline.
        self.assertEqual(policy.choose_with_load(self.hints, profiles, True,
                                                load(process_in_flight=1, process_queued=4)), 'thread')

    def test_exploration_is_per_key_bounded_and_avoids_busy_cold_target(self):
        policy = AdaptivePolicy(exploration_interval=3, max_keys=2)
        other = TaskHints(kind='cpu', key='other', process_safe=True)
        self.assertEqual(policy.choose(self.hints, {}, True), 'process')
        self.assertEqual(policy.choose(self.hints, {}, True), 'process')
        self.assertEqual(policy.choose(other, {}, True), 'process')
        self.assertEqual(policy.choose(self.hints, {}, True), 'thread')
        self.assertEqual(policy.choose(other, {}, True), 'process')
        self.assertEqual(policy.choose_with_load(other, {}, True,
                            load(thread_in_flight=1)), 'process')
        for index in range(10):
            policy.choose(TaskHints(kind='cpu', key=str(index), process_safe=True), {}, True)
        self.assertEqual(policy.snapshot()['tracked_keys'], 2)
        detached = policy.snapshot()
        detached['last_decision']['backend'] = 'invalid'
        self.assertEqual(policy.snapshot()['last_decision']['backend'], 'process')

    def test_shared_builtin_configuration_creates_independent_executor_sessions(self):
        template = AdaptivePolicy(exploration_interval=2, max_keys=4)
        template.choose(self.hints, {}, True)
        with IntelligentExecutor(policy=template, max_profile_keys=2) as first, \
                IntelligentExecutor(policy=template) as second:
            self.assertIsNot(first._policy, template)
            self.assertIsNot(first._policy, second._policy)
            self.assertEqual(first._policy.max_keys, 2)
            self.assertEqual(first._policy.choose(self.hints, {}, True), 'process')
            self.assertEqual(first._policy.choose(self.hints, {}, True), 'thread')
            self.assertEqual(second._policy.choose(self.hints, {}, True), 'process')
        self.assertEqual(template.choose(self.hints, {}, True), 'thread')

    def test_executor_routes_backlog_and_records_only_uncontended_baselines(self):
        # Frozen transport futures create a deterministic process backlog without
        # subprocess timing or replaying a user callable. The spill runs normally.
        first_raw, second_raw = Future(), Future()
        with IntelligentExecutor(max_workers=2, process_workers=1,
                                 policy=AdaptivePolicy(exploration_interval=100)) as executor:
            with patch.object(executor._processes, 'submit', side_effect=[first_raw, second_raw]):
                try:
                    first = executor.submit_task(str, 'first', hints=self.hints)
                    second = executor.submit_task(str, 'second', hints=self.hints)
                    spill = executor.submit_task(str, 'spill', hints=self.hints)
                    self.assertEqual(spill.result(5), 'spill')
                    snapshot = executor.snapshot()
                    self.assertEqual(snapshot['counts']['process_submitted'], 2)
                    self.assertEqual(snapshot['counts']['thread_submitted'], 1)
                    self.assertEqual(snapshot['load']['process']['queued'], 1)
                    self.assertEqual(snapshot['policy']['last_decision']['reason'], 'queue-pressure')
                    first_raw.set_result('first')
                    second_raw.set_result('second')
                    self.assertEqual((first.result(5), second.result(5)), ('first', 'second'))
                finally:
                    for raw in (first_raw, second_raw):
                        if not raw.done():
                            raw.set_result('cleanup')
        profiles = executor.snapshot()['profiles']['job']
        self.assertEqual(profiles['process']['samples'], 2)
        self.assertEqual(profiles['process']['uncontended_samples'], 1)
        self.assertEqual(profiles['thread']['uncontended_samples'], 1)

    def test_custom_subclass_state_is_not_silently_discarded(self):
        class Custom(AdaptivePolicy):
            pass
        custom = Custom()
        custom.user_state = 'supplied'
        self.assertIs(custom.for_executor(), custom)

    def test_concurrent_decisions_have_exact_exploration_accounting(self):
        policy = AdaptivePolicy(exploration_interval=8)
        barrier = threading.Barrier(4)
        def decide(_):
            barrier.wait(5)
            return [policy.choose(self.hints, {}, True) for _ in range(40)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = [value for batch in pool.map(decide, range(4)) for value in batch]
        self.assertEqual(results.count('thread'), 20)
        self.assertEqual(results.count('process'), 140)
        self.assertEqual(policy.snapshot()['tracked_keys'], 1)

    def test_margin_and_bound_validation(self):
        for kwargs in ({'switch_margin': True}, {'switch_margin': float('nan')},
                       {'switch_margin': 1}, {'switch_margin': 10 ** 1000},
                       {'switch_margin': -.1}, {'max_keys': 0},
                       {'max_keys': True}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                AdaptivePolicy(**kwargs)


class PredictorLoadTests(unittest.TestCase):
    def test_sparse_and_burst_demand_choose_different_observed_budgets(self):
        predictor = Predictor(HardwareCapabilities(4), ExecutionObjective('throughput'))
        for _ in range(2):
            predictor.record('job', RuntimeSample(1, concurrency=1))
            predictor.record('job', RuntimeSample(1, concurrency=4))
        idle = predictor.recommend('job', load=load(thread_workers=4))
        self.assertEqual(idle.concurrency, 1)
        burst = predictor.recommend('job', load=load(thread_workers=4, thread_in_flight=3))
        self.assertEqual(burst.concurrency, 4)
        self.assertEqual(burst.source, 'empirical')
        self.assertTrue(burst.load_adjusted)
        self.assertEqual(burst.expected_wall_seconds, 1)

    def test_latency_pressure_and_deadband_use_occupancy_not_cpu_claims(self):
        predictor = Predictor(HardwareCapabilities(5), ExecutionObjective('latency'))
        for _ in range(2):
            predictor.record('job', RuntimeSample(.9, backend='thread'))
            predictor.record('job', RuntimeSample(1, backend='process'))
        def recommend(**kwargs):
            return predictor.recommend('job', process_safe=True, process_available=True,
                                       load=load(thread_workers=5, **kwargs))
        self.assertEqual(recommend().backend, 'thread')
        slight = recommend(thread_in_flight=1)
        self.assertEqual(slight.backend, 'thread')
        self.assertTrue(slight.stabilized)
        changed = recommend(thread_in_flight=2)
        self.assertEqual(changed.backend, 'process')
        self.assertEqual(changed.expected_wall_seconds, 1)  # Service observation only.
        self.assertEqual(predictor.recommend('job', load=load(thread_queued=10)).backend, 'thread')

    def test_adjusted_concurrency_clears_unmeasured_service_prediction(self):
        predictor = Predictor(HardwareCapabilities(4), ExecutionObjective('latency'))
        for _ in range(2):
            predictor.record('job', RuntimeSample(.1, concurrency=4))
        result = predictor.recommend('job', load=load(thread_workers=4))
        self.assertEqual(result.concurrency, 1)
        self.assertEqual(result.source, 'measurement-guided')
        self.assertIsNone(result.expected_wall_seconds)

    def test_service_profile_changes_supersede_old_workload(self):
        predictor = Predictor(HardwareCapabilities(4), ExecutionObjective('latency'))
        kwargs = dict(process_safe=True, process_available=True, load=load())
        for _ in range(2):
            predictor.record('job', RuntimeSample(.1, backend='thread'))
            predictor.record('job', RuntimeSample(1, backend='process'))
        self.assertEqual(predictor.recommend('job', **kwargs).backend, 'thread')
        # The existing bounded 128-sample window fully replaces stale evidence.
        for _ in range(64):
            predictor.record('job', RuntimeSample(2, backend='thread'))
            predictor.record('job', RuntimeSample(.1, backend='process'))
        self.assertEqual(predictor.recommend('job', **kwargs).backend, 'process')
        self.assertEqual(len(predictor.profiles[0].samples), 128)

    def test_choice_cache_and_features_are_bounded_and_exact(self):
        predictor = Predictor(HardwareCapabilities(4), ExecutionObjective('latency'), max_profiles=2)
        for index in range(5):
            for _ in range(2):
                predictor.record('job', RuntimeSample(.1), {'n': index})
            predictor.recommend('job', {'n': index}, load=load())
        self.assertEqual(len(predictor._choices), 2)
        self.assertEqual(len(predictor.profiles), 2)
        self.assertEqual(predictor.recommend('job', {'n': 99}, load=load()).source, 'heuristic')

    def test_explicit_backend_uses_its_own_demand_and_evidence(self):
        predictor = Predictor(HardwareCapabilities(4), ExecutionObjective('throughput'))
        active = load(thread_workers=4, thread_in_flight=1, process_workers=4)
        result = predictor.recommend('job', kind='cpu', process_safe=True,
                                     process_available=True, load=active, backend='thread')
        self.assertEqual((result.backend, result.concurrency), ('thread', 2))
        # Evidence for a different route must not override the explicit policy.
        for _ in range(2):
            predictor.record('job', RuntimeSample(.001, concurrency=1, backend='process'))
            predictor.record('job', RuntimeSample(.001, concurrency=4, backend='process'))
        result = predictor.recommend('job', kind='cpu', process_safe=True,
                                     process_available=True, load=active, backend='thread')
        self.assertEqual((result.backend, result.concurrency), ('thread', 2))
        self.assertIsNone(result.expected_wall_seconds)
        process_active = load(thread_workers=4, process_workers=4, process_in_flight=2)
        result = predictor.recommend('other', process_safe=True, process_available=True,
                                     load=process_active, backend='process')
        self.assertEqual((result.backend, result.concurrency), ('process', 3))

    def test_explicit_backend_cannot_grant_process_consent_or_capacity(self):
        predictor = Predictor(HardwareCapabilities(4))
        for kwargs in ({}, {'process_available': True}, {'process_safe': True},
                       {'process_safe': True, 'process_available': True,
                        'load': load(process_workers=0)}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                predictor.recommend('job', backend='process', **kwargs)
        with self.assertRaises(ValueError):
            predictor.recommend('job', backend='gpu')

    def test_forced_thread_executor_does_not_cap_to_idle_process_demand(self):
        class CustomThreads:
            def choose(self, hints, profiles, process_available):
                return 'thread'

        for policy in ('threads', CustomThreads()):
            with self.subTest(policy=type(policy).__name__):
                entered, release = threading.Event(), threading.Event()
                def blocked():
                    entered.set()
                    if not release.wait(5):
                        raise AssertionError('test gate not released')
                    return 1
                executor = IntelligentExecutor(policy=policy, predictive=True,
                    max_workers=4, process_workers=4, max_pending=8, submit_timeout=0,
                    hardware=HardwareCapabilities(4), objective=ExecutionObjective('throughput'))
                try:
                    hints = TaskHints(kind='cpu', process_safe=True)
                    first = executor.submit_task(blocked, hints=hints)
                    self.assertTrue(entered.wait(5))
                    second = executor.submit_task(blocked, hints=hints)
                    snapshot = executor.snapshot()
                    self.assertEqual(snapshot['counts']['thread_submitted'], 2)
                    self.assertEqual(snapshot['counts']['process_submitted'], 0)
                    self.assertGreaterEqual(snapshot['predictive']['concurrency_limit'], 2)
                    self.assertEqual(snapshot['predictive']['last_prediction']['backend'], 'thread')
                    release.set()
                    self.assertEqual((first.result(5), second.result(5)), (1, 1))
                finally:
                    release.set()
                    executor.shutdown(wait=True, cancel_futures=True)

    def test_forced_process_executor_does_not_cap_to_idle_thread_demand(self):
        class CustomProcess:
            def choose(self, hints, profiles, process_available):
                return 'process'

        raws = (Future(), Future())
        executor = IntelligentExecutor(policy=CustomProcess(), predictive=True,
            max_workers=4, process_workers=4, max_pending=8, submit_timeout=0,
            hardware=HardwareCapabilities(4), objective=ExecutionObjective('throughput'))
        try:
            with patch.object(executor._processes, 'submit', side_effect=raws):
                try:
                    hints = TaskHints(process_safe=True)
                    futures = [executor.submit_task(str, value, hints=hints) for value in ('one', 'two')]
                    snapshot = executor.snapshot()
                    self.assertEqual(snapshot['counts']['process_submitted'], 2)
                    self.assertGreaterEqual(snapshot['predictive']['concurrency_limit'], 2)
                    self.assertEqual(snapshot['predictive']['last_prediction']['backend'], 'process')
                    for raw, value in zip(raws, ('one', 'two')):
                        raw.set_result(value)
                    self.assertEqual([future.result(5) for future in futures], ['one', 'two'])
                finally:
                    for raw in raws:
                        if not raw.done():
                            raw.set_result('cleanup')
        finally:
            executor.shutdown(wait=True, cancel_futures=True)

    def test_disabled_process_load_and_explicit_bounds(self):
        predictor = Predictor(HardwareCapabilities(4), ExecutionObjective(max_concurrency=2))
        result = predictor.recommend('job', kind='cpu', process_safe=True, process_available=True,
                                     load=load(process_workers=0, thread_in_flight=2))
        self.assertEqual((result.backend, result.concurrency), ('thread', 2))
        with self.assertRaises(TypeError):
            predictor.recommend('job', load={})


class RegistryThreadSafetyTests(unittest.TestCase):
    def test_concurrent_duplicate_registration_has_one_winner(self):
        registry = PolicyRegistry()
        barrier = threading.Barrier(4)
        def register(_):
            barrier.wait(5)
            try:
                registry.register('new', AdaptivePolicy)
                return True
            except ValueError:
                return False
        with ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(sum(pool.map(register, range(4))), 1)
        self.assertIsNot(registry.create('new'), registry.create('new'))


if __name__ == '__main__':
    unittest.main()
