"""Policy unit tests use synthetic profiles, not noisy microbenchmarks."""
from dataclasses import FrozenInstanceError
import unittest

from intelligent_futures import AdaptivePolicy, HintPolicy, PolicyRegistry, Profile, TaskHints, ThreadPolicy


class TaskHintsTests(unittest.TestCase):
    def test_defaults_and_immutability(self):
        hints = TaskHints()
        self.assertEqual((hints.kind, hints.key, hints.process_safe), ("unknown", "default", False))
        with self.assertRaises(FrozenInstanceError):
            hints.key = "changed"
        self.assertEqual(TaskHints(key="x" * 256).key, "x" * 256)

    def test_invalid_kind_key_and_consent(self):
        for kind in ("gpu", "CPU", "", None, 12):
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                TaskHints(kind=kind)
        for key in ("", "x" * 257, None, 12):
            with self.subTest(key=key), self.assertRaises(ValueError):
                TaskHints(key=key)
        for consent in (1, 0, "true", None):
            with self.subTest(consent=consent), self.assertRaises(TypeError):
                TaskHints(process_safe=consent)

    def test_profile_is_immutable(self):
        profile = Profile(samples=2, ewma_seconds=0.5, total_seconds=1, failures=1)
        with self.assertRaises(FrozenInstanceError):
            profile.samples = 0


class BasicPolicyTests(unittest.TestCase):
    def test_thread_policy_always_selects_threads(self):
        policy = ThreadPolicy()
        for kind in ("unknown", "io", "cpu"):
            for available in (False, True):
                self.assertEqual(policy.choose(TaskHints(kind=kind, process_safe=True), {}, available), "thread")

    def test_hint_policy_requires_all_three_conditions(self):
        policy = HintPolicy()
        for kind in ("unknown", "io", "cpu"):
            for consent in (False, True):
                for available in (False, True):
                    expected = "process" if kind == "cpu" and consent and available else "thread"
                    with self.subTest(kind=kind, consent=consent, available=available):
                        self.assertEqual(policy.choose(TaskHints(kind=kind, process_safe=consent), {}, available), expected)


class AdaptivePolicyTests(unittest.TestCase):
    def test_no_process_or_consent_and_io_tasks_use_threads(self):
        policy = AdaptivePolicy(exploration_interval=2)
        for hints, available in ((TaskHints(kind="cpu", process_safe=True), False),
                                  (TaskHints(kind="cpu"), True),
                                  (TaskHints(kind="io", process_safe=True), True)):
            for _ in range(5):
                self.assertEqual(policy.choose(hints, {}, available), "thread")
        # Ineligible decisions do not advance the exploration counter.
        self.assertEqual(policy.choose(TaskHints(kind="cpu", process_safe=True), {}, True), "process")

    def test_cold_start_default_and_deterministic_exploration(self):
        policy = AdaptivePolicy(exploration_interval=3, minimum_samples=2)
        hints = TaskHints(kind="cpu", process_safe=True)
        self.assertEqual([policy.choose(hints, {}, True) for _ in range(6)],
                         ["process", "process", "thread", "process", "process", "thread"])

    def test_unknown_work_starts_on_threads_and_explores_processes(self):
        policy = AdaptivePolicy(exploration_interval=2, minimum_samples=2)
        hints = TaskHints(process_safe=True)
        profiles = {"default": {"thread": Profile(samples=2, ewma_seconds=1)}}
        self.assertEqual(policy.choose(hints, profiles, True), "thread")
        self.assertEqual(policy.choose(hints, profiles, True), "process")

    def test_chooses_lower_ewma_with_enough_samples(self):
        for fast_backend in ("thread", "process"):
            with self.subTest(fast_backend=fast_backend):
                policy = AdaptivePolicy(exploration_interval=10)
                profiles = {"work": {
                    "thread": Profile(samples=2, ewma_seconds=1 if fast_backend == "thread" else 5),
                    "process": Profile(samples=2, ewma_seconds=1 if fast_backend == "process" else 5),
                }}
                self.assertEqual(policy.choose(TaskHints(kind="cpu", key="work", process_safe=True), profiles, True), fast_backend)

    def test_uses_matching_key_and_warmup_threshold(self):
        policy = AdaptivePolicy(exploration_interval=10, minimum_samples=2)
        profiles = {"other": {"thread": Profile(samples=100, ewma_seconds=0.01), "process": Profile(samples=100, ewma_seconds=20)}}
        self.assertEqual(policy.choose(TaskHints(kind="cpu", key="work", process_safe=True), profiles, True), "process")
        profiles["work"] = {"thread": Profile(samples=1, ewma_seconds=0.01), "process": Profile(samples=100, ewma_seconds=20)}
        self.assertEqual(policy.choose(TaskHints(kind="cpu", key="work", process_safe=True), profiles, True), "process")

    def test_ties_prefer_threads_except_exploration(self):
        policy = AdaptivePolicy(exploration_interval=2)
        profiles = {"default": {"thread": Profile(samples=2, ewma_seconds=1), "process": Profile(samples=2, ewma_seconds=1)}}
        hints = TaskHints(kind="cpu", process_safe=True)
        self.assertEqual(policy.choose(hints, profiles, True), "thread")
        self.assertEqual(policy.choose(hints, profiles, True), "process")

    def test_periodic_exploration_selects_slower_backend(self):
        policy = AdaptivePolicy(exploration_interval=2)
        profiles = {"default": {"thread": Profile(samples=2, ewma_seconds=10), "process": Profile(samples=2, ewma_seconds=1)}}
        hints = TaskHints(process_safe=True)
        self.assertEqual(policy.choose(hints, profiles, True), "process")
        self.assertEqual(policy.choose(hints, profiles, True), "thread")

    def test_failed_observations_do_not_satisfy_warmup(self):
        policy = AdaptivePolicy()
        profiles = {"default": {"thread": Profile(samples=0, ewma_seconds=0, failures=100), "process": Profile(samples=2, ewma_seconds=5)}}
        self.assertEqual(policy.choose(TaskHints(kind="cpu", process_safe=True), profiles, True), "process")

    def test_parameters_require_actual_integers(self):
        for name in ("exploration_interval", "minimum_samples"):
            for value in (True, False, 2.5, "3", None, float("nan")):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    AdaptivePolicy(**{name: value})

    def test_parameter_bounds(self):
        for kwargs in ({"exploration_interval": 1}, {"exploration_interval": 0}, {"minimum_samples": 0}, {"minimum_samples": -1}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                AdaptivePolicy(**kwargs)


class RegistryTests(unittest.TestCase):
    def test_builtin_names_and_factories(self):
        registry = PolicyRegistry()
        self.assertEqual(registry.names(), ("adaptive", "hints", "threads"))
        self.assertIsInstance(registry.create("adaptive"), AdaptivePolicy)
        self.assertIsInstance(registry.create("hints"), HintPolicy)
        self.assertIsInstance(registry.create("threads"), ThreadPolicy)
        self.assertIsNot(registry.create("adaptive"), registry.create("adaptive"))

    def test_custom_factories_and_registry_instances_are_isolated(self):
        registry, other = PolicyRegistry(), PolicyRegistry()
        registry.register("custom", lambda: AdaptivePolicy(exploration_interval=3))
        self.assertIn("custom", registry.names())
        self.assertNotIn("custom", other.names())
        self.assertEqual(registry.create("custom").exploration_interval, 3)
        self.assertIsNot(registry.create("custom"), registry.create("custom"))

    def test_registry_names_must_be_nonempty_strings(self):
        registry = PolicyRegistry()
        for name in (None, 12, True, (), []):
            with self.subTest(name=name), self.assertRaises(ValueError):
                registry.register(name, ThreadPolicy)
        self.assertEqual(registry.names(), ("adaptive", "hints", "threads"))

    def test_factory_error_is_not_misreported_as_unknown_name(self):
        registry = PolicyRegistry()

        def factory():
            raise KeyError("factory dependency missing")

        registry.register("broken", factory)
        with self.assertRaisesRegex(KeyError, "factory dependency missing"):
            registry.create("broken")

    def test_unknown_duplicate_empty_and_noncallable(self):
        registry = PolicyRegistry()
        with self.assertRaises(ValueError):
            registry.create("unknown")
        with self.assertRaises(ValueError):
            registry.register("threads", ThreadPolicy)
        with self.assertRaises(ValueError):
            registry.register("", ThreadPolicy)
        with self.assertRaises(TypeError):
            registry.register("broken", object())


if __name__ == "__main__":
    unittest.main()
