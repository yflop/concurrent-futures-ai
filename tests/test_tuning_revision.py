"""Saved choices cannot silently cross scheduler or constructor-strategy changes."""
import dataclasses
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import intelligent_futures
from intelligent_futures.prediction import HardwareCapabilities
from intelligent_futures.tuning import (
    Configuration, Measurement, SAVED_CONFIGURATION_SCHEMA_VERSION,
    SCHEDULER_REVISION, STRATEGY_REVISION, SavedConfiguration,
    calibrate, export_configuration, load_configuration, tune,
)


class RevisionIdentityTests(unittest.TestCase):
    def setUp(self):
        self.hardware = HardwareCapabilities(4, 4, None, "test", "test", "test")
        self.key = "fixture-v1"
        self.calls = []
        self.configs = [Configuration("thread", 1, 2), Configuration("thread", 2, 2)]
        with patch.object(HardwareCapabilities, "detect", return_value=self.hardware):
            self.reports = (
                calibrate(self.configs, self.evaluate, repeats=2, seed=7),
                tune(self.configs, self.evaluate, max_evaluations=4, seed=7,
                     min_repeats=2, max_repeats=2),
            )

    def evaluate(self, config, seed):
        self.calls.append((config, seed))
        return Measurement(3 - config.workers, 1, True)

    def payload(self, report=None):
        return export_configuration(self.reports[0] if report is None else report,
                                    workload_key=self.key)

    def load(self, payload):
        return load_configuration(payload, workload_key=self.key, hardware=self.hardware)

    def test_both_calibration_apis_capture_and_export_identity_without_replay(self):
        self.assertEqual(SAVED_CONFIGURATION_SCHEMA_VERSION, 2)
        self.assertEqual(SCHEDULER_REVISION, "owned-removable-queue-physical-dispatch-v1")
        count = len(self.calls)
        for report in self.reports:
            with self.subTest(report=type(report).__name__):
                data = json.loads(self.payload(report))
                self.assertEqual(data["schema_version"], SAVED_CONFIGURATION_SCHEMA_VERSION)
                for name, value in (
                    ("scheduler_revision", SCHEDULER_REVISION),
                    ("strategy_revision", STRATEGY_REVISION),
                    ("package_version", intelligent_futures.__version__),
                ):
                    self.assertEqual(getattr(report, name), value)
                    self.assertEqual(report.to_dict()[name], value)
                    self.assertEqual(data[name], value)
                saved = self.load(json.dumps(data))
                self.assertEqual(saved.configuration, report.selected)
                self.assertEqual(json.loads(saved.to_json()), data)
                self.assertEqual(saved.executor_kwargs()["max_workers"], 2)
        self.assertEqual(len(self.calls), count)

    def test_old_and_future_scheduler_and_strategy_revisions_fail_closed(self):
        for name, revision in (
            ("scheduler_revision", "stdlib-executor-submit-v1"),
            ("scheduler_revision", SCHEDULER_REVISION + "-next"),
            ("strategy_revision", STRATEGY_REVISION + "-next"),
        ):
            data = json.loads(self.payload())
            data[name] = revision
            with self.subTest(name=name, revision=revision):
                with self.assertRaisesRegex(ValueError, "revision mismatch; recalibrate"):
                    self.load(json.dumps(data))

    def test_missing_identity_and_extra_fields_are_not_defaulted_by_loader(self):
        for name in ("scheduler_revision", "strategy_revision", "package_version"):
            data = json.loads(self.payload())
            del data[name]
            with self.subTest(missing=name), self.assertRaisesRegex(ValueError, "schema v2 exactly"):
                self.load(json.dumps(data))
        data = json.loads(self.payload())
        data["allow_incompatible_strategy"] = True
        with self.assertRaisesRegex(ValueError, "schema v2 exactly"):
            self.load(json.dumps(data))

    def test_legacy_schema_requires_recalibration_even_when_other_identities_match(self):
        data = json.loads(self.payload())
        data["schema_version"] = 1
        for name in ("scheduler_revision", "strategy_revision", "package_version"):
            del data[name]
        with self.assertRaisesRegex(ValueError, "schema v1.*recalibrate.*schema v2"):
            self.load(json.dumps(data))

    def test_package_version_changes_are_informational_and_export_preserves_origin(self):
        for report in self.reports:
            original = report.package_version
            with self.subTest(report=type(report).__name__):
                with patch.object(intelligent_futures, "__version__", "999.0.1"):
                    saved = self.load(self.payload(report))
                    self.assertEqual(saved.package_version, original)
                    saved.executor_kwargs()
                data = json.loads(self.payload(report))
                data["package_version"] = "0.4.1"
                self.assertEqual(self.load(json.dumps(data)).package_version, "0.4.1")

    def test_export_rejects_old_reports_instead_of_restamping_revisions(self):
        for report in self.reports:
            for name in ("scheduler_revision", "strategy_revision"):
                old = dataclasses.replace(report, **{name: "previous-contract"})
                with self.subTest(report=type(report).__name__, name=name):
                    with self.assertRaisesRegex(ValueError, "revision mismatch; recalibrate"):
                        self.payload(old)
            for constant in ("SCHEDULER_REVISION", "STRATEGY_REVISION"):
                with patch("intelligent_futures.tuning." + constant, "next-contract"):
                    with self.assertRaisesRegex(ValueError, "revision mismatch; recalibrate"):
                        self.payload(report)

    def test_saved_options_recheck_revisions_after_load_or_direct_construction(self):
        saved = self.load(self.payload())
        for name, constant in (("scheduler_revision", "SCHEDULER_REVISION"),
                               ("strategy_revision", "STRATEGY_REVISION")):
            with self.subTest(name=name):
                old = dataclasses.replace(saved, **{name: "previous-contract"})
                with self.assertRaisesRegex(ValueError, "revision mismatch; recalibrate"):
                    old.executor_kwargs()
                with patch("intelligent_futures.tuning." + constant, "next-contract"):
                    with self.assertRaisesRegex(ValueError, "revision mismatch; recalibrate"):
                        saved.executor_kwargs()

    def test_identity_defaults_preserve_existing_positional_construction(self):
        saved = self.load(self.payload())
        direct = SavedConfiguration(saved.configuration, saved.workload_key,
                                    saved.hardware_fingerprint, saved.python_runtime,
                                    saved.provenance, saved.measured_scope)
        self.assertEqual(direct, saved)
        names = {"scheduler_revision", "strategy_revision", "package_version"}
        for report in self.reports:
            args = [getattr(report, field.name) for field in dataclasses.fields(report)
                    if field.name not in names]
            self.assertEqual(type(report)(*args), report)

    def test_direct_and_exported_identity_metadata_revalidate_strict_types(self):
        saved = self.load(self.payload())
        for name in ("scheduler_revision", "strategy_revision", "package_version"):
            for value in (None, True, 2, "", "bad\ntext", []):
                with self.subTest(name=name, value=value):
                    with self.assertRaises(ValueError):
                        dataclasses.replace(saved, **{name: value})
                    for report in self.reports:
                        with self.assertRaises(ValueError):
                            self.payload(dataclasses.replace(report, **{name: value}))

    def test_revision_metadata_never_imports_or_executes_payload_text(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "must-not-exist"
            source = f"__import__('pathlib').Path({str(target)!r}).touch()"
            for name in ("scheduler_revision", "strategy_revision"):
                data = json.loads(self.payload())
                data[name] = source
                with self.assertRaises(ValueError):
                    self.load(json.dumps(data))
            self.assertFalse(target.exists())

    def test_duplicate_identity_keys_rejected(self):
        payload = self.payload()
        for name, value in (("scheduler_revision", SCHEDULER_REVISION),
                            ("strategy_revision", STRATEGY_REVISION),
                            ("package_version", intelligent_futures.__version__)):
            original = json.dumps(name) + ": " + json.dumps(value)
            duplicate = payload.replace(original, original + ", " + original)
            self.assertNotEqual(duplicate, payload)
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.load(duplicate)


if __name__ == "__main__":
    unittest.main()
