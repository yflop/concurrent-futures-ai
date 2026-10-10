"""Fast configuration-only interchange checks; no benchmark workloads."""
import dataclasses
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from intelligent_futures.prediction import HardwareCapabilities
from intelligent_futures.tuning import (
    Configuration, MAX_SAVED_CONFIGURATION_BYTES, Measurement, SavedConfiguration,
    calibrate, export_configuration, load_configuration,
)


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.hardware = HardwareCapabilities(4, 2, 1.5, 'test-machine', 'test-os', 'test-cpu')
        self.key = 'resize:v3|dataset:sha256-example|pillow:11.0|seed:7'
        self.calls = []
        def evaluate(config, seed):
            self.calls.append((config, seed))
            return Measurement(4 / config.workers, 2 / config.workers, True)
        with patch.object(HardwareCapabilities, 'detect', return_value=self.hardware):
            self.report = calibrate([Configuration('thread', 1, 2), Configuration('thread', 2, 4)],
                                    evaluate, repeats=2, seed=7)
        self.payload = export_configuration(self.report, workload_key=self.key,
                                            measured_scope='Whole image fixture including setup')

    def load(self, payload=None, **kwargs):
        return load_configuration(self.payload if payload is None else payload,
                                  workload_key=kwargs.pop('workload_key', self.key),
                                  hardware=kwargs.pop('hardware', self.hardware), **kwargs)

    def mutated(self, **updates):
        data = json.loads(self.payload)
        data.update(updates)
        return json.dumps(data)

    def test_file_roundtrip_reuses_selection_with_zero_callback_calls(self):
        self.calls.clear()
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'choice.json'
            target.write_text(self.payload, encoding='utf-8')
            with patch('intelligent_futures.tuning.calibrate', side_effect=AssertionError('no calibration')):
                saved = self.load(target.read_text(encoding='utf-8'))
                options = saved.executor_kwargs()
        self.assertEqual(saved.configuration, self.report.selected)
        self.assertEqual(saved.to_json(), self.payload)
        self.assertEqual(options['max_workers'], 2)
        self.assertEqual(saved.configuration.batch_size, 4)
        self.assertEqual(self.calls, [])
        from intelligent_futures import IntelligentExecutor
        with IntelligentExecutor(**options) as executor:
            self.assertEqual(executor.submit(sum, [2, 3, 5]).result(), 10)
        self.assertEqual(self.calls, [])
        with self.assertRaises(dataclasses.FrozenInstanceError):
            saved.workload_key = 'changed'

    def test_current_hardware_detected_by_default(self):
        with patch.object(HardwareCapabilities, 'detect', return_value=self.hardware) as detect:
            saved = load_configuration(self.payload, workload_key=self.key)
        detect.assert_called_once_with()
        self.assertEqual(saved.configuration, self.report.selected)

    def test_foreign_identity_rejected(self):
        for changes in ({'workload_key': self.key + '-new'},
                        {'python_runtime': 'other-python-build'},
                        {'hardware_fingerprint': 'f' * 64}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.load(self.mutated(**changes))
        with self.assertRaises(ValueError):
            self.load(workload_key=self.key + '-new')
        with self.assertRaises(ValueError):
            self.load(hardware=dataclasses.replace(self.hardware, logical_cpus=8))

    def test_invalid_hardware_and_stale_cache_rejected(self):
        with self.assertRaises(ValueError):
            self.load(hardware={})
        corrupted = dataclasses.replace(self.hardware)
        _ = corrupted.fingerprint
        object.__setattr__(corrupted, 'logical_cpus', True)
        with self.assertRaises(ValueError):
            self.load(hardware=corrupted)
        changed = dataclasses.replace(self.hardware)
        _ = changed.fingerprint
        object.__setattr__(changed, 'logical_cpus', 16)
        with self.assertRaises(ValueError):
            self.load(hardware=changed)

    def test_process_consent_is_not_persisted(self):
        config = json.loads(self.payload)['configuration']
        config['backend'] = 'process'
        payload = self.mutated(configuration=config)
        with self.assertRaises(ValueError):
            self.load(payload)
        saved = self.load(payload, allow_process=True)
        with self.assertRaises(ValueError):
            saved.executor_kwargs()
        self.assertEqual(saved.executor_kwargs(allow_process=True)['process_workers'], 2)
        for value in (1, 'yes', None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.load(allow_process=value)

    def test_required_fields_schema_and_unknown_fields(self):
        original = json.loads(self.payload)
        for field in original:
            data = dict(original)
            del data[field]
            with self.subTest(missing=field), self.assertRaises(ValueError):
                self.load(json.dumps(data))
        for version in (True, 2.0, '2', 0, 1, 3, None):
            with self.subTest(version=version), self.assertRaises(ValueError):
                self.load(self.mutated(schema_version=version))
        with self.assertRaises(ValueError):
            self.load(self.mutated(callback='ignored?'))

    def test_configuration_requires_exact_fields_and_types(self):
        original = json.loads(self.payload)['configuration']
        variants = [None, [], {}, {**original, 'source': 'pass'}]
        for field in original:
            variants.append({k: v for k, v in original.items() if k != field})
        for field in ('workers', 'batch_size', 'profile_sample_interval'):
            for value in (True, 1.0, '1', None, -1, 10**30):
                variants.append({**original, field: value})
        variants.extend({**original, 'backend': x} for x in (None, True, [], 'gpu'))
        for config in variants:
            with self.subTest(config=config), self.assertRaises(ValueError):
                self.load(self.mutated(configuration=config))

    def test_duplicate_keys_at_all_depths(self):
        for payload in (self.payload.replace('"schema_version": 2', '"schema_version": 2, "schema_version": 2'),
                        self.payload.replace('"workers": 2', '"workers": 2, "workers": 2')):
            with self.assertRaises(ValueError):
                self.load(payload)

    def test_malformed_truncated_nonfinite_oversized_or_nontext(self):
        for payload in ('', '{', self.payload[:-1], self.payload + 'junk', 'null', '[]',
                        '[' * 2000 + ']' * 2000, '\ud800', b'{}', None, 1,
                        ' ' * (MAX_SAVED_CONFIGURATION_BYTES + 1),
                        'é' * MAX_SAVED_CONFIGURATION_BYTES):
            # None is passed directly because self.load uses None as its default.
            with self.subTest(payload=str(payload)[:30]), self.assertRaises(ValueError):
                load_configuration(payload, workload_key=self.key, hardware=self.hardware)
        for token in ('NaN', 'Infinity', '-Infinity', '1e999'):
            with self.subTest(token=token), self.assertRaises(ValueError):
                self.load(self.payload.replace('"workers": 2', '"workers": ' + token))

    def test_metadata_strict_types_and_bounds(self):
        for field, limit in (('workload_key', 512), ('python_runtime', 2048),
                             ('provenance', 256), ('measured_scope', 2048),
                             ('scheduler_revision', 256), ('strategy_revision', 256),
                             ('package_version', 128)):
            for value in ('', 'x' * (limit + 1), 123, [], {}, 'bad\ntext', '\ud800'):
                with self.subTest(field=field, value=str(value)[:20]), self.assertRaises(ValueError):
                    self.load(self.mutated(**{field: value}))
        for value in ('A' * 64, 'g' * 64, 'a' * 63, True, None):
            with self.assertRaises(ValueError):
                self.load(self.mutated(hardware_fingerprint=value))

    def test_unsuccessful_or_corrupted_report_cannot_export(self):
        for report in (None, dataclasses.replace(self.report, selected=None),
                       dataclasses.replace(self.report, candidates=()),
                       dataclasses.replace(self.report, records=())):
            with self.assertRaises(ValueError):
                export_configuration(report, workload_key=self.key)
        selected = dataclasses.replace(self.report.selected)
        object.__setattr__(selected, 'workers', True)
        with self.assertRaises(ValueError):
            export_configuration(dataclasses.replace(self.report, selected=selected), workload_key=self.key)

    def test_source_text_never_executed(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'must-not-exist'
            source = f"__import__('pathlib').Path({str(target)!r}).touch()"
            saved = self.load(self.mutated(measured_scope=source))
            self.assertEqual(saved.measured_scope, source)
            with self.assertRaises(ValueError):
                self.load(self.mutated(source=source))
            with self.assertRaises(ValueError):
                self.load(source)
            self.assertFalse(target.exists())

    def test_direct_saved_configuration_is_validated(self):
        saved = self.load()
        with self.assertRaises(ValueError):
            dataclasses.replace(saved, configuration={})
        object.__setattr__(saved.configuration, 'workers', False)
        with self.assertRaises(ValueError):
            saved.to_json()
        with self.assertRaises(ValueError):
            saved.executor_kwargs()

    def test_optional_scope_and_unicode_byte_bound(self):
        payload = export_configuration(self.report, workload_key=self.key)
        self.assertIsNone(self.load(payload).measured_scope)
        with self.assertRaises(ValueError):
            export_configuration(self.report, workload_key='😀' * 512, measured_scope='😀' * 2048)


if __name__ == '__main__':
    unittest.main()
