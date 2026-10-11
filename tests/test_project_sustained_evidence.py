from copy import deepcopy
import unittest
from tools.project_sustained_evidence import redact, numeric_tree, replace_path, PATH


class ProjectionTests(unittest.TestCase):
    def test_projection_retains_every_number_boolean_and_gate(self):
        value = {'runtime': '/workspace/source/pkg/executor.py',
                 'command': 'python /workspace/source/tool.py --output /workspace/results/report.json',
                 'authorization': 'non-public bookkeeping', 'seconds': 1.25,
                 'correct': True, 'exclusions': [], 'rows': [1, 2.5, False, None]}
        counts = {'local_path_occurrences_redacted': 0, 'nonpublic_bookkeeping_fields_removed': 0}
        projected = redact(value, counts)
        self.assertEqual(numeric_tree(value), numeric_tree(projected))
        self.assertNotIn('authorization', projected)
        self.assertEqual(projected['exclusions'], value['exclusions'])
        self.assertEqual(counts['local_path_occurrences_redacted'], 3)
        self.assertEqual(counts['nonpublic_bookkeeping_fields_removed'], 1)
        self.assertEqual(projected['runtime'], '[local path]')

    def test_protocol_redaction_preserves_sentence_punctuation(self):
        text = 'Checkout: /workspace/source/repo. Conditions unchanged.'
        actual = PATH.sub(lambda match: replace_path(match, '[source checkout]'), text)
        self.assertEqual(actual, 'Checkout: [source checkout]. Conditions unchanged.')

    def test_no_sensitive_metadata_means_no_change(self):
        value = {'sha256': 'abc123', 'hardware': 'example CPU', 'version': '0.5.1',
                 'cpu_seconds': 123.456, 'peak_rss_bytes': 1024, 'source': 'src/worker.py'}
        counts = {'local_path_occurrences_redacted': 0, 'nonpublic_bookkeeping_fields_removed': 0}
        self.assertEqual(redact(value, counts), value)
        self.assertEqual(sum(counts.values()), 0)


if __name__ == '__main__':
    unittest.main()
