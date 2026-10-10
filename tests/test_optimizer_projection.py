import unittest
from tools.project_optimizer_evidence import project

class ProjectionTests(unittest.TestCase):
    def test_metadata_changes_leave_measurements_and_configuration_unchanged(self):
        raw={'environment':{'command':'/workspace/private/.venv/bin/python -m benchmark --output results.json',
            'hardware_fingerprint':'a'*64,'cpu_model':'AMD example','source_sha256':{'src/tuning.py':'b'*64}},
            'measurement':{'seconds':1.23,'workers':8,'batch_size':16},
            'saved_configuration':{'hardware_fingerprint':'c'*64}}
        public=project(raw)
        self.assertEqual(public['measurement'],raw['measurement'])
        self.assertEqual(public['environment']['source_sha256'],raw['environment']['source_sha256'])
        self.assertEqual(public['environment']['command'],'python -m benchmark --output results.json')
        self.assertNotEqual(public['saved_configuration'],raw['saved_configuration'])
        self.assertEqual(raw['environment']['hardware_fingerprint'],'a'*64)

    def test_paths_in_failure_text_are_redacted(self):
        self.assertNotIn('/tmp/',project({'failure':'cannot open /tmp/private-123/input.png'})['failure'])
        self.assertEqual(project({'value':42}),{'value':42})

if __name__=='__main__':unittest.main()
