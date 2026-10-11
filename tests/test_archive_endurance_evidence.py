import gzip
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('archive_endurance_evidence',ROOT/'tools/archive_endurance_evidence.py')
archive=importlib.util.module_from_spec(spec);spec.loader.exec_module(archive)

class ArchiveTests(unittest.TestCase):
    def test_numeric_types_structure_and_paths(self):
        original={'path':'/workspace/private/example.py','future':None,'ok':True,'count':1,'value':1.25,'rows':[1,False]}
        changes={'path_occurrences':0};projected=archive.redact(original,changes)
        self.assertEqual(projected['path'],'[local path]')
        self.assertEqual(archive.nonstring_tree(original),archive.nonstring_tree(projected))
        self.assertEqual(changes['path_occurrences'],1)
        self.assertNotEqual(archive.nonstring_tree({'n':True}),archive.nonstring_tree({'n':1}))

    def test_jsonl_roundtrip_keeps_every_record_and_original(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=root/'raw.jsonl';target=root/'public.jsonl.gz'
            raw=b'{"event":"failure","value":7,"path":"/home/runner/file.py"}\n{"event":"later","ok":false}\n'
            source.write_bytes(raw)
            ledger=archive.project_file(source,target)
            rows=[json.loads(line) for line in gzip.decompress(target.read_bytes()).splitlines()]
            self.assertEqual(len(rows),2)
            self.assertEqual(rows[0]['event'],'failure')
            self.assertEqual(rows[1]['ok'],False)
            self.assertEqual(source.read_bytes(),raw)
            self.assertEqual(ledger['records'],2)
            self.assertEqual(ledger['original_sha256'],archive.sha(raw))
            with self.assertRaises(FileExistsError):archive.project_file(source,target)

    def test_nonfinite_input_is_rejected(self):
        with self.assertRaises(ValueError):archive.parse('{"value":NaN}')

    def test_missing_terminal_is_not_completed(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                archive.create_archive([],Path(directory)/'new','attempt','completed',['summary.json'])

    def test_interrupted_status_and_missing_files_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=root/'checkpoint.json';source.write_text('{"count":3}')
            ledger=archive.create_archive([(source,'checkpoint.json')],root/'new',
                'local-interrupted','interrupted',['summary.json','watchdog.json'])
            self.assertEqual(ledger['classification'],'interrupted')
            self.assertEqual(ledger['missing_evidence'],['summary.json','watchdog.json'])
            self.assertEqual(len(ledger['files']),1)

if __name__=='__main__':unittest.main()
