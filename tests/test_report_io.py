import gzip
import json
from pathlib import Path
import tempfile
import unittest
from tools.report_io import read_report, resolve_report


class ReportIOTests(unittest.TestCase):
    def test_plain_and_gzip_identical(self):
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory) / 'report.json'
            payload = b'{"values": [1, 2.5, null], "text": "evidence"}\n'
            p.write_bytes(payload)
            expected = read_report(p)
            archive = p.with_suffix('.json.gz')
            archive.write_bytes(gzip.compress(payload, mtime=0))
            with self.assertRaises(ValueError):
                read_report(p)
            p.unlink()
            self.assertEqual(read_report(p), expected)
            self.assertEqual(read_report(archive), expected)
            self.assertEqual(gzip.decompress(archive.read_bytes()), payload)
            self.assertEqual(resolve_report(p), archive)

    def test_bounded_plain_and_compressed_input(self):
        with tempfile.TemporaryDirectory() as directory:
            for suffix in ('.json', '.json.gz'):
                p = Path(directory) / ('report' + suffix)
                payload = json.dumps('x' * 10000).encode()
                p.write_bytes(gzip.compress(payload, mtime=0) if suffix.endswith('.gz') else payload)
                with self.assertRaises(ValueError):
                    read_report(p, max_bytes=50)
                p.unlink()

    def test_invalid_limits_and_extensions(self):
        for limit in (True, 0, -1, 1.5, 33 * 1024 * 1024):
            with self.assertRaises(ValueError):
                read_report('not-needed.json', max_bytes=limit)
        with self.assertRaises(ValueError):
            read_report('untrusted.pickle')

    def test_corruption_invalid_json_and_utf8_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory) / 'report.json.gz'
            for payload in (b'not gzip', gzip.compress(b'not JSON'), gzip.compress(b'"\xff"')):
                p.write_bytes(payload)
                with self.assertRaises((OSError, ValueError, UnicodeError, EOFError)):
                    read_report(p)

    def test_exact_size_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory) / 'report.json'
            p.write_bytes(b'null')
            self.assertIsNone(read_report(p, max_bytes=4))
            with self.assertRaises(ValueError):
                read_report(p, max_bytes=3)

    def test_gzip_deterministic_and_no_filename_metadata(self):
        payload = b'{"measurement":1.25}\n'
        a = gzip.compress(payload, mtime=0)
        self.assertEqual(a, gzip.compress(payload, mtime=0))
        self.assertEqual(a[4:8], b'\0' * 4)
        self.assertEqual(a[3] & 8, 0)

class ArchiveReportsTests(unittest.TestCase):
    def test_manifest_roundtrip_and_summary_retention(self):
        import hashlib
        from tools.archive_reports import archive, deterministic_gzip
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = json.dumps({'measurements': list(range(100))}).encode()
            (root / 'raw.json').write_bytes(payload)
            (root / 'summary.json').write_bytes(payload)
            (root / 'rankings.json').write_bytes(payload)
            manifest = archive(root, threshold=10)
            self.assertFalse((root / 'raw.json').exists())
            self.assertTrue((root / 'summary.json').exists())
            self.assertTrue((root / 'rankings.json').exists())
            row = manifest['archives'][0]
            data = (root / row['path']).read_bytes()
            self.assertEqual(gzip.decompress(data), payload)
            self.assertEqual(data, deterministic_gzip(payload))
            self.assertEqual(row['payload_sha256'], hashlib.sha256(payload).hexdigest())
            self.assertEqual(row['archive_sha256'], hashlib.sha256(data).hexdigest())
            self.assertEqual(archive(root, threshold=10), manifest)

class ArchivedConsumerTests(unittest.TestCase):
    def test_optimizer_summaries_preserve_failed_records_from_archives(self):
        from tools.summarize_optimizer import summary
        from tools.analyze_optimizer_rankings import summary as rankings
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / 'images.json'
            payload = json.dumps({'status': 'failed', 'failures': ['preserved interruption'],
                                  'training': [{}]}).encode()
            path.write_bytes(payload)
            plain = [fn(root) for fn in (summary, rankings)]
            path.unlink()
            path.with_suffix('.json.gz').write_bytes(gzip.compress(payload, mtime=0))
            self.assertEqual([fn(root) for fn in (summary, rankings)], plain)

class DecompressionCLITests(unittest.TestCase):
    def test_exact_bytes_and_no_overwrite(self):
        from unittest.mock import patch
        from tools.report_io import main
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'archived.json.gz'
            target = Path(directory) / 'plain.json'
            payload = b'{ "value": 1.00 }\n'
            source.write_bytes(gzip.compress(payload, mtime=0))
            with patch('sys.argv', ['report_io', str(source), '--output', str(target)]):
                main()
                self.assertEqual(target.read_bytes(), payload)
                with self.assertRaises(FileExistsError):
                    main()
