"""CLI/provenance checks only: these tests never run media workloads."""
from contextlib import redirect_stderr
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools import compare_video_memory as comparison


class CompareVideoMemoryTests(unittest.TestCase):
    def test_workspace_is_required_before_work_can_start(self):
        with patch.object(comparison, "run") as run, redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                comparison.main(["--mode", "prepare"])
        self.assertEqual(raised.exception.code, 2)
        run.assert_not_called()

    def test_before_and_after_require_output(self):
        for mode in ("before", "after"):
            with self.subTest(mode=mode), redirect_stderr(io.StringIO()):
                with patch.object(comparison, "run") as run:
                    with self.assertRaises(SystemExit):
                        comparison.main(["--mode", mode, "--workspace", "local-fixtures"])
                run.assert_not_called()

    def test_explicit_workspace_and_source_are_resolved_without_io(self):
        with tempfile.TemporaryDirectory() as folder:
            workspace = Path(folder) / "new-fixtures"
            source = Path(folder) / "old.py"
            output = Path(folder) / "report.json"
            args = comparison.parse_args(["--mode", "after", "--workspace", str(workspace),
                                          "--before-source", str(source), "--output", str(output)])
            self.assertEqual(args.workspace, workspace.resolve())
            self.assertEqual(args.before_source, source.resolve())
            self.assertEqual(args.output, output.resolve())
            self.assertFalse(workspace.exists())
            self.assertFalse(source.exists())
            self.assertFalse(output.exists())

    def test_default_source_is_preserved_snapshot(self):
        args = comparison.parse_args(["--mode", "prepare", "--workspace", "local-fixtures"])
        self.assertEqual(args.before_source, comparison.DEFAULT_BEFORE_SOURCE.resolve())
        self.assertEqual(hashlib.sha256(args.before_source.read_bytes()).hexdigest(),
                         "cb71269b9a2f6053bd8b36b1ff39bf296ab505acd14ca871ff05f383a75b1109")

    def test_output_cannot_replace_sources_or_manifest(self):
        with tempfile.TemporaryDirectory() as folder:
            workspace = Path(folder)
            for output in (workspace / "video-inputs.json", comparison.DEFAULT_BEFORE_SOURCE,
                           Path(comparison.media.__file__)):
                with self.subTest(output=output), redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        comparison.parse_args(["--mode", "after", "--workspace", str(workspace),
                                               "--output", str(output)])

    def test_main_dispatches_arguments_without_running_workloads(self):
        with patch.object(comparison, "run", return_value=0) as run:
            self.assertEqual(comparison.main(["--mode", "prepare", "--workspace", "local-fixtures"]), 0)
        run.assert_called_once()
        self.assertEqual(run.call_args.args[0].mode, "prepare")

    def test_published_reports_keep_exact_results_and_source_provenance(self):
        root = comparison.DEFAULT_BEFORE_SOURCE.parent
        summary = json.loads((root / "video-streaming-summary.json").read_text())
        reports = [json.loads((root / f"video-{mode}-{pair}.json").read_text())
                   for pair in (1, 2) for mode in ("before", "after")]
        self.assertEqual(sum(report["tasks"] for report in reports), 96)
        self.assertEqual(len({report["expected_digest"] for report in reports}), 1)
        for report in reports:
            self.assertEqual(report["correct_per_repeat"], [True, True, True])
            self.assertTrue(report["rss_collection"]["complete"])
            self.assertIn("public_metadata_projection", report)
            expected_source = (summary["backup_source_sha256"] if report["mode"] == "before"
                               else summary["diagnostic_combined_source_sha256"])
            self.assertEqual(report["source_sha256"], expected_source)


if __name__ == "__main__":
    unittest.main()
