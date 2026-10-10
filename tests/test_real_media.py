"""Real media tests; unavailable optional dependencies are explicit skips."""
import concurrent.futures
import json
import multiprocessing
from pathlib import Path
import pickle
import tempfile
import unittest
from unittest.mock import patch

from benchmarks.real_world import media


class MediaTests(unittest.TestCase):
    def _exercise(self, name, scale="smoke", count=2):
        with tempfile.TemporaryDirectory() as folder:
            try:
                tasks = media.prepare(name, Path(folder), count, 17, scale=scale)
            except media.MissingDependency as exc:
                self.skipTest(str(exc))
            self.assertEqual(tasks, pickle.loads(pickle.dumps(tasks)))
            self.assertEqual(tasks, json.loads(json.dumps(tasks)))
            original_tasks = json.loads(json.dumps(tasks))
            serial = [media.run_task(task) for task in tasks]
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                self.assertEqual(serial, list(executor.map(media.run_task, tasks)))
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=2, mp_context=multiprocessing.get_context("spawn")
            ) as executor:
                self.assertEqual(serial, list(executor.map(media.run_task, tasks)))
            self.assertEqual(tasks, original_tasks)
            self.assertEqual(serial, json.loads(json.dumps(serial)))
            self.assertNotEqual(serial[0], serial[1])
            # Repeated preparation reproduces the semantic result, even though PDF
            # or Matroska container metadata need not be byte-identical.
            repeated = media.prepare(name, Path(folder) / "again", count, 17, scale=scale)
            self.assertEqual(serial, [media.run_task(t) for t in repeated])
            corrupt = Path(tasks[0]["path"])
            corrupt.write_bytes(b"corrupt fixture")
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                media.run_task(tasks[0])

    def test_images_backends(self):
        self._exercise("images")

    def test_real_ocr_backends(self):
        self._exercise("ocr")

    def test_video_backends(self):
        self._exercise("video")

    def test_representative_images_backends(self):
        self._exercise("images", "representative", len(media.IMAGE_PROFILES))

    def test_representative_ocr_backends(self):
        self._exercise("ocr", "representative", len(media.OCR_PROFILES))

    def test_ocr_bold_fixture_preserves_failed_primary_truth(self):
        # Original typography read DELTA as DELIA on these declared documents.
        # The revised fixture keeps the independent expected text unchanged.
        with tempfile.TemporaryDirectory() as folder:
            try:
                tasks = media.prepare("ocr", Path(folder), 16,
                                      6864650689634725, scale="representative")
            except media.MissingDependency as exc:
                self.skipTest(str(exc))
            for index in (11, 15):
                task = tasks[index]
                self.assertEqual(task["fixture_revision"], "bold42-exact-text-v2")
                self.assertEqual(task["font_name"], "DejaVuSans-Bold.ttf")
                self.assertEqual(len(task["font_sha256"]), 64)
                self.assertIn("HOTEL NOVEMBER DELTA", task["expected_text"])
                self.assertNotIn("DELIA", task["expected_text"])
                self.assertEqual(media.run_task(task)["text"], task["expected_text"])

    def test_representative_ocr_resolution_regression(self):
        # At native 1650px long-side resolution, task 6/page 2 was recognized
        # as "ALPHA NOVEMBER DELIA". Keep the real independent DELTA oracle.
        with tempfile.TemporaryDirectory() as folder:
            try:
                tasks = media.prepare("ocr", Path(folder), 12, 12011,
                                      scale="representative")
            except media.MissingDependency as exc:
                self.skipTest(str(exc))
            self.assertIn("ALPHA NOVEMBER DELTA", tasks[6]["expected_pages"][1])
            for task in tasks:
                with self.subTest(index=task["index"]):
                    self.assertGreaterEqual(task["render_side"], 1800)
                    self.assertEqual(media.run_task(task)["text"], task["expected_text"])

    def test_representative_video_backends(self):
        self._exercise("video", "representative", len(media.VIDEO_PROFILES))

    def test_representative_profiles_and_caps(self):
        with tempfile.TemporaryDirectory() as folder:
            try:
                images = media.prepare("images", Path(folder), 6, 0, scale="representative")
                ocr = media.prepare("ocr", Path(folder), 4, 0, scale="representative")
                video = media.prepare("video", Path(folder), 4, 0, scale="representative")
            except media.MissingDependency as exc:
                self.skipTest(str(exc))
            self.assertEqual({t["source_format"] for t in images}, {"JPEG", "PNG", "BMP"})
            self.assertGreater(len({tuple(t["source_size"]) for t in images}), 3)
            self.assertEqual({t["page_count"] for t in ocr}, {1, 2, 3})
            self.assertEqual({t["codec"] for t in video}, {"ffv1", "png"})
            for task in images + ocr + video:
                self.assertLessEqual(Path(task["path"]).stat().st_size, media.MAX_INPUT_BYTES)
            bad_image = dict(images[0], source_size=[1, 1])
            with self.assertRaisesRegex(ValueError, "dimensions"):
                media.run_task(bad_image)
            with self.assertRaisesRegex(ValueError, "orientation"):
                media.run_task(dict(images[0], orientation=8))
            with self.assertRaisesRegex(ValueError, "page count"):
                media.run_task(dict(ocr[0], page_count=2))
            with self.assertRaisesRegex(ValueError, "cap"):
                media.run_task(dict(ocr[0], render_side=media.MAX_RENDER_SIDE + 1))
            with self.assertRaisesRegex(ValueError, "frame count"):
                media.run_task(dict(video[0], frames=video[0]["frames"] - 1))
            with self.assertRaisesRegex(ValueError, "cap"):
                media.run_task(dict(video[0], frames=media.MAX_VIDEO_FRAMES + 1))

    def test_invalid_requests(self):
        with tempfile.TemporaryDirectory() as folder:
            for count in (0, -1, 257, 1.5, True):
                with self.assertRaises(ValueError):
                    media.prepare("images", Path(folder), count, 1)
            with self.assertRaises(ValueError):
                media.prepare("unknown", Path(folder), 1, 1)
            with self.assertRaises(ValueError):
                media.prepare("images", Path(folder), 1, False)
            with self.assertRaisesRegex(ValueError, "scale"):
                media.prepare("images", Path(folder), 1, 1, scale="unbounded")

    def test_missing_dependency_is_explicit(self):
        with patch.object(media.importlib.util, "find_spec", return_value=None):
            with self.assertRaisesRegex(media.MissingDependency, "Pillow"):
                media._require("images")
        with patch.object(media.shutil, "which", return_value=None):
            # Ensure this test is independent of whether Pillow is installed.
            with patch.object(media.importlib.util, "find_spec", return_value=object()):
                with self.assertRaisesRegex(media.MissingDependency, "ffmpeg"):
                    media._require("video")

    def test_wrong_ocr_oracle_fails(self):
        with tempfile.TemporaryDirectory() as folder:
            try:
                task = media.prepare("ocr", Path(folder), 1, 3)[0]
            except media.MissingDependency as exc:
                self.skipTest(str(exc))
            task["expected_text"] = "NOT THE PAGE CONTENT"
            with self.assertRaisesRegex(ValueError, "OCR correctness mismatch"):
                media.run_task(task)

    def test_wrong_video_oracle_fails(self):
        with tempfile.TemporaryDirectory() as folder:
            try:
                task = media.prepare("video", Path(folder), 1, 3)[0]
            except media.MissingDependency as exc:
                self.skipTest(str(exc))
            task["expected_frames"][0] = "0" * 64
            with self.assertRaisesRegex(ValueError, "pixels differ"):
                media.run_task(task)


if __name__ == "__main__":
    unittest.main()
