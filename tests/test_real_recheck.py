import json
from pathlib import Path
import tempfile
import unittest
from benchmarks.real_world.recheck import selections


class RecheckTests(unittest.TestCase):
    def test_validates_frozen_settings_without_evaluation(self):
        configs = {key: {"backend": "thread", "workers": 2, "batch_size": 1,
                        "profile_sample_interval": 0} for key in ("stdlib", "intelligent")}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            path.write_text(json.dumps({"workloads": [{"name": "images", "summary": {"chosen_from_calibration": configs}}]}))
            rows = selections([path])
            self.assertEqual(rows["images"]["configurations"]["intelligent"].workers, 2)
            with self.assertRaisesRegex(ValueError, "duplicate"):
                selections([path, path])
            configs["stdlib"]["workers"] = 0
            path.write_text(json.dumps({"workloads": [{"name": "images", "summary": {"chosen_from_calibration": configs}}]}))
            with self.assertRaises(ValueError):
                selections([path])


if __name__ == "__main__":
    unittest.main()
