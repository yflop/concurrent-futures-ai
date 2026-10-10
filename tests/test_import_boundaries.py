"""The core/framework must stay usable without optional dependencies."""
import json
import subprocess
import sys
import unittest


class ImportBoundaryTests(unittest.TestCase):
    def test_optional_sdk_is_not_loaded_by_imports(self):
        script = '''
import json, sys
import intelligent_futures
import intelligent_futures.framework
import intelligent_futures.integrations.mcp
print(json.dumps([name for name in sys.modules if name == "mcp" or name.startswith("mcp.")]))
'''
        result = subprocess.run([sys.executable, "-c", script], check=True,
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(json.loads(result.stdout), [])


if __name__ == "__main__":
    unittest.main()
