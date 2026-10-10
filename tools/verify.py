"""Repository verification entry point (run after installing the package).

python tools/verify.py --examples
Use an isolated environment with a built wheel to verify installed artifacts.
"""
import argparse
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--examples", action="store_true")
    args = parser.parse_args()
    commands = [
        ["tools/sync_catalog.py", "--check"],
        ["-m", "unittest", "discover", "-s", "tests", "-v"],
        ["-m", "unittest", "benchmarks.test_run", "-v"],
        ["-m", "compileall", "-q", "src", "tests", "benchmarks", "examples", "tools"],
        ["-m", "intelligent_futures", "--status", "experimental", "--json"],
    ]
    if args.examples:
        commands.extend(["-m", f"examples.{name}"] for name in (
            "profiling", "bounded_pipeline", "cancellation", "custom_policy", "process_hints", "registered_tasks"))
    for command in commands:
        print("+", sys.executable, *command, flush=True)
        subprocess.run([sys.executable, *command], cwd=ROOT, check=True)
    print("All requested checks passed. Remote CI and other interpreters are separate checks.")


if __name__ == "__main__":
    main()
