"""Recheck previously selected settings on the final source and fresh inputs.

This never retunes or treats old timing as a measurement of the current source.
The source reports are trusted local benchmark artifacts, not configuration code.
"""
import argparse
from contextlib import nullcontext
import hashlib
import json
from pathlib import Path
import tempfile

from . import module_for, execute_batch
from .run import Configuration, trial, acceptance_failures, workload_environment


def selections(paths):
    selected = {}
    for path in paths:
        if path.stat().st_size > 32 * 1024 * 1024:
            raise ValueError("source report exceeds 32 MiB")
        payload = json.loads(path.read_text())
        for row in payload["workloads"]:
            name = row["name"]
            module_for(name)  # Validate the workload allowlist.
            if name in selected:
                raise ValueError("duplicate workload selection: " + name)
            configs = row["summary"]["chosen_from_calibration"]
            if set(configs) != {"stdlib", "intelligent"}:
                raise ValueError("both engine selections are required")
            validated = {key: Configuration(**value) for key, value in configs.items()}
            if any(c.workers > 8 or c.batch_size > 32 for c in validated.values()):
                raise ValueError("recheck permits at most 8 workers and batch size 32")
            selected[name] = {"configurations": validated,
                              "source_report": str(path),
                              "source_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    return selected


def main():
    import shlex
    import sys
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="+", type=Path)
    parser.add_argument("--count", type=int, default=12)
    parser.add_argument("--seed", type=int, default=12011)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.count <= 64:
        parser.error("count must be 1..64")
    chosen = selections(args.reports)
    result = {"schema_version": 1, "environment": workload_environment(),
              "scope": "Final-source regression of frozen selected configurations on fresh representative fixtures; no retuning, no new full-grid superiority claim.",
              "seed": args.seed, "count_per_phase": args.count,
              "selection_callback_invocations": 0, "workloads": [], "failures": []}
    result["environment"]["command"] = shlex.join([sys.executable, "-m", "benchmarks.real_world.recheck", *sys.argv[1:]])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="workload-recheck-") as directory:
        for name, selection in chosen.items():
            print("Rechecking " + name, flush=True)
            root = Path(directory) / name
            root.mkdir()
            module = module_for(name)
            fixture = module.fixture(root) if hasattr(module, "fixture") else nullcontext()
            try:
                with fixture:
                    tasks = module.prepare(name, root / "fixture", args.count, args.seed, scale="representative")
                    oracle = execute_batch((name, tasks))
                    if any(row["error"] for row in oracle):
                        raise RuntimeError("sequential oracle failed: " + repr(oracle))
                    checksum = hashlib.sha256(json.dumps([r["value"] for r in oracle], sort_keys=True,
                        separators=(",", ":")).encode()).hexdigest()
                    prepared = {"tasks": tasks, "reference_sha256": checksum}
                    heldout = [trial(name, engine, config, args.count, args.seed, root / engine, prepared=prepared)
                               for engine, config in selection["configurations"].items()]
                failures = acceptance_failures(heldout)
                result["workloads"].append({"name": name, "source_report": selection["source_report"],
                    "source_report_sha256": selection["source_sha256"], "heldout": heldout,
                    "accepted": not failures, "acceptance_failures": failures})
                if failures:
                    result["failures"].append({"name": name, "details": failures})
            except Exception as exc:
                result["failures"].append({"name": name, "error": type(exc).__name__ + ": " + str(exc)})
            args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    return 1 if result["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
