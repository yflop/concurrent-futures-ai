"""Short fixture-only API demo, not evidence for production adoption.

Eight held-out blocks keep the default quick; the default EvaluationPlan uses
24. More repeats of this toy workload do not make it representative of an app.
All negative observations remain in the report. --save writes a new JSON file
without overwriting an earlier experiment. No live work is captured or replayed.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import random
from tempfile import TemporaryDirectory

from intelligent_futures.evaluation import (
    EvaluationConfiguration, EvaluationPlan, evaluate_workload,
)
from intelligent_futures.evaluation_workloads import EvaluationFixture, callable_workload


MAX_PENDING = 4
FILE_COUNT = 8
FILE_BYTES = 32_768


def file_digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


@contextmanager
def fresh_fixture(seed):
    """Each measurement owns fresh files; equal seeds produce equal contents."""
    rng = random.Random(seed)
    with TemporaryDirectory(prefix="intelligent-futures-demo-") as directory:
        paths = []
        expected = []
        for index in range(FILE_COUNT):
            payload = rng.randbytes(FILE_BYTES)
            path = Path(directory) / f"input-{index}.bin"
            path.write_bytes(payload)
            paths.append(str(path))
            expected.append(hashlib.sha256(payload).hexdigest())
        yield EvaluationFixture(
            inputs=tuple(paths),
            validate=lambda results: results == tuple(expected),
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--heldout-blocks", type=int, default=8,
                        help="fixed held-out count, declared before running (default: 8 demo; 24 plan default)")
    parser.add_argument("--expected-reuses", type=int, default=None,
                        help="honest expected future runs of this same fixture scope; unknown blocks adoption")
    parser.add_argument("--save", type=Path,
                        help="write complete evidence JSON to a new file, without overwrite")
    args = parser.parse_args()
    workload = callable_workload(
        file_digest, fresh_fixture,
        name="temporary-file-sha256-demo",
        version="v1:8x32768:fresh-files:writes-before-timed-reads",
        repeatable=True,
        # A declaration for the generated fixture distribution. Random seeds
        # alone do not prove independent timing noise or production relevance.
        independent=True,
        max_tasks=FILE_COUNT, max_pending=MAX_PENDING,
    )
    configurations = (
        EvaluationConfiguration("thread", workers=1),
        EvaluationConfiguration("thread", workers=2),
    )
    plan = EvaluationPlan(
        training_blocks=3, heldout_blocks=args.heldout_blocks,
        max_workers=2, max_pending=MAX_PENDING,
        expected_reuses=args.expected_reuses,
    )
    report = evaluate_workload(
        workload,
        stdlib_configurations=configurations,
        intelligent_configurations=configurations,
        fixed_baseline=configurations[0], plan=plan,
    )
    print("Short fixture demonstration; this is not application performance evidence.")
    print("Scope: pool construction + execution + waiting shutdown; setup/validation excluded.")
    print("Files are written before timing; filesystem cache state is not cold-controlled.")
    print(f"Decision: {report.status}")
    print(f"Reasons: {', '.join(report.reasons) or 'all declared gates passed for this fixture'}")
    print(f"Evaluations: {report.attempted_evaluations} attempted, "
          f"{report.successful_evaluations} successful, {report.failed_evaluations} failed, "
          f"{report.skipped_evaluations} skipped")
    print(f"Discovery cost: {report.discovery_seconds:.6f}s")
    print(f"Empirical break-even reuses: {report.empirical_break_even_reuses}")
    for comparison in report.comparisons:
        print(f"{comparison.role} baseline: median speedup={comparison.median_speedup!r}; "
              f"lower speedup={comparison.lower_speedup!r}; "
              f"reasons={comparison.reasons}")
    if args.save is not None:
        with args.save.open("x", encoding="utf-8") as output:
            json.dump(report.to_json(), output, indent=2, allow_nan=False)
            output.write("\n")
        print(f"Complete report saved to {args.save}")
    # Even a recommendation here applies only to the generated demo fixture.
    # Nothing changes an application factory or deploys a saved JSON report.


if __name__ == "__main__":
    main()
