"""Tiny real-fixture illustration of budgeted selection, not a speed benchmark.

Run: PYTHONPATH=src python -m examples.budgeted_tuning
Optional: --save choice.json writes a new configuration-only file, never overwrites.
No network, process workers, external dependencies, or production-task replay.
"""
import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import random
import time

from intelligent_futures import IntelligentExecutor, TaskHints
from intelligent_futures.tuning import (
    CalibrationObjective, Configuration, Measurement, export_configuration,
    inspect_holdout, load_configuration, tune,
)


WORKLOAD_KEY = "example-sha256:v1|64x4096-bytes|stdlib|fixed-batches|thread-only"
MEASURED_SCOPE = (
    "Cold: construction + first pass + shutdown; warm: second pass in same pool. "
    "CPU: both passes and pool lifecycle in this process. Input generation, serial "
    "reference, and output comparisons excluded. RSS and batch p95 unmeasured."
)


def digest_batch(items):
    return [hashlib.sha256(item).hexdigest() for item in items]


def run_fixture(pool, configuration, inputs):
    batches = pool.map_batches(
        digest_batch, inputs, buffersize=2 * configuration.workers,
        min_batch_size=configuration.batch_size,
        max_batch_size=configuration.batch_size,
        hints=TaskHints(kind="cpu", key="example-sha256-v1"),
    )
    return [value for batch in batches for value in batch]


def fixture_evaluator(namespace):
    def evaluate(configuration, seed):
        # Only caller-owned, side-effect-free, reproducible fixtures are executed.
        # The seed deliberately does not depend on candidate configuration.
        rng = random.Random(f"{namespace}:{seed}")
        inputs = [rng.randbytes(4096) for _ in range(64)]
        expected = digest_batch(inputs)
        cpu_started = time.process_time()
        cold_started = time.perf_counter()
        with IntelligentExecutor(**configuration.executor_kwargs()) as pool:
            cold_result = run_fixture(pool, configuration, inputs)
            cold = time.perf_counter() - cold_started
            warm_started = time.perf_counter()
            warm_result = run_fixture(pool, configuration, inputs)
            warm = time.perf_counter() - warm_started
            shutdown_started = time.perf_counter()
        shutdown = time.perf_counter() - shutdown_started
        cpu = time.process_time() - cpu_started
        # Exceptions propagate to the tuner's failure record; no success fallback.
        # Do not clamp a zero or invalid timer value into a fictional observation.
        return Measurement(
            cold_seconds=cold + shutdown,
            warm_seconds=warm,
            correct=(cold_result == expected and warm_result == expected),
            cpu_seconds=cpu,
        )
    return evaluate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--save", type=Path, help="new configuration-only JSON file")
    args = parser.parse_args()
    candidates = [Configuration("thread", workers, batch)
                  for workers in (1, 2) for batch in (1, 8)]
    report = tune(
        candidates, fixture_evaluator("training-v1"), max_evaluations=18, seed=42,
        min_repeats=3, max_repeats=6, reduction_factor=2,
        objective=CalibrationObjective(elapsed="combined"), allow_process=False,
    )
    # Preserve the full diagnostic result even when no configuration qualifies.
    print(json.dumps({"illustration_only": True, "scope": MEASURED_SCOPE,
                      "tuning": report.to_dict()}, indent=2, allow_nan=False))
    if report.selected is None:
        raise RuntimeError("No final candidate passed all observed tuning gates")

    # Fresh inputs from a separate namespace and seed; still the same generator.
    # This does not establish generalization to another real workload population.
    heldout = inspect_holdout(
        report, fixture_evaluator("heldout-v1"), repeats=3, seed=9001,
        allow_process=False,
    )
    print(json.dumps({"heldout": [asdict(record) for record in heldout]},
                     indent=2, allow_nan=False))
    if any(record.exclusion is not None for record in heldout):
        raise RuntimeError("Held-out checks failed; do not adopt or save this choice")

    # Export/load runs no evaluator and does not carry process permission forward.
    # A compatibility match is not a guarantee of future correctness or speed.
    payload = export_configuration(
        report, workload_key=WORKLOAD_KEY, measured_scope=MEASURED_SCOPE,
    )
    saved = load_configuration(payload, workload_key=WORKLOAD_KEY, allow_process=False)
    reuse_rng = random.Random("reuse-v1:19007")
    reuse_inputs = [reuse_rng.randbytes(4096) for _ in range(64)]
    with IntelligentExecutor(**saved.executor_kwargs(allow_process=False)) as pool:
        reused_result = run_fixture(pool, saved.configuration, reuse_inputs)
    if reused_result != digest_batch(reuse_inputs):
        raise RuntimeError("Illustrative reuse correctness check failed")
    if args.save is not None:
        # Exclusive creation avoids silently overwriting an existing selection.
        with args.save.open("x", encoding="utf-8") as stream:
            stream.write(payload + "\n")
    print(json.dumps({"saved_configuration": json.loads(payload),
                      "reuse_correct": True, "saved_file": str(args.save) if args.save else None},
                     indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
