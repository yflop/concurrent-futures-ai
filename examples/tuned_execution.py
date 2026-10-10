"""Explicit local-fixture calibration, followed by independent held-out inspection.

Run: python examples/tuned_execution.py
No network, saved profiles, application task discovery, or process workers.
"""
from dataclasses import asdict
import hashlib
import json
import random
import time

from intelligent_futures import IntelligentExecutor, TaskHints
from intelligent_futures.tuning import Configuration, Measurement, calibrate, inspect_holdout


def digest_batch(items):
    return [hashlib.sha256(item).hexdigest() for item in items]


def fixture_evaluator(item_count, fixture_namespace):
    def evaluate(configuration, seed):
        # This callback explicitly creates and executes two idempotent fixture
        # passes: one cold, one warm. The library never decides to replay tasks.
        rng = random.Random(f'{fixture_namespace}:{seed}')
        inputs = [rng.randbytes(4096) for _ in range(item_count)]
        expected = digest_batch(inputs)
        hints = TaskHints(kind='cpu', key='local-sha256-v1')
        def run(pool):
            batches = pool.map_batches(
                digest_batch, inputs, buffersize=2 * configuration.workers,
                min_batch_size=configuration.batch_size,
                max_batch_size=configuration.batch_size, hints=hints,
            )
            return [value for batch in batches for value in batch]
        # Cold = construction + first pass, excluding shutdown and correctness
        # comparison. Warm = second pass on that same live executor.
        start = time.perf_counter()
        with IntelligentExecutor(**configuration.executor_kwargs()) as pool:
            cold_result = run(pool)
            cold = time.perf_counter() - start
            start = time.perf_counter()
            warm_result = run(pool)
            warm = time.perf_counter() - start
        return Measurement(cold, warm, cold_result == expected and warm_result == expected)
    return evaluate


def main():
    candidates = [Configuration('thread', workers, batch, sampling)
                  for workers in (1, 2, 4) for batch in (1, 16) for sampling in (0, 8)]
    report = calibrate(candidates, fixture_evaluator(128, 'calibration'), repeats=3, seed=42)
    if report.selected is None:
        raise RuntimeError('No candidate passed calibration checks')
    holdout = inspect_holdout(report, fixture_evaluator(192, 'holdout'), repeats=2, seed=917)
    print(json.dumps({'calibration': report.to_dict(),
                      'holdout': [asdict(record) for record in holdout]}, indent=2, allow_nan=False))


if __name__ == '__main__':
    main()
