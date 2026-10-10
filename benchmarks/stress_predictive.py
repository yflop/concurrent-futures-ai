"""Seeded correctness stress and raw timing observations for predictive mode.

Run: PYTHONPATH=src python -m benchmarks.stress_predictive --repeats 3
No performance threshold or prediction of general speedup is asserted. Tasks
are synthetic. Seeds fix payloads, not operating-system scheduling/timing.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import sys
import threading
import time

from intelligent_futures import IntelligentExecutor, TaskHints
from intelligent_futures.prediction import ExecutionObjective, HardwareCapabilities


def work(spec):
    index, iterations, delay, fail = spec
    if delay:
        time.sleep(delay)
    value = index
    for _ in range(iterations):
        value = (value * 1664525 + 1013904223) & 0xFFFFFFFF
    if fail:
        raise ValueError(f"expected failure {index}")
    return index, value


def batch_work(specs):
    """One process/thread invocation handles each explicit tuple chunk."""
    return tuple(work(spec) for spec in specs)


def run_batch_trial(specs, *, workers, max_pending, process_workers):
    # Batch failure semantics are tested separately; this trial checks exactly
    # one result per successful item and fewer executor submissions.
    clean_specs = [(i, iterations, delay, False) for i, iterations, delay, _ in specs]
    started = time.perf_counter()
    with IntelligentExecutor(max_workers=workers, process_workers=process_workers,
            max_pending=max_pending, predictive=True) as executor:
        chunks = list(executor.map_batches(batch_work, clean_specs, buffersize=2,
            min_batch_size=4, max_batch_size=32,
            hints=TaskHints(kind="cpu" if process_workers else "io",
                            process_safe=bool(process_workers), key="explicit-batches")))
    elapsed = time.perf_counter() - started
    outputs = [result for chunk in chunks for result in chunk]
    expected = [work((i, iterations, 0, False)) for i, iterations, _, _ in clean_specs]
    assert outputs == expected
    snapshot = executor.snapshot()
    assert snapshot["pending"] == 0
    assert snapshot["counts"]["submitted"] == len(chunks)
    assert snapshot["counts"]["completed"] == len(chunks)
    assert snapshot["counts"]["peak_pending"] <= max_pending
    assert len(chunks) <= (len(specs) + 3) // 4
    return {"elapsed_seconds": elapsed, "chunk_sizes": [len(chunk) for chunk in chunks],
            "outputs": outputs, "snapshot": snapshot,
            "checksum": hashlib.sha256(json.dumps(outputs).encode()).hexdigest()}


def machine():
    def optional(path):
        try:
            return Path(path).read_text().strip()
        except OSError:
            return None
    return {"python": sys.version, "platform": platform.platform(),
            "logical_cpus": os.cpu_count(), "hardware": asdict(HardwareCapabilities.detect()),
            "cgroup_cpu_max": optional("/sys/fs/cgroup/cpu.max"),
            "cgroup_memory_max": optional("/sys/fs/cgroup/memory.max"),
            "process_start_method": "spawn"}


def run_trial(specs, *, predictive, workers, producers, max_pending, process_workers, sample_interval=1):
    executor = IntelligentExecutor(max_workers=workers, process_workers=process_workers,
        max_pending=max_pending, predictive=predictive, policy="hints",
        profile_sample_interval=sample_interval,
        objective=ExecutionObjective(max_concurrency=workers) if predictive else None)
    records = []
    record_lock = threading.Lock()
    barrier = threading.Barrier(producers)
    def produce(shard):
        barrier.wait(timeout=30)
        local = []
        for spec in shard:
            started = time.perf_counter()
            hints = TaskHints(key="synthetic-io" if spec[2] else "synthetic-cpu",
                              kind="io" if spec[2] else "cpu", process_safe=bool(process_workers))
            future = executor.submit_profiled(work, spec, hints=hints) if predictive else executor.submit_task(work, spec, hints=hints)
            admitted = time.perf_counter()
            local.append((spec, future, started, admitted))
        with record_lock:
            records.extend(local)
    started = time.perf_counter()
    try:
        with ThreadPoolExecutor(max_workers=producers) as producer_pool:
            for future in [producer_pool.submit(produce, specs[i::producers]) for i in range(producers)]:
                future.result(timeout=120)
        outputs, observations = [], []
        for spec, future, submitted, admitted in records:
            try:
                result = future.result(timeout=120)
            except ValueError:
                if not spec[3]:
                    raise
                result = None
                status = "expected_failure"
            else:
                if spec[3]:
                    raise AssertionError("expected task failure was lost")
                expected = work((spec[0], spec[1], 0, False))
                if result != expected:
                    raise AssertionError("incorrect task result")
                status = "success"
                outputs.append(result)
            observations.append({"index": spec[0], "admission_seconds": admitted - submitted,
                                 "status": status, "result": result})
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
    elapsed = time.perf_counter() - started
    snapshot = executor.snapshot()
    counts = snapshot["counts"]
    assert snapshot["pending"] == 0
    assert counts["submitted"] == len(specs)
    assert counts["completed"] + counts["failed"] + counts["cancelled"] == len(specs)
    assert counts["cancelled"] == 0
    assert counts["peak_pending"] <= max_pending
    assert len({row["index"] for row in observations}) == len(specs)
    if predictive:
        assert counts["peak_pending"] <= workers
    return {"predictive": predictive, "elapsed_seconds": elapsed,
            "checksum": hashlib.sha256(json.dumps(sorted(outputs)).encode()).hexdigest(),
            "observations": sorted(observations, key=lambda row: row["index"]),
            "snapshot": snapshot, "exported_profiles": [asdict(profile) for profile in executor.export_profiles()]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=41073)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--tasks", type=int, default=300)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--producers", type=int, default=4)
    parser.add_argument("--max-pending", type=int, default=8)
    parser.add_argument("--process-workers", type=int, default=0)
    parser.add_argument("--sample-interval", type=int, default=1)
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results/predictive-stress.json"))
    args = parser.parse_args(argv)
    for name in ("repeats", "tasks", "workers", "producers", "max_pending", "sample_interval"):
        if getattr(args, name) < 1:
            parser.error(f"{name} must be positive")
    if args.process_workers < 0:
        parser.error("process-workers must be nonnegative")
    rng = random.Random(args.seed)
    specs = [(i, rng.randrange(50, 5000), rng.choice([0.0, 0.0005, 0.002]), rng.random() < .08) for i in range(args.tasks)]
    report = {"schema_version": 1, "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
              "machine": machine(), "config": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
              "methodology": "Synthetic seeded mixed work, repeated fresh executors; randomized baseline/predictive trial order. Timing includes executor work, validation and shutdown, excludes construction. No universal speedup claim. Per-task timing is admission latency only. Batch trials use successful variants of all inputs and include construction/shutdown, excluding correctness validation; no direct timing comparison to single-item trials is intended.",
              "specs": specs, "trials": [], "batch_trials": []}
    for repeat in range(args.repeats):
        order = [False, True]
        rng.shuffle(order)
        checksums = set()
        for predictive in order:
            result = run_trial(specs, predictive=predictive, workers=args.workers,
                producers=args.producers, max_pending=args.max_pending, process_workers=args.process_workers,
                sample_interval=args.sample_interval)
            result["repeat"] = repeat + 1
            checksums.add(result["checksum"])
            report["trials"].append(result)
            print(f"repeat={repeat + 1} predictive={predictive} elapsed={result['elapsed_seconds']:.6f}s", flush=True)
        batch = run_batch_trial(specs, workers=args.workers,
            max_pending=args.max_pending, process_workers=args.process_workers)
        batch["repeat"] = repeat + 1
        report["batch_trials"].append(batch)
        print(f"repeat={repeat + 1} batches={len(batch['chunk_sizes'])} items={len(specs)} elapsed={batch['elapsed_seconds']:.6f}s", flush=True)
        if len(checksums) != 1:
            raise AssertionError("baseline/predictive checksum mismatch")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
    print(f"Wrote raw observations and machine configuration to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
