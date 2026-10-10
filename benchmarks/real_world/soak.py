"""Bounded local sustained checks, not a production reliability certification."""
from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, wait
from contextlib import nullcontext
import hashlib
import json
import math
from pathlib import Path
import shlex
import sys
import tempfile
import threading
import time

from intelligent_futures import IntelligentExecutor, TaskHints
from . import execute_batch, module_for
from .run import cpu_seconds, workload_environment

NAMES = ("pipeline", "sqlite", "http", "cached_http", "downloads", "discovery")
CONFIGURATION = dict(max_workers=2, process_workers=0, max_pending=4,
                     policy="hints", predictive=False, profile_sample_interval=1)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def tree_rss():
    import psutil
    process = psutil.Process()
    total = 0
    for child in [process, *process.children(recursive=True)]:
        try:
            total += child.memory_info().rss
        except psutil.NoSuchProcess:
            pass
    return total


def children():
    import psutil
    return {(p.pid, p.create_time()) for p in psutil.Process().children(recursive=True)}


class MemorySampler:
    """10ms aggregate tree RSS; observed budgets, never enforced memory limits."""
    def __init__(self, read=tree_rss):
        self.read = read
        self.stop = threading.Event()
        self.samples = 0
        self.errors = []
        self.start = self.peak = self.end = None

    def sample(self):
        try:
            value = self.read()
            if self.start is None:
                self.start = value
            self.end = value
            self.peak = max(self.peak or 0, value)
            self.samples += 1
        except Exception as exc:
            if len(self.errors) < 8:
                self.errors.append(f"{type(exc).__name__}: {exc}")

    def __enter__(self):
        self.sample()
        def loop():
            while not self.stop.wait(.01):
                self.sample()
        self.thread = threading.Thread(target=loop, name="soak-rss", daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join()
        self.sample()

    def report(self):
        return {"start_bytes": self.start, "peak_bytes": self.peak, "end_bytes": self.end,
                "end_minus_start_bytes": None if self.end is None or self.start is None else self.end-self.start,
                "samples": self.samples, "errors": self.errors}


def accounting_ok(snapshot):
    counts = snapshot["counts"]
    return (snapshot["closed"] and snapshot["pending"] == 0
            and counts["submitted"] == counts["completed"] + counts["failed"] + counts["cancelled"]
            and counts["peak_pending"] <= CONFIGURATION["max_pending"]
            and counts["process_submitted"] == 0 and counts["rejected"] == 0)


def observe(run_task, tasks, expected, *, seconds=60., task_budget=50000,
            min_checks=12, clock=time.perf_counter, completion_callback=None):
    """Repeat fixed inputs until admission deadline or task cap, then drain.

    Clock injection supports fast pure-task tests. Callbacks run on workers and
    are explicitly caught/reported; Future.result alone does not propagate their
    exceptions. No workload sleeps, retries, cancellation or task killing.
    """
    if not math.isfinite(seconds) or not 0 < seconds <= 300:
        raise ValueError("seconds must be finite and in (0, 300]")
    if type(task_budget) is not int or not 1 <= task_budget <= 50000:
        raise ValueError("task budget must be 1..50000")
    if type(min_checks) is not int or not 1 <= min_checks <= task_budget:
        raise ValueError("min checks must be 1..task budget")
    if not tasks or len(tasks) != len(expected):
        raise ValueError("nonempty inputs and one reference per input required")
    original = digest(tasks)
    references = [digest(value) for value in expected]
    submitted = completed = errors = mismatches = 0
    examples = []
    windows = {}
    pending = {}
    callback_errors = []
    callback_count = 0
    callback_lock = threading.Lock()
    def callback(future):
        nonlocal callback_count
        try:
            if completion_callback is not None:
                completion_callback(future)
        except Exception as exc:
            with callback_lock:
                if len(callback_errors) < 8:
                    callback_errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            with callback_lock:
                callback_count += 1
    pool = IntelligentExecutor(**CONFIGURATION)
    started = clock()
    admission_end = started
    stop_reason = None
    try:
        while pending or stop_reason is None:
            while stop_reason is None and len(pending) < CONFIGURATION["max_pending"]:
                now = clock()
                if now - started >= seconds:
                    stop_reason = "duration_reached"
                    admission_end = now
                    break
                if submitted >= task_budget:
                    stop_reason = "task_budget_exhausted"
                    admission_end = now
                    break
                index = submitted % len(tasks)
                future = pool.submit_task(run_task, tasks[index], hints=TaskHints(kind="io", key="soak"))
                future.add_done_callback(callback)
                pending[future] = index
                submitted += 1
            if not pending:
                break
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                index = pending.pop(future)
                completed += 1
                now = clock()
                # Fixed 10s windows, bounded by duration plus one drain bucket.
                bucket = min(int(max(0., now-started)//10), math.ceil(seconds/10))
                window = windows.setdefault(bucket, {"tasks": 0, "errors": 0, "mismatches": 0})
                window["tasks"] += 1
                try:
                    value = future.result()
                    if digest(value) != references[index]:
                        mismatches += 1
                        window["mismatches"] += 1
                except Exception as exc:
                    errors += 1
                    window["errors"] += 1
                    if len(examples) < 8:
                        examples.append(f"{type(exc).__name__}: {exc}")
    finally:
        pool.shutdown(wait=True)
    elapsed = clock()-started
    snapshot = pool.snapshot()
    failures = []
    if stop_reason != "duration_reached":
        failures.append("task cap ended observation before requested duration")
    if completed < min_checks:
        failures.append("minimum completed checks not reached")
    if errors or mismatches:
        failures.append("task errors or reference mismatches")
    if callback_errors or callback_count != submitted:
        failures.append("completion callback errors or missing callbacks")
    if not accounting_ok(snapshot) or snapshot["counts"]["submitted"] != submitted or completed != submitted:
        failures.append("executor accounting or pending bound failed")
    if original != digest(tasks):
        failures.append("task inputs were mutated")
    window_rows = []
    for bucket, values in sorted(windows.items()):
        start = bucket*10
        end = min(elapsed, (bucket+1)*10) if bucket < math.ceil(seconds/10) else elapsed
        width = max(0., end-start)
        window_rows.append({"start_seconds": start, "end_seconds": end,
                            "throughput_per_second": values["tasks"]/width if width else None,
                            **values})
    return {"requested_seconds": seconds, "admission_seconds": admission_end-started,
            "actual_seconds_including_drain_shutdown": elapsed, "stop_reason": stop_reason,
            "duration_completed": stop_reason == "duration_reached" and completed >= min_checks,
            "submitted": submitted, "completed": completed, "errors": errors,
            "error_rate": errors/completed if completed else None,
            "reference_mismatches": mismatches, "error_examples": examples,
            "throughput_per_second": completed/elapsed if elapsed > 0 else None,
            "time_windows": window_rows,
            "callback_count": callback_count, "callback_errors": callback_errors,
            "snapshot_after_shutdown": snapshot, "failures": failures}


def _intentional_failure():
    raise ValueError("intentional separate fault check")


def exception_probe():
    """Separate from successful workload stream: exception reaches caller/callback."""
    observed = []
    with IntelligentExecutor(**CONFIGURATION) as pool:
        future = pool.submit(_intentional_failure)
        future.add_done_callback(lambda done: observed.append(type(done.exception()).__name__))
        try:
            future.result()
        except ValueError as exc:
            propagated = str(exc) == "intentional separate fault check"
        else:
            propagated = False
    snapshot = pool.snapshot()
    return {"passed": propagated and observed == ["ValueError"] and accounting_ok(snapshot)
            and snapshot["counts"]["failed"] == 1, "snapshot_after_shutdown": snapshot}


def acceptance_failures(row, *, max_rss_bytes, max_growth_bytes, max_cpu_seconds):
    failures = list(row["failures"])
    def valid(value):
        return type(value) in (float, int) and math.isfinite(value)
    memory = row["memory"]
    if memory["errors"] or not valid(memory["peak_bytes"]) or not 0 <= memory["peak_bytes"] <= max_rss_bytes:
        failures.append("sampled process-tree RSS unavailable or over peak budget")
    drift = memory["end_minus_start_bytes"]
    if not valid(drift) or drift > max_growth_bytes:
        failures.append("sampled end-start RSS unavailable or over growth budget")
    cpu = row["lifecycle_cpu_seconds"]
    if not valid(cpu) or cpu < 0 or cpu > max_cpu_seconds:
        failures.append("lifecycle CPU unavailable or over budget")
    if row["new_live_threads"] or row["new_live_child_processes"]:
        failures.append("new worker/server threads or child processes remain after cleanup")
    return failures


def soak(name, root, *, seconds=60., task_budget=50000, min_checks=12, count=6, seed=8701,
         max_rss_bytes=1024**3, max_growth_bytes=128*1024**2, max_cpu_seconds=600.):
    if name not in NAMES:
        raise ValueError("unsupported sustained workload")
    if type(count) is not int or not 1 <= count <= 12:
        raise ValueError("fixture count must be 1..12")
    baseline_threads = set(threading.enumerate())
    baseline_children = children()
    module = module_for(name)
    root.mkdir(parents=True, exist_ok=True)
    fixture = module.fixture(root) if hasattr(module, "fixture") else nullcontext()
    with fixture:
        tasks = module.prepare(name, root / "inputs", count, seed, scale="representative")
        rows = execute_batch((name, tasks))
        if any(row["error"] is not None for row in rows):
            raise RuntimeError("sequential reference failed: " + repr(rows))
        before = cpu_seconds()
        with MemorySampler() as memory:
            result = observe(module.run_task, tasks, [row["value"] for row in rows],
                             seconds=seconds, task_budget=task_budget, min_checks=min_checks)
        after = cpu_seconds()
    result.update(name=name, fixture_count=count, seed=seed, scale="representative",
                  reference_sha256=digest([row["value"] for row in rows]), memory=memory.report(),
                  lifecycle_cpu_seconds=None if before is None or after is None else after-before,
                  new_live_threads=[t.name for t in threading.enumerate() if t not in baseline_threads],
                  new_live_child_processes=sorted(children()-baseline_children))
    result["failures"] = acceptance_failures(result, max_rss_bytes=max_rss_bytes,
        max_growth_bytes=max_growth_bytes, max_cpu_seconds=max_cpu_seconds)
    result["accepted"] = not result["failures"]
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workloads", nargs="+", choices=NAMES, default=["http", "sqlite", "pipeline"])
    parser.add_argument("--seconds", type=float, default=60.)
    parser.add_argument("--task-budget", type=int, default=50000)
    parser.add_argument("--min-checks", type=int, default=12)
    parser.add_argument("--fixture-count", type=int, default=6)
    parser.add_argument("--seed", type=int, default=8701)
    parser.add_argument("--max-rss-mib", type=float, default=1024.)
    parser.add_argument("--max-growth-mib", type=float, default=128.)
    parser.add_argument("--max-cpu-seconds", type=float, default=600.)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if not math.isfinite(args.seconds) or not 0 < args.seconds <= 300:
        parser.error("seconds must be finite and in (0, 300]")
    if not 1 <= args.min_checks <= args.task_budget <= 50000 or not 1 <= args.fixture_count <= 12:
        parser.error("require 1 <= min-checks <= task-budget <= 50000 and fixture-count 1..12")
    if any(not math.isfinite(v) or v <= 0 for v in (args.max_rss_mib, args.max_growth_mib, args.max_cpu_seconds)):
        parser.error("resource budgets must be finite and positive")
    report = {"schema_version": 1, "environment": workload_environment(),
              "command": shlex.join([sys.executable, "-m", "benchmarks.real_world.soak", *(sys.argv[1:] if argv is None else argv)]),
              "configuration": CONFIGURATION, "configuration_provenance": "Explicit fixed thread-only reliability configuration; not calibrated or a performance winner.",
              "scope": "Bounded local representative-fixture observations, not uptime, task-killing, leak-freedom or deployment guarantees.",
              "budgets": {"task_budget_per_workload": args.task_budget, "min_checks": args.min_checks,
                          "max_rss_bytes": int(args.max_rss_mib*1024**2),
                          "max_growth_bytes": int(args.max_growth_mib*1024**2),
                          "max_cpu_seconds": args.max_cpu_seconds},
              "exception_probe": exception_probe(), "workloads": [], "failures": []}
    report["environment"]["command"] = report["command"]
    if not report["exception_probe"]["passed"]:
        report["failures"].append({"name": "exception_probe", "error": "exception propagation failed"})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="real-soak-") as directory:
        for name in args.workloads:
            try:
                row = soak(name, Path(directory)/name, seconds=args.seconds, task_budget=args.task_budget,
                           min_checks=args.min_checks, count=args.fixture_count, seed=args.seed,
                           max_rss_bytes=int(args.max_rss_mib*1024**2),
                           max_growth_bytes=int(args.max_growth_mib*1024**2), max_cpu_seconds=args.max_cpu_seconds)
                report["workloads"].append(row)
                if not row["accepted"]:
                    report["failures"].append({"name": name, "errors": row["failures"]})
            except Exception as exc:
                report["failures"].append({"name": name, "error": f"{type(exc).__name__}: {exc}"})
            args.output.write_text(json.dumps(report, indent=2)+"\n")
    return int(bool(report["failures"]))


if __name__ == "__main__":
    raise SystemExit(main())
