"""Run comparable, explicitly scoped cold/warm executor measurements.

Install the package, then run ``python -m benchmarks.run --quick`` at the
repository root. The JSON contains every repeat; the Markdown is a summary.
Only the main guard launches processes. No wall-clock threshold is a test.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import platform
import random
import shlex
import statistics
import subprocess
import sys
import sysconfig
import threading
import time
from typing import Any

from intelligent_futures import IntelligentExecutor, TaskHints

from .workloads import TaskSpec, build_workload, execute_task

ENGINES = ("sequential", "thread", "process", "intelligent-threads",
           "intelligent-hints", "intelligent-adaptive")
WORKLOADS = ("cpu", "io", "mixed")


def percentile(values: list[float], fraction: float) -> float:
    """Linearly interpolate the sorted sample at (n - 1) * fraction.

    >>> percentile([0.0, 10.0], .95)
    9.5
    >>> percentile([2.0], .95)
    2.0
    """
    if not values:
        raise ValueError("Cannot summarize an empty sample")
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower, upper = math.floor(position), math.ceil(position)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def distribution(values: list[float]) -> dict[str, float | int]:
    return {"count": len(values), "min": min(values),
            "median": statistics.median(values), "p95": percentile(values, .95),
            "max": max(values), "mean": statistics.mean(values),
            "stdev": statistics.stdev(values) if len(values) > 1 else 0.0}


def read_optional(path: str) -> str | None:
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except OSError:
        return None


def environment_metadata() -> dict[str, Any]:
    try:
        affinity = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        affinity = None
    cpu_model = None
    for line in (read_optional("/proc/cpuinfo") or "").splitlines():
        if line.startswith("model name"):
            cpu_model = line.split(":", 1)[-1].strip()
            break
    cpu_max = read_optional("/sys/fs/cgroup/cpu.max")
    cpu_quota = None
    if cpu_max:
        parts = cpu_max.split()
        if len(parts) == 2 and parts[0] != "max":
            try:
                cpu_quota = int(parts[0]) / int(parts[1])
            except (ValueError, ZeroDivisionError):
                pass
    root = Path(__file__).resolve().parents[1]
    try:
        revision = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root,
            stderr=subprocess.DEVNULL, text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        revision = None
    return {"recorded_at_utc": datetime.now(timezone.utc).isoformat(),
            "python": sys.version, "implementation": platform.python_implementation(),
            "platform": platform.platform(), "cpu_model": cpu_model,
            "logical_cpus": os.cpu_count(), "affinity_cpus": affinity,
            "cgroup_cpu_max": cpu_max, "cgroup_cpu_quota_cores": cpu_quota,
            "cgroup_memory_max": read_optional("/sys/fs/cgroup/memory.max"),
            "free_threaded_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
            "process_start_method": "spawn", "git_revision": revision,
            "command": shlex.join([sys.executable, "-m", "benchmarks.run", *sys.argv[1:]])}


def make_executor(engine: str, config: dict[str, Any]) -> Any:
    if engine == "sequential":
        return None
    if engine == "thread":
        return ThreadPoolExecutor(max_workers=config["workers"])
    if engine == "process":
        return ProcessPoolExecutor(max_workers=config["workers"],
                                   mp_context=multiprocessing.get_context("spawn"))
    policy = engine.removeprefix("intelligent-")
    return IntelligentExecutor(
        max_workers=config["hybrid_thread_workers"],
        process_workers=(0 if policy == "threads" else config["hybrid_process_workers"]),
        max_pending=config["max_pending"], policy=policy,
        mp_context=multiprocessing.get_context("spawn"))


def run_batch(executor: Any, engine: str, specs: list[TaskSpec]) -> dict[str, Any]:
    """Measure from first submission attempt until parent completion callbacks.

    The stdlib pools have their usual unbounded submission queues. Intelligent
    admission may block inside submit_task. Both that blocking and queue delay
    remain in end-to-end latency, and submission-call duration is separate.
    Worker service times use durations only: no cross-process clock subtraction.
    """
    count = len(specs)
    started = time.perf_counter()
    values: list[int] = []
    service_times: list[float] = []
    latencies = [0.0] * count
    submit_times = [0.0] * count
    if executor is None:
        for index, spec in enumerate(specs):
            attempted = time.perf_counter()
            value, service = execute_task(spec)
            values.append(value)
            service_times.append(service)
            latencies[index] = time.perf_counter() - attempted
        # Sequential work has no submit call. Null prevents misleading zero-cost claims.
        submission_samples: list[float] | None = None
    else:
        completed = 0
        callback_lock = threading.Lock()
        callbacks_finished = threading.Event()
        futures = []

        def on_done(_future: Any, *, index: int, attempted: float) -> None:
            nonlocal completed
            receipt = time.perf_counter()
            with callback_lock:
                latencies[index] = receipt - attempted
                completed += 1
                if completed == count:
                    callbacks_finished.set()

        for index, spec in enumerate(specs):
            attempted = time.perf_counter()
            if engine.startswith("intelligent-"):
                kind = "unknown" if engine == "intelligent-adaptive" else spec.kind
                future = executor.submit_task(
                    execute_task, spec,
                    hints=TaskHints(kind=kind, key=f"benchmark.{spec.kind}", process_safe=True))
            else:
                future = executor.submit(execute_task, spec)
            submit_times[index] = time.perf_counter() - attempted
            # add_done_callback also works if this Future has already completed.
            future.add_done_callback(
                lambda f, i=index, t=attempted: on_done(f, index=i, attempted=t))
            futures.append(future)
        for future in futures:
            value, service = future.result()
            values.append(value)
            service_times.append(service)
        callbacks_finished.wait()
        submission_samples = submit_times
    elapsed = time.perf_counter() - started
    checksum = hashlib.sha256(json.dumps(values, separators=(",", ":")).encode()).hexdigest()
    return {"batch_seconds": elapsed, "tasks": count,
            "throughput_tasks_per_second": count / elapsed,
            "checksum": checksum, "task_latency_seconds": latencies,
            "task_service_seconds": service_times,
            "submission_call_seconds": submission_samples}


def run_trial(engine: str, specs: list[TaskSpec], phase: str,
              config: dict[str, Any]) -> dict[str, Any]:
    lifecycle_started = time.perf_counter()
    executor = make_executor(engine, config)
    setup_seconds = time.perf_counter() - lifecycle_started
    warmup_seconds: list[float] = []
    counts_before: dict[str, int] = {}
    try:
        if phase == "warm":
            for _ in range(config["warmup_batches"]):
                warmup_seconds.append(run_batch(executor, engine, specs)["batch_seconds"])
            if engine.startswith("intelligent-"):
                counts_before = executor.snapshot()["counts"]
        result = run_batch(executor, engine, specs)
    finally:
        shutdown_started = time.perf_counter()
        if executor is not None:
            executor.shutdown(wait=True)
        shutdown_seconds = time.perf_counter() - shutdown_started
    lifecycle_seconds = time.perf_counter() - lifecycle_started
    snapshot = executor.snapshot() if engine.startswith("intelligent-") else None
    # Cold includes construction, lazy startup, work, and orderly shutdown.
    # Warm excludes construction, all untimed warmups, snapshot, and shutdown.
    result.update({"phase": phase, "setup_seconds": setup_seconds,
                   "shutdown_seconds": shutdown_seconds,
                   "warmup_batch_seconds": warmup_seconds,
                   "lifecycle_seconds": lifecycle_seconds,
                   "timed_seconds": lifecycle_seconds if phase == "cold" else result["batch_seconds"],
                   "measured_backend_counts": {
                       backend: snapshot["counts"][f"{backend}_submitted"]
                       - counts_before.get(f"{backend}_submitted", 0)
                       for backend in ("thread", "process")} if snapshot else None,
                   "executor_snapshot": snapshot})
    return result


def summarize(runs: list[dict[str, Any]]) -> dict[str, Any]:
    samples: dict[str, Any] = {}
    for workload in WORKLOADS:
        by_engine = {}
        for engine in ENGINES:
            phases = {}
            for phase in ("cold", "warm"):
                matching = [run for run in runs if run["workload"] == workload
                            and run["engine"] == engine and run["phase"] == phase]
                if not matching:
                    continue
                task_latency = [sample for run in matching for sample in run["task_latency_seconds"]]
                service = [sample for run in matching for sample in run["task_service_seconds"]]
                submission = [sample for run in matching for sample in run["submission_call_seconds"] or []]
                phases[phase] = {
                    "timed_seconds": distribution([run["timed_seconds"] for run in matching]),
                    "batch_seconds": distribution([run["batch_seconds"] for run in matching]),
                    "task_latency_seconds": distribution(task_latency),
                    "task_service_seconds": distribution(service),
                    "submission_call_seconds": distribution(submission) if submission else None,
                    "throughput_tasks_per_second": distribution([
                        run["tasks"] / run["timed_seconds"] for run in matching])}
            if phases:
                by_engine[engine] = phases
        if by_engine:
            samples[workload] = by_engine
    return samples


METHODOLOGY = [
    "CPU is a deterministic pure-Python integer loop; I/O is time.sleep, not real network/storage.",
    "Mixed batches contain alternating CPU/I/O task identities, shuffled with a recorded fixed seed.",
    "Each cold and warm sample uses an independent executor. Execution order is shuffled per repeat.",
    "Cold timed_seconds is the full lifecycle including construction, work, and shutdown. Warm timed_seconds is only the measured batch after untimed warmup batches.",
    "Warmup means prior batches on that executor. A route selected only after learning can still start a previously unused process pool during a warm sample.",
    "All task results must match each workload's checksum across every engine, phase, and repeat.",
    "Task latency starts immediately before submission and ends in a parent-process done callback, including bounded-admission waiting, queueing, execution, transfer, and callback scheduling. Sequential latency is one direct call.",
    "Submission-call duration is recorded separately. Stdlib executors retain their default unbounded queues; IntelligentExecutor has max_pending admission. The backlog and latency distributions therefore differ by design.",
    "Worker service duration is measured locally and includes descheduling. No timestamps from different processes are subtracted.",
    "The default hybrid splits the baseline worker budget between thread and process pools. CPU-only and I/O-only workloads can use only part of that split; compare the recorded pool sizes, not only timings.",
    "The intelligent-threads control uses the hybrid thread count and zero process workers. Hints uses known task kinds. Adaptive uses unknown kinds with separate CPU/I/O profile keys; all benchmark tasks explicitly permit processes.",
    "Each batch uses small inputs and results. Serialization-heavy, cancellation, native GIL-releasing code, memory, fairness, and production throughput are not measured.",
    "p95 uses linear interpolation over finite observations. Repeat-level distributions and pooled task-level distributions are distinct; neither is a confidence interval.",
    "No performance pass/fail threshold or universal speedup is asserted. Host contention and container CPU quotas can dominate results."
]


def write_markdown(report: dict[str, Any], path: Path) -> None:
    env, config = report["environment"], report["config"]
    lines = ["# Executor benchmark measurements", "",
             "Generated by `python -m benchmarks.run`; raw samples are in the adjacent JSON file.", "",
             f"- UTC: {env['recorded_at_utc']}",
             f"- Python: {env['python'].splitlines()[0]}",
             f"- Platform: {env['platform']}",
             f"- CPU model: {env['cpu_model'] or 'not available'}",
             f"- CPUs: logical={env['logical_cpus']}, affinity={env['affinity_cpus']}, cgroup quota={env['cgroup_cpu_quota_cores']}",
             f"- Process start: {env['process_start_method']}; free-threaded build: {env['free_threaded_build']}",
             f"- Baseline workers: {config['workers']}; hybrid thread/process workers: {config['hybrid_thread_workers']}/{config['hybrid_process_workers']}",
             f"- Tasks/batch: {config['tasks']}; CPU iterations/task: {config['cpu_iterations']}; sleep/task: {config['io_delay']} s",
             f"- Repeats: {config['repeats']}; warmup batches per warm sample: {config['warmup_batches']}; max pending: {config['max_pending']}", "",
             "All recorded checksums matched. Measurements describe this one run, not a product-wide speedup claim.", ""]
    for workload, engines in report["summary"].items():
        lines += [f"## {workload.capitalize()}", "",
                  "Batch timings are across repeats; task latencies pool individual tasks across repeats.", "",
                  "| Engine | Phase | Median timed batch (ms) | p95 timed batch (ms) | Median task latency (ms) | p95 task latency (ms) | Median submit call (ms) | Median tasks/s |",
                  "|---|---|---:|---:|---:|---:|---:|---:|"]
        for engine, phases in engines.items():
            for phase, stats in phases.items():
                timing, latency = stats["timed_seconds"], stats["task_latency_seconds"]
                submission = stats["submission_call_seconds"]
                submit_text = f"{1000 * submission['median']:.3f}" if submission else "n/a"
                lines.append(f"| {engine} | {phase} | {1000 * timing['median']:.3f} | {1000 * timing['p95']:.3f} | {1000 * latency['median']:.3f} | {1000 * latency['p95']:.3f} | {submit_text} | {stats['throughput_tasks_per_second']['median']:.1f} |")
        lines.append("")
    route_rows = []
    for workload in report["summary"]:
        for engine in ENGINES:
            for phase in ("cold", "warm"):
                measured = [run["measured_backend_counts"] for run in report["runs"]
                            if run["workload"] == workload and run["engine"] == engine
                            and run["phase"] == phase and run.get("measured_backend_counts")]
                if measured:
                    threads = sum(row["thread"] for row in measured)
                    processes = sum(row["process"] for row in measured)
                    route_rows.append(f"| {workload} | {engine} | {phase} | {threads} | {processes} |")
    if route_rows:
        lines += ["## Measured routing", "",
                  "Accepted tasks summed across repeats, excluding warmup batches. Final raw snapshots also include warmups.", "",
                  "| Workload | Engine | Phase | Thread tasks | Process tasks |",
                  "|---|---|---|---:|---:|", *route_rows, ""]
    lines += ["## Methodology and limits", "", *[f"- {note}" for note in report["methodology"]], "",
              "## Reproduce", "", "```sh", env["command"], "```", ""]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true", help="Small smoke run; not a statistically strong benchmark")
    parser.add_argument("--tasks", type=int)
    parser.add_argument("--cpu-iterations", type=int)
    parser.add_argument("--io-delay", type=float)
    parser.add_argument("--repeats", type=int)
    parser.add_argument("--workers", type=int, default=4, help="Workers in each stdlib pool")
    parser.add_argument("--hybrid-thread-workers", type=int)
    parser.add_argument("--hybrid-process-workers", type=int)
    parser.add_argument("--max-pending", type=int, default=64)
    parser.add_argument("--warmup-batches", type=int, default=2)
    parser.add_argument("--seed", type=int, default=20261010)
    parser.add_argument("--engines", nargs="+", choices=ENGINES, default=list(ENGINES))
    parser.add_argument("--workloads", nargs="+", choices=WORKLOADS, default=list(WORKLOADS))
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results/latest.json"))
    parser.add_argument("--markdown", type=Path, help="Defaults to --output with .md suffix")
    args = parser.parse_args(argv)
    defaults = {"tasks": 12 if args.quick else 48,
                "cpu_iterations": 20_000 if args.quick else 200_000,
                "io_delay": .004 if args.quick else .01,
                "repeats": 2 if args.quick else 5}
    for field, default in defaults.items():
        if getattr(args, field) is None:
            setattr(args, field, default)
    if args.hybrid_thread_workers is None:
        args.hybrid_thread_workers = max(1, args.workers // 2)
    if args.hybrid_process_workers is None:
        args.hybrid_process_workers = max(0, args.workers - args.hybrid_thread_workers)
    for field in ("tasks", "cpu_iterations", "repeats", "workers", "hybrid_thread_workers", "max_pending", "warmup_batches"):
        if getattr(args, field) < 1:
            parser.error(f"--{field.replace('_', '-')} must be positive")
    if args.hybrid_process_workers < 0:
        parser.error("--hybrid-process-workers must be non-negative")
    if not math.isfinite(args.io_delay) or args.io_delay < 0:
        parser.error("--io-delay must be finite and non-negative")
    if args.tasks < 2 and "mixed" in args.workloads:
        parser.error("Mixed workloads require at least two tasks")
    if args.markdown is None:
        args.markdown = args.output.with_suffix(".md")
    if args.output.resolve() == args.markdown.resolve():
        parser.error("JSON and Markdown outputs must have different paths")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = {key: value for key, value in vars(args).items() if key not in {"output", "markdown"}}
    report: dict[str, Any] = {"schema_version": 1, "environment": environment_metadata(),
                              "config": config, "methodology": METHODOLOGY, "runs": []}
    workloads = {name: build_workload(name, args.tasks, args.cpu_iterations,
                                      args.io_delay, args.seed) for name in args.workloads}
    expected: dict[str, str] = {}
    rng = random.Random(args.seed)
    for repeat in range(args.repeats):
        order = [(name, engine, phase) for name in args.workloads for engine in args.engines
                 for phase in ("cold", "warm")]
        rng.shuffle(order)
        for name, engine, phase in order:
            result = run_trial(engine, workloads[name], phase, config)
            result.update({"repeat": repeat + 1, "workload": name, "engine": engine})
            if name in expected and result["checksum"] != expected[name]:
                raise RuntimeError(f"Checksum mismatch for {name}/{engine}/{phase}")
            expected[name] = result["checksum"]
            report["runs"].append(result)
            print(f"{repeat + 1}/{args.repeats} {name:5} {engine:21} {phase:4}: {result['timed_seconds']:.4f}s", flush=True)
    report["checksums"] = expected
    report["summary"] = summarize(report["runs"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_markdown(report, args.markdown)
    print(f"Wrote {args.output} and {args.markdown}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
