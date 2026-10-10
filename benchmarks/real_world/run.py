"""Bounded calibration/held-out measurements; no third-party traffic.

python -m benchmarks.real_world.run --quick --output benchmarks/results/real-world.json
Run from the repository after installing its optional workload requirements.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, FIRST_COMPLETED
from contextlib import nullcontext
from dataclasses import asdict
import hashlib
import json
import multiprocessing
import os
import math
from pathlib import Path
import random
import shlex
import subprocess
import sys
from importlib.metadata import version, PackageNotFoundError
import statistics
import tempfile
import threading
import time

from intelligent_futures import IntelligentExecutor, TaskHints, ExecutionObjective
from intelligent_futures.tuning import Configuration, Measurement, CalibrationObjective, calibrate
from benchmarks.run import environment_metadata, percentile
from . import NAMES, module_for, execute_batch
from .features import batch_features


def cpu_seconds():
    """POSIX CPU seconds for self plus waited children; unavailable on Windows."""
    try:
        import resource
        own = resource.getrusage(resource.RUSAGE_SELF)
        children = resource.getrusage(resource.RUSAGE_CHILDREN)
        return own.ru_utime + own.ru_stime + children.ru_utime + children.ru_stime
    except (ImportError, AttributeError, OSError):
        return None


def workload_environment():
    info = environment_metadata()
    info["native_runtime_environment"] = {key: os.environ.get(key) for key in (
        "MALLOC_ARENA_MAX", "MALLOC_MMAP_THRESHOLD_", "GLIBC_TUNABLES",
        "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS")}
    spec = getattr(sys.modules.get("__main__"), "__spec__", None)
    command_module = getattr(spec, "name", None) or "benchmarks.real_world.run"
    info["command"] = shlex.join([sys.executable, "-m", command_module, *sys.argv[1:]])
    root = Path(__file__).resolve().parents[2]
    files = sorted(list((root / "src" / "intelligent_futures").rglob("*.py"))
                   + list((root / "benchmarks" / "real_world").rglob("*.py")))
    info["source_sha256"] = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    try:
        info["git_dirty"] = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=root, text=True))
    except (OSError, subprocess.CalledProcessError):
        info["git_dirty"] = None
    info["dependencies"] = {}
    for package in ("Pillow", "requests", "requests-cache", "networkx", "psutil"):
        try:
            info["dependencies"][package] = version(package)
        except PackageNotFoundError:
            info["dependencies"][package] = None
    info["native_tools"] = {}
    for executable, flag in (("ffmpeg", "-version"), ("pdftoppm", "-v"), ("tesseract", "--version")):
        try:
            output = subprocess.check_output([executable, flag], stderr=subprocess.STDOUT, text=True, timeout=10)
            info["native_tools"][executable] = output.splitlines()[0]
        except (OSError, subprocess.SubprocessError):
            info["native_tools"][executable] = None
    return info


# Keep the public benchmark import compatible; collection now fails closed.
from .resource_sampling import RSSSampler


def make_executor(engine, config):
    if engine == "stdlib":
        if config.backend == "process":
            return ProcessPoolExecutor(max_workers=config.workers,
                                       mp_context=multiprocessing.get_context("spawn"))
        return ThreadPoolExecutor(max_workers=config.workers)
    return IntelligentExecutor(
        max_workers=config.workers,
        process_workers=config.workers if config.backend == "process" else 0,
        max_pending=2 * config.workers, policy="hints",
        predictive=config.profile_sample_interval > 0,
        profile_sample_interval=max(1, config.profile_sample_interval),
        objective=ExecutionObjective(mode="throughput", max_concurrency=config.workers)
        if config.profile_sample_interval else None)


def measure_pool(pool, engine, config, name, tasks, feature_mode="declared"):
    """Same bounded submission window for both; latency includes admission wait.

    Per-item completion latency is the containing batch's submission-to-completion
    time; service latency is measured separately within each task.
    """
    chunks = [tasks[i:i + config.batch_size] for i in range(0, len(tasks), config.batch_size)]
    pending = {}
    output = {}
    latencies = []
    trace = []
    next_chunk = 0
    started = time.perf_counter()
    while next_chunk < len(chunks) or pending:
        while next_chunk < len(chunks) and len(pending) < 2 * config.workers:
            index = next_chunk
            submitted = time.perf_counter()
            args = (name, chunks[index])
            if engine == "stdlib":
                future = pool.submit(execute_batch, args)
            else:
                hints = TaskHints(key="real-world-" + name,
                                  kind="cpu" if config.backend == "process" else "io",
                                  process_safe=config.backend == "process")
                if config.profile_sample_interval:
                    future = pool.submit_profiled(execute_batch, args, hints=hints,
                                                  features=(batch_features(name, chunks[index]) if feature_mode == "declared"
                                                            else {"batch_size": len(chunks[index])}))
                else:
                    future = pool.submit_task(execute_batch, args, hints=hints)
            completed = []
            # Timestamp actual completion, not ordered result retrieval.
            future.add_done_callback(lambda _, box=completed: box.append(time.perf_counter()))
            pending[future] = (index, submitted, completed)
            next_chunk += 1
        done, _ = wait(pending, return_when=FIRST_COMPLETED)
        for future in done:
            index, submitted, completed = pending.pop(future)
            try:
                rows = future.result()
            except Exception as exc:
                rows = [{"value": None, "error": type(exc).__name__ + ": " + str(exc),
                         "service_seconds": None} for _ in chunks[index]]
            output[index] = rows
            latency = (completed[0] if completed else time.perf_counter()) - submitted
            latencies.extend([latency] * len(rows))
            trace.append({"completed_tasks": sum(len(v) for v in output.values()),
                          "elapsed_seconds": time.perf_counter() - started})
    elapsed = time.perf_counter() - started
    rows = [row for index in sorted(output) for row in output[index]]
    errors = [row["error"] for row in rows if row["error"] is not None]
    canonical = json.dumps([row["value"] for row in rows], sort_keys=True, separators=(",", ":"))
    service = [row["service_seconds"] for row in rows if row["service_seconds"] is not None]
    return {"seconds": elapsed, "tasks": len(rows), "throughput_per_second": len(rows) / elapsed,
            "batch_completion_latency_p95_seconds": percentile(latencies, .95),
            "task_service_p95_seconds": percentile(service, .95) if service else None,
            "errors": errors, "error_rate": len(errors) / len(rows),
            "result_sha256": hashlib.sha256(canonical.encode()).hexdigest(),
            "completion_trace": trace}


def trial(name, engine, config, count, seed, root, prepared=None, feature_mode="declared"):
    module = module_for(name)
    root.mkdir(parents=True)
    fixture = module.fixture(root) if prepared is None and hasattr(module, "fixture") else nullcontext()
    with fixture:
        if prepared is None:
            setup_started = time.perf_counter()
            cold_tasks = module.prepare(name, root / "cold", count, seed)
            warm_tasks = module.prepare(name, root / "warm", count, seed)
            fixture_seconds = time.perf_counter() - setup_started
            reference_tasks = module.prepare(name, root / "reference", count, seed)
            reference_rows = execute_batch((name, reference_tasks))
            if any(row["error"] for row in reference_rows):
                raise RuntimeError("Sequential fixture verification failed: " + repr(reference_rows))
            reference = hashlib.sha256(json.dumps([row["value"] for row in reference_rows],
                sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        else:
            cold_tasks = warm_tasks = prepared["tasks"]
            reference = prepared["reference_sha256"]
            fixture_seconds = 0.0  # Shared immutable fixture setup reported once per seed.
        before_cpu = cpu_seconds()
        with RSSSampler() as rss:
            start = time.perf_counter()
            pool = make_executor(engine, config)
            construction = time.perf_counter() - start
            try:
                cold = measure_pool(pool, engine, config, name, cold_tasks, feature_mode)
                warm = measure_pool(pool, engine, config, name, warm_tasks, feature_mode)
                profiles = [json.loads(p.to_json()) for p in pool.export_profiles()] if engine != "stdlib" else []
            finally:
                start = time.perf_counter()
                pool.shutdown(wait=True)
                shutdown = time.perf_counter() - start
        after_cpu = cpu_seconds()
        measured_cpu = None if before_cpu is None or after_cpu is None else max(0.0, after_cpu - before_cpu)
        for phase in (cold, warm):
            phase["correct"] = not phase["errors"] and phase["result_sha256"] == reference
        return {"engine": engine, "configuration": asdict(config), "seed": seed,
                "fixture_seconds": fixture_seconds, "executor_construction_seconds": construction,
                "executor_shutdown_seconds": shutdown, "cold": cold, "warm": warm,
                "cold_including_construction_seconds": cold["seconds"] + construction,
                "cold_lifecycle_seconds": cold["seconds"] + construction + shutdown,
                "lifecycle_cpu_seconds": measured_cpu,
                "sampled_tree_peak_rss_bytes": rss.peak, "rss_samples": rss.samples,
                "rss_collection": rss.report(),
                "profiles_after_two_batches": profiles}


def score(row):
    if not all(row[p]["correct"] for p in ("cold", "warm")):
        return float("inf")
    return row.get("cold_lifecycle_seconds", row["cold_including_construction_seconds"]) + row["warm"]["seconds"]


def summarize(calibration, heldout):
    chosen = {}
    for engine in ("stdlib", "intelligent"):
        rows = [row for row in calibration if row["engine"] == engine]
        best = min(rows, key=score)
        if score(best) == float("inf"):
            raise RuntimeError(f"No correct calibration candidate for {engine}")
        chosen[engine] = best["configuration"]
    comparisons = []
    for seed in sorted({row["seed"] for row in heldout}):
        rows = {row["engine"]: row for row in heldout if row["seed"] == seed}
        base, intelligent = rows["stdlib"], rows["intelligent"]
        for phase in ("cold", "warm"):
            base_time = base.get("cold_lifecycle_seconds", base["cold_including_construction_seconds"]) if phase == "cold" else base[phase]["seconds"]
            intelligent_time = intelligent.get("cold_lifecycle_seconds", intelligent["cold_including_construction_seconds"]) if phase == "cold" else intelligent[phase]["seconds"]
            ratio = base_time / intelligent_time
            comparisons.append({"seed": seed, "phase": phase, "T_best_tuned_stdlib_over_T_intelligent": ratio,
                                "within_10_percent": intelligent_time <= 1.1 * base_time,
                                "correct": base[phase]["correct"] and intelligent[phase]["correct"]})
    return {"chosen_from_calibration": chosen, "heldout_comparisons": comparisons,
            "near_optimal_coverage": sum(c["within_10_percent"] and c["correct"] for c in comparisons) / len(comparisons),
            "convergence": {"status": "not_established", "reason": "Two pool passes measure cold/warm behavior, not statistical policy convergence."}}


def acceptance_failures(heldout, *, max_p95_seconds=30.0, max_rss_bytes=1024**3,
                        max_cpu_seconds=300.0):
    failures = []
    if not heldout:
        return ["No held-out measurements"]
    def number(value):
        return type(value) in (int, float) and math.isfinite(value) and value >= 0
    seeds = {row.get("seed") for row in heldout}
    for seed in seeds:
        engines = [row.get("engine") for row in heldout if row.get("seed") == seed]
        if sorted(engines) != ["intelligent", "stdlib"]:
            failures.append(f"seed={seed}: require one held-out row per engine")
    for row in heldout:
        label = f"{row.get('engine')} seed={row.get('seed')}"
        for phase in ("cold", "warm"):
            measured = row.get(phase, {})
            errors = measured.get("error_rate")
            p95 = measured.get("batch_completion_latency_p95_seconds")
            if measured.get("correct") is not True or not number(errors) or errors != 0:
                failures.append(label + " " + phase + ": incorrect result, task errors or invalid error metric")
            if not number(p95) or p95 > max_p95_seconds:
                failures.append(label + " " + phase + ": missing/invalid p95 or budget exceeded")
        rss = row.get("sampled_tree_peak_rss_bytes")
        cpu = row.get("lifecycle_cpu_seconds")
        if not number(rss) or rss > max_rss_bytes:
            failures.append(label + ": missing/invalid sampled RSS or budget exceeded")
        if not number(cpu) or cpu > max_cpu_seconds:
            failures.append(label + ": missing/invalid lifecycle CPU or budget exceeded")
    return failures


def benchmark(name, configs, count, calibration_seed, heldout_seeds, root, calibration_repeats=1,
              scale="smoke", calibration_count=None, predictive_only=False, feature_mode="declared"):
    calibration_count = count if calibration_count is None else calibration_count
    module = module_for(name)
    root.mkdir(parents=True, exist_ok=True)
    datasets = {}
    def get_dataset(seed, size):
        key = (seed, size)
        if key not in datasets:
            setup = time.perf_counter()
            tasks = module.prepare(name, root / f"dataset-{seed}-{size}", size, seed, scale=scale)
            preparation_seconds = time.perf_counter() - setup
            reference_start = time.perf_counter()
            rows = execute_batch((name, tasks))
            if any(row["error"] for row in rows):
                raise RuntimeError("Sequential fixture verification failed: " + repr(rows))
            reference = hashlib.sha256(json.dumps([r["value"] for r in rows], sort_keys=True,
                separators=(",", ":")).encode()).hexdigest()
            datasets[key] = {"tasks": tasks, "reference_sha256": reference,
                             "seed": seed, "count": size, "preparation_seconds": preparation_seconds,
                             "reference_seconds": time.perf_counter() - reference_start}
        return datasets[key]
    fixture = module.fixture(root) if hasattr(module, "fixture") else nullcontext()
    with fixture:
        rng = random.Random(calibration_seed)
        for _ in range(calibration_repeats):
            get_dataset(rng.randrange(2**63), calibration_count)
        report = _benchmark_prepared(name, configs, count, calibration_seed, heldout_seeds, root,
                                     calibration_repeats, calibration_count, get_dataset, predictive_only, feature_mode)
    report["fixture_datasets"] = [{k: v for k, v in dataset.items() if k != "tasks"}
                                  for dataset in datasets.values()]
    report["scale"] = scale
    return report


def _benchmark_prepared(name, configs, count, calibration_seed, heldout_seeds, root,
                        calibration_repeats, calibration_count, get_dataset, predictive_only, feature_mode):
    calibration = []
    tuning_reports = {}
    selected = {}
    engines = ["stdlib", "intelligent"]
    random.Random(calibration_seed).shuffle(engines)
    start = time.perf_counter()
    for engine in engines:
        candidates = [c for c in configs if
                      (c.profile_sample_interval == 0 if engine == "stdlib" else
                       c.profile_sample_interval > 0 if predictive_only else True)]
        def evaluate(config, fixture_seed):
            row = trial(name, engine, config, calibration_count, fixture_seed,
                        root / f"calibration-{len(calibration)}",
                        prepared=get_dataset(fixture_seed, calibration_count), feature_mode=feature_mode)
            calibration.append(row)
            print(f"  calibrated {engine} {len(calibration)}: {config}", flush=True)
            return Measurement(cold_seconds=row["cold_lifecycle_seconds"],
                               warm_seconds=row["warm"]["seconds"],
                               correct=row["cold"]["correct"] and row["warm"]["correct"],
                               error_count=len(row["cold"]["errors"]) + len(row["warm"]["errors"]),
                               peak_rss_bytes=row["sampled_tree_peak_rss_bytes"],
                               cpu_seconds=row["lifecycle_cpu_seconds"])
        report = calibrate(candidates, evaluate, repeats=calibration_repeats, seed=calibration_seed,
                           objective=CalibrationObjective(elapsed="combined"),
                           allow_process=any(c.backend == "process" for c in candidates))
        if report.selected is None:
            raise RuntimeError(f"No eligible {engine} calibration candidate: " + repr(report.to_dict()))
        tuning_reports[engine] = report.to_dict()
        selected[engine] = report.selected
    calibration_seconds = time.perf_counter() - start
    heldout = []
    for seed in heldout_seeds:
        engines = ["stdlib", "intelligent"]
        random.Random(seed).shuffle(engines)
        for engine in engines:
            heldout.append(trial(name, engine, selected[engine], count, seed, root / f"heldout-{seed}-{engine}",
                                 prepared=get_dataset(seed, count), feature_mode=feature_mode))
    summary = summarize(calibration, heldout)
    summary["chosen_from_calibration"] = {engine: asdict(config) for engine, config in selected.items()}
    summary["calibration_convergence"] = {
        engine: {"evaluations_to_stable_near_best": report["evaluations_to_stable_near_best"],
                 "trace": report["trace"],
                 "reference": report["convergence_reference"]}
        for engine, report in tuning_reports.items()}
    return {"name": name, "calibration_seconds": calibration_seconds, "calibration": calibration,
            "configuration_selection": tuning_reports, "heldout": heldout, "summary": summary}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true", help="Small correctness/measurement run; not a deployment claim")
    parser.add_argument("--workloads", nargs="+", choices=NAMES, default=list(NAMES))
    parser.add_argument("--workers", nargs="+", type=int, default=[1, 2])
    parser.add_argument("--batches", nargs="+", type=int, default=[1, 4])
    parser.add_argument("--backends", nargs="+", choices=["thread", "process"], default=["thread", "process"])
    parser.add_argument("--profile-intervals", nargs="+", type=int, default=[0],
                        help="0=fixed admission; positive=opt-in predictive profiling interval")
    parser.add_argument("--feature-mode", choices=["declared", "batch-only"], default="declared")
    parser.add_argument("--predictive-only", action="store_true",
                        help="Compare predictive admission only against equal backend/worker/batch stdlib grid")
    parser.add_argument("--scale", choices=["smoke", "representative"], default="smoke")
    parser.add_argument("--calibration-count", type=int)
    parser.add_argument("--heldout-seeds", nargs="+", type=int, default=[2903, 3911, 4933])
    parser.add_argument("--calibration-repeats", type=int, default=1)
    parser.add_argument("--max-p95-seconds", type=float, default=30.0)
    parser.add_argument("--max-rss-mib", type=float, default=1024.0)
    parser.add_argument("--max-cpu-seconds", type=float, default=300.0)
    parser.add_argument("--count", type=int)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if any(w < 1 or w > 8 for w in args.workers) or any(b < 1 or b > 32 for b in args.batches):
        parser.error("Bounded runner permits 1..8 workers, 1..32 batch size")
    if not 1 <= args.calibration_repeats <= 8:
        parser.error("calibration repeats must be 1..8")
    if any(not 0 < v < float("inf") for v in (args.max_p95_seconds, args.max_rss_mib, args.max_cpu_seconds)):
        parser.error("acceptance budgets must be finite positive numbers")
    count = (4 if args.quick else 16) if args.count is None else args.count
    if not 1 <= count <= 256:
        parser.error("count must be 1..256")
    if args.calibration_count is not None and not 1 <= args.calibration_count <= 256:
        parser.error("calibration count must be 1..256")
    if not 1 <= len(args.heldout_seeds) <= 16 or len(set(args.heldout_seeds)) != len(args.heldout_seeds):
        parser.error("use 1..16 distinct heldout seeds")
    if 0 not in args.profile_intervals or any(v < 0 for v in args.profile_intervals):
        parser.error("profile intervals must contain 0 (fixed baseline) and be nonnegative")
    if args.predictive_only and not any(v > 0 for v in args.profile_intervals):
        parser.error("predictive-only requires a positive profile interval as well as baseline 0")
    configs = [Configuration(b, w, c, p) for b in args.backends for w in args.workers
               for c in args.batches for p in args.profile_intervals]
    report = {"schema_version": 1, "environment": workload_environment(), "count_per_trial": count,
              "scope": "Single host, local generated fixtures, original workload replicas; no upstream interoperability or deployment certification.",
              "method": {"calibration_shuffle_and_fixture_seed_source": 1701, "heldout_seeds": args.heldout_seeds,
                         "scale": args.scale, "calibration_count": args.calibration_count or count,
                         "calibration_repeats": args.calibration_repeats, "predictive_only": args.predictive_only,
                         "predictive_feature_mode": args.feature_mode,
                         "selection": "Minimum mean calibration cold construction+execution+shutdown plus warm execution; same search grid for both engines",
                         "cpu": "POSIX getrusage SELF + waited CHILDREN delta for executor lifecycle, including sampling/fixture threads; unavailable on Windows",
                         "warm": "Second pass in same pool; reused immutable fixture inputs, fresh per-invocation caches/outputs; OS/library caches uncontrolled",
                         "memory": "10 ms sampled absolute aggregate process-tree RSS including fixture server, shared pages may double-count",
                         "intelligent": "Calibration-selected explicit thread/process hints; fixed admission at interval 0, opt-in predictive otherwise. No automatic workload replay.",
                         "tuning_budget": "Same backend/worker/batch grid; additional intelligent profiling modes only when explicitly requested, costs reported",
                         "comparison": "Held-out time of calibration-selected stdlib divided by separately calibration-selected intelligent; no held-out oracle tuning",
                         "configuration_grid": [asdict(c) for c in configs],
                         "acceptance_budgets": {"zero_errors_and_correct_results": True,
                             "p95_seconds": args.max_p95_seconds, "rss_bytes": int(args.max_rss_mib * 1024**2),
                             "lifecycle_cpu_seconds": args.max_cpu_seconds}}, "workloads": [], "failures": []}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="real-workloads-") as directory:
        for name in args.workloads:
            print(f"Measuring {name}", flush=True)
            try:
                row = benchmark(name, configs, count, 1701, args.heldout_seeds, Path(directory) / name,
                                args.calibration_repeats, args.scale, args.calibration_count, args.predictive_only, args.feature_mode)
                row["acceptance_failures"] = acceptance_failures(row["heldout"],
                    max_p95_seconds=args.max_p95_seconds, max_rss_bytes=int(args.max_rss_mib * 1024**2),
                    max_cpu_seconds=args.max_cpu_seconds)
                row["accepted"] = not row["acceptance_failures"]
                report["workloads"].append(row)
                if not row["accepted"]:
                    report["failures"].append({"name": name, "error": "held-out acceptance failed",
                                               "details": row["acceptance_failures"]})
                print(f"  near-optimal coverage: {row['summary']['near_optimal_coverage']:.0%}", flush=True)
            except Exception as exc:
                report["failures"].append({"name": name, "error": type(exc).__name__ + ": " + str(exc)})
                print(f"  FAILED: {exc}", flush=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
    return 1 if report["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
