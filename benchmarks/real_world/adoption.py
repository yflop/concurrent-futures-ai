"""Serial evidence-gated adoption on eleven explicitly repeatable local fixtures.

This is a new experiment, not a reinterpretation of the historical 0.4 results.
Each workload uses the same small declared configuration grid, fresh fixture
paths and frozen held-out seeds. Run with no other CPU-heavy work on the host.

python -m benchmarks.real_world.adoption --output benchmarks/results/adoption-v050.json
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager, nullcontext
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import tempfile
import time

from intelligent_futures.evaluation import (EvaluationConfiguration, EvaluationPlan,
    RepeatableWorkload, evaluate_workload)
from intelligent_futures.evaluation_workloads import EvaluationFixture, callable_workload
from . import NAMES, module_for
from .resource_sampling import RSSSampler
from .run import cpu_seconds, workload_environment


# Keep these top-level for the spawn transport. Every original adapter already
# validates task-specific invariants; equality to an independent sequential run
# provides an additional ordered whole-fixture oracle.
def run_item(payload):
    name, task = payload
    return module_for(name).run_task(task)


def _digest(values):
    return hashlib.sha256(json.dumps(values, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def evaluate_family(name, *, count, scale, plan):
    module = module_for(name)
    reference_digests = {}
    details = []
    resources = {}
    @contextmanager
    def fixtures(seed):
        with tempfile.TemporaryDirectory(prefix="intelligent-adoption-") as directory:
            root = Path(directory)
            with module.fixture(root) if hasattr(module, "fixture") else nullcontext():
                tasks = module.prepare(name, root / "measured", count, seed, scale=scale)
                if seed not in reference_digests:
                    reference = module.prepare(name, root / "reference", count, seed, scale=scale)
                    reference_digests[seed] = _digest(tuple(run_item((name, task)) for task in reference))
                before_cpu = cpu_seconds()
                with RSSSampler() as rss:
                    yield EvaluationFixture(tuple((name, task) for task in tasks),
                        lambda values: _digest(values) == reference_digests[seed])
                after_cpu = cpu_seconds()
                resources.clear()
                resources.update(cpu_seconds=None if before_cpu is None or after_cpu is None else max(0., after_cpu-before_cpu),
                    peak_rss_bytes=rss.peak, rss_collection=rss.report())

    adapter = callable_workload(run_item, fixtures, name=name,
        version=f"local-replicas-v050:{scale}:count={count}:fresh-paths:batch-kernel-v1",
        repeatable=True, independent=True, process_safe=True, max_tasks=count,
        max_pending=plan.max_pending)
    def measure(engine, config, seed):
        resources.clear()
        measurement = adapter.measure(engine, config, seed)
        details.append({"engine": engine, "configuration": vars(config), "seed": seed,
                        "rss_collection": resources.get("rss_collection")})
        return replace(measurement, cpu_seconds=resources.get("cpu_seconds"),
                       peak_rss_bytes=resources.get("peak_rss_bytes"))
    measure._evaluation_max_pending = plan.max_pending
    measure._evaluation_process_safe = True
    workload = RepeatableWorkload(adapter.name, adapter.version, measure, True, True,
        "cold_pool_lifecycle;resources_pool_collection_validation;fresh_fixtures;os_cache_uncontrolled")
    # Same worker/batch budget options for both engines; one extra explicitly
    # identified adaptive split candidate. This is a small grid, not a global
    # optimum or a comparison to expert hybrid pipelines.
    standard = tuple(EvaluationConfiguration(backend, workers, 1)
                     for backend in ("thread", "process") for workers in (1, 2))
    intelligent = standard + (EvaluationConfiguration("adaptive", 2, 1, process_workers=1),)
    report = evaluate_workload(workload, stdlib_configurations=standard,
        intelligent_configurations=intelligent, fixed_baseline=EvaluationConfiguration("thread", 2),
        plan=plan, allow_process=True)
    return {"name": name, "scale": scale, "logical_items_per_fixture": count,
            "measured_logical_items_attempted_upper_bound": count * report.attempted_evaluations,
            "sequential_oracle_fixtures": len(reference_digests),
            "sequential_oracle_logical_items": count * len(reference_digests),
            "report": report.to_json(), "resource_collection": details}


def run(args):
    environment = workload_environment()
    # Published provenance keeps versions/hardware/source hashes, not host paths.
    for key in ("python_executable", "executable"):
        if key in environment:
            environment[key] = "[local interpreter]"
    environment["command"] = "python -m benchmarks.real_world.adoption " + " ".join((
        "--names", *args.names, "--scale", args.scale, "--count", str(args.count),
        "--training-blocks", str(args.training_blocks), "--heldout-blocks", str(args.heldout_blocks),
        "--seed", str(args.seed), "--output", "[report path]"))
    result = {"schema_version": 1, "purpose": "evidence-gated adoption; all outcomes retained",
        "environment": environment, "predeclared_plan": {
            "names": args.names, "scale": args.scale, "ordinary_count": args.count,
            "media_and_graph_count": min(args.count, 2), "batch_size": 1,
            "stdlib_worker_grid": [1,2], "stdlib_backends": ["thread","process"],
            "intelligent_extra_mode": "adaptive, total 2 slots, split 1 thread + 1 process",
            "primary_objective": "cold pool lifecycle elapsed seconds",
            "family_alpha_scope": "two baselines within each one fixed workload report; no family claim across workloads",
            "cache_scope": "fresh fixture paths and application state; OS/page caches uncontrolled",
            "resource_scope": "pool lifecycle plus input collection and ordered validation; fixture prep/oracle excluded",
            "limits": "item windows enforced; sampled RSS/CPU and observed elapsed are rejection caps, not preemption"},
        "workloads": [], "status": "running"}
    output = Path(args.output)
    output.parent.mkdir(parents=True,exist_ok=True)
    def save():
        temporary=output.with_suffix(output.suffix+'.tmp')
        temporary.write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
        temporary.replace(output)
    save()
    start=time.perf_counter()
    for index,name in enumerate(args.names):
        plan=EvaluationPlan(training_blocks=args.training_blocks,heldout_blocks=args.heldout_blocks,
            seed=args.seed+index,max_workers=2,max_pending=4,max_evaluations=512,
            expected_reuses=10000,max_cpu_seconds=120,max_peak_rss_bytes=1024**3,
            max_observed_seconds=60)
        count=min(args.count,2) if name in ('images','ocr','video','geospatial') else args.count
        row=evaluate_family(name,count=count,scale=args.scale,plan=plan)
        result['workloads'].append(row)
        print(name,row['report']['status'],row['report']['reasons'],flush=True)
        save()
    result['status']='complete'
    result['elapsed_seconds']=time.perf_counter()-start
    result['outcomes']={status:sum(w['report']['status']==status for w in result['workloads'])
        for status in ('recommend_intelligent','retain_stdlib','inconclusive')}
    save()
    print(json.dumps(result['outcomes']),flush=True)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--names',nargs='+',choices=NAMES,default=list(NAMES))
    parser.add_argument('--scale',choices=('smoke','representative'),default='representative')
    parser.add_argument('--count',type=int,default=4)
    parser.add_argument('--training-blocks',type=int,default=3)
    parser.add_argument('--heldout-blocks',type=int,default=24)
    parser.add_argument('--seed',type=int,default=505010)
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    if not 1<=args.count<=64:parser.error('--count must be in [1,64]')
    run(args)


if __name__=='__main__':main()
