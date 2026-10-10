# Appendix D: Benchmark methodology

## 1. Begin with a falsifiable question

A benchmark should answer a narrow question, such as “Does a hint-routed hybrid reduce batch completion time for this mixed workload under this worker budget?” It should not begin with “Show that AI is faster.” The sequential loop may win. An ordinary thread pool may win. The adaptive learner may never collect useful evidence before the run ends. Those are valid outcomes.

For the framework's configuration-selection objective, ask how close the selected configuration comes to an independently measured eligible baseline under the same resource envelope, including calibration cost and held-out workload correctness. A same-configuration stdlib comparison measures execution-layer overhead; it cannot by itself establish configuration-selection quality or universal engine speedup.

The included [harness](../../benchmarks/README.md) is a reproducible starting point, not an exhaustive production benchmark. It verifies deterministic output checksums before writing its reports. It compares sequential execution, standard thread and process pools, and the project's thread-only, hint, and adaptive policies on pure-Python CPU loops, simulated I/O sleeps, and a seeded mixture.

## 2. Running the supplied harness

From the repository root after installing the package:

```sh
python -m benchmarks.run --quick --output benchmarks/results/smoke.json
python -m benchmarks.run --output benchmarks/results/local.json
```

For development without installation, prefix the command with `PYTHONPATH=src`. The quick run checks that the harness functions; it supplies too little repetition to support a stable performance conclusion. The raw JSON is the authoritative record and the generated Markdown is a summary.

For a deliberately scoped comparison:

```sh
python -m benchmarks.run \
  --engines sequential thread process intelligent-hints intelligent-adaptive \
  --workloads cpu mixed --tasks 64 --cpu-iterations 400000 \
  --workers 4 --hybrid-thread-workers 2 --hybrid-process-workers 2 \
  --max-pending 8 --warmup-batches 2 --repeats 7 --seed 42 \
  --output benchmarks/results/comparison.json
```

These settings are an experiment specification, not recommended production defaults. Seven repetitions do not establish any universal confidence level; inspect variance and run more independent trials if the effect is small relative to noise.

## 3. Compare resources honestly

A four-thread pool and a hybrid of four threads plus four processes have different resource budgets. The harness defaults instead compare four stdlib workers with two thread and two process workers for the hybrid. This equalizes the configured combined count but does not make all resource use equivalent: the hybrid has only two workers on each route, and processes have separate memory and startup costs.

Use several comparisons to answer different questions:

- **Equal combined worker count:** useful for a broad deployment budget comparison.
- **Equal backend worker count:** useful for isolating instrumentation overhead against the same thread or process capacity.
- **Equal measured resource envelope:** useful when memory or CPU quotas matter more than worker count.
- **Best fixed configuration found on a separate tuning set:** useful when evaluating whether learning is worth its complexity.

Report the selected comparison explicitly. Do not tune the adaptive method on the evaluation workload while leaving its baselines intentionally poorly configured.

## 4. Timing boundaries

The executor's internal profile excludes admission wait. The benchmark's task-latency measurement begins before the attempt to submit and therefore includes it. These numbers answer different questions and should not be plotted with the same label.

| Metric | Start | Stop | Interpretation |
| --- | --- | --- | --- |
| Submit-call duration | Before submission attempt | Submission returns | Producer blocking plus submission overhead |
| Benchmark task latency | Before submission attempt | Parent completion callback | Admission, queueing, useful work, transport, and observation delay |
| Executor profile latency | Owned work-item creation after admission/policy | Public-Future accounting callback | Owned queueing, dispatch/startup, execution and transfer; excludes admission and policy |
| Uncontended profile latency | Same start, only when chosen backend has no outstanding work | Same accounting callback | Includes startup/serialization; not intrinsic service time |
| Scheduling load | Locked scheduler snapshot | Point-in-time sample | Queued items and occupied dispatch slots; not CPU utilisation |
| Worker-local service duration | Inside benchmark worker function | Before that function returns | Local elapsed work interval; includes descheduling, not just CPU cycles |
| Batch duration | First batch action | Required result collection completed | Useful workload completion time for that phase |

No subtraction of timestamps from different processes is required for the service-duration metric. Each process measures its own elapsed interval. Returned service durations and parent-observed latency still cannot perfectly decompose all serialization or scheduling costs; subtraction is only an approximate residual, not an independently observed queue timer.

## 5. Cold and warm phases

A cold sample uses a fresh executor and includes construction, lazy startup, the batch, and orderly shutdown. A warm sample uses a separate fresh executor, performs untimed warmup batches, and measures a later batch. Setup, warmup, and shutdown measurements remain available separately.

Warmup is not magical. The adaptive policy may not have used a process route during warmup, so an apparently warm measured batch can still start that backend for the first time. Profile inspection and route counts are necessary to explain such a result. Do not remove unexpected startup cost from the report after seeing which implementation paid it.

Compare at least two practical deployment shapes: short-lived command-line jobs that pay startup every time, and long-lived executors with repeated comparable work. Reusing the same executor for all experimental policies would contaminate the comparison; a policy's learned state belongs to its own trial.

## 6. Correctness precedes performance

The supplied workloads use deterministic result checksums. A missing, duplicated, failed, or corrupted result invalidates the trial even if its wall time looks excellent. For an application benchmark, replace a generic digest with domain checks where necessary: approximate floating-point values may need tolerances, and external effects may need an isolated local fixture.

Do not benchmark writes against a live service merely to obtain timing samples. Use synthetic or explicitly authorized data and a test destination. Adaptive exploration already avoids duplicate execution; the benchmark must preserve that property rather than running the same real-world side effect through every baseline.

Fault tests should distinguish a correct failed operation from a scheduler defect. Exceptions expected from a fault scenario are an outcome to measure; unexpected errors should stop the run or clearly invalidate that configuration rather than silently disappearing from the throughput denominator.

## 7. Variance and presentation

The harness shuffles engine, phase, and workload order with a recorded seed and executes experiments serially. This reduces a predictable ordering effect but does not remove noisy neighbors, thermal behavior, background tasks, power policy, or container throttling.

Publish raw trials alongside summaries. A median communicates a typical observed trial; p95 communicates a tail location under the chosen interpolation convention. Neither is a confidence interval. Pooled per-task p95 and p95 over independent batch repetitions have different populations and cannot be substituted for each other.

For a publishable result, also consider paired comparisons by workload seed, uncertainty estimates over independent runs, and sensitivity sweeps. If the difference is smaller than ordinary run-to-run variation, describe it as inconclusive. Never convert one lucky trial into a universal percentage improvement.

## 8. Essential additional experiments

The built-in harness deliberately omits several important dimensions. Before relying on a policy in production, extend the experiment for the actual risk:

| Question | Controlled experiment | Required observations |
| --- | --- | --- |
| Is admission effective? | Saturate blocked workers, vary producer count | Logical pending peak, producer wait, rejection count |
| Are physical item bounds preserved? | Repeat submit/cancel behind blocked workers while deliberately retaining cancelled public Futures | Payload weak-reference release; owned queued + dispatched peak <= max_pending; each backend dispatched <= workers |
| Is delayed cancellation accounted for? | Hold backend acknowledgement after a dispatched thread cancellation | Logical capacity releases; physical capacity remains occupied until acknowledgement |
| What memory remains? | Vary payload size, producer count and retained results/Futures | Traced allocation/RSS scope; distinguish caller-retained objects and native/transport memory from item bounds |
| Is streaming useful? | Large generator, slow consumer, early exit | Input read-ahead, buffered results, cancellation outcomes |
| Does payload transfer dominate? | Sweep argument and return sizes | Total latency, startup, memory, same-result verification |
| Does learning survive drift? | Change workload costs midstream | Route history, cumulative excess latency, recovery delay |
| Are keys comparable? | Mix several input sizes under one key, then separate them | Route changes and per-size latency |
| Is native parallelism oversubscribed? | Vary library threads and executor workers independently | Runnable threads, effective CPU use, latency, memory |
| Is failure handling correct? | Script task exceptions and worker termination | Future terminal states, pending reclamation, no silent retries |

The memory and cancellation experiments need a subprocess watchdog and explicit waiting shutdown. Do not create an intentionally hung pool in the test runner that must later finish unrelated tests. The updated `benchmarks/stress/churn_limitation.py` checks physical counts and queued payload release while retaining cancelled public Futures. Its name reflects the historical regression; old churn JSON records the earlier direct-submission limitation. A new source-matched result must be recorded before claiming a completed regression run, and even a passing run is not a hard byte/RSS guarantee.

Load-aware experiments should include sparse demand, bursty queues, differing backend capacities and changing workload costs. Record routing/exploration decisions and load snapshots, while measuring actual CPU separately if relevant. Uncontended latency still includes startup/serialization, and a queue-pressure score is a heuristic. Predictive global admission may idle an available backend: measure that behavior rather than assuming work-conserving scheduling.

## 9. Reproducibility record

Retain the command, raw JSON, workload version, git revision when available, Python version and build, OS, process start method, pool sizes, task count and sizes, seed, admission limit, warmup policy, and repetition count. The harness records CPU and resource metadata where available; missing cgroup-v1, nested quota, or platform-specific limits remain possible.

A future performance claim must state what was actually executed, what was skipped, and which limits were not observed. A Linux run is not evidence that Windows or macOS behavior was tested. A successful smoke run is not evidence of throughput superiority. The value of this harness is that it makes those distinctions reproducible.

## 10. Reading the historical checked-in reference run

The repository includes a [reference report](../../benchmarks/results/reference.md) and its [raw observations](../../benchmarks/results/reference.json.gz). This run used Python 3.12.14 on Linux, `spawn`, five repetitions, 48 tasks per batch, two warmup batches, and a pending limit of eight. Standard pools had four workers; the hybrid had two threads and two processes.

| Warm workload | Adaptive median batch | Relevant fixed baseline median batch |
| --- | ---: | ---: |
| Pure Python CPU | 548.489 ms | Process: 256.123 ms |
| Simulated I/O | 164.110 ms | Thread: 121.979 ms |
| Mixed | 343.583 ms | Process: 195.875 ms |

On these measurements, the adaptive policy did **not** beat the listed fixed baselines. Do not reinterpret that as either universal failure or universal success: the resource split, short learning horizon, small payloads, and synthetic workload are all part of the experiment.

The adaptive harness intentionally receives `kind="unknown"` with stable CPU/I/O-family keys and explicit process eligibility, so it is allowed to explore both routes for these synthetic tasks. The hints policy receives the true CPU/I/O kinds. Consequently, adaptive results on the simulated-I/O dataset do not contradict the rule that an explicitly `kind="io"` adaptive submission remains on threads: those benchmark submissions were deliberately labeled unknown. This difference in supplied information is essential when interpreting the comparison.

This report predates the owned FIFO/load-aware contract and is retained as historical evidence for its measured implementation and environment. It is not final-source validation of those changes. It is not a performance acceptance threshold for other machines, and all numeric claims should remain linked to its raw data.
