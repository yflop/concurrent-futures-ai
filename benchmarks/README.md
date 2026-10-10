# Benchmarks

These benchmarks measure a prototype on a particular machine. They do not assert
that adaptive dispatch is faster. A small workload, spawn startup, instrumentation,
serialization, contention, and a restrictive container quota can make it slower.
The JSON is the authoritative record; Markdown is a generated summary.

See [recorded results and interpretation](results/README.md) for the included
five-repeat reference run, including cases where the adaptive policy lost.

## Run

Install the package, then run from the repository root:

```sh
python -m benchmarks.run --quick --output benchmarks/results/smoke.json
python -m benchmarks.run --output benchmarks/results/local.json
```

For development without installing, use `PYTHONPATH=src python -m benchmarks.run`.
All process pools explicitly use `multiprocessing.get_context("spawn")`. Worker
functions and workload records are importable from `benchmarks.workloads`, and
processes are created only under the CLI's main guard.

Control size, pool sizes, admission, and reproducibility:

```sh
python -m benchmarks.run --tasks 64 --cpu-iterations 400000 --io-delay 0.02 \
  --workers 4 --hybrid-thread-workers 2 --hybrid-process-workers 2 \
  --max-pending 8 --warmup-batches 2 --repeats 7 --seed 42 \
  --output benchmarks/results/my-machine.json

python -m benchmarks.run --engines sequential thread process intelligent-hints \
  --workloads cpu mixed --tasks 16 --repeats 3
```

Default full runs use 48 tasks/batch, 200,000 pure-Python loop iterations per CPU
task, 10 ms sleep per simulated I/O task, five repeats, and two untimed warmup
batches for each warm sample. `--quick` reduces size and repeats for smoke checks;
it is not evidence of stable performance. Independent engine/phase/workload
orders are shuffled with the recorded seed on every repeat. Repeats run serially
so benchmark engines do not deliberately compete with each other.

## What is compared

- Direct sequential calls
- `ThreadPoolExecutor`
- `ProcessPoolExecutor`
- `IntelligentExecutor(policy="threads")`, an instrumentation/admission control
- `IntelligentExecutor(policy="hints")`, with truthful CPU/I/O task kinds
- `IntelligentExecutor(policy="adaptive")`, with unknown kinds, separate stable
  CPU/I/O profile keys, and explicit process consent for these safe synthetic tasks

The default stdlib pools each have four workers. The hybrid has two thread workers
plus two process workers: the same maximum combined worker count, but only two
workers in each route. CPU-only and I/O-only work cannot use the whole hybrid
budget. The intelligent-threads control uses the configured hybrid thread count.
For a same-thread-count instrumentation comparison, pass
`--hybrid-thread-workers 4`; this also increases the hybrid's combined capacity
unless `--hybrid-process-workers` is reduced. Pool sizes are always recorded.
No hidden worker-budget normalization or automatic speedup claim is performed.

## What timing means

Each cold sample uses a fresh executor. Its reported timed duration includes
construction, lazy worker startup, the batch, and orderly shutdown. Each warm
sample uses a separate fresh executor, executes untimed warmup batches, and then
measures only the next batch. Setup, warmups, and shutdown are also retained as
separate measurements. Adaptive exploration can activate a previously unused
process pool after warmup; “warm” does not guarantee that every route is prestarted.

Task latency starts immediately before attempting submission and ends when a
parent-process completion callback runs. It includes admission blocking, queueing,
execution, transport, and callback scheduling. The sequential equivalent is the
direct function-call duration. For small tasks, registering a callback after the
Future already completes adds some observation overhead. Worker-local service
duration is returned separately; it includes descheduling, not only CPU execution.
No cross-process clocks are subtracted.

`IntelligentExecutor.max_pending` bounds accepted unfinished work and can block
submission. Stdlib pools use their default unbounded queues. Comparing submit-call
times alone would favor an executor that merely accumulates backlog. The report
therefore records submission-call durations, end-to-end task latency, full batch
time, throughput, and observed route counters. Latency and queue shapes are not
identical under these different admission semantics. The benchmark retains every
Future to verify outputs, so it does not measure producer memory savings.

Each output result must match the deterministic checksum for its workload. CPU is
an integer loop written in Python; simulated I/O is `time.sleep`, not a real disk
or network service. Mixed batches are fixed-seed shuffled half-CPU/half-I/O (one
extra CPU task for an odd batch size). Payloads are deliberately small. Large
serialization costs, native GIL-releasing libraries, cancellation, memory, tail
fairness, real service limits, and production workload distributions need separate
experiments.

Median/p95 batch times summarize independent repeat samples. Median/p95 task times
pool individual tasks across those repeats and describe a different population.
p95 is linear interpolation at `(n - 1) * .95`; it is not a confidence interval.
The raw JSON includes Python/platform/CPU metadata, affinity and cgroup-v2 quota
when available, spawn method, build GIL mode, pool sizes, command, seed, raw
samples, checksums, and final executor profiles. Linux cgroup-v1, nested quota
hierarchies, and other resource limits may not be represented by that metadata.
The harness writes reports only after all checksums pass; an error is a failed
run rather than a partial benchmark success.

## Harness correctness checks

```sh
PYTHONPATH=src python -m unittest benchmarks.test_run -v
python -m compileall -q benchmarks examples
```

These checks validate argument handling, deterministic workloads, percentile
calculations, documented arithmetic, output checksums, and warmup-excluding
route counts. They deliberately do not assert speedup or a timing threshold.
