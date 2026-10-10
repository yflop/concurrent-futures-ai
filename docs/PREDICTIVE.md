# Predictive execution foundation (0.3)

This opt-in experiment moves toward **callable + input summaries + hardware +
objective → an efficient, constrained execution strategy**. It is not a learned
cross-machine performance model or a promise of speedup. `predictive=False`
disables predictive admission and service-time profiling; the core executor's
owned FIFO, physical bounds and load-aware adaptive routing still apply. The wider
framework evaluates and selects configurations; it does not promise a universal
same-configuration execution-engine improvement.

## Start safely

```python
from intelligent_futures import (
    IntelligentExecutor, TaskHints, ExecutionObjective, WorkloadProfile,
)

with IntelligentExecutor(
    max_workers=4, predictive=True,
    objective=ExecutionObjective(mode="balanced", max_concurrency=4),
) as executor:
    future = executor.submit_profiled(
        sum, [1, 2, 3], hints=TaskHints(key="sum-v1", kind="cpu"),
        features={"item_count": 3},
    )
    assert future.result() == 6
    # Snapshots are immutable. Saving/loading files is deliberately your choice.
    exported = [profile.to_json() for profile in executor.export_profiles()]

profiles = tuple(WorkloadProfile.from_json(text) for text in exported)
with IntelligentExecutor(predictive=True, workload_profiles=profiles) as executor:
    pass
```

Ordinary `submit` preserves all callable keywords, including `features` and
`hints`. `submit_task` reserves only `hints`. `submit_profiled` reserves `features`
and `hints`; numeric features are explicit caller-approved summaries, never
extracted from inputs. Use stable versioned keys for comparable operations.
Features must match exactly; continuous features need caller-designed buckets.
No raw input, source code, result, traceback, or exception message is exported.
Keys, feature names, feature values, timings, and hardware fingerprints are still
potentially identifying metadata: do not put secrets in them; review before sharing.

## What is actually measured

With `profile_sample_interval=1` (default), successful thread invocations collect callable wall time and current-thread
CPU time (when available). Callable timing excludes pool queueing and admission;
CPU time excludes other threads and subprocesses, including native helper threads.
It does not prove that work holds/releases the GIL.
Set `profile_sample_interval=8`, for example, to measure the first and every
eighth invocation in each key/feature bucket, reducing overhead for tiny tasks.
Sampling is deterministic, not an unbiased statistical sampling guarantee;
periodic workloads can alias the interval. Counters remain bounded. Failures and cancellation do
not train successful-runtime profiles. Existing snapshot latency profiles still
measure accepted-submission-to-completion, including owned queueing, startup,
dispatch and serialization. Their `uncontended_*` subset records work accepted
when the chosen backend had no outstanding work; it still includes startup and
serialization and is distinct from predictive callable service-time samples.

Process runtime and per-task RSS are **unavailable** in the predictive executor.
A process result is not silently wrapped or replayed. `RuntimeSample` lets an
application import its own genuine measurements, with their provenance remaining
its responsibility. `memory_limit_bytes` is rejected, rather than suggesting a
memory guarantee this runtime cannot enforce.

A `WorkloadProfile.scaling_curve()` groups observed service times by backend and
global active thread count (across workload keys) at invocation start. These are observational buckets, not
controlled scaling experiments: concurrency can change during a task, and input
variation/background load can confound them. `estimated_throughput` is explicitly
concurrency / mean service time, a proxy, not measured batch throughput.

## Hardware, predictions, and constraints

`HardwareCapabilities.detect()` reports logical CPU count, affinity count where
available, and accessible Linux cgroup v1/v2 CPU quotas (including visible
ancestors). Usable CPU count conservatively takes their minimum, floors fractional
quotas, and keeps at least one worker. Fractional quota is retained in the report.
Missing constraints stay unknown. The compact fingerprint includes architecture, CPU model when available,
OS and observed CPU constraints; matching it does not prove identical processors,
clock speed, RAM, container isolation, or runtime load.

A `Predictor` uses exact workload key/features/fingerprint matches. A foreign-hardware profile with source hardware provenance and at least two
thread CPU/wall measurements supplies a conservative `source="transfer-prior"`,
`transfer="unvalidated"` cold-start budget. Source observed concurrency is scaled
down with target/source usable CPU capacity, never increased just because the
target is larger; waiting evidence additionally caps it by a bounded CPU/wall
heuristic. Objective and executor limits still apply. No foreign latency or
speedup is predicted. Missing provenance/evidence falls back to hardware/hint
heuristics with `transfer="unknown"`. This is a transparent transfer prior, not
trained transfer-learning. Local measured CPU/wall evidence supersedes it. With one concurrency bucket, balanced/throughput modes use
CPU/wall ratios to guide a heuristic budget, without freezing later bursts at one
worker. This has `source="measurement-guided"` when CPU measurements are available.
With multiple eligible buckets it ranks observed choices (`source="empirical"`):
without live load, latency selects shortest mean time and throughput uses the
labeled proxy. Balanced excludes choices more than 25% slower than the fastest
bucket. Latency mode can use a single observed bucket. `Prediction.confidence` is a qualitative label: heuristic, observational, or
unvalidated; none is a calibrated probability. Scaling buckets expose
`provenance="observational-concurrency-bucket"`. Samples are bounded and aging is FIFO;
there is no causal inference, explicit experiment design, or drift guarantee.

The executor supplies an immutable `SchedulingLoad` to
`Predictor.recommend(..., load=...)`. It records per-backend workers, queued items
and occupied dispatch slots, not CPU utilisation. With load, latency ranking
scales observed service time by queue pressure, while throughput/balanced ranking
caps useful concurrency by capacity and current demand. A bounded per-workload
switch margin (10% by default) suppresses small ranking changes. The recommended
budget is also capped by selected-backend outstanding work plus one, with the
objective's lower bound applied. Later arrivals can grow it again.

`Prediction.load_adjusted` identifies this path and `stabilized` reports when the
switch margin retained an incumbent. `expected_wall_seconds`, when present,
remains an observed service-time mean, not a queueing or batch-duration forecast.
If demand adjustment changes an empirical concurrency, the source becomes
`measurement-guided` and that unmeasured expected time is cleared. These controls
are heuristic; queue pressure is never stored as a service-time measurement.

Admission caps **all accepted unfinished tasks globally** at the prediction,
objective, selected routing backend's worker capacity, and `max_pending`.
Separately, owned queued plus dispatched items cannot exceed `max_pending`, and
each backend's dispatches cannot exceed its worker budget. Capacity changes affect
future admission, never mutate live stdlib pool internals or kill running tasks.
A smaller limit can temporarily be below already accepted work; new submissions
wait for it to drain. Because the pending limit is global, load on one backend can
prevent admission to an otherwise idle backend. This is not a work-conserving
scheduling guarantee. `submit_timeout` still limits admission only.
`min_concurrency` must fit every enabled backend and pending budget; it is a
search lower bound, not a promise that tasks will run simultaneously.

With `predictive=True`, default `policy="adaptive"` selects predictive routing.
Explicit `policy="threads"`, `"hints"`, or a custom policy retains routing control;
its selected route is passed as `Predictor.recommend(..., backend=...)` so observed
backend ranking, live demand and capacity refer to that actual route. Predictions
then constrain admission without changing the explicit route. The public
`backend` option accepts `None`, `"thread"`, or `"process"`; it never supplies
process consent or enables a disabled backend. Processes still require BOTH
`process_workers > 0` and `TaskHints(process_safe=True)`, regardless of imported
profiles. No pickling failure, crash, or exception triggers a retry or fallback.

## Actual adaptive batching

```python
def square_batch(items):
    return [value * value for value in items]

if __name__ == "__main__":
    with IntelligentExecutor(max_workers=4) as executor:
        batches = executor.map_batches(
            square_batch, range(1000), buffersize=2,
            min_batch_size=1, max_batch_size=64, target_batch_seconds=0.02,
        )
        squares = [value for batch in batches for value in batch]
```

`map_batches` calls `batch_fn(tuple_of_inputs)` once per chunk, with one Future and
one dispatch per chunk, and yields one result per chunk in order. The application
owns the batch function and result shape. Batch sizes adapt to measured
admission-to-completion-callback time, including admission wait, queueing,
serialization and preceding completion callbacks, but excluding
consumer pauses. They grow at most 2× or shrink by at most approximately 2× per
feedback step, within explicit bounds. Initial chunks use the minimum size;
short chunks at end-of-input may be smaller. No hidden warmup executes work twice.

`buffersize` bounds retained batches, each with at most `max_batch_size` inputs.
A failure raises for that whole batch; preceding side effects are not rolled back,
unstarted queued batches are cancelled best-effort, and running batches finish.
There is no partial retry or per-item cancellation inside a running batch. A batch
function must explicitly support tuple inputs and process pickling if selected.
Closing/abandoning an iterator should be explicit (`iterator.close()`); do not rely
on garbage-collection timing. Inputs are consumed lazily. A source error can
consume part of a not-yet-submitted chunk without invoking it.

`map_adaptive` is a separate adaptive **submission-window** map: every input still
gets its own call/Future. It does not amortize dispatch like `map_batches`.
`map_bounded` keeps its original fixed-size buffering behavior.

## Bounds and limitations

- At most 256 predictive profiles by default, controlled by `max_profile_keys`
  (predictive mode accepts 1–256); at most 128 samples per profile, 32 numeric
  features, and 262,144 UTF-8 bytes per schema-v1 JSON profile.
- Unknown schema versions/fields, duplicate object keys, nonfinite numbers,
  malformed samples, excessive sizes, and unsupported values are rejected.
  JSON is data only; there is no pickle import or code loading.
- The core executor owns standard Futures and removable FIFO queues. Owned
  queued cancellation removes payloads; successful dispatched thread cancellation
  retains a physical slot until backend acknowledgement. Process Futures commit
  to `RUNNING` at dispatch and cannot then be cancelled. Older cancellation-churn
  artifacts describe the earlier direct-submission implementation.
- Logical and physical item-count bounds do not impose byte/RSS limits. Callable
  inputs/results, blocked producers, transport buffers and native allocations can
  consume arbitrary memory. There is no task preemption or callback isolation.
- Normal completion callbacks run serially on one owned completion pump. Never
  wait for another Future from this executor inside a callback, even with free
  worker slots; result publication may need that same pump. Immediate callbacks
  added after completion and external cancellation callbacks can run on the caller.
  Future API compatibility does not imply identical stdlib callback scheduling.
- Use a context manager or explicit `shutdown(wait=True)` to guarantee accepted
  work is drained before interpreter exit. Omitted shutdown can leave deferred
  submissions to settle with `RuntimeError` during backend teardown; implicit
  stdlib exit behavior is not an equivalent lifecycle contract.
- Nested blocking submission into a saturated bounded executor can deadlock,
  as can tasks that require more peers than the current admission limit. Use a
  suitable explicit fixed policy/budget for barrier-style workloads.
- Measurement overhead and lower admission can hurt tiny-task throughput. Raw
  seeded trials are provided; this release claims capability, not a speedup.
- The task framework and MCP adapter keep their existing explicit thread-only
  execution model. Their metrics include the executor snapshot, but do not
  silently enable predictive execution or export profiles to clients. Agent-side
  counterfactual, transfer-learning, and remote prediction remain future work.
