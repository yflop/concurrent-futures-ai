# API and operational contract

## Construction

`IntelligentExecutor(max_workers=4, *, process_workers=0, max_pending=64,
policy="adaptive", submit_timeout=None, ewma_alpha=0.25, max_profile_keys=256,
mp_context=None, predictive=False, hardware=None, objective=None,
workload_profiles=(), profile_sample_interval=1)`

Worker counts are fixed for each executor. A process count of zero disables that
backend. A provided multiprocessing context is passed through; the default is
`multiprocessing.get_context("spawn")`. The caller owns initialization, import
safety, environment, and secrets inherited by child processes.

`max_pending` caps both accepted unfinished tasks and physical scheduling items
(owned queued plus dispatched) across both backends. These are two checks against
one configured bound, not additive capacities. Each backend also permits at most
its worker count of outstanding dispatches. Calls block until both admission
checks allow work; `submit_timeout=0` provides fail-fast rejection;
positive values bound the admission wait; `None` waits indefinitely. A failed
admission raises `BackpressureError` (a `TimeoutError`). Time spent waiting for
admission is not a task metric. Concurrent producers race fairly only according
to Python condition scheduling: strict FIFO fairness is not promised.

## Submission and futures

`submit(fn, /, *args, **kwargs)` passes every keyword through to the callable.
It supplies default `TaskHints()` and never consents to process execution.
`submit_task(fn, /, *args, hints=None, **kwargs)` reserves `hints` for scheduling.
Invalid hints and policy routes fail synchronously. Process serialization and
execution errors normally arrive asynchronously through `future.result()`.

`TaskHints(kind="unknown", key="default", process_safe=False)` is immutable.
Kinds are `unknown`, `cpu`, and `io`; keys are nonempty strings of at most 256
characters. The key is a comparison group, not a task ID. Never include secrets.
`process_safe` must be a bool, and asserts both pickle compatibility and semantic
safety. A valid pickle is not proof of safe semantics.

Returned objects are genuine executor-owned `concurrent.futures.Future` instances,
not the backend's Future identities. They support standard result/exception
retrieval, callbacks, `wait`, and `as_completed`. Accepted work waits in removable
per-backend FIFO queues when dispatch slots are occupied; FIFO dispatch does not
promise producer fairness or completion order.

Successful queued cancellation prevents callable execution and releases the
executor-owned payload. A dispatched thread Future remains cancellable until its
worker commits to execution; successful cancellation keeps its physical slot
occupied until the backend acknowledges completion. A process Future commits to
`RUNNING` at dispatch, so `cancel()` returns false even if the child callable has
not visibly started. Running tasks cannot be preempted.

If an immediately attempted backend submission fails, `submit` raises and no task
is counted as accepted. If dispatch fails after queue acceptance, the error is
stored on the returned Future and counted as a failed task. No automatic fallback,
replay, or duplicate retry occurs.

## Completion callback scheduling

Normal completion callbacks run serially on one executor-owned completion pump,
outside the scheduler lock. **Do not wait for another Future from this executor
inside a callback, even if worker slots are free.** Publishing that other result
can require the same blocked pump. Slow callbacks also delay acknowledgement and
further dispatch. Notify an external coordinator instead of waiting for dependent
work or submitting into full admission from a callback.

A callback added after its Future is already done runs immediately on the adding
caller; external cancellation can also run callbacks on the cancelling caller.
A waiting shutdown from the executor's task/pump context raises `RuntimeError`.
There is no general callback/dependency-deadlock isolation. Standard Future method
compatibility does not imply identical stdlib callback threads, concurrent callback
execution, or implicit interpreter-exit semantics.

## Mapping

Inherited `Executor.map` follows the host Python version. Before Python 3.14 it
consumes input eagerly; admission bounds unfinished work but not all retained
results. This package does not reimplement `chunksize` or host-version features.

`map_bounded(fn, iterable, *, buffersize=16, timeout=None, hints=None)` returns an
ordered lazy iterator for one input iterable. It retains at most `buffersize`
futures and consumes new input when iteration advances. Head-of-line blocking is
intentional. Timeout is a total budget measured from calling `map_bounded`, not a
per-task runtime limit. Submission/admission can independently block beyond it;
combine a finite `submit_timeout` with application deadlines when that matters.
Close an abandoned iterator explicitly to cancel buffered unstarted tasks.
Running tasks continue; breaking a loop alone does not necessarily close an
iterator retained elsewhere. No automatic source iterator close is promised.

## Metrics

`snapshot()` returns an independent JSON-safe dictionary containing:

- `counts`: submitted, completed (success), failed, cancelled, rejected (admission
  timeout), thread_submitted, process_submitted, peak_pending, profiles_evicted
- `pending`, `closed`, `max_pending`
- `physical`: per-backend `queued` and `in_flight`, their peaks, `retained`,
  `peak_retained`, `max_retained`, and per-backend `dispatch_limits`
- `load`: `thread` and `process` records containing `workers`, `queued`, and
  `in_flight`, plus the shared `max_pending`
- `policy`: bounded built-in `AdaptivePolicy` decision summary, otherwise `None`
- `predictive`: hardware, objective, last prediction and profiling metadata when
  enabled, otherwise `None`
- `profiles[key][backend]`: samples (successful observations), ewma_seconds,
  total_seconds (successful observations), failures, uncontended_samples,
  uncontended_ewma_seconds, uncontended_total_seconds

At quiescence, `submitted == completed + failed + cancelled + pending`.
Synchronous validation/immediate pool-submit errors are not accepted tasks and do
not count as failed tasks; deferred dispatch errors do. Cancellation does not train
latency or create profiles. Completion latency excludes admission wait and includes
owned queueing, dispatch/startup, serialization and completion handling. Uncontended
samples are the subset accepted when the chosen backend had no outstanding work;
they are not intrinsic service-time or CPU-utilisation measurements.
Snapshots are copied under a lock. A future can wake a `result()` caller just
before its done callback records metrics; `shutdown(wait=True)` establishes final
metrics. Profiles retain at most `max_profile_keys` keys and two backend records
per key. Counts continue after profile eviction. Eviction resets that key's
learning history. Raw arguments and results are never recorded by this package.

## Extension policies

A policy has `choose(hints, profiles, process_available) -> "thread" | "process"`.
It receives nested read-only maps of immutable `Profile` dataclasses. Selection
runs under the scheduling lock; keep it cheap and nonblocking. Never submit to,
shut down, or wait on the same executor from a policy. A policy must not perform
network calls or side effects in scheduling. Exceptions propagate to the caller.
Policy routing is validated against the process-consent boundary after selection.

`PolicyRegistry()` contains `threads`, `hints`, and `adaptive`. Use
`register(name, factory)` and `create(name)`, then pass the created policy object
to the executor. Custom registries do not mutate a process-global registry.
Factories run outside the registry lock and should return fresh instances. Custom
stateful policies shared across executors must synchronize their own state, or
provide `for_executor(*, max_keys=...)` to return an independent session. Exact
built-in `AdaptivePolicy` instances are configuration templates: each executor
gets fresh bounded exploration/switching state; custom subclass state is retained
unless that subclass overrides the hook.

The optional `choose_with_load(hints, profiles, process_available, load)` hook takes
precedence over `choose`. The exported immutable `SchedulingLoad` contains two
`BackendLoad` values and `max_pending`. `BackendLoad.outstanding` is queued plus
in-flight work; `pressure` is outstanding / workers, and `slot_utilisation` is
in-flight / workers (both zero for a disabled backend). These are dispatch-slot
counts, never measurements of CPU utilisation or child-process activity.

The adaptive load-aware path combines explicit hints, bounded per-key/kind
exploration, uncontended completion EWMA, queue pressure, and a switching margin
(default 10%). Its three-argument `choose` remains available and uses completion
EWMA without load. Neither path guarantees optimality or a speedup. See
[ARCHITECTURE.md](ARCHITECTURE.md) for measurement boundaries.

## Shutdown and deadlocks

`shutdown(wait=True, *, cancel_futures=False)` prevents new work and wakes blocked
submitters. One shutdown coordinator keeps both backend pools open until accepted
owned queues and dispatches drain, then joins the completion pump and pools.
With `cancel_futures=True`, it attempts cancellation of all known uncommitted work, including owned queued
work; running thread calls and committed process dispatches remain live.

`wait=False` starts the same coordinated drain and returns without joining it.
Later calls may escalate cancellation or join that same coordinator. Use a context
manager or explicit `shutdown(wait=True)` before interpreter exit to guarantee
that accepted work is drained. Omitting shutdown does not provide full stdlib
implicit-teardown equivalence: deferred submissions may settle with `RuntimeError`
if a backend has already begun interpreter shutdown. No signal handlers, forced
termination, task CPU limits, or persistence/restart guarantees are installed.

A waiting shutdown from one of this executor's own tasks or its executor-driven
completion callback context raises `RuntimeError` rather than joining itself. Do not call blocking submission
recursively from saturated pools or build cyclic Future dependencies. These
remain deadlock hazards; the executor does not detect or resolve them.

### Cancellation and physical bounds

`pending <= max_pending` and `physical.retained <= max_pending` are separate
invariants. Retained work is owned queued plus in-flight work, with each backend's
in-flight count bounded by its worker budget. Cancelling an owned queued Future
removes its payload rather than adding a tombstone to the backend queue. A
cancelled dispatched thread item releases logical admission but retains physical
capacity until acknowledgement, so admission can remain blocked even at
`pending == 0`.

This item-count bound does not limit argument/result bytes, memory retained by
blocked producers or caller-owned Futures, process transport buffers, native
allocations, or RSS. Older cancellation-churn evidence describes the earlier
direct-submission implementation; it is not the current queue contract.

## Optional layers added in 0.2.0

[TaskRegistry and TaskService](FRAMEWORK.md) build on the core API and current
lifecycle contract documented above. They provide a dependency-free explicit allowlist and bounded JSON task lifecycle.
The separate [MCP stdio adapter](MCP.md) requires the `mcp` extra. Neither is
imported by the top-level core package; importing existing symbols adds no SDK,
network, or application-service side effects.

## Opt-in predictive and batch APIs (0.3)

See [PREDICTIVE.md](PREDICTIVE.md) for complete semantics, measured/unavailable
metrics, privacy, process consent, JSON bounds, and cancellation limitations.

- Constructor additions: `predictive=False`, `hardware=None`, `objective=None`,
  `workload_profiles=()`, `profile_sample_interval=1`; they never enable processes implicitly.
- `submit_profiled(fn, /, *args, hints=None, features=(), **kwargs)`
- `export_profiles()` returns immutable bounded profile snapshots
- `map_batches(batch_fn, iterable, *, buffersize=2, min_batch_size=1,
  max_batch_size=64, target_batch_seconds=0.05, hints=None)` executes real chunks
- `map_adaptive(fn, iterable, *, min_batch_size=1, max_batch_size=64,
  target_batch_seconds=0.05, hints=None)` adapts submission windows only
- Public models: `HardwareCapabilities`, `ExecutionObjective`, `RuntimeSample`,
  `WorkloadProfile`, `ScalingBucket`, `Prediction`, `Predictor`
