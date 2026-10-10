# API and operational contract

## Construction

`IntelligentExecutor(max_workers=4, *, process_workers=0, max_pending=64,
policy="adaptive", submit_timeout=None, ewma_alpha=0.25, max_profile_keys=256,
mp_context=None)`

Worker counts are fixed for each executor. A process count of zero disables that
backend. A provided multiprocessing context is passed through; the default is
`multiprocessing.get_context("spawn")`. The caller owns initialization, import
safety, environment, and secrets inherited by child processes.

`max_pending` is admission capacity, not a second independent queue size. Calls
block until capacity exists; `submit_timeout=0` provides fail-fast rejection;
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

Futures are returned directly from the selected standard-library executor. No
proxy guesses whether a task is running. `cancel()` can return false before the
callable actually starts, especially for process-pool dispatch queues. Exceptions
are not swallowed; no automatic fallback or duplicate retry occurs.

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
- `profiles[key][backend]`: samples (successful observations), ewma_seconds,
  total_seconds (successful observations), failures

At quiescence, `submitted == completed + failed + cancelled + pending`.
Synchronous validation/pool-submit errors are not accepted tasks and do not count
as failed tasks. Cancellation does not train latency or create profiles.
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
Factories should return fresh instances; stateful policies are not guaranteed
thread-safe across different executors.

## Shutdown and deadlocks

`shutdown(wait=True, *, cancel_futures=False)` prevents new work and wakes blocked
submitters. It shuts down both backend pools. With `cancel_futures=True`, only
queued futures the backend can cancel are cancelled. Running tasks remain live.
With `wait=False`, interpreter exit can still wait for tasks, as in stdlib.
Repeated shutdown is supported. No signal handlers, forced termination, or task
CPU limits are installed.

Do not call blocking submission recursively from saturated worker pools, call
`shutdown(wait=True)` inside a worker, or build cyclic Future dependencies. These
are standard executor hazards with additional admission constraints. An explicit
DAG scheduler is a planned concept, not part of this executor.

### Cancellation churn and memory

The admission limit is not a strict physical queue or memory bound. Cancelling a
queued Future frees logical admission immediately, but stdlib pools can retain
its cancelled work item and arguments until a worker drains the queue. Repeated
submit/cancel loops behind blocked workers can therefore accumulate tombstones.
Rate-limit producers and cancellation churn; do not treat this executor as a
hard memory limiter. A removable bounded physical queue is planned work.

## Optional layers added in 0.2.0

The core API above is unchanged. [TaskRegistry and TaskService](FRAMEWORK.md)
provide a dependency-free explicit allowlist and bounded JSON task lifecycle.
The separate [MCP stdio adapter](MCP.md) requires the `mcp` extra. Neither is
imported by the top-level core package; importing existing symbols adds no SDK,
network, or application-service side effects.
