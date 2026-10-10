# Appendix B: Architecture and API contracts

## 1. Scope of the executor

`IntelligentExecutor` is an `Executor` subclass coordinating two standard-library pools. It adds logical and physical admission bounds, removable per-backend FIFO queues, route selection, and completion measurement. Returned Futures are genuine executor-owned `concurrent.futures.Future` instances; backend Future identities remain internal. Successful cancellation prevents callable execution. The main implementation is [`executor.py`](../../src/intelligent_futures/executor.py), and policy types are in [`policies.py`](../../src/intelligent_futures/policies.py).

This execution layer supports a workload-aware framework for evaluating and selecting concurrency configurations. Configuration quality is the objective, not a universal execution-engine speedup against the same stdlib settings.

The design favors explicit behavior over automatic discovery. A task's author knows whether its arguments are transferable and whether its semantics tolerate a different process. A small routing policy can use measurements after that permission is established. It cannot infer permission from a task name or timing.

## 2. Constructor and configuration

```python
IntelligentExecutor(
    max_workers=4,
    process_workers=0,
    max_pending=64,
    policy="adaptive",
    submit_timeout=None,
    ewma_alpha=0.25,
    max_profile_keys=256,
    mp_context=None,
    predictive=False,
    hardware=None,
    objective=None,
    workload_profiles=(),
    profile_sample_interval=1,
)
```

| Setting | Meaning | Important limit |
| --- | --- | --- |
| `max_workers` | Maximum thread worker count; positive integer | Does not count processes or native threads created by callables |
| `process_workers` | Process worker count; zero disables this backend | Enabling it does not make every callable eligible |
| `max_pending` | Shared cap on logical unfinished tasks and on owned queued plus dispatched items | Two checks against one bound; not a byte or RSS budget |
| `policy` | Built-in policy name or an object implementing `choose` | Adaptive is an experimental default; custom code runs under the scheduler lock |
| `submit_timeout` | Maximum capacity-wait interval before `BackpressureError` | Not a callable runtime or complete-operation deadline |
| `ewma_alpha` | Weight of the latest successful latency sample in `(0, 1]` | Higher values react faster and are more sensitive to noise |
| `max_profile_keys` | Maximum number of retained task-key profile buckets | Snapshot copies retained by callers are outside this bound |
| `mp_context` | Optional multiprocessing context | Omission selects `spawn` in this project |

The worker counts are independent. `max_workers=4, process_workers=2` means up to four thread workers and two process workers. It does not mean a single shared budget of four workers. `max_pending` is the shared logical and physical item-count budget. Each backend also limits outstanding dispatches to its own worker count. Predictive options and their narrower global admission cap are described in [PREDICTIVE.md](../PREDICTIVE.md).

## 3. Two deliberately separate submission methods

`submit(fn, *args, **kwargs)` forwards all keyword arguments to `fn`, including an argument named `hints`. It uses default `TaskHints`, which do not authorize processes. Ordinary submission therefore remains thread-only with the supplied built-in policies, even if a process pool is configured.

`submit_task(fn, *args, hints=TaskHints(...), **kwargs)` reserves `hints` for executor metadata. Other keywords still belong to the callable. A callable that itself accepts a keyword called `hints` can receive it through ordinary `submit`; do not accidentally feed application arguments into routing metadata.

```python
from intelligent_futures import IntelligentExecutor, TaskHints


def count_squares(limit):
    return sum(value * value for value in range(limit))


def main():
    with IntelligentExecutor(
        max_workers=4, process_workers=2, max_pending=16, policy="hints"
    ) as executor:
        future = executor.submit_task(
            count_squares,
            100_000,
            hints=TaskHints(kind="cpu", key="count-squares:v1", process_safe=True),
        )
        assert future.result() == count_squares(100_000)


if __name__ == "__main__":
    main()
```

This example demonstrates the API and a correctness check. It is not a performance demonstration: a small calculation may cost less than process startup and transfer.

### Task metadata

- `kind`: `"unknown"`, `"io"`, or `"cpu"`. This expresses application knowledge, not an automatically inferred property.
- `key`: a nonempty string of at most 256 characters, defaulting to `"default"`. Use a stable identifier for comparable work. Avoid credentials, names, request bodies, or a unique identifier for every request.
- `process_safe`: a boolean, default `False`. `True` declares both serialization feasibility and acceptable process semantics. It does not certify either one.

The `hints` policy selects processes for explicitly process-safe CPU work when the process backend exists. It selects threads otherwise. The `threads` policy always selects threads. The adaptive policy may explore both eligible backends for CPU and unknown work; it always keeps I/O hints on threads. See Appendix C for the exact learning logic.

## 4. Admission and execution path

Conceptually, a submission passes through these steps:

1. Validate the callable and metadata.
2. Wait on shared admission capacity, subject to `submit_timeout`.
3. Refuse new work after shutdown.
4. Ask the selected policy for a backend.
5. Validate the decision and enforce process eligibility.
6. Create the public standard Future; dispatch if a backend slot is free, otherwise retain a removable FIFO item.
7. Count the accepted work and attach completion bookkeeping before returning the public Future.
8. The backend enqueues a completion notification. An owned completion pump acknowledges it, releases the physical slot, dispatches the next eligible FIFO item, and publishes the result or exception. Public completion accounting releases logical admission.

Thread workers commit the public Future to running immediately before invocation. Process work commits to running when it is dispatched, before child execution necessarily begins. No dispatch exceeds its backend's worker budget. FIFO governs accepted queued items within a backend, not producer fairness or cross-backend completion order.

The policy is called while holding the scheduling lock. It must be short, nonblocking, and free of executor reentry. A slow policy delays other submissions and callbacks. The core rejects unsupported backend names and process decisions without caller permission or a configured process pool. It does not impose a safe execution deadline on arbitrary plugin code.

If immediate backend submission fails, the caller receives the error directly and no task is counted as accepted. If a queued task's later dispatch fails, the already accepted public Future receives the error and failure accounting runs. Callable failures are likewise reported through the public Future; no failure triggers replay.

Existing custom policies retain `choose(hints, profiles, process_available)`. An optional `choose_with_load(..., load)` hook receives immutable exported `SchedulingLoad`/`BackendLoad` values and takes precedence. Counts represent owned queued work and occupied dispatch slots, including cancelled dispatches awaiting acknowledgement; they do not measure CPU utilisation. The built-in adaptive path uses uncontended completion observations, pressure, bounded per-key/kind exploration and a switching margin. These are heuristics, not optimality guarantees. Built-in policy templates receive independent executor sessions; shared custom state must synchronize itself or implement `for_executor(*, max_keys=...)`.

## 5. What the bound does and does not guarantee

`max_pending` bounds logical admitted unfinished work and, separately, physical scheduling items: the sum of owned queued and dispatched work across both backends. Each backend's dispatched count is at most its worker count. Successful completion, failure, or cancellation releases logical capacity through accounting. With multiple producers, admission order depends on thread scheduling; FIFO producer fairness is not promised.

Owned queued cancellation removes the item and releases its callable/argument payload without submitting a backend tombstone. A cancelled dispatched thread item keeps its physical slot until backend acknowledgement. Both checks must permit a new submission, so zero logical pending work does not necessarily mean physical capacity is free. Committed process dispatch cannot be cancelled. Historical direct-submission cancellation-churn reports do not describe the current queue contract.

This is an **item-count bound, not a memory bound**. Other consumers include producer threads waiting to submit, their argument graphs, results/Futures or snapshots retained by the application, transport buffers, and allocations inside running tasks. One admitted task can consume most of the machine's memory. Byte accounting, preemption, callback isolation and dependency-deadlock prevention are not supplied.

## 6. Measurements and snapshots

The timed interval starts when the owned work item is created after admission/policy selection and ends when public-Future accounting runs. It includes owned FIFO waiting, dispatch/startup, execution, applicable process serialization and result transfer, and completion handling. It excludes admission waiting and policy evaluation. This is **accepted-submission-to-completion latency**, not pure CPU time, intrinsic service time, or total request latency.

Successful samples update `samples`, `ewma_seconds`, and `total_seconds` under `(key, backend)`. The `uncontended_*` subset records tasks accepted when their backend had no outstanding work; it still includes startup/serialization overhead. The load-aware adaptive policy uses that subset instead of multiplying an already queue-delayed EWMA by pressure. Failures increment the failure count without training latency. Cancellations update executor counts and do not become successful timing samples. Key buckets are evicted by least recent update after the configured key cap is exceeded.

`snapshot()` returns a detached JSON-safe data structure with counts, logical pending state, physical queued/in-flight counts and peaks, configuration bounds, immutable-load values, policy/predictive metadata, and profiles. A caller can serialize it without receiving mutable internal dictionaries. This is a local export, not a persistence or restart-learning feature.

A future can make its result available just before its callbacks finish. Do not require an immediately following snapshot to contain its final counters. `shutdown(wait=True)` or exit from the context manager establishes the final accounting point for work managed by the executor. The latency measurement's callback delay is another reason not to treat tiny timing differences as a reliable optimization signal.

Normal completion callbacks execute serially on one owned completion pump, outside the scheduler lock. **A callback must not wait for another Future from the same executor, even if worker slots are free:** the other result can require that blocked pump for publication. Slow callbacks can delay acknowledgement, results and dispatch. Notify an external coordinator instead. Callbacks attached after completion and externally triggered cancellation callbacks may run on the caller. Standard Future interoperability does not mean identical stdlib callback/thread or implicit-exit scheduling.

## 7. Mapping and result pressure

`map_bounded(fn, iterable, buffersize=16, timeout=None, hints=None)` is a project-specific, ordered, single-iterable streaming adapter. At most `buffersize` submitted-but-not-yet-yielded results are buffered by the adapter. It reads and submits on iterator advancement. It can accept the same `TaskHints` for each item.

Ordered delivery deliberately allows head-of-line blocking: one slow early item can delay later completed results. Closing the iterator attempts to cancel remaining buffered work. Already-running work continues. If the consumer abandons an iterator, explicitly call its `close()` method rather than relying on eventual garbage collection.

The map timeout is measured from the map call, but its result wait does not preempt admission blocking. Use a finite `submit_timeout` where indefinite producer blocking is unacceptable. Even then there is no hard end-to-end deadline guarantee.

Inherited `Executor.map` follows the installed Python version. It must not be described as equivalent to `map_bounded`: older Python versions eagerly collect inputs; newer versions have their own `buffersize` option. The project-specific method has one iterable and no process chunking argument. Check the [versioned standard-library API](https://docs.python.org/3.13/library/concurrent.futures.html) when migrating code.

## 8. Non-goals of this contract

Use a context manager or explicit `shutdown(wait=True)` to drain accepted work before interpreter exit. Shutdown starts one coordinator that keeps the pools open until accepted work drains and then joins the completion pump and both pools; later calls can escalate cancellation or join the same coordinator. A waiting shutdown from this executor's own task or executor-driven completion callback context raises `RuntimeError`. Omitting explicit shutdown does not provide full stdlib implicit-teardown equivalence: deferred work can settle with `RuntimeError` when backend teardown has already begun.

The base executor neither persists tasks nor recovers them after host loss. It does not make external writes transactional, automatically retry failures, inspect user code for safety, sandbox plugins, or guarantee a speedup. Those absences keep the current execution contract reviewable. Extensions should preserve that clarity rather than attach broad reliability or intelligence claims to a small measured mechanism.
