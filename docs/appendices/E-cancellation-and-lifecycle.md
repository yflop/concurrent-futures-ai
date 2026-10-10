# Appendix E: Cancellation, timeouts, and lifecycle

## 1. Cancellation is a truthful state transition

The returned object is the actual backend future. A successful `cancel()` means the backend accepted cancellation before the work became non-cancellable. A false result means the caller must still account for that operation's outcome. The executor does not manufacture a cancelled proxy while the underlying call continues invisibly.

The [standard-library contract](https://docs.python.org/3.13/library/concurrent.futures.html) is the baseline for result retrieval and cancellation. Process submission can become non-cancellable before the application function visibly begins, because the backend has already committed work for dispatch. Treat `cancel()` as an attempt and inspect its returned boolean; do not infer success from an empty log or an elapsed deadline.

## 2. Lifecycle review matrix

| Situation | Caller-visible behavior | Resource-accounting expectation |
| --- | --- | --- |
| Validation fails | Submission raises before acceptance | No admitted-task count retained |
| Capacity wait expires | `BackpressureError` | No backend task submitted |
| Shutdown starts while a producer waits | Producer wakes and receives `RuntimeError` | No new task admitted |
| Policy returns an invalid backend | Submission raises | No implicit fallback execution |
| Backend submission raises | Submission raises | No accepted future returned |
| Callable completes | Future contains its result | Completion callback releases logical capacity |
| Callable raises | Future contains the exception | Failure count grows; latency learner is not trained by the failure |
| Pending cancellation succeeds | Future is cancelled | Logical capacity is reclaimed, subject to callback completion |
| Cancellation loses a race to start | `cancel()` returns false | Task remains accounted for until terminal completion |
| Worker process breaks | Affected futures report backend errors | No silent replay or automatic side-effect retry |

This matrix is a test checklist, not a promise that a Python program can recover arbitrary corrupted process state. Abruptly exiting the host process loses local bookkeeping and unfinished work.

## 3. Four different meanings of timeout

1. `submit_timeout` controls waiting for admission capacity. `None` allows an indefinite wait; zero requests no capacity wait when full. It does not preempt a slow custom policy or a running callable.
2. `future.result(timeout=...)` controls how long that caller waits for a result. On timeout, the future can still complete later.
3. `map_bounded(..., timeout=...)` limits result waiting relative to the map call. Admission inside the iterator can still block independently.
4. An application's desired execution deadline is a separate requirement. The current executor does not enforce a per-task runtime deadline.

A network operation should set its own service-level timeout as well. Without that, an executor wait timeout can leave the actual socket operation running. A cooperative cancellation token is a planned extension; killing threads is not a safe substitute.

## 4. Shutdown choices

Use the context manager for ordinary finite work. It waits for outstanding work during exit, including when the surrounding block raises. Handle application exceptions knowing that cleanup may still wait for running tasks.

`shutdown(wait=True)` closes admission and waits for the underlying pools. The executor also wakes producers waiting for capacity so they can observe closure. With `cancel_futures=True`, the underlying pools attempt to cancel work that has not started; running work continues.

`shutdown(wait=False)` returns without waiting for all work. It is not a guarantee that the Python program immediately exits or that external effects stop. Completion callbacks can still update state afterward, so an immediate snapshot is not a final ledger.

Call shutdown from an owning coordinator, not from one of the executor's own workers or completion callbacks. Worker-owned shutdown can try to join the very machinery executing the call. Short callbacks should report state to a coordinator rather than manage the whole executor lifecycle.

## 5. Callback ordering and races

Completing a future and finishing every callback are distinct events. `future.result()` can return before bookkeeping is fully observed. This project registers its bookkeeping while the submission is serialized, and `shutdown(wait=True)` is the intended final-accounting boundary.

User callbacks can run on backend management or worker threads, or immediately when added after completion. Keep them small. A callback that submits into a full executor can block the thread needed to make progress. A callback that performs slow network I/O contaminates apparent completion handling and delays other work.

Tests should use events and barriers to establish ordering, then add a timeout as a safety net. Sleeping for an arbitrary number of milliseconds and assuming a task has started is fragile under slow CI. For cancellation-versus-start races, permit either legal winner and assert the invariant: a task whose cancellation returned true must not execute its callable.

## 6. The cancellation tombstone limitation

Logical admission and physical queue occupancy are different. A cancelled future can release logical capacity while a corresponding cancelled work item remains inside a standard-library queue. If all workers are blocked, a producer can repeatedly submit and cancel replacement work, accumulating physical tombstones.

The current implementation does not own a removable dispatcher queue. `max_pending` must never be sold as a strict memory cap or strict physical queued-item bound. When this workload is plausible, apply an application-side limit on cancellation churn, avoid permanently blocked workers, and measure memory. A physical-removal design is recorded separately in the catalog because it needs a new dispatch contract and race tests.

This also matters for process pools, whose dispatch and transport layers have their own buffering. Cancelling a client-visible task does not prove bytes were removed from all transport queues. A future byte bound must state exactly which buffers it accounts for.

## 7. Nested work and deadlock

Consider a worker that occupies the last execution slot and waits for a child future submitted to the same executor. The child cannot run until the parent releases that slot, while the parent cannot release it until the child finishes. Bounded admission adds another version: all admitted parents can block trying to submit more work into the full bound before they even receive child futures.

The prototype does not automatically detect these cycles. Prefer orchestration in a separate coordinator, flatten nested parallel work, or use a separately budgeted executor with a clear ownership model. A larger pool only hides a cycle until workload size exceeds it. Automatically running nested work inline would change scheduling and isolation semantics, so it is not a transparent fix.

## 8. Minimum lifecycle test suite for contributors

- Cancel a queued thread task behind a worker held by an event.
- Verify cancellation is visible through both `wait` and `as_completed`.
- Race cancellation with release of the start barrier.
- Raise an application exception and verify capacity is reusable.
- Cause submission failure and verify no permit leak.
- Close admission with a producer waiting and verify that producer wakes.
- Exercise `cancel_futures=True` with running and queued work.
- Close a partially consumed bounded-map iterator.
- Use an importable process fixture and inspect normal result and exception transport.
- Run intentionally deadlocking or crashing scenarios in disposable subprocesses with bounded cleanup.
- Verify final snapshots after a full waiting shutdown rather than relying on callback timing.

Future retries, DAGs, and fail-fast groups must preserve these invariants and explicitly define what happens to already-running side effects. No orchestration label can turn cancellation into rollback.
