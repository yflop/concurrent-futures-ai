# Appendix E: Cancellation, timeouts, and lifecycle

## 1. Cancellation is a truthful state transition

The returned object is a genuine executor-owned `concurrent.futures.Future`, separate from the backend's internal Future. A successful `cancel()` means the callable will not execute. A false result means the caller must still account for that operation's outcome. The executor never reports successful cancellation while allowing that callable to begin afterward.

Owned queued cancellation removes the payload from a per-backend FIFO. A dispatched thread task can still be cancelled before its worker commits to execution, but its physical slot stays occupied until backend acknowledgement. A process Future becomes `RUNNING` at dispatch commitment because process transport cannot share the public Future. Its callable may not yet have started; cancellation is already unavailable.

The [standard-library contract](https://docs.python.org/3.13/library/concurrent.futures.html) is the baseline for result retrieval and cancellation. Process submission can become non-cancellable before the application function visibly begins, because the backend has already committed work for dispatch. Treat `cancel()` as an attempt and inspect its returned boolean; do not infer success from an empty log or an elapsed deadline.

## 2. Lifecycle review matrix

| Situation | Caller-visible behavior | Resource-accounting expectation |
| --- | --- | --- |
| Validation fails | Submission raises before acceptance | No admitted-task count retained |
| Capacity wait expires | `BackpressureError` | No backend task submitted |
| Shutdown starts while a producer waits | Producer wakes and receives `RuntimeError` | No new task admitted |
| Policy returns an invalid backend | Submission raises | No implicit fallback execution |
| Immediate backend submission raises | Submission raises | No accepted future returned |
| Deferred backend submission raises | Accepted Future contains the error | Failed count grows; payload released; no replay |
| Backend acknowledges completion | Result/exception is published on the public Future | Physical slot is released; next owned queued item may dispatch |
| Callable completes | Future contains its result | Accounting callback releases logical capacity |
| Callable raises | Future contains the exception | Failure count grows; latency learner is not trained by the failure |
| Owned queued cancellation succeeds | Future is cancelled and visible to `wait`/`as_completed` | Logical and queued physical capacity are reclaimed; payload removed |
| Dispatched thread cancellation succeeds | Callable will not run | Logical capacity is released; physical slot retained until acknowledgement |
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

`shutdown(wait=True)` closes admission and wakes producers waiting for capacity. A single shutdown coordinator leaves the pools open while accepted queues and dispatches drain, then joins owned completion handling and both pools. With `cancel_futures=True`, the executor attempts cancellation of owned queued and uncommitted dispatched work; running threads and committed process work continue.

`shutdown(wait=False)` starts the same coordinator and returns without waiting for all work. Later calls can escalate cancellation or wait for that coordinator; cancellation remains sticky. It is not a guarantee that the Python program immediately exits or that external effects stop. Completion callbacks can still update state afterward, so an immediate snapshot is not a final ledger.

Use `with` or explicit `shutdown(wait=True)` to guarantee draining accepted work before interpreter exit. There is no full stdlib implicit-teardown equivalence: without explicit shutdown, deferred submissions can encounter backend teardown and settle with `RuntimeError` instead of executing. A bounded fresh-interpreter regression is a check of this behavior, not a promise of recovery from arbitrary interpreter/process failure.

Call waiting shutdown from an owning coordinator. Calling `shutdown(wait=True)` from one of this executor's own tasks or its executor-driven completion callback context raises `RuntimeError` to avoid a self-join. A callback invoked immediately on an external caller after completion is a different context; the guard is not general callback isolation. Short callbacks should report state to a coordinator rather than manage the whole executor lifecycle.

## 5. Callback ordering and races

Completing a future and finishing every callback are distinct events. `future.result()` can return before bookkeeping is fully observed. This project registers its bookkeeping while the submission is serialized, and `shutdown(wait=True)` is the intended final-accounting boundary.

Normal user completion callbacks run **serially on one executor-owned completion pump**, outside the scheduler lock. Backend callbacks only enqueue notifications; the pump acknowledges dispatch completion and publishes public results. A callback added after completion runs immediately on the adding caller, and external cancellation may run callbacks on its caller.

**Never wait for another Future from the same executor inside a callback, even if worker slots are free.** That other Future can need the same blocked pump to publish its result. A callback that submits into full admission can likewise stop progress. Keep callbacks short and notify an external coordinator to handle dependencies. Slow network I/O in a callback can delay unrelated result publication, physical acknowledgement, and future dispatch.

Future API compatibility and explicit waiting shutdown do not imply identical stdlib callback/thread/implicit-exit scheduling semantics. Callback `BaseException` behavior remains outside the supported application contract. The pump has a defensive handler to avoid stranding unrelated accepted work, but this is not callback fault isolation and does not guarantee every callback on the same Future runs. Cancellation-caller callback exceptions can still propagate.

Tests should use events and barriers to establish ordering, then add a timeout as a safety net. Sleeping for an arbitrary number of milliseconds and assuming a task has started is fragile under slow CI. For cancellation-versus-start races, permit either legal winner and assert the invariant: a task whose cancellation returned true must not execute its callable.

## 6. Logical and physical scheduling bounds

The executor maintains both `pending <= max_pending` and `queued + in_flight <= max_pending`, summed across the two backends. Each backend's in-flight count is at most its worker budget. Its owned FIFO can remove cancelled queued items immediately; those items were never handed to a backend. Cancellation churn behind a blocked worker therefore does not enqueue a replacement backend tombstone on each iteration.

Logical admission and physical acknowledgement remain different events. Successful cancellation of a dispatched thread Future releases logical capacity but retains the physical slot until its backend acknowledges completion. New admission must satisfy both bounds. `snapshot()["physical"]` exposes current and peak counts; `snapshot()["load"]` exposes scheduling occupancy, not measured CPU utilisation.

These bounds count items, not bytes. They do not bound callable allocations, blocked-producer argument graphs, retained results, transport bytes, or total RSS. Process work is committed at dispatch and cannot be cancelled to reclaim a running child or undo external side effects.

Earlier cancellation-churn artifacts record a limitation of the previous direct-submission implementation. Treat them as historical evidence, not the current physical queue contract or proof of current memory behavior. The new bounded regression and any updated stress results must be evaluated on their own recorded source and environment.

## 7. Nested work and deadlock

Consider a worker that occupies the last execution slot and waits for a child future submitted to the same executor. The child cannot run until the parent releases that slot, while the parent cannot release it until the child finishes. Bounded admission adds another version: all admitted parents can block trying to submit more work into the full bound before they even receive child futures.

The executor does not automatically detect these cycles. Prefer orchestration in a separate coordinator, flatten nested parallel work, or use a separately budgeted executor with a clear ownership model. A larger pool only hides a cycle until workload size exceeds it. Automatically running nested work inline would change scheduling and isolation semantics, so it is not a transparent fix.

## 8. Minimum lifecycle test suite for contributors

- Cancel a queued thread task behind a worker held by an event.
- Verify cancellation is visible through both `wait` and `as_completed`.
- Race cancellation with release of the start barrier.
- Raise an application exception and verify capacity is reusable.
- Cause immediate and deferred submission failures; verify correct error delivery and no permit leak.
- Retain cancelled Futures while checking weak references to queued callable/argument payloads.
- Hold backend acknowledgement after dispatched cancellation and verify physical capacity is not reused.
- Verify FIFO drain, per-backend dispatch limits, and independent backend progress.
- Close admission with a producer waiting and verify that producer wakes.
- Exercise `cancel_futures=True` with running and queued work, including escalation after nonblocking shutdown.
- Verify process Futures become non-cancellable at dispatch commitment.
- Exercise omitted-shutdown behavior in bounded disposable interpreters; do not confuse it with guaranteed draining.
- Close a partially consumed bounded-map iterator.
- Use an importable process fixture and inspect normal result and exception transport.
- Run intentionally deadlocking or crashing scenarios in disposable subprocesses with bounded cleanup.
- Verify waiting shutdown joins the public completion pump and its callbacks.
- Verify completion pumps retire during idle periods without retaining prior result payloads.
- Contain a same-executor Future wait from a callback in a disposable subprocess; free worker slots must not be mistaken for safe callback reentry.
- Verify final snapshots after a full waiting shutdown rather than relying on callback timing.

Future retries, DAGs, and fail-fast groups must preserve these invariants and explicitly define what happens to already-running side effects. No orchestration label can turn cancellation into rollback.
