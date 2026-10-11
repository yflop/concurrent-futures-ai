# Operational compatibility audit

The returned objects are genuine stdlib Futures. That supports the public Future
API; it does not promise all scheduling or lifecycle behavior of both stdlib
executors. The paired probe in `tools/probe_operational_semantics.py` records
host/interpreter/source identity and uses bounded gates and subprocess timeouts.

## Genuine additional constraints

- **Completion callbacks:** Intelligent Futures uses one completion pump for
  both backends. A slow callback blocks later result publication and scheduler
  acknowledgement even when thread workers are free. CPython ThreadPoolExecutor
  normally publishes on independent workers, so one such callback need not block
  a different worker. CPython ProcessPoolExecutor publishes through one manager
  thread and exhibits a similar serial-callback hazard. These thread-placement
  observations are implementation details, not a portable callback-thread promise.
- **Nested admission:** Bounded admission can block before submit returns, even
  if a task never waits on the submitted Future and workers remain available.
  Stdlib submit is unbounded. Caller-supplied bounded stdlib wrappers can create
  the same constraint. The candidate conservatively rejects saturated
  same-executor task/pump submissions immediately with BackpressureError and a
  reentrant_rejected count. It does not bypass the bound or add capacity.
- **Implicit interpreter exit:** Deferred owned work still needs fresh backend
  submissions. During interpreter teardown the stdlib backends reject those;
  explicit shutdown/context-manager use is required to guarantee draining.
  The controlled 0.5.0 probe accepted four tasks, completed the first and failed
  the three deferred tasks with RuntimeError; stdlib threads completed all four.
- **Waiting self-shutdown:** The candidate rejects shutdown(wait=True) in its
  own task/pump context before closing admission. Stdlib threads also cannot join
  their current worker; on the probed interpreter they raise after closing it.
  Run lifecycle operations from an external coordinator.

## Shared restrictions

Worker starvation and dependency cycles exist in stdlib thread pools too. A
running thread task cannot be forcibly cancelled, and a hung task can block
waiting shutdown. Processes need pickleable, spawn-safe code. Calls from process
work into executor/Future methods are unsupported. Abrupt process loss breaks
that process backend; recovery here means explicitly creating a new executor,
not automatically replaying side-effecting work or repairing the old pool.

Callbacks added after completion execute immediately in the caller; cancellation
may likewise invoke callbacks on the cancelling thread. Standard Future callback
ordering is preserved for each Future. BaseException behavior in callbacks is
undefined by stdlib; Intelligent Futures attempts to preserve unrelated progress
but does not turn arbitrary callbacks into safe code.

## Safe patterns and remaining limits

Keep callbacks short and nonblocking, handing notifications to an external
coordinator. Use an externally drained, bounded application queue with an
explicit full-queue policy; blocking to put onto a full queue from a callback can
reintroduce a dependency cycle. Do not wait on same-executor Futures in callbacks,
including after successful nested submission. Schedule dependency graphs from
outside task bodies. Use finite admission and result timeouts appropriate for
the caller, but remember a result timeout does not stop the task. Use context
managers or explicit shutdown(wait=True) from the lifecycle coordinator.

A bounded pool of publication workers could reduce single-callback interference,
but blocked callbacks can exhaust any finite publisher budget. It requires new
publication-slot accounting, shutdown/cancellation rules, and substantial tests.
It is deferred; this candidate does not claim callback isolation.

Sources: Python 3.12 standard-library documentation for
[Executor and Future contracts](https://docs.python.org/3.12/library/concurrent.futures.html),
the published package's `executor.py`, and the recorded paired probe. Cross-
version/OS callback placement and implicit-exit behavior require their own tests.
