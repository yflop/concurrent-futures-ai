# Production reliability and migration checklist

Version 0.4 is a **Beta trusted-code executor**. Beta is not production
certification. Source verification, bounded endurance, built-artifact checks and
exact-commit CI have distinct scopes; retain their source-matched records.
The executor is not a sandbox, process supervisor, hard deadline engine, or hard
memory limiter. Use the
[API contract](API.md), [predictive contract](PREDICTIVE.md), and
[security boundaries](../SECURITY.md) as the supported behavior. This guide records
adversarial validation and the deployment responsibilities it cannot eliminate.

## Adoption without changing callable behavior

The repository is named `concurrent-futures-ai`; the distribution is
`intelligent-futures`, and Python imports use `intelligent_futures`. It does not
replace or monkeypatch `concurrent.futures`.

```python
from intelligent_futures import IntelligentExecutor, TaskHints

with IntelligentExecutor(max_workers=4, max_pending=32,
                         submit_timeout=0.25, policy="threads") as executor:
    future = executor.submit(my_callable, payload, hints="callable argument")
    result = future.result(timeout=10)
```

- Ordinary `submit` preserves every callable keyword, including `hints` and
  `features`. To pass scheduling metadata use `submit_task(..., hints=TaskHints(...))`.
  To supply explicit numeric summaries use `submit_profiled(..., features=...)`
  with `predictive=True`. These are separate APIs with reserved keywords.
- Returned objects are genuine executor-owned stdlib Futures, not backend Future
  identities. Standard `wait`, `as_completed`, exception propagation and
  cancellation state remain applicable. Queued tasks can be accepted before a
  backend dispatch slot is available; a later dispatch error belongs to that
  already accepted Future.
- Start with an explicit thread policy and finite admission timeout. Bounded
  admission can block a previously nonblocking producer; handle `BackpressureError`
  without automatically replaying a task that might already have side effects.
- Adding `process_workers` alone never grants process consent. Opt in per task
  using `process_safe=True`, an appropriate routing policy, importable callables,
  pickleable inputs/results and spawn-safe application startup.
- Predictive mode changes admission and can change routing when using the default
  adaptive policy. It does not resize pools. Benchmark it separately with realistic
  inputs and failure handling before adoption; measurements do not guarantee speedup.
- Core import does not import the framework, MCP adapter or optional MCP SDK.
  Import `TaskRegistry` and `TaskService` from `intelligent_futures.framework`.
  Services freeze registry contents at construction; register tasks before building
  a service when all definitions must be available together.

## Ownership, cancellation and shutdown

1. Give each executor an explicit lifetime. Stop application producers before
   shutdown when possible. Shutdown also wakes producers already blocked on
   admission; those calls raise `RuntimeError`, not an admission timeout.
2. Use `shutdown(wait=False)` only when the application will manage the remaining
   work. It is not a task termination primitive and does not make interpreter exit
   independent of running workers.
3. A later `shutdown(cancel_futures=True)` can escalate an earlier noncancelling
   shutdown. The cancellation request remains sticky. Owned queued work and
   uncommitted dispatched thread work can be cancelled. Process Futures become
   `RUNNING` at dispatch commitment, before their child callable necessarily starts.
4. Use a context manager or explicit `shutdown(wait=True)` to drain accepted work
   before interpreter exit and establish final metrics. One coordinator drains
   the owned queues before joining completion handling and closing/joining pools.
   At that point submitted equals completed plus failed plus cancelled. A caller returning from
   `future.result()` can precede the bookkeeping callback. Without explicit
   shutdown, deferred submissions can fail with `RuntimeError` during backend
   interpreter teardown; full stdlib implicit-teardown equivalence is not promised.
5. Explicitly close a partially consumed mapping iterator. Source errors and task
   errors trigger best-effort cancellation of buffered unstarted work. For
   `map_batches`, a source failure partway through a chunk can consume inputs
   without submitting that chunk. Already running work and prior side effects
   remain; there is no automatic retry or rollback.

A `future.result(timeout=...)` timeout limits waiting, not execution. Likewise,
`submit_timeout` limits admission only, and `map_bounded`'s result deadline does
not independently interrupt admission. Put hard resource/lifetime enforcement in
an external supervisor when required. Never call blocking recursive submission
from a saturated pool. A waiting shutdown from this executor's own task or
executor-driven completion callback context raises `RuntimeError`; other
dependency deadlocks are not automatically prevented.

### Done callbacks are application code

Normal completion callbacks run serially on one executor-owned completion pump,
outside the scheduler lock. **A callback must not wait for another Future from the
same executor, even when worker slots are free.** The other result can require
that blocked pump to be published. Slow callbacks delay result publication,
physical acknowledgement and dispatch; signal an external coordinator instead.
Callbacks added after completion, or triggered by external cancellation, may run
on the caller. Future API compatibility does not imply identical stdlib callback
threads, parallel callback execution or interpreter-exit scheduling.

The package registers accounting before returning a Future. A user cancellation
callback raising `SystemExit` on the cancelling caller cannot undo accounting
already recorded by the earlier callback. A focused regression covers that
ordering; it is not general callback fault isolation.

This does **not** make arbitrary callbacks safe: stdlib defines callback behavior
for raised `BaseException` subclasses as undefined. Do not raise `SystemExit`,
`KeyboardInterrupt`, or other `BaseException` subclasses from done callbacks. The
owned pump defensively handles callback failures to avoid stranding unrelated
accepted work; this is not a guarantee that later callbacks on the same Future
run. Exceptions from external cancellation callbacks can still propagate and
interrupt their caller. Standard per-Future callback order is retained and
backend Future identity is not exposed. Callbacks are not a fault isolation layer.
Callable `BaseException` results are a different case and are covered by the
executor's existing exception tests.

## Capacity and metadata bounds

- Core `max_pending` independently bounds logical unfinished work and physical
  retained items (owned queued plus dispatched). Each backend's outstanding
  dispatches are also bounded by its worker budget. Owned queued cancellation
  removes callable/argument payloads. Dispatched thread cancellation retains its
  physical slot until backend acknowledgement, even after logical completion.
  These item bounds are not byte/RSS limits. Earlier cancellation-churn artifacts
  describe the historical direct-submission implementation.
- Live policy load describes queued items and occupied dispatch slots, not CPU
  utilisation. Uncontended completion observations include startup/serialization.
  Routing, switching margins and predictive demand caps are heuristics; validate
  configuration quality on your workload. Predictive global admission can leave
  another backend idle and does not promise work-conserving scheduling.
- TaskService separately retains cancelled physical queue slots until drained,
  including after a record is forgotten. Its `max_tasks` record limit requires
  explicit forgetting of terminal records; there is no silent record eviction.
- Predictive profile keys/features and sampling counters have bounded retention;
  each profile retains at most 128 samples. Eviction loses history. These metadata
  bounds are not an RSS budget, nor a bound on callable inputs, outputs or native
  allocations. Exported immutable snapshots retained by callers consume additional
  application memory outside the executor's retention bound.
- Exception messages, raw inputs and results are absent from predictive exports,
  but caller-defined keys, numeric features and hardware metadata can identify
  workloads. Review exports before sharing.

## Validation evidence

`tests/test_production_reliability.py` contains adversarial checks including:

- Eight simultaneously blocked producers all wake on shutdown without inflating
  accepted/rejected counts.
- Nonblocking shutdown followed by cancellation escalation leaves running work
  alive while cancelling all known queued thread tasks.
- Cancellation callback `BaseException` preserves already-recorded accounting.
- Predictive backend submission `BaseException` leaks no credit or profile and a
  subsequent successful submission remains possible.
- Adaptive-window source failure cancels buffered work; partial batch source
  failure discards the incomplete chunk and cancels an earlier queued chunk.
- Twelve concurrent registrations of one name produce exactly one winner.
- Registry registration races with sorted discovery, service freezing and detached
  schema mutation without corrupting registry/service metadata.
- 150 unique feature buckets followed by 150 observations of one bucket preserve
  profile/counter/sample limits and total completion counts.
- A spawned process flushes one side effect and crashes. Its Future reports
  `BrokenProcessPool`; a later submission fails rather than replaying the callable.
- A fresh interpreter verifies stdlib compatibility, preserved callable keywords,
  genuine stdlib Future type, public exports and absence of optional-layer imports.

`tests/test_physical_dispatch.py` adds focused coverage for owned FIFO removal,
retained cancelled Futures without retained queued payloads, delayed physical
acknowledgement, cancellation/dispatch races, immediate waiter visibility,
per-backend dispatch budgets, process commitment, coordinated shutdown, deferred
submission failure, completion-pump joins/idle retirement, and omitted-shutdown
subprocess behavior.
`tests/test_load_aware_policies.py` covers immutable scheduling loads, uncontended
measurement provenance, deterministic exploration, switching margins, bounded
policy state, executor isolation, and predictive demand adjustment. These are
coverage descriptions, not a claim that final-source validation or long-running
stress testing has finished. Consult source-matched validation results separately.

Tests use bounded events/barriers and explicit thread/process exit checks. Run
from the repository root with an outer process timeout, for example on Linux:

```sh
PYTHONPATH=src timeout 90 python -m unittest discover -s tests \
  -p test_production_reliability.py -v
PYTHONPATH=src timeout 180 python -m unittest discover -s tests -v
```

The outer timeout is test containment, not an executor runtime feature. The suite
cannot prove absence of all scheduler races or recover arbitrary user code that
hangs. Re-run on every supported Python/OS target and test real application
shutdown, backpressure and failure handling before deploying. This work makes no
package publication, deployment or certification claim.
