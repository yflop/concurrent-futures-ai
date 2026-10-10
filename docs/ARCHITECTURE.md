# Architecture

Intelligent Futures is a workload-aware execution framework for Python that
automates the evaluation and selection of concurrency configurations. Execution
policies, workload characteristics, runtime measurements, and adaptive
optimisation help identify effective thread and process strategies under defined
resource constraints. Its purpose is configuration quality; the execution layer
does not promise to outperform the same stdlib configuration for every workload.

```
caller -> condition-protected admission -> policy + immutable load -> validated route
          -> executor-owned per-backend FIFO -> bounded dispatch -> stdlib pool
          <- executor-owned standard Future <- owned completion pump <- backend acknowledgement
          -> counters + bounded profiles -> later routing/admission decisions
```

The executor owns genuine `concurrent.futures.Future` objects and removable queued
payloads. Backend Futures remain internal. Separate thread/process FIFOs preserve
accepted queue order within each backend; they do not promise producer fairness or
completion order. Backend transport and execution remain stdlib responsibilities.
A condition lock guards admission, queues, dispatch slots, counts, profile updates,
and lifecycle state. Backend callbacks only enqueue completion notifications;
one executor-owned completion pump handles dispatch acknowledgement and public
result publication. The pump starts lazily and retires when the executor is idle.
This avoids entering scheduling/user code while a backend holds its internal
completion locks. No event loop, remote service, or generated code is needed.
Explicit shutdown starts a coordinator to drain accepted work, join completion
handling, and join both pools.

`max_pending` caps both logical unfinished tasks and retained scheduling items
(owned queued plus dispatched work). Each backend has at most its worker budget
of outstanding dispatches. Cancelling an owned queued Future removes its payload;
a cancelled dispatched thread item retains its physical slot until backend
acknowledgement. Process Futures become `RUNNING` at dispatch commitment, before
the child callable necessarily starts, and cannot then be cancelled.

Acceptance may precede backend submission when a dispatch slot is occupied.
Immediate backend submission failure raises from `submit`; a later dispatch
failure is published on the already accepted Future. Neither path replays work.
Accounting is registered before the public Future is returned. Application
completion callbacks and pool joins run outside the scheduling lock. Metrics use
a monotonic performance clock.

Normal completion callbacks run serially on the owned completion pump. They must
not wait for another Future from the same executor, even when worker slots are
free: the other result may need that blocked pump to become public. Slow callbacks
can delay result publication, acknowledgement and further dispatch. Use callbacks
to notify an external coordinator rather than wait for dependencies. A callback
added after completion, or invoked by external cancellation, may run on the caller
instead. Standard Future API compatibility does not imply identical stdlib
callback/thread scheduling or interpreter-exit behavior.

## Transparent online decisions

An LLM in a hot scheduling path would add network latency, nondeterminism, privacy
risk, and an unearned claim of safety. The built-in adaptive policy instead uses
explicit task hints, bounded per-workload exploration, and live queue/dispatch-slot
counts. With sufficient comparable uncontended observations from both backends,
it ranks an uncontended completion EWMA multiplied by a queue-pressure factor. A
switch margin suppresses small ranking changes. Without sufficient observations,
it uses hints and can divert an established backlog with a material pressure gap.
Explicit consent gates every process choice; exploration never replays a task.

An uncontended observation means that the selected backend had no outstanding
work when the task was accepted. It still includes cold startup, dispatch,
serialization, result transfer, and completion handling. Load counts occupied
slots and queued work, not CPU utilisation. These are auditable heuristics, not
intrinsic service-time estimates, optimality guarantees, or a statistically
rigorous contextual bandit. There are no automatic input features, causal
counterfactuals, calibrated uncertainty estimates, or drift guarantees. The
opt-in predictor adds exact caller-supplied feature buckets and qualitative
provenance labels. Offline tuning separately evaluates explicit configurations.

## Deliberate boundaries

- Logical and physical item-count limits do not bound argument bytes, results,
  blocked producer memory, process transport bytes, native allocations, or RSS.
- No task arguments/results are inspected or copied for profiling.
- Worker counts never change dynamically; backend infrastructure may start lazily.
- `process_safe` is supplied by a knowledgeable caller, not inferred from code.
- Process pickle execution is trusted-code execution, never an isolation sandbox.
- Metrics are in memory, coarse, and local to an executor.
- Extension factories are trusted Python code, not a plugin sandbox.
- Standard Future result, exception, wait and cancellation APIs apply; public
  Future identity is executor-owned, not backend identity.
- No preemption, dependency-deadlock prevention, callback isolation, or automatic
  retry is provided.
- Use a context manager or explicit `shutdown(wait=True)` to guarantee accepted
  work is drained before interpreter exit. Implicit stdlib teardown is not an
  equivalent contract: deferred dispatch can fail with `RuntimeError` if pools
  have already begun interpreter shutdown.

## Repository structure

`src/intelligent_futures/` is the installable core; `tests/` contains portable
unittest regression tests; `examples/` shows runnable patterns; `benchmarks/`
stores experiments and raw results; `docs/concepts.json` is the reviewable concept
index; `docs/appendices/` is the detailed design library. Changes to concepts do not
automatically change runtime behavior. A maintained library needs evidence gates,
not a growing number of undocumented switches.

## Optional application and transport layers (0.2.0)

```
trusted application -> framework.TaskRegistry -> framework.TaskService
                                                -> IntelligentExecutor (threads)
MCP client <-> integrations.mcp (official optional SDK, stdio)
                    -> same TaskService
```

The scheduling core retains its existing imports and zero runtime dependencies.
`framework` owns explicit registration, JSON contracts, bounded task bookkeeping,
and application lifecycle. `integrations.mcp` translates protocol tools into that
API; the SDK is imported only when the adapter is created or run. The framework
can be used without MCP and other future adapters can reuse it.

The service freezes registered definitions at construction. Clients can choose
only a registered name and validated JSON arguments. They cannot select a Python
module, import code, change policy, enable process execution, or register tasks.
The application author still controls trusted Python callables: this is not a
sandbox, authorization system, or multi-tenant service. One stdio connection has
access to every task/result on the supplied service.

Pending work and retained records have separate limits. Completed records remain
until explicitly forgotten; admission rejects rather than blocking a protocol
request or silently evicting uncollected results. Queued cancellation is handled
by a service wrapper so cancel/forget churn cannot release the physical admission
slot before that wrapper drains. Running Python code cannot be forcibly stopped.
JSON input/result byte limits constrain transmitted and retained JSON, not memory
allocated by a callable, the SDK's input parser, or total process RSS.

## Opt-in predictive foundation (0.3)

`prediction.py` owns immutable hardware/objective/sample/profile models and bounded
validated JSON transfer. `IntelligentExecutor(predictive=True)` measures successful
thread-call wall and current-thread CPU time at a configurable sampling interval.
A predictor uses compatible observed buckets or an explicitly unvalidated foreign
hardware prior to adjust admission, bounded by objective, worker and pending limits.
Its live-load adjustment uses queue/dispatch-slot demand and a bounded switching
history. The admission limit is global across both backends: a smaller prediction
can leave the other backend idle. Work-conserving scheduling is not guaranteed.
Pool objects and their private worker counts remain unchanged. Explicit routing
policies and process consent still constrain backend selection.

`map_batches` sends actual tuple chunks to a caller-supplied batch function. It
adapts future chunk sizes using callback-observed completion latency and never
replays failed items. `map_adaptive` changes only submission-window size; every
input remains a separate Future. Both are bounded streaming APIs with explicit
close/cancellation limitations, not hard byte/RSS bounds.

See [PREDICTIVE.md](PREDICTIVE.md) for confidence/provenance, metadata privacy,
non-controlled observational scaling, unavailable metrics, and source/target
hardware distinctions. Framework/MCP execution remains unchanged and thread-only.
