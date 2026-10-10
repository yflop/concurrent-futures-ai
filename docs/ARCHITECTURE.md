# Architecture

```
caller -> condition-protected admission -> policy -> validated routing
                                                -> ThreadPoolExecutor
                                                -> ProcessPoolExecutor (optional)
          completion callback <- genuine backend Future
                 -> counters + bounded per-key EWMA profiles -> next choice
```

No scheduler thread, event loop, remote service, or generated code is required.
The underlying executors own task lifecycle and process transport. A single
condition lock guards admission, counts, profile updates, lifecycle state, and
policy choice. Worker callbacks decrement admission and notify waiting producers.
The scheduling lock is not held while waiting for worker-pool shutdown.

A submission is counted only after the backend accepts it. The done callback is
installed under the reentrant lock; if work already completed, Python immediately
invokes it. This prevents shutdown from overtaking callback registration. Metrics use a monotonic performance clock.

## Why lightweight online learning?

An LLM in a hot scheduling path would add network latency, nondeterminism, privacy
risk, and an unearned claim of safety. Here, the transparent learner is a small
baseline: after sufficient comparable successful observations from both backends,
it chooses the lower EWMA latency, occasionally explores the alternative, and
continues adapting. Explicit consent gates every process choice.

This is a useful experimental seam, not a statistically rigorous contextual
bandit implementation. It lacks uncertainty estimates, queue-length features,
size buckets, causal counterfactuals, failure-aware decisions, and drift tests.
The catalog describes how those could be added with explicit experiments.

## Deliberate boundaries

- Admission count bounds retained unfinished jobs, not argument byte size or RSS.
- No task arguments/results are inspected or copied for profiling.
- Worker counts never change dynamically; backend infrastructure may start lazily.
- `process_safe` is supplied by a knowledgeable caller, not inferred from code.
- Process pickle execution is trusted-code execution, never an isolation sandbox.
- Metrics are in memory, coarse, and local to an executor.
- Extension factories are trusted Python code, not a plugin sandbox.
- Error propagation/cancellation remain owned by stdlib Future implementations.

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
