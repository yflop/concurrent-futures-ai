# Intelligent Futures

Intelligent Futures is a workload-aware execution framework for Python that automates the evaluation and selection of concurrency configurations. It combines execution policies, workload characteristics, runtime measurements, and adaptive optimisation to help identify effective thread and process execution strategies under defined resource constraints.

Version 0.5 is a **Beta workload-aware execution framework**. Beta is not
Production/Stable or a deployment certification. Individual adaptive/predictive
capabilities retain their documented experimental status. The release workflow
requires source verification, built-artifact checks and exact-commit CI; maturity
labels do not substitute for those records.

Calibration is explicit: ordinary submitted work is never silently replayed.
See [configuration selection](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/TUNING.md),
[budgeted optimisation](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/BUDGETED-TUNING.md),
[configuration reuse](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/TUNING-PERSISTENCE.md),
[configuration-quality evidence](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/CONFIGURATION-QUALITY.md), and
[historical workload evidence](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/PRODUCTION-EVIDENCE.md).

Public repository: [yflop/concurrent-futures-ai](https://github.com/yflop/concurrent-futures-ai).

## Start in a minute

Python 3.10 or newer; the core has no runtime dependencies. Install the optional
MCP adapter with `python -m pip install "intelligent-futures[mcp]"`.

```sh
python -m pip install -e .
python -m unittest discover -s tests -v
python examples/profiling.py
```

```python
from intelligent_futures import IntelligentExecutor, TaskHints

def square(n):
    return n * n

if __name__ == "__main__":  # required when using spawn-based processes
    with IntelligentExecutor(max_workers=4, process_workers=2,
                             max_pending=32, policy="hints") as executor:
        ordinary = executor.submit(square, 7)  # conservative thread routing
        cpu = executor.submit_task(
            square, 9,
            hints=TaskHints(kind="cpu", key="square", process_safe=True),
        )
        print(ordinary.result(), cpu.result())
    print(executor.snapshot())
```

Tiny tasks like `square` generally do not benefit from process overhead. This
example demonstrates semantics, not a performance recommendation.

## Evidence-gated adoption and simple rollback (0.5)

Use one factory boundary to try the executor on new work, keeping the same
`submit`/`Future`/`wait` interfaces. The thread-only
[incremental example](https://github.com/yflop/concurrent-futures-ai/blob/main/examples/incremental_adoption.py) runs with either engine:

```sh
python -m examples.incremental_adoption --engine stdlib
python -m examples.incremental_adoption --engine intelligent
python -m examples.evaluate_adoption --save adoption-demo.json
```

Choose `stdlib` at the same factory before the next fresh batch to roll back.
For adaptive/predictive adoption, also restore the original stdlib configuration.
Drain existing work explicitly; never retry arbitrary failed tasks as part of
rollback. Normal completion callbacks share a serial pump: do not wait for
another same-executor Future inside a callback. Both example paths use the same
caller-side submission window; `max_pending` is an item count, not an RSS limit.

The opt-in `evaluate_workload` API runs only explicitly repeatable fixtures.
It independently tunes both engine families, freezes the selected intelligent
configuration, then compares it against both the fixed and tuned stdlib baselines
on untouched paired blocks. Defaults are 3 training and 24 held-out blocks,
a 5% practical margin for the median paired log-speedup, correctness/resource
gates, and empirical accounting for evaluation cost over declared future reuse.
The short runnable demonstration uses 8 held-out blocks and provides no
production speedup evidence. `retain_stdlib` and `inconclusive` are normal results.

Recommendations concern the exact measured configuration and scope, require
compatibility/expiry checks, and renew process consent. Saved JSON is evidence,
not an executable deployment profile. Read the [adoption guide](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/ADOPTION.md),
[new APIs](docs/API.md#evidence-gated-adoption-apis-05-beta), and
[0.5 release evaluation record](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/ADOPTION-EVIDENCE.md). Earlier workload
reports remain historical evidence for their stated versions; synthetic tests
verify the protocol rather than performance. No universal speedup, globally best
configuration, or hard future memory guarantee is claimed.

## Predictive foundation and actual batching (0.3)

Opt in with `IntelligentExecutor(predictive=True)`: successful thread calls collect
wall/current-thread CPU measurements, reusable validated JSON profiles guide cold
starts, and admission adapts within hardware/objective/worker budgets.
`map_batches(batch_fn, iterable)` submits actual bounded tuple chunks and adjusts
chunk sizes from measured completion time, without replaying work. See
[the predictive guide](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/PREDICTIVE.md) and `examples/predictive_batches.py`.
Cross-machine learned transfer, per-task RSS enforcement, and process runtime
profiling are not implemented. Tiny-workload measurements show overhead; this
experimental foundation does not promise a speedup.

## Eleven executable workload replicas

[The workload guide](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/WORKLOADS.md) supplies real local HTTP, images, graph
routing, cached HTTP, range downloads, streaming pipelines, PDF OCR,
collision-safe deduplication, SQLite, loopback discovery, and video-frame harnesses.
They use actual libraries and correctness oracles. They are original workload
replicas, not certified integrations with the referenced applications; ArcGIS
itself remains untested. Optional dependencies and media tools stay outside the core.
The [current configuration-quality report](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/CONFIGURATION-QUALITY.md)
preserves the original **7/11** workload outcome, including failed cases, and
separates later revised-source results. Earlier representative, predictive and
sustained checks remain in the [historical evidence report](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/PRODUCTION-EVIDENCE.md).

[Explicit offline calibration](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/TUNING.md) selects backend/worker/batch and
profiling settings from caller-supplied safe calibration fixtures. It never replays
ordinary submitted tasks. A bounded benchmark search compares the same algorithms against
separately tuned standard-library executors on held-out seeds. It records cold/warm
costs, p95, throughput, errors, sampled memory, and negative results. Read
[current measured evidence](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/CONFIGURATION-QUALITY.md) and
[acceptance gates and limits](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/WORKLOADS.md#production-acceptance-gates) before
using smoke measurements as deployment evidence. Planned catalog entries remain
planned; this work does not implement every optimization concept.

## What works today

- Genuine executor-owned standard-library `Future` objects, `wait`,
  `as_completed`, `map`, exceptions, callbacks and queued cancellation;
  backend Future identities and stdlib callback scheduling are not preserved
- Independent thread/process worker budgets; processes disabled by default
- `max_pending` bounds both **logical unfinished accepted tasks** and
  **physical owned-queued-plus-dispatched items** across both backends
- Removable per-backend FIFO queues and worker-bounded dispatch; cancelled
  dispatched thread work retains its physical slot until acknowledgement
- Blocking backpressure, optional admission timeout, and shutdown wakeup
- Bounded per-key/backend latency profiles, failures, and aggregate counters
- Safe hint routing, thread-only mode, custom policies, and a policy factory registry
- Experimental load-aware EWMA routing with bounded per-key/kind exploration
  and switching margins
- Ordered `map_bounded` for streaming inputs and bounded retained results
- A dependency-free registered-task framework with bounded JSON task records
- Optional local stdio MCP tools, restricted to explicitly registered callables
- Runnable examples, reproducible baseline benchmarks, tests, packaging, and CI

## Safety first

`submit(fn, *args, **kwargs)` keeps the standard keyword contract, including a
function argument named `hints`. Use `submit_task` to reserve the scheduling
`hints` keyword. Process routing requires both a configured process pool and
`process_safe=True`, even from a custom policy. The flag is **your assertion**:
functions and arguments must be pickleable, importable, and semantically safe in
a separate process. Do not rely on mutated thread-shared globals across processes.

No retries, speculative duplicates, hard timeouts, task killing, distributed
execution, live pool resizing, or remote AI calls are implemented. Predictive mode
can adjust admission within fixed worker capacities.
Cancellation never stops a running callable. Process Futures commit to `RUNNING`
at dispatch, before the child callable necessarily starts. Use a context manager
or explicit `shutdown(wait=True)` to guarantee accepted work drains before
interpreter exit. Omitted shutdown can settle deferred submissions with
`RuntimeError`; implicit stdlib teardown equivalence is not promised.

**Normal completion callbacks run serially on one owned completion pump. Never
wait for another Future from this executor inside a callback, even when worker
slots are free.** The other result may require that blocked pump for publication.
Callbacks added after completion or triggered by external cancellation may run on
the caller. Waiting shutdown from the executor's task/pump context raises
`RuntimeError`. Genuine stdlib Future APIs do not imply identical callback/thread
scheduling. Keep callbacks short and notify an external coordinator.

Blocking nested submissions into a saturated executor can also deadlock; there is
no general dependency-deadlock prevention or callback isolation. Orchestrate from
outside worker tasks, and use a finite `submit_timeout` where producer blocking
must be bounded. No item-count limit is a hard byte/RSS cap.

## Optional MCP integration (0.2.0)

Expose a trusted allowlist of Python tasks to an MCP client without changing the
executor API. Task submission returns an ID immediately; tools inspect results,
cancel queued work, forget completed records, and read metrics and concepts.
The provided runner uses stdio and opens no listening port.

See [the MCP setup and safety guide](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/MCP.md),
[framework API](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/FRAMEWORK.md), and [changelog](https://github.com/yflop/concurrent-futures-ai/blob/main/CHANGELOG.md).
The SDK is optional; registered code still runs with your Python process permissions.

## Choose the right policy

| Policy | Behavior | Recommended use |
| --- | --- | --- |
| `threads` | Always threads | Closures, shared state, general I/O |
| `hints` | CPU + explicit process consent routes to processes | Known workloads, predictable routing |
| `adaptive` (default) | Eligible workloads use hints, uncontended completion EWMA, queue/dispatch pressure and bounded exploration | Controlled experiments with comparable task keys |

Full latency profiles measure accepted submission to completion, including owned
queueing, dispatch/startup and serialization, **excluding admission wait**. The
executor's load-aware adaptive path uses the uncontended subset, accepted when the
selected backend had no outstanding work, plus current queue/dispatch-slot
pressure. Uncontended measurements still include startup and serialization; load
counts occupied slots and queued items, not CPU utilisation. Routing and switching
margins are heuristics, without causal or optimality guarantees. Errors do not train
successful latency. Share keys only across comparable work.

Exploration counters and incumbent choices are bounded per `(key, kind)`.
Exact built-in `AdaptivePolicy` instances are configuration templates: every
executor receives a fresh independent exploration/switching session. Custom
subclasses retain supplied state unless they override `for_executor`; shared
custom policies must synchronize themselves or return independent sessions.
Executor-local EWMA statistics are bounded and evicted. Separately, the opt-in
predictor supports explicit strict JSON profile export/import, and offline
calibration supports version-checked JSON configuration reuse; see
[prediction](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/PREDICTIVE.md) and
[configuration persistence](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/TUNING-PERSISTENCE.md).
Predictive admission is global and can leave another backend idle; it is not a
work-conserving scheduling guarantee.

## Browse the concept library

```sh
python -m intelligent_futures queue --status planned
python -m intelligent_futures --status experimental --json
```

The bundled `ConceptRegistry` validates unique IDs, status labels, and acyclic
dependencies. Catalog content never imports or executes plugin code.

## API and documents

- [API and semantics](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/API.md)
- [Architecture](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/ARCHITECTURE.md)
- [Concept library and appendices](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/appendices/)
- [Machine-readable concept catalog](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/concepts.json)
- [Activepieces inspiration and attribution](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/INSPIRATION.md)
- [Benchmarks](https://github.com/yflop/concurrent-futures-ai/blob/main/benchmarks/) and [verification record](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/VALIDATION.md)
- [Contributing](https://github.com/yflop/concurrent-futures-ai/blob/main/CONTRIBUTING.md), [Security](https://github.com/yflop/concurrent-futures-ai/blob/main/SECURITY.md), [Publishing](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/PUBLISHING.md)

Appendices separate implemented mechanisms from experimental and planned work.
Ideas are extension proposals, not hidden features. Benchmark raw data is the
source of performance claims; a measured loss is a useful result too.

## Development

```sh
PYTHONPATH=src python -m unittest discover -s tests -v
python tools/verify.py --examples
python -m compileall -q src examples benchmarks tests
python -m pip wheel . --no-deps -w dist
```

See benchmark help for workload sizes, repeated cold/warm comparisons, and JSON
output. A hybrid executor may use more total workers than a single baseline pool;
compare resource budgets before interpreting a timing ratio.

## Historical measured result

The included five-repeat reference run predates the owned-FIFO/load-aware
implementation and did not beat the best stdlib pool:
for warm CPU, synthetic I/O, and mixed batches, adaptive took approximately
548, 164, and 344 ms versus 256, 122, and 196 ms for the best measured stdlib
configuration. Worker allocation and hint information differ; see the full
[validation record](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/VALIDATION.md). These historical observations do not establish
current configuration quality or a default performance upgrade. Current
configuration-selection evidence and its failed cases are reported separately in
[configuration quality](https://github.com/yflop/concurrent-futures-ai/blob/main/docs/CONFIGURATION-QUALITY.md).

## License

Original code and documentation are MIT licensed. Activepieces is a conceptual
inspiration, not a dependency or copied implementation. Third-party linked
material retains its own license.

### Cancellation churn and memory

`max_pending` separately caps logical unfinished work and physical retained
scheduling items (owned queued plus dispatched), with each backend's dispatches
also capped by its worker budget. Cancelling owned queued work removes its payload
without adding a backend tombstone. Successful dispatched thread cancellation
releases logical capacity but keeps the physical slot until acknowledgement.

These item-count bounds do not constrain payload/result bytes, blocked-producer
memory, caller-retained Futures, native allocations, transport bytes, or RSS.
Earlier churn reports describe the historical direct-submission implementation;
they are not the current queue contract or a hard memory guarantee.
