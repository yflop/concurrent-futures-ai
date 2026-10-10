# Intelligent Futures

A practical, extensible laboratory for better Python concurrency: a bounded
`concurrent.futures.Executor`, transparent workload measurements, an experimental
online scheduler, and a growing library of optimization concepts.

**Status: alpha research prototype.** No universal speedup is promised. The
“intelligence” is an auditable EWMA latency learner with controlled exploration,
not an LLM, code generator, or automatic proof of process safety.

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

## What works today

- Standard-library `Future` objects, `wait`, `as_completed`, `map`, exceptions,
  callbacks, and best-effort queued cancellation
- Independent thread/process worker budgets; processes disabled by default
- `max_pending` bounds **all unfinished accepted tasks**, queued plus running
- Blocking backpressure, optional admission timeout, and shutdown wakeup
- Bounded per-key/backend latency profiles, failures, and aggregate counters
- Safe hint routing, thread-only mode, custom policies, and a policy factory registry
- Experimental EWMA latency selection with deterministic exploration
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
execution, automatic worker resizing, or remote AI calls are implemented.
Cancellation never stops a running callable. Context-manager exit waits for work.
Blocking nested submissions into a saturated executor can deadlock: orchestrate
from outside worker tasks, or use a finite `submit_timeout` and handle failure.

## Optional MCP integration (0.2.0)

Expose a trusted allowlist of Python tasks to an MCP client without changing the
executor API. Task submission returns an ID immediately; tools inspect results,
cancel queued work, forget completed records, and read metrics and concepts.
The provided runner uses stdio and opens no listening port.

See [the MCP setup and safety guide](docs/MCP.md),
[framework API](docs/FRAMEWORK.md), and [changelog](CHANGELOG.md).
The SDK is optional; registered code still runs with your Python process permissions.

## Choose the right policy

| Policy | Behavior | Recommended use |
| --- | --- | --- |
| `threads` | Always threads | Closures, shared state, general I/O |
| `hints` | CPU + explicit process consent routes to processes | Known workloads, predictable routing |
| `adaptive` (default) | Eligible workloads can explore both backends and learn EWMA completion latency | Controlled experiments with comparable task keys |

The learner optimizes observed submission-to-completion latency, including pool
queueing and serialization, **excluding admission wait**. It cannot identify
causal backend advantages under arbitrary changing load. Errors are counted but
do not train latency. Share keys only across comparable work. Profiles are
executor-local, not persisted, and evicted with an LRU bound. The policy's
exploration counter is global to that policy instance. Keep stateful policy
instances private to one executor.

## Browse the concept library

```sh
python -m intelligent_futures queue --status planned
python -m intelligent_futures --status experimental --json
```

The bundled `ConceptRegistry` validates unique IDs, status labels, and acyclic
dependencies. Catalog content never imports or executes plugin code.

## API and documents

- [API and semantics](docs/API.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Concept library and appendices](docs/appendices/)
- [Machine-readable concept catalog](docs/concepts.json)
- [Activepieces inspiration and attribution](docs/INSPIRATION.md)
- [Benchmarks](benchmarks/) and [verification record](docs/VALIDATION.md)
- [Contributing](CONTRIBUTING.md), [Security](SECURITY.md), [Publishing](docs/PUBLISHING.md)

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

## Measured result

The included five-repeat reference run did not beat the best stdlib pool:
for warm CPU, synthetic I/O, and mixed batches, adaptive took approximately
548, 164, and 344 ms versus 256, 122, and 196 ms for the best measured stdlib
configuration. Worker allocation and hint information differ; see the full
[validation record](docs/VALIDATION.md). Treat this as an extensible research
baseline with tested contracts, not a performance upgrade by default.

## License

Original code and documentation are MIT licensed. Activepieces is a conceptual
inspiration, not a dependency or copied implementation. Third-party linked
material retains its own license.

### Cancellation churn and memory

The admission limit is not a strict physical queue or memory bound. Cancelling a
queued Future frees logical admission immediately, but stdlib pools can retain
its cancelled work item and arguments until a worker drains the queue. Repeated
submit/cancel loops behind blocked workers can therefore accumulate tombstones.
Rate-limit producers and cancellation churn; do not treat this executor as a
hard memory limiter. A removable bounded physical queue is planned work.
