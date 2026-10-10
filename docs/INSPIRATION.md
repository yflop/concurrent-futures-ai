# Inspiration and provenance

This is an original Python project inspired by the extensibility and execution-design ideas in [Activepieces](https://github.com/activepieces/activepieces). It is not an Activepieces component, port, integration, endorsed project, or compatible workflow engine. No Activepieces code has been copied into this repository. The implementation uses Python's standard-library executors; the concept catalog is this project's own design backlog.

## What was studied

The public repository and official documentation were reviewed on **2026-10-10**. These are live upstream pages, not a vendored or commit-pinned specification. Upstream behavior may change independently of this project.

| Primary source | Observation used here | Translation into this project |
| --- | --- | --- |
| [Repository README](https://github.com/activepieces/activepieces) | Activepieces presents an extensible TypeScript pieces ecosystem and composable automation. | Keep the Python scheduling core small; give ideas stable catalog identities and policies explicit extension points. |
| [Building pieces](https://www.activepieces.com/docs/build-pieces/building-pieces/overview) | Contributions can add focused reusable pieces. | Prefer focused, testable policy contributions over a single opaque optimizer. |
| [Architecture](https://www.activepieces.com/docs/install/architecture) | The application, workers, engine, and sandbox have distinct responsibilities. | Separate admission, policy decisions, backend execution, and measurement in the design. This is an analogy, not a claim of architectural equivalence. |
| [Workers](https://www.activepieces.com/docs/install/architecture/workers) | Workers consume queued jobs; deployment concurrency and isolation choices affect resource behavior. | Describe capacity limits precisely, and distinguish local pool sizing from distributed scaling. |
| [Crash recovery](https://www.activepieces.com/docs/install/guarantees/crash-recovery) | Recovery has explicit checkpoint, persistence, and repeated-in-flight-step boundaries. | Document that the base executor has no crash durability, and place persistence and retry concepts in the planned catalog. |
| [Execution isolation](https://www.activepieces.com/docs/install/guarantees/execution-isolation) | Isolation depends on an explicit execution environment and configuration. | Do not advertise a process pool or policy registry as a hostile-code sandbox. |

These observations motivate the design; they do not establish that an optimization improves performance. In particular, a workflow platform's queueing, sandboxing, durable state, connectors, or user interface do not appear automatically when a Python executor is made extensible.

## What “intelligent” means in this repository

The prototype records successful backend submit-to-completion latency by a caller-chosen task key. An experimental policy uses an exponentially weighted moving average (EWMA), plus deterministic exploration, to compare eligible thread and process routes. This is small, inspectable online adaptation. It does not inspect source code to infer intent, train a neural network, call an LLM, send code or profiles to an inference service, or prove an optimal route.

Eligibility remains explicit: the caller must permit process execution and configure process workers. A learned preference cannot grant that permission. For fully predictable routing, use the `threads` or `hints` policy.

## Licensing and contributions

The upstream README distinguishes community and enterprise licensing. This repository does not inherit upstream licensing merely by citing design inspiration. Its own license file governs the original code here. A future contributor who imports third-party source, assets, or dependencies must review their actual licenses, preserve required notices, and document provenance before merging. This paragraph is a contribution requirement, not a legal interpretation of any particular upstream file.

## Primary Python references

- [Python 3.13 concurrent.futures](https://docs.python.org/3.13/library/concurrent.futures.html): baseline executor and future semantics.
- [Python 3.13 multiprocessing](https://docs.python.org/3.13/library/multiprocessing.html): process contexts, importability, and data transfer.
- [Current concurrent.futures documentation](https://docs.python.org/3/library/concurrent.futures.html): version-specific APIs, including additions absent from older runtimes.
- [PEP 703](https://peps.python.org/pep-0703/): background for the planned free-threaded-runtime work.

The appendices and catalog primarily describe original project contracts, experiments, and proposals. A reference on a planned concept identifies relevant background; it never changes its status to implemented.

## Version 0.2.0: the two requested commits

The exact public diffs were inspected, not inferred from commit titles:

- [90ba4b23ad](https://github.com/activepieces/activepieces/commit/90ba4b23ad)
  renames `@activepieces/pieces` to `@activepieces/framework`, relocates the source
  under `packages/pieces/framework`, adds its package/build configuration, and
  updates imports. This motivates an explicit reusable framework boundary here;
  it does not establish that Activepieces uses Python futures.
- [f78edf10fc](https://github.com/activepieces/activepieces/commit/f78edf10fc)
  moves seven existing MCP files from `app/ee/mcp` to `app/mcp` and adjusts import
  paths in those files and two callers (nine files, 11 additions/11 deletions).
  It is a folder relocation, not the introduction of MCP, new authentication,
  new tools, or a protocol implementation.

Our original translation is a dependency-free registered-task framework plus an
optional MCP transport adapter in a separate `integrations` package. No upstream
code is copied and no Activepieces interoperability is claimed. MCP behavior is
implemented using the [official Python SDK](https://github.com/modelcontextprotocol/python-sdk)
and its [low-level server documentation](https://py.sdk.modelcontextprotocol.io/).
The SDK is an optional dependency, not vendored source. This release does not
change routing performance and does not reinterpret existing benchmark results.
