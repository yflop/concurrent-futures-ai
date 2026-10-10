# Technical appendices

These appendices are a design handbook for extending a small, auditable executor. They distinguish the code that exists from experiments and future possibilities. Start with the runnable repository examples; use these documents to understand limits, evaluate changes, and contribute responsibly.

1. [A: Concept catalog](A-concept-catalog.md): the mechanism index and the meaning of implementation status.
2. [B: Architecture and API contracts](B-architecture-and-contracts.md): logical/physical admission, policies, executor-owned standard Futures, and measurement boundaries.
3. [C: Learning, evidence, and tradeoffs](C-learning-and-tradeoffs.md): exactly what EWMA learns, exploration costs, and why adaptation can lose.
4. [D: Benchmark design](D-benchmark-methodology.md): reproducible experiments, fair baselines, falsification, and interpretation.
5. [E: Cancellation and lifecycle](E-cancellation-and-lifecycle.md): state transitions, shutdown races, timeouts, and nested-submission hazards.
6. [F: Process portability](F-process-portability.md): spawn, serialization, state sharing, native libraries, and trust boundaries.
7. [G: Extension roadmap](G-extension-roadmap.md): staged contribution gates, policy extensions, and maintaining an expanding catalog.
8. [H: Workload recipes](H-workload-recipes.md): concrete design choices for downloads, pure CPU jobs, streaming transforms, and services.

The machine-readable catalog is [`../concepts.json`](../concepts.json). The source is the authority for the current implementation; if a document describes a planned concept, it is not an available option. All example settings are illustrative rather than a claim of optimal tuning.

## Current boundary

Implemented building blocks include logical and physical item-count bounds, removable per-backend FIFO queues, worker-bounded dispatch, explicit eligibility and hint routing, bounded profiles, executor-owned standard-library Futures, an explicit policy registry, and ordered bounded streaming. The load-aware EWMA adaptive policy is executable but experimental. Load means queued work and occupied dispatch slots, not CPU utilisation; uncontended latency includes startup/serialization. Configuration evaluation and selection are the framework's purpose, without an optimality or same-configuration speedup guarantee. The constructor currently defaults to it; choose `policy="threads"` or `policy="hints"` when you do not want adaptive routing.

Use a context manager or explicit `shutdown(wait=True)` to guarantee accepted work drains before interpreter exit. Omitted shutdown may settle deferred submissions with `RuntimeError`; implicit stdlib teardown equivalence is not promised.

Normal completion callbacks run serially on one owned pump; do not wait for
another Future from that executor in a callback even with free worker slots.
Standard Future APIs do not imply identical stdlib callback/thread scheduling.

There is no byte/RSS bound, callback isolation, dependency-deadlock prevention, durable task store, distributed execution, automatic retry, LLM integration, hostile-code sandbox, execution deadline enforcement, automatic worker resizing, or claim of universal speedup. Those are independent design problems, several represented in the planned catalog.
