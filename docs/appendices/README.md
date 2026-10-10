# Technical appendices

These appendices are a design handbook for extending a small, auditable executor. They distinguish the code that exists from experiments and future possibilities. Start with the runnable repository examples; use these documents to understand limits, evaluate changes, and contribute responsibly.

1. [A: Concept catalog](A-concept-catalog.md): the index of 61 mechanisms and the meaning of implementation status.
2. [B: Architecture and API contracts](B-architecture-and-contracts.md): admission, policies, actual backend futures, and measurement boundaries.
3. [C: Learning, evidence, and tradeoffs](C-learning-and-tradeoffs.md): exactly what EWMA learns, exploration costs, and why adaptation can lose.
4. [D: Benchmark design](D-benchmark-methodology.md): reproducible experiments, fair baselines, falsification, and interpretation.
5. [E: Cancellation and lifecycle](E-cancellation-and-lifecycle.md): state transitions, shutdown races, timeouts, and nested-submission hazards.
6. [F: Process portability](F-process-portability.md): spawn, serialization, state sharing, native libraries, and trust boundaries.
7. [G: Extension roadmap](G-extension-roadmap.md): staged contribution gates, policy extensions, and maintaining an expanding catalog.
8. [H: Workload recipes](H-workload-recipes.md): concrete design choices for downloads, pure CPU jobs, streaming transforms, and services.

The machine-readable catalog is [`../concepts.json`](../concepts.json). The source is the authority for the current implementation; if a document describes a planned concept, it is not an available option. All example settings are illustrative rather than a claim of optimal tuning.

## Current boundary

Implemented building blocks include bounded admission, explicit eligibility and hint routing, profile records with bounded key retention, genuine standard-library futures, cancellation through those futures, an explicit policy registry, and ordered bounded streaming. The EWMA adaptive policy is executable but experimental. The constructor currently defaults to it; choose `policy="threads"` or `policy="hints"` when you do not want adaptive routing.

There is no durable task store, distributed execution, automatic retry, LLM integration, hostile-code sandbox, execution deadline enforcement, automatic worker resizing, or claim of universal speedup. Those are independent design problems, several represented in the planned catalog.
