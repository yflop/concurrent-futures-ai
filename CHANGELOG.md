# Changelog

## 0.3.0

- Opt-in hardware-aware predictive admission with explicit objectives and unchanged
  default executor behavior/process consent. No live stdlib pool resizing.
- Lightweight successful thread wall/CPU measurements, observational scaling
  buckets, and bounded versioned JSON profile transfer with numeric feature summaries.
- Actual adaptive tuple-chunk execution through `map_batches`, plus separately
  documented adaptive submission windows. No duplicate execution or automatic retries.
- Seeded stress/performance harness and regression tests; observed overhead is
  reported alongside limitations. No cross-machine learned speedup or RSS guarantee.


## 0.2.0

- Add a standalone, standard-library-only task registration and lifecycle layer.
- Add optional MCP stdio tools for explicitly registered tasks, results,
  best-effort queued cancellation, cleanup, metrics, and concept discovery.
- Bound pending work, retained records, JSON arguments, and serialized results;
  reject unknown tasks, invalid inputs, saturation, and unsafe result values.
- Keep the existing Executor/Future API and dependency-free core unchanged.
- Add protocol integration tests, isolated optional-dependency CI, examples,
  security guidance, and precise attribution to the two requested upstream diffs.

This is an additive alpha release, not an authentication layer, code sandbox,
durable job system, or claim of improved scheduler performance.

## 0.1.0

Initial bounded Executor, explicit policies, concept catalog, benchmark harness,
examples, and package publication.
