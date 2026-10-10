# Changelog

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
