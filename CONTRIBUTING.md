# Contributing

Small evidence-backed contributions are welcome. Open a focused issue before a
large policy, backend, or API change. This alpha has no stability guarantee yet.

1. Explain the workload, constraint, observable problem, and expected outcome.
2. Link or add a concept entry with status, mechanism, risks, and a falsifiable
   benchmark. “Planned” is the correct status for ideas without executable code.
3. Keep core dependencies in the standard library. Optional integrations should
   remain outside the hot path and explicit about data transmission.
4. Add deterministic tests for failures, cancellation, shutdown, saturation, and
   process spawn, not just happy-path throughput.
5. Compare sequential, thread, process, hinted, and adaptive baselines where
   relevant. Record platform, worker budgets, cold/warm state, repetitions, and
   full raw results. Publish regressions alongside wins.
6. Update API docs, examples, catalog maturity, and security implications.
7. Run `PYTHONPATH=src python -m unittest discover -s tests -v` and compileall;
   build and install a wheel into a clean environment before release.

Do not submit secrets, private workload data, generated benchmark numbers,
copy-pasted incompatible licensed code, or silently networked inference features.
Do not add retries to arbitrary callables: side effects may not be idempotent.

## Concept proposal checklist

- Stable unique ID and specific mechanism
- Implemented / experimental / planned status and code links when applicable
- Conditions where it helps and where it hurts
- Resource, privacy, cancellation, fairness, and failure consequences
- Reproducible success metric and baseline; explicit rejection criterion
- Sources with attribution and licensing notes if code is adapted

A concept count is not a quality metric. Prefer fewer complete proposals over
synonyms, marketing language, or filler. Public PRs should be draft until their
verification record is complete. Maintainers choose release compatibility rules.

After editing docs/concepts.json, run `python tools/sync_catalog.py`. CI and
unit tests check that the packaged catalog matches its canonical source.

For framework changes also test cancel/forget churn while a worker is blocked,
strict JSON boundaries, sanitized failures, capacity reclamation, and shutdown.
For MCP changes install `.[mcp]` and run `tests.test_mcp` against the actual SDK
stdio client/server subprocess; core-only optional skips are not an MCP pass.
Keep the adapter separate from `framework` and avoid importing the SDK in the core.
