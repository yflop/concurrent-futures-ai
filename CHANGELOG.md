# Changelog

## 0.5.1 — unreleased Beta candidate

- Fail fast with `BackpressureError` when a submission from this executor's own
  task or completion context would block on bounded admission. Successful
  reentrant submissions and external admission timeout behavior are unchanged.
  This conservative guard replaces a possible indefinite wait; it does not
  prevent arbitrary task dependencies or callback-result waits.
- Expose the fixed-cardinality `reentrant_rejected` count, also included in
  aggregate `rejected`, without retaining task identities or adding queues.
- Add paired operational compatibility probes and focused regressions; retain
  the documented serial-callback publication and explicit-shutdown constraints.
- Record a preregistered four-hour default-adaptive endurance run on one Linux
  host, with 6,627,904 measured workload jobs, sixteen separate health checks,
  failure-inclusive diagnostics, bounded-state/resource checks and final cleanup.
  Preserve the interrupted prior attempt and its unknown terminal state. Short
  predictive probes do not establish multi-hour predictive endurance.
- Retain the sustained warm-workload comparison's negative result: no supported
  advantage against tuned stdlib/hybrid alternatives. All evidence remains
  bounded to its measured workload and host, without production certification.
- Add raw-evidence reconciliation and lossless-numeric public archive tooling,
  with source/run identity checks and failure/mutation regressions.

See [operational endurance evidence](docs/OPERATIONAL-ENDURANCE-EVIDENCE.md),
[compatibility limits](docs/OPERATIONAL-COMPATIBILITY.md), and
[sustained performance evidence](docs/ADOPTION-SUSTAINED-EVIDENCE.md).

## 0.5.0 — Beta

- Conservative cross-platform numerical margin ties; evaluator revision v2 invalidates v1 reuse. Original measurements are preserved with a zero-task decision recomputation proof.

- Add opt-in evidence-gated adoption for caller-declared repeatable fixtures:
  independent bounded stdlib/intelligent searches followed by frozen, paired
  held-out comparisons against both fixed and tuned stdlib baselines.
- Require an exact median paired log-speedup lower bound above the declared
  practical margin, complete correctness/resource observations, noise and
  observed-regression gates, and empirical amortization of discovery cost.
  Results can recommend the exact measured mode, retain stdlib, or be inconclusive.
- Add fresh context-managed callable fixtures with equivalent batching and
  submission windows, cold pool-lifecycle timing, output validation, and explicit
  process-safety declarations. Ordinary submitted work is never captured/replayed.
- Add deterministic `ProcessPolicy`, opt-in bounded routing traces and fixed-category
  routing counters. Process consent remains per task; traces omit workload data.
- Add workload/runtime/hardware/revision/expiry guards on in-memory adoption
  recommendations, renewed process consent, and evidence-only JSON export.
  No report deserialization or automatic deployment is provided.
- Add one-factory incremental adoption and explicit pre-submission rollback
  examples. Switching executors never retries failed application tasks.

See the [adoption contract](docs/ADOPTION.md) and the separate
[0.5 release evaluation record](docs/ADOPTION-EVIDENCE.md). All negative and
inconclusive observations belong in the evidence. Synthetic protocol tests are
not performance measurements. The 0.4 reports below retain their original scope
and outcomes. Beta status does not assert a universal speedup, a globally best
configuration, hard resource enforcement, or production certification.


## 0.4.0 — Beta

- Purpose-led workload-aware configuration evaluation and selection: backend,
  worker count and batch size are evaluated under caller-declared objectives
  and resource constraints. Ordinary submitted work is never silently replayed.
- Budgeted paired-repeat successive halving with robust median selection,
  explicit exclusions, comparable finalist scores and separately reported
  search costs; exhaustive calibration remains available.
- Removable executor-owned queues, physical scheduling-item bounds, cancellation
  acknowledgement, shutdown coordination and bounded completion handling.
  Genuine stdlib Future APIs retain explicitly documented scheduling differences.
- Isolated, synchronized load-aware policy state and occupancy-sensitive routing;
  predictive admission follows the selected backend.
- Saved configuration schema 2 checks scheduler/strategy compatibility in addition
  to workload, hardware and Python identity, with renewed process consent.
- Eleven original local-fixture workload replicas with real optional libraries,
  image/PDF OCR/video operations, collision-safe deduplication, graph/SQLite
  oracles, and loopback-only networking. No upstream interoperability claim.
- Explicit bounded offline configuration calibration and frozen hold-out
  inspection; process candidates still require caller consent. No silent replay.
- Fair backend/worker/batch benchmark grid, separate calibration/held-out seeds,
  cold/warm timing, p95, throughput, errors, sampled process-tree RSS and raw results.
- Optional workload dependency ranges, exact tested Python dependency pins,
  provenance/license notes, dedicated Linux workload CI and acceptance gates.
- Strict configuration-only JSON reuse with workload/hardware/Python checks and
  renewed process consent; reuse performs no calibration callbacks.
- Additional adversarial lifecycle, admission, cancellation and migration checks.

Configuration quality is measured against the eligible candidates actually
tested, with independent held-out inputs and explicit discovery costs. The
[configuration-quality report](docs/CONFIGURATION-QUALITY.md) retains failed
resource/correctness scenarios and comparisons with independently tuned stdlib.
Production certification, ArcGIS integration, task preemption and hard memory
enforcement are outside the implemented contract.

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
