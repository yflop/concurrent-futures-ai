# Changelog


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
