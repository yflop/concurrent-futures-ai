# Historical 0.4 candidate workload evidence

**Historical snapshot, 2026-10-10. This is not the current release-verification or
configuration-quality summary.** The representative runs below were recorded at
09:19 and 09:33 UTC, and the frozen-selection recheck at 09:52 UTC, against their
own recorded source hashes and dirty working-tree snapshots. References to
“final source,” all-eleven passing coverage and 288 tests describe that earlier
candidate only. They do not validate later owned-FIFO/load-aware/runtime changes,
schema-v2 persistence, revised media paths, or the original integrated optimiser
matrix, which achieved **7/11** planned workload means within its 10% goal and
retained OCR/video failures plus cached-HTTP/discovery performance misses.

Read [current configuration quality and operating limits](CONFIGURATION-QUALITY.md),
the [original integrated results, including failures](../benchmarks/results/optimizer/primary-v2/README.md),
and their separately linked revised-source follow-ons for the current evidence.
Later successful checks do not replace failures in an earlier denominator. The
raw historical files linked below are preserved unchanged.

This report records the earlier measured execution and real-workload coverage
for a 0.4 candidate. “Production hardening” describes work performed; it does not
certify deployment readiness. Current maturity/classification and release gates
must be established by final verification of the intended release source.

## Capability in the historical candidate

- Original runnable replicas for eleven observed workload patterns, with genuine
  Pillow, Poppler/Tesseract, FFmpeg, Requests/requests-cache, NetworkX, SQLite,
  local file processing and loopback TCP/HTTP operations.
- Heterogeneous representative fixture profiles in addition to small smoke tests.
- Explicit offline backend/worker/batch/profiling-mode calibration. Caller-owned
  calibration fixtures may be evaluated repeatedly; ordinary tasks never replay.
- Frozen held-out evaluation, correctness oracles, complete raw measurements,
  sampled RSS and POSIX lifecycle CPU, p95, throughput, errors, configuration cost,
  and retrospective progress toward the best observed calibration candidate.
- Runtime lifecycle, process-consent, cancellation, backpressure, physical-queue
  limits and API migration checks. Existing core limitations remain documented.

## What this does not establish

The fixtures are generated and bounded; they are not a production customer corpus.
Only one actual machine/environment is measured. ArcGIS and the cited upstream
applications are not integration-tested. The sixteen/hundred/thousand-node graphs
are original coordinate topologies, not road data. An application-specific SLO,
long-duration operational history, security audit, hostile-input sandbox, hard
resource enforcement and universal scheduler advantage are not established.

The online predictive policy and offline configuration tuner are separate features.
A fixed-admission result is evidence about a calibrated executor configuration,
not a learned-policy speedup. A >1 ratio can also reflect measurement noise. The
baseline uses the same algorithm and independently tunes the same permitted grid.

## Acceptance and evidence interpretation

The runner preserves incorrect/failed held-out rows and exits nonzero on missing
or invalid metrics, any incorrect result/task error, or declared p95/RSS/CPU budget
breaches. Default budgets are bounded test limits, not a user's application SLO.
Raw reports record command, Python/hardware/constraints, dependency/native-tool
versions, dirty revision state, and per-source SHA-256 hashes.

Inputs are immutable and shared across candidates; each task creates its own output
and cache state. A reference run touches input files before measurement. “Cold”
means a newly constructed executor with lazy worker startup and teardown cost;
it does not mean cold OS caches. Warm means the second pool pass. Native helper
CPU is included by POSIX self+waited-children accounting after shutdown. Aggregate
RSS is sampled and can miss spikes or double-count shared pages; neither is a
per-task resource metric or enforcement mechanism.

Small earlier smoke files remain historical diagnostic evidence. They lack later
provenance/resource fields and must not be presented as final production evidence.
Use representative reports and their declared source hashes for the larger runs.

## Measured representative results

Results are populated only after the corresponding run completes. See the linked
raw reports for every trial, chosen configurations, resource gates and timing.

- Images: 24 heterogeneous tasks per phase, three held-out seeds, two calibration
  repeats. All six cold/warm comparisons were within 10% of tuned stdlib;
  median T_stdlib/T_intelligent was 0.966. This is approximately 3.5% overhead,
  not a speedup. Both chose two threads and batch size one with fixed admission.
  Peak sampled RSS was 178,339,840 bytes; calibration took 29.8 seconds.
  Source: [representative-priority.json](../benchmarks/results/representative-priority.json.gz).
  The 29.8-second search cost above includes both independently tuned engines;
  intelligent execution-search cost alone was 14.6 seconds.

In these earlier representative reports, all eleven workloads completed with
their declared correctness/error/resource gates passing. This is a different
experiment from the later original integrated optimiser matrix's 7/11 outcome. 64 of 66 cold/warm comparisons were within 10% of tuned
stdlib; SQLite and discovery each had one loss beyond that threshold. The complete
[derived summary](../benchmarks/results/representative-summary.json) includes
per-engine configuration, CPU, sampled RSS, p95, timing, errors and search cost.

Ratios are single-host observations, not statistically established universal
advantages. Some memory/latency dimensions improve while others worsen: e.g.
dedup's measured intelligent peak was 201 MiB versus 145 MiB for stdlib, while its
maximum batch-completion p95 was 115 ms versus 208 ms with different selected chunking.
OCR peak was 839 MiB versus 784 MiB; it does not show a memory saving.

OCR's total two-engine search took 388 s (intelligent execution-search 192.5 s),
without an observed mean throughput repayment against the tuned baseline. Images
also showed no repayment. For workloads with positive observed mean savings,
summary break-even counts are only conditional arithmetic for execution-search
cost. They exclude shared fixture generation, sequential references, hold-out
validation, input curation and operational adoption; full adoption amortization
is not established. Saved configuration reuse is separately tested to perform
zero calibration callbacks after strict compatibility checks.

### Evidence snapshots versus final source

Raw reports identify the exact candidate source hashes measured. Later additive
feature-mode controls in the harness and configuration-persistence APIs in
`tuning.py` have different final file hashes and their own focused validation.
The earlier fixed-admission timings are retained as candidate-snapshot evidence;
they are not relabelled as measurements of an immutable final release commit.
Final package tests and exact-commit CI are separate release gates.

## Release gate and historical scope

The earlier candidate's release gate required final full tests, examples,
build/clean-wheel checks, independent review and exact-commit CI. These remain
source-specific checks: the historical counts below cannot satisfy them for a
changed candidate. Consult [current evidence](CONFIGURATION-QUALITY.md) for
completed checks and open verification. Publication is a separate approved step;
no classification or deployment-readiness promotion follows from this historical
report alone. See [workload acceptance boundaries](WORKLOADS.md).


## Predictive feature experiment

The image experiment separately compares batch-size-only profiles with explicit
pixel-cost buckets, using the same two-thread/batch-one selected configuration and
new held-out seeds. With default sample-every-task profiling, batch-only profiles
met the 10% comparison threshold in 4 of 6 phases (median ratio 0.987); declared
features met it in 6 of 6 (median ratio 1.034). This is useful targeted evidence,
not a broad causal proof: the two runs occur sequentially, share a host, and have
only three correlated cold/warm seed pairs. Retain the negative batch-only cases.

Sample-every-eight runs both met 6 of 6, with median ratios 1.026 (batch-only) and
1.002 (declared); they do not demonstrate a consistent feature speedup. The new
adapter improves the explicit comparability of profile inputs; it does not infer
GIL behavior or prove the online predictor optimal. Its benefit must be validated
for each workload. Pipeline/dedup lack explicit fixture cost metadata and currently
use the documented batch-size fallback.

Raw evidence:
[predictive default batch-only](../benchmarks/results/predictive-default-batch-only.json.gz),
[predictive default declared](../benchmarks/results/predictive-default-declared.json.gz),
[predictive sampled batch-only](../benchmarks/results/predictive-images-batch-only.json.gz),
[predictive sampled declared](../benchmarks/results/predictive-images-declared.json.gz).


## Sustained local reliability

All six 60-second checks passed: HTTP 16,872 tasks, cached HTTP 7,536,
range downloads 6,401, loopback discovery 19,898, SQLite 17,521, and pipeline 2,088.
Across 70,316 executions there were zero task errors/reference mismatches;
completion callbacks, bounded pending/accounting, worker/server cleanup and
observed RSS/CPU gates passed. Largest end-minus-start sampled RSS growth was
29.14 MiB (HTTP). This is one minute per workload, not a long-haul leak proof or
external-service load test. Peak and growth budgets are observation thresholds,
not runtime enforcement. The fixed configuration was two threads/four pending.
See [raw sustained evidence](../benchmarks/results/sustained-workloads.json) and
[method/limits](WORKLOAD-SOAK.md).

## Historical OCR generalization regression and repair

A fresh seed (12011) on the then-current candidate exposed a genuine recognition error: on task 6,
page 2, Tesseract read `DELTA` as `DELIA`. The page was clean and unclipped;
changing page-segmentation mode did not repair it. That failing historical candidate report
is preserved as [pre-fix evidence](../benchmarks/results/final-source-recheck-pre-ocr-fix.json).

The corrected path uses a minimum rendered long side of 1800 pixels, matching the
existing smoke path, while retaining larger profiles and the 2400-pixel cap.
The same PDF/text passes at 1800, 2000, 2200 and 2400 pixels; the exact text oracle
is unchanged. All 72 representative documents across six seeds, including the
original failure, pass after the fix. The 12-test media suite includes the full
12-document failing-seed regression and serial/thread/spawn checks.
See [resolution evidence](../benchmarks/results/ocr-resolution-regression.json).

The earlier full-grid OCR timings use the pre-fix rendering path. They are not
retagged as corrected-path measurements. The historical frozen-selection recheck
ran both engines with that corrected renderer and recorded its source-specific
resource/correctness results separately. Recognition on arbitrary documents remains
fallible; the harness fails closed rather than substituting expected text.

## Historical frozen-selection recheck (09:52 UTC)

After that OCR repair, all eleven workloads passed on seed 12011 with 12
representative tasks per phase, both engines, frozen earlier-selected settings and
zero selection callback invocations. This was a regression check of its recorded
source snapshot, not a repeated full-grid search or validation of the later
integrated/revised matrices. The corrected OCR cold-plus-warm lifecycle was
19.396 seconds for stdlib and 19.596 seconds for intelligent on this check; peak
sampled RSS was 240,480,256 and 188,399,616 bytes respectively. Different fixture
counts/state mean these values must not be compared directly with earlier 24-item
runs as an optimization claim.

[Historical raw recheck](../benchmarks/results/final-source-recheck.json) records
that snapshot's source hashes, native tools/dependencies, correctness, resource budgets
and both engine measurements. The earlier failing report remains preserved.

## Historical local verification

The earlier candidate recorded in [local-validation.json](../benchmarks/results/local-validation.json)
passed 288 discovered unit/integration tests with the optional
workload and MCP dependencies installed, eight benchmark-contract tests, all eight
examples, catalog synchronization and compilation checks. No optional test skips
occurred in that environment. Tests include the repaired OCR regression.

That candidate's built wheel also passed all 288 tests from an isolated environment outside
the source tree; import origin/version were checked and `pip check` passed.
The source archive installed in another clean environment, where the tuning and
predictive examples plus packaged catalog succeeded. Remote CI had not been run
for that recorded candidate. These local single-host observations do not validate
subsequent source changes. Later exact-commit CI and final package verification
are separate records linked from [current configuration quality](CONFIGURATION-QUALITY.md).
