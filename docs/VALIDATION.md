# Validation record

## 0.3.0 verification scope (2026-10-10)

The runtime is an opt-in predictive foundation; existing defaults, real Future
semantics, and explicit process consent remain. No production-readiness or
cross-hardware speedup claim is made.

- CPython 3.12.14/Linux with official MCP 2.3.0: **179 tests passed**, including
  actual SDK stdio integration. CPython 3.13.5: **179 tests run, 10 optional MCP
  tests skipped**, all remaining tests passed. Eight benchmark-harness tests,
  synchronized 65-concept catalog, compileall, and seven finite examples pass.
- Independent review exercised multi-producer threads, spawned processes,
  sampling/key eviction, hostile profile JSON, local-measurement refinement,
  simulated foreign hardware, and strict consent/cap invariants. Two issues were
  corrected and independently rechecked: adaptive-window callback timing race
  and catastrophic subtraction during dominant-sample eviction from cached sums.
- Twenty-nine new runtime tests and 21 model tests cover opt-in compatibility,
  real batching, cancellation/failure boundaries, no duplicate invocation, clock
  provenance, feature privacy, strict bounded JSON, CPU quotas, objectives,
  unvalidated foreign priors, and stable rolling aggregates.
- Extended whole-package stress, 20-second service soak, 4,480 real MCP calls,
  malformed protocol recovery, repeated synthetic thread/process trials, and a
  real JSON/gzip transform are reproducible in `benchmarks/stress/` and
  `benchmarks/stress_predictive.py`. Full raw reports and timings, including
  regressions, are linked in [the results guide](../benchmarks/results/README.md).
- Synthetic mixed-work medians were slower with prediction: 0.813s vs 0.559s
  for 1,000 thread tasks; sampling every eighth invocation gave 0.739s vs 0.551s.
  For 400 mixed process/thread tasks, 0.505s vs 0.307s. These are observations,
  not calibrated comparisons to tuned optimum baselines. Genuine batching
  reduced dispatch counts but no incomparable timing speedup is inferred.
- The legacy raw-core cancellation-queue retention limit is explicitly measured
  (~64.6 MB for 10,000 cancelled queued 4 KB jobs while a worker blocks).
  Logical `max_pending` is not a physical queue/RSS bound.

Local checks are Linux-only. Windows/macOS and Python 3.10/3.14 are configured in
remote CI and are separate from this record. Process task CPU/RSS, calibrated
uncertainty, controlled scaling curves and trained transfer-learning remain
unimplemented; observational profiles and conservative foreign priors are labeled.


## 0.2.0 verification scope

The 0.2.0 update preserves the core executor and existing benchmark data. It adds
a separate standard-library-only task framework and an optional MCP2.3 adapter.
Validation includes real subprocess stdio protocol calls through the official SDK,
strict input/schema and JSON-output checks, malformed/unregistered tasks, bounded
admission/retention, cancel/forget churn, errors, resources, metrics, import
isolation, and disconnect/startup-failure cleanup.

Installed-wheel checks use a clean core-only environment and an SDK2.3 environment;
core-only optional test skips are reported separately. The example server is not
started as a public network listener. No new scheduler speedup is claimed.

### Local 0.2.0 checks (2026-10-10)

- CPython 3.12.14/Linux with official MCP2.3.0: **129 tests passed**, including
  13 adapter tests (10 requiring the SDK) and 26 framework tests.
- CPython 3.13.5/Linux without the optional SDK: **129 tests run, 10 optional
  tests skipped**; all remaining tests passed.
- Eight benchmark-harness regression tests, catalog synchronization, compilation,
  and six finite examples passed. Existing benchmark data was not regenerated
  because executor/policy code and benchmark algorithms are unchanged.
- Core-only installed wheel contains no mandatory dependency; SDK imports remain
  absent until the adapter is created. Optional extra metadata is `mcp>=2.3,<3`.
- Wheel and sdist pass strict distribution metadata checks. Clean installed-wheel
  tests cover the core-only and SDK-enabled paths.

Windows/macOS and Python3.10/3.14 are configured in CI, not locally verified here.
The optional MCP job explicitly installs the extra on all three OSes with both
minimum and newest configured Python versions. CI/publication are separate remote
checks and are not implied by this local record.

### Review corrections

- Stdio ownership now covers SDK loading and runner startup failure, not just an
  established session. Failure-path tests verify the service is actually closed.
- Discovery bounds and service bounds agree; oldest-first record listing permits
  bounded drain-and-forget cleanup.
- Application outcome counters are distinct from lower-level wrapper counters.
- JSON strings and keys must be valid Unicode scalar text; lone UTF-16 surrogates
  are rejected so successfully stored results remain serializable by the SDK.
- Parameter defaults, descriptions, names, and hint keys are explicitly documented
  as client-visible metadata, unsuitable for secrets.

## 0.1.0 historical baseline

The measurements and original validation below belong to the initial release;
they are retained rather than replaced by unmeasured 0.2.0 performance claims.

Local validation: 2026-10-10, Linux x86_64, CPython 3.12.14. This is a tested
prototype, not a production certification. Cross-platform and multi-version
GitHub Actions configuration is included; remote CI results are a separate check.

## Verified checks

Final installed-wheel run: **89 main-suite tests and 8 benchmark-suite tests
passed**, plus all five runnable examples. Doctests are included in those suites.

- Executor/policy regression tests cover genuine Futures, errors including
  BaseException and worker-process crashes, cancellation, callback reentrancy,
  shutdown races, blocked producer wakeup, process spawn and pickle failures,
  ordered maps, global timeouts, bounded streaming cleanup, profile eviction,
  immutable policy views, and concurrent producer stress.
- Catalog tests validate fields, IDs, statuses, dependency cycles, independent
  returned copies, search, and synchronization of the bundled catalog.
- Doctest examples and benchmark-harness regression tests pass.
- A wheel is built locally and installed without dependencies into a clean
  virtual environment. The full tests, catalog CLI, and all five runnable
  examples are verified against that installation.
- `compileall` checks source, tests, examples, benchmarks, and verification tools.
- Both smoke and reference benchmark runs match deterministic result checksums.
- A basic secret-pattern scan found no common token/private-key patterns. This
  is not a formal secret-scanning or security-audit guarantee.

Run the checks again from an installed environment:

```sh
python tools/verify.py --examples
```

## Independent review findings and resolution

1. Registering the instrumentation callback outside the lifecycle lock allowed
   shutdown to overtake bookkeeping. Registration now happens under an RLock,
   supporting immediate reentrant callbacks and final metrics.
2. Calling process shutdown with `wait=False` and then `wait=True` loses the
   manager reference in stdlib. Shutdown now calls the process pool with the
   intended wait mode and separately joins thread workers. A pending-condition
   wait establishes final task metrics after earlier nonblocking shutdown.
3. Later shutdown calls could downgrade an earlier cancellation request. The
   executor now keeps cancellation requests sticky.
4. Cancellation frees logical admission, while cancelled backend queue entries
   can retain arguments until drained. This is an explicit remaining limitation,
   not a fixed hard queue/memory bound. See CF-061 for a removable-queue design.
5. Policy integer validation and factory exception propagation were tightened
   and covered by regression tests.

Repeated nonblocking process shutdown follows stdlib resource-teardown behavior;
subsequent waiting guarantees accepted-task completion/accounting here, but does
not promise every OS process resource is already reaped in that specific path.
Never call blocking shutdown from an executor worker or callback.

## Measured reference outcome

Five repeats, 48 tasks per batch, 200,000 pure-Python iterations per CPU task,
10 ms synthetic I/O sleep, spawn processes, two warmup batches per warm sample,
and `max_pending=8`. There are 180 recorded cold/warm samples across six engines
and three workloads. All checksums matched.

Warm median batch timings in milliseconds:

| Workload | Best measured stdlib pool | Adaptive hybrid |
| --- | ---: | ---: |
| CPU | Process: 256.123 | 548.489 |
| Synthetic I/O | Thread: 121.979 | 164.110 |
| Mixed | Process: 195.875 | 343.583 |

The adaptive prototype lost to the best measured stdlib pool in all three.
The baseline pools have four workers; the hybrid has two threads plus two
processes. Hinted routing gets truthful kinds, whereas adaptive routing gets
unknown kinds and stable comparable keys. Consequently, these are operational
configurations, not an isolated causal experiment on the learning algorithm.
Read [methodology](../benchmarks/README.md) and
[raw reference results](../benchmarks/results/reference.json) before comparing.
Synthetic sleep is not a production network or storage benchmark. Five repeats
are descriptive, not a statistical guarantee or a tail-latency SLA.

## Not verified here

- Remote GitHub Actions for the 0.2.0 candidate (check current commit checks separately)
- Python 3.10, 3.14, Windows, macOS, free-threaded CPython, subinterpreters
- Production network/storage workloads, sustained days-long load, memory ceilings
- Adversarial/untrusted tasks, security isolation, durable/distributed execution
- Publication of the 0.2.0 candidate (verify index/release artifacts separately)
