# Validation record

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

- Remote GitHub Actions or an externally published GitHub commit
- Python 3.10, 3.14, Windows, macOS, free-threaded CPython, subinterpreters
- Production network/storage workloads, sustained days-long load, memory ceilings
- Adversarial/untrusted tasks, security isolation, durable/distributed execution
- Package-name availability or a PyPI release
