# Recorded measurements

- [Reference raw JSON](reference.json): five repeats of six engines, three
  workloads, and cold/warm phases, 180 measured batches in total. Every workload
  checksum matched across every engine, phase, and repeat.
- [Reference tables and methodology](reference.md): generated from that JSON,
  including median/p95 batch and task latency, submission timing, and measured
  thread/process route counts.
- [Smoke raw JSON](smoke.json) and [smoke summary](smoke.md): an earlier small
  two-repeat correctness check, not stable performance evidence. That report
  predates the added per-measured-batch route-count field; its final snapshots
  include warmup work.

## Reference run in brief

Recorded on 2026-10-10 with CPython 3.12.14, Linux x86-64, an AMD EPYC 9V74 host,
and nine visible/affinity CPUs. Cgroup quota metadata was unavailable; that does
not establish that the environment had no resource constraints. Other host load
was not isolated. Process start method was `spawn`.

Each workload has 48 tasks. CPU tasks perform 200,000 pure-Python integer-loop
iterations; simulated I/O tasks sleep for 10 ms. Baseline pools use four workers;
the hybrid uses two thread plus two process workers, with eight accepted
unfinished tasks allowed. Every warm sample follows two untimed batches.

Warm median measured batch times, in milliseconds:

| Workload | Sequential | ThreadPool | ProcessPool | Intelligent hints | Intelligent adaptive |
|---|---:|---:|---:|---:|---:|
| CPU | 875.543 | 901.755 | 256.123 | 529.761 | 548.489 |
| Simulated I/O | 484.936 | 121.979 | 123.253 | 243.715 | 164.110 |
| Mixed | 683.331 | 450.050 | 195.875 | 250.065 | 343.583 |

**The adaptive policy did not beat the fastest stdlib baseline in any of these
three workloads.** It was also slower than explicit hints on CPU and mixed work.
This is useful negative evidence, not a performance acceptance test. These
experiments cannot distinguish every cause of a timing difference.

Important qualifications:

- CPU-only hinted work uses two process workers, while the process baseline uses
  four. Its lower throughput cannot be attributed solely to policy overhead.
- Hinted I/O uses only the two thread workers. Adaptive routing sent 119 of 240
  measured warm I/O tasks to processes, using additional configured capacity.
  Its improvement over the two-thread hinted mode does not show that processes
  are intrinsically better for I/O. Four plain threads were fastest here.
- Cold CPU median lifecycle times were 370.653 ms for the process baseline,
  572.046 ms with hints, and 818.609 ms with the adaptive policy. Cold timings
  include startup and shutdown; warm timings measure the batch alone.
- Bounded submission changes when later tasks enter the queue. Task latency
  includes admission waiting, but does not represent a common external arrival
  schedule. Compare batch throughput and all recorded latency definitions.
- Five repeats are a small sample. Task-level p95 pools 240 observations per
  engine/workload/phase, while batch-level p95 uses five observations. Neither
  is a confidence interval or a guarantee about another environment.
- Pure-Python loops and `sleep` with small payloads do not represent production
  applications, serialization-heavy work, or native libraries that release the GIL.

Reproduce the reference configuration from the repository root:

```sh
PYTHONPATH=src python -m benchmarks.run --max-pending 8 \
  --output benchmarks/results/reference.json
```

The source tree had no Git revision when these samples were recorded, so the
raw metadata correctly records `git_revision: null`. Numerical timing values
will naturally differ between runs.
