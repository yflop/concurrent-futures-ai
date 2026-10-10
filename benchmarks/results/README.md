# Recorded measurements

Large reports are published as lossless `.json.gz` archives. See
[archive identities and bounded reading/decompression](ARCHIVES.md) before using
archived reports as inputs to reproduction harnesses. Markdown links point to
the shipped files; reproduction `--output` paths still name new plain JSON reports.

- [Reference raw JSON](reference.json.gz): five repeats of six engines, three
  workloads, and cold/warm phases, 180 measured batches in total. Every workload
  checksum matched across every engine, phase, and repeat.
- [Reference tables and methodology](reference.md): generated from that JSON,
  including median/p95 batch and task latency, submission timing, and measured
  thread/process route counts.
- [Smoke raw JSON](smoke.json.gz) and [smoke summary](smoke.md): an earlier small
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

## 0.3 predictive foundation and regression stress

New reports are separate from historical 0.1 reference measurements above.
All were run on the 0.3 working tree on 2026-10-10, CPython 3.12/Linux with
9 visible/affinity CPUs; raw predictive reports include the CPU model, exact
configuration, seeds and complete observations. Host load was not isolated.

- [Predictive thread trials](predictive-thread.json.gz): five seeded repeats of
  1,000 mixed tasks. Median baseline 0.559s versus predictive 0.813s.
- [Sample interval 8](predictive-sampled.json.gz): five repeats of the same
  1,000-task mix. Median baseline 0.551s versus predictive 0.739s.
- [Mixed thread/process trials](predictive-process.json.gz): five repeats of
  400 tasks with two process workers. Median baseline 0.307s versus predictive
  0.505s. Baseline and predictive runs share worker budgets; predictive admission
  is intentionally lower and variable. These are not tuned optimum comparisons.
- Actual batching produced 36 submissions for 1,000 successful thread items and
  18 submissions for 400 process items, in every repeat. Its success-only input
  variant/timing boundaries differ from failure-including single-task trials;
  do not compare their wall times as a batching speedup.
- [Extended stress](extended-stress.json): 10 subprocess-isolated regression
  scenarios, including 16,800 producer tasks, 7,000 service tasks, 110 shutdown
  race iterations, cancellation, process crashes, callback nesting, JSON bounds,
  retention pressure and 5,000 ordered map outputs.
- [MCP stress](mcp-stress.json): 4,480 real SDK calls across 8 connections and
  64 rounds; capacity rejections expected. [Raw protocol](raw-protocol.json):
  60 malformed requests followed by recovery checks and clean EOF cleanup.
- [Service soak](service-soak.json): 20 seconds with 32 producers, 18,577 terminal
  tasks, ~565 KB peak traced Python allocation. This is not RSS or a long-run SLA.
- [Cancellation queue limitation](churn-limitation.json): 10,000 cancelled queued
  jobs retain ~64.6 MB until blocked workers drain, despite logical max_pending=2.
  This documented raw-core limitation is preserved, not disguised by throughput
  measurements; TaskService's distinct physical-admission tests pass.
- [Real JSON/gzip transformation](realistic.json): three randomized-order cold
  repetitions, matching outputs, equal four active compute workers. This short
  workload favors stdlib threads; no intelligent-engine superiority is claimed.

The three larger raw reports are losslessly gzipped with mtime=0. Read them using
`json.load(gzip.open(path, "rt"))` or `gzip -dc path.json.gz`. Their full per-task
observations, inputs, checksums, profile samples, and machine configuration remain
available. Reproduction commands are in `benchmarks/stress/README.md`; predictive
commands use `python -m benchmarks.stress_predictive --tasks 1000 --repeats 5`
(add `--sample-interval 8`, or use `--tasks 400 --process-workers 2` for the other
reports). Synthetic seed is 41073; OS scheduling is not deterministic.

These results establish tested behavior and exposed overhead, not production
certification, controlled cross-hardware transfer, or universal speedup.

## 0.4 workload evidence

`representative-*.json` records the larger heterogeneous workloads, source hashes,
actual command/dependencies/native tools, repeated calibration, separate held-out
seeds and explicit resource/correctness gates. See `docs/PRODUCTION-EVIDENCE.md`
and `docs/WORKLOADS.md` for interpretation. Results from fixed admission do not
establish an online-predictive speedup.

Historical `real-world-images.json` and `real-world-fixed.json` are early smoke
runs. Their inherited metadata erroneously names `benchmarks.run` rather than
`benchmarks.real_world.run`; do not use that field to reproduce them. They also
predate explicit source hashes, lifecycle CPU/teardown accounting and fail-closed
acceptance gates. Their raw timings remain diagnostic evidence, not final
production evidence. `integrated-smoke.json` is a later small selector smoke run,
not a representative capacity measurement.


## Publication metadata redaction

For publication, report commands use `python` instead of an absolute interpreter
path, checkout source paths are relative, temporary build paths are normalized,
and derived hardware fingerprint identifiers are set to `null`. Compressed logs
and historical report copies in this release receive the same treatment. These
metadata-only edits preserve all measured timings, counts, errors, oracle outputs,
resource observations, source-content hashes, software versions and hardware
capabilities. These public report snapshots are evidence, not directly reusable
saved-profile/configuration files; use the documented export API to create those.
The original 0.1–0.3 release assets remain unchanged.
