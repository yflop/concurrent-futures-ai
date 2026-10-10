# 0.5 Beta adoption evaluation evidence

## Result

The fixed experiment retained standard-library executors for **all 11 tested
workload families**. There were **1,017 measured fixture calls and zero failed
correctness/resource observations**. No intelligent-executor adoption was
recommended. Total experiment wall time was **647.15 seconds**
(about 10.8 minutes), including fixture preparation, sequential oracles, search,
held-out validation and cleanup. These costs matter before adopting any result.

The recipes used actual optional libraries and native tools, but deliberately
small **2- or 4-item fixtures and cold pool lifecycles**. This is evidence about
safe small-batch adoption decisions on this host. It does not establish large
batch, persistent-service, warm-throughput, p95 or production-deployment benefit.
The 10,000-reuse horizon was a predeclared illustrative scenario, not measured
user demand. It did not override a failing gate.

[Raw evidence](../benchmarks/results/adoption-v050.json.gz) retains every scalar
measurement, failed/excluded slot, execution order, seed, resource sampler record,
configuration, cost and outcome. The report has 3420 measured logical-item
attempts and 972 additional sequential oracle items. All completed correctly;
these totals are fixture work, not a throughput guarantee.

## Predeclared protocol

- Identical algorithms, tuple-batch kernel, batch size 1 and caller-side window 4
- Independent stdlib and intelligent training grids: thread/process × workers
  1/2; intelligent additionally had a two-slot adaptive split (one thread, one
  process). No profiling/prediction was enabled in this experiment.
- Fixed stdlib comparator: two threads. Tuned stdlib: best observed eligible
  training configuration in the declared grid, not a global optimum.
- Three common-seed training blocks; frozen selections then 24 untouched common-
  seed held-out blocks, with randomized mode order in each block
- One primary metric: pool construction + fixture execution + waiting shutdown
- Required median paired speedup lower bound above 5%, family alpha 0.05 divided
  between fixed and tuned baseline comparisons; margin ties count against wins
- Additional rejection gates: paired log-ratio MAD at most 0.10, positive
  aggregate elapsed saving, no observed pair over 25% slower, and every compared
  duration at least 0.001 seconds
- Observed caps: process-tree sampled RSS 1 GiB, self + waited-child CPU 120 seconds,
  and lifecycle elapsed 60 seconds. These are rejection caps, not runtime
  preemption or hard memory limits. All 1,017 observations met these caps.
- Empirical amortization within the declared 10,000 equal-sized future fixtures
  would also have been required. No result reached that adoption stage.

The inferential target is the median of independent representative paired
log(baseline elapsed / candidate elapsed), not mean throughput, task latency,
future tail safety or cumulative savings. Family alpha applies to the two
comparisons inside each report, not across eleven workloads or repeated campaigns.
Random order reduces ordering bias; it cannot prove independent timing noise or
remove arbitrary shared-host interference. The generated workload/version and
independence declarations are explicit experimental assumptions.

Fresh paths and application fixture state were recreated for every measurement.
OS/page caches were not controlled. Setup and sequential oracle generation are
excluded from the primary elapsed metric but included in total discovery cost.
CPU/RSS observations cover pool execution plus input collection and ordered
validation, excluding fixture/oracle setup; that boundary is identical across
compared modes. No batch-p95 data was collected, so no p95 claim is made.

## Paired outcomes

All rows below ended in `retain_stdlib`. Values are paired speedup percentages
(B/C − 1), not percentage reductions in elapsed time. “Lower” is the one-sided
order-statistic bound on the typical paired gain. Passing that column alone
would not bypass noise, aggregate, tail, correctness, resources or cost gates.
Matching fixed/tuned baseline configurations share one measurement, explaining
75 versus 99 calls without dropping planned observations.

| Family | Items / fixture | Calls | Fixed median | Fixed lower | Tuned median | Tuned lower | Discovery seconds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| http | 4 | 99 | -8.2% | -12.1% | -16.4% | -21.8% | 53.3 |
| images | 2 | 75 | +2.8% | -4.7% | +2.8% | -4.7% | 43.0 |
| geospatial | 2 | 99 | +0.7% | -29.4% | -2.2% | -29.5% | 10.4 |
| cached_http | 4 | 99 | -1.7% | -12.7% | -14.4% | -21.3% | 55.2 |
| downloads | 4 | 99 | +0.2% | -5.4% | -5.6% | -14.9% | 55.2 |
| pipeline | 4 | 99 | +18.7% | +11.7% | +1.1% | -3.2% | 18.9 |
| ocr | 2 | 75 | -0.1% | -1.9% | -0.1% | -1.9% | 258.2 |
| dedup | 4 | 99 | +42.9% | +31.5% | -2.3% | -5.4% | 19.1 |
| sqlite | 4 | 99 | +23.5% | +17.3% | -18.2% | -21.9% | 25.5 |
| discovery | 4 | 99 | +7.4% | -8.1% | -5.3% | -19.9% | 53.4 |
| video | 2 | 75 | -1.8% | -5.4% | -1.8% | -5.4% | 54.7 |

The pipeline illustrates the protection against a weak baseline: the candidate
passed the fixed comparator (18.7% typical paired speedup; 11.7% lower bound) but
showed only 1.1% typical gain versus tuned stdlib, with a −3.2% lower bound. SQLite
also passed the fixed comparison while losing to independently tuned stdlib.
Dedup's large fixed-baseline point gain still failed the predeclared noise gate.
The evaluator kept all of these distinctions and retained stdlib.

### Frozen training selections

Every selected intelligent finalist was deterministic thread-only with prediction
disabled. The adaptive split was tested during training but never selected for
held-out evaluation. No predictive candidate was included. These eleven outcomes
therefore do not evaluate persistent adaptive/predictive benefits.

Every row uses batch size 1 and fresh empty executor/policy state. A selected
configuration is not an adoption recommendation. The intelligent selections
below identify exactly what was measured; no result authorizes enabling a
different adaptive/predictive mode later.

| Family | Tuned stdlib | Selected intelligent |
| --- | --- | --- |
| http | thread / 1 workers | thread / 1 workers |
| images | thread / 2 workers | thread / 2 workers |
| geospatial | thread / 1 workers | thread / 2 workers |
| cached_http | thread / 1 workers | thread / 2 workers |
| downloads | thread / 1 workers | thread / 1 workers |
| pipeline | thread / 1 workers | thread / 1 workers |
| ocr | thread / 2 workers | thread / 2 workers |
| dedup | thread / 1 workers | thread / 1 workers |
| sqlite | thread / 1 workers | thread / 1 workers |
| discovery | thread / 1 workers | thread / 1 workers |
| video | thread / 2 workers | thread / 2 workers |

## Separate real heterogeneous experiment

A separately predeclared experiment combined **four real graph-routing tasks
and four loopback HTTP tasks**, interleaved in an eight-item window. It used the
caller-measured `RepeatableWorkload` API with explicit per-task CPU/I/O hints and
separate workload keys. The convenience callable adapter supplies one generic
key/hint set and is not a mixed-task classifier; use a caller-measured fixture
when those semantics matter.

The single intelligent candidate was an adaptive **four-slot split: two thread
workers and two process workers**. Stdlib independently searched threads/processes
with two/four workers; the fixed baseline used four threads. This comparator
class is tuned single-backend pools, **not an expert hand-built hybrid pipeline**.
The three training / 24 held-out blocks, 5% margin, noise/tail/resource gates and
10,000-reuse scenario were unchanged and declared before the run. Each measurement
started with fresh empty policy state and included cold pool startup/shutdown;
no predictive profile was enabled.

Result: **retain_stdlib**, with **87 calls, zero failed observations**, 696 measured
logical items plus 216 sequential oracle items, and **58.21 seconds**
of discovery/runner time. The tuned stdlib selection was two threads. Across all
24 held-out intelligent fixtures, actual route counters recorded four tasks on
threads and four on processes (96 each in total), so hybrid dispatch was exercised.

| Comparator | Median paired speedup | One-sided lower | Aggregate speedup |
| --- | ---: | ---: | ---: |
| Fixed four-thread stdlib | -48.8% | -53.2% | -49.9% |
| Tuned two-thread stdlib | -51.7% | -55.3% | -51.4% |

The candidate took about **2.07×** the typical paired cold-lifecycle time of the
tuned stdlib pool. Both comparisons failed practical-gain, aggregate and worst-
observed-pair gates. This does not establish that hybrid scheduling always loses;
it rejects adoption for this small mixed fixture, mode and timing scope. No
positive outcome was manufactured or required.

[Raw mixed evidence](../benchmarks/results/adoption-mixed-v050.json.gz) and the
[runner](../benchmarks/real_world/adoption_mixed.py) preserve the declared plan and
all observations. Reproduce as a new separately budgeted experiment:

```sh
python -m benchmarks.real_world.adoption_mixed --output mixed-rerun.json
python tools/summarize_adoption.py mixed-rerun.json
```

After this uncancelled run, review added an explicit `CancelledError` abort guard
to the benchmark wrapper; the runtime/evaluator and measured normal path did not
change. A dedicated cancellation test verifies propagation. The
[exact measured runner snapshot](../benchmarks/results/adoption-mixed-measured-source.py)
is retained for the original source hash; do not mistake that historical snapshot
for the current reusable runner. All numeric evidence remains unchanged.

Mixed JSON SHA256: `739f39af4be79b28c22f4b8d7b5233b6a3b71c0f305a081f0ee6391e49dda7f3`.

## Trace-enabled repeated lifecycle check

A separate [lifecycle report](../benchmarks/results/adoption-lifecycle-v050.json)
records 128 four-producer thread-executor cycles plus eight spawned-process
cycles: **32,768 accepted thread tasks** (26,074 completed and 6,694 cancelled),
64 completed process tasks, and 30,720 trace evictions. Observed maxima were eight
pending/retained scheduler items and sixteen trace entries, matching the declared
bounds. Final Python-thread count returned to one; one additional descendant and one
additional descriptor remained, within the declared allowance for multiprocessing
support state. Child identity was not recorded by this check. Observed parent RSS growth was 1,208,320 bytes against a
128 MiB test gate. This 3.85-second rapid lifecycle/churn check is separate from
historical endurance evidence and is not a new long-duration soak.

The runtime tests also hold a trace-enabled thread executor at its one-item
admission limit while a separate forced-process executor completes work and shuts
down. Their route counters, queues and shutdown states remain independent. Other
tests cover immediate payload release during queued cancellation, trace access
from completion callbacks, and contending submitters/readers. None of the three
actual experiment reports recorded a resource-gate failure. Synthetic tests
separately verify that missing or excessive required resource measurements reject
adoption; successful observed caps do not impose hard byte/RSS limits at runtime.

```sh
python tools/stress_adoption_lifecycle.py --cycles 128 --process-cycles 8 --output lifecycle-rerun.json
```

## Benefits and costs this release actually establishes

The new evaluator automates a bounded reproducible comparison and returns a
conservative decision. In this run its useful outcome was preventing unsupported
adoption, including cases that looked favorable only against a fixed baseline.
It does not make the underlying scheduler faster by itself. Search, validation,
fixture reset, process startup, trace collection when enabled, and maintaining
fixture/version contracts all add cost. No developer-hours-saved figure was
measured or inferred. Runnable [incremental adoption and rollback examples](ADOPTION.md)
show the actual application boundary and its compatibility limits.

Deterministic process mode, bounded trace snapshots/counters, and repeated
lifecycle tests establish operational behavior separately from speed. Item and
worker bounds remain distinct from sampled RSS/CPU. Running work is not killed;
explicit shutdown and the no-same-executor-wait completion-callback rule remain
required. Beta is not production certification.

## Reproduce and verify

Install the checkout and optional dependencies from `requirements-workloads.lock`;
install the documented FFmpeg, Poppler, Tesseract English data and DejaVu font
prerequisites. Run serially, without another CPU-heavy experiment:

```sh
python -m benchmarks.real_world.adoption --output adoption-rerun.json
python tools/summarize_adoption.py adoption-rerun.json
```

Declare a new report, assumptions and alpha budget before any repetition; never
rerun until a positive result appears. Host timings and decisions may differ.
The summary command validates slot accounting and exactly recomputes the paired
gates from raw observations without running any tasks or deploying a configuration.

Measured host: 9 logical / 9
affinity-visible CPUs; no cgroup CPU quota or memory limit was detected by the
existing probe. This is one host, not cross-hardware transfer evidence. Full
runtime, dependencies, native tools and exact runtime/fixture `.py` hashes are in
the report. Its Git revision is the local snapshot of published 0.4 plus a dirty
0.5 worktree; the source hashes, rather than that snapshot commit, identify the
measured implementation. Every recorded source hash was checked again after
completion. Source verification and installed-artifact/exact-commit CI are
separate release gates.

Raw JSON SHA256: `06cbf8f8ebc4f9e1affabb0aab53a37b663d0e291fd0f2be9598bb0c3a7f8c47`.

The two large reports are lossless deterministic gzip archives. The
[adoption archive ledger](../benchmarks/results/adoption-archives-v050.json)
records compressed and decompressed SHA256 hashes and byte counts. The hashes
above identify the exact decompressed JSON. The bounded reader and summary tool
accept these archives directly:

```sh
python tools/summarize_adoption.py benchmarks/results/adoption-v050.json.gz
python tools/report_io.py benchmarks/results/adoption-v050.json.gz --output local-report.json
```

## Installed-artifact verification

A clean Python 3.12 environment containing only the installed 0.5.0 wheel passed
640 collected unit tests: 601 passed and 39 expected optional-dependency skips. A separate
clean environment with the workload lock and MCP extra passed all 640 tests,
eight benchmark tests, runnable examples, catalog checks and compilation checks.
The installed import path was checked to exclude the source tree. Strict wheel
and source-distribution metadata checks passed. Exact-commit cross-platform CI
remains a separate release gate; these local checks do not claim other platforms.

## Historical evidence

All **109 existing files under benchmarks/results** remain byte-identical to the
published 0.4 source manifest. The earlier
[configuration-quality report](CONFIGURATION-QUALITY.md),
[production evidence](PRODUCTION-EVIDENCE.md), failure reports and archives retain
their exact original scope and outcomes. This experiment neither erases nor
retroactively strengthens them.
