# Configuration selection: quality, cost and operating limits

Evidence snapshot: 2026-10-10. The original integrated attempt is finished and
retained, including its failures. Revised video/OCR, live pipeline, declared
shifted images, all four affinity experiments, and load/churn checks are complete.
The first 128-item capacity attempt failed before execution; its separately
declared two-shard replacement and final-adapter endurance both passed. All
planned heavy follow-on experiments are complete. Revised-source
compatibility CI passed all 16 jobs at the exact commit linked below. Final local
source verification passed 569 tests plus eight benchmark tests and its listed
examples/catalog/compilation checks. Version 0.4 has Beta maturity; built-artifact
verification and final exact-commit CI are separate publication-workflow checks.
Later outcomes must be recorded as separate follow-ons, not substituted into the
original denominator.

## Purpose and current maturity

Intelligent Futures helps a caller choose and reuse measured concurrency settings
for a specific workload. The useful question here is whether explicit offline
search can choose a good backend, worker count and batch size at lower discovery
cost than an exhaustive search. It is not a claim that an executor wrapper makes
identically configured Python execution inherently faster.

Version 0.4 is a **Beta trusted-code execution framework**, with experimental
empirical-tuning and online predictive/adaptive capabilities. Beta is not
Production/Stable or a deployment certification. Release verification requires
source, built-artifact and exact-commit CI evidence with their distinct scopes.
The completed original experiment reached its within-10%-of-eligible-oracle goal
for **7 of 11 planned workloads**, not all eleven. The primary 72-call search
picked the same choices as exhaustive median on the ten completed matrices,
while both retained the same held-out failures. These observations support
bounded configuration search as a capability; they do not establish universal
near-optimality, future resource compliance, or blanket production readiness.
[Original results and limitations](../benchmarks/results/optimizer/primary-v2/README.md).

A sensible use is to supply approved, representative, idempotent fixtures and a
real correctness oracle, run `tune`, inspect every gate, freeze the choice, and
validate it on separate inputs before adoption. Reuse compatible configuration
JSON only while its workload/runtime/hardware assumptions remain appropriate.
`tune` selects the best observed median in its **final comparable eligible
cohort**; eliminated partial scores cannot win. Exhaustive `calibrate` selects
the best observed eligible mean over all supplied candidates. See the
[budgeted API](BUDGETED-TUNING.md) and [persistence contract](TUNING-PERSISTENCE.md).

## Original result, including the failed cases

The [original matrix](../benchmarks/results/optimizer/primary-v2/README.md) is the
final original attempt: ten completed workload reports, 2,730 real trials and
131,040 measured task observations. OCR failed before calibration. Its summary's
`partial` status means incomplete evidence, not that this original run is still
running. All eleven workloads and 33 combined seed slots remain in the denominator.
The primary result is **7/11 workload means within 10%** and **20/33 combined
seed slots within 10%**. No pooled successful-only speedup is reported.
[Complete summary](../benchmarks/results/optimizer/primary-v2/summary.json).

Failures and misses are material results:

- **OCR:** its executor-independent same-algorithm reference read `DELTA` as
  `DELIA` in two of 24 generated documents. No valid calibration or held-out
  ranking follows. This is a workload/fixture correctness failure, not proof of
  an executor scheduling error. It remains a failed original workload.
  [Failed record](../benchmarks/results/optimizer/primary-v2/ocr.json).
- **Video:** the selected eight-thread, batch-one configuration returned correct
  results but exceeded the 1 GiB sampled process-tree RSS gate on two held-out
  seeds: 1,118,076,928 and 1,260,670,976 bytes. The third was 843,984,896 bytes.
  Its all-repeat eligible mean/regret and aggregate baseline ratios are undefined.
  All nine original stdlib/direct reference rows passed their own gates;
  baseline-labelled comparison failures came from the selected intelligent side.
  [Failed selection](../benchmarks/results/optimizer/primary-v2/video.json.gz).
- **Cached HTTP and discovery:** both passed correctness/resource eligibility but
  missed the performance objective, with combined mean regret of 19.51% and
  20.68%, respectively. An `accepted` record describes correctness/resource and
  complete-oracle eligibility, not automatic success on the 10% goal.
  [Cached HTTP](../benchmarks/results/optimizer/primary-v2/cached_http.json.gz),
  [discovery](../benchmarks/results/optimizer/primary-v2/discovery.json.gz).

Primary budgeted, lower-cost secondary, and exhaustive-median selectors chose
identical configurations on all ten completed workloads. Exhaustive-mean differed
only on downloads: four processes, batch one, versus four processes, batch four
for the three median selectors. Exhaustive search therefore did not repair the
shared failed selections. These methods share training observations and are not
independent deployment trials. The secondary's matching result does not make it
a retrospectively chosen replacement primary.

## What was actually compared

The [predeclared protocol](OPTIMIZER-PROTOCOL.md) covers one Linux x86-64 host,
Python 3.12, and eight-CPU affinity. CPU-quota/memory-limit information unavailable
to the environment remains unknown. Workloads use real local HTTP, SQLite,
NetworkX, Pillow, FFmpeg, Poppler and Tesseract execution, with generated mixed
inputs. They are workload replicas, not upstream application integrations,
external-service tests, or a customer production corpus.

The grid is thread/process × workers 1/2/4/8 × batches 1/4/16, always with
profiling interval zero. Four common training fixtures feed frozen selectors;
three distinct held-out seeds are measured afterward. Each trial executes two
passes of 24 tasks. The whole eligible held-out candidate grid defines a
phase-specific hindsight oracle. A candidate must pass every declared seed and
gate; missing/duplicate rows invalidate an exhaustive oracle claim.

References have deliberately different roles:

1. **Fixed stdlib:** ThreadPoolExecutor at the declared eight-worker affinity
   cap, batch one, without discovery. This is neither the uncapped Python default
   formula nor a claim about the user's existing application configuration.
2. **Independently tuned stdlib:** its own exhaustive 96-callback training-only
   grid, using the same candidate set, repeats, combined objective and gates.
   This is a reproducible manual-tuning proxy, not human-expert optimization.
3. **Direct Python:** sequential calls to the same per-item algorithm, with no
   executor. Its direct-service latency is not batched future-completion latency.
4. **Eligible candidate oracle:** lowest held-out phase mean among all-repeat
   eligible intelligent configurations. It is conditional on this grid and
   unavailable to a deployable selector; it is never treated as free discovery.

The same inputs, algorithms and exact result oracles are used across engines.
The reference is executor-independent, not an independent implementation of every
algorithm, so shared algorithm bugs are not ruled out. Engine/candidate order is
seeded and shuffled, but caches, contention and native-library state are not
fully controlled. Matching-configuration engine comparisons are separately
available in [rankings.json](../benchmarks/results/optimizer/primary-v2/rankings.json.gz)
and must not be labelled configuration-selection benefit.

## Combined execution results

All times below are arithmetic means over the three declared held-out seeds, in
seconds. **Cold** includes construction, the first pass and shutdown; **warm** is
the second pass in the same pool. **Combined** is their sum. Warm execution is
not saved-configuration reuse, and cold executor lifecycle does not imply cold
OS/native caches. Cached HTTP creates a fresh per-task cache database, so its
warm phase is pool reuse rather than persistence of a previous task's cache.

`—` means the all-seed eligible aggregate is unavailable, not zero. Video's
reference/oracle times remain individually valid, but its selected aggregate and
all comparison ratios are withheld. OCR has no comparison measurements.
The following tables are derived from the published
[phase-aware ranking audit](../benchmarks/results/optimizer/primary-v2/rankings.json.gz).

### Absolute combined seconds

| Workload | Selected s | Fixed stdlib s | Tuned stdlib s | Direct s | Eligible oracle s |
| --- | --- | --- | --- | --- | --- |
| HTTP | 0.1622 | 0.2536 | 0.1549 | 0.1653 | 0.1572 |
| Images | 0.9535 | 0.9567 | 1.0345 | 4.2171 | 0.9535 |
| Geospatial | 0.6901 | 2.7373 | 0.5894 | 1.7867 | 0.6901 |
| Cached HTTP | 0.4370 | 0.5044 | 0.3817 | 0.4069 | 0.3656 |
| Downloads | 0.4194 | 0.6409 | 0.4469 | 0.4657 | 0.4038 |
| Pipeline | 0.3786 | 1.2669 | 0.3208 | 0.9741 | 0.3454 |
| Deduplication | 0.2896 | 4.0541 | 0.2829 | 0.6321 | 0.2896 |
| SQLite | 0.0914 | 3.2598 | 0.0846 | 0.0849 | 0.0908 |
| Discovery | 0.1616 | 0.1943 | 0.1356 | 0.1346 | 0.1339 |
| Video | — | 2.0058 | 1.9659 | 8.7093 | 2.0180 |
| OCR: reference failed | — | — | — | — | — |

### Ratios of combined means

Every ratio is **reference time / selected time**. Above 1 means selected was
faster in these observations; below 1 means slower. The oracle column uses the
same direction for consistency. Within-10% uses selected/oracle ≤ 1.10, not a
rounded ratio in this table. These are ratios of means, not mean paired ratios.

| Workload | Fixed / selected | Tuned / selected | Direct / selected | Oracle / selected |
| --- | --- | --- | --- | --- |
| HTTP | 1.563 | 0.955 | 1.019 | 0.969 |
| Images | 1.003 | 1.085 | 4.423 | 1.000 |
| Geospatial | 3.967 | 0.854 | 2.589 | 1.000 |
| Cached HTTP | 1.154 | 0.873 | 0.931 | 0.837 |
| Downloads | 1.528 | 1.065 | 1.110 | 0.963 |
| Pipeline | 3.346 | 0.847 | 2.573 | 0.912 |
| Deduplication | 13.998 | 0.977 | 2.183 | 1.000 |
| SQLite | 35.673 | 0.926 | 0.929 | 0.994 |
| Discovery | 1.202 | 0.839 | 0.833 | 0.829 |
| Video | — | — | — | — |
| OCR: reference failed | — | — | — | — |

Among the **nine complete eligible comparison workloads**, selected execution had
lower combined mean time than fixed stdlib in 9/9, tuned stdlib in 2/9 (images and
downloads), and direct Python in 6/9. These conditional descriptions exclude two
unavailable comparisons; they are not 9/9 or 2/9 claims over the full eleven-workload
experiment. OCR/video remain failed or undemonstrated in full coverage. In
particular, large gains over a fixed thread configuration do not imply an
advantage over a separately tuned process/batch configuration.

The objective represents one cold lifecycle plus one warm pass. It is not the
right objective automatically for a long-lived pool: HTTP's selected warm mean
is 0.0785 s versus a phase-specific warm oracle of 0.0369 s, despite a much closer
combined result. Choose the objective to match expected reuse and inspect the
[phase appendix](#appendix-cold-and-warm-phase-tables), rather than treating a
combined score as a recommendation for every lifecycle pattern.

### Observed variation and available parallel work

| Workload | Backend / workers / batch | Batches / concurrency ceiling | Selected range s | Selected MAD s |
| --- | --- | --- | --- | --- |
| HTTP | thread / 4 / 16 | 2 / 2 | 0.1559–0.1664 | 0.0021 |
| Images | process / 8 / 1 | 24 / 8 | 0.8970–0.9982 | 0.0330 |
| Geospatial | process / 8 / 1 | 24 / 8 | 0.6609–0.7219 | 0.0265 |
| Cached HTTP | thread / 8 / 16 | 2 / 2 | 0.3790–0.5037 | 0.0493 |
| Downloads | process / 4 / 4 | 6 / 4 | 0.3687–0.4673 | 0.0449 |
| Pipeline | process / 8 / 4 | 6 / 6 | 0.3697–0.3889 | 0.0076 |
| Deduplication | process / 8 / 1 | 24 / 8 | 0.2601–0.3344 | 0.0143 |
| SQLite | thread / 1 / 16 | 2 / 1 | 0.0848–0.0977 | 0.0062 |
| Discovery | thread / 1 / 4 | 6 / 1 | 0.1348–0.1813 | 0.0127 |
| Video | thread / 8 / 1 | 24 / 8 | — | — |
| OCR: reference failed | — | — | — | — |

Range and median absolute deviation (MAD) above summarize the three selected
combined observations; they are not confidence intervals. Full per-seed timings,
paired ratios, range/MAD and explicit failed/missing slots for **every phase and
reference** remain in the ranking audit. Three correlated seed pairs cannot
establish a success probability; do not infer significance from a small mean gap.

Capacity is calculated from 24 tasks per pass, not measured CPU use. Batch 16
creates only two runnable batches, so four or eight configured workers cannot
run more than two batches simultaneously. Batch four creates six. Equal batch
availability ceilings do not prove equal performance: startup, buffering,
process serialization and noise can still differ. Configurations that differ
only in unused worker capacity may have near-tied observations.

## Discovery cost, live execution and conditional repayment

The training matrix was really executed, but selectors were then **replayed**
against its stored rows. `virtual_callback_seconds` sums actual recorded callback
durations requested by a selector. It is neither the millisecond lookup/replay
wall time nor a measured end-to-end live search duration. Actual experiment time
also includes both engines' training, fixture/reference preparation, the entire
held-out oracle and reference set, adoption checks and supervision. The original
experiment intentionally pays much more than one deployable selector would.

Across ten completed matrices, including video's failed held-out choice:

| Selector | Evaluator calls | Virtual callback seconds |
| --- | --- | --- |
| Primary budgeted median | 720 | 859.819 |
| Exhaustive median | 960 | 1,296.943 |
| Exhaustive mean | 960 | 1,296.943 |
| Secondary lower-cost median | 480 | 506.625 |
| Independently tuned stdlib proxy | 960 | 1,294.411 |

The primary requests 25% fewer callbacks than exhaustive search in this design;
its virtual callback sum is about 33.7% lower here because retained candidates
have different costs. This is an observed component-cost comparison, not a
claimed live wall-time saving. Failed OCR fixture/reference work is additional
experimental cost, not hidden inside these completed-matrix sums. The separate
live pipeline follow-on below measures one actual evaluator path; it does not
convert these original replay totals into live-search wall times. Export/load
checks report zero calibration callbacks; matching identities cannot ensure
unchanged contention or future speed.

Repayment estimates in the ranking audit use one **combined two-pass lifecycle**
as a reuse cycle. Only if the all-seed mean execution saving is positive is the
component estimate defined:

`ceil(max(0, primary discovery cost − reference discovery cost) / mean combined execution saving)`

Fixed/direct discovery cost is zero. The manual proxy's actual virtual discovery
cost is subtracted from the primary's; negative extra cost is reported separately
and floored at zero only for this estimate. A cheaper search with slower execution
has **no positive-savings repayment estimate**. No repayment is assigned to the
hindsight oracle. These estimates omit preparation, holdout, operational adoption
and drift, and assume observed savings recur.

Examples, not a guaranteed deployment payback:

- **HTTP:** primary discovery 16.386 s versus manual 22.951 s. The estimated
  extra component needs 180 cycles versus fixed stdlib or 5,288 versus direct.
  Selected is slower than tuned stdlib, so there is no positive-savings estimate
  against that proxy despite the cheaper search.
- **Images:** the very small mean advantage over fixed stdlib implies 51,264
  cycles; against direct it implies 51. The manual proxy is slower and its search
  more expensive here, yielding zero **additional discovery component** to repay,
  not zero deployment cost or a reliability guarantee.

## Separately scoped corrective follow-ons

### Revised video: completed, without replacing the original failure

The [fresh video plan](../benchmarks/results/optimizer/revised-media/video-plan.json)
and [completed report](../benchmarks/results/optimizer/revised-media/video.json.gz)
use bounded streaming decode buffers, fresh training/holdout seeds and a
post-failure experimental memory margin. Training filters at 715,827,882 sampled
RSS bytes (1 GiB / 1.5), while held-out eligibility stays at the original 1 GiB.
The factor 1.5 is a development choice, not validated universal headroom,
a reservation or runtime enforcement. Multiple changes and a new context prevent
attributing the result causally to that margin alone.

This follow-on completed 171 trials / 8,208 task observations: 96 training, 72
held-out grid trials and three additional fresh-interpreter selected trials.
All 24 candidates were held-out eligible. All four selectors chose eight
processes, batch one. The primary's combined mean, 2.032880 s, equals the exact
observed combined mean oracle; cold was 1.126816 s versus a 1.110298 s cold oracle,
and warm was 0.906064 s, equal to the warm oracle. These remain conditional
three-seed observations, not a global optimum.

Primary selection requested 72 callbacks / 292.698 virtual seconds versus
96 / 442.249 for exhaustive search. Its roughly 0.00136 s replay wall time is
bookkeeping, not live calibration. Standard-library/direct baselines were
intentionally omitted from this follow-on, so no new engine/baseline speedup
claim follows from it.

All three reused-parent and three fresh-interpreter selected trials passed
correctness/resource gates. Their sampled tree peaks ranged from 588,201,984 to
634,281,984 bytes. The pairwise fresh/reused comparison is order-confounded and
compares different parent histories; it does not prove a causal retention fix
or memory-leak freedom. Exact generated reference digests matched. See also the
[smaller decoder diagnostic and scope](../benchmarks/results/optimizer/reliability/README.md#bounded-video-decoding-diagnostic).

### Revised OCR: completed, without replacing the original failure

The [OCR diagnostic history](../benchmarks/results/optimizer/reliability/OCR-README.md)
retains the original failure and nonmonotonic render-size result. Bold 42 and
Mono 42 each passed the 24-document development probe. A corrected Bold corpus
then passed all 480 documents: every one of 120 heading permutations in each of
four profiles, with exact unchanged text truth. The earlier purported 480-document
attempt accidentally repeated one heading and is excluded from the diversity
claim. The corrected run's sampled peak was about 1.884 GiB, so it is not evidence
of passing the primary 1 GiB resource gate.

Those typography checks are post-failure development results. The separately
declared [fresh OCR plan](../benchmarks/results/optimizer/revised-media/ocr-plan.json)
now has a [completed matrix](../benchmarks/results/optimizer/revised-media/ocr.json.gz),
[phase-aware rankings](../benchmarks/results/optimizer/revised-media/ocr-rankings.json)
and [supervisor record](../benchmarks/results/optimizer/revised-media/ocr-supervisor.json).
It uses Bold 42 fixture revision `bold42-exact-text-v2`, training seed source
`20271013`, and held-out seeds `20272011`, `20272017`, `20272023`. Both training
and held-out RSS filters are 1 GiB; the video's experimental 1.5 training margin
does not apply to this OCR run. Exact text truth is unchanged.

The revised matrix completed **273 trials and 13,104 task observations**, with
zero recorded callback/task errors, complete grid coverage, and all 24 candidates
held-out eligible. All four selectors chose eight threads, batch one. The
selected combined mean was **13.033896 s**, versus **12.598101 s** for the eligible
aggregate oracle at eight processes, batch one: **3.45921% regret**. All three
declared combined seed slots were within 10% of their respective per-seed
hindsight minima. The aggregate oracle and per-seed minima are different
comparisons; neither is a deployable selector or a global optimum.

The same cold/warm scopes and reference/selected ratio direction used above apply
to these separate revised OCR observations:

| Phase | Selected s | Fixed stdlib s | Tuned stdlib s | Direct s | Eligible oracle s |
| --- | --- | --- | --- | --- | --- |
| Cold | 6.400065 | 6.410580 | 6.362370 | 34.913572 | 6.400065 |
| Warm | 6.633831 | 6.163670 | 6.137907 | 35.107985 | 6.158661 |
| Combined | 13.033896 | 12.574250 | 12.500277 | 70.021557 | 12.598101 |

| Phase | Fixed / selected | Tuned / selected | Direct / selected | Oracle / selected |
| --- | --- | --- | --- | --- |
| Cold | 1.001643 | 0.994110 | 5.455190 | 1.000000 |
| Warm | 0.929127 | 0.925243 | 5.292264 | 0.928372 |
| Combined | 0.964735 | 0.959059 | 5.372266 | 0.966565 |

Selected combined observations ranged from 12.554762 to 13.430873 s, with MAD
0.314820 s over three seeds. These are descriptive, correlated observations, not
confidence bounds. Selected execution was slower on average than both fixed and
independently training-tuned stdlib, despite being near the intelligent-grid
oracle. Its observed advantage over direct sequential execution does not imply
an advantage over those executor references.

Primary search requested **72 callbacks / 2,620.143 virtual seconds** (43.669
minutes), versus **96 / 4,057.352** for exhaustive search. The manual stdlib
proxy cost 4,068.260 virtual seconds. Primary replay wall time was about
0.00125 s; that is selector bookkeeping, not live calibration. The full experiment
also paid for both training matrices, fixture/reference preparation and held-out
evaluation, which these selector component costs do not represent.

There is **no positive-execution-savings repayment estimate versus fixed or tuned
stdlib**: selected combined execution is slower. A lower discovery cost versus
the tuned proxy does not change that. Against direct Python only, the observed
mean saving is 56.987662 s per combined two-pass lifecycle; the component formula
`ceil(2620.142999660966 / 56.98766152766136)` gives **46 cycles**. This assumes those
savings recur and excludes preparation, independent validation, adoption and
drift; it is not guaranteed end-to-end payback.

The new typography, seeds and source identities define a separate post-failure
experiment. The original OCR reference failure and original **7/11** result are
unchanged; the revised success is not inserted into that denominator.

### Live pipeline: actual evaluator cost measured

The [completed live pipeline report](../benchmarks/results/optimizer/follow-ons/live-pipeline.json.gz)
executed **72 actual calibration callbacks**, rather than replaying a recorded
matrix. Search wall time, including checkpoint I/O, was **56.529 s**; callback
durations summed to **56.343 s**. Preparing the four training fixtures took
**1.274 s**, and their serial references took **1.536 s**, both **outside** that
search timer. Held-out grid/reference work is additional experimental cost.
These scopes distinguish a real search measurement from the earlier virtual
callback accounting without presenting the narrower timer as total adoption cost.

The fresh live search selected eight processes, batch one. Its combined held-out
mean **0.340966 s** matched the eligible aggregate mean oracle, with all three
seed slots within 10%; cold and warm means were 0.249601 s and 0.091365 s.
The original frozen pipeline choice, eight processes/batch four, was retained as
a separate comparator and had 6.96748% combined regret on these new inputs.
This is one same-generator, single-host live run, not evidence that live selection
will always improve the original choice or beat an external executor baseline.

Saved configuration export/load made **zero calibration callbacks**. The probe
also changed actual affinity from eight CPUs to one and verified that loading
the previously saved choice was rejected for hardware-identity mismatch, then
restored affinity. That tests compatibility refusal, not execution quality under
the one-CPU limit; the dedicated affinity experiments remain separate.

### Shifted images: declared heavy-input reweighting completed

The [completed shifted-image report](../benchmarks/results/optimizer/follow-ons/shifted-images.json.gz)
is accepted under its declared
correctness/resource gates. Both the original frozen and freshly live-selected
choices were eight processes, batch one. Combined held-out mean was **1.000755 s**
versus **0.975797 s** for the eligible aggregate oracle: **2.55771% mean regret**.
Only **2/3 combined seed slots** were within 10%, so the successful aggregate
does not turn the remaining seed into a pass. The phase distinction also matters:
cold mean regret was 29.6533% with 0/3 cold seed slots within 10%, while the warm
mean equalled its oracle with 3/3 warm slots within 10%.

The declared transformation repeats the six largest prepared cost-proxy inputs
three times and appends the six smallest, keeping 24 tasks. **Duplicates are
intentional.** This is purposeful reweighting of generated heavy inputs, not a
new upstream population, independent real-world images, or proof of arbitrary
distribution-shift resilience. Both selection and held-out results retain this
scope. Its public projection preserves the original raw report SHA-256:
`29f4e72e4adde4d35acb4aaef97da1d8d1781149bcfbf1211e28b8f55d045cd3`.

### Four affinity experiments: recalibration and strict compatibility

All four same-host experiments at actual one-/two-CPU affinity are complete and
accepted. They used fresh declared seeds and new searches over constrained grids:
six candidates at one CPU and twelve at two CPUs. Together they executed
**432 actual trials and 20,736 task observations**. In every case, the primary's
combined mean equalled its eligible aggregate mean oracle (**0% aggregate
regret**), and all **3/3 combined seed slots** were within 10%.

| Workload / affinity | Backend / workers / batch | Combined mean s | Actual trials / tasks | Primary replay calls | Virtual callback s |
| --- | --- | --- | --- | --- | --- |
| [Images / 1 CPU](../benchmarks/results/optimizer/follow-ons/images-affinity1.json.gz) | thread / 1 / 4 | 4.179050 | 75 / 3,600 | 18 | 77.748 |
| [Images / 2 CPUs](../benchmarks/results/optimizer/follow-ons/images-affinity2.json.gz) | thread / 2 / 4 | 2.290513 | 141 / 6,768 | 36 | 110.916 |
| [Geospatial / 1 CPU](../benchmarks/results/optimizer/follow-ons/geospatial-affinity1.json.gz) | thread / 1 / 16 | 1.802564 | 75 / 3,600 | 18 | 32.876 |
| [Geospatial / 2 CPUs](../benchmarks/results/optimizer/follow-ons/geospatial-affinity2.json.gz) | process / 2 / 4 | 1.075719 | 141 / 6,768 | 36 | 57.068 |

The grid is conditional on each declared CPU budget; these are not global optima.
The replay costs above sum actual training-row durations requested by each search,
not live search wall time. Across the four runs, primary replay requests total
108 calls / 278.609 virtual seconds; complete matrices took 1,252.372 s in total,
including both engines' training and all held-out/reference trials. Shared fixture
preparation/reference costs and replay wall bookkeeping are itemized separately
in each report's `constraint_accounting`. Do not add virtual search cost to actual
matrix cost, since the underlying training executions are already included.

Each run correctly rejected its original eight-CPU saved configuration for a
hardware fingerprint mismatch, with zero calibration callbacks and no adoption
of that incompatible profile. The measured choices came from **recalibration
under changed CPU budgets**, not a transferred configuration or learned scaling
model. Rejection followed by new search demonstrates strict compatibility plus
independently measured constrained-host choices; it is not transfer-learning
performance evidence. Fresh seeds and changed grids also prevent reading these
means as a controlled scaling curve. Cross-machine generalization remains
unvalidated.

### Controlled load transitions and cancellation churn: completed

The [load-transition report](../benchmarks/results/optimizer/follow-ons/load-transitions-final.json)
passed its controlled real-pipeline/image checks with no recorded failures:

- A process backlog diverted eligible CPU work to threads; draining that backlog
  restored the CPU-process preference.
- A thread backlog allowed independent CPU-process progress.
- Sparse, burst and recovery image epochs exercised predictive admission within
  the configured four-thread ceiling, retaining three profile buckets.

The routing and predictive executors completed 11 and 22 accepted Futures,
respectively, and drained their logical/physical queues before closing. These
observations exercise the intended load-aware transitions and bounded admission.
The orchestration gates create known queue states; their elapsed times are not
speedup evidence, and the heuristics are not shown to be optimal or universally
work-conserving. A profile fingerprint match within this process is not
cross-machine learning evidence.

The [bounded cancellation-churn regression](../benchmarks/results/optimizer/follow-ons/bounded-churn-final.json)
also passed: **10,000 queued cancellations** used 4,096-byte payloads while the
10,000 cancelled Futures were deliberately retained by the caller. There were
**zero live cancelled payloads before draining** the blocked worker. Physical
retention peaked at two items under `max_pending=2` and returned to zero after
explicit shutdown. Retained Future objects still consumed traced Python memory;
this is payload-release/item-accounting evidence, not an RSS/byte bound, leak-free
proof, task-preemption feature or arbitrary-callback/deadlock guarantee.

### Original 128-item capacity attempt: failed before workload execution

The [first capacity attempt](../benchmarks/results/optimizer/follow-ons/capacity-pipeline128-failed.json.gz)
requested 128 representative
pipeline items from a generator capped at 64. Fixture preparation failed before
any workload callback started. The report retains **62 unexecuted failure slots**
(32 training, 24 held-out, six baseline), **zero actual trials and zero task
observations**. Its `observed_trials: 62` field is a misleading slot-count label;
it must not be counted as 62 measurements or as evidence of execution throughput.
The per-phase `started_callbacks: 0` and `execution_attempted: false` records
establish what actually ran.

This failed attempt is preserved, not overwritten or repaired in place. Its
public projection preserves the original raw report SHA-256:
`b536d0af1bc82d03946f51ca0c5d02093f9bb36578f5615ca1ff2840e96ec18b`.

### Corrected two-shard capacity follow-on: complete, gates passed

The separately declared [capacity experiment](../benchmarks/results/optimizer/follow-ons/capacity-sharded-v2.json.gz)
completed **62 actual trials and 15,872 task observations**, with no unexecuted
slots, callback errors, correctness/resource-gate failures or source changes
during the run. These are 32 training, 24 held-out and six baseline trials, each
with two 128-item passes. The [fixture-only preflight](../benchmarks/results/optimizer/follow-ons/capacity-sharded-preflight.json)
verified the composed input before measurement; it contributes no executed trial
or reference-task observations.

Each input consists of two distinct, independently seeded 64-item shards,
concatenated without repeating, truncating or relabelling tasks. Every task has
128 pages, for **16,384 pages per pass**. This doubles the unrealizable 8,192-page
work declaration in the failed 128-at-once plan: it changes both composition and
work, so it is not a pure task-count scaling comparison. The eight-candidate grid
fixes batch size at 16 and varies thread/process workers over 1, 2, 4 and 8.
There are eight runnable batches per pass and a computed ceiling of eight
concurrent batches for the selected eight-worker configuration. That is capacity,
not an observed CPU-utilization measurement.

Budgeted median, exhaustive median and exhaustive mean all selected
**process / 8 workers / batch 16**. All eight candidates were eligible on all
three held-out seeds. The selection exactly matches the conditional aggregate
and each per-seed oracle in cold, warm and combined phases: zero observed regret
and 3/3 within 10% for each phase. The oracle remains conditional on this fixed
batch size, generated fixture, host and affinity; it is not a free deployable
baseline or a global optimum.

Mean elapsed seconds and ratios use all three expected seeds. A ratio above one
means the selected configuration is faster. The fixed stdlib reference uses eight
threads and batch 16; direct uses sequential same-algorithm calls. **No independent
training-tuned stdlib baseline was measured in this follow-on.** These comparisons
include engine and configuration differences, not an identical-configuration
engine-speed claim.

| Phase | Selected = eligible oracle (s) | Fixed stdlib (s) | Direct (s) | Fixed / selected | Direct / selected |
|---|---:|---:|---:|---:|---:|
| Cold | 0.393163032 | 1.739450528 | 1.238537446 | 4.424247 | 3.150188 |
| Warm | 0.217991443 | 1.751328653 | 1.251616568 | 8.033933 | 5.741586 |
| Combined | 0.611154475 | 3.490779181 | 2.490154014 | 5.711779 | 4.074508 |

Selected cold times ranged from 0.382860051 to 0.400648277 seconds (MAD
0.004667510); warm from 0.216235219 to 0.220263359 (MAD 0.001240533); combined
from 0.599095270 to 0.620911636 (MAD 0.007455117). Three-seed ranges and median
absolute deviations describe this run; they are not confidence guarantees.

Primary selection requested 24 recorded training callbacks with **49.034953954
virtual callback seconds**, versus 32 callbacks and 75.452967729 seconds for
each exhaustive control: 25% fewer calls and 35.013% less requested recorded
execution cost. Primary replay itself took 0.000857716 seconds; that bookkeeping
time is not live discovery time. Full observed experiment wall time was
**163.493562384 seconds**, including 148.095014687 callback seconds,
6.719533655 fixture-preparation seconds, 8.231961974 reference seconds and
remaining bookkeeping/lifecycle costs. The 896 separate serial-reference task
observations are not included in the 15,872 measured callback-task total.
Virtual selection costs overlap the executed matrix and must not be added to
actual experiment cost.

With zero discovery cost assigned to fixed/direct execution and only the positive
combined mean execution savings, the earlier component-estimate formula gives
18 combined two-pass cycles versus fixed stdlib and 27 versus direct to repay
49.034953954 virtual callback seconds. These exclude preparation, references and
adoption, assume the observed savings persist, and are not measured live
end-to-end break-even points. No tuned-stdlib or oracle amortization is claimed.
The [supervisor](../benchmarks/results/optimizer/follow-ons/capacity-sharded-v2-supervisor.json)
completed without a stop reason and sampled a 201,187,328-byte process-tree peak;
its emergency sampling and the acceptance gates are not hard resource caps.

Protocol SHA-256:
`b9c522aba8f3cb2ecee6be46a001e56ace8b3f57b1449f852cbf95305e2f4235`.
Original raw report SHA-256:
`7b94897146fb651547f32923bf3e08f6be736793070c8930ccd8174d13fd6d81`.
This separate completion does not reclassify the original 62 unexecuted failure
slots or change the original 11-workload quality denominator.

### Release verification and publication gates

The planned heavy experiments are complete, including the corrected capacity
probe and final-adapter endurance. The [final local source verification](../benchmarks/results/optimizer/reliability/final-source-verification.json)
also passed. Built-artifact/install checks, review and CI at the final release
commit are separate publication-workflow requirements; source tests or an earlier
green commit cannot substitute for them. Beta maturity and follow-on measurements
do not authorize publication or establish Production/Stable readiness. All
original failed cases remain part of the evidence.

## Lifecycle reliability is a separate question

The first roughly 20-minute, 22-epoch mixed workload run completed but **failed**
its unchanged 128 MiB final parent-RSS-growth gate: growth was 145,539,072 bytes.
After the completed-Future ownership-cycle correction, a separate run completed
22 epochs and 191,340 task observations with the original gates passing. Final
parent growth was 98,414,592 bytes and the observed tree peak was 577,441,792 bytes.
The first failure remains evidence; allocator/GC diagnostics do not re-score it.
[Original run](../benchmarks/results/optimizer/reliability/endurance-20min.json.gz),
[corrected run](../benchmarks/results/optimizer/reliability/endurance-after-retention-fix.json.gz).

The separate [final-adapter run](../benchmarks/results/optimizer/follow-ons/endurance-final-adapters.json.gz)
also **passed**: 22 epochs over 1,238.920014 seconds, 188,502 task observations,
zero unexpected errors or digest mismatches, and no recorded gate failures.
Final parent RSS growth relative to the warmed fixture/library baseline was
88,805,376 bytes (84.69 MiB), below the unchanged 128 MiB gate. Final thread,
child-process and file-descriptor counts returned to their baseline census
(one thread, one pre-existing child and five descriptors). Expected fault-probe
exceptions and cancellations remain intentional checks, not successful workload
results. The report records the exact source hashes and original raw SHA-256
`4ad0496b920fdfd1efbe14f69cf9fa2fda8facdd3a7a0fe5099ce1098efc8633`.
This later pass does not erase the original failure. Endurance, local source
verification, built-artifact/install checks and exact-commit CI are separate
evidence categories; success in one does not establish success in the others.

This is bounded local endurance, not uptime certification or a proof of leak
freedom. It uses a different small-fixture/lifecycle workload than the 24-task
configuration-quality matrix. Native/library/adapter source hashes identify the
run; do not relabel every later source edit as already endurance-tested.

The supported [executor and migration contract](PRODUCTION-RELIABILITY.md),
[API](API.md), and [security boundaries](../SECURITY.md) matter independently of
benchmark scores:

- Futures are genuine stdlib Futures with executor-owned scheduling/identity;
  callback threads and implicit interpreter teardown are not identical to stdlib.
- Logical unfinished items and physical retained queue/dispatch items are bounded
  by declared capacities. Those are item bounds, not byte or process-tree RSS caps.
- Queued cancellation does not kill running callables. Process dispatch commitment
  can precede the child callable's start. Admission/result timeouts limit waiting,
  not execution; accepted side effects are not automatically retried or rolled back.
- Normal done callbacks run serially on an owned completion pump. A callback must
  not wait for another result from that executor. Slow or unsafe callback code
  can block progress; callbacks are not a fault-isolation layer.
- Use explicit lifetime/shutdown, stop producers, avoid saturated recursive
  submission, and obtain fresh process consent with import/pickle-safe workers.
  Hard deadlines and resource isolation require an external supervisor.

## Verification state and supported evidence envelope

The [final integrated local source verification](../benchmarks/results/optimizer/reliability/final-source-verification.json)
completed on **2026-10-10 at 17:53:22 UTC**, using CPython 3.12.14 on Linux x86-64:
**569 unit/integration tests and eight benchmark tests passed**, with zero failures
or errors. The recorded eight finite examples, catalog synchronization, catalog
CLI and compilation checks also passed. The record includes SHA-256 identities
for 110 source files and explicitly separates this local run from installed
artifact checks and final remote CI.

An earlier local source checkpoint passed 514 tests plus eight benchmark tests
before the subsequent OCR reproducer additions. That checkpoint remains
historical; the final record above identifies the later tested source. Focused
OCR qualification/projection checks used mocked fixtures and did not add a new
media qualification run.

The earlier [exact-commit CI record](../benchmarks/results/optimizer/reliability/ci-validation.json)
records 16 successful jobs for commit
`50fec55e4d5b3c4d8ab8561334c422cffc13863d`: core Python 3.10/3.12/3.14 across
Linux, Windows and macOS, MCP Python 3.10/3.14 across those systems, and a dedicated
Ubuntu/Python 3.12 native-workload job. That is compatibility coverage, not
cross-platform performance validation. **Revised-source CI passed all 16 jobs** at commit
`44699678e9a0ba0295be74344801d46663102436`: [verified run](https://github.com/yflop/concurrent-futures-ai/actions/runs/38056853958)
and [scope record](../benchmarks/results/optimizer/reliability/ci-validation-revised-media.json).
Its fixes concern test portability and archived-source newline preservation;
measured runtime/harness bytes are unchanged. Those 16-job successes apply to the
named earlier commits. The publication workflow separately requires CI on the
final release commit and verification of the exact built/installed artifacts;
this report does not claim those later checks passed. Package publication and
deployment approval remain separate steps.

The evidence supports investigating this bounded local workload envelope with
explicit caller safety and validation. It does not establish external HTTP
service behavior, ArcGIS/upstream application compatibility, cross-hardware
transfer, arbitrary distribution-shift resilience, an enforced p95/RSS/CPU SLA, or safe
arbitrary callables. The matrix has fixed admission/profiling interval zero; it
is not evidence of learned online Predictor benefits. Profiling overhead and
[declared-feature observations](PRODUCTION-EVIDENCE.md#predictive-feature-experiment)
are workload-dependent, so fixed admission remains a legitimate choice.

## Reading archives and reproducing measurements

Large raw reports are now lossless `.json.gz` archives; link targets identify the
shipped representation. The [archive guide and hash manifest](../benchmarks/results/ARCHIVES.md)
explain compressed-byte versus decompressed-payload identity. Storage changes do
not alter the recorded measurements or establish a new successful run.

The three analysis tools `tools.summarize_optimizer`,
`tools.analyze_optimizer_rankings`, and `tools.summarize_workloads` accept archived
reports directly; the optimizer tools resolve archived reports inside an input
directory. Measured reproduction harness source remains unchanged:
`benchmarks.real_world.recheck` and `benchmarks.real_world.optimizer_variants`
expect plain JSON inputs. Before supplying an archived report or `--baseline`,
use bounded, lossless decompression into a new local file:

```sh
python -m tools.report_io benchmarks/results/optimizer/primary-v2/pipeline.json.gz --output /tmp/if-evidence/pipeline-baseline.json
python -m tools.report_io benchmarks/results/representative-priority.json.gz --output /tmp/if-evidence/representative-priority.json
python -m tools.report_io benchmarks/results/representative-remaining.json.gz --output /tmp/if-evidence/representative-remaining.json
```

Pass `/tmp/if-evidence/pipeline-baseline.json` to `optimizer_variants --baseline`,
or the two representative plain-JSON paths to `recheck`. The helper caps decoded
input at 32 MiB, validates UTF-8 JSON, preserves exact payload bytes and refuses
to overwrite an existing destination. Decompression does not execute workloads.
Actual reproduction still requires the [workload dependencies and generated
fixtures](WORKLOADS.md#reproduce), approved repeatable inputs, and separate new
plain-JSON `--output` destinations. Do not overwrite historical evidence or infer
that a new run matches the recorded source, fixture, protocol or results.

## Evidence identity and auditability

Original measurements are public metadata-only projections: local paths and
report-only machine fingerprints are redacted, while numeric measurements,
configuration choices, source/plan hashes and outcomes remain intact. Redacted
saved-profile copies are not loadable profiles. Each JSON records its original
file SHA-256, source identities, per-workload protocol hash, and frozen selection
provenance. The derived [summary](../benchmarks/results/optimizer/primary-v2/summary.json)
and [rankings](../benchmarks/results/optimizer/primary-v2/rankings.json.gz) link the
whole original experiment; individual workload files contain every row.

Key identities make the separation explicit:

- Original source-manifest digest:
  `23d27908876f7c140bbfb5b4093fd4b76fa169e75bb3feb7137689090159376c`.
- Original tuner source:
  `a83cc65456d08dbb993f456f80f4973cedd3c08148f5c9dc34470e4fbac9ee81`.
- Original media adapter:
  `cb71269b9a2f6053bd8b36b1ff39bf296ab505acd14ca871ff05f383a75b1109`.
- Revised-video protocol:
  `355020e1bef0b10829e8721e47cedb751b148476c72b1286f27304ce73706e66`.
- Revised-video media adapter:
  `69884e5e47a43b1520ec8588ea19e81621835a32bacaee49e3392eed84026798`.
- Revised-video original report:
  `22a5c02274a25f8557f34db3c33ac489aa24d29791f33f3b2495599a57b56cd4`.
- Revised-OCR protocol:
  `4fecbf467be13f72f5ac8050cc0353605fae9f4ff0232f480b1da8b1113945d8`.
- Revised-OCR original report:
  `865ab6affd9fc685fbe56f7daf833fb55f4e18e98981a30c9eb3850ec54b9e0a`.
  It uses the revised media-adapter hash above and Bold font bytes
  `a4c5bc453ca281d90ea079e596da7ae0dfeb5777497c29ec254e76d97ff6f890`;
  its protocol also records the Tesseract English trained-data identity.
- Live-pipeline plan:
  `d83888c09b8431067407973de8dc357b34c0b7db044024e3594f8558e88a1932`;
  original report:
  `d6fc3564f374cb6d00df5ca6c194a2cc023d46937d7514c506824b3fa27a2cdb`.

The [protocol](OPTIMIZER-PROTOCOL.md) also documents the earlier superseded matrix
and RSS-collection audit correction. Its nine exploratory checkpoints are not
added to this integrated run as independent confirmations. Resource observations
filter eligibility after execution; even the external watchdog's sampling cannot
prevent every instantaneous spike. Missing collection data fails closed.

## Appendix: cold and warm phase tables

These are the same frozen combined-objective selections, evaluated phase by
phase, not separate cold/warm retuning. Each phase's oracle is the eligible
configuration minimizing that phase's held-out mean; its identity can differ
from the combined oracle. Thus cold and warm oracle values need not sum to the
combined oracle. Ratio direction and unavailable-value rules match the main
combined tables. Full precision, per-seed ratios, MAD/range and failure reasons
are in [rankings.json](../benchmarks/results/optimizer/primary-v2/rankings.json.gz).

### Cold absolute seconds

| Workload | Selected s | Fixed stdlib s | Tuned stdlib s | Direct s | Eligible oracle s |
| --- | --- | --- | --- | --- | --- |
| HTTP | 0.0837 | 0.1274 | 0.0776 | 0.0826 | 0.0779 |
| Images | 0.5863 | 0.4894 | 0.5249 | 2.0920 | 0.5005 |
| Geospatial | 0.4965 | 1.3246 | 0.4324 | 0.8904 | 0.4965 |
| Cached HTTP | 0.2215 | 0.2521 | 0.1924 | 0.2034 | 0.1872 |
| Downloads | 0.3162 | 0.3276 | 0.3327 | 0.2225 | 0.2306 |
| Pipeline | 0.2716 | 0.6393 | 0.2454 | 0.4837 | 0.2552 |
| Deduplication | 0.2233 | 2.0508 | 0.2233 | 0.3190 | 0.2221 |
| SQLite | 0.0473 | 1.6975 | 0.0433 | 0.0428 | 0.0457 |
| Discovery | 0.0834 | 0.0995 | 0.0659 | 0.0706 | 0.0658 |
| Video | — | 1.0174 | 0.9708 | 4.3484 | 1.0048 |
| OCR: reference failed | — | — | — | — | — |

### Cold ratios of means

| Workload | Fixed / selected | Tuned / selected | Direct / selected | Oracle / selected |
| --- | --- | --- | --- | --- |
| HTTP | 1.523 | 0.928 | 0.987 | 0.931 |
| Images | 0.835 | 0.895 | 3.568 | 0.854 |
| Geospatial | 2.668 | 0.871 | 1.793 | 1.000 |
| Cached HTTP | 1.138 | 0.869 | 0.918 | 0.845 |
| Downloads | 1.036 | 1.052 | 0.704 | 0.729 |
| Pipeline | 2.353 | 0.904 | 1.780 | 0.939 |
| Deduplication | 9.185 | 1.000 | 1.429 | 0.995 |
| SQLite | 35.868 | 0.915 | 0.905 | 0.966 |
| Discovery | 1.192 | 0.789 | 0.846 | 0.788 |
| Video | — | — | — | — |
| OCR: reference failed | — | — | — | — |

### Warm absolute seconds

| Workload | Selected s | Fixed stdlib s | Tuned stdlib s | Direct s | Eligible oracle s |
| --- | --- | --- | --- | --- | --- |
| HTTP | 0.0785 | 0.1262 | 0.0773 | 0.0827 | 0.0369 |
| Images | 0.3671 | 0.4673 | 0.5096 | 2.1251 | 0.3671 |
| Geospatial | 0.1936 | 1.4127 | 0.1570 | 0.8963 | 0.1936 |
| Cached HTTP | 0.2155 | 0.2524 | 0.1892 | 0.2035 | 0.0575 |
| Downloads | 0.1033 | 0.3134 | 0.1142 | 0.2432 | 0.1026 |
| Pipeline | 0.1070 | 0.6277 | 0.0753 | 0.4905 | 0.0902 |
| Deduplication | 0.0663 | 2.0034 | 0.0595 | 0.3131 | 0.0663 |
| SQLite | 0.0441 | 1.5623 | 0.0413 | 0.0421 | 0.0134 |
| Discovery | 0.0781 | 0.0948 | 0.0697 | 0.0640 | 0.0299 |
| Video | — | 0.9884 | 0.9951 | 4.3609 | 1.0132 |
| OCR: reference failed | — | — | — | — | — |

### Warm ratios of means

| Workload | Fixed / selected | Tuned / selected | Direct / selected | Oracle / selected |
| --- | --- | --- | --- | --- |
| HTTP | 1.606 | 0.984 | 1.053 | 0.470 |
| Images | 1.273 | 1.388 | 5.789 | 1.000 |
| Geospatial | 7.297 | 0.811 | 4.629 | 1.000 |
| Cached HTTP | 1.171 | 0.878 | 0.944 | 0.267 |
| Downloads | 3.035 | 1.106 | 2.355 | 0.993 |
| Pipeline | 5.869 | 0.704 | 4.586 | 0.844 |
| Deduplication | 30.196 | 0.897 | 4.720 | 1.000 |
| SQLite | 35.463 | 0.937 | 0.956 | 0.304 |
| Discovery | 1.213 | 0.892 | 0.819 | 0.383 |
| Video | — | — | — | — |
| OCR: reference failed | — | — | — | — |
