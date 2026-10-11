# Fixed-preconditioned mixed workload: no supported adoption advantage

## Result

A new, separately preregistered experiment found **no supported performance advantage** for the reviewed 0.5.1 candidate over tuned standard-library executors or tuned hand-built stdlib hybrids. This was a new warm-pool experiment, not a reinterpretation of the earlier 0.5 cold results.

All **216 observations completed with correct results**: 96 training observations, followed by 24 untouched paired blocks containing five modes (120 held-out observations). One training configuration, stdlib eight threads/batch four, exceeded the predeclared 5-second empirical latency cap. That observation is retained, and the configuration was ineligible for selection. No held-out observation breached an absolute correctness/resource/latency cap. Relative performance and tail gates still rejected the candidate.

The candidate used two threads, six processes and batch size four. Its typical paired elapsed time was approximately **11.0% longer than tuned eight-process stdlib** and **6.0% longer than the tuned static hybrid**. Its small point improvement over the tuned overflow hybrid did not clear the practical-gain bound and came with observed HTTP latency regressions. No adoption recommendation is emitted by this standalone benchmark.

## What was actually measured

Each fresh observation process started a separate loopback HTTP server process, built fresh application fixture paths, constructed fresh executor/policy state, completed **32 disjoint preconditioning jobs**, then executed a **256-job measured stream** (128 graph jobs and 128 HTTP jobs) before waiting shutdown. The graph jobs used the existing NetworkX route-length checks and generated geographic-grid fixtures; HTTP jobs used actual Requests transfers and exact response-byte digests. Every result also matched the ordered sequential reference for that observation.

The server had a separate interpreter/GIL, so graph work on client threads did not directly hold the server's GIL. Server CPU and memory were still included in resource accounting. The client, workers and server shared affinity CPUs 0–7. The worker budget counts executor workers, not server/handler/support threads.

This is a finite, saturated, closed-loop **fixed-preconditioned session**, not a continuously operating production service or an open-loop arrival/latency test. Selected candidate warm streams lasted 1.068–1.462 seconds. The study establishes neither minute-scale steady-state throughput nor internet-service behavior. Pools did not restart between preconditioning and the measured stream. Worker identity records confirm that every configured thread/process worker executed preconditioning work in all 216 observations, and no new worker first appeared during the measured stream. This verifies operational pool warmup; it does not establish converged policy learning.

Preconditioning did **not** establish converged adaptive learning. Across 24 candidate sessions × three graph keys, none of the 72 key/session pairs had the required uncontended samples on both backends, either immediately after preconditioning or at session end. The candidate remains the default adaptive implementation, but these measurements primarily expose its hint/exploration/queue-pressure behavior in this particular continuously backlogged stream. They do not rule out benefits from sparse workloads with genuine idle gaps and mature comparable profiles.

## Frozen selections and paired results

Single-pool stdlib, static stdlib hybrid, overflow stdlib hybrid and intelligent grids each contained eight configurations. Each received three common-seed training blocks; all selections were frozen before any held-out fixture ran. Worker budgets were four/eight, batch sizes one/four, and hybrid splits were 2+2, 4+4, 2+6 and 6+2. The caller enforced the same 32 accepted-but-uncollected logical-job bound across engines and batch sizes. Every engine used the same homogeneous tuple-batch kernel and FIRST_COMPLETED collection.

| Role | Frozen configuration | Median warm seconds | Candidate median paired speedup over reference | One-sided lower bound |
| --- | --- | ---: | ---: | ---: |
| Fixed stdlib | 8 threads, batch 1 | 7.353 | +510.03% | +439.28% |
| Tuned single stdlib | 8 processes, batch 1 | 1.079 | −9.94% | −16.84% |
| Tuned static stdlib hybrid | 2 threads + 6 processes, batch 1 | 1.113 | −5.67% | −15.38% |
| Tuned overflow stdlib hybrid | 2 threads + 6 processes, batch 4 | 1.247 | +2.88% | −4.83% |
| Intelligent finalist | 2 threads + 6 processes, batch 4 | 1.214 | — | — |

Paired speedup is **baseline elapsed / candidate elapsed − 1**; it is not percentage reduction in elapsed time. The absolute medians are descriptive and are not divided to derive the paired results. The sign-bound target is the median independent paired log ratio, not mean throughput or population request latency.

All four comparisons failed at least one declared gate:

- Fixed stdlib showed a large gain, but paired log-ratio MAD was 0.100790, just above the unchanged 0.10 gate. The cap was not relaxed after seeing this result.
- Tuned single stdlib and static hybrid showed aggregate regressions, inadequate practical-gain bounds, worst observed elapsed regressions beyond 25%, and relative empirical p95 regressions.
- Tuned overflow showed only 11 of 24 strict wins above the 5% margin, with a negative lower bound and a relative HTTP p95 regression.

The largest observed candidate/reference HTTP p95 ratio was 1.827 versus tuned single-pool stdlib, 7.053 versus the static hybrid, and 4.284 versus overflow. These are maxima of paired fixture-level empirical p95 ratios, **not** population p95 estimates. Candidate HTTP p95 was at most 0.330 seconds; small baseline HTTP tails can make a relative regression large. On the seed producing the largest overflow ratio, the candidate was 315.29 ms versus overflow 73.60 ms and static hybrid 44.70 ms. Every logical job is weighted once in the nearest-rank quantile, even when jobs share a containing batch's completion timestamp. The latency clock is requested submission to done callback, not external end-user arrival to response.

The candidate ran 35 of its 3,072 held-out measured graph jobs on client threads; static and overflow finalists ran their graph jobs on processes. All candidate/static/overflow HTTP jobs ran on client threads. These routing observations do not by themselves establish what caused any timing difference.

## Gates, independence and costs

The single prespecified primary endpoint was elapsed time to complete the fixed measured stream. The candidate had to beat all four reference roles with a strict >5% lower bound. Family alpha 0.05 was split into four comparisons (0.0125 each). With 24 pairs the exact bound uses the seventh ascending log ratio; at least 18 guarded strict wins above the margin are needed. The same positive 1e-12 log-margin guard as evaluator v2 was used.

Additional gates required positive aggregate saving, no observed pair more than 25% slower, paired log-ratio MAD ≤0.10, timings ≥0.001 seconds, relative overall/per-family empirical p95 ≤1.25× reference, absolute p95 ≤5 seconds, lifecycle CPU ≤120 seconds, sampled process-tree RSS ≤1 GiB, and executor lifecycle ≤60 seconds. Resource caps are observed rejection thresholds, not hard OS quotas. A separate recursive watchdog imposed 90-second observation and 60-minute campaign emergency limits and verified cleanup.

Maximum observed lifecycle CPU was **14.36 seconds**, maximum lifecycle elapsed **9.96 seconds**, and maximum per-observation sampled tree RSS **501,985,280 bytes** (478.7 MiB). The whole-campaign supervisor peaked at 547,643,392 bytes including orchestration and all owned descendant sessions. It exited successfully with a complete, empty live-process census.

The full campaign took **2,521.19 seconds (42.02 minutes)**. The retained eight-observation development pass took **82.85 seconds**, giving **2,604.05 seconds (43.40 minutes)** of recorded development plus campaign preparation/oracles/search/held-out/internal-analysis time. Subsequent external review and documentation time are additional, not included in that numerator. The campaign executed 55,296 measured logical jobs plus 6,912 preconditioning jobs (62,208 combined), and 62,208 additional sequential-reference jobs; development added 2,304 in each category. These counts are fixture work, not an operational throughput promise.

The candidate did not establish savings against the strong references, so no finite adoption break-even or reuse recommendation is claimed. No actual application reuse horizon was supplied. The initial development timings were used only to check feasibility and estimate runtime; they did not select configurations or enter inference.

Each measurement used a fresh interpreter, fixture paths, pools and policy state. Every paired block used the same generated input bytes/metadata, verified by content digests. Run order was randomized; OS/page caches were uncontrolled. Independent representative blocks remain an experimental assumption. Random seeds and randomized ordering cannot prove independent shared-host timing noise. This is one host and generated local data, not production-corpus or cross-hardware transfer evidence.

## Source, raw evidence and verification

The runtime base was published 0.5.0 commit `7c95c6fac134e2aaf53245dc20ac0e195bf5e6ed`, obtained from release ZIP SHA256 `7b18e8288c95155b763be82f1f03807fbcbda53438a28367b3a47e1a127d9940`. The measured runtime was the independently reviewed **0.5.1 candidate**, containing the narrow reentrant-admission guard and version update. Its canonical 15-file package manifest digest was `7774e028757fd04922b22db1cb61c5483a147fcfd11f25015536094df8f4ba4e`. This experiment does not measure a before/after speed effect of that guard and is not a claim about an already published 0.5.1 release.

- [Preregistered protocol](SUSTAINED-PERFORMANCE-PROTOCOL.md)
- [Measured benchmark implementation](../benchmarks/real_world/adoption_sustained.py)
- [Recursive campaign watchdog](../tools/supervise_sustained.py)
- [Complete confirmation report](../benchmarks/results/adoption-sustained-v051.json.gz)
- [Complete development report](../benchmarks/results/adoption-sustained-development-v051.json.gz)
- [Exact scalar/selection/order recomputation](../benchmarks/results/adoption-sustained-recomputation-v051.json)
- [Resource/process-cleanup supervisor result](../benchmarks/results/adoption-sustained-supervisor-v051.json)
- [Evidence archive ledger](../benchmarks/results/adoption-sustained-archives-v051.json)

Seventeen deterministic harness tests and five supervisor tests passed before confirmation. Three additional projection tests check metadata removal, numerical preservation and punctuation; the four new test modules also passed in the core-only environment (21 passed, seven optional-psutil watchdog tests skipped). Three additional read-only audit tests cover complete synthetic replay and rejection of modified timings, missing slots, changed input digests/order, and changed selections. Public reports are explicitly labeled metadata-redacted derivatives: local interpreter, workspace and output paths and non-public execution bookkeeping are removed. Original bytes remain preserved, and the archive ledger binds original, public JSON and compressed-file hashes. Every numeric/boolean observation, the complete plan, source hashes, selections and comparisons remain exactly unchanged. The public protocol is a labeled path-redacted projection of the original frozen text, whose hash remains recorded.

The completed report's 216 slots, all randomized orders, paired input digests, training selections and four stored comparisons were recomputed. The common scalar gates also matched the stock evaluator v2; normalized mean-saving arithmetic uses a documented 1e-12 relative/absolute tolerance. No workload was executed during this replay.

The stock convenience adapter remains cold-lifecycle only, and the stock evaluator does not express stdlib hybrid comparator configurations. This benchmark is separate caller-measured evidence; it does not silently add warm adapter support or issue a stock AdoptionRecommendation.

For read-only replay from this checkout:

```sh
PYTHONPATH=src python tools/summarize_sustained.py benchmarks/results/adoption-sustained-v051.json.gz
```

A new measurement is a new budgeted experiment. Do not overwrite these results, rerun until a positive outcome appears, relax a gate after seeing its result, or pool this campaign's best number with other workload families. A useful next performance investigation should be grounded in an actual application's workload/arrival/cache constraints, with equally strong stdlib alternatives and a separately declared hypothesis.
