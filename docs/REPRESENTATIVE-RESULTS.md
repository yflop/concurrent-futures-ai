# Historical representative comparison details

**Historical snapshot, 2026-10-10 at 09:19/09:33 UTC.** These tables summarize the
earlier candidate's representative reports and their recorded source hashes.
They are not the later original integrated optimiser matrix, which reached its
10% goal on **7/11** planned workloads with failures retained. They do not validate
the current source or replace failed rows with revised-source successes. See
[current configuration quality](CONFIGURATION-QUALITY.md) and the
[historical report's scope](PRODUCTION-EVIDENCE.md). Raw values below are unchanged.

Single host; three held-out seeds × cold/warm per workload. Both engines use the same algorithm and separately tune the same grid. Ratios above 1 favor intelligent time. These are observations, not statistically proven universal speedups.

| Workload | Median time ratio | Within 10% | Stdlib / intelligent peak RSS MiB | Stdlib / intelligent CPU seconds, mean lifecycle | Stdlib / intelligent max batch p95 seconds | Errors |
|---|---:|---:|---:|---:|---:|---:|
| images | 0.966 | 100% | 170.1 / 163.5 | 4.392 / 4.538 | 0.252 / 0.259 | 0 / 0 |
| ocr | 0.999 | 100% | 784.4 / 839.1 | 61.181 / 61.642 | 4.146 / 4.057 | 0 / 0 |
| dedup | 1.034 | 100% | 144.7 / 201.3 | 0.899 / 0.870 | 0.208 / 0.115 | 0 / 0 |
| http | 1.019 | 100% | 76.7 / 76.7 | 0.186 / 0.238 | 0.039 / 0.032 | 0 / 0 |
| geospatial | 1.000 | 100% | 88.0 / 88.0 | 1.672 / 1.682 | 0.317 / 0.104 | 0 / 0 |
| cached_http | 1.008 | 100% | 114.0 / 126.9 | 0.545 / 0.559 | 0.073 / 0.161 | 0 / 0 |
| downloads | 1.095 | 100% | 104.0 / 104.0 | 0.468 / 0.434 | 0.103 / 0.087 | 0 / 0 |
| pipeline | 1.090 | 100% | 143.8 / 143.8 | 1.163 / 1.077 | 0.257 / 0.136 | 0 / 0 |
| sqlite | 1.026 | 83% | 105.0 / 105.0 | 0.096 / 0.093 | 0.007 / 0.019 | 0 / 0 |
| discovery | 1.081 | 83% | 105.6 / 105.5 | 0.143 / 0.144 | 0.025 / 0.038 | 0 / 0 |
| video | 1.006 | 100% | 544.4 / 339.0 | 9.623 / 9.613 | 0.467 / 0.472 | 0 / 0 |

RSS is sampled process-tree peak, not exact allocation or a hard bound. Different selected chunk sizes affect completion p95. CPU includes setup/drain and native children on POSIX. Neither errors nor memory improve universally.

## Search cost and conditional repayment

These are execution-search seconds, excluding shared preparation/reference work, held-out validation and adoption effort. Repayment assumes the observed mean saving persists across identical cold+warm cycles; it is not demonstrated long-run amortization. A dash means no positive observed mean saving.

| Workload | Stdlib search seconds | Intelligent search seconds | Illustrative cold+warm cycles to repay intelligent execution search only |
|---|---:|---:|---:|
| images | 15.15 | 14.62 | — |
| ocr | 195.54 | 192.54 | — |
| dedup | 5.05 | 4.79 | 201 |
| http | 1.96 | 1.98 | 456 |
| geospatial | 7.78 | 7.40 | — |
| cached_http | 3.48 | 3.59 | 1070 |
| downloads | 3.04 | 2.92 | 88 |
| pipeline | 4.87 | 4.83 | 106 |
| sqlite | 1.25 | 1.32 | 900 |
| discovery | 1.74 | 1.72 | — |
| video | 29.33 | 30.43 | 3908 |

Source: [representative summary](../benchmarks/results/representative-summary.json), [priority raw report](../benchmarks/results/representative-priority.json.gz), [remaining raw report](../benchmarks/results/representative-remaining.json.gz). See [scope, snapshot differences and acceptance limits](PRODUCTION-EVIDENCE.md).
