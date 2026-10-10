# Original integrated configuration-quality matrix

This is the preserved original attempt across all eleven declared workloads, on
one Linux x86-64 host, Python 3.12, with eight-CPU affinity. Exact runtime/software,
source hashes, commands, seeds and limits are in each report. Metadata-only public
projection removes local paths and report hardware fingerprints; it leaves all
measurements and outcomes unchanged. Redacted saved-profile copies are evidence,
not loadable profiles.

The 24 configurations are thread/process × workers 1/2/4/8 × batches 1/4/16.
Each trial performs two passes of 24 identical seeded tasks. Selection uses four
training fixtures; three distinct held-out fixtures are never used for selection.
The predeclared primary has a 72-callback budget, the secondary 48, and exhaustive
controls 96. Costs are sums of actual recorded callback durations requested by a
replayed search, not observed end-to-end live search wall time.

Ten workloads completed 2,730 trials and 131,040 task observations. OCR failed its
executor-independent exact-text reference before calibration: two documents read
`DELTA` as `DELIA`. It remains failed in the original eleven-workload denominator.
Video completed all trials without correctness mismatches, but its selected
8-thread/batch-1 configuration exceeded the 1 GiB sampled-RSS eligibility gate on
two of three held-out seeds. Its aggregate eligible regret is undefined.

Primary within-10%-of-eligible-oracle coverage is **7/11 workloads** and **20/33
combined seed slots**. Cached HTTP and discovery also miss the 10% objective.
The primary, lower-cost secondary and statistic-matched exhaustive-median control
chose the same configurations on the ten completed workloads. Exhaustive-mean
differs on downloads; all four agree on the failed choices, so exhaustive search
did not repair those generalization/resource failures. No later
fixture, adapter or safety-margin follow-on replaces these observations.

Among the nine workloads with complete eligible automatic/baseline comparisons,
automatic selection had lower observed mean combined time than the fixed stdlib
configuration in 9/9, independently training-tuned stdlib in 2/9, and direct Python
in 6/9. These descriptive three-seed results do not establish general superiority.
`rankings.json` preserves absolute cold/warm/combined timings, separate baseline
ratios, paired-seed ranges/MAD, failure slots, effective batch-capacity ceilings,
and conditional discovery-cost repayment estimates only for positive execution
savings. A hindsight oracle is not treated as a free deployable baseline.

`summary.json` includes all planned workload/seed slots, including failed and
unobserved slots. Its `partial` status denotes incomplete evidence, not a running
job. Method cost totals cover completed workload reports; failed reference setup
is additional experimental cost. Baseline-labelled video failure strings describe
comparison-pair failure from the intelligent selection; the nine raw stdlib/direct
baseline rows themselves passed their sampled resource gates.

At batch 16 there are only two runnable batches per pass, regardless of configured
workers above two. Capacity fields are computed work-availability ceilings, not
measured CPU utilization. RSS is sampled absolute parent-plus-descendant memory,
can double-count shared mappings and miss spikes, and is not runtime enforcement.
Later revised-media evidence has separate source/protocol identities and seeds.
