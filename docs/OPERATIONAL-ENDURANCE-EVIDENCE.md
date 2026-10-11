# 0.5.1 operational endurance evidence

## Result and limits

The preregistered replacement attempt completed **14,400.017 measured workload
seconds** on one long-lived default-adaptive executor on one GitHub-hosted Linux
runner. All declared correctness, bounded-state, resource-growth, diagnostic and
final-cleanup gates passed. Independent reconciliation checked the raw events,
summary, process/resource samples, terminal reports and exact launched source.

This is a bounded four-hour synthetic-workload observation, not production
certification, evidence of leak-free behavior, a performance advantage, or a
promise about other hosts and applications. The serial callback-publication
constraint remains. Predictive mode received only short lifecycle probes.
TaskService/MCP record longevity was outside this experiment.

- [Original frozen protocol](OPERATIONAL-ENDURANCE-PROTOCOL.md)
- [Preregistered host-change and interruption addendum](OPERATIONAL-ENDURANCE-RESTART.md)
- [Hosted run 38109973786, attempt 1](https://github.com/yflop/concurrent-futures-ai/actions/runs/38109973786)
- [Measured source e0079e05](https://github.com/yflop/concurrent-futures-ai/tree/e0079e050682931b7cb96e13c5fdf88babebbc90)
- [Operational compatibility and safe patterns](OPERATIONAL-COMPATIBILITY.md)
- [Separate sustained-performance comparison](ADOPTION-SUSTAINED-EVIDENCE.md), which found no supported advantage over tuned stdlib/hybrid alternatives

## Exact scope

The attempt ran on 2026-10-11, from approximately 03:56:40 to 07:57:46 UTC.
The 60-second warmup, sixteen diagnostic pauses and final lifecycle operations
were additional to the four measured workload hours. Total harness elapsed time
was 14,465.883 seconds; supervisor elapsed time was 14,466.370 seconds.

Actual host: Ubuntu runner image `20261004.327.1`, Linux
`6.17.0-1022-azure`, x86-64, glibc 2.39, four available CPUs, 16,766,410,752 bytes
of physical memory, assertion-enabled CPython 3.12.14 built with GCC 13.3.0, and
psutil 7.2.2. The host was not the interrupted local attempt's host.

The primary executor kept two thread workers, two spawn-process workers,
`max_pending=32`, trace capacity 64 and profile-key capacity 16. Sixteen
900-second cycles repeated paced, burst and bounded-Future-retention phases.
The declared mix used blocking tasks, checksum CPU work, 64-KiB payload/result
work and intentional exceptions, with deterministic cancellation attempts,
short callbacks and key churn. CPU/failure tasks exercised unknown-kind default
adaptive scheduling; the primary executor did not enable predictive mode.
The same two process-worker PID/creation-time identities survived every warmed
quiescent checkpoint. No process was replaced in the primary four-hour pool.

The fifteen loaded runtime files have canonical manifest digest
`7774e028757fd04922b22db1cb61c5483a147fcfd11f25015536094df8f4ba4e`.
The measured harness, supervisor and original protocol hashes remained:

| File | SHA-256 |
| --- | --- |
| `tools/operational_endurance.py` | `4202e4a01f4f5a53f7f854ad4fb7b1e3d7601d9894b8bc840091fcdc1d3497f7` |
| `tools/supervise_endurance.py` | `5fd9bf150fda82051611c49eb2f71cc463b5ec045fb2bd421470f6263b0da2dc` |
| Original protocol | `384435dc15821af4c3ba88cc761ab9cd4cbd941ea681262e1b77c4a43b2dfbed` |

The later CI-helper portability fix at
`b8328cbdb04dfe7ded1944c1352f69e86bc90d43` is a different source commit.
It canonicalizes helper manifest paths and fixes the ordinary CI checkout action;
it does not change the measured runtime, harness, supervisor or protocol.
Its [16-job CI result](https://github.com/yflop/concurrent-futures-ai/actions/runs/38110949953)
is separate from the long experiment's identity.

## Failure-inclusive denominators

Primary admissions, including warmup and health checks:

| Outcome | Warmup | Post-warmup | Total |
| --- | ---: | ---: | ---: |
| Attempted / accepted | 9,600 | 6,627,920 | 6,637,520 |
| Rejected | 0 | 0 | 0 |
| Successful | 7,917 | 5,463,256 | 5,471,173 |
| Expected exceptions | 412 | 284,053 | 284,465 |
| Cancelled | 1,271 | 880,611 | 881,882 |
| Unexpected failures | 0 | 0 | 0 |
| Cancellation attempted | 1,372 | 946,843 | 948,215 |
| Cancellation returned false | 101 | 66,232 | 66,333 |

The post-warmup column includes sixteen successful health-check tasks after the
isolated diagnostic pauses. Thus **6,627,904 are measured workload jobs**, plus
sixteen health checks. Successful cancellation counts equal actual cancelled
outcomes; planned exception tasks cancelled before execution are cancellations,
not missing exceptions. Every accepted primary Future reached an observed
terminal outcome, and all 6,637,520 attached callbacks were counted.

The isolated diagnostics have their own denominator: sixteen sets of 38 accepted
tasks, or **608 auxiliary admissions** = 352 successes + 144 expected failures +
112 cancellations. There were also 64 expected rejected submissions. These are
not added to the primary workload totals. They exercised bounded admission,
reentrant worker/callback rejection, cancellation release, repeated shutdown,
process crashes and explicit recovery using a new executor. A crashed pool was
not repaired, and application work was not replayed.

Sixteen short predictive probes accepted 256 tasks in total, all routed to
threads, with four-profile/eight-trace limits. Their reported execution intervals
sum to approximately 0.107 seconds within approximately 4.088 seconds of all
auxiliary diagnostics. This provides only a short lifecycle check, not four-hour
predictive endurance or multi-backend predictive coverage.

## Bounds, trends and final census

The independent audit reconciled 2,992 raw events, including 1,445 active resource
samples and eighteen quiescent checkpoints: one fully warmed baseline, sixteen
cycle boundaries and the final drained state. The watchdog recorded 28,534
separate process-group/RSS samples.

Observed historical logical-pending and physical-retention peaks were both 29,
below the declared limit of 32. In-flight backend peaks were two each. The final
trace recorded 6,637,520 routes, retained 64 and dropped 6,637,456. Profile and
adaptive-key state remained at or below sixteen; 5,755,622 profile evictions
confirmed actual churn. Final primary routing counts were 3,809,838 thread and
2,827,682 process submissions, including warmup and health checks. These counts
are operational evidence, not a throughput comparison.

| Quiescent resource | Warmed baseline | Final drained | Change | OLS trend per hour |
| --- | ---: | ---: | ---: | ---: |
| Parent RSS, bytes | 29,274,112 | 30,408,704 | +1,134,592 | +110,313.94 |
| Tree RSS, bytes | 94,687,232 | 96,059,392 | +1,372,160 | +132,525.94 |
| Parent file descriptors | 16 | 16 | 0 | 0 |
| Python threads | 6 | 6 | 0 | 0 |
| Primary process workers | 2 | 2 | 0 | 0 |

The slopes use all eighteen checkpoint values against recorded monotonic elapsed
hours, including diagnostic pauses, as declared before the run. Parent/tree RSS
increases were approximately 1.08/1.31 MiB, below the 128/256-MiB quiescent growth
gates. Maximum quiescent growth above baseline, rather than just endpoint
growth, was 1,708,032 parent bytes and 1,929,216 tree bytes. Positive growth was observed; this is not a proof of absence of leaks.

Active primary samples peaked at 31,072,256 bytes parent RSS and 96,714,752 bytes
tree RSS, with sixteen parent FDs, seven Python/native threads and two primary
process workers. The independent watchdog, which could also sample auxiliary
pools, peaked at 122,335,232 bytes tree RSS, below its 1-GiB emergency threshold.
Sampling is not a hard OS memory quota and can miss shorter peaks.

After explicit shutdown the harness had one Python/native thread, six FDs and
zero task-process children. One multiprocessing resource tracker was still
present in that immediate census; the supervisor then observed an empty owned
process census, return code zero, no errors and no stop reason. Its 15,000-second
wall and 90-second productive-progress limits were unchanged. This verified
cleanup applies to this hosted attempt only.

## Interrupted attempt retained

The earlier local attempt has no completed summary or final watchdog result.
Only 2,502.246 durable measured seconds, 530 events, 5,125 watchdog samples and
two completed diagnostic checkpoints survived the execution-environment recovery.
A previously reported later checkpoint did not survive the filesystem rollback
and was not reconstructed. The underlying interruption cause and inaccessible
old namespace's cleanup remain unknown. Its counters and elapsed time are never
combined with the successful replacement attempt.

## Evidence, projection and reproduction

The [campaign ledger](../benchmarks/results/operational-endurance-campaign-v051.json)
identifies every original artifact, public derivative and source layer. The
original hosted ZIP digest is
`9013c6c775882ca83d638998eca4ef2b25d78e13d5e87278b1a9bf7da9ebf04c`,
verified against GitHub's artifact digest. Original raw files are retained
unchanged. Hosted artifacts have short service retention; the committed
projections preserve the observations after that service copy expires.

Public bundles:
- [Hosted summary](../benchmarks/results/operational-endurance/github-v1/operational-endurance-github-v1-summary.json)
- [Hosted archive ledger](../benchmarks/results/operational-endurance/github-v1/archive-ledger.json), including all 2,992 events and 28,534 watchdog samples
- [Independent consistency audit](../benchmarks/results/operational-endurance/github-v1/operational-endurance-github-v1-independent-audit.json)
- [Interrupted-attempt ledger](../benchmarks/results/operational-endurance/interrupted-v1/archive-ledger.json)
- [Paired 0.5.0 / 0.5.1 semantics ledger](../benchmarks/results/operational-endurance/semantics-v1/archive-ledger.json)

`tools/archive_endurance_evidence.py` creates deterministic gzip/JSON derivatives
and records original, public-payload and archive hashes. It replaces local paths
only. Numeric, boolean and null observations, object/list structure and record
counts are checked for exact preservation. Source identities remain explicit:
published 0.5.0, measured 0.5.1, the interrupted local attempt and the fresh hosted
attempt are distinct. Public CI inventory hashes refer to original bytes, not to
path-redacted derivatives; this distinction must be retained when verifying them.

`tools/audit_endurance_evidence.py` checks the original extracted CI artifact
against the preregistered plan, exact launched commit/run/attempt and independently
verified 320-file launch manifest. It rejects incomplete reports, dropped outcome
categories, per-kind denominator defects, nonfinite observations, incomplete
inventories, altered source, inconsistent diagnostics/trends and failed cleanup.
Its synthetic fixture tests exercise the analysis code only; they are not
empirical endurance observations. The analysis tools have 37 passing tests.

For an original extracted artifact, the audit invocation is:

```sh
python tools/audit_endurance_evidence.py \
  --evidence /path/to/original-extracted-artifact \
  --plan benchmarks/results/operational-endurance-github-v1-plan.json \
  --expected-commit e0079e050682931b7cb96e13c5fdf88babebbc90 \
  --expected-run-id 38109973786 --expected-run-attempt 1 \
  --expected-source-files benchmarks/results/operational-endurance/github-v1/expected-source-files.json \
  --output /path/outside-artifact/audit.json
```

Re-running the workload creates a new attempt with its own host identity and
failure-inclusive evidence. It does not reproduce the historical observations
merely by using the same source or defaults.
