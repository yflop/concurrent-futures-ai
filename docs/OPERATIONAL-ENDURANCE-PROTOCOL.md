# Operational endurance protocol, revision 1

Declared before measurement. This is a single-host reliability experiment, not a
production certification, performance comparison, or a hard resource guarantee.
The published 0.5.0 evidence is preserved separately. The candidate adds only the
reviewed fail-fast reentrant-admission guard and a fixed rejection counter.

## Identity and duration

- Refuse optimized Python: the harness requires enabled assertions and records
  sys.flags.optimize. Record Python/platform, source-module hashes, harness hash, protocol hash,
  package version, affinity, psutil version, and the published base commit.
- A 60-second warm-up is followed by **14,400 measured seconds (four hours)** on
  one unchanged long-lived executor, then explicit draining and a final census.
  Measured workload-window time includes normal pacing and resource sampling;
  periodic diagnostics and quiescent drains are additional time, excluded from
  the four-hour workload-window denominator. Executor lifetime is longer.
- Short development runs are labeled smoke tests and never counted as endurance.
- Run serially after the performance campaign; do not overlap CPU-heavy jobs.
- Default adaptive policy (predictive routing disabled for the primary pool),
  two thread workers, two spawn process workers, max_pending 32, trace capacity
  64, profile-key capacity 16. One orchestrator supplies bounded batches of 32.
- Diagnostic lifecycle/crash pools run while the primary pool is quiescent.
  At most one auxiliary process worker exists at a time; expected maximum is
  three task children plus the multiprocessing resource tracker.

## Repeated workload and bounded state

Every fifteen-minute cycle repeats ten minutes steady mixed load, three minutes
bursty saturation, and two minutes result-retention/profile-key churn. The same
primary executor and process workers remain alive throughout all sixteen cycles.
All phases use a deterministic task-kind cycle: 40% sleep-based blocking tasks,
35% integer-checksum CPU tasks with unknown-kind hints for adaptive learning,
20% 64 KiB payload/result transfer with CPU hints, and 5% intentional ValueError
tasks with unknown-kind hints. Blocking tasks use IO hints; every task grants
explicit process consent. Record observed thread/process routing rather than
assuming unknown tasks use a particular backend. Payload CPU hints exercise the
process route; blocking IO hints exercise threads.
This is an operational synthetic fixture; sleep is not a claim to represent all
network or storage systems. Validate each successful result against a fixed
oracle. Alternate accepted Future traversal order. Never retry task bodies.

Steady batches are paced to at most 160 attempted tasks/second. Bursty batches
have no artificial delay. Retention phases keep only the newest 64 completed
Futures (at most 4 MiB of byte results, plus bounded Future/exception overhead),
then discard the ring at phase transitions. Other phases discard every batch.
All phases use exactly 64 rotating workload keys, key-(sequence mod 64),
against a profile budget of 16. The repeatable key/workload overlap intentionally
exercises adaptive profile churn; this is not a realistic workload classifier.
Attempt cancellation on every seventh submitted Future; a false cancel is a
recorded outcome, not a failure. Register short callback counters on every
Future. Every twentieth batch adds a finite 2 ms callback delay to exercise
publication pressure without deliberately blocking forever.

At every fifteen-minute boundary, with the primary pool quiescent:
1. Block one auxiliary thread worker; enqueue and cancel seven queued payloads;
   assert payload release, successful cancellation, and wait/as_completed wakeup.
2. Exercise finite timeout admission, worker and callback reentrant fail-fast,
   then wait=False / wait=True / cancel_futures escalation and final accounting.
3. Submit an intentional os._exit worker crash in a separate one-process pool;
   require BrokenProcessPool on every affected accepted Future and later process
   rejection, without replay or thread fallback. Close that pool.
4. Construct a **new** one-process executor, validate successful work and clean
   teardown. There is no claim of repairing a broken executor in place.
5. Run a separate 16-task predictive-mode lifecycle probe with two thread
   workers, at most one process worker, max_pending 8, max_concurrency 1, four
   profile slots and eight trace slots. Validate results, profile bounds and
   explicit nonwaiting/waiting shutdown; record actual routes. These short probes
   do not establish four-hour predictive-policy endurance.
6. Verify long-lived executor health again. Preserve all checkpoint failures.

## Measurements and invariants

Append raw JSONL samples at least every ten seconds, plus each phase/cycle and
fault outcome. Flush each checkpoint; replace the small current checkpoint
atomically for watchdog observation. Results include all submitted, settled,
expected-failed, cancelled, rejected, callback, crash, and recovery counts.
On unexpected error, write its type and stage plus current counters and census,
stop submitting, attempt bounded externally supervised drain, and mark failed.
A stopped, hung, or interrupted run is never promoted to passed.

Use public executor snapshots for pending/physical queued/in-flight counts,
peaks, profile count, route counts, trace length/evictions, and reentrant rejects.
Check snapshot.pending <= 32 and physical.retained <= 32, each in-flight count
<= its worker budget, trace <= 64,
profiles <= 16, adaptive tracked_keys <= 16, and completed + failed + cancelled
<= submitted. Reconcile trace recorded == submitted and dropped == recorded -
retained at quiescence; require nonzero evictions after 64 accepted tasks. Verify
primary process worker (PID, creation time) identities match the warmed baseline. At quiescent
checkpoints require equality, zero pending/physical retention, and matching
observed Future outcomes/callback counts. Maintain only fixed counters and a
bounded sample ring in memory; full sample history is streamed to disk.

An independent sampler thread observes active state every ten seconds. Only
actual workload/diagnostic progress refreshes the supervisor progress checkpoint;
sampler activity cannot hide a stalled workload. Atomic locked callback counters
are compared at quiescence; task-kind denominators separate true/false cancel
attempts and intentional failures cancelled before execution.

Sample parent and child RSS, file descriptors, native/Python thread counts,
child identities/status, queue depths, and completed-Future retention. Record
RSS/FD/thread/child peaks. Compare resource values only at equivalent warm,
quiescent checkpoints: fully warmed baseline, each drained 900-second cycle boundary, and
final drain. Report first/last/delta and ordinary least-squares slope per hour
using all these checkpoints (elapsed monotonic hours against resource values),
without discarding outliers, across those
checkpoints, plus every raw sample; allocator plateaus are not a leak proof.

Predeclared failure gates after warm-up: parent RSS growth >128 MiB, process-tree
RSS growth >256 MiB, parent FD growth >4, Python thread growth >2, or extra worker
children surviving two seconds after auxiliary teardown; final census permits
only one persistent resource-tracker child and at most four associated FDs.
These are observational gates, not enforcement of hard memory/CPU budgets.

## Watchdog and stopping

Use an independent process-tree supervisor: 15,000-second total wall limit,
1,024 MiB sampled emergency RSS threshold, and 90-second stale progress limit.
Every task/gate/result wait has a finite deadline (normally ten seconds).
The supervisor records the triggering reason, sends SIGTERM to the owned process
group, waits ten seconds, then SIGKILLs only that owned group if needed. Persist
supervisor status and perform an owned-PID census after completion. A failure
stops the campaign; diagnose it without deleting or overwriting failed evidence.
The intended success condition is four full measured hours with all invariants,
repeated fault/recovery checks, explicit shutdown, and final census complete.

## Interpretation

A pass establishes only that this candidate survived this declared workload,
host, interpreter, worker/payload budgets, and observation window. It does not
establish universal production endurance, weeks-long leak freedom, OS hard limits,
callback isolation, arbitrary nested dependencies, task preemption, durable work,
automatic replay/recovery, or unchanged behavior on other Python/OS versions.
TaskService/MCP retained-record longevity is outside this executor-only campaign.
The serial completion-pump callback hazard and explicit-shutdown requirement
remain documented compatibility differences.
