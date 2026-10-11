# Four-hour endurance restart preregistration

Declared before the replacement attempt. This addendum changes the execution
host and evidence retention route only. It does not change the frozen workload,
worker counts, durations, correctness/resource gates, watchdog limits, or runtime.
The original `OPERATIONAL-ENDURANCE-PROTOCOL.md` remains byte-for-byte unchanged.

## Interrupted attempt

The local attempt began on 2026-10-11 at approximately 01:49 UTC. An execution
transport recovery at approximately 03:43 UTC reported an executor-key change.
The original command session is no longer reachable. The recovered filesystem
contains 2,502.2461496 measured workload seconds and two complete diagnostic
checkpoints; it has neither a final workload summary nor a final supervisor
result. An earlier one-hour checkpoint was read and reported before recovery,
but the corresponding raw bytes are absent from the restored files. Those lost
measurements are not reconstructed or counted toward this replacement attempt.

A process census in the new execution namespace does not establish cleanup in
the inaccessible original namespace. Its terminal state is unknown. The prior
attempt is classified as interrupted/incomplete with unknown underlying cause,
never passed.
All available raw files and their hashes are retained separately. There is no
claim that the library caused the environment interruption.

## Separate replacement host

Use one standard public-repository `ubuntu-24.04` x64 GitHub-hosted runner, with
no larger-runner or paid-runner configuration. GitHub documents standard public
Ubuntu runners as 4 CPUs, 16 GB RAM and 14 GB SSD, with free standard-runner use;
the hosted-job execution limit is six hours. This comfortably accommodates the
unchanged 15,000-second supervisor wall budget (250 minutes) a 270-minute
endurance-step timeout, and a 300-minute job timeout, leaving a finite allowance for setup, verification and artifacts.

References checked before launch:
- https://docs.github.com/en/actions/reference/runners/github-hosted-runners
- https://docs.github.com/en/actions/reference/limits
- https://docs.github.com/en/actions/concepts/billing-and-usage

The interrupted local host recorded nine CPUs in its affinity, a different
Linux/glibc image, and a Clang-built Python 3.12.14. The replacement runner is a
new host with a new process lifetime and a fresh warm baseline. Record actual
runner OS/image, CPU count and affinity, memory, Python build/version, psutil
version, workflow commit and run identity. Do not pool its resource trend,
throughput, elapsed time or task counts with the interrupted host. A pass would
be a new single-host result, not a same-host continuation or cross-host proof.

## Frozen code and dependency preflight

The verified candidate base is commit
`978da9206ffbd46a312b52ed3f572b5ad430a86e`. A workflow/addendum-only descendant is
permitted if all measured runtime/harness/protocol bytes match these gates:
- Recursive 15-file runtime manifest digest:
  `7774e028757fd04922b22db1cb61c5483a147fcfd11f25015536094df8f4ba4e`
- `tools/operational_endurance.py`:
  `4202e4a01f4f5a53f7f854ad4fb7b1e3d7601d9894b8bc840091fcdc1d3497f7`
- `tools/supervise_endurance.py`:
  `5fd9bf150fda82051611c49eb2f71cc463b5ec045fb2bd421470f6263b0da2dc`
- Original protocol:
  `384435dc15821af4c3ba88cc761ab9cd4cbd941ea681262e1b77c4a43b2dfbed`

Use normal, assertion-enabled CPython 3.12.14, and psutil 7.2.2 from the official
package registry. Fail setup if the pinned interpreter or dependency cannot be
resolved; do not silently substitute versions. The package core itself has no
third-party dependency. Linux supports the harness's POSIX process groups,
process file-descriptor census, spawn multiprocessing and SIGTERM/SIGKILL
cleanup. Record the actual imported module paths/hashes and optimization level.

## Unchanged measurement and gates

Begin at zero. Warm for 60 seconds, then run 14,400 measured workload-window
seconds on one long-lived default-adaptive executor. Keep the sixteen repeated
900-second cycles (each with three workload phases), two thread workers, two process workers, max_pending 32,
trace capacity 64 and profile capacity 16. Keep all original fault, cancellation,
retention, process-crash/new-executor recovery and short predictive checkpoints.
Diagnostics add to wall time; they do not contribute to the four-hour workload
window. Reuse the same 1 GiB sampled emergency tree-RSS limit, 90-second productive
progress limit, 15,000-second wall limit, quiescent growth gates and final census.
No threshold is relaxed because the replacement host differs.

Use a new attempt directory and run identity. Never append to, overwrite, or
resume the interrupted attempt's counters. Any setup failure, interruption,
invariant failure, timeout, missing terminal report, or failed final census
makes the replacement attempt failed or incomplete, not passed. The public
read-only workflow must retain terminal and partial artifacts on failure when
runner service permits it. Print bounded checkpoint information to service logs
so a missing artifact does not masquerade as an unobserved successful run.

## Security and claims

Run only on the validation branch, with `contents: read`, pinned official actions,
no `id-token` grant, no secrets, no deployment/publishing environment, no release,
no tag and no main-branch update. Use the job's ordinary artifact mechanism and
short retention. Free standard-runner compute does not establish zero artifact
storage charges: retained artifacts remain subject to the account's storage
allowances and quotas. Retain only bounded campaign evidence, use no cache or
paid storage configuration, and make no billing changes. A storage/quota failure
is an evidence-retention failure to disclose, not successful durable delivery. No application task communicates externally; setup and evidence
upload are outside the measured task mix.

The same narrow interpretation applies: one declared synthetic adaptive-executor
workload on one recorded host and interpreter. The predictive checks are short
lifecycle probes only. No production certification, performance advantage,
callback isolation, TaskService/MCP longevity, arbitrary dependency progress,
thread preemption, or automatic task replay/recovery claim follows from a pass.
