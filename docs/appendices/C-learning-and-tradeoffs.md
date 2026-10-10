# Appendix C: Learning, evidence, and tradeoffs

## 1. The current learner in one sentence

`AdaptivePolicy` is an experimental, inspectable rule that combines explicit task kinds, comparable uncontended completion observations, queue/dispatch-slot pressure, bounded per-workload exploration and a switching margin. It routes each newly accepted task once. There is no model service, source-code analysis, task duplication, calibrated statistical confidence or global-optimality guarantee in that mechanism.

The framework's goal is better configuration evaluation and selection under declared resource constraints. Online routing is one mechanism toward that goal, not evidence that the same fixed configuration always runs faster than stdlib.

“Adaptive” is the constructor default, not a maturity certification. Use `policy="hints"` to choose routing from explicit declarations or `policy="threads"` for a thread-only reference. Set `process_workers=0` when process execution is unwanted; the learner then has no alternative backend to select.

## 2. Exact eligibility and default route

Before learning can matter, all these conditions must hold:

- A process pool is configured.
- The caller supplies `process_safe=True`.
- The task is not marked `kind="io"`.

If any condition fails, the built-in adaptive policy returns `"thread"`. A live-load snapshot must also show an enabled process backend. For eligible work with insufficient observations on either route, the default is process for `"cpu"` and thread for `"unknown"`. The load-aware path can divert from that default when its queued count is at least its worker count and its capacity-normalized pressure exceeds the alternative by at least `1 + switch_margin`. Mere occupied slots do not establish that the alternative is faster. Exploration can also change a particular decision. Process eligibility is enforced again by the executor, so a custom policy cannot override a missing process declaration.

The core does not prove that a process-safe function is pure or idempotent. Exploration routes a future submission once. It never replays the same task to collect an alternate timing. This is essential for functions with external side effects.

## 3. What is learned

For successful observation `x` and smoothing factor `alpha`, the update is:

```text
first observation: m = x
later observations: m = alpha * x + (1 - alpha) * previous_m
```

The default `alpha` is `0.25`. New data then receives one quarter of the immediate weight; older data decays geometrically. This gives an inexpensive recent-history estimate, not a confidence interval, variance estimate, drift detector, or unbiased service-time estimator.

Full snapshot samples measure accepted-submission-to-completion latency. Admission and policy time are absent; owned queueing, dispatch/startup, serialization, return transfer and completion handling can be present. The `uncontended_*` fields additionally record the subset accepted when the selected backend had no queued or dispatched work. Uncontended observations still include startup/serialization; they are not intrinsic callable service time and do not prove controlled hardware conditions.

The executor calls `choose_with_load`, which uses uncontended counts/times. Once both backends meet `minimum_samples`, it ranks:

```text
pressure = (queued + in_flight) / workers
score = uncontended_completion_ewma * (1 + pressure)
```

A lower score is preferred, with ties initially favoring threads. If an incumbent route exists, switching requires more than the configured fractional margin of improvement (default `switch_margin=0.1`). The pressure multiplication is a heuristic, not a calibrated completion forecast. Load is scheduling occupancy, not CPU utilisation or direct child-process activity.

The three-argument `choose` remains compatible and uses full completion EWMAs without load. Profiles lacking uncontended provenance can still serve that legacy interface and reporting, but do not train the load-aware baseline. Keys and task kinds must describe comparable work; the learner cannot detect misleading grouping or separate every confounder.

Failed calls add failure counts but do not update the latency mean. Otherwise a backend that fails instantly could appear faster than one that successfully computes a result. This exclusion does not yet make the policy failure-rate aware: it does not actively penalize a route's probability of failure.

## 4. Deterministic exploration

The defaults are `exploration_interval=8`, `minimum_samples=2`,
`switch_margin=0.1`, and `max_keys=256`. Each eligible `(key, kind)` pair has its
own counter and incumbent preference. State is bounded with least-recently-used
key eviction; eviction loses that exploration/switching history. Interleaved keys
do not share one global exploration turn. Full measurement profiles remain grouped
by task key/backend, so reusing one key for incomparable kinds still mixes evidence.

On every eighth eligible decision for that pair, an exploration candidate is:

1. Thread if its relevant sample count is below two.
2. Otherwise process if its relevant sample count is below two.
3. Otherwise the opposite of the preferred route after switching-margin handling.

For the load-aware hook, exploration never adds work to an already queued target.
While either route still lacks enough uncontended samples, the candidate must
have no outstanding work so it can yield an uncontended observation. Otherwise
only an empty target queue is required. If the gate fails, use the preferred
route. The three-argument interface has no load gate and uses full sample counts.

This is a bounded deterministic frequency rule, not a hard cumulative exploration
cost budget or statistically justified sampling policy. Busy workloads can fail
to produce uncontended comparisons; two observations are only an implementation
threshold. Pending work can leave decisions with stale or empty evidence.

Built-in policy objects supplied to executors are configuration templates:
`for_executor` creates independent exploration/switching sessions, capped by the
executor's profile-key bound. Custom subclasses preserve their supplied state
unless they override that hook. Record settings and session lifetime in experiments;
do not change them mid-comparison without labeling the change.

## 5. Keys are part of the model

A default key of `"default"` aggregates unrelated work. That is convenient for ordinary calls but poor evidence for learning. Prefer a stable identifier such as `"resize-jpeg:small:v2"` when the application already knows these tasks are comparable. Do not introduce a unique key for every input, which eliminates repeated observations and forces eviction.

A key should usually reflect:

- the operation and its meaningful implementation version;
- a coarse application-known size class if cost varies greatly with size;
- a materially different execution mode, such as compression level;
- a task family whose resource and correctness requirements are similar.

The current implementation does not automatically create these buckets or check their comparability. The key cap protects internal profile cardinality, not statistical quality. Recent completion updates keep a key resident; a slow rare operation may lose its history between runs. Nonpredictive profile limits can exceed 256, but a larger cap should be justified by reuse and memory measurements. Predictive profiles remain capped at 256; their exact caller-supplied feature buckets are described in [PREDICTIVE.md](../PREDICTIVE.md).

## 6. Where adaptation can lose

### Short jobs

For a script that submits one batch and exits, there may be no later comparable decision to improve. Exploration and process startup can be pure cost. Compare against a direct loop and a fixed thread pool before enabling learning.

### Tiny tasks

Locking, profile snapshots, callbacks, future allocation, and transport can dominate useful work. A faster choice between two expensive routes may still lose to sequential execution. `map_batches` amortizes dispatch through caller-owned tuple batches, with its own latency/cancellation tradeoffs; `map_adaptive` only changes the submission window. Neither guarantees a throughput improvement.

### Large arguments and results

A CPU-intensive operation can still be better in threads when its transfer cost is high or when native code already releases the GIL. A boolean CPU hint cannot summarize these economics. Sweep payload size rather than benchmarking only a convenient small fixture.

### Nonstationary traffic

An EWMA responds to a regime change only as relevant successful observations arrive. Periodic exploration may be too slow or too costly. Failure streaks add no new successful latency evidence. Explicit drift and uncertainty mechanisms remain planned.

### Selection feedback

Sending more work to a preferred route changes its future pressure. The load-aware policy uses uncontended observations to avoid simply multiplying an already queue-delayed aggregate EWMA by another congestion factor, and a switching margin dampens small changes. These controls do not eliminate oscillation, contention, cold-start effects, or stale evidence. Different workloads sharing a backend can still distort comparisons.

The opt-in predictor has separate service-time buckets and demand-aware ranking. Its global pending limit can stop admission while another backend has free dispatch slots. Neither load-aware path promises optimality, calibrated forecasting, or work-conserving scheduling.

### Shared policy objects

Exact built-in `AdaptivePolicy` templates create independent executor sessions and synchronize direct use internally. Custom policies can contain mutable state, and different executor locks do not protect one shared custom object. Create independent instances with `for_executor(*, max_keys=...)`, or explicitly synchronize and document shared state. Custom subclass state is not silently discarded.

## 7. A better claim than “AI makes it faster”

A defensible finding names the workload, machine, Python runtime, task sizes, cold or warm state, worker counts, admission bound, policy settings, and repetitions. It then compares useful throughput and latency with equivalent fixed baselines, including correctness and failures.

For example, a report might say: “On this measured workload and environment, the adaptive policy selected processes for most eligible tasks after warmup; the attached raw trials show the latency distribution.” It must not infer universal improvement from that observation. Judge configuration selection against suitable independently measured eligible baselines, report tuning overhead and regret or missed savings where available, and keep holdout evaluation independent. Same-configuration engine overhead is a different question; neither result establishes universal superiority.

## 8. Next evidence improvements

Prioritize better measurements and decisions before more sophisticated learning:

1. Record reproducible benchmark and policy provenance.
2. Define meaningful workload keys and size strata.
3. Evaluate uncertainty and workload drift with controlled traces.
4. Test a hard exploration budget and explicit safety constraints.
5. Evaluate candidates offline and in shadow mode without claiming missing counterfactual outcomes.
6. Only then consider contextual models, with held-out workload families and a conservative fallback.

The [concept catalog](A-concept-catalog.md) gives these ideas separate statuses. None is implied by the current use of an EWMA.
