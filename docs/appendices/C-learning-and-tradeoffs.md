# Appendix C: Learning, evidence, and tradeoffs

## 1. The current learner in one sentence

`AdaptivePolicy` is an experimental, inspectable rule that compares recent successful backend latency for comparable task keys and occasionally sends a new eligible task to an alternate backend. There is no model service, training job, source-code analysis, task duplication, or general intelligence in that mechanism.

“Adaptive” is the constructor default, not a maturity certification. Use `policy="hints"` to choose routing from explicit declarations or `policy="threads"` for a thread-only reference. Set `process_workers=0` when process execution is unwanted; the learner then has no alternative backend to select.

## 2. Exact eligibility and default route

Before learning can matter, all these conditions must hold:

- A process pool is configured.
- The caller supplies `process_safe=True`.
- The task is not marked `kind="io"`.

If any condition fails, the built-in adaptive policy returns `"thread"`. For eligible work with insufficient evidence on either backend, the default is process for `"cpu"` and thread for `"unknown"`, except on exploration decisions. Process eligibility is enforced again by the executor, so a custom policy cannot override a missing process declaration.

The core does not prove that a process-safe function is pure or idempotent. Exploration routes a future submission once. It never replays the same task to collect an alternate timing. This is essential for functions with external side effects.

## 3. What is learned

For successful observation `x` and smoothing factor `alpha`, the update is:

```text
first observation: m = x
later observations: m = alpha * x + (1 - alpha) * previous_m
```

The default `alpha` is `0.25`. New data then receives one quarter of the immediate weight; older data decays geometrically. This gives an inexpensive recent-history estimate, not a confidence interval, variance estimate, drift detector, or unbiased service-time estimator.

The sample `x` is backend submit-to-completion latency. Admission and policy time are absent. Queueing, serialization, return transfer, and callback delay can be present. A process backend may look slow because its workers are saturated; a thread backend may look fast because it received smaller inputs. The learner cannot disentangle those causes.

Failed calls add failure counts but do not update the latency mean. Otherwise a backend that fails instantly could appear faster than one that successfully computes a result. This exclusion does not yet make the policy failure-rate aware: it does not actively penalize a route's probability of failure.

## 4. Deterministic exploration

The current defaults are `exploration_interval=8` and `minimum_samples=2`. The policy increments a decision counter for each eligible routing decision across all keys using that policy instance.

On every eighth eligible decision it explores:

1. If the thread route has fewer than two successful samples for the key, choose thread.
2. Otherwise, if the process route has fewer than two successful samples, choose process.
3. Otherwise, choose the opposite of the lower-EWMA backend.

On other decisions, use the hint-based default until both routes have at least two successful samples. Once both do, choose the smaller EWMA; an equality prefers threads.

This is a frequency rule, not a per-key exploration budget. Interleaved keys affect which key receives an exploration turn. Many submissions made before prior calls complete can all see stale or empty profiles. Two successful samples are only the implementation threshold, not statistically persuasive evidence. These are known prototype limitations, not hidden tunables that establish optimality.

An application may construct a policy object with different interval and sample settings and pass it to the executor. Record those settings in any experiment. Do not change them while comparing results without labeling the change.

## 5. Keys are part of the model

A default key of `"default"` aggregates unrelated work. That is convenient for ordinary calls but poor evidence for learning. Prefer a stable identifier such as `"resize-jpeg:small:v2"` when the application already knows these tasks are comparable. Do not introduce a unique key for every input, which eliminates repeated observations and forces eviction.

A key should usually reflect:

- the operation and its meaningful implementation version;
- a coarse application-known size class if cost varies greatly with size;
- a materially different execution mode, such as compression level;
- a task family whose resource and correctness requirements are similar.

The current implementation does not automatically create these buckets or check their comparability. The key cap protects internal profile cardinality, not statistical quality. Recent completion updates keep a key resident; a slow rare operation may lose its history between runs. More than 256 keys can be configured, but a larger cap should be justified by reuse and memory measurements.

## 6. Where adaptation can lose

### Short jobs

For a script that submits one batch and exits, there may be no later comparable decision to improve. Exploration and process startup can be pure cost. Compare against a direct loop and a fixed thread pool before enabling learning.

### Tiny tasks

Locking, profile snapshots, callbacks, future allocation, and transport can dominate useful work. A faster choice between two expensive routes may still lose to sequential execution. Batching is a planned mechanism with its own latency tradeoff.

### Large arguments and results

A CPU-intensive operation can still be better in threads when its transfer cost is high or when native code already releases the GIL. A boolean CPU hint cannot summarize these economics. Sweep payload size rather than benchmarking only a convenient small fixture.

### Nonstationary traffic

An EWMA responds to a regime change only as relevant successful observations arrive. Periodic exploration may be too slow or too costly. Failure streaks add no new successful latency evidence. Explicit drift and uncertainty mechanisms remain planned.

### Selection feedback

Sending more work to the current winner can increase its queue and make it lose. Alternating aggregate observations can then create oscillation. Different workloads sharing one pool make this harder. A future queue-aware predictor would need to avoid counting congestion both in the EWMA and as an extra penalty.

### Shared policy objects

Policy instances can contain mutable state. The executor calls its own policy under its own lock, but that does not make one object shared across several executors thread-safe. Create independent instances unless the policy deliberately synchronizes and documents cross-executor behavior.

## 7. A better claim than “AI makes it faster”

A defensible finding names the workload, machine, Python runtime, task sizes, cold or warm state, worker counts, admission bound, policy settings, and repetitions. It then compares useful throughput and latency with equivalent fixed baselines, including correctness and failures.

For example, a report might say: “On this measured workload and environment, the adaptive policy selected processes for most eligible tasks after warmup; the attached raw trials show the latency distribution.” It must not infer universal improvement from that observation. This repository provides a mechanism to investigate routing, not a theorem that routing is always helpful.

## 8. Next evidence improvements

Prioritize better measurements and decisions before more sophisticated learning:

1. Record reproducible benchmark and policy provenance.
2. Define meaningful workload keys and size strata.
3. Evaluate uncertainty and workload drift with controlled traces.
4. Test a hard exploration budget and explicit safety constraints.
5. Evaluate candidates offline and in shadow mode without claiming missing counterfactual outcomes.
6. Only then consider contextual models, with held-out workload families and a conservative fallback.

The [concept catalog](A-concept-catalog.md) gives these ideas separate statuses. None is implied by the current use of an EWMA.
