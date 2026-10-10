# Appendix A: Concept catalog

The machine-readable source of truth is [`../concepts.json`](../concepts.json). It contains 68 concepts: 13 implemented, 8 experimental, and 47 planned. These labels describe delivered mechanisms, not deployment readiness or performance guarantees. Adaptive routing remains a bounded heuristic; explicit hints and threads are available. Each entry records mechanism, risks, applicability, benchmark method and dependencies.

Status is a delivery contract, not a quality rating. A planned item is not accepted by the executor API. References explain background; they do not imply endorsement or an implementation in this repository.

## Overlapping scopes are not equivalent features

Some planned concepts overlap with narrower delivered mechanisms:

- CF-010 is a hard exploration-cost/decision budget; existing per-key interval
  counters and bounded state do not implement that budget.
- CF-015 is a resource/latency-feedback concurrency controller; experimental
  demand/profile-driven predictive admission is recorded separately as CF-065.
- CF-030 is a destructive process-only shutdown escalation path, not the
  implemented `cancel_futures=True` cancellation escalation and waiting join.
- CF-043 covers comprehensive policy/code/workload/environment provenance for
  reusable observations. Saved configuration schema-v2 revision checks are
  delivered under CF-066, without claiming that broader profile provenance.
- CF-044 covers managed persistence of aggregate adaptive-policy profiles,
  including file lifecycle and compatibility concerns. Explicit strict JSON
  predictive-profile transfer is delivered experimentally under CF-064, while
  configuration-only reuse is part of CF-066. Neither persists unfinished tasks.

These distinctions do not change the catalog statuses below. Future API
interoperability (CF-006) does not promise stdlib callback/thread/implicit-exit
scheduling equivalence; physical item bounds (CF-061) are not byte/RSS caps.

## Catalog index

| ID | Concept | Status | Category |
| --- | --- | --- | --- |
| CF-001 | Bounded admission | implemented | admission |
| CF-002 | Explicit thread and process hints | implemented | routing |
| CF-003 | Per-key backend EWMA selection | experimental | learning |
| CF-004 | Execution profile records | implemented | observability |
| CF-005 | Cancellation propagation | implemented | lifecycle |
| CF-006 | Standard Future interoperability | implemented | api |
| CF-007 | Explicit policy registry | implemented | extensibility |
| CF-008 | Input-size-aware profiles | experimental | learning |
| CF-009 | Cold-start-aware routing | planned | learning |
| CF-010 | Bounded exploration budget | planned | learning |
| CF-011 | Concept-drift detection | planned | learning |
| CF-012 | Uncertainty-aware selection | planned | learning |
| CF-013 | Queue-aware routing | experimental | routing |
| CF-014 | Failure-aware policy feedback | planned | learning |
| CF-015 | Adaptive concurrency control | planned | admission |
| CF-016 | Byte-weighted admission | planned | admission |
| CF-017 | Tenant-weighted fairness | planned | admission |
| CF-018 | Deadline-aware admission | planned | admission |
| CF-019 | Priority with aging | planned | scheduling |
| CF-020 | Per-resource rate limits | planned | admission |
| CF-021 | Burst reservation pools | planned | admission |
| CF-022 | Reentrant submission guard | planned | lifecycle |
| CF-023 | Bounded streaming map | implemented | api |
| CF-024 | Small-task batching | experimental | scheduling |
| CF-025 | Adaptive chunk sizing | experimental | scheduling |
| CF-026 | Explicit shared-memory payloads | planned | data-movement |
| CF-027 | Data-local worker affinity | planned | data-movement |
| CF-028 | Resource-class budgets | planned | admission |
| CF-029 | Cooperative cancellation tokens | planned | lifecycle |
| CF-030 | Explicit shutdown escalation | planned | lifecycle |
| CF-031 | Retry budgets and backoff | planned | resilience |
| CF-032 | Dependency circuit breakers | planned | resilience |
| CF-033 | In-flight duplicate coalescing | planned | data-movement |
| CF-034 | Explicit result caching | planned | data-movement |
| CF-035 | Idempotency metadata | planned | resilience |
| CF-036 | Dependency DAG execution | planned | composition |
| CF-037 | Checkpointed workflow outputs | planned | composition |
| CF-038 | Remote executor adapter | planned | backends |
| CF-039 | Durable recovery adapter | planned | resilience |
| CF-040 | Structured execution event journal | planned | observability |
| CF-041 | Trace-context integration | planned | observability |
| CF-042 | Deterministic decision replay | planned | verification |
| CF-043 | Policy and workload version provenance | planned | verification |
| CF-044 | Versioned profile persistence | planned | learning |
| CF-045 | Profile cardinality limits | implemented | observability |
| CF-046 | Shadow policy evaluation | planned | verification |
| CF-047 | Policy decision guardrails | planned | verification |
| CF-048 | Workload equivalence checks | planned | verification |
| CF-049 | Explicit hostile-code boundary | planned | security |
| CF-050 | Interpreter-pool backend | planned | backends |
| CF-051 | Free-threaded runtime awareness | planned | backends |
| CF-052 | Accelerator batch backend | planned | backends |
| CF-053 | Native-thread oversubscription budgets | planned | admission |
| CF-054 | Cost-aware scheduling objective | planned | learning |
| CF-055 | Energy-aware scheduling experiments | planned | learning |
| CF-056 | Offline contextual policy training | planned | learning |
| CF-057 | Static advisory analysis | planned | developer-experience |
| CF-058 | Async application bridge | planned | api |
| CF-059 | Worker initializer and warmup hooks | planned | backends |
| CF-060 | Fail-fast task groups | planned | composition |
| CF-061 | Removable bounded dispatch queue | implemented | admission |
| CF-062 | Registered JSON task framework | implemented | extensibility |
| CF-063 | Optional MCP stdio adapter | implemented | integrations |
| CF-064 | Bounded transferable workload profiles | experimental | observability |
| CF-065 | Hardware-aware predictive admission | experimental | learning |
| CF-066 | Explicit offline configuration calibration | implemented | learning |

## How to choose the next concept

Start with an observed problem and a reproducible baseline. A catalog item is worth implementing when its mechanism plausibly addresses that problem, a falsifiable experiment can show whether it helped, and its safety and compatibility costs are acceptable. Do not implement the largest collection of knobs merely to increase the concept count.

Useful sequences include:

- Admission safety: CF-001 → CF-022 → CF-023 → CF-016. Diagnose blocking first, then control consumed-result and byte pressure.
- Learning quality: CF-004 → CF-043 → CF-008 → CF-012 → CF-011. Improve the evidence before introducing a more complex predictor.
- Fair mixed workloads: CF-001 → CF-017 → CF-019 → CF-021. Define the fairness objective before tuning priority.
- Reliable orchestration: CF-005 → CF-035 → CF-036 → CF-037. Idempotency and failure semantics precede durability.
- Optional backends: CF-002 → CF-048 → CF-050 or CF-052. Prove equivalent results before benchmarking new machinery.

## Promotion checklist

A planned concept becomes experimental only after runnable code, a focused test, and an explicit selection or disable path exist. New experimental features should be opt-in; the initial adaptive policy is the documented constructor default. Promotion to implemented also requires public API documentation, failure and shutdown coverage, a benchmark protocol, and explicit limits. If the experiment produces no improvement, keep the result and its provenance; a negative result is more useful than an unsupported success claim.

The dependency graph is advisory. It is not executable orchestration, a release promise, or a schedule. Review it when changing scope because an alternative implementation may remove a dependency.

## 0.3 predictive experiments

CF-008, CF-024 and CF-025 now have opt-in executable experiments. CF-064
records bounded transferable measurements; CF-065 uses hardware-aware constrained
admission. See [PREDICTIVE.md](../PREDICTIVE.md) for exact scope. Foreign-hardware
reuse is an unvalidated conservative prior, not learned transfer. Controlled
counterfactual/scaling experiments remain future work.


## Offline measured selection (0.4)

CF-066, **Explicit offline configuration calibration**, is implemented in
`intelligent_futures.tuning`. It evaluates only the caller's explicit repeatable
calibration callback, validates observations and process consent, records all
configuration costs, and freezes selection before held-out inspection. See
[TUNING.md](../TUNING.md). Its bounded measured search does not implement the
remaining planned concepts or establish a universal optimal scheduler.


## Evidence-gated adoption and explanations (0.5)

CF-067 adds an opt-in, fixed-plan decision about safe repeatable fixtures, with
independent stdlib tuning, conservative held-out comparison and explicit
`retain_stdlib`/`inconclusive` outcomes. CF-068 adds a bounded sanitized routing
ring and fixed-category counters. See [ADOPTION.md](../ADOPTION.md) for scope,
statistical assumptions, consent, rollback and lifecycle contracts. Neither
mechanism silently evaluates live application tasks.
