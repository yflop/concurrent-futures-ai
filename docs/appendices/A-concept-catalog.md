# Appendix A: Concept catalog

The machine-readable source of truth is [`../concepts.json`](../concepts.json). It contains 61 concepts: 8 implemented, 1 experimental, and 52 planned. The experimental concept is the current EWMA adaptive learner; it is the constructor default, and callers can explicitly choose threads or hints instead. Each entry gives a mechanism, use case, failure risks, benchmark method, references, and dependency IDs.

Status is a delivery contract, not a quality rating. A planned item is not accepted by the executor API. References explain background; they do not imply endorsement or an implementation in this repository.

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
| CF-008 | Input-size-aware profiles | planned | learning |
| CF-009 | Cold-start-aware routing | planned | learning |
| CF-010 | Bounded exploration budget | planned | learning |
| CF-011 | Concept-drift detection | planned | learning |
| CF-012 | Uncertainty-aware selection | planned | learning |
| CF-013 | Queue-aware routing | planned | routing |
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
| CF-024 | Small-task batching | planned | scheduling |
| CF-025 | Adaptive chunk sizing | planned | scheduling |
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
| CF-061 | Removable bounded dispatch queue | planned | admission |

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
