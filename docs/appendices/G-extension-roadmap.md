# Appendix G: Extension roadmap and contribution gates

## 1. An expanding library needs stable meaning

“Ever-expanding” should mean a maintainable collection of ideas that can be searched, tested, promoted, revised, or rejected. It should not mean a background agent silently modifies the package or that every catalog entry is a runtime switch.

The current catalog has 61 concepts with stable IDs, explicit statuses, mechanisms, use cases, risks, benchmark methods, references, and dependencies. `ConceptRegistry` validates the JSON structure and dependency graph and returns read-only-by-copy query results. It never installs plugins or executes catalog prose.

```sh
python -m intelligent_futures "queue" --status planned
python -m intelligent_futures --status experimental --json
```

```python
from intelligent_futures import ConceptRegistry

catalog = ConceptRegistry.bundled()
entry = catalog.get("CF-061")
assert entry["status"] == "planned"
for concept in catalog.search("cancellation"):
    print(concept["id"], concept["title"])
```

Search is a simple text match over entry data; references and dependency IDs are also searchable. It is not semantic search, a dependency solver, or a ranking of the best optimization for a workload.

## 2. Status is a public promise

| Status | Required evidence | What it does not imply |
| --- | --- | --- |
| Planned | Clear problem, proposed mechanism, risks, and falsifiable experiment | Runnable code, API support, schedule, or commitment to ship |
| Experimental | Executable prototype, focused tests, explicit limitations, and a way to select or disable it | Production suitability or measured superiority |
| Implemented | Code, documented contract, tests covering normal and failure paths, and a benchmark method | Optimality, portability beyond tested environments, or universal speedup |

The initial EWMA learner is experimental even though `adaptive` is the constructor default. That fact must remain visible. New experiments should normally be opt-in and preserve conservative behavior by default.

A negative experiment should remain discoverable. Record why it failed and under which conditions, rather than relabeling an unhelpful prototype as successful or deleting the evidence. A future schema can add a separate disposition field for rejected or superseded ideas without overloading implementation status.

## 3. Contribution sequence

1. Identify an actual bottleneck or correctness limitation, with a reproducible fixture.
2. Locate an existing catalog ID; add a new one only if the mechanism is materially distinct.
3. Describe how the proposed change differs from existing features and what remains unsupported.
4. Specify invariants, failure behavior, shutdown behavior, and backend eligibility effects.
5. Implement the narrowest useful version without unrelated refactoring.
6. Add tests that fail without the change and cover at least one adverse path.
7. Run an appropriately controlled benchmark; retain raw results and any regressions.
8. Update API documentation and the relevant appendix.
9. Update both the documentation catalog and the packaged catalog without changing existing IDs.
10. Validate the catalog, build the package, and test the installed artifact.

Changes to data sharing, third-party services, code execution, or trusted/untrusted boundaries need explicit security review. The local executor should not acquire a remote dependency merely because a catalog entry mentions one.

## 4. Policy extension example

A policy selects a route. It must not run user tasks, mutate profile input, start threads, perform network calls, sleep for backoff, or call back into its owning executor while choosing.

```python
from intelligent_futures import IntelligentExecutor, PolicyRegistry


class PreferThreads:
    def choose(self, hints, profiles, process_available):
        return "thread"


registry = PolicyRegistry()
registry.register("prefer-threads", PreferThreads)
policy = registry.create("prefer-threads")

with IntelligentExecutor(policy=policy, max_pending=8) as executor:
    assert executor.submit(pow, 3, 4).result() == 81
```

The registry is per instance. Registering `"prefer-threads"` does not modify a global registry or make that name available to every new executor. Pass the created policy object. Factories should create fresh policy state rather than sharing one mutable instance accidentally.

The core rejects an invalid route or a process route lacking `process_safe=True` and an available process pool. That enforcement is a correctness boundary, not sandboxing. A slow policy still holds the scheduling lock. An untrusted policy must not be loaded into the application.

## 5. Suggested milestones

### Milestone 1: Safer admission under cancellation and composition

Prioritize the removable dispatch queue (CF-061), reentrant-submission guard (CF-022), and byte-weighted admission (CF-016). The target is a clearly specified capacity model that remains useful under cancellation churn and nested workloads.

Acceptance evidence should include physical queue and memory plateau tests, cancellation-versus-dispatch races, shutdown with blocked producers, and proof that no callable whose cancellation succeeded later executes. Do not introduce a dispatcher thread without measuring its new overhead and cleanup obligations.

### Milestone 2: Better learning evidence

Add provenance (CF-043), size-aware profiles (CF-008), and uncertainty (CF-012) before a more elaborate learned model. Evaluate hard exploration budgets (CF-010) and drift handling (CF-011) on changing workloads.

The target is reduced decision error under a declared workload distribution, not an impressive model name. Require held-out cases, fixed-policy baselines, and accounting for exploration cost. Existing profile eviction means any persistence design must also explain what happens when histories are incomplete.

### Milestone 3: Application resource control

Resource-class budgets (CF-028), rate limiting (CF-020), and fairness (CF-017) can be useful without changing backend learning. Their target is good useful throughput subject to explicit constraints, such as a connection count or per-tenant service guarantee.

Acceptance tests should use fake local dependencies with observable quotas. Hard resource constraints must not be hidden inside a weighted optimization score that can violate them when another term looks attractive.

### Milestone 4: Composition outside the core

Dependency DAGs (CF-036), fail-fast groups (CF-060), and cooperative cancellation (CF-029) belong in separate opt-in layers. Their target is clearer orchestration without blocking worker threads on unresolved dependencies.

A DAG is not durable merely because nodes are futures. Checkpoints (CF-037), remote execution (CF-038), and durable recovery (CF-039) introduce storage, security, and side-effect semantics. Treat them as a new system boundary and avoid forcing those dependencies on local-executor users.

## 6. Benchmark gate for a new policy

A policy change needs more than one favorable mean:

- Correct outputs and preserved cancellation/admission invariants.
- The same explicit backend-eligibility rules.
- End-to-end latency including admission, plus throughput and resource measurements.
- Cold-start, steady-state, tiny-task, and adversarial mixed-workload cases.
- Comparable worker budgets and transparent parameter tuning.
- Evidence that faster failures or skipped work are not counted as optimization.
- At least one known weak case and a documented fallback or scope limit.

If a performance check requires a precise speed threshold on shared CI hardware, it is likely too fragile for a correctness gate. Keep correctness deterministic and performance analysis distribution-based, with tolerances justified by observed noise.

## 7. Backward compatibility and maintenance

Never silently repurpose a `TaskHints` field or consume a keyword from ordinary `submit` that belongs to the callable. Preserve standard Future interoperability. Changing admission from blocking to rejecting, or replacing actual futures with proxies, is a contract change even when function signatures stay the same.

Keep catalog labels synchronized with shipped code. Review built wheels because documentation files present in a checkout are not automatically included in the installed package. Retain IDs when titles improve so external references stay meaningful. A reference URL should explain a foundation or verified source; it should not be invented to make a proposal look established.

A small dependable executor plus a candid research catalog is more valuable than a large list of unverified features. Growth should make the system easier to reason about, not harder to trust.
