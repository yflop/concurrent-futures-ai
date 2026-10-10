# Appendix G: Extension roadmap and contribution gates

## 1. An expanding library needs stable meaning

“Ever-expanding” should mean a maintainable collection of ideas that can be searched, tested, promoted, revised, or rejected. It should not mean a background agent silently modifies the package or that every catalog entry is a runtime switch.

The catalog gives concepts stable IDs, explicit statuses, mechanisms, use cases, risks, benchmark methods, references, and dependencies. `ConceptRegistry` validates the JSON structure and dependency graph and returns read-only-by-copy query results. It never installs plugins or executes catalog prose.

```sh
python -m intelligent_futures "queue" --status planned
python -m intelligent_futures --status experimental --json
```

```python
from intelligent_futures import ConceptRegistry

catalog = ConceptRegistry.bundled()
entry = catalog.get("CF-061")
print(entry["status"])  # Consult the candidate's source and validation record too.
for concept in catalog.search("cancellation"):
    print(concept["id"], concept["title"])
```

Search is a simple text match over entry data; references and dependency IDs are also searchable. It is not semantic search, a dependency solver, or a ranking of the best optimization for a workload.

## 2. Status is a public promise

| Status | Required evidence | What it does not imply |
| --- | --- | --- |
| Planned | Clear problem, proposed mechanism, risks, and falsifiable experiment | Runnable code, API support, schedule, or commitment to ship |
| Experimental | Executable implementation, focused tests, explicit limitations, and a way to select or disable it | Production suitability or measured superiority |
| Implemented | Code, documented contract, tests covering normal and failure paths, and a benchmark method | Optimality, portability beyond tested environments, or universal speedup |

The load-aware EWMA learner is experimental even though `adaptive` is the constructor default. That fact must remain visible. New experiments should normally be opt-in and preserve conservative behavior by default.

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

An optional `choose_with_load(hints, profiles, process_available, load)` hook takes precedence over `choose`. Its public immutable `SchedulingLoad`/`BackendLoad` values expose worker budgets, owned queued items and occupied dispatch slots, not measured CPU utilisation. Keep existing three-argument policies working. Custom policies may implement `for_executor(*, max_keys=...)` for independent sessions; otherwise shared custom state must synchronize itself. Exact built-in adaptive templates already create independent sessions.

The core rejects an invalid route or a process route lacking `process_safe=True` and an available process pool. That enforcement is a correctness boundary, not sandboxing. A slow policy still holds the scheduling lock. An untrusted policy must not be loaded into the application.

## 5. Suggested milestones

### Milestone 1: Safer admission under cancellation and composition

The removable FIFO mechanism (CF-061) is implemented in version 0.4. It bounds logical unfinished work and physical owned-queued-plus-dispatched items by `max_pending`, bounds each backend's dispatches by its worker count, and removes owned queued payloads on cancellation. Dispatched thread cancellation retains physical capacity until acknowledgement; process Futures commit to running at dispatch. Mechanism completion is separate from operational maturity. Consult the [source-matched verification record](../VALIDATION.md) and [measured reliability boundaries](../CONFIGURATION-QUALITY.md); release-artifact checks and exact-commit CI retain their own gates.

Acceptance evidence must include retained cancelled public Futures with released queued payloads, physical counter bounds, cancellation-versus-dispatch races, delayed backend acknowledgement, blocked producers, separate-backend progress, deferred dispatch failure, coordinated shutdown, completion-pump retirement, and contained callback reentry.
Normal completion callbacks run serially on the owned pump and must not wait for
another Future from the same executor even when workers are free. The updated churn regression must have its own source-matched result; historical tombstone measurements are not current-code verification. Count bounds do not establish a memory plateau for caller-retained results/Futures, transport buffers or arbitrary callable allocations.

Reentrant-submission protection (CF-022) and byte-weighted admission (CF-016) remain separate work. The current targeted waiting-shutdown self-join rejection does not detect general dependency deadlocks. Explicit `with` or `shutdown(wait=True)` is required to guarantee accepted work drains before interpreter exit; implicit stdlib teardown equivalence is not promised.

### Milestone 2: Better learning evidence

The candidate now has live queue/dispatch-slot snapshots, uncontended completion provenance, bounded per-key/kind exploration and switching margins. The opt-in predictor separately uses exact caller-supplied feature buckets and demand-aware admission. None of these supplies calibrated uncertainty or optimality. Continue improving provenance (CF-043), controlled size-aware evidence (CF-008), and uncertainty (CF-012) before a more elaborate learned model. Evaluate hard exploration cost budgets (CF-010) and drift handling (CF-011) on changing workloads.

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
- Sparse/bursty demand, unequal backend capacities, and workload shifts; report
  queue/dispatch occupancy separately from actual CPU measurements.
- Independent evaluation of configuration-selection quality and calibration cost;
  distinguish these from same-configuration execution-engine overhead.
- Explicit evidence of idle capacity caused by global predictive admission rather
  than an assumed work-conserving scheduling guarantee.
- Evidence that faster failures or skipped work are not counted as optimization.
- At least one known weak case and a documented fallback or scope limit.

If a performance check requires a precise speed threshold on shared CI hardware, it is likely too fragile for a correctness gate. Keep correctness deterministic and performance analysis distribution-based, with tolerances justified by observed noise.

## 7. Backward compatibility and maintenance

Never silently repurpose a `TaskHints` field or consume a keyword from ordinary `submit` that belongs to the callable. Preserve standard Future interoperability. Version 0.4 returns genuine executor-owned Futures instead of backend identities, so document that lifecycle change even though common method signatures remain the same. Likewise, changing admission, process dispatch commitment, cancellation acknowledgement or interpreter-exit semantics requires explicit migration notes; signature compatibility is not full lifecycle equivalence.

Keep catalog labels synchronized with shipped code. Review built wheels because documentation files present in a checkout are not automatically included in the installed package. Retain IDs when titles improve so external references stay meaningful. A reference URL should explain a foundation or verified source; it should not be invented to make a proposal look established.

A small dependable executor plus a candid research catalog is more valuable than a large list of unverified features. Growth should make the system easier to reason about, not harder to trust.

## 8. Delivered opt-in layers in 0.2.0

CF-062 adds the reusable registered-task framework; CF-063 adapts it to local MCP
stdio using an optional SDK dependency. See [framework](../FRAMEWORK.md) and
[MCP](../MCP.md) for contracts and boundaries. These do not implement the planned
remote executor, durable recovery, hostile-code sandbox, or byte-weighted core
admission concepts. Existing concept IDs remain stable; statuses must track the
corresponding implementation rather than being inherited from an older release.

## 9. Delivered opt-in foundation in 0.3

Bounded workload profiles, explicit numeric feature summaries, measured successful
thread wall/CPU time, objective-constrained admission, and actual adaptive batch
execution now exist. A foreign-hardware cold start may use a conservative prior,
explicitly unvalidated; local samples refine it. This does not deliver statistical
transfer-learning, a causal/counterfactual model, process CPU/RSS profiling, or
automatic semantic proof that a callable can be parallelized.

Further promotion requires controlled same-workload scaling trials, held-out
hardware validation, uncertainty calibration, drift detection, and realistic
end-to-end evidence beating useful baselines without relaxing safety constraints.
