# Evidence-gated incremental adoption (0.5 Beta)

Start with one executor factory and one bounded, repeatable workload. Version
0.5 adds an **opt-in evaluation decision**, not an automatic deployment or a
universal speedup claim. Its outcomes are `recommend_intelligent`,
`retain_stdlib`, and `inconclusive`. Both of the latter leave adoption unsupported
by this experiment. Existing executor behavior does not run this evaluator.

## 1. Keep the application change small and reversible

```python
from concurrent.futures import wait
from intelligent_futures.evaluation import EvaluationConfiguration
from intelligent_futures.evaluation_workloads import make_executor

ENGINE = "stdlib"  # choose "intelligent" here before accepting fresh work
CONFIGURATION = EvaluationConfiguration("thread", workers=2)

def executor_factory():
    return make_executor(ENGINE, CONFIGURATION, max_pending=4)

def square(value):
    return value * value

with executor_factory() as executor:
    futures = [executor.submit(square, value) for value in range(4)]
    wait(futures)
    print([future.result() for future in futures])
```

For the thread configuration above, `submit`, standard-library `Future`, `wait`,
`as_completed`, and finite `map` calls keep their familiar interfaces. See the
[complete runnable example](../examples/incremental_adoption.py):

```sh
python -m examples.incremental_adoption --engine stdlib
python -m examples.incremental_adoption --engine intelligent
```

Install the checkout with `python -m pip install -e .` first, or prefix commands
with `PYTHONPATH=src` for source-tree use. To roll back, select `stdlib` at that
same factory boundary **before the next fresh batch**. Drain the old executor
with a context manager or explicit `shutdown(wait=True)`. Existing Futures stay
with their original executor. A failed or timed-out task may already have side
effects; switching factories does not authorize replay or automatic retry.

That one-variable switch works because the example keeps a deterministic,
non-predictive thread configuration. For an adaptive or predictive recommendation,
save the original stdlib configuration separately. Rollback must restore **both**
the engine and that configuration; stdlib has no adaptive/predictive counterpart
that `make_executor` can silently substitute. For example, after evaluation:

```python
fallback_configuration = report.fixed_baseline  # preserve the original stdlib mode
engine, configuration = "stdlib", fallback_configuration
adopt = False  # your explicit decision after inspecting the report
if adopt and report.recommendation is not None:
    configuration = report.recommendation.checked_configuration(workload)
    engine = "intelligent"

# Later, drain the old pool, then restore both before creating the next one:
engine, configuration = "stdlib", fallback_configuration
```

Process-capable recommendations additionally need the renewed consent described
below. Passing an adaptive or predictive configuration to
`make_executor("stdlib", ...)` deliberately raises rather than downgrading it.

Compatibility has important lifecycle limits:

- Normal IntelligentExecutor completion callbacks share one serial completion
  pump. Never wait for another same-executor Future inside a callback, including
  when worker slots appear free. Notify an outside coordinator instead.
- Explicit waiting shutdown is required for reliable draining. Do not depend
  on identical stdlib interpreter-exit or callback-thread behavior.
- Before Python 3.14, inherited `map` eagerly consumes its input. Use an explicit
  caller-side window for a stream; the example bounds both engine paths equally.
- `make_executor("stdlib", ...)` returns an ordinary stdlib executor. Its
  `max_pending` argument does not retrofit an admission bound. The caller must
  apply the same window and batching to both paths for a fair comparison.

## 2. Evaluate only a deliberately safe fixture

Run the short API demonstration:

```sh
python -m examples.evaluate_adoption --save adoption-demo.json
```

It reads and hashes freshly generated temporary files and checks every output.
The default is deliberately short: 3 training blocks and 8 held-out blocks,
rather than the plan default of 24 held-out blocks. `inconclusive` and
`retain_stdlib` are useful results. The file is created without overwriting prior
evidence. This toy fixture is not application performance evidence.

Before a real experiment, replace that fixture with a bounded representative one,
choose the sample count and candidate grid in advance, and declare an honest
reuse horizon. For example, after making those changes:

```sh
python -m examples.evaluate_adoption --heldout-blocks 24 --expected-reuses 1000 --save adoption-run-01.json
```

The example reuse count is not a recommendation or a way to force acceptance.
Use your expected number of future executions of the **same measured scope and
size**. More repeats cannot repair an unrepresentative fixture or dependent
timing noise. Do not repeatedly rerun until one report passes.

### Ready-made callable adapter

`callable_workload(fn, fixture_factory, name=..., version=..., repeatable=True,
independent=True, max_tasks=..., max_pending=...)` creates a `RepeatableWorkload`.
The factory is a context manager accepting a seed and yielding
`EvaluationFixture(inputs, validate)`. Each measurement gets a fresh fixture;
the same block seed must produce equivalent inputs for every compared mode.
The callable takes one input. The validator receives an ordered result tuple
and must return an actual `bool`. Construction alone executes nothing.

The adapter uses the identical tuple-batch kernel and ordered submission window
for both engines. Its `metric_scope="pool_lifecycle"` times pool construction,
execution, and waiting shutdown. Fixture setup, input collection, validation,
and cleanup are excluded. There is no warmup or hidden replay. Only
`metric="cold_seconds"` is supported; the compatibility `warm_seconds` field
contains the same timing, not a separately observed warm run.

“Cold” here refers to pool lifecycle. In the demonstration, setup writes files
before timing reads, so filesystem caches are not cold-controlled. A real
fixture must document cache state, external services, dataset size, dependencies,
and which costs the comparison includes.

The adapter collects a finite fixture up front, checking at most `max_tasks + 1`
inputs to detect overflow. Empty or oversized fixtures fail. Inputs and results
remain local and are not included in the report. It does not supply CPU,
process-tree RSS, or batch-p95 measurements. Setting their caps therefore fails
closed, rather than treating missing values as zero.

### Caller-measured workloads

For a different timing boundary, warm runs, or measured resource metrics, use
`RepeatableWorkload(name, version, measure, repeatable=True, independent=True,
metric_scope=...)`. Your callback `measure(engine, configuration, seed)` returns
a validated `Measurement` from `intelligent_futures.tuning`. `engine` is exactly
`"stdlib"` or `"intelligent"`.

You own fixture reset, validation, timing, resource sampling, identical batching
and queue windows, cleanup, and scope equivalence. Report genuine measurements;
do not return projected timings or fabricated resource values. `repeatable=True`
is consent to execute only this fixture. `independent=True` declares that blocks
are independent and representative for the stated target; a different random
seed does not prove that assumption. Without the independence declaration,
exploratory measurements can run but cannot recommend adoption.

## 3. Predeclare the comparison and its gates

`evaluate_workload` takes separate `stdlib_configurations` and
`intelligent_configurations`, a `fixed_baseline` contained in the stdlib grid,
and an `EvaluationPlan`. There is no claim of a globally best configuration.

- `EvaluationConfiguration("thread", workers=N, batch_size=B)` and `"process"`
  are deterministic routes. `"adaptive"` splits a total active slot budget:
  `workers=4, process_workers=2` means two thread and two process workers.
- Stdlib candidates use deterministic, non-predictive configurations. Every
  intelligent mode must have same-budget stdlib equivalents in the search.
  An adaptive mode requires both a thread and a process equivalent at its total
  worker/batch budget. These are active worker budgets, not OS thread counts.
- `predictive=True` is explicit. Each measurement starts with empty predictive
  profiles. Fixed-route prediction may affect admission; it does not turn a
  fixed thread/process mode into adaptive routing.
- Processes require `allow_process=True` on evaluation. The callable adapter
  additionally requires `process_safe=True`: importable/picklable callables and
  data, safe semantics, and a guarded `if __name__ == "__main__":` entry point.
  Both process engines use `spawn`. Picklability alone is insufficient.

The default plan is 3 training blocks, then 24 untouched held-out blocks.
Training evaluates both grids and independently selects each engine's eligible
minimum-median candidate. A training candidate with any failed gate is excluded.
The selected intelligent mode is frozen before held-out measurements begin.
Every held-out block compares it with **both** the fixed stdlib baseline and the
independently tuned stdlib selection. Matching baseline aliases share a measured
run. Within each block, mode order is seeded and randomized. There is no
held-out reselection, early-success stop, dropped failure, or automatic retry.

For baseline elapsed time `B` and candidate elapsed time `C`, the inferential
target is the population **median of paired log ratios `log(B/C)`** under the
declared block assumptions. The code uses an exact binomial sign calculation
and a one-sided order-statistic lower bound. It requires that bound to exceed
`log(1.05)` by default, against both baselines. Ties at the margin do not count as
wins. See the [NIST sign-test background](https://www.itl.nist.gov/div898/software/dataplot/refman1/auxillar/signtest.htm).
Unlike NIST's illustrative tie-excluding procedure, this implementation keeps
the fixed sample denominator and handles margin ties conservatively.

Default `family_alpha=0.05` is divided between the two comparisons, including
when their baseline configurations coincide. This uses the
[Bonferroni finite-family principle](https://www.itl.nist.gov/div898/handbook/prc/section4/prc463.htm).
It controls this report's declared comparison family under its assumptions; it
does **not** control repeated experiments, cherry-picked workloads, changed
thresholds, or repeat-until-win campaigns. Plan those separately and retain
every experiment.

The inference is not about mean throughput, individual task latency, or tail
quantiles. Additional default observational gates require:

- All held-out slots present, correct, and free of task/resource errors
- At least 0.001 seconds for every compared elapsed measurement
- Median absolute deviation of paired log ratios at most 0.10
- Positive aggregate elapsed saving, and no observed pair in which candidate
  elapsed time is more than 25% slower than baseline
- Every configured CPU, sampled RSS, batch-p95, or elapsed cap satisfied;
  a missing capped metric fails closed

These are observed acceptance gates, not guarantees of future tail behavior or
hard memory/CPU limits. `max_workers` bounds declared configurations;
`max_evaluations` bounds measurement calls, not their duration or preemption.
`max_pending` counts scheduling items, not bytes. With the adapter, an item is a
batch, so it can contain multiple user inputs. The adapter still retains the
finite fixture's inputs and results. Sampled process-tree RSS is distinct from
all of these counts and can miss brief peaks. Running work cannot be killed by
these gates.

## 4. Include the cost of finding the answer

The report separates training, held-out validation, and overall discovery wall
time. Adoption also requires empirical amortization within `expected_reuses`.
The saving used is the smallest observed paired elapsed saving across both
baseline comparisons, not the mean saving or the largest win.

`empirical_break_even_reuses` is the ceiling of discovery time divided by that
positive observed minimum saving. `empirical_net_seconds` applies the declared
reuse horizon to the same saving, less discovery cost. Unknown horizons and
insufficient savings cannot recommend adoption. These are conservative
**within-sample accounting figures**, not confidence bounds for future savings;
deployment changes or distribution shifts can invalidate them.

## 5. Inspect, guard, then decide

Inspect `report.status`, `reasons`, `comparisons`, and all `records`. A report
retains failed/excluded observations, raw scalar measurements, fixed seeds and
order, candidate choices, count accounting, discovery costs, and compatibility
metadata. `report.to_json()` returns detached JSON-compatible data, not a JSON
string. Ordinary callback errors are sanitized. Interruptions and cancellation
abort rather than silently becoming successful observations.

Keep negative and inconclusive reports alongside passing ones. Synthetic
protocol tests establish decision/lifecycle behavior; they are not performance
evidence. The earlier [configuration-quality](CONFIGURATION-QUALITY.md) and
[production-evidence](PRODUCTION-EVIDENCE.md) reports are historical evidence for
their stated versions and scopes, not new evaluator certification. The separate
[0.5 release evaluation record](ADOPTION-EVIDENCE.md) describes new experiments;
read their full observations and limitations before drawing an adoption claim.

Only a `recommend_intelligent` report has a recommendation. In the same process,
after inspecting it, validate the exact configuration before using it:

```python
# Continuation after evaluate_workload(...); these objects come from that run.
if report.recommendation is not None:
    recommendation = report.recommendation
    configuration = recommendation.checked_configuration(workload)
    # After your adoption decision, use this exact configuration and its window
    # for new work with matching scope, batching, and lifecycle assumptions.
    executor = make_executor(
        "intelligent", configuration, max_pending=recommendation.max_pending,
    )
    try:
        # Submit only fresh, application-authorized work here.
        pass
    finally:
        executor.shutdown(wait=True)
```

`checked_configuration` rejects changed workload identity/version/scope,
repeatability/independence declarations, package/Python runtime, scheduler or
evaluation revision, hardware fingerprint, and expired or backwards-clock
evidence. The default TTL is one day. Hardware matching is a compatibility hint,
not proof of identical load or deployment. Bump `workload.version` when code,
data, dependencies, deployment, or cache assumptions change.

A process recommendation needs renewed `allow_process=True` at both the check
and executor construction. For IntelligentExecutor process submission, still use
`submit_task(..., hints=TaskHints(process_safe=True))`; ordinary `submit` does not
assert task process safety. `make_executor` does not implement batching for you.
It constructs the chosen pool, and the caller must preserve measured batch size,
window, and workload scope. There is no report deserializer or automatic
deployment path. Saved evidence does not authorize process execution.

### An explicit process-submission boundary

The one-factory thread example needs no task hint changes. For processes, review
both serialization and side effects first. This separate example makes executor
consent and per-task safety assertions visible for a known-safe callable:

```python
from intelligent_futures import TaskHints
from intelligent_futures.evaluation import EvaluationConfiguration
from intelligent_futures.evaluation_workloads import make_executor

def square(value):
    return value * value

def submit_reviewed_process_task(executor, engine, value):
    if engine == "intelligent":
        return executor.submit_task(
            square, value, hints=TaskHints(process_safe=True),
        )
    return executor.submit(square, value)

if __name__ == "__main__":
    engine = "stdlib"  # choose before fresh work; "intelligent" is also explicit
    configuration = EvaluationConfiguration("process", workers=2)
    with make_executor(
        engine, configuration, max_pending=2, allow_process=True,
    ) as executor:
        print(submit_reviewed_process_task(executor, engine, 7).result())
```

Save this as a Python file and run it with the guarded entry point. The helper's
assertion is specific to the reviewed callable and inputs; do not apply it
automatically to arbitrary application functions. Intelligent fixed-process
mode rejects ordinary `submit`, which supplies no process-safety consent.
Changing the engine for future submissions does not rerun a failed process task.
This tiny arithmetic example demonstrates the boundary, not a process speedup.

## Optional bounded routing explanation

Construct `IntelligentExecutor(..., trace_capacity=64)` to retain the newest
accepted routing decisions. `routing_trace()` returns immutable records of
backend, a fixed-category reason, exploration, and scheduler counts/limits.
`routing_counters()` gives cumulative backend/reason counts and the number of
evicted trace records. A zero capacity disables both traces and their counters.
These contain no callables, arguments, results, exceptions, workload keys,
features, or custom-policy explanation text. They explain accepted scheduling
choices; they do not prove performance or measure CPU/RSS.
