# Budgeted configuration selection

`intelligent_futures.tuning.tune` selects a configuration from an explicit,
bounded candidate set using common fixture seeds, median scores, and successive
halving. It spends a caller-specified **number of evaluator calls**, concentrating
later repeats on surviving candidates. It never discovers application tasks or
silently replays ordinary submissions.

The purpose is to select backend, worker count, batch size, and profiling settings
for a workload. Selection quality and search cost are different questions from
executor implementation overhead at identical settings. A good configuration
choice does not establish an identical-configuration engine speedup, an optimum
outside the tested grid, or a future speed guarantee.

For the exhaustive mean-ranked API, see [calibration](TUNING.md). For the fixed
configuration-quality experiment, cost accounting, and held-out oracle design,
see the [predeclared protocol](OPTIMIZER-PROTOCOL.md). This guide describes the
API and makes no claim that pending benchmark measurements have succeeded.

## Offline search and online prediction are separate

`tune` is an explicit offline search: it returns the best observed median in its
**final comparable eligible cohort**. Earlier eliminated candidates cannot win.
By contrast, exhaustive `calibrate` returns the best observed mean across **all
eligible supplied candidates**, each measured for all requested repeats.

The existing [online predictor](PREDICTIVE.md) collects opt-in runtime profiles
and adjusts admission within configured worker capacities. Offline selection
does not itself learn a live admission policy. `profile_sample_interval=0`
disables predictive admission; a positive interval enables it through the
configuration's executor options. Include that dimension only when your
evaluator exercises and measures the intended profiling path.

The [24-candidate protocol](OPTIMIZER-PROTOCOL.md) fixes profiling interval to
zero for every thread/process × worker × batch candidate. Its evidence concerns
configuration search over backend/workers/batching, not learned online
adaptation. It cannot justify enabling predictive mode on those workloads.

[Declared workload features](WORKLOAD-FEATURES.md#confidence-and-comparison) are
cost proxies that improve description, not measured costs or confidence scores.
The separate [predictive-feature experiment](PRODUCTION-EVIDENCE.md#predictive-feature-experiment)
used images, two threads, batch size one, and three held-out seeds per run. It
compared batch-only and declared pixel features at sample-every-task and
sample-every-eight rates; the sampled runs did not show a consistent feature
speedup. These sequential, single-host, correlated phase observations do not
establish general online-predictor superiority. Profiling/admission overhead and
any benefit are workload-dependent. Keep fixed admission as a valid choice;
do not force expensive predictive mode when its measured net result is worse.

## Full runnable example

[examples/budgeted_tuning.py](../examples/budgeted_tuning.py) is a complete,
standard-library-only example. It really hashes small generated inputs, checks
both executor passes against a serial reference, inspects a frozen selection on
held-out inputs, rejects failed checks, and exports/loads the configuration for
explicit reuse. It uses threads only, fixed-size batches, no network, and no
production work. Its small generated workload illustrates the API; its timings
are not performance evidence or deployment recommendations.

```sh
PYTHONPATH=src python -m examples.budgeted_tuning
# Optionally save to a new file after all example checks pass:
PYTHONPATH=src python -m examples.budgeted_tuning --save choice.json
```

The main search is:

```python
from examples.budgeted_tuning import fixture_evaluator
from intelligent_futures.tuning import CalibrationObjective, Configuration, tune

report = tune(
    [Configuration("thread", workers, batch)
     for workers in (1, 2) for batch in (1, 8)],
    fixture_evaluator("training-v1"),
    max_evaluations=18,
    seed=42,
    min_repeats=3,
    max_repeats=6,
    reduction_factor=2,
    objective=CalibrationObjective(elapsed="combined"),
    allow_process=False,
)
if report.selected is None:
    raise RuntimeError("No eligible final candidate; inspect report.records")
```

With no failures, this schedule calls all four candidates three times, then the
two survivors three more times: 18 calls rather than 24 for six repeats on all
four candidates. This is call-count arithmetic, not a measured time saving.
The example measures cold construction + first pass + shutdown and a separate
warm second pass in the same pool. Fixture generation, serial reference, and
output comparison sit outside those elapsed fields. CPU time for the current
process (including its threads) covers both passes and the pool lifecycle; RSS
and batch p95 are unmeasured and uncapped. Callback wall time includes the
evaluator's preparation and validation.

For real work, replace the fixture and oracle together, version their identity,
and use representative input sizes and state. In particular, a SHA-256 smoke
example does not tell you how many workers your application should use.

## API and bounds

```python
tune(candidates, evaluate, *, max_evaluations, seed,
     min_repeats=3, max_repeats=32, reduction_factor=2,
     objective=None, allow_process=False)
```

- `candidates`: 1–128 distinct `Configuration` objects. The four fields are
  `backend`, `workers`, `batch_size`, and `profile_sample_interval=0`.
- `evaluate(configuration, seed)`: an explicitly supplied callable returning
  `Measurement`. The library does not create fixtures, infer safety, or sandbox it.
- `max_evaluations`: an integer from 1 to 4,096, at least the number of candidates
  multiplied by `min_repeats`.
- `min_repeats`, `max_repeats`: integers from 1 to 32, with the minimum no greater
  than the maximum. A failed candidate stops early despite the initial budget.
- `reduction_factor`: an integer from 2 to 128; survivor counts round upward.
- `seed`: an integer from 0 through 2**63−1. Booleans are not valid integers here.
- `objective`: a `CalibrationObjective`; defaults to cold elapsed seconds.
- `allow_process`: an actual boolean, default `False`. Any process candidate
  requires explicit `True` before the first evaluator is called.

Input validation precedes callbacks. An oversized candidate iterable is rejected
after consuming at most its 129th item; it is not fully materialized without a
bound. The iterable itself is caller code and can still block or have side effects.
These are bounds on candidate/repeat/call counts, **not elapsed time, CPU, memory,
network traffic, money, or the work inside a callback**. A single callback can
block forever. Add an appropriate external supervisor when those limits matter.

## Paired-seed median successive halving

1. Sort the configurations, then shuffle their order independently each repeat
   with a local seeded RNG. The same repeat's fixture seed goes to every active
   candidate. Input-list order therefore does not change the schedule.
2. Complete a full round of the active cohort. Permanently exclude candidates
   whose returned measurements or callbacks fail any gate.
3. After `min_repeats`, rank the surviving candidates by the median of their
   observed objective scores. Keep `ceil(active / reduction_factor)` and multiply
   the target cumulative repeat count by `reduction_factor`, capped at
   `max_repeats`. Do not eliminate if the remaining budget cannot fund even one
   additional repeat of that survivor cohort.
4. Continue complete common-seed rounds. Stop when the repeat limit is reached,
   there are no eligible finalists, or the remaining budget cannot pay for an
   entire active-cohort round. Leave leftover calls unused rather than create
   unequal final sample counts.
5. Select the smallest median only among the final eligible active cohort.
   Exact ties use `Configuration` field ordering, not callback order.

For even repeat counts the median is the midpoint of the central two values.
For `elapsed="combined"`, compute cold + warm for each repeat and take the median
of those sums; this is not a sum of separate cold and warm medians. Pairing means
shared fixture seeds, not subtraction of paired values in the ranking. The caller
must actually use seeds consistently and reset state; identical seed numbers do
not prove identical data, independent observations, or controlled caches/load.

`fixture_seeds(seed=..., repeats=...)` exposes the deterministic common seed
sequence used by both `calibrate` and `tune`. It does not execute or authorize
replay. A seeded schedule is reproducible; timing results need not be.

Successive halving is an allocation idea described by
[Jamieson and Talwalkar (AISTATS 2016)](https://proceedings.mlr.press/v51/jamieson16.html).
This implementation is a median-ranked heuristic over repeated complete fixture
evaluations, with caller-owned gates and a fixed candidate set. It is not an
implementation of the complete Hyperband algorithm, and it does not inherit a
paper's mathematical performance or convergence guarantees.

## Eligibility fails closed

Every observed repeat of a final candidate must pass all gates:

- `correct` must be `True` and `error_count` must be zero.
- Both cold and warm elapsed values must be finite and strictly positive, even
  when only one is ranked. Optional resource/latency values must also be valid.
- A configured CPU, sampled peak RSS, or batch p95 cap requires its corresponding
  measurement. Missing data fails with `cpu_unavailable`, `rss_unavailable`, or
  `batch_p95_unavailable`. Missing uncapped data remains unknown, not zero.
- Observations greater than a configured cap fail; equality passes. A nonfinite
  combined elapsed score fails with `elapsed_overflow`.

```python
objective = CalibrationObjective(
    elapsed="combined",
    max_cpu_seconds=300,
    max_peak_rss_bytes=1024**3,
    max_batch_p95_seconds=60,
)
```

Those illustrative thresholds are not application SLAs. This objective requires
the evaluator to provide `cpu_seconds`, `peak_rss_bytes`, and
`batch_p95_seconds` in every observation. The tiny example deliberately does not
use it because it does not measure all three fields.

These caps **filter observations after the evaluator returns**. They cannot
terminate a task, prevent an RSS spike, or enforce a p95 service deadline. Define
the batch p95 population and quantile calculation, including whether latency
contains admission wait, queueing, execution, and result transfer. Batch p95 is
not automatically per-item p95, and the tuner does not calculate either. Keep
measurement scope comparable when changing batch size. CPU aggregation must
include the intended child processes; a parent-only timer is insufficient for a
process-backed whole-executor budget. Sampled RSS can miss short-lived peaks.

Ordinary callback exceptions and malformed measurements produce
`callback_error_or_invalid_measurement` and permanently exclude that candidate.
Exception text and traceback are not included in the report. Diagnose details
through safe caller-owned logging rather than converting an error into a timing
or success. There are no automatic retries. Other eligible candidates may still
continue and win; their selection does not erase failed records.
`CancelledError`, `KeyboardInterrupt`, and `SystemExit` propagate immediately,
abort tuning, and do not become successful or ranked observations.

An eliminated candidate is never reactivated, even when a survivor later fails.
If no final candidate qualifies, `selected` is `None` with
`stopping_reason="no_eligible_finalists"`; no fallback or success is invented.

## Reading the report without overclaiming certainty

`TuningReport.records` retains each attempted callback's configuration, repeat,
seed, measurement or failure, exclusion reason, and elapsed callback cost.
`candidates` retains every supplied configuration, including:

- `evaluated`: eligible member of the final comparable cohort.
- `eliminated`: passed observed gates but lost a halving round. Its partial
  median remains visible; fewer observations make it ineligible for final ranking.
- `excluded`: failed an observed gate. It has no eligible score or dispersion.

An eliminated partial median may be lower than the selected candidate's later
median. That is reported in `uncertainty.lower_scoring_eliminated`; it does not
allow that candidate to win. Search can discard the truly better configuration.

For a candidate with an eligible observed score, the report supplies the median,
median absolute deviation (`mad_seconds`), minimum, maximum, evaluation count,
and callback cost. Median and MAD are descriptive summaries; small samples,
outliers, drift, contention, and early elimination remain important.
`uncertainty` compares only the selected candidate and final-cohort runner-up:
their median relative margin, paired repeat count, selected-faster/tied counts,
and whether observed ranges overlap. With only one finalist there is no runner-up
comparison. These are not confidence intervals, significance tests, posterior
probabilities, or a probability that the selection will be best next time.

`trace` records active-cohort leaders after complete repeats, including the
round's eliminations and exclusions. Scores can worsen and leaders can change.
`selection_changes` counts changes between adjacent recorded leaders;
`stable_selection_rounds` counts trailing trace points led by the selected
configuration. Neither is an early-stopping confidence criterion. The
exhaustive API's retrospective `within_ten_percent` trace is different; do not
interpret it as the budgeted API's held-out success rate.

Other report fields include `evaluations`, `unused_evaluations`, `elapsed_seconds`,
`callback_seconds`, objective, limits, seed, process consent used for this search,
hardware identity, and provenance. Stopping reasons are `budget_exhausted`,
`incomplete_round_budget`, `repeat_limit`, and `no_eligible_finalists`.
`to_dict()` exports JSON-compatible diagnostics, including explicit scope labels.
Keep full failures and metadata in an evidence record; review metadata before
sharing it externally.

## Live cost, replay cost, and matched controls

In a live search, `elapsed_seconds` measures the whole `tune` call and
`callback_seconds` sums actual evaluator wall durations, including evaluator
setup and validation. Neither should be confused with the ranked elapsed field,
which may intentionally have a narrower scope. Fixture generation, reference
execution, adoption, and held-out validation should be accounted for explicitly.
Fewer calls do not necessarily mean proportionally less wall time: eliminated
candidates may be cheaper or slower than survivors, and measurements can drift.

When replaying a prerecorded real training matrix, the evaluator usually just
looks up a row. The report's callback and total times then measure that lookup
and selector bookkeeping, **not live calibration**. Compute virtual callback
cost separately by summing the original measured callback durations for rows
actually requested. Name it replay/virtual cost, preserve its source, and report
the tiny bookkeeping time separately. A separate live run is required to test
the actual evaluator path. Synthetic timing inputs can test selection mechanics
but cannot establish a workload speedup.

`calibrate` uses all planned repeats of every candidate and ranks eligible
arithmetic means. `tune` ranks medians and prunes candidates. Comparing just
those two changes both the allocation policy and the scoring statistic. Include
an exhaustive **median** control on the same complete training rows to isolate
pruning from mean-versus-median effects. The existing exhaustive API has no median
switch: compute the matched control explicitly, or use `tune` with
`min_repeats=max_repeats=R` and `max_evaluations=len(candidates)*R` when all rows
are eligible. Failed candidates stop early in `tune`; it is not then a complete
rectangular measurement grid like `calibrate`.

The [protocol](OPTIMIZER-PROTOCOL.md) freezes all three selectors before held-out
measurement and defines all-candidate held-out oracles, regret, failure-inclusive
coverage, and complete-grid checks. An oracle is best observed in that measured
grid, not a global optimum. A technically accepted evidence row is not itself
evidence that a performance target passed. Report misses, exclusions, and failed
runs instead of dropping them from success-rate denominators.

## Holdout, adoption, and configuration reuse

`inspect_holdout(report, evaluate, repeats=..., seed=..., allow_process=False)`
tests the frozen selected configuration only with the original objective's gates.
It never retunes, changes the report, establishes dataset independence, or proves
that the selection beats the rest of the grid. Inspect every returned exclusion
before adoption. If you alter the candidate set or algorithm in response to the
holdout, use a new holdout. Distinct seeds from one generator are a same-generator
check, not evidence of resilience to a shifted input distribution.

Both `CalibrationReport` and `TuningReport` support configuration-only export:

```python
from intelligent_futures.tuning import export_configuration, load_configuration

# After separately checking the frozen selection's held-out records:
payload = export_configuration(
    report,
    workload_key="my-workload:v3|fixture:v2|dependencies:v4",
    measured_scope="Describe the actual cold, warm, CPU, RSS and latency scopes",
)
saved = load_configuration(
    payload, workload_key="my-workload:v3|fixture:v2|dependencies:v4",
    allow_process=False,
)
options = saved.executor_kwargs(allow_process=False)
batch_size = saved.configuration.batch_size
```

Export validates the selected candidate's eligibility in the final cohort and its
observed measurements. It does not inspect separate held-out records for you.
Export in the same Python runtime that performed tuning. Load parses bounded,
strict JSON, checks the workload key, hardware fingerprint, and Python identity,
and runs no evaluator or automatic recalibration. It loads no executable state.
Schema validation is not authentication; read only from trusted storage. File
I/O, bounded reading, permissions, and replacement remain caller-owned. See the
[persistence guide](TUNING-PERSISTENCE.md) for a bounded file reader and schema.

Process permission is never saved or inherited: a process choice requires fresh
`allow_process=True` on `load_configuration`, and separately on
`saved.executor_kwargs`. Considering processes in `tune` does not authorize later
holdout or construction automatically. The caller must still supply import-safe,
pickle-compatible functions and inputs, a main guard, and explicit
`TaskHints(kind="cpu", process_safe=True)` for process dispatch. Nothing here
proves that an arbitrary evaluator actually uses its proposed backend safely.

`executor_kwargs` sets constructor options only. Preserve the measured dispatch
path, buffering, hints, and explicit batching when adopting a choice. A matching
identity cannot detect changed load, thermal conditions, memory availability,
dependency semantics, or distribution shift. Version the workload key and
remeasure when assumptions change. Saved-configuration reuse avoids another
search; it is distinct from a warm second pass in an already-running pool.
