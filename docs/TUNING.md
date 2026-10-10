# Explicit offline configuration calibration

`intelligent_futures.tuning` selects an actually measured configuration from a
bounded caller-supplied candidate set. It can choose backend, workers, batch size,
and fixed versus predictive admission. This is an offline empirical selector,
not a global optimizer, online task replayer, or performance guarantee.

Use representative, idempotent local fixtures. Calibration deliberately runs
multiple fixture evaluations. The caller owns side effects, reset/isolation,
correctness checks, process safety, and measurement scope. The library invokes
only `evaluate(configuration, seed)`; it never discovers application callables,
executes hidden warmups, retries failed tasks, or reruns production submissions.

## Minimal use

```python
from intelligent_futures.tuning import (
    Configuration, Measurement, CalibrationObjective, calibrate, inspect_holdout,
)

# Implement with your own fixtures, oracle, and genuine timers.
def evaluate(config, seed):
    cold, warm, correct = run_my_idempotent_fixture(config, seed)
    return Measurement(cold_seconds=cold, warm_seconds=warm, correct=correct)

report = calibrate(
    [Configuration("thread", workers, batch, sampling)
     for workers in (1, 2, 4)
     for batch in (1, 16)
     for sampling in (0, 8)],
    evaluate, repeats=3, seed=42,
    objective=CalibrationObjective(elapsed="cold"),
)
if report.selected is None:
    raise RuntimeError("No candidate passed every calibration check")
print(report.selected)
```

A complete runnable example is [tuned_execution.py](../examples/tuned_execution.py).
It explicitly performs cold and warm SHA-256 fixture passes, checks every output,
then inspects the selected configuration on separate held-out fixture inputs.
It does not establish a speedup over other engines or other machines.

## Configuration and executor use

`Configuration(backend, workers, batch_size, profile_sample_interval=0)` is frozen.
Backend is `thread` or `process`; workers are 1–4096; batch size is 1–1,000,000;
sampling interval is 0–1,000,000. Booleans are not integers for these fields.
Zero sampling means fixed admission (`predictive=False`), a legitimate candidate
that can beat predictive execution when profiling/admission overhead dominates.
A positive interval enables predictive admission with that sample interval.

`config.executor_kwargs(allow_process=False)` returns options for a fresh
`IntelligentExecutor`: selected worker counts, `max_pending=2*workers`, hints
routing, and optional throughput predictive objective bounded by worker count.
It does not construct or mutate a pool. Batch size stays caller-owned: set both
`min_batch_size` and `max_batch_size` in `map_batches` to the selected size, or
explicitly dispatch chunks of that size. Use exactly the execution path measured
by the callback; changing buffering or dispatch later may change performance.

Process candidates require `calibrate(..., allow_process=True)` before *any*
callback is invoked. Process executor options and held-out execution require
explicit permission separately. This permission authorizes considering processes;
it cannot certify that the callback is safe. Process dispatch still requires
`TaskHints(kind="cpu", process_safe=True)` with hints routing, pickle-compatible
functions and inputs, and the normal `if __name__ == "__main__"` spawn guard.
Callbacks can run arbitrary code, so the library cannot sandbox a dishonest
callback or prove that its execution matches the proposed configuration.

## Measurements, objectives, and exclusion

Callbacks must return `Measurement`, not an unvalidated dictionary:

- `cold_seconds` and `warm_seconds`: finite, strictly positive elapsed seconds
- `correct`: explicit boolean from a genuine correctness oracle
- `error_count=0`: nonnegative integer, at most 1,000,000
- `cpu_seconds=None`: optional finite nonnegative whole-fixture CPU seconds
- `peak_rss_bytes=None`: optional integer sampled peak, 0 through 2**63−1

Document whether cold timing includes initialization/shutdown and which resources
CPU/RSS cover. Warm timing should exclude setup. The library cannot verify timer
quality, correctness oracles, CPU aggregation, or missed RSS spikes. Timing values
must be real observations; zero-duration fixtures need more work or a better timer,
not fabricated floor values. Inputs/results, callback names, exception text, and
tracebacks are not retained in reports.

`CalibrationObjective(elapsed="cold" | "warm" | "combined", max_cpu_seconds=None,
max_peak_rss_bytes=None)` ranks eligible candidates by arithmetic mean elapsed
across all repeats. Combined scores are cold + warm seconds; a sum too large
for a finite float excludes that observation as `elapsed_overflow`. Cold and warm
measurements remain distinct, unchanged fields. Every repeat must be correct, error-free, and within each
specified cap. An unavailable capped resource fails closed. Caps filter measured
observations; they do not enforce hard runtime CPU/RSS limits or terminate work.
Uncapped missing resource metrics remain unknown, not zero.

Malformed callback results and ordinary callback exceptions are recorded as
`callback_error_or_invalid_measurement`. Planned remaining evaluations continue;
there are no extra retries. One failed repeat excludes the entire configuration,
so successful-only timing cannot make an unreliable candidate win. If every
candidate is excluded, `selected` is `None`; no fallback is invented.
`CancelledError`, `KeyboardInterrupt`, and `SystemExit` propagate immediately;
there are no further callbacks and no partial recommendation is returned. The
library cannot forcibly stop or time out a running callback or its child work.

## Bounds, order, provenance, and cost

A calibration requires explicit `repeats` (1–32) and nonnegative `seed`
(0 through 2**63−1), with 1–128 distinct candidates. All candidate validation and
process consent checks finish before the first callback. Candidate iterables are
consumed only up to the 129th item to reject oversize input. At most 4,096 callback
invocations occur. These count bounds do not bound callback wall time or memory.

Candidates are sorted then shuffled with a local seeded RNG; each candidate runs
all planned repeats consecutively. Repeat fixture seeds are identical across
candidates to support matched inputs. Input-list order does not change scheduling;
noise, environmental drift, and stateful fixtures can still change results.
Ties use the frozen configuration's field order, not evaluation order.

The immutable report includes all evaluation records (configuration, repeat,
fixture seed, measured values, exclusion, callback wall cost), all candidate scores
and total callback costs, selected configuration, objective, repeat/seed/permission
settings, total calibration elapsed time, and detected hardware metadata and
fingerprint. `report.to_dict()` exports an independent JSON-compatible report.
The provenance is `caller-owned-offline-calibration-v1`; hardware detection has the
limitations described in [PREDICTIVE.md](PREDICTIVE.md). Record additional fixture
version, timer scope, dependency versions, and hardware/load context alongside the
report when reproducing results. Review hardware/timing metadata before sharing.
Reports are not loaded automatically; no pickle or executable profile is imported.

## What the convergence trace does and does not show

After each fully evaluated candidate, the report records the best eligible mean
seen so far and its relative gap to the final best eligible calibration mean.
`within_ten_percent` means that incumbent was at most 1.10 times that final mean.
`evaluations_to_stable_near_best` identifies the first such prefix; because the
incumbent score cannot worsen, all later prefixes stay within that band. The count
includes excluded evaluations. Candidates lacking all planned repeats never enter
the ranking. Relative gaps too large for a finite float are reported as `None`,
as are gaps before any eligible candidate; `within_ten_percent` remains false.

This is a **retrospective best-observed calibration trace**, computed after the
bounded experiment finishes. It is not an early-stopping rule, a confidence
interval, convergence to the true/global optimum, or held-out convergence. The
best observed candidate can be noise-favored, and an untested configuration can
be better. No speedup, generalization, or portability promise follows from it.

## Held-out inspection without automatic tuning leakage

```python
heldout_records = inspect_holdout(
    report, evaluate_on_independent_holdout, repeats=3, seed=9001,
    allow_process=False,
)
```

Only `report.selected` is evaluated, with the original objective's checks. The
immutable selection and calibration trace are unchanged, even if held-out
correctness fails or elapsed time regresses. Supply genuinely independent inputs
and preserve the held-out result separately. Different seeds alone do not prove
independence. This API cannot prevent manually changing the candidates after
viewing held-out results; doing so requires a fresh holdout for honest validation.

The real-world benchmark's `Configuration` has the same four fields. Integrators
can replace that local model with this one, or explicitly convert it using
`Configuration(**dataclasses.asdict(existing_config))`; adapt measured rows to
`Measurement` after validating their correctness and metric scope. No benchmark
runner or production task stream is executed by importing this module.

## Reusing a measured choice

The configuration-only [persistence API](TUNING-PERSISTENCE.md) exports a successful
selection as bounded strict JSON. Loading checks caller workload identity,
hardware fingerprint, Python runtime and fresh process consent, and runs no
calibration callback. This avoids repeating search cost for a scenario the caller
has verified is compatible. A matching fingerprint/key cannot detect changed
load, input distribution, dependencies or an unsuitable deployment automatically.
