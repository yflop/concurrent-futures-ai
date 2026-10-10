# Reuse a measured configuration

`intelligent_futures.tuning` provides `export_configuration`, `load_configuration`,
and the frozen `SavedConfiguration` value. These are configuration-only JSON APIs.
They never discover work, invoke a calibration callback, load Python source/pickle,
or automatically recalibrate. Existing calibration/export/load function signatures
are unchanged; calibration and tuning reports now also carry scheduler, strategy
and package-version metadata.

## Save after explicit calibration

```python
from pathlib import Path
from intelligent_futures.tuning import export_configuration

# report = calibrate(explicit_candidates, your_evaluator, repeats=3, seed=17)
# Export in the Python runtime that performed calibration.
workload_key = "image-resize:v3|dataset:2026-10-sha256-abc|Pillow:11.0|seed:17"
text = export_configuration(
    report,
    workload_key=workload_key,
    measured_scope="Whole fixture including executor setup and result validation",
)
Path("resize-choice.json").write_text(text, encoding="utf-8")
```

Export requires an eligible selection, its measured candidate score, and its
successful repeat measurements. No callback, fixture, raw input, timing record,
credential, process consent, or executable code is included. `measured_scope` is an
optional short caller-written description; omit it rather than storing sensitive
input details. Provenance is the report's short provenance label. Export preserves
the report's scheduler/strategy identity and producing package version, rather
than relabeling old measurements as if they used the current implementation.
Incompatible scheduler/strategy revisions are rejected.

## Load and reuse without calibration

```python
from pathlib import Path
from intelligent_futures import IntelligentExecutor
from intelligent_futures.tuning import load_configuration, MAX_SAVED_CONFIGURATION_BYTES

# Bounded file reading is the caller's responsibility too.
with Path("resize-choice.json").open("rb") as stream:
    raw = stream.read(MAX_SAVED_CONFIGURATION_BYTES + 1)
if len(raw) > MAX_SAVED_CONFIGURATION_BYTES:
    raise ValueError("saved configuration is too large")
saved = load_configuration(raw.decode("utf-8"), workload_key=workload_key)

with IntelligentExecutor(**saved.executor_kwargs()) as executor:
    # Submit your own tasks here, with your own batching/validation.
    pass
batch_size = saved.configuration.batch_size
```

`executor_kwargs()` returns constructor settings only. Batch formation remains the
caller's responsibility. Process selection requires `allow_process=True` separately
on **both** `load_configuration` and `saved.executor_kwargs`; permission is never
persisted. The caller must still ensure import-safe workers, serialization safety,
correct task hints, and an appropriate `if __name__ == "__main__"` entry point.
For a saved process configuration, submit CPU work with
`TaskHints(kind="cpu", process_safe=True, key=...)`; ordinary `submit` does not
consent to process execution. Saved settings use the explicit `hints` policy, so
loading a configuration never silently switches routing to the adaptive policy.

Current constructor settings use `max_pending=2 * workers`. The executor enforces
both a logical unfinished-task bound and a physical owned-queued-plus-dispatched
item bound, with each backend's dispatches capped by its workers. These limits do
not bound bytes, RSS, callable allocations, or caller-retained results. A positive
saved `profile_sample_interval` enables predictive profiling/admission with a
throughput objective capped at the saved worker count; it can reduce the global
admission limit further. Current queue/dispatch-slot load is observed afresh, not
restored from the configuration file. It is not CPU utilisation, and a global
predictive limit can leave another backend idle.

Keep the context manager shown above, or call `shutdown(wait=True)` explicitly,
to guarantee accepted work drains before interpreter exit. Saved configuration
reuse does not supply implicit stdlib teardown equivalence, task persistence,
preemption, automatic retries, or callback/dependency-deadlock isolation. Normal
completion callbacks run serially on an owned pump; they must not wait for another
Future from that executor even when workers are free. Standard Future APIs do not
imply identical stdlib callback/thread scheduling.

The loader raises `ValueError` for a workload, hardware, Python runtime, scheduler
revision, or strategy revision mismatch. It does not fall back or transfer the
selection silently. Schema-v1 files are rejected with an explicit instruction to
recalibrate; there is no unsafe legacy opt-in. Run a new caller-owned calibration
and export schema v2 rather than changing the version number in an old file.

By default the loader detects current hardware. Controlled environments and tests
may pass `hardware=HardwareCapabilities(...)`; fields are revalidated and its
fingerprint recomputed, rather than trusting cached values. Supply an accurate
current snapshot: overriding detection does not establish machine equivalence.

## Identity and limits

The caller must version `workload_key` whenever the algorithm, dataset/fixture
population, relevant dependencies, expected input sizes, state, or execution
assumptions change. The API requires a nonempty key but cannot verify its meaning
or that a version is actually present. A matching key is the caller's assertion.

Hardware identity covers the existing `HardwareCapabilities` fingerprint: logical
CPUs, affinity count, CPU quota, machine, system, and CPU model. It does **not** prove
identical available memory, contention, thermal conditions, disk/network behavior,
or background load. Python identity includes implementation, cache tag, full
version/build string and ABI flags. Export records the exporting Python runtime;
export in the same process/runtime as the original calibration. Dependency versions
are not automatically discovered: include them in the workload key.

Scheduler and configuration-to-executor strategy identity are checked separately
from that caller-owned key. The public constants in `intelligent_futures.tuning`
are:

- `SAVED_CONFIGURATION_SCHEMA_VERSION = 2`
- `SCHEDULER_REVISION = "owned-removable-queue-physical-dispatch-v1"`
- `STRATEGY_REVISION = "configuration-executor-kwargs-v1"`

Both saved revisions must match the running implementation on loading and again
when `SavedConfiguration.executor_kwargs()` is used. This guards reuse across
material queue/lifecycle or configuration interpretation changes. The recorded
`package_version` is provenance, not an exact-match gate: package versions may
differ, including patch releases, when both strategy revisions remain compatible.
Matching package versions alone cannot substitute for revision checks or validate
the workload, hardware, Python runtime, or current operating conditions.

`CalibrationReport`, `TuningReport`, and `SavedConfiguration` carry defaulted
`scheduler_revision`, `strategy_revision`, and `package_version` fields. Generated
reports capture the producing package version; export preserves it. Manually
constructed report metadata remains the caller's responsibility, not authenticated
proof of which source executed.

Schema v2 is a single JSON object with exactly these fields:

- `schema_version`: integer `2` (not a boolean or float)
- `configuration`: exactly `backend`, `workers`, `batch_size`, and
  `profile_sample_interval`, validated by `Configuration`
- `workload_key`: nonempty printable text, at most 512 characters
- `hardware_fingerprint`: lowercase 64-character SHA-256 hexadecimal digest
- `python_runtime`: nonempty printable text, at most 2,048 characters
- `provenance`: nonempty printable text, at most 256 characters
- `measured_scope`: `null` or nonempty printable text, at most 2,048 characters
- `scheduler_revision`: nonempty printable text, at most 256 characters; must
  match `SCHEDULER_REVISION` for reuse
- `strategy_revision`: nonempty printable text, at most 256 characters; must
  match `STRATEGY_REVISION` for reuse
- `package_version`: nonempty printable text, at most 128 characters; provenance
  retained without requiring an exact package-version match

The total UTF-8 JSON size is limited to 16,384 bytes. Unknown/missing fields,
duplicate keys at any level, NaN/infinity, malformed/truncated JSON, invalid types,
and out-of-range values fail closed. Even a field within its character limit may
exceed the overall serialized byte limit. Loading accepts a string, not a path,
bytes, object, callback, or module name. File access, atomic replacement, storage
permissions, integrity protection, and recovery are caller-owned.

JSON validation is not authentication. A forged but schema-valid file can specify
an allowed configuration; use trusted storage and ordinary resource limits.
`SavedConfiguration` may also be constructed directly for data handling, but only
`load_configuration` checks workload/current hardware/Python runtime compatibility.
`executor_kwargs()` independently rejects an incompatible saved scheduler or
strategy revision even for a directly constructed value.

A saved choice is merely the eligible configuration selected from the candidates
observed in that calibration. It is not evidence of global optimality, a future
speedup guarantee, or independent holdout validity. Configuration selection
quality is the objective; reuse alone is not evidence of an execution-engine
speedup against the same stdlib settings. Re-measure when assumptions or conditions
change; keep any holdout independent of tuning decisions.
