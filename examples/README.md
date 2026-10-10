# Runnable examples

Install the repository package (`python -m pip install -e .`), then run these
from the repository root. Every process example has a main guard and uses a
top-level callable, so it works with the executor's default `spawn` context.
The finite core examples below need no external services, accounts, network
requests or third-party dependencies. The separate `mcp_server.py` example needs
the optional MCP dependency and a local stdio client; it is an interactive server,
not a finite verification example.

```sh
python -m examples.profiling
python -m examples.bounded_pipeline
python -m examples.cancellation
python -m examples.custom_policy
python -m examples.process_hints
python -m examples.predictive_batches
python -m examples.registered_tasks
python -m examples.tuned_execution
python -m examples.budgeted_tuning
```

- **Profiling:** Stable task keys, explicit process eligibility, per-key/kind
  exploration and final counters. Full profiles include queue/completion delay;
  load-aware routing uses their uncontended subset plus queue/dispatch occupancy.
  Startup/serialization remain in that subset; it is not intrinsic CPU demand.
- **Bounded pipeline:** Lazy JSON input, ordered consumption, bounded buffering,
  and cleanup when the consumer stops. Slow early results can delay later results.
- **Cancellation:** Deterministically queue a task behind one running worker,
  cancel the queued task, then use a cooperative stop flag for the running one.
  A Future cannot kill a running thread or process.
- **Custom policy:** Route a named, process-safe class of CPU work. Keep `choose`
  fast and side-effect free because the executor invokes it under its lock.
- **Process hints:** Picklable inputs, importable functions, a main guard, and
  explicit CPU hints. Processes do not share ordinary Python mutable state.

- **Predictive batches:** Explicit numeric feature summaries, strict JSON profile
  transfer and real tuple-batch execution without replaying ordinary tasks.
- **Registered tasks:** Trusted callable registration, bounded JSON task records,
  result inspection and explicit service cleanup.
- **Tuned execution:** Explicit repeatable SHA-256 calibration and independent
  held-out input inspection; an API illustration, not a speedup claim.
- **Budgeted tuning:** Bounded search, frozen holdout validation and schema-v2
  configuration reuse. Optional `--save` creates a new JSON file without overwrite.

All executor examples use a context manager or explicit waiting shutdown. Keep
that lifetime boundary: omitted shutdown can settle deferred work with
`RuntimeError` during interpreter teardown. Normal completion callbacks are
serial on one owned pump; never wait for another same-executor Future inside a
callback even when workers are free. Genuine stdlib Future APIs do not imply
identical callback/thread/implicit-exit scheduling. Physical scheduling-item
bounds do not limit payload bytes, results or RSS.

For source-tree development without installing, prefix each command with
`PYTHONPATH=src` on Unix-like shells. Do not submit to a saturated executor from
one of its own workers and then wait for the result; nested waits can deadlock.
