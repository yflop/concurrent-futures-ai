# Runnable examples

Install the repository package (`python -m pip install -e .`), then run these
from the repository root. Every process example has a main guard and uses a
top-level callable, so it works with the executor's default `spawn` context.
No external services, accounts, network requests, or dependencies are required.

```sh
python -m examples.profiling
python -m examples.bounded_pipeline
python -m examples.cancellation
python -m examples.custom_policy
python -m examples.process_hints
```

- **Profiling:** Stable task keys, explicit opt-in exploration, and final counters.
  The learner measures queue-and-completion latency; it does not infer intrinsic
  CPU demand or classify arbitrary functions.
- **Bounded pipeline:** Lazy JSON input, ordered consumption, bounded buffering,
  and cleanup when the consumer stops. Slow early results can delay later results.
- **Cancellation:** Deterministically queue a task behind one running worker,
  cancel the queued task, then use a cooperative stop flag for the running one.
  A Future cannot kill a running thread or process.
- **Custom policy:** Route a named, process-safe class of CPU work. Keep `choose`
  fast and side-effect free because the executor invokes it under its lock.
- **Process hints:** Picklable inputs, importable functions, a main guard, and
  explicit CPU hints. Processes do not share ordinary Python mutable state.

For source-tree development without installing, prefix each command with
`PYTHONPATH=src` on Unix-like shells. Do not submit to a saturated executor from
one of its own workers and then wait for the result; nested waits can deadlock.
