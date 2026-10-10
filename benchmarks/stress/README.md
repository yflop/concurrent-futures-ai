# Extended regression stress suite

These scripts originated as an independent 0.2.0 stress suite and are rerun against 0.3.0 without modifying tested sources. Set `PYTHONPATH` and `TEST_ROOT` to the checkout under test. All scripts use standard-library modules except the explicitly optional MCP scripts. Harness assertions indicate contract violations; throughput numbers are observations, not guarantees.

## Reproduce

```sh
# Run from the repository root; MCP commands require the optional extra.
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$PWD/src"
export TEST_ROOT="$PWD"
python benchmarks/stress/stress.py --output benchmarks/results/extended-stress.json
python benchmarks/stress/soak.py > benchmarks/results/service-soak.json
python benchmarks/stress/churn_limitation.py > benchmarks/results/churn-limitation.json
python benchmarks/stress/realistic.py > benchmarks/results/realistic.json
python benchmarks/stress/mcp_stress.py > benchmarks/results/mcp-stress.json
python benchmarks/stress/raw_protocol.py > benchmarks/results/raw-protocol.json
```

`stress.py` runs ten separate processes, each with a 90-second timeout. Its `--case` option isolates a scenario. Other scripts terminate after fixed finite work; `mcp_stress.py` also has a 120-second async deadline and protocol reads have eight-second timeouts. For a hostile or changed implementation, wrap standalone scripts with an OS-level timeout as an extra guard. `soak.py` has a 20-second production window; it waits for accepted work to drain.

## Coverage
- 5,000 ordered bounded-map outputs, source failure propagation, abandoned iterator close, and finite map timeout cleanup.

- 24 core producers, 16,800 identity tasks, bounded 32-profile-key eviction and final metric conservation, submit-to-result p50/p99.
- 20 service producers, 7,000 echo tasks with cancellation and forget churn, final outcome counters and no retained records.
- Service physical-queue saturation under 5,000 cancellation/forget attempts behind a blocked worker.
- 60 core and 50 service producer/shutdown races, repeated wait=False then wait=True, worker completion and metrics checks.
- Spawn-process output correctness, deliberate child process exit, BrokenProcessPool propagation, repeated shutdown and accounting.
- Safe nested submissions, done-callback submission, finite-timeout saturated nested rejection; no assertion that cyclic dependencies are supported.
- Invalid JSON shapes, cycles, depth, NaN/infinity, surrogate, large integer/string, exact result-size boundary.
- Five capacity-and-forget cycles with 64 approximately 64 KB results and tracemalloc pressure measurements.
- 20-second, 32-producer service soak with 1 KB payloads, cancellation and retention cleanup, memory and capacity sampling.
- MCP SDK 2.3 real stdio: eight connections, 64 rounds, 32 simultaneous submissions per round, polling/forget, malformed tool argument rejection and limits.
- Raw protocol: 60 malformed JSON/message shapes, each followed by successful valid discovery, then stdin EOF and verified closed-service marker.
- Explicit measurement of the documented core cancellation-tombstone limitation.
- Real deterministic JSON parsing/filtering/aggregation/sorting plus gzip/SHA256 transformation, three randomized-order cold repetitions comparing sequential, stdlib threads/processes, and intelligent threads/processes. Same four active compute workers; intelligent process mode has an unused required thread-pool configuration.

## Interpretation and limits

These tests do not establish all possible interleavings, hard RSS bounds, production throughput, security isolation, distributed behavior, arbitrary task termination, long-running stability, or Windows/macOS correctness. Python callables remain trusted. `tracemalloc` measures Python allocations, not full RSS or child-process memory. Core cancellation tombstones are documented and deliberately recorded, not classified as a new defect. The service uses a different cancellation contract that preserves physical admission until dequeue. Process pools intentionally remain broken after a worker crash; there is no recovery feature.

See docs/VALIDATION.md for the exact version, environment, test counts and raw
results. The protocol stress harness uses POSIX selectors; it is a Linux-local
check, not a Windows compatibility claim. Other operating systems are verified
separately by the unit/SDK CI matrix.
