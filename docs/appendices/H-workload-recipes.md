# Appendix H: Workload recipes and decision examples

These are reasoning recipes, not tuned configurations or benchmark results. Validate each one with representative data and the actual deployment runtime. The executor provides local scheduling; application correctness and external-service contracts remain the application's responsibility.

## 1. Many network reads

**Starting point:** use threads with explicit connection and request timeouts. If a client library shares connections, confirm its documented thread-safety and pool limits. A CPU hint is irrelevant merely because response parsing eventually uses CPU.

**Admission:** set a finite `max_pending` so producers do not create unlimited logical outstanding requests. Decide whether a producer should block, receive `BackpressureError`, or shed load at an application layer. A count bound does not enforce an API rate limit or total retained response bytes.

**Observation:** benchmark real local test-server latency distributions rather than treating `time.sleep` as a complete network model. Record useful responses, timeouts, remote throttles, response sizes, connection reuse, and the user's end-to-end latency.

**When to revisit:** if response decoding dominates and is process-safe, separate it into a distinct operation and stable task key. Do not send authenticated client objects into processes. Retries and circuit breakers are planned extensions and must be explicitly implemented elsewhere if needed.

## 2. Pure Python transformations over small inputs

**Starting point:** confirm a top-level importable function returns deterministic values without relying on mutable parent state. Compare sequential, thread, and process execution using an explicit process hint and a guarded script.

**Admission:** choose a pending budget that keeps workers occupied without accumulating unnecessary input graphs. Increasing it beyond useful saturation can increase latency without increasing throughput.

**Observation:** measure cold startup separately from a warm executor. If the operation takes very little time, per-future and serialization overhead may dominate. The fact that an operation is CPU-bound does not establish that each item should be submitted separately.

**When to revisit:** if the fixed process route consistently wins and workload shape is stable, adaptive routing may add no value. If input sizes vary widely, use application-known size-class keys before considering a more complex predictor. `map_batches` provides explicit bounded batching; the caller-supplied batch function must specify how item errors and cancellation are represented. Offline configuration search can compare declared batch sizes on safe fixtures.

## 3. Image or numerical work using native extensions

**Starting point:** investigate the actual library's thread behavior, internal parallelism, and state requirements. A thread pool may already run independent native operations concurrently. A process pool may duplicate a large model or create too many native threads.

**Admission:** budget both simultaneous operations and per-operation memory. The current executor only enforces task count; add an application-controlled byte/resource permit when inputs are large and correctness permits that design.

**Observation:** sweep worker count, library-native thread count, image or array size, and result size. Include memory and initialization. A single throughput number can hide a large tail-latency or memory regression.

**When to revisit:** shared-memory buffers, worker affinity, accelerator batching, and native-thread budgets are planned catalog concepts. They should not be assumed to exist because the callable can run in a process today.

## 4. A large streaming transform

**Starting point:** use `map_bounded` when ordered output is required and the input is large or unbounded. Choose an explicit `buffersize` based on result size and consumer speed, independently from the executor's `max_pending`.

**Admission:** the two bounds serve different purposes. `max_pending` limits logically unfinished work across the whole executor. The map buffer limits this adapter's submitted-but-unconsumed results, including completed results behind an earlier item. Neither measures bytes.

**Observation:** instrument the source iterator to count read-ahead and measure retained results. Add one deliberately slow early item to expose head-of-line blocking. A completion-order collector has different semantics; do not compare it as if the same ordering guarantee were free.

**Cleanup:** explicitly close the map iterator on early exit so it attempts cancellation of buffered tasks. Running tasks still complete. Avoid building a list of every yielded result if bounded output retention is the goal.

## 5. A mixed request service

**Starting point:** keep request orchestration outside pool workers. Identify which task families truly permit processes and give each a comparable key. If an async server calls a blocking admission method from its event-loop thread, it can stall the loop; the executor is not an async admission queue.

**Admission:** select a finite submit timeout and decide how the service represents overload. A rejected admission is different from a failed accepted operation. Do not retry immediately in a tight loop, which can turn backpressure into more load.

**Observation:** record complete request latency as well as internal executor snapshots. The internal profiles omit admission and policy time, so improving their EWMA need not improve user experience. Test bursts and cross-family interference, not only a uniformly spaced batch.

**When to revisit:** tenant fairness, deadlines, rate limits, and automatic concurrency control remain planned. If the service needs those guarantees now, implement a reviewed application-level controller or choose a system that already provides them.

## 6. Tasks with external side effects

**Starting point:** state exactly what the task changes, how success is acknowledged, and whether repeating it is safe. An executor cannot roll back a sent message, payment, or database mutation merely because its future timed out.

**Admission:** use timeouts to manage waiting, not as proof that the operation stopped. Before resubmitting after an ambiguous failure, query the destination or use its explicit idempotency protocol if one exists.

**Observation:** test against a local fake destination that records operation IDs and can fail between effect and acknowledgement. Count effects independently from returned successes. A “fast failure” is not a useful throughput improvement.

**When to revisit:** durable workflows and retries need a side-effect protocol. Activepieces inspired the separation of reusable pieces and execution concerns; its durability architecture is not included in this local executor.

## 7. Choosing whether adaptation is worthwhile

Ask these questions in order:

1. Is there more than one backend that is safe and actually available?
2. Will enough comparable tasks arrive after learning begins to benefit from observations?
3. Can task keys represent the meaningful cost differences without creating one-off histories?
4. Is workload drift slow enough for the existing learner, and is periodic exploration acceptable?
5. Does an end-to-end benchmark beat a well-configured fixed policy under comparable resources?
6. Does that gain survive cold starts, noisy load, failures, and a second representative dataset?

If the first two answers are no, a fixed policy is usually the clearer baseline. If later answers are unknown, the appropriate result is an experiment, not a production speedup claim.
