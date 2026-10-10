# Registered-task framework

Added in 0.2.0. `intelligent_futures.framework` uses only the standard library;
MCP is not needed. Existing `IntelligentExecutor` and real `Future` APIs are
unchanged. The service is an application layer, not a replacement Executor.

```python
from intelligent_futures.framework import TaskRegistry, TaskService


def add(a: int, b: int = 1) -> int:
    return a + b


registry = TaskRegistry()
registry.register("add", add, description="Add two integers")
service = TaskService(registry, max_workers=2, max_pending=8, max_tasks=32)
try:
    job = service.submit("add", {"a": 2, "b": 3})
    print(job["task_id"])
    # get is nonblocking: check status before reading result.
    print(service.get(job["task_id"]))
finally:
    service.close()
```

## Registration and validation

`register(name, callable, *, description='', hints=None)` creates a named explicit
allowlist entry. Registration names are not import paths. `registry.list()`
returns definitions with `as_dict()` discovery metadata. A service snapshots the
registry when constructed; later registrations do not change the running service.

Parameters must be keyword-callable: positional-only parameters and variadic
`*args`/`**kwargs` are unsupported. Type annotations specify supported JSON types
such as `str`, `int`, `float`, `bool`, `None`, `list`, `dict`, typed lists/dictionaries,
and optional unions. Unannotated parameters allow JSON values. Unknown/missing
arguments and wrong types are rejected before submission. Integers and booleans
are distinct; data is not coerced. Arbitrary Python objects, non-string dictionary
keys, non-finite floats, invalid Unicode scalar strings (including lone surrogates),
cycles, and oversized JSON are rejected. An annotation
is not a business-rule validator: implement task-specific limits in trusted code.

## Service operations

- `list_definitions()` returns registered names, descriptions, and input schemas.
- `submit(name, arguments)` validates and returns a task snapshot immediately.
- `get(task_id)` returns the current snapshot without waiting.
- `cancel(task_id)` adds `cancel_accepted` to a snapshot. Only queued work can be
  canceled; running work continues and cancellation returns false.
- `forget(task_id)` removes a terminal record and its retained result. Pending or
  running tasks cannot be forgotten. Subsequent lookup returns an unknown-ID error.
- `list_tasks(limit=100)` returns at most the requested number of retained records.
- `snapshot()` returns service limits/counters and executor measurements.
- `close(wait=True, cancel_futures=True)` closes admission, optionally cancels
  queued work, and optionally waits for running tasks. Never block on close from
  a service worker. Shutdown cannot terminate a running callable.

Snapshots include `task_id`, `name`, `status`, timestamps, `result`, and `error`.
Statuses are `queued`, `running`, `completed`, `failed`, and `cancelled`.
A successful JSON `null` result is distinguished from pending work by status.
Failures use safe error codes/messages, not exception text or tracebacks. Invalid
or oversized results become failed tasks. Tasks may already have performed side
effects before failing or exceeding the result limit; there are no automatic retries.
`TaskServiceError` exposes `code`, `message`, and `as_dict()` for expected API errors.

## Capacity and lifecycle

Defaults: four workers, 64 pending tasks, 256 retained task records, 65,536 input
JSON bytes, and 1,048,576 result JSON bytes. Set these explicitly for your workload.
The service uses thread-only routing and immediate rejection on saturation.
`max_pending` includes work physically waiting for a worker, even canceled work
whose wrapper has not drained. `max_tasks` counts retained records, including
completed results. Clients must forget terminal records to admit more once full.

Canceling and forgetting a queued task does not reclaim its physical admission
slot until the wrapper is dequeued. This prevents submit/cancel churn from filling
an unbounded backend queue. Service cancellation is a separate contract from
calling `Future.cancel()` directly on the core executor.

JSON-size bounds are not a sandbox or an RSS limit. Trusted callables can allocate
arbitrary memory, perform I/O, or run indefinitely. Serialized-result limits apply
after the callable produces its return value. Records are process-local, not
durable; restarting loses all task IDs/results. Registration, protocol access,
and data retention require application-specific review.

Service counters describe user-task outcomes. Nested `executor` counters describe
wrapper execution, so a sanitized user-task failure or skipped canceled invocation
can still be a completed executor wrapper. Use top-level service `counts` for
application success/failure/cancellation and `pending` for physical admission.
Repeated cancellation of an already-canceled record is accepted idempotently.
Task listings return the oldest retained records first, making drain-and-forget
cleanup possible without losing discoverability behind newer records.

Discovery schemas include parameter defaults. Names, descriptions, defaults, and
hint/profile keys must not contain secrets; every adapter client can discover them.
