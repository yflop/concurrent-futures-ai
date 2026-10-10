# Optional MCP stdio integration

Version 0.2.0 adds a local MCP adapter using the official Python SDK v2:

```sh
python -m pip install "intelligent-futures[mcp]"
```

The core and `framework` remain dependency-free. The extra installs
`mcp>=2.3,<3` and its transitive dependencies. Importing the core never imports
MCP. Missing extras produce an actionable installation error when starting the
adapter. No network server is started by the supplied runner.

## Create an explicit allowlist

Save this as `my_tasks.py` (or use `examples/mcp_server.py`):

```python
from intelligent_futures.framework import TaskRegistry, TaskService
from intelligent_futures.integrations.mcp import run_stdio


def add(a: int, b: int) -> int:
    return a + b


if __name__ == "__main__":
    registry = TaskRegistry()
    registry.register("add", add, description="Add two integers")
    service = TaskService(registry, max_workers=2, max_pending=8, max_tasks=32)
    run_stdio(service)
```

Configure your MCP host to launch the installed environment's Python executable
with the absolute script path. A typical host configuration has this shape;
consult that host's configuration format:

```json
{
  "mcpServers": {
    "intelligent-futures": {
      "command": "/absolute/path/to/venv/bin/python",
      "args": ["/absolute/path/to/my_tasks.py"]
    }
  }
}
```

On Windows use the virtual environment's `Scripts/python.exe`. The executable and
script paths must exist on the host machine. Never put credentials in this sample
or write diagnostics with `print()` to stdout; stdio reserves stdout for protocol
messages. Use logging to stderr. Only run servers and registered callables you trust.

## Discover and manage work

Tools exposed by the adapter:

| Tool | Purpose |
| --- | --- |
| `list_registered_tasks` | Names, descriptions, and JSON parameter schemas |
| `submit_task` | Submit a registered name with an `arguments` object; receive a task ID |
| `get_task` | Inspect status and JSON result/error without blocking |
| `cancel_task` | Cancel queued work; report whether cancellation was accepted |
| `forget_task` | Release a terminal record and its result |
| `list_tasks` | Bounded listing of retained records |
| `executor_metrics` | Service and executor counters/limits |
| `search_concepts` | Read-only search of implemented, experimental, or planned concepts |

Resources `intelligent-futures://registered-tasks` and
`intelligent-futures://concepts` provide discovery data. A catalog label is never
permission to invoke a task. Submit accepts only the live registered allowlist.
Expected tool errors use MCP error results, not traceback text. Results include
structured JSON and a text representation for clients.

Example interaction: discover `add`, submit `{ "name": "add", "arguments":
{ "a": 2, "b": 3 } }`, then call `get_task` with the returned `task_id` until its
status is terminal. Read `result` only when completed, then `forget_task` when the
result is no longer needed. Use a reasonable polling interval; this adapter has
no durable tasks, server-push completion subscription, or hard execution deadline.

## Ownership and trust

`create_server(service)` returns an SDK low-level Server for embedding.
`run_stdio(service)` runs that server on stdio and owns the service's lifespan.
Server shutdown closes admission and cancels queued tasks without waiting in the
protocol loop. Running callables cannot be stopped and Python worker threads may
delay process exit until they finish. An embedder that uses the server must run
its normal SDK lifespan, and must not share a service it expects to remain open
after that server exits.

There is no authentication, tenant isolation, HTTP transport helper, dynamic
plugin loading, eval, shell tool, or client-controlled Python import. Anyone with
access to the stdio connection can invoke every registered task and read all
retained task results. Do not expose this service to untrusted users. Registered
code runs with process permissions; this integration is not a hostile-code sandbox.
Do not register callables that accept arbitrary code, commands, URLs, paths, or
secrets without application-level validation/authorization appropriate to them.

Review [framework limits](FRAMEWORK.md) and [security guidance](../SECURITY.md).
The JSON bounds apply after MCP parsing and do not bound raw transport input or
trusted callable allocations. Tool descriptions/results are application data and
must not be treated by a host as authorization for additional actions.

## Verify the real protocol

```sh
python -m pip install ".[mcp]"
python -m unittest tests.test_framework tests.test_mcp -v
```

The tests launch an actual stdio subprocess and official SDK client for initialize,
discovery, tool calls, resources, errors, and lifecycle checks. Core-only runs skip
optional SDK tests; CI has separate extra-installed jobs so those skips cannot
stand in for MCP validation.

Registration metadata is public to the connected client: names, descriptions,
parameter defaults in schemas, and `hints.key` in metrics can all be discovered.
Never put secrets in those fields or default arguments. Keep credentials in
application-owned state and register narrow operations that do not return them.
