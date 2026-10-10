# Security and trust boundaries

This is an alpha trusted-code concurrency library, not a sandbox. Do not submit
untrusted Python code or unpickle attacker-controlled objects. Processes isolate
address spaces, not filesystem, credentials, network, or user permissions.

No network/LLM calls, telemetry export, persisted profiles, task retries, or
credential handling are built in. Snapshot keys are caller-controlled metadata:
keep PII and secrets out. Application callables can still transmit or log data.

Bounded task count does not bound bytes, CPU, memory, file descriptors, network
requests, or process lifetime. A hung running task can block shutdown indefinitely.
Use external operational isolation for hostile or resource-heavy work.

Custom policies execute trusted code under a lock. A blocking policy can stall
submission; a malicious policy can perform arbitrary actions. Process routing
validation protects only accidental selection without explicit caller consent.

## Reporting

Do not disclose credentials or exploit data in public issues. Before publication,
contact the repository owner through their existing private channel. After
publication, use GitHub private vulnerability reporting if the owner enables it.
No reporting endpoint or response SLA is assumed to exist yet.

### Cancellation churn and memory

The admission limit is not a strict physical queue or memory bound. Cancelling a
queued Future frees logical admission immediately, but stdlib pools can retain
its cancelled work item and arguments until a worker drains the queue. Repeated
submit/cancel loops behind blocked workers can therefore accumulate tombstones.
Rate-limit producers and cancellation churn; do not treat this executor as a
hard memory limiter. A removable bounded physical queue is planned work.

## Optional registered tasks and MCP (0.2.0)

The stdio MCP adapter is for a trusted local client. It has no authentication or
multi-tenant isolation: its client can invoke all registered tasks and inspect
all retained results. The supplied runner opens no listening port. Do not expose
it publicly or wrap it in a network service without separate access controls,
transport limits, and application security review.

Only application-registered callables can run. Client strings are never Python
imports, expressions, shell commands, or plugin installation instructions. This
allowlist is not a sandbox: registered code retains full process permissions and
must validate its own business inputs, side effects, file paths, and destinations.
Task errors omit exception messages/tracebacks to avoid accidental disclosure;
returned task data can still contain sensitive content chosen by the callable.

Pending work, retained records, and serialized JSON have explicit bounds. These
do not bound SDK parsing, callable allocations, execution duration, or total RSS.
Cancellation stops only queued work; it cannot interrupt a running callable.
Stdio stdout is reserved for protocol messages: log to stderr instead. See
[the MCP guide](docs/MCP.md) for ownership, cleanup, and deployment limits.

Registered task descriptions, parameter defaults, names, and hint/profile keys
are visible through discovery or metrics. Never use secrets as default argument
values or metadata. Keep them in trusted application state with narrow operations.

## Predictive profiles and batching

Profile JSON never loads code or pickle. Numeric features are caller-provided;
keys, summaries, timing and hardware metadata may still identify private workloads.
Review before sharing and never include secrets. Imported measurements are advice,
not authority to use processes or exceed worker limits. Per-task RSS limits are
unsupported. Logical pending-Future limits do not bound cancelled entries retained
in stdlib queues or arbitrary task allocations. A failed batch may already have
side effects; running batches cannot be partially cancelled and are never retried.
