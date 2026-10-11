# Security and trust boundaries

Version 0.5 is a **Beta trusted-code concurrency library**. Beta is not a security
or production certification. The release workflow requires source verification,
built-artifact checks and exact-commit CI. This is not a sandbox: do not submit untrusted Python
code or unpickle attacker-controlled objects. Processes isolate
address spaces, not filesystem, credentials, network, or user permissions.

The core makes no network/LLM calls and provides no automatic telemetry upload,
credential handling, task retries, or durable task/restart storage. Explicit
strict JSON import/export is available for predictive profiles and measured
configurations; file storage and access control remain caller-owned. JSON loads
data, not Python source or pickle, and never starts calibration or grants process
consent. Schema validation is not authentication: use trusted files and verify the
workload/runtime/hardware and strategy compatibility required by the API.
Snapshot/profile keys, features and hardware metadata can identify workloads;
keep PII and secrets out. Application callables can still transmit or log data.

Bounded task count does not bound bytes, CPU, memory, file descriptors, network
requests, or process lifetime. A hung running task can block shutdown indefinitely.
Use external operational isolation for hostile or resource-heavy work.

Use `with` or `shutdown(wait=True)` to guarantee accepted work drains before
interpreter exit. Omitted shutdown can settle deferred submissions with
`RuntimeError` during backend teardown. Standard Future API compatibility does
not imply identical stdlib callback/thread or implicit-exit scheduling semantics.

Normal completion callbacks run serially on an owned completion pump. **Never
wait for another Future from the same executor in a callback, even with free
worker slots.** The result can need that blocked pump for publication. Slow
callbacks can delay unrelated work; there is no callback isolation or general
dependency-deadlock prevention. A callback added after completion or invoked by
external cancellation may run on the caller. Waiting shutdown from the executor's
task/pump context raises `RuntimeError`; use an external lifecycle coordinator.

Reentrant submissions from this executor's task/pump context fail immediately
with `BackpressureError` if admission would block. The `reentrant_rejected`
counter exposes this reason without retaining task identities. This guard does
not detect or prevent waits on another Future after successful submission,
general dependency cycles, or callbacks blocking on external resources.

Custom policies execute trusted code under a lock. A blocking policy can stall
submission; a malicious policy can perform arbitrary actions. Process routing
validation protects only accidental selection without explicit caller consent.

## Reporting

Do not disclose credentials or exploit data in public issues. Before publication,
contact the repository owner through their existing private channel. After
publication, use GitHub private vulnerability reporting if the owner enables it.
No reporting endpoint or response SLA is assumed to exist yet.

### Cancellation churn and memory

`max_pending` separately caps logical unfinished work and physical retained
scheduling items (owned queued plus dispatched), with each backend's dispatches
also capped by its worker budget. Cancelling owned queued work removes its payload
without adding a backend tombstone. Successful dispatched thread cancellation
releases logical capacity but keeps the physical slot until acknowledgement.

These item-count bounds do not constrain payload/result bytes, blocked-producer
memory, caller-retained Futures, native allocations, transport bytes, or RSS.
Earlier churn reports describe the historical direct-submission implementation;
they are not the current queue contract or a hard memory guarantee.

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
unsupported. Logical and physical scheduling item bounds do not bound arbitrary
task allocations, transport bytes or retained results. Process Futures become
non-cancellable at dispatch commitment, before child execution necessarily begins.
A failed batch may already have side effects; running batches cannot be partially
cancelled and are never retried.


## Optional real-workload harnesses

The eleven workload replicas run only generated local fixtures; HTTP and discovery
accept explicit 127.0.0.1 endpoints and disable proxies/redirects. They are not
network scanners, file-upload clients, or production ingestion services. Their
fixtures and task dictionaries are trusted inputs. Arbitrary hostile media,
mutable filesystem trees and attacker-controlled databases require separate
validation and isolation. Local-only endpoints do not make untrusted services safe.

Media commands have finite subprocess waits and bounded fixtures, and receive
argument lists rather than shell command strings. OCR/video invoke installed native
tools with their own parser risks. The harness does not sandbox them or terminate
an arbitrary descendant process tree. No binaries or upstream source are vendored.

Offline tuning invokes only the explicitly supplied calibration callback. That
callback may run once per candidate/repeat, so callers must use repeatable fixtures
without consequential side effects. Process candidates require explicit consent.
Observed resource-cap filters are not live resource enforcement. Keep calibration
artifacts, hardware fingerprints, and task identifiers private when appropriate.


## Evidence-gated fixture evaluation (0.5)

The evaluator runs only an explicitly supplied `RepeatableWorkload` after
`repeatable=True` consent. That declaration is not a sandbox or proof of
idempotence. A caller-controlled fixture factory, callable and validator remain
trusted code with the process's permissions; repeats can produce repeated side
effects. Use isolated test state and fresh context-managed fixtures. Ordinary
submissions are never discovered, intercepted or replayed. Exceptions never
trigger automatic retries on another backend.

Correctness/resource/uncertainty gates are conservative decisions about supplied
observations, not guarantees about adversarial callbacks or future work. Required
missing telemetry fails closed. `max_evaluations` bounds measurement calls; the
built-in adapter bounds submitted batches and fixture item count. These are not
hard byte, RSS, CPU or execution-time limits. Use an external process supervisor
if task termination is required.

Evidence exports contain scalar measurements, non-sensitive caller-supplied
workload identity/version/scope, configuration, runtime and hardware metadata.
They omit callable inputs, outputs and exception messages. Review identifiers
before sharing reports. There is no executable report importer. In-memory
recommendations check workload identity, runtime/revisions, hardware and expiry,
and require renewed process consent; matching identity cannot establish
representative workloads or unchanged load.

Optional routing traces retain bounded accepted-decision records and fixed
reason categories only. They omit keys, features, callable/input/result objects,
exception content and custom-policy text. Other existing profile APIs may include
caller-supplied workload keys and feature summaries; those retain their separate
data-handling contract.
