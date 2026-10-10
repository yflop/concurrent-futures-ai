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
