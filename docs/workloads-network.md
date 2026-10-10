# Real local network replicas

These are original implementations of four observed workload patterns. They run
real Requests, TCP, disk I/O and (optionally) requests-cache SQLite operations.
They do not vendor upstream source or execute upstream applications. Source was
inspected on 2026-10-10; URLs refer to moving upstream branches, not pinned code.

## Inspected source and licensing

- **HTTP:** [requests-futures sessions.py](https://github.com/ross/requests-futures/blob/main/requests_futures/sessions.py),
  `FuturesSession.request`: schedules a Requests operation through an executor.
  [Apache-2.0 license](https://github.com/ross/requests-futures/blob/main/LICENSE).
  Our replica submits `run_task` externally, creates a task-local Requests session,
  retrieves bytes and verifies their size and SHA-256. It does not benchmark the
  actual FuturesSession wrapper or share a mutable session between workers.
- **Cached HTTP:** [requests-cache session.py](https://github.com/requests-cache/requests-cache/blob/main/requests_cache/session.py),
  `CacheMixin.send` / `_send_and_cache`: look up, fetch and persist responses.
  [BSD-2-Clause license](https://github.com/requests-cache/requests-cache/blob/main/LICENSE).
  This replica actually imports `CachedSession` with its SQLite backend and checks
  `from_cache` on both responses. Each invocation gets a fresh private database,
  performs one cold miss and one warm hit, then closes and removes the database.
  Results report the two states separately; total task timing includes both phases
  and SQLite setup/teardown. It is not a separate warm-only latency measurement.
- **Multipart downloads:** [gofile-dl run.py](https://github.com/rkwyu/gofile-dl/blob/main/run.py),
  `Downloader._download_range`, `download`, `_merge_temp_files`: byte-range requests,
  streamed part files and ordered reassembly.
  [MIT license](https://github.com/rkwyu/gofile-dl/blob/main/LICENSE).
  Our task downloads four validated ranges, writes parts, merges and verifies the
  complete file. Outer tasks run concurrently; ranges within each task are serial
  to avoid concealing another executor under the executor being compared. This
  changes scheduling granularity from upstream's parallel parts. No account,
  credentials, Gofile API, upstream downloader, resume or retries are involved.
- **Discovery:** [Commandcracker daikin_scan.py gist](https://gist.github.com/Commandcracker/3c11b62565f849815ea0d9a869c58c20),
  `is_port_open` / `scan_network` and `/common/basic_info` response parsing.
  No explicit license was visible in the inspected single-file gist. No code is
  copied or redistributed. The independent replica alternates between our open
  fixture listener and a reserved non-listening loopback socket, with finite TCP
  timeout, then obtains and parses a local device-info response on successes.
  It never discovers interfaces, enumerates a subnet, or probes other devices.

Optional dependencies retain their own licenses. Install `requests` and
`requests-cache` in a virtual environment (or use the project's workload extra).
Missing packages raise an actionable ImportError; optional tests explicitly skip.
No public endpoint is contacted by preparing or executing these workloads.

## Calling contract

```python
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from benchmarks.real_world.network import fixture, prepare, run_task

root = Path("/tmp/my-network-trial")
with fixture(root):
    tasks = prepare("downloads", root, count=8, seed=123)
    with ThreadPoolExecutor(4) as executor:
        results = list(executor.map(run_task, tasks))
```

The same top-level callable and JSON-compatible task dictionaries work with
spawned ProcessPoolExecutor and IntelligentExecutor. The fixture remains in the
parent process; keep its context open until every future completes. Root must be
an owned scratch directory. `prepare` also accepts a measurement subdirectory
beneath that fixture root (for example `root / "cold"`). Use a new root for each trial or remove its generated
inputs afterward. Fixture metadata is removed at shutdown; fixture inputs and
measurement directories remain caller-owned. Up to 10,000 tasks are accepted,
so callers must choose counts appropriate to disk and memory constraints.

Every prepared task has a manifest (size/hash or expected discovery state), and
run_task raises on mismatch. Responses are checked for HTTP errors; ranges require
206 and exact Content-Range, with exact received length. Exceptions propagate via
Future.result with no implicit retry. Results omit timestamps, ephemeral ports and
paths so identical seeds compare deterministically across executors.

Requests use a finite 3-second connect/read timeout, disable ambient proxy/netrc
configuration and disable redirects. Only explicit HTTP IPv4 loopback URLs are
accepted. This is a benchmark fixture guard, not a security boundary for malicious
Python callers. HTTP handler uses standard ThreadingHTTPServer with joined handler
threads; the workload runner controls client concurrency. Server CPU, loopback and
filesystem-cache effects are part of these local observations; they do not model
WAN latency, TLS, real servers, rate limits or network-device behavior.

Tests: `python -m unittest discover -s tests -p test_real_network.py -v`.

## Representative profile and limits

The four-argument `prepare(name, root, count, seed)` contract retains the smoke
profile. Pass `scale="representative"` for larger, heterogeneous tasks; use
24–64 tasks per measurement and choose worker limits appropriate to the machine.
HTTP, cached HTTP and downloads cycle through exact payload sizes of 4 KiB,
16 KiB, 64 KiB, 256 KiB, 1 MiB and 4 MiB. Bytes are seeded and manifests are
verified. Responses are written in varied 1 KiB, 8 KiB and 64 KiB application
chunks (TCP may coalesce these writes). Downloads vary between 2, 4 and 8 serial
ranges and still stream, write, reassemble and hash actual data. Twenty-four
payload tasks occupy about 22 MiB of fixture input; there is no compression,
network-delay simulation, busy loop or sleep in normal tasks.

Cached HTTP continues to measure a fresh SQLite cold/warm pair at every size.
The single task duration combines both phases plus database lifecycle costs;
`cold_misses` and `warm_hits` count verified states, not separate latency samples.
Discovery repeats four bounded TCP probes per task against only the same local
open listener or reserved closed socket, checking and parsing the local HTTP
endpoint after every successful probe. It never broadens the target address set.
Tasks accept at most 32 discovery probes; representative preparation chooses four.

The server backlog accommodates 128 pending connections, each handler has a
finite socket timeout, and fixture teardown closes the listener and joins all
handler threads. All work must complete before leaving the fixture context.
Fault-only endpoints provide HTTP 503, HTTP 404, a forbidden redirect, a truncated
response and an event-gated blocked response. The last is released at fixture
teardown or after a bounded three seconds; tests inject a shorter client timeout.
These endpoints are not included in prepared performance tasks. There are no
implicit retries, including on failure, and failed cache/download invocations
clean up their temporary databases and part files.

Integration tests exercise repeated concurrent transfers, every representative
payload size and chunk size, nested fixture metadata discovery, bounded repeated
probes, deterministic replay, exact server request counts, poisoned ambient proxy
settings, redirects, truncation, timeouts and cleanup. These are correctness and
connection-lifecycle checks, not claims about production throughput or WAN loads.
