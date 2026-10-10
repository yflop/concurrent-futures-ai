# Data workload replicas

These are original implementations of four workload patterns, not adapters or
benchmarks of the upstream applications. No upstream source, prose, fixtures, or
market data is vendored. Preparation creates bounded local files before timing;
workers do actual file reads, hashing, parsing, compression, graph algorithms,
and database queries. There are no sleeps, API credentials, external requests,
financial actions, or writes to user data.

## Provenance inspected

Reviewed 2026-10-10:

- [Esri parallel_odcm.py](https://github.com/Esri/large-network-analysis-tools/blob/b72ddd9f4f60aa5d6f058d53b1d47865f50c8524/parallel_odcm.py)
  and its README: Apache-2.0. The source splits origin/destination calculations
  into chunks and uses ArcPy network analysis. Our replica runs NetworkX Dijkstra
  on a small geographic graph. ArcGIS Pro, Network Analyst, routing credits,
  ArcPy, geodatabases, and proprietary solver behavior remain untested.
- [stream_executors wordcount.py](https://github.com/pkch/stream_executors/blob/326677ab98de374314bfa76e75624a705c34bdda/examples/wordcount.py)
  and README: MIT. The example connects download, count, and upload stages through
  bounded executor maps. Our local pipeline replaces network sources/sinks with
  JSONL and compressed serialized output. Its word matching is explicitly
  case-insensitive whole tokens rather than the upstream substring rule.
- [Anthropic-Interview-Notes file deduplication](https://github.com/Schuture/Anthropic-Interview-Notes/blob/21d22578156c4cfc485466ca919de09430d5c0d6/coding/file-deduplication/README.md):
  code MIT; prose CC BY-NC 4.0, as specified in the repository LICENSE and
  LICENSE-CODE. Our implementation and tests are newly written; no prose or code
  is copied. It implements the recursive byte-identity problem with size/prefix
  filtering, streaming full hashes, and additional exact comparison.
- [ai-trader sqlite_storage.py](https://github.com/whchien/ai-trader/blob/7681fe331fb638beab1ae20d3552c802bbaa9174/ai_trader/data/storage/sqlite_storage.py)
  and [storage guide](https://github.com/whchien/ai-trader/blob/7681fe331fb638beab1ae20d3552c802bbaa9174/SQLITE_STORAGE_GUIDE.md):
  GPL-3.0. Upstream uses SQLModel sessions and OHLCV date-range loads. Our original
  raw-sqlite3 workload preserves the market-cache access pattern without copying
  GPL code or importing ai-trader. SQLModel/pandas overhead and upstream's full
  schema, metadata, updates, and strategy system are not benchmarked.

Links pin the repository HEAD commits resolved when these sources were inspected;
rerunning a replica needs no upstream download. NetworkX is an optional BSD-licensed runtime
library. All fixture content and graph topology are generated or authored here.

## Exact measured units

`prepare(name, root, count, seed, scale="smoke")` returns `count` JSON/pickle-safe dictionaries.
The default retains the original smoke fixtures. `scale="representative"` enables
the larger bounded profiles below. Representative preparation accepts at most 64
tasks; larger requests fail before creating files rather than quietly exploding
disk usage. Use a fresh fixture root for each preparation.
`run_task(task)` is a module-level spawn-safe function. The common benchmark
runner invokes that identical function and chunk boundaries for both executor
families. Returned correctness results contain no absolute paths or durations.
Seeded preparation at separate cold/warm/reference roots produces equal results.

### dedup

Each task scans a separate small tree: six duplicate pairs, six same-size,
same-prefix but unequal-tail files, two empty duplicates, and one unique-size
file. The tree is about 1.3 MB. File size rejects singletons without opening them;
4 KiB prefixes reject incompatible candidates before full SHA-256 hashing in
64 KiB chunks. Every hash bucket is partitioned again by actual byte comparison,
so even forced full-digest collisions cannot merge unequal content. Results are
sorted relative-path groups and a duplicate-file count. Symlinks and nonregular
entries are ignored and I/O errors skip a file. No files are deleted.

This fixture parallelizes independent trees, not the hash stages inside one giant
tree. It measures repeated bounded dedup jobs; it does not claim to reproduce
upstream's whole-tree concurrent hashing scheduler. Immutable fixture trees are
required: this is not a transactional filesystem snapshot, and arbitrary changes
or permissions changes during a scan can invalidate the result.

### geospatial

A NetworkX graph has 16 named US cities with rounded geographic coordinates.
Edges use great-circle kilometres computed with the haversine formula. A
connected backbone and three nearest neighbours produce an original illustrative
transport network, **not actual road geometry, commercial routing, or live flight
routes**. Each task computes 8 × 16 origin/destination routes, validates each
path's edge sum, and returns route count, aggregate kilometres and output digest.

The shortest-path work is real, but the tiny graph is a correctness/calibration
fixture, not evidence about large ArcGIS workloads. Tests independently compare
Dijkstra totals against Floyd–Warshall and check a known equatorial distance.
Missing NetworkX raises an actionable optional-dependency error before timing.

### pipeline

Each task consumes 32 JSONL pages of 256 seeded tokens. Lazy generator stages
parse, count case-insensitive whole-word `python`, serialize, compress, and verify
decompression. Only one page is consumed at a time; output is reduced to counts,
compressed length, and a SHA-256 digest. No actual upload takes place.

Concurrency is across independent page shards. Stages within a shard are lazy
and sequential, so this does not test independently scheduled interstage
backpressure or background production in `stream_executors` itself. Submission
and batch bounds are exercised by the shared benchmark runner.

### sqlite

Preparation transactionally creates 8,192 fictional daily OHLCV rows across four
`DEMO_*` symbols, with integer prices and a compound primary key. It commits and
checkpoints the WAL before workers start. Each task opens its own read-only
connection, begins a read snapshot, retrieves 1,024 ordered rows and runs an
indexed aggregate query. SQL totals are checked against a separate Python
reduction of those rows. Connections are explicitly closed and are never shared
across threads/processes. The benchmark measures concurrent reads only; it makes
no concurrent-write or real financial performance claim.

## Representative profile

This scale is a bounded, heterogeneous local workload, not a reproduction of a
production deployment. The same seed and task count reproduce the same files and
results. Preparation and independent test oracles remain outside measurement.

- **Dedup:** 128–512 files per independent tree, with an 8,192-file aggregate cap.
  The per-tree maximum is `min(512, 8192 // count)`; task index varies the target
  between half and all that maximum, with a 128-file minimum. Six repeating
  nonempty sizes range from 32 bytes to 65,573 bytes. Nested duplicate pairs,
  unequal same-size tails (including shared 4 KiB prefixes for larger files),
  empty duplicates and unique sizes exercise every filtering stage. At 24 tasks,
  there are thousands of files across the trees. The logical content size is
  bounded below 170 MiB in aggregate; filesystem allocation/metadata can differ.
- **Geospatial:** one shared original 32×32 perturbed coordinate grid has 1,024
  nodes and horizontal, vertical and selected diagonal edges. Every weight is a
  haversine distance. Tasks vary between 4/8/16 origins and 32/64/128 sampled
  destinations (128–2,048 routes). Each task reads/builds its own NetworkX graph
  and runs real single-source Dijkstra; it does not use road data or claim road
  routing fidelity. Tests use an independent heap-based shortest-path oracle,
  avoiding cubic Floyd–Warshall on the larger graph.
- **Pipeline:** shards vary between 128/256/512 JSONL pages (further limited by
  `8192 // count` pages/shard), with 64/128/256/512 seeded words per page.
  The aggregate never exceeds 8,192 pages. Each page is decoded, token-counted,
  serialized, compressed and roundtrip-checked before the next is consumed.
  Independent streaming tests verify counts, serialized digest and compressed
  bytes. Concurrency remains across shards, not between pipeline stages.
- **SQLite:** one database per preparation contains 98,304 integer OHLCV rows
  (12 fictional symbols × 8,192 days), inserted in one transaction. Read-only
  task snapshots vary symbol, start, 256/1,024/4,096-day span, ascending/descending
  result order and volume threshold. Actual SQL range/filter queries and SQL
  aggregates are measured. Independent tests fetch each relevant symbol once,
  then filter/sort/reduce in Python and verify the exact ordered-row digest and
  unchanged database bytes. This is concurrent reads, not writes or trading.

Representative tests also compare serial, thread and explicit spawn-process
results, repeat seeded preparation in independent roots, and verify JSON/pickle
round trips. These tests are correctness checks rather than performance claims.

## Run and test

From the repository root with Python 3.10+:

```sh
python -m venv --system-site-packages .venv-workloads
.venv-workloads/bin/python -m pip install 'networkx>=3.2'
PYTHONPATH=src .venv-workloads/bin/python -m unittest discover -s tests -p test_real_data.py -v
PYTHONPATH=src .venv-workloads/bin/python -m benchmarks.real_world.run \
  --quick --workloads geospatial pipeline dedup sqlite \
  --output benchmarks/results/real-world-data.json
```

Focused validation includes smoke and representative tests on Python 3.12 with NetworkX 3.7. They
cover forced hash collisions, distinct-size and prefix pruning, unreadable and
symlink entries, an independent content-group oracle, independent word counts,
SQLite integrity and unchanged database bytes, graph correctness, deterministic
cross-root results, JSON/pickle round trips and spawn-process execution. The
graph-specific test skips explicitly if NetworkX is not installed.

These tests establish fixture correctness. They do not establish a deployment
speedup, cold filesystem caches, disk durability under crashes, multi-host
transfer learning, or compatibility with any upstream application.
