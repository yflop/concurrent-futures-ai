# Eleven real local workload replicas

These executable examples use real libraries, real local I/O, and generated
fixtures with correctness checks. They are independently written **workload
replicas**, not adapters that execute or certify the cited upstream applications.
They send no traffic to those projects or third-party servers. The ArcGIS application
itself is **not tested**; its open graph counterpart is tested separately.

| Workload | Actual operation | Source and replication boundary |
| --- | --- | --- |
| HTTP | Requests against loopback HTTP; size/hash check | ross/requests-futures pattern, not FuturesSession wrapper |
| Images | Pillow JPEG decoding, EXIF orientation, resize, JPEG/PNG encoding | Prushka image gist; independent implementation |
| Geospatial | NetworkX routing on a coordinate-bearing graph | Esri large-network-analysis-tools pattern; no ArcPy/ArcGIS |
| Cached HTTP | requests-cache SQLite miss followed by verified cache hit | Actual CachedSession; task includes both cache phases |
| Downloads | HTTP byte ranges, part writes, ordered merge and checksum | gofile-dl pattern; concurrent files, serial parts within each file |
| Pipeline | Streaming transform/filter/aggregation stages | stream_executors pattern; independent bounded local fixture |
| PDF OCR | Raster-only PDF rendered by Poppler, Tesseract text extraction | Arcaneum pattern; no Arcaneum service/model integration |
| Deduplication | Size/prefix/full hash grouping plus byte-level verification | Anthropic interview notes; forced-hash-collision tests |
| SQLite | Real database query/aggregation and integrity checks | ai-trader data access pattern; no trading or remote account |
| Discovery | TCP success/refusal and device-info parsing on loopback | Commandcracker gist; no subnet or device scan |
| Video | FFV1 lossless video decoding and verified frame extraction | Resolved jainxy video gist; FFmpeg replica of OpenCV pattern |

Exact inspected files, provenance, license restrictions, fixture scale, and
intentional differences are in [media](workloads-media.md),
[network](workloads-network.md), and [data](workloads-data.md).
No upstream code is copied; no compatibility or endorsement is implied.

## Reproduce

The harnesses live in the source checkout/source archive, not in the core wheel
import namespace. The `workloads` extra supplies their dependencies; run the
commands below from an extracted source tree.

Use Python 3.12 for the exact checked dependency set. The core package still
supports Python 3.10+ and has no mandatory runtime dependencies.

```sh
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-workloads.lock
python -m pip install -e .
# Debian/Ubuntu; binaries are external optional tools:
sudo apt-get install ffmpeg poppler-utils tesseract-ocr tesseract-ocr-eng fonts-dejavu-core
python -m unittest tests.test_real_media tests.test_real_network tests.test_real_data tests.test_real_runner -v
python -m benchmarks.real_world.run --quick --output benchmarks/results/real-world.json
```

`pip install '.[workloads]'` instead resolves supported ranges; the lock is the
exact Python dependency set used for recorded runs, not a universal multi-platform
lock. OS binaries and their versions must be recorded for each environment.
The benchmark never silently substitutes sleeps or fake results for a missing
package/tool. Missing dependencies and operational failures appear as failures in
its JSON; optional unit tests explicitly skip unavailable libraries.

The default quick grid covers thread/process backends, one/two workers, and
one/four items per batch. Seed source 1701 creates calibration fixtures and order; held-out seeds
2903, 3911 and 4933 are never used to tune. Each measured phase has four tasks in quick
mode, so results are a bounded smoke experiment, not capacity planning evidence.
Use larger counts and independent machines before deployment conclusions.

## Representative profiles and repeated held-out runs

`--scale representative` enables heterogeneous inputs rather than simply repeating
one small fixture. The current bounded profiles include:

- Images: six size/format/orientation profiles, 640×480 through 2560×1440
- OCR: four raster-only PDF profiles, 1–3 pages through 1654×2339
- Video: four lossless codec/size profiles, 12–48 frames through 640×360
- Deduplication: 128–512 files per tree, at most 8,192 files per prepared dataset
- Graph: a 1,024-node original coordinate grid with varying origin/destination sets
- Pipeline: varying pages and token counts, at most 8,192 pages per dataset
- SQLite: 98,304 synthetic market rows with variable filters/ranges/order
- Network: 4 KiB–4 MiB payloads, varied chunks/ranges, loopback discovery probes

For example, the priority held-out run uses 24 heterogeneous tasks per phase,
two independent calibration repeats with six tasks each, and three held-out seeds:

```sh
python -m benchmarks.real_world.run --scale representative \
  --workloads images ocr dedup --count 24 --calibration-count 6 \
  --calibration-repeats 2 --heldout-seeds 2903 3911 4933 \
  --workers 1 2 --batches 1 4 --backends thread process \
  --output benchmarks/results/representative-priority.json
```

Preparation and independent sequential validation occur once per seed, outside
executor timing. Immutable source tasks are reused across engines/candidates;
task-local caches and outputs are recreated on every invocation. This avoids
charging repeated fixture generation as executor work and preserves equal input.
Calibration setup, reference time, actual tuning time and held-out timing remain
separate. A larger held-out task count is a deliberate generalization test, not a
claim that the selected configuration is the held-out oracle.

Zero task errors and correct results are mandatory. The CLI also fails closed on
missing/invalid resource metrics and configurable p95, sampled RSS and lifecycle
CPU budgets (defaults: 30 s, 1 GiB, 300 CPU s per trial). These are test observation
budgets, not live enforcement or universal application SLOs. A nonzero CLI exit
preserves raw failed measurements. Resource budgets are final acceptance gates;
the public tuning API additionally supports resource-based calibration exclusions.

Use `--profile-intervals 0 8 --predictive-only` to compare predictive admission
against an equal backend/worker/batch stdlib grid. Without `--predictive-only`,
requesting both intervals searches more intelligent candidates; that is a separate
best-achieved search with explicitly higher tuning cost, not an equal-budget win.
Default fixed-admission results measure calibrated executor overhead/performance;
they are not evidence of an online learned-policy improvement.

## Measurement contract

- The same callable, algorithm, fixture data, batching, and bounded submission
  window run under stdlib and IntelligentExecutor. Fixture/reference validation
  occurs outside executor timing and is separately reported.
- Both engines search the same backend/worker/batch grid by default. The comparison is
  `T_calibration-selected_stdlib / T_calibration-selected_intelligent` on held-out
  data, **not** a held-out oracle. Both are tuned; no intentionally weak baseline.
- Fixed admission is the default measured mode. `--profile-intervals 0 8` also
  tests sampled predictive admission; this expands the intelligent search and its
  reported calibration cost. Backend permission remains explicit.
- Cold lifecycle timing includes executor construction, lazy worker startup, and
  teardown (measured after the warm pass and charged once). Warm timing
  is a second pass through the same pool with reused immutable input fixtures and
  fresh task-local outputs/caches. The sequential oracle already read those inputs.
  OS/library/page-cache state is uncontrolled; “cold” does not mean cold disk.
- Reports include task service p95 and batch submission-to-completion p95 (assigned
  to every contained item), throughput, error rate, checksums, and completion traces.
- Memory is absolute process-tree RSS sampled every 10 ms, including fixture
  server/instrumentation. Shared pages may count twice and short peaks can be missed.
  This is not per-task memory or a hard memory bound.
- <=10% coverage means intelligent time <=1.1 times the selected stdlib time.
  Calibration reports include retrospective steps to within 10% of the best
  observed calibration candidate, not an online-policy convergence proof.
  Small samples do not establish policy convergence, statistical significance,
  universal speedup, or production readiness. Raw errors and losses remain visible.
- One machine was tested. Hardware profiles or simulated constraints are not
  additional measured physical machines.

## Dependency and license policy

Optional dependency ranges are bounded by major versions; the checked environment
uses exact pins. Update pins intentionally, run all workload and package checks,
review advisories/licenses, and regenerate evidence. Pins are not an assertion
that a version has no security issues. This change has not performed a full
supply-chain audit. The lock does not vendor any dependency code.

Observed installed metadata: Pillow MIT-CMU; Requests Apache-2.0;
requests-cache BSD-2-Clause; NetworkX/psutil BSD-3-Clause; attrs/cattrs/platformdirs/
url-normalize/urllib3/charset-normalizer MIT; idna BSD-3-Clause; certifi MPL-2.0.
External FFmpeg/Poppler/Tesseract builds retain their own licenses and build-dependent
conditions. Distributing binaries requires a separate review; none are bundled.

## Production acceptance gates

Before calling a particular deployment production-ready, require:

1. Correctness and error/cancellation/backpressure tests pass for its real input
   distribution, dependencies, operating systems, and exact shipped wheel.
2. Representative calibration and independent held-out data, enough repeated runs
   to quantify variability, and fixed resource/algorithm budgets.
3. Declared throughput/p95/error/memory budgets pass, including cold and sustained
   runs, downstream rate limits, and failure injection. No mandatory speedup claim.
4. Calibration/configuration cost amortizes within the deployment's lifetime.
5. Operational ownership, observability, shutdown policy, dependency updates,
   compatibility/migration plan, and workload-specific security review are in place.

This repository supplies executable harnesses and evidence toward those gates;
it does not assert that eleven smoke fixtures satisfy them for arbitrary production
inputs or that the planned concept catalog is implemented.

### Explicit predictive input summaries

Predictive runs default to the [declared feature adapter](WORKLOAD-FEATURES.md):
allowlisted fixture metadata such as pixels, pages, frames and query lengths is
bucketed explicitly. No file contents, paths, hashes or source code are inspected
for features. `--feature-mode batch-only` preserves the older batch-size-only
comparison. Pipeline/dedup currently lack declared cost counts and conservatively
fall back to batch size. Separating cost buckets is not itself evidence of a
performance improvement; compare the recorded modes on independent inputs.

### Source-matched frozen-selection regression

After source changes, rerun previously selected settings without recalibration on
fresh fixtures, writing a new report rather than overwriting historical evidence:

The shipped representative inputs are archived; the measured recheck harness
still reads plain JSON. Decompress to new local inputs first using the bounded
[archive helper](../benchmarks/results/ARCHIVES.md):

```sh
python -m tools.report_io benchmarks/results/representative-priority.json.gz --output /tmp/if-evidence/representative-priority.json
python -m tools.report_io benchmarks/results/representative-remaining.json.gz --output /tmp/if-evidence/representative-remaining.json
python -m benchmarks.real_world.recheck \
  /tmp/if-evidence/representative-priority.json \
  /tmp/if-evidence/representative-remaining.json \
  --count 12 --seed 12011 --output benchmarks/results/local-frozen-selection-recheck.json
```

A completed report checks only its recorded source and frozen choices. It is not
a new exhaustive grid comparison or proof that later source changes pass. The
historical seed-12011 check led to a bounded raster-resolution repair; its failed
and corrected records remain in the [historical evidence report](PRODUCTION-EVIDENCE.md).
Later integrated/revised results and their distinct failure denominators are
tracked in [current configuration quality](CONFIGURATION-QUALITY.md).
