# Declared workload feature schema v1

`benchmarks.real_world.features.batch_features(name, tasks)` is an explicit caller-side adapter for the local fixture runner. It is not automatic library input inspection. It returns a new dictionary of finite integers compatible with `submit_profiled(features=...)`; the workload's stable profile key still separates different workloads. Do not share a profile key across unrelated workloads or adapter schema versions.

## Metadata allowlist

| Workload | Explicit task fields read | Numeric dimensions |
| --- | --- | --- |
| images | `source_size` (two dimensions) | source pixels |
| ocr | `page_count`, `expected_render_size` | pages, total rendered pixels |
| video | `width`, `height`, `frames` | frames, total frame pixels |
| http, cached_http | `size`, `response_chunk` | known payload bytes, response chunk size |
| downloads | `size`, `response_chunk`, `parts` | same, plus range part count |
| discovery | `probes` | declared probe count |
| geospatial | lengths of the `origins` and `destinations` lists/tuples | origins, destinations, origin–destination pairs |
| sqlite | `length` | declared query range length |
| pipeline, dedup, unknown names | none | batch size only |

The graph labels themselves are never traversed or serialized. Graph size is not present in current task dictionaries and is not inferred from a path, scale, task index or a file. Pipeline page/token counts and dedup file/byte counts are likewise absent from current prepared dictionaries. Adding those dimensions requires a separately reviewed metadata declaration in preparation and this adapter. Discovery smoke tasks omit `probes`, so they fall back to batch size; no execution defaults are assumed.

Paths, URLs, ports, identities, symbols, offsets, text, hashes, payload bytes, expected text, expected frame hashes and raw results never enter features. No file is opened or statted and no payload is decoded. Integer-like objects, custom mappings or conversion hooks are not supported.

## Batching, buckets and validation

`batch_size` is exact. Every available dimension yields `<dimension>_sum_log2` and `<dimension>_max_log2`, each the ceiling of the base-two logarithm of the positive sum or maximum. Thus 1 maps to 0, 2 maps to 1, and 3–4 map to 2. Pixel totals multiply per-task dimensions first, then sum across tasks; pages and frames are included before aggregation. Sum and maximum retain coarse aggregate and largest-task information in heterogeneous batches. Chunk size is a transport-shape proxy, not an additive estimate of total work.

Feature names and boundaries are deterministic across runs and batch order. Nearby costs intentionally share buckets. These summaries do not capture the full cost distribution and can alias different batches. Finer dimensions may fragment matching observations and reduce predictive coverage; they are not presumed to improve admission.

A dimension is emitted only when valid for **every** task in the batch. Missing, nonnumeric, boolean, floating-point, nonpositive or over-cap values drop that whole dimension; unrelated valid dimensions remain. No partial sum represents a whole batch. Unknown metadata is ignored. Empty batches return only `batch_size: 0`. A non-string workload name, non-list/tuple batch, non-plain-dictionary task, or more than 10,000 tasks raises `ValueError`.

Positive integers are capped before arithmetic: general values/products at 2^30; image sides at 16,384; OCR sides at 2,400 and pages at 3; video at 640×360 and 48 frames; payloads at 16 MiB; chunks at 65,536; parts at 64; probes at 32; origin/destination lengths at 10,000; query length at 1,048,576. Aggregate sums are bounded by the per-task cap times the batch cap. No value can become NaN or infinity. These validation bounds are adapter safety limits, not promises that workload execution accepts every bounded input.

## Confidence and comparison

These are declared cost proxies, not measured execution costs or confidence scores. Batch-only fallback has no evidence of within-batch cost comparability. Declared dimensions improve description but still do not establish comparable codec, compression, cache, graph, text, disk or network behavior. No CPU/I/O/GIL classification is inferred. Matching local timing observations and the predictive library's existing evidence thresholds remain necessary.

To compare batch-only and declared profiling fairly, use the same prepared workload distribution, seed split, stdlib tuning budget and executor options; keep modes and schema distinct in reported results. Report held-out speed, failures and predictive coverage as observed, including regressions and insufficient matching observations. Do not call bucket separation a demonstrated performance benefit without that experiment.

Run lightweight metadata tests with `PYTHONPATH=src python -m unittest tests.test_real_features`.
