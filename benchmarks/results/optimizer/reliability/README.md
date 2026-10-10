# Lifecycle and allocation evidence

These JSON files are public metadata projections. Absolute interpreter, checkout,
and temporary paths and report-only machine fingerprints are redacted; numerical
measurements, outcomes and source hashes are unchanged. Saved-profile copies with
redacted fingerprints are evidence records, not loadable profiles.

- `endurance-20min.json`: the first integrated 22-epoch run. It completed all real
  workload epochs but **failed** its predeclared 128 MiB final parent-RSS-growth
  gate. This is retained failed evidence, not a pass for the corrected source.
- `memory-{intelligent,stdlib,direct}.json`: shorter fresh-process diagnostic runs.
  They add tracing and per-epoch garbage collection and omit the fault probes;
  they are **not** reproductions or replacements for the endurance gate.
- `memory-diagnosis-summary.json`: measured allocation comparison and limitations.
  The intelligent run precedes the completed-Future ownership-cycle correction.

A diagnostic allocator trim shows that some pages are reclaimable. It does not
identify the responsible library or establish leak freedom. Direct execution has
lower concurrency; the standard-library run is the matched executor reference.
The original gate is neither relaxed nor re-scored after inspecting these data.

- `endurance-after-retention-fix.json`: the corrected executor completed all 22
  epochs and 191,340 tasks with all original gates passing. Final parent growth
  was 98,414,592 bytes under the unchanged 134,217,728-byte allowance; observed
  process-tree peak was 577,441,792 bytes under 1 GiB. This is one bounded local
  endurance observation, not uptime certification or proof of a memory-leak-free
  implementation. The source hashes identify the image adapter used in this run.

- `memory-arena2-*.json`: separate short fresh-process diagnostics with the explicit
  process environment `MALLOC_ARENA_MAX=2`. Library globals are unchanged. These
  traced, per-epoch-GC observations do not replace the ordinary 20-minute test or
  establish a portable deployment memory guarantee.
- `image-memory-{before,after}.json`: the same 12 image inputs, all six format/EXIF
  profiles, three two-thread pool lifecycles (36 tasks), exact unchanged result
  digest. The conservative adapter change preserves EXIF transpose, RGB conversion,
  resize and encoding, while closing prior full-size decoded cores sooner. Peak
  sampled RSS was 152,440,832 versus 139,640,832 bytes; final parent RSS was
  66,977,792 versus 80,269,312 bytes. A lower observed peak is not a claim of lower
  retained RSS or general performance improvement. `media_before_cleanup.py`
  preserves the old adapter and `tools/compare_image_memory.py` reproduces the
  before/after comparison in separate processes.

## Bounded video decoding diagnostic

- `video-{before,after}-{1,2}.json` and `video-streaming-summary.json` preserve two
  observed fresh-process pairs. Each cell uses the same eight prepared video
  tasks covering all four representative profiles, three two-worker standard-
  library thread-pool lifecycles, and exact old-adapter output oracles. All 96
  measured task outputs match, including the full decoded-stream digest and
  every selected PNG digest.
- Sampled process-tree peaks were **185.95 → 121.84 MiB** in pair 1 and
  **205.53 → 128.52 MiB** in pair 2. Each process finished with one thread, no
  descendants, and four file descriptors. These two small diagnostic pairs do
  not establish that the original primary 1 GiB selection gate now passes, nor
  replace its failed evidence. RSS sampling can miss spikes and double-count
  shared mappings.
- The video adapter now hashes decoded frames incrementally, retains one RGB
  frame plus bounded pipe chunks, drains bounded stderr concurrently, and closes
  extracted image cores. Its 60-second deadline is not a hard total-task wall
  limit: synchronous PNG callbacks and process/thread cleanup can extend it.
  Cleanup assumes controlled FFmpeg is the only pipe writer; this helper is not
  a generic runner for arbitrary descendant-spawning commands.
- `media_before_streaming.py` is the exact old-source snapshot used by the
  measurements, SHA-256
  `cb71269b9a2f6053bd8b36b1ff39bf296ab505acd14ca871ff05f383a75b1109`.
  The diagnostic reports retain their actual source hashes. The final source
  differs from the measured post-change source only by a video-helper docstring
  clarification; the combined source also includes a separately owned OCR
  fixture change, which this video diagnostic does not execute.

`tools/compare_video_memory.py` requires an explicit local `--workspace`; it has
no machine-specific paths. From the repository root with workload dependencies
installed, prepare once, then run each measurement in a separate Python process:

```sh
PYTHONPATH=src:. python tools/compare_video_memory.py --mode prepare --workspace ./video-memory-work
PYTHONPATH=src:. python tools/compare_video_memory.py --mode before --workspace ./video-memory-work --output ./video-memory-work/before-1.json
PYTHONPATH=src:. python tools/compare_video_memory.py --mode after --workspace ./video-memory-work --output ./video-memory-work/after-1.json
```

Repeat the last two commands with `before-2.json` and `after-2.json` for the second
pair. `--before-source` defaults to the preserved source snapshot above and can
be explicitly overridden. The workspace fixture manifest contains local paths;
do not publish it. Use `tools/project_optimizer_evidence.py` for public report
copies. The five committed JSON files are metadata-only projections: their
measurements, outcomes, and source hashes have not been changed.
