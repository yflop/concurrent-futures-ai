# Real media workload replicas

These are original, offline, bounded replicas of useful operation shapes. They
execute actual libraries and binaries; they do not run upstream applications or
substitute hashing, sleeps, simulated OCR, or mocked decoding for media work.
Checksums are correctness checks after real processing. All fixtures are generated
locally. No source media, model weights, or external services are downloaded.

## Provenance and differences

Sources inspected on 2026-10-10:

- Images: [Prushka `transform.py`](https://gist.github.com/Prushka/8b415ea5194d12034c2932a57459faba),
  revision `ee6ce5ed140f946790d8bb5e2b05180f0ece1139`. The gist performs
  EXIF-aware resize and multi-format encoding through a process pool. The replica
  uses Pillow JPEG and PNG, including an actual EXIF orientation fixture, Lanczos
  downsampling, progressive optimized JPEG and optimized PNG. AVIF/WebP,
  incremental directory crawling, and original-file copying are omitted.
  No license was visible in the gist; none of its code is copied.
- OCR: [Arcaneum OCR worker](https://github.com/cwensel/arcaneum/blob/dcba306e9262a12d773a7380622732d26bd76380/src/arcaneum/indexing/pdf/ocr.py)
  and [PDF benchmark](https://github.com/cwensel/arcaneum/blob/dcba306e9262a12d773a7380622732d26bd76380/scripts/benchmark_pdf_indexing.py),
  commit `dcba306e9262a12d773a7380622732d26bd76380`.
  [MIT license](https://github.com/cwensel/arcaneum/blob/dcba306e9262a12d773a7380622732d26bd76380/LICENSE).
  Original replica covers rasterization and Tesseract recognition, using direct
  Poppler/Tesseract subprocesses rather than the upstream Python wrappers and
  OpenCV preprocessing. It does not benchmark embeddings, indexing services,
  confidence metadata, or page-selection logic. The upstream indexing benchmark
  disables OCR; this replica deliberately measures the separate real OCR path.
- Video: the supplied jainxy profile resolves to
  [`video_to_frames.py`](https://gist.github.com/jainxy/9482e0347331757ee7ccb6ec371ad2a4),
  revision `76b0fb691a5b6139466069ce1d1be86dff447c86`, a fork of
  HaydenFaulkner's frame-extraction gist. Its workload splits frame ranges across
  processes and saves selected frames with OpenCV. The original replica uses
  FFmpeg lossless decoding and Pillow PNG output per clip, rather than OpenCV
  seeking/chunking or JPEG output. It includes the final frame in decode
  validation. No license was visible; none of its code is copied. This resolves
  a concrete relevant source, rather than claiming the profile itself is code.

External tools retain their own licenses. This repository does not vendor their
source or binaries. Installed Pillow 12.3.0 declares MIT-CMU in its package metadata; Poppler is GPL; Tesseract is Apache-2.0;
FFmpeg licensing varies with the installed build. Inspect the installed build
before redistributing it. These facts do not imply any dependency is required by
the core executor package.

## Setup and exact commands

Python 3.10+ is required. Optional Python installation in an isolated environment:

```sh
python -m venv .venv-media
.venv-media/bin/python -m pip install Pillow
```

OCR additionally needs Poppler's `pdftoppm` and `pdfinfo`, Tesseract, English `eng` traineddata,
and DejaVu Sans. Video needs an FFmpeg build with FFV1 and PNG encode/decode support.
For Debian/Ubuntu these are official distribution packages:

```sh
sudo apt-get install poppler-utils tesseract-ocr tesseract-ocr-eng fonts-dejavu-core ffmpeg
```

No installation is performed automatically. Missing tools, Pillow, the font, or
English traineddata raise `MissingDependency`; test cases skip with the exact
reason. A broken binary, failed codec invocation, timeout, corrupt fixture, or
wrong recognition result is a failure, not a dependency skip.

From the repository root, run the actual serial/thread/spawn-process validation:

```sh
python -m unittest discover -s tests -p test_real_media.py -v
```

Use `.venv-media/bin/python` instead of `python` when using the isolated venv.
Run individual workload tasks directly:

```sh
python - <<'PY'
from pathlib import Path
from tempfile import TemporaryDirectory
from benchmarks.real_world.media import prepare, run_task
with TemporaryDirectory() as directory:
    for name in ('images', 'ocr', 'video'):
        tasks = prepare(name, Path(directory), count=2, seed=17)
        for task in tasks:
            print(run_task(task))
PY
```

The shared runner can submit the same module-level `run_task` with the same task
records to any executor. Both tasks and results are JSON/pickle compatible. Use a
`__main__` guard when constructing spawn process pools. The timed region should
exclude `prepare`; it includes input integrity checks, processing, output
validation, and ephemeral output cleanup. There are no nested executor pools.
The runner must catch `MissingDependency` explicitly rather than suppressing all
exceptions. Retain generated fixtures until every submitted future finishes.

## Work and correctness contract

Default `scale="smoke"` retains the small compatibility profiles:

- `images`: 1024×768 seeded RGB JPEG with EXIF rotation; decode, orient, resize to
  384×512, encode two formats, reopen both, and validate PNG pixels losslessly.
  Returns encoded byte counts/checksums and transformed pixel checksum.
- `ocr`: image-only single-page PDF with four known text lines. Poppler renders
  the page and Tesseract recognizes the image. Whitespace/case-normalized OCR must
  equal the independent fixture text exactly. Embedded PDF text is never read.
  Returns actual recognized text, word count, and text checksum.
- `video`: 24 seeded 320×180 RGB frames encoded as a real lossless FFV1 Matroska
  clip during setup. FFmpeg decodes all 24 frames. Each decoded pixel checksum
  must match the independently generated raw-frame oracle. Every third frame is
  written as PNG, reopened and pixel-checked. Returns full decoded checksum and
  eight extracted PNG checksums. No content is represented by a fake codec.

### Representative bounded scale

Pass `prepare(name, root, count, seed, scale="representative")` for heterogeneous
costs. Profile choice cycles by `(index + seed) % profile_count`, and media/text
content is seeded independently for each task. Counts of at least six images or
four OCR/video tasks cover every profile; 24–32 tasks and multiple seeds make
parallel scheduling comparisons more informative than a two-task smoke run.

- Images: 640×480 JPEG (orientation 1), 1600×1200 JPEG (6), 2048×1536 PNG (3),
  2560×1440 BMP (1), 1200×1800 JPEG (8), and 1920×1080 PNG (1). Seeded RGB noise
  intentionally prevents unrealistically cheap solid-color compression. Actual
  EXIF metadata, source format/dimensions, aspect-preserving thumbnail sizes,
  multi-format encodes and PNG pixel roundtrips are checked.
- OCR: 1200×900 / one page / six lines; 1275×1650 / two pages / ten lines per
  page; 1654×2339 / three pages / sixteen lines per page; and 1650×1275 / two
  pages / eight lines per page. All PDFs contain raster images, not extractable
  text. `pdfinfo` checks the page count before bounded Poppler rasterization;
  Tesseract reads every rendered page. Rendering uses a minimum 1800-pixel
  long side (2339 for the largest profile), as for the existing smoke fixture.
  Native-size rendering previously misread `DELTA` as `DELIA` on task 6/page 2
  of representative seed 12011, despite an unclipped, visibly legible page.
  Raising raster resolution fixed recognition without changing the PDF text,
  dictionary, page segmentation, or exact oracle. The complete 12-task seed is
  a regression test; this remains clean synthetic print, not an OCR accuracy
  guarantee for arbitrary text, scan quality, or Tesseract versions.
  Each page's actual recognized text and
  the entire document must match the independently generated known text exactly
  after whitespace collapse and uppercase conversion. Those normalizations
  ignore layout whitespace/case only; no tokens, punctuation or OCR mistakes are
  discarded, and the oracle is never learned from Tesseract output.
- Video: 320×180 / 12 FFV1 frames; 640×360 / 24 PNG frames; 640×360 / 48 FFV1
  frames; and 480×270 / 36 PNG frames. Both are real installed FFmpeg lossless
  codecs inside Matroska. Frame dimensions/counts come from task records. Decoder
  requests one extra frame to detect surplus input, validates the full raw-frame
  pixel oracle, then extracts every third frame as a checked PNG. Colors and
  moving rectangles vary, but this is still deliberately simple video content.

These profiles represent several useful operation sizes, not a production corpus
or proof of production throughput. They exclude damaged/untrusted files, noisy
scans, handwriting, complex document layouts, long/full-HD video, lossy interframe
codecs, and whole-service I/O/congestion. Run separately on actual representative
production data before making deployment or capacity claims. The tests exercise
all profiles across serial, threads, and spawned processes, plus deterministic
re-preparation and deliberate wrong-oracle/dimension/page/frame-count failures.

Preparation is untimed. The runner may prepare immutable input tasks and a serial
oracle once per seed and reuse them across executor candidates and cold/warm
passes, retaining their directory until all futures finish. `run_task` does not
mutate task records or fixtures; all output files use unique temporary folders.
No cross-root/global fixture cache hides preparation provenance. Avoid concurrent
performance runs; preparation and correctness validation can also consume CPU.

Image/video results are deterministic within the same installed codec versions;
encoded bytes can differ between library releases. PDF/Matroska metadata may
vary between preparations; container bytes are not used for backend equality.
Compare results across backends on the same prepared fixtures and environment.
A real Tesseract recognition difference across versions fails visibly and should
be investigated, not normalized into a pass. Record Pillow, Tesseract, Poppler,
and FFmpeg versions alongside performance results.

## Resource and safety boundaries

Only trusted task records from `prepare` are supported. This is not an arbitrary
file ingestion service or security sandbox. Each task verifies its input digest
and has a 16 MiB file-size cap. Preparation accepts 1–256 fixtures. Images are capped at 4,000,000 source
pixels, and outputs fit within 512×512. OCR is capped at three pages and 2400
pixels on the rendered long side (the smoke profile uses 1800). Video caps are
640×360 and 48 frames; the declared decoded RGB buffer including one surplus
frame must fit within 64 MiB (largest profile is about 32 MiB). Preparation writes
raw video to a temporary file a frame at a time rather than accumulating it in
RAM. Input-size caps are verified during preparation and replay. These caps are
per task, not aggregate limits: total fixture disk usage and concurrent RSS scale
with count and workers. For example, one BMP profile occupies about 11 MiB.
Use a small count and worker limit on constrained machines.

Every child command has a 60-second timeout. FFmpeg decoder, encoder, and filter
threads are set to one; Tesseract subprocesses use `OMP_THREAD_LIMIT=1` and
`OMP_NUM_THREADS=1`. These are per-process parallelism controls, not hard RSS or
CPU limits. Limit outer executor concurrency separately. No global environment
variables or Pillow decompression-bomb limits are modified. Unique temporary
output directories permit safe concurrent replay of the same task. Child command
stderr is surfaced on failure; subprocess timeout kills/waits for the direct
child via Python's subprocess implementation.

Subprocess CPU consumption is not captured by Python current-thread CPU time;
therefore predictive executor thread CPU/wall ratios for OCR/video primarily
measure orchestration. They must not be presented as evidence that OCR/video are
intrinsically I/O-bound or as universal scheduling guidance.
