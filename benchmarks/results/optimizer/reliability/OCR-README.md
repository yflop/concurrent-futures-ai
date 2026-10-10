# OCR typography diagnosis and corrected development qualification

These are development diagnostics following a real OCR correctness failure.
They are **not held-out configuration-selection evidence, a performance gate,
production-corpus validation, or proof of reliable recognition of arbitrary text**.
The original failure is retained; no expected text was corrected to match OCR and
no failing document was removed from a declared qualification corpus.

## Failure and font experiments

The original generated 24-document training fixture produced two exact-text
failures, at indices 11 and 15 for seed `6864650689634725`: Tesseract read `DELTA`
as `DELIA`. Both affected documents used the 1200 × 900, one-page profile.
The [render-size diagnostic](ocr-render-diagnostic.json) reran those two documents:

- Longest rendered side 1800: both failed with `DELIA`.
- Longest rendered side 2100: both passed exact text.
- Longest rendered side 2400: both failed with `DELIA` again.

This nonmonotonic result does not justify treating a larger render size as a
universal fix. It is a targeted diagnosis of these two inputs, not a replacement
for the failed primary experiment.

The [font diagnostic](ocr-font-report.json) regenerated the original 24-document
training fixture at 42 px. Both `DejaVuSans-Bold.ttf` and `DejaVuSansMono.ttf`
passed all 24 exact-text checks. Bold 42 was then chosen for further development
qualification. These observations do not establish that it outperforms Mono or
that either font is universally OCR-safe.

## The first 480-document attempt is excluded from the diversity claim

An initial 480-document development run incorrectly reset its heading counter to
zero whenever the fixture generator created a new per-document RNG. It therefore
repeated the same heading rather than covering all declared permutations. Its
successful OCR rows do not demonstrate 120 distinct heading orders per profile.
The original file is retained privately for the audit trail and is **excluded**
from the corrected corpus coverage claim below. Its raw SHA-256 identity is
`84ecf8e48e4f4617d3890df3773546d8a79e937ac43f9f6433c2149a6304949d`.

The corrected implementation derives each document's heading index from that
individual document's seeded index. Every page of that document repeats its
assigned heading. Before OCR execution, it explicitly verifies the expected
heading in every generated page and verifies 120 distinct orders per profile.

## Corrected, explicitly verified corpus

The [corrected qualification](ocr-corrected-bold-qualification-v2.json.gz) declares:

- Six heading words: ALPHA, BRAVO, DELTA, ECHO, HOTEL, NOVEMBER.
- All 120 ordered triples without repetition, once for each of four profiles.
- Four profiles `(width, height, pages, lines per page)`:
  `(1200, 900, 1, 6)`, `(1275, 1650, 2, 10)`, `(1654, 2339, 3, 16)`,
  `(1650, 1275, 2, 8)`.
- 480 documents in total, seed `6101317`, and four standard-library worker threads.
- Exact generated page/text truth, without spelling substitutions or exclusions.

The corrected report records 480 results, zero OCR errors, and verified coverage
of all 120 distinct heading orders in **each** profile. Its `accepted: true`
applies only to that declared development correctness corpus. It does not turn
this post-failure, typography-selected experiment into an independent holdout.
Font choice and diagnostic findings were known before this run.

The [separate supervisor record](ocr-bold-qualification-supervisor-v2.json)
observed a sampled process-tree RSS peak of 2,022,539,264 bytes, approximately
1.884 GiB. Its emergency sampling threshold was 2 GiB. Sampling is not hard memory
enforcement and can miss spikes. This development run does **not** establish
passing the primary experiment's 1 GiB memory gate. Its recorded timings are
execution diagnostics, not comparative speed or resource-efficiency claims.

## Reusable development reproducer

[`tools/qualify_ocr_typography.py`](../../../../tools/qualify_ocr_typography.py)
is based on the corrected v2 script, whose source SHA-256 is
`8483b557d5a35e24466afc4e68b2175b42b7173f43c5c34398463a8d600b4db8`.
The reusable tool keeps the four profiles, all 120 document-index-derived
permutations, fixed Bold 42 font, fixed seed, exact truth and four-thread limit.
It adds explicit destinations, overwrite protection, actual font/source byte
hashes, and validation of **all four profiles before any OCR worker executes**.
Generation-time patches are restored before execution, including on failure.

Run only when another measured benchmark is not sharing the machine:

```sh
PYTHONPATH=src python tools/qualify_ocr_typography.py \
  --workspace ocr-development-corpus-new \
  --output ocr-development-qualification-new.json
```

Pillow, Poppler (`pdfinfo`/`pdftoppm`), Tesseract with English data, and the declared
font must be available. Both destinations must be new; the tool never silently
replaces existing evidence. Ordinary OCR errors remain failed result rows and
make acceptance false. A broken coverage assertion aborts before OCR. The CLI
exits nonzero for failed acceptance. Interrupted or preparation-failed runs retain
an unsuccessful report rather than converting a partial corpus into a pass.

The four-thread setting limits workers, not CPU, RSS, or elapsed time. Use an
appropriate external supervisor for actual runs. The reusable tool was checked
with mocked fixtures/coverage/CLI tests; no new OCR qualification was executed
while preparing these artifacts. A future execution creates a new observation
with its own source identities and timings, not a byte-for-byte recreation of the
historical report. Changing typography or truth requires a new fixture revision
and fresh held-out evaluation; this tool does not select or validate a production
concurrency configuration.

## Evidence identities and public projection

The existing metadata-only projection tool produced the JSON files here. Numeric
observations, result hashes, plan hashes, font hashes and outcomes are unchanged;
private absolute paths are not published. Each projection stores its original
file SHA-256 in `public_metadata_projection.original_file_sha256`.

- Corrected `bold-qualification-v2.json`:
  `d58654ab25ed07d9dfb203e824911e818e9ebe3fa03338bfbf05095a09b83644`
- `font-report.json`:
  `21d2217cf49b4cfd822e133458670fbb78694078d9fb7ac79ae983667d915ec3`
- Render diagnostic `report.json`:
  `84e88f828c58a821802b16433fb6c323852c8cbd042fcf516bc9ea85ae26bf05`
- Corrected supervisor `bold-supervisor-v2.json`:
  `98c2daa4550b866c9964298d2b412143a176b898f899bfaf4891b423c1417b61`
- Corrected declared plan:
  `ec235c6872fac124788a468b5f36ec3f443409251de211094f1ed5092a33906d`

The chosen Bold font's actual bytes, verified against the corrected report's
`plan.font_sha256`, have SHA-256
`a4c5bc453ca281d90ea079e596da7ae0dfeb5777497c29ec254e76d97ff6f890`.
The locally inspected Mono font bytes have SHA-256
`54bf827eb99404e8f430c330ad30f063334f637eba0109b6a18d4f566a8e9dd8`;
that hash is a later local file check, not a font-hash field captured by the
historical 24-document font diagnostic. A name alone is not a byte identity.
