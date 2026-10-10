# Reading archived benchmark evidence

Large archival reports use `.json.gz` to retain every recorded measurement while
reducing repository and source-distribution size. Small summaries and ranking
reports remain readable JSON. These are lossless gzip archives of the already
public-safe reports, not new measurements or filtered trial subsets.

[`archives.json`](archives.json) records each newly archived file's compressed
and uncompressed byte counts and SHA256 values. `payload_sha256` identifies the
exact decompressed public report bytes. `archive_sha256` identifies the shipped
gzip bytes. Any `original_file_sha256` within a public report instead identifies
its pre-projection measurement checkpoint; source hashes identify measured code.
These distinct hashes must not be substituted for one another.

## Read or decompress

From an installed source checkout, `tools.summarize_workloads`,
`tools.summarize_optimizer`, and `tools.analyze_optimizer_rankings` accept archived
evidence directly. The workload summarizer accepts `.json` or `.json.gz` report
paths; directory-based optimizer tools also find the compressed representation
when an expected `.json` file has been archived:

```sh
python -m tools.summarize_optimizer --input benchmarks/results/optimizer/primary-v2 --output /tmp/optimizer-summary.json
python -m tools.analyze_optimizer_rankings --input benchmarks/results/optimizer/primary-v2 --output /tmp/optimizer-rankings.json
python -m tools.summarize_workloads benchmarks/results/representative-priority.json.gz benchmarks/results/representative-remaining.json.gz --output /tmp/representative-summary.json
```

The measured `benchmarks.real_world.recheck` and
`benchmarks.real_world.optimizer_variants` harnesses still expect plain JSON; their
source is unchanged. Before passing an archive as a report or `--baseline` input,
first decompress it to a separate local directory. The following command preserves exact payload
bytes and refuses to overwrite an existing file:

```sh
python -m tools.report_io benchmarks/results/representative-priority.json.gz --output /tmp/if-evidence/representative-priority.json
```

Use the resulting plain JSON path as the harness input. Do not overwrite the
archival evidence when running new measurements. The helper accepts only UTF-8
JSON or gzip-wrapped UTF-8 JSON and caps decompressed input at 32 MiB before
parsing; it does not load pickle or execute report content. Supplying both the
plain and compressed representation under the same logical name is rejected as
ambiguous.

## Reproduce archive storage

`python -m tools.archive_reports benchmarks/results` compresses plain reports
over 100,000 bytes, retaining summary/ranking files up to 512,000 bytes. It checks
an exact byte-for-byte round trip before removing each plain copy and updates
the archive manifest. Gzip uses level 9, a zero modification time, and no original
filename header. Existing `.json.gz` archives are left unchanged. No numeric
measurements, configuration choices, failures, or metadata values are rewritten
by this storage operation.
