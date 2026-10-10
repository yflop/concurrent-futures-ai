# Publishing checklist

Repository: public [`yflop/concurrent-futures-ai`](https://github.com/yflop/concurrent-futures-ai).
Python distribution: `intelligent-futures`; import package: `intelligent_futures`.
A local package or successful upload command is not proof of publication. Verify
remote source, CI, release assets, and package-index metadata separately.

## Prepare and verify

1. Review the complete diff, MIT license ownership, benchmark metadata, and absence
   of secrets or private data. Keep virtual environments, caches, authentication
   files, and task logs out of the repository and release artifacts.
2. Update the version consistently. The core package remains dependency-free;
   the optional MCP adapter uses the `mcp` extra in the same distribution.
3. Run the full tests, benchmark tests, catalog synchronization check, compilation,
   examples, and applicable optional-dependency integration tests. Record which
   checks ran and any remaining limits. Do not substitute local tests for CI.
4. Build a wheel and source distribution from the reviewed source. Install the
   exact wheel in a clean virtual environment, verify its metadata and imports,
   and run the relevant tests against that installed package. Check both files
   with `python -m twine check --strict`.
5. Freeze those distribution bytes. Compute their SHA-256 hashes, then update
   `.github/workflows/publish-pypi.yml` to the new version, release URLs, filenames,
   hashes, and concurrency group. Never leave old hashes attached to a new version.
6. Commit the reviewed source and workflow; push only when authorized. Verify the
   exact remote commit SHA and wait for every required CI job on that SHA.

## GitHub release

1. Create a new version tag on the verified commit. Never move an existing release
   tag or replace a previously published package version.
2. Prepare a complete tracked-source ZIP from that commit. Include the final
   publication workflow; exclude local build output and untracked files.
3. Write `SHA256SUMS.txt` covering the wheel, source distribution, and source ZIP.
   Upload these four files to the new release and verify the published assets.
4. State the approved Beta maturity and experimental capability limits. Explain
   the optional MCP dependency, transport boundaries and known limitations.
   Include the source commit, CI evidence, and actual verification results.
   Select the GitHub prerelease flag according to the approved release plan;
   do not infer that decision from the package classifier alone. Do not imply
   planned concepts are implemented or claim unmeasured speedups.

Version 0.4 has Beta maturity. The publication workflow requires source and
built-artifact verification, exact-commit CI, review and source-matched endurance
evidence. Beta is not Production/Stable or a production certification. The GitHub prerelease flag, Python version ordering,
and package maturity classifier are separate: a version such as `0.4.0` is a
normal index version even if its GitHub release is marked as a prerelease.

## PyPI publication

PyPI publication needs its own authorization; a GitHub push alone does not grant
it. The manual `publish-pypi.yml` workflow downloads the exact GitHub release
wheel and source distribution, verifies the pinned hashes, validates their
metadata, and uploads those bytes using the existing `PYPI_API_TOKEN` repository
secret. It does not rebuild packages or publish the source ZIP.

- Dispatch the reviewed workflow only after the corresponding source CI and
  GitHub release checks pass. Verify the selected branch and workflow version.
- Preserve the isolated upload virtual environment and pinned tooling. Never log,
  copy, or expose the secret, or create new credentials merely to run a release.
- Follow the workflow through its terminal result. If it fails, inspect the
  failing step and the index before retrying; uploads may have partially succeeded.
- Verify the expected version and both filenames on PyPI, compare index SHA-256
  digests with the release assets, and test installation from the index in a clean
  virtual environment. Verify the optional extra separately when applicable.
- PyPI distribution files cannot be overwritten. If published content is wrong,
  prepare a corrected new version rather than silently replacing release bytes.
