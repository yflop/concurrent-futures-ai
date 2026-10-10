# Publishing checklist

Intended destination: public `yflop/concurrent-futures-ai`. A local package is not
proof of publication; verify the remote URL and exact commit after pushing.

1. Review MIT license ownership, repository name, public visibility, all source,
   benchmark metadata, and absence of secrets or private conversation content.
2. Run tests, examples, benchmarks, catalog checks, and a clean wheel install.
3. Inspect the staged diff and generated artifacts. Do not commit `.venv`, caches,
   authentication files, task logs, or large unreviewed benchmark output.
4. Authenticate using a supported secure GitHub flow. Never paste a token into
   documentation, command history, a git remote URL, or an assistant message.
5. Create the public repository using the authorized owner's account. Add the
   verified remote, push the reviewed main commit, and verify the SHA remotely.
6. Wait for GitHub Actions on that SHA. Local tests are not remote CI results.
7. Enable desired security reporting and branch protection manually or under
   explicit authorization. Decide on release ownership before publishing to PyPI.

Typical manual commands after authorization and secure login:

```sh
git init -b main
git add .
git commit -m "Initial Intelligent Futures prototype and concept library"
gh repo create yflop/concurrent-futures-ai --public --source=. --remote=origin --push
gh run list --repo yflop/concurrent-futures-ai
```

A GitHub repository publication does not authorize a PyPI release. Package name
availability and trademark conflicts have not been checked. Use a virtual
environment for local installs. The distribution name is `intelligent-futures`;
the import name is `intelligent_futures`.
