# Skills Usage

This repository stores Codex repo-level skills under `.agents/skills`.

- Current working directory (`$CWD`) is the skill root scope for Codex repo-level loading.
- Put shared repository skills here.
- Keep skill-specific references under each skill folder (for example: `tidb-test-guidelines/references/`).

Current operational workflow skills:

- `tidb-verify-profile`: choose WIP/Ready/Heavy validation scope before running checks.
- `tidb-bazel-prepare-gate`: decide whether `make bazel_prepare` is required from changed files.
- `tidb-failpoint-test-runner`: decide failpoint enable/disable and run unit tests safely.
- `tidb-integrationtest-recorder`: run and review `tests/integrationtest` recording flow.
- `tidb-realtikv-runner`: run RealTiKV tests with startup/cleanup discipline.

Suggested invocation order for test/build work:

1. `tidb-verify-profile` (pick `WIP` / `Ready` / `Heavy`)
2. `tidb-bazel-prepare-gate` (only if Bazel-prepare requirement is uncertain)
3. One execution skill by scope (`tidb-failpoint-test-runner`, `tidb-integrationtest-recorder`, or `tidb-realtikv-runner`)
4. Final report via `AGENTS.md` -> `Agent Output Contract`
