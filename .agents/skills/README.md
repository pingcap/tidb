# Skills Usage

This repository stores TiDB's repo-level skills under `.agents/skills`.

- Current working directory (`$CWD`) is the skill root scope for repo-level loading.
- Put shared repository skills here.
- Keep skill-specific references under each skill folder (for example: `tidb-test-guidelines/references/`).

Current operational workflow skills:

- `tidb-verify-profile`: choose WIP/Ready/Heavy validation scope before running checks.
- `tidb-bazel-prepare-gate`: decide whether `make bazel_prepare` is required from changed files.
- `tidb-failpoint-test-runner`: decide failpoint enable/disable and run unit tests safely.
- `tidb-integrationtest-recorder`: run and review `tests/integrationtest` recording flow.
- `tidb-realtikv-runner`: run RealTiKV tests with startup/cleanup discipline.
- `tidb-test-diff-triage`: triage unexpected test diffs (failpoint vs upstream vs local regression).
- `tidb-test-guidelines`: test placement, naming, writing conventions, and shard_count guidance.

Use `AGENTS.md` for repository policy, validation/reporting requirements, and the pre-flight checklist.
Use `docs/agents/testing-flow.md` for canonical build/test command playbooks.
