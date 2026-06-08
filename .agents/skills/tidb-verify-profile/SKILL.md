---
name: tidb-verify-profile
description: "Selects which local checks to run during TiDB development: targeted unit tests, make lint, integration test recording, or RealTiKV suites. Use when deciding what to test, preparing to mark a task done, running pre-commit validation, or choosing between fast and full verification."
---

# TiDB Verification Profiles

Decide how much local validation to run before and after code changes.
Policy requirements come from `AGENTS.md`; this skill is the execution guide.

## `WIP` (coding loop)

Use while still iterating. Run only what validates the changed behavior.

```bash
# Targeted unit test (most common)
pushd pkg/<package_name>
go test -run <TestName> -tags=intest,deadlock
popd

# If the package uses failpoints (check first):
rg --fixed-strings "failpoint." pkg/<package_name>
# If matches found, use the failpoint wrapper instead:
./tools/check/failpoint-go-test.sh pkg/<package_name> -run <TestName>
```

Avoid `make lint`, package-wide runs, and `realtikvtest` during WIP.

## `Ready` (completion gate)

Required before claiming "done", "fixed", "all tests pass", or "ready for review".

1. **Map changes to test surfaces** using `AGENTS.md` -> `Task -> Validation Matrix`.
   Common mappings:
   - `pkg/planner/**` -> planner unit tests + update rule testdata
   - `pkg/executor/**` -> unit tests + integration tests (`tests/integrationtest`)
   - `pkg/ddl/**` -> DDL unit/integration tests
   - `tests/integrationtest/t/**` -> record and verify result files
2. **Run targeted tests** for each matched surface.
3. **Run `make lint`** if any Go code changed.
4. **If lint or tests fail**: fix the issue, re-run the failing command, verify it passes.
5. **Report** per `AGENTS.md` -> `Agent Output Contract`: files changed, profile used, risks, exact commands run, what was not verified.

## `Heavy` (explicitly required)

Use only when scope or user request requires expensive checks.

- Examples: CI reproduction, broad refactor confidence, change scope requiring RealTiKV.
- Never run `make bazel_lint_changed` unless the user explicitly requests it.
- For RealTiKV lifecycle details see `.agents/skills/tidb-realtikv-runner`.
