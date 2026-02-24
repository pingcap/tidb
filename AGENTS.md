# AGENTS.md

This file provides guidance to agents working in this repository.

## Purpose and Precedence

- MUST means required.
- SHOULD means recommended unless there is a concrete reason to deviate.
- MAY means optional.
- Root `AGENTS.md` defines repository-wide defaults. If a deeper path later adds a more specific `AGENTS.md`, the deeper file SHOULD be treated as higher precedence for that subtree.

## Non-negotiables

1. Correctness first. TiDB is a distributed SQL database; seemingly small changes can alter SQL semantics, consistency, or cluster behavior.
2. No speculative behavior. Do not invent APIs, defaults, protocol behavior, or test workflows.
3. Keep diffs minimal. Avoid unrelated refactors, broad renames, or formatting-only churn unless explicitly requested.
4. Leave verifiable evidence. Run targeted checks and report exact commands.
5. Respect generated code artifacts. Do not hand-edit generated code outputs; regenerate from source inputs.

## Quick Decision Matrix

| Task | Required action |
| --- | --- |
| Added/moved/renamed/removed Go files, changed Bazel files, updated Bazel test targets, or changed `go.mod`/`go.sum` | MUST run `make bazel_prepare` and include resulting Bazel metadata changes in the PR (for example `BUILD.bazel`, `**/*.bazel`, and `**/*.bzl`). |
| Running package unit tests | SHOULD run targeted tests (`go test -run <TestName> -tags=intest,deadlock`) and avoid full-package runs unless needed. |
| Unit tests in a package that uses failpoints | MUST enable failpoints before tests and disable afterward (see `docs/agents/testing-flow.md`). |
| Recording integration tests | MUST use `pushd tests/integrationtest && ./run-tests.sh -r <TestName> && popd` (not `-record`; `-record` is for unit-test suites that explicitly support it). |
| RealTiKV tests | MUST start playground in background, run tests, then clean up playground/data (see `docs/agents/testing-flow.md`). |
| Bug fix | MUST add a regression test and verify it fails before fix and passes after fix. |
| Fmt-only PR | MUST NOT run costly `realtikvtest`; local compilation is enough. |
| Before finishing | SHOULD run `make bazel_lint_changed`. |

### Skills

- Repository-level Codex skills are maintained under `.agents/skills` (relative to the repository root / current working directory).
- Keep skill content and references together under each skill folder (for example: `.agents/skills/<skill>/SKILL.md` and `.agents/skills/<skill>/references/`).
- `.github/skills` is kept only as a migration note path and should not be used as the primary location for new skill updates.

## Pre-flight Checklist

1. Restate the task goal and acceptance criteria.
2. Locate the owning subsystem and the closest existing tests (`Repository Map`, `Task -> Validation Matrix`).
3. Decide prerequisites before running tests/build (`docs/agents/testing-flow.md` -> `Failpoint decision for unit tests`; `AGENTS.md` -> `Build Flow` -> `When make bazel_prepare is required`).
4. Pick the smallest valid validation set and prepare final reporting items (`Agent Output Contract`).
5. If `AGENTS.md` or docs under `docs/agents/` changed, follow the checklist in `docs/agents/agents-review-guide.md` before finishing.

## Repository Map (Entry Points)

- Detailed subsystem path mapping and test surfaces live in `docs/agents/architecture-index.md` (source of truth).
- Update policy: when module/path mapping changes, update `docs/agents/architecture-index.md` first; update this section only when top-level entry points change.
- `/pkg/planner/`: planner and optimization entrypoint.
- `/pkg/executor/`, `/pkg/expression/`: SQL execution and expression evaluation.
- `/pkg/session/`, `/pkg/sessionctx/`: session lifecycle and runtime statement context.
- `/pkg/ddl/`, `/pkg/infoschema/`, `/pkg/meta/`: schema and metadata management.
- `/pkg/store/`, `/pkg/kv/`: storage and distributed query interfaces.
- `/pkg/statistics/`: statistics and estimation behavior entrypoint.
- `/pkg/parser/`: SQL grammar and AST.
- `/tests/integrationtest/`, `/tests/realtikvtest/`: SQL integration and real TiKV test surfaces.
- `/cmd/tidb-server/`: TiDB server entrypoint.

## Notes

- Follow `docs/agents/notes-guide.md`.

## Build Flow

### When `make bazel_prepare` is required

Run `make bazel_prepare` before building when any of the following is true:

- New workspace or fresh clone.
- Bazel-related files changed (for example `WORKSPACE`, `DEPS.bzl`, `BUILD.bazel`, `MODULE.bazel`, `MODULE.bazel.lock`).
- Any Go source file is added/removed/renamed/moved in the PR.
- Go module dependencies changed (for example `go.mod`, `go.sum`), including adding third-party dependencies.
- UT or RealTiKV tests were added and Bazel test targets were updated (for example `_test.go` in `srcs`, `shard_count`, or `tests/realtikvtest/**/BUILD.bazel` updates).
- Local Bazel dependency/toolchain errors occurred.

Recommended local build flow:

```bash
make bazel_prepare
make bazel_bin
make gogenerate   # optional: regenerate generated code
go mod tidy       # optional: if go.mod/go.sum changed
git fetch origin --prune
make bazel_lint_changed
```

## Task -> Validation Matrix

Use the smallest set that still proves correctness.

Typical package unit test command: `go test -run <TestName> -tags=intest,deadlock` (see `docs/agents/testing-flow.md`).

| Change scope | Minimum validation |
| --- | --- |
| `pkg/planner/**` rules or logical/physical plans | Targeted planner unit tests (`go test -run <TestName> -tags=intest,deadlock`) and update rule testdata when needed |
| `pkg/executor/**` SQL behavior | Targeted unit test plus relevant integration test (`tests/integrationtest`) |
| `pkg/expression/**` builtins or type inference | Targeted expression unit tests with edge-case coverage |
| `pkg/session/**` / variables / protocol behavior | Targeted package tests plus SQL integration tests for user-visible behavior |
| `pkg/ddl/**` schema changes | DDL-focused unit/integration tests and compatibility impact checks |
| `pkg/store/**` / `pkg/kv/**` storage behavior | Targeted unit tests; use realtikv tests if behavior depends on real TiKV |
| Parser files (`pkg/parser/**`) | Parser-specific Make targets and related unit tests |
| `tests/integrationtest/t/**` changed | `pushd tests/integrationtest && ./run-tests.sh -r <TestName> && popd` and verify regenerated result correctness |
| `tests/realtikvtest/**` changed | Start playground, run scoped `go test -tags=intest,deadlock`, then mandatory cleanup |

## Testing Policy

- Detailed command playbooks live in `docs/agents/testing-flow.md`.
- Select required test surfaces first (`Task -> Validation Matrix`), then run scoped commands.
- Prefer targeted runs (`-run <TestName>`). Avoid package-wide runs unless needed for broad refactors, CI reproduction, or shared golden/testdata updates.
- If a package uses failpoints, MUST enable failpoints before tests and disable them afterward.
- Failpoint decision MUST follow `docs/agents/testing-flow.md`: if failpoint search checks have no matches, run without failpoint enable/disable and state the evidence in the final report.
- Bug fixes MUST add regression tests and verify fail-before-fix/pass-after-fix (or document why pre-fix reproduction is infeasible).
- Integration test recording MUST use `pushd tests/integrationtest && ./run-tests.sh -r <TestName> && popd`.
- RealTiKV tests MUST start playground in background and perform mandatory cleanup.

## Code Style Guide

### Go and backend code

- Follow existing package-local conventions first and keep style consistent with nearby files.
- Keep changes focused; avoid unrelated refactors, renames, or moves in the same PR.
- For implementation details, add comments only for non-obvious intent, invariants, concurrency guarantees, SQL/compatibility contracts, or important performance trade-offs; avoid comments that only restate nearby code. Keep exported-symbol doc comments, and prefer semantic constraints over name restatement.
- Keep error handling actionable and contextual; avoid silently swallowing errors.
- For new source files (for example `*.go`), include the standard TiDB license header (copyright + Apache 2.0) by copying from a nearby file and updating year if needed.

### Tests and testdata

- Prefer extending existing test suites and fixtures over creating new scaffolding.
- Unit test suite size in one package SHOULD stay around 50 or fewer as a practical target; use `shard_count` in package `BUILD.bazel` as a reference when splitting.
- Keep test changes minimal and deterministic; avoid broad golden/testdata churn unless required.
- For planner predicate pushdown cases, keep SQL-only statements in `predicate_pushdown_suite_in.json` and put DDL in setup.
- When recording outputs, verify changed result files before reporting completion.

### Docs and command snippets

- Commands in docs SHOULD be copy-pasteable from repository root unless explicitly scoped.
- Use explicit placeholders such as `<package_name>`, `<TestName>`, and `<dir>`.
- Keep guidance executable and concrete; avoid ambiguous phrasing.
- Issues and PRs MUST be written in English (title and description).

## Issue and PR Rules

### Issue rules

- Follow templates under `.github/ISSUE_TEMPLATE/` and fill all required fields.
- Bug reports should include minimal reproduction, expected/actual behavior, and TiDB version (for example `SELECT tidb_version()` output).
- Search existing issues/PRs first (for example `gh search issues --repo pingcap/tidb --include-prs "<keywords>"`), then add relevant logs/configuration/SQL plans.
- Labeling requirements:
  - `type/*` usually comes from template; add `type/regression` when applicable.
  - Add at least one `component/*` label.
  - For bug/regression, include `severity/*` and affected-version labels (for example `affects-8.5`, or `may-affects-*` if unsure).
  - If label permissions are missing, include `Suggested labels: ...` in issue body.

### PR requirements

- PR title MUST use one of:
  - `pkg [, pkg2, pkg3]: what is changed`
  - `*: what is changed`
- PR description MUST follow `.github/pull_request_template.md`.
- PR description MUST contain one line starting with `Issue Number:` and reference related issue(s) using `close #<id>` or `ref #<id>`.
- Keep HTML comments unchanged, including `Tests <!-- At least one of them must be included. -->`, because CI tooling depends on them.
- Avoid force-push when possible; prefer follow-up commits and squash merge.
- If force-push is unavoidable, use `--force-with-lease` and coordinate with reviewers.

## Agent Output Contract

When finishing a task, report:

1. Files changed.
2. Risks: correctness, compatibility, performance.
3. Exact commands run for validation.
4. What was not verified locally.
