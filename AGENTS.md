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
| Added/moved/renamed/removed Go files, added a new top-level Go test function matching `func TestXxx(t *testing.T)` in an existing `*_test.go` file, changed Bazel files, updated Bazel test targets, or changed `go.mod`/`go.sum` | MUST run `make bazel_prepare` and include resulting Bazel metadata changes in the PR (for example `BUILD.bazel`, `**/*.bazel`, and `**/*.bzl`). |
| Running package unit tests | SHOULD run targeted tests and avoid full-package runs unless needed (see `docs/agents/testing-flow.md` -> `Unit tests`). |
| Unit tests in a package that uses failpoints | MUST enable failpoints before tests and disable afterward (see `docs/agents/testing-flow.md` -> `Failpoint decision for unit tests`). |
| Recording integration tests | MUST use the recording command in `docs/agents/testing-flow.md` -> `Integration tests` (not `-record`; `-record` is for unit-test suites that explicitly support it). |
| RealTiKV tests | MUST start playground in background, run tests, then clean up playground/data (see `docs/agents/testing-flow.md` -> `RealTiKV tests`). |
| Bug fix | MUST add a regression test and verify it fails before fix and passes after fix. |
| Fmt-only PR | MUST NOT run costly `realtikvtest`; local compilation is enough. |
| During local coding iterations (not claiming completion) | SHOULD use the `WIP` verification profile from `.agents/skills/tidb-verify-profile` to run only scoped checks. |
| Claiming task completion / PR readiness | MUST use the `Ready` verification profile from `.agents/skills/tidb-verify-profile`; if there are code changes, this includes `make lint`. `Ready` is mandatory before making final-status claims such as "fixed", "done", "all tests pass", "ready for review", or "ready for PR". |
| Before finishing | SHOULD self-review diff quality before finishing. |
| Expensive optional sweeps (for example `make bazel_lint_changed`, broad package runs) | MUST run only when required by change scope, CI reproduction, or explicit user request. |

### Skills

- Repository-level skills are maintained under `.agents/skills` (relative to the repository root / current working directory).
- Keep skill content and references together under each skill folder (for example: `.agents/skills/<skill>/SKILL.md` and `.agents/skills/<skill>/references/`).
- `.github/skills` is kept only as a migration note path and should not be used as the primary location for new skill updates.
- Policy belongs in `AGENTS.md`; detailed command playbooks SHOULD live in `docs/agents/*`, and skills SHOULD provide entrypoint workflows that reference those playbooks.
- Operational testing/build skills are indexed in `.agents/skills/README.md` to avoid duplicated lists drifting in multiple docs.

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
- DDL module-only rules (applies to changes under `pkg/ddl/` and `docs/agents/ddl/`):
  - MUST: Before making/reviewing any DDL changes in the DDL module, read `docs/agents/ddl/README.md` first and use it as the default map of the execution framework.
  - Debugging: You MAY reference `docs/agents/ddl/*`, but you MUST NOT treat it as authoritative. Treat it as hypotheses until verified in code/tests (avoid hallucination/outdated assumptions).
  - Doc drift: If implementation and `docs/agents/ddl/*` differ, you MUST update the docs to match reality and call it out in the PR/issue. Do not defer.

## Build Flow

### When `make bazel_prepare` is required

Run `make bazel_prepare` before building when any of the following is true:

- New workspace or fresh clone.
- Bazel-related files changed (for example `WORKSPACE`, `DEPS.bzl`, `BUILD.bazel`, `MODULE.bazel`, `MODULE.bazel.lock`).
- Any Go source file is added/removed/renamed/moved in the PR.
- A code change adds a new top-level Go test function matching `func TestXxx(t *testing.T)` in an existing `*_test.go` file.
- Go module dependencies changed (for example `go.mod`, `go.sum`), including adding third-party dependencies.
- Bazel test targets were updated (for example `shard_count` changed, test `srcs` list edited, or `tests/realtikvtest/**/BUILD.bazel` modified).
- Local Bazel dependency/toolchain errors occurred.

For an operational decision checklist, use `.agents/skills/tidb-bazel-prepare-gate`.

Recommended local build flow:

```bash
# Conditional step: run only when required by this section or `.agents/skills/tidb-bazel-prepare-gate`.
make bazel_prepare
```

```bash
# Then continue with normal local build steps.
make bazel_bin
make gogenerate   # optional: regenerate generated code
go mod tidy       # optional: if go.mod/go.sum changed
git fetch origin --prune
```

`make bazel_lint_changed` is intentionally excluded from the default local flow because it can be slow and resource-intensive on local macOS environments. Agents MUST NOT run `make bazel_lint_changed` unless the user explicitly requests it.

## Task -> Validation Matrix

Use the smallest set that still proves correctness.
Command details for package, integration-test, and RealTiKV surfaces live in `docs/agents/testing-flow.md`.

| Change scope | Minimum validation |
| --- | --- |
| `pkg/planner/**` rules or logical/physical plans | Targeted planner unit tests and update rule testdata when needed |
| `pkg/executor/**` SQL behavior | Targeted unit test plus relevant integration test (`tests/integrationtest`) |
| `pkg/expression/**` builtins or type inference | Targeted expression unit tests with edge-case coverage |
| `pkg/session/**` / variables / protocol behavior | Targeted package tests plus SQL integration tests for user-visible behavior |
| `pkg/ddl/**` schema changes | DDL-focused unit/integration tests and compatibility impact checks |
| `pkg/store/**` / `pkg/kv/**` storage behavior | Targeted unit tests; use realtikv tests if behavior depends on real TiKV |
| Parser files (`pkg/parser/**`) | Parser-specific Make targets (`make parser`, `make parser_yacc`, `make parser_fmt`, `make parser_unit_test`) and related unit tests |
| `tests/integrationtest/t/**` changed | Record and verify regenerated result correctness (see `docs/agents/testing-flow.md` -> `Integration tests`) |
| `tests/realtikvtest/**` changed | Start playground, run scoped tests, then mandatory cleanup (see `docs/agents/testing-flow.md` -> `RealTiKV tests`) |

## Testing Policy

- Detailed command playbooks live in `docs/agents/testing-flow.md`.
- Select required test surfaces first (`Task -> Validation Matrix`), then run scoped commands from the playbook.
- Use `.agents/skills/tidb-verify-profile` to pick a validation profile (`WIP` / `Ready` / `Heavy`). `Ready` is required before any final-status claim; trigger phrases are defined in `Quick Decision Matrix`.
- All other testing rules (failpoints, integration recording, RealTiKV lifecycle, regression tests) are stated once in `Quick Decision Matrix` above; do not duplicate them here.

## Code Style Guide

### Go and backend code

- Because TiDB is a complex system, code SHOULD remain maintainable for future readers with basic TiDB familiarity, including readers who are not experts in the specific subsystem/feature.
- Follow existing package-local conventions first and keep style consistent with nearby files.
- Code SHOULD be self-documenting through clear naming and structure.
  - Example: when implementing a well-known algorithm, naming SHOULD be clear enough to make the approach recognizable; if naming alone may not make intent obvious, add a brief comment.
- Keep changes focused; avoid unrelated refactors, renames, or moves in the same PR.
- Keep error handling actionable and contextual; avoid silently swallowing errors.
- For new source files (for example `*.go`), include the standard TiDB license header (copyright + Apache 2.0) by copying from a nearby file and updating year if needed.
- Comments SHOULD explain non-obvious intent, constraints, invariants, concurrency guarantees, SQL/compatibility contracts, or important performance trade-offs, and SHOULD NOT restate what the code already makes clear.
- Keep exported-symbol doc comments, and prefer semantic constraints over name restatement.

### Tests and testdata

- Prefer extending existing test suites and fixtures over creating new scaffolding.
- Keep test changes minimal and deterministic; avoid broad golden/testdata churn unless required.
- Follow `.agents/skills/tidb-test-guidelines` for placement, naming, `shard_count` guidance, planner-specific casetest rules, and related testdata conventions.
- When recording outputs, verify changed result files before reporting completion.

### Docs and command snippets

- Commands in docs SHOULD be copy-pasteable from repository root unless explicitly scoped.
- Use explicit placeholders such as `<package_name>`, `<TestName>`, and `<dir>`.
- Documentation updates SHOULD keep terminology, policy wording, and command conventions consistent across related docs.
- Keep guidance executable and concrete; avoid ambiguous phrasing.
- Issues and PRs MUST be written in English (title and description).

## Issue and PR Rules

### Issue rules

- Follow templates under `.github/ISSUE_TEMPLATE/` and fill all required fields.
- Bug reports should include minimal reproduction, expected/actual behavior, and TiDB version (for example `SELECT tidb_version()` output).
- Search existing issues/PRs first (for example `gh search issues --repo pingcap/tidb --include-prs "<keywords>"`), then add relevant logs/configuration/SQL plans.
- Labeling requirements:
  - `type/*` is usually applied by the issue template (GitHub UI); if creating issues via `gh issue create`, add it explicitly via `--label` (or follow up with `gh issue edit --add-label`).
  - Add at least one `component/*` label.
  - For bug/regression, include `severity/*` and affected-version labels (for example `affects-8.5`, or `may-affects-*` if unsure).
  - If label permissions are missing, include `Suggested labels: ...` in issue body.

### PR requirements

- PR title MUST use one of:
  - `pkg [, pkg2, pkg3]: what is changed`
  - `*: what is changed`
- PR description MUST follow `.github/pull_request_template.md`.
- PR description MUST contain one line starting with `Issue Number:` and reference related issue(s) using `close #<id>` or `ref #<id>`.
- If you create PRs via GitHub CLI, start from the template to avoid breaking required HTML comments: `gh pr create -T .github/pull_request_template.md` (then fill in the fields; do not delete/alter the HTML comment markers).
- Keep HTML comments unchanged, including `Tests <!-- At least one of them must be included. -->`, because CI tooling depends on them.
- Avoid force-push when possible; prefer follow-up commits and squash merge.
- If force-push is unavoidable, use `--force-with-lease` and coordinate with reviewers.

## Agent Output Contract

When finishing a task, report:

1. Files changed.
2. Validation profile used (`WIP`, `Ready`, or `Heavy`) and why.
3. Risks: correctness, compatibility, performance.
4. Exact commands run for validation.
5. What was not verified locally.
