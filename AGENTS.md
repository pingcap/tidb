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

## Agent Interaction Overrides (Repo-Local)

- For short non-code questions about definitions, acronyms, symbols, errors, or other potentially repository-specific terms, agents MUST search the active repository and read the closest authoritative local definition before answering, lead with the repository-specific meaning, and use general knowledge only when local evidence is absent or the user explicitly requests broader context.

## ExecPlans

When writing complex features or significant refactors, use an ExecPlan from design to implementation.

- Definition and format: `PLANS.md` at repository root.
- Requirement: keep the ExecPlan updated as a living document while implementation progresses.
- Scope: use this for multi-step work where losing context would risk correctness or incomplete validation.

## Quick Decision Matrix

| Task | Required action |
| --- | --- |
| Build/test preparation or changes affecting Bazel metadata | Apply the complete trigger list in `Build Flow` -> `When make bazel_prepare is required`; use `tidb-bazel-prepare-gate` if the decision is unclear. |
| Running package unit tests | SHOULD run targeted tests and avoid full-package runs unless needed (see `docs/agents/testing-flow.md` -> `Unit tests`). |
| Unit tests in a package that uses failpoints | MUST enable failpoints before tests and disable afterward (see `docs/agents/testing-flow.md` -> `Failpoint decision for unit tests`). |
| Recording integration tests | MUST use the recording command in `docs/agents/testing-flow.md` -> `Integration tests` (not `-record`; `-record` is for unit-test suites that explicitly support it). |
| RealTiKV tests | MUST start playground in background, run tests, then clean up playground/data (see `docs/agents/testing-flow.md` -> `RealTiKV tests`). |
| Bug fix | MUST add a regression test and verify it fails before fix and passes after fix. |
| Fmt-only PR | MUST NOT run costly `realtikvtest`; local compilation is enough. |
| During local coding iterations | SHOULD use the `WIP` verification profile from `.agents/skills/tidb-verify-profile` to run only scoped checks. |
| Delivering repository changes or preparing a PR | MUST use the `Ready` verification profile from `.agents/skills/tidb-verify-profile`, selecting checks by change type. Code changes require `make lint`; documentation, testdata, and build changes retain their applicable checks. Read-only analysis does not trigger build/test checks. |
| Creating or updating a GitHub issue | SHOULD use `.agents/skills/tidb-issue-metadata-guard` to preserve issue templates and label hygiene. |
| Creating a PR or editing PR metadata | SHOULD use `.agents/skills/tidb-pr-metadata-guard` to preserve PR templates, title scope, and bot-parsed checklist sections. |
| Before finishing | SHOULD self-review diff quality before finishing. |
| Expensive optional sweeps (for example broad package runs) | MUST run only when required by change scope, CI reproduction, or explicit user request. The stricter rule for `make bazel_lint_changed` is in `Build Flow`. |

### Skills

- Repository-level skills are maintained under `.agents/skills` (relative to the repository root / current working directory).
- Keep skill content and references together under each skill folder (for example: `.agents/skills/<skill>/SKILL.md` and `.agents/skills/<skill>/references/`).
- `.github/skills` is kept only as a migration note path and should not be used as the primary location for new skill updates.
- Policy belongs in `AGENTS.md`; detailed command playbooks SHOULD live in `docs/agents/*`, and skills SHOULD provide entrypoint workflows that reference those playbooks.
- Operational testing/build skills are indexed in `.agents/skills/README.md` to avoid duplicated lists drifting in multiple docs.

## Pre-flight Checklist

1. Restate the task goal and acceptance criteria.
2. Locate the owning subsystem and the closest existing tests (`Repository Map`, `Task -> Validation Matrix`). Before changing or reviewing code behavior, agents MUST read the target package's `doc.go` when present. Pure spelling or formatting edits may skip this contract reading; reuse unchanged documentation already read during the task.
3. Decide prerequisites before running tests/build (`docs/agents/testing-flow.md` -> `Failpoint decision for unit tests`; `AGENTS.md` -> `Build Flow` -> `When make bazel_prepare is required`).
4. Pick the smallest valid validation set and prepare final reporting items (`Agent Output Contract`).
5. If `AGENTS.md`, repository skills, or docs under `docs/agents/` changed, follow `docs/agents/agents-review-guide.md` before delivery.

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
  - MUST: Before changing or reviewing DDL behavior, read `docs/agents/ddl/README.md` and use it as the entrypoint to the execution framework. Pure spelling or formatting edits may skip this reading; reuse unchanged content already read during the task.
  - Debugging: You MAY reference `docs/agents/ddl/*`, but you MUST NOT treat it as authoritative. Treat it as hypotheses until verified in code/tests (avoid hallucination/outdated assumptions).
  - Doc drift: When implementation and `docs/agents/ddl/*` differ on behavior involved in the task, you MUST update the affected docs with the change and call it out in the PR/issue. In a read-only review, report the drift without editing files.

## Build Flow

### When `make bazel_prepare` is required

When any condition below applies, MUST run `make bazel_prepare` before build/test and include resulting Bazel metadata changes (for example `BUILD.bazel`, `**/*.bazel`, and `**/*.bzl`) in the change:

- New workspace or fresh clone.
- Bazel-related files changed (for example `WORKSPACE`, `DEPS.bzl`, `BUILD.bazel`, `MODULE.bazel`, `MODULE.bazel.lock`).
- Any Go source file is added/removed/renamed/moved in the PR.
- The import section changed in any existing Go source file (including `*_test.go`).
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
```

Run `git fetch origin --prune` when the task needs current remote refs, such as comparing an upstream base or preparing a backport; it is not a build prerequisite.

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
| Agent instructions or skills only | Follow `docs/agents/agents-review-guide.md`; validate changed skill metadata and affected references. No code build/test is required for documentation-only changes. |

## Testing Policy

- Detailed command playbooks live in `docs/agents/testing-flow.md`.
- Select required test surfaces first (`Task -> Validation Matrix`), then run scoped commands from the playbook.
- Use `.agents/skills/tidb-verify-profile` to select checks for iteration or delivery as defined in `Quick Decision Matrix`. Completion depends on the applicable checks and evidence, not the wording of a status message.
- Reuse completed checks when they still cover the delivered changes. Rerun affected checks after relevant changes or new failures invalidate that evidence; do not repeat checks solely to report status.
- All other testing rules (failpoints, integration recording, RealTiKV lifecycle, regression tests) are stated once in `Quick Decision Matrix` above; do not duplicate them here.

## Code Style Guide

### Go and backend code

- Because TiDB is a complex system, code SHOULD remain maintainable for future readers with basic TiDB familiarity, including readers who are not experts in the specific subsystem/feature.
- Follow existing package-local conventions first and keep style consistent with nearby files.
- Code SHOULD be self-documenting through clear naming and structure.
  - Example: when implementing a well-known algorithm, naming SHOULD be clear enough to make the approach recognizable; if naming alone may not make intent obvious, add a brief comment.
- Keep error handling actionable and contextual; avoid silently swallowing errors.
- For new source files (for example `*.go`), include the standard TiDB license header (copyright + Apache 2.0) by copying from a nearby file and updating year if needed.
- Comments SHOULD explain non-obvious intent, constraints, invariants, concurrency guarantees, SQL/compatibility contracts, or important performance trade-offs, and SHOULD NOT restate what the code already makes clear.
- Keep exported-symbol doc comments, and prefer semantic constraints over name restatement.

### Tests and testdata

- Prefer extending existing test suites and fixtures over creating new scaffolding.
- Keep test changes minimal and deterministic; avoid broad golden/testdata churn unless required.
- When recording outputs, verify changed result files before reporting completion.

### Docs and command snippets

- Commands in docs SHOULD be copy-pasteable from repository root unless explicitly scoped.
- Use explicit placeholders such as `<package_name>`, `<TestName>`, and `<dir>`.
- Documentation updates SHOULD keep terminology, policy wording, and command conventions consistent across related docs.
- Keep guidance executable and concrete; avoid ambiguous phrasing.

## Agent Output Contract

When delivering changes, report:

1. Files changed.
2. Validation profile used (`WIP`, `Ready`, or `Heavy`) and why.
3. Risks: correctness, compatibility, performance.
4. Exact commands run for validation.
5. What was not verified locally.

For read-only analysis, report findings, supporting evidence, and verification limits; a code-validation profile is not required.
