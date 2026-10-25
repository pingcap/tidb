# AGENTS.md

This file provides guidance to agents working in this repository.

MUST means required, SHOULD means recommended unless there is a concrete reason to deviate, MAY means optional. A deeper `AGENTS.md` takes precedence for its subtree.

## Non-negotiables

1. Correctness first. TiDB is a distributed SQL database; seemingly small changes can alter SQL semantics, consistency, or cluster behavior.
2. No speculative behavior. Do not invent APIs, defaults, protocol behavior, or test workflows. For repo-specific terms, check the repository first.
3. Keep diffs minimal. Avoid unrelated refactors, broad renames, or formatting-only churn unless explicitly requested.
4. Leave verifiable evidence. Run targeted checks and report exact commands.
5. Do not hand-edit generated code; regenerate it from its source inputs.
6. TiDB Go-to-Rust transcreation MUST use one complete upstream Go package (or pinned external-module package) as the unit of implementation and completion. A claim MUST include every production source, build/platform/generated variant and input, original test/support artifact, fixture, build artifact, and required validation gate for that package. One Go package MAY map to several Rust crates, but keeps one atomic claim, inventory, integration decision, and receipt. Partial file/function/branch/feature ports MAY remain as seed evidence, but MUST NOT be dispatched, integrated, or reported as a transcreated package.
7. Rust build gate: for any change under `rust/`, `cd rust && cargo build --locked -p tidb-server` MUST pass in the pre-commit hook (`hooks/pre-commit`) and again immediately before pushing. A build without `--locked`, or an earlier build, does not satisfy this gate.

## Change Scope Decides the Gates

Run only the gates the changed files call for:

- Go or Bazel files changed: the Go gates below (`make bazel_prepare` when required, Go tests, `make lint`).
- Only `rust/` changed: the Rust gates (non-negotiable 7 and the Rust rows of the validation matrix). Go lint, `make bazel_prepare` and Go tests are not required.
- Only docs changed: no build or test; for `AGENTS.md` or `docs/agents/**`, follow `docs/agents/agents-review-guide.md`.

Expensive optional sweeps (`make bazel_lint_changed`, full-package or full-workspace runs) MUST run only when the change scope, a CI reproduction, or the user requires them. Agents MUST NOT run `make bazel_lint_changed` unless the user explicitly requests it.

## Go Gates

`make bazel_prepare` MUST run, and its Bazel metadata changes (`BUILD.bazel`, `**/*.bazel`, `**/*.bzl`) MUST be included, when any of these holds:

- a Go source file is added, removed, renamed or moved;
- the import section of an existing Go file (including `*_test.go`) changed;
- a new top-level `func TestXxx(t *testing.T)` is added to an existing `*_test.go` file;
- Bazel files or Bazel test targets changed (`WORKSPACE`, `DEPS.bzl`, `BUILD.bazel`, `MODULE.bazel`, `MODULE.bazel.lock`, `shard_count`, test `srcs`, `tests/realtikvtest/**/BUILD.bazel`);
- `go.mod` / `go.sum` changed;
- a fresh clone, or a local Bazel dependency/toolchain error.

`.agents/skills/tidb-bazel-prepare-gate` is the operational checklist. Claiming completion of a Go code change MUST include `make lint`.

## Task -> Validation Matrix

Use the smallest set that proves correctness. Command playbooks (unit tests, failpoints, integration recording, RealTiKV) live in `docs/agents/testing-flow.md`.

| Change scope | Minimum validation |
| --- | --- |
| `pkg/planner/**` rules or plans | Targeted planner unit tests; update rule testdata when needed |
| `pkg/executor/**` SQL behavior | Targeted unit test plus relevant `tests/integrationtest` |
| `pkg/expression/**` builtins or type inference | Targeted expression unit tests with edge cases |
| `pkg/session/**`, variables, protocol | Targeted package tests plus SQL integration tests for user-visible behavior |
| `pkg/ddl/**` | DDL-focused unit/integration tests and compatibility checks; read `docs/agents/ddl/README.md` first |
| `pkg/store/**`, `pkg/kv/**` | Targeted unit tests; RealTiKV tests if behavior depends on real TiKV |
| `pkg/parser/**` | `make parser`, `make parser_yacc`, `make parser_fmt`, `make parser_unit_test` and related tests |
| `tests/integrationtest/t/**` | Record with the command in `docs/agents/testing-flow.md` (not `-record`) and verify the regenerated results |
| `tests/realtikvtest/**` | Start the playground in the background, run scoped tests, then clean up playground and data |
| `rust/` crate code | That crate's tests (`cargo nextest run -p <crate>`) plus non-negotiable 7 |
| `rust/` change crossing crates | The tests of every crate whose public items changed, plus non-negotiable 7 |
| Fmt-only change | Compilation only; MUST NOT run `realtikvtest` |

Rules that apply to every row:

- A bug fix MUST add a regression test and show it fails before the fix and passes after it, or state why that is infeasible.
- Unit tests in a package that uses failpoints MUST enable failpoints before the run and disable them afterwards.
- Prefer targeted tests over full-package runs.

## Working Method

- Multi-step features and significant refactors use an ExecPlan (`PLANS.md`), kept current as work proceeds.
- Before changing a package, read its `doc.go` if it has one. `docs/agents/architecture-index.md` maps subsystems to paths and test surfaces.
- DDL module changes (`pkg/ddl/`, `docs/agents/ddl/`): read `docs/agents/ddl/README.md` first and treat it as hypotheses to verify in code; when code and those docs differ, update the docs in the same change and call it out.
- Notes follow `docs/agents/notes-guide.md`.
- Skills live under `.agents/skills` (index: `.agents/skills/README.md`). Use `.agents/skills/tidb-issue-metadata-guard` for issues and `.agents/skills/tidb-pr-metadata-guard` for PRs and their metadata. Policy belongs here; command playbooks belong in `docs/agents/*`.
- Self-review the diff before finishing.

## Code Style

- Follow the conventions of the surrounding code; keep code maintainable for readers with basic TiDB familiarity who are not experts in the subsystem.
- Prefer self-documenting names; comment non-obvious intent, invariants, concurrency guarantees, SQL/compatibility contracts and performance trade-offs, not what the code already says.
- Keep error handling actionable; do not swallow errors silently.
- New source files carry the standard TiDB license header (copy it from a nearby file).
- Tests: extend existing suites and fixtures, keep changes deterministic, and follow `.agents/skills/tidb-test-guidelines`. Verify recorded outputs before reporting completion.
- Doc commands are copy-pasteable from the repository root and use explicit placeholders (`<package_name>`, `<TestName>`, `<dir>`).

## Agent Output Contract

When finishing a task, report: files changed; the validation scope and why; risks (correctness, compatibility, performance); the exact commands run; what was not verified locally.
