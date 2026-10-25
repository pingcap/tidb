# AGENTS.md

Guidance for agents working in this repository. MUST is required, SHOULD is the default unless there is a concrete reason. A deeper `AGENTS.md` takes precedence for its subtree.

## Non-negotiables

1. Correctness first: small changes can alter SQL semantics, consistency or cluster behavior.
2. Go TiDB (`pkg/`) is the source of truth. The Rust rewrite (`rust/`) reads and ports the owning Go code, one whole Go package at a time, and follows Go's structure; do not invent APIs, defaults, protocol behavior or test workflows.
3. Keep diffs minimal: no unrelated refactors, renames or formatting churn.
4. Do not hand-edit generated code; regenerate it.
5. A bug fix adds a regression test that fails before the fix and passes after it, or states why that is infeasible.
6. Before pushing a branch with `rust/` changes: `cd rust && cargo build --locked -p tidb-server` passes.
7. Never put credentials or secrets (storage URLs with keys, passwords, tokens) into logs, errors or metric labels; redact URLs first (`ast.RedactURL`).

## Work Efficiently

- Do not sit waiting on builds or tests. Start long jobs in the background and keep working; read the result when it lands.
- Check before running: `cargo check -p <crate> --tests` catches most mistakes in seconds.
- While iterating, run only the tests that cover the change: `cargo nextest run -p <crate> -E 'test(<name>)'`.
- Before a commit, run the suites of the crates whose behavior changed. Workspace-wide runs and the replays (`cargo test -p difftest-result-tests --test integration_diff`, `cargo test -p tidb-server --lib production_replay_stays_within_budget -- --ignored`) are milestone checks, not per-commit ones.
- A pure move or rename (behavior unchanged) is verified by `cargo check --tests` alone; batch several such steps before any test run.
- Keep incremental compilation on: rebuild time matters more than disk. When disk runs low, delete `rust/target/debug/incremental`.
- A test failure that also fails on the unchanged tip is not yours; confirm that once and move on.
- MUST NOT run `make bazel_lint_changed` unless the user asks.

## Go Gates

Only when Go or Bazel files change. `make bazel_prepare` MUST run, and its Bazel metadata changes MUST be committed, when a Go file is added, removed or moved; a Go file's imports change; a new top-level `func TestXxx(t *testing.T)` is added to an existing test file; Bazel files or test targets change; `go.mod` / `go.sum` change; or after a fresh clone or a Bazel toolchain error. `.agents/skills/tidb-bazel-prepare-gate` is the checklist. A Go code change is complete only with `make lint`.

## Task -> Validation Matrix

Command playbooks: `docs/agents/testing-flow.md`.

| Change scope | Minimum validation |
| --- | --- |
| `rust/` crate code | That crate's targeted tests while iterating, its suite before commit |
| `pkg/planner/**`, `pkg/expression/**`, `pkg/executor/**`, `pkg/session/**` | Targeted unit tests; `tests/integrationtest` for user-visible SQL behavior |
| `pkg/ddl/**` | DDL unit/integration tests; read `docs/agents/ddl/README.md` first |
| `pkg/store/**`, `pkg/kv/**` | Targeted unit tests; RealTiKV tests if behavior depends on real TiKV |
| `pkg/parser/**` | `make parser`, `make parser_fmt`, `make parser_unit_test` |
| `tests/integrationtest/t/**` | Record with the command in `docs/agents/testing-flow.md` (not `-record`) and check the result files |
| `tests/realtikvtest/**` | Playground in the background, scoped tests, then clean up playground and data |
| Fmt-only change | Compilation only; no `realtikvtest` |
| Docs only | None |

Go unit tests in a package that uses failpoints MUST enable failpoints before the run and disable them afterwards.

## Working Method

- Multi-step work uses an ExecPlan (`PLANS.md`), kept current as work proceeds.
- `docs/agents/architecture-index.md` maps subsystems to paths. Skills live in `.agents/skills`.
- DDL changes: treat `docs/agents/ddl/*` as hypotheses to verify in code, and update them when the code differs.
- When finishing, report what changed, the exact verification commands, and what was not verified.
