# `pkg/planner/core/constraint` — Go-master parity audit receipt

Go authority: `origin/master` at
`aec988ea500de42dd6c8b2cf429dff907ce5bd41`.

## Complete Go inventory

The package contains exactly two tracked artifacts and 84 lines. Every
production file and build target was read in full before editing:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 14 | public Go library target and expression/parser dependencies |
| `exprs.go` | 70 | true-condition elimination and NOT-NULL schema proof |

The declaration inventory was checked function by function:
`DeleteTrueExprs`, `DeleteTrueExprsBySchema`, `isNullWithNotNullColumn`, and
the constant/plan-cache and schema/nullability branches within both exported
helpers. The package has no Go test file, `doc.go`, `TestMain`, benchmark, fuzz
target, example, fixture/testdata tree, generated source, platform/build-tag
variant, nested package, or other support artifact. The BUILD file lists
exactly the one production source file above.

## Rust ownership and parity

The dependency-closed owner is `rust/crates/tidb-planner/src/constraint.rs`.
Its constant conversion and plan-cache guard, exact NOT(ISNULL(not-null
column)) shape, inline owner tests, logical join/predicate-simplification
consumers, and field-type flag handling were re-read. Go callers may discard
the slices returned by both exported helpers; Rust had imposed `#[must_use]` on
`delete_true_exprs` and `delete_true_exprs_by_schema`. Those annotations were
removed. Constant conversion/error retention, parameter guard, expression
shape matching, schema lookup, and NOT NULL flag behavior remain unchanged.

The focused regression uses `#[deny(unused_must_use)]` and discards both
source-shaped slice results. In a temporary clean worktree at the pre-fix
revision it failed with exactly two `unused return value` diagnostics; after
the edit it passes 1/1.

## Validation (Ready profile)

- Current Go-master two-artifact inventory and complete declaration mapping —
  passed.
- Temporary clean-worktree probe:
  `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib constraint::tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — failed before the edit with exactly two diagnostics (captured in `/tmp/tidb-codex/constraint-prefix.log`).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib constraint::tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — passed after the edit (1/1).
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib` — passed.
- `rustfmt --check rust/crates/tidb-planner/src/constraint.rs` and
  `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed (repository Ready gate).

The package has no Go test target and no failpoint use. This is a Rust-only
batch: no Go source, import section, Go test function, Bazel file, or module
dependency changed, so `make bazel_prepare` is not required. Full workspace
tests, Bazel execution, and cross-platform planner builds were not run.

## Risks and boundaries

- Correctness: the fail-before/pass-after regression and planner library check
  pass; condition filtering and nullability proof behavior are unchanged.
- Compatibility: only Rust lint policy changed for source-shaped slice helpers
  whose Go callers may discard results.
- Performance: no expression traversal, conversion, or schema lookup path
  changed.
- Not verified locally: full workspace tests, Bazel execution, and
  cross-platform planner builds.
