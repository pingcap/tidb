# `pkg/planner/core/partidx` — Go-master parity audit receipt

Go authority: `origin/master` at
`aec988ea500de42dd6c8b2cf429dff907ce5bd41`.

## Complete Go inventory

The package contains exactly two tracked artifacts and 238 lines. Every
production file and build target was read in full before editing:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 16 | public Go library target and ranger/planner dependencies |
| `check_constraint.go` | 222 | partial-index predicate implication and null-rejection proofs |

The declaration inventory was checked function by function:
`CheckConstraints`, `exactMatch`, `canBeImpliedFromExprs`, `implCompareExpr`,
`implIsNotNull`, `AlwaysMeetConstraints`, and `checkIsNullRejected`. The
package has no Go test file, `doc.go`, `TestMain`, benchmark, fuzz target,
example, fixture/testdata tree, generated source, platform/build-tag variant,
nested package, or other support artifact. The BUILD file lists exactly the
one production source file above.

## Rust ownership and parity

The dependency-closed owner is `rust/crates/tidb-planner/src/partidx.rs`. Its
partial-index exact-match and ranger implication logic, narrow plan-cache
null-rejection proof, inline owner tests, and logical data-source consumer were
re-read. Go permits callers to discard the boolean results of both exported
source functions; Rust had imposed `#[must_use]` on `check_constraints` and
`always_meet_constraints`. Those annotations were removed. Exact-match
multiset handling, comparison range union, null rejection, panic/debug
assertion boundaries, and data-source partial-path pruning remain unchanged.

The focused regression uses `#[deny(unused_must_use)]` and discards both
source-shaped boolean results. In a temporary clean worktree at the pre-fix
revision it failed with exactly two `unused return value` diagnostics; after
the edit it passes 1/1.

## Validation (Ready profile)

- Current Go-master two-artifact inventory and complete declaration mapping —
  passed.
- Temporary clean-worktree probe:
  `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib partidx::tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — failed before the edit with exactly two diagnostics (captured in `/tmp/tidb-codex/partidx-prefix.log`).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib partidx::tests::source_return_values_may_be_ignored_like_go -- --exact --nocapture` — passed after the edit (1/1).
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib` — passed.
- `rustfmt --check rust/crates/tidb-planner/src/partidx.rs` and
  `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed (repository Ready gate).

The package has no Go test target and no failpoint use. This is a Rust-only
batch: no Go source, import section, Go test function, Bazel file, or module
dependency changed, so `make bazel_prepare` is not required. Full workspace
tests, Bazel execution, and cross-platform planner builds were not run.

## Risks and boundaries

- Correctness: the fail-before/pass-after regression and planner library check
  pass; range implication and null-rejection behavior are unchanged.
- Compatibility: only Rust lint policy changed for source-shaped boolean
  helpers whose Go callers may discard results.
- Performance: no ranger, expression, or partial-index path changed.
- Not verified locally: full workspace tests, Bazel execution, and
  cross-platform planner builds.
