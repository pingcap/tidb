# `pkg/planner/core/constraint` — Go-master parity audit receipt

Go authority: `origin/master` at
`f5cf8f6337612c6ae51fb6e384e4bb3469dde680`.

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

## 2026-09-08 re-audit: statement conversion policy

The complete two-artifact package and all three Go functions were read again
before editing. Inventory remains 84 lines with no extra source, test, fixture,
generated/platform file or build input. Both Rust exported functions, the
schema helper, every existing owner test and every `DeleteTrueExprs` call site
were reviewed. The dependency review followed `Datum.ToBool`, `StrToFloat`,
`BinaryLiteral.ToInt`, and the existing Rust statement truncation interface.

The Rust true-condition helper ignored `Converted.event`. A strict conversion
of `1garbage` therefore deleted the predicate; Go retains it because `ToBool`
returns an error. The regression was observed failing before the production
edit (zero retained expressions, expected one). The helper now receives the
statement evaluation context, routes DOUBLE/BINARY truncation through the
existing error/warning/ignore policy, and deletes only successfully converted
true values. False constants can publish warnings while remaining in the list.
Plan-cache protected constants never convert or publish warnings. Schema
proof and condition order are preserved.

`RuleContext` carries the same borrowed evaluation context as the live
executor's `StmtContext`; join-reorder fallback retains it, and both test
constructors provide `NoColumns`. The predicate-simplification call forwards
that context to the corrected helper. These are dependency-call adaptations,
not completion claims for the core/rule, core, executor, or base packages.

Ready checks (all exit 0):

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib constraint::tests` — 6/6 passed.
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-executor --lib` — passed.
- `make lint` — passed with Go 1.26.2.
- `rustfmt +nightly-2026-08-22 --check rust/crates/tidb-planner/src/constraint.rs` and `git diff --check` — passed.

Logs: `/tmp/constraint-red-20260908.log`, `/tmp/constraint-green-20260908.log`,
`/tmp/constraint-consumer-20260908.log`, `/tmp/constraint-ready-lint-20260908.log`.
`cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-planner --lib logical::rule_tests` also passed 57/57 (`/tmp/constraint-rules-20260908.log`).

### Remaining cross-package findings

This re-audit does not certify complete optimizer parity. Before the constraint
call, `pkg/planner/core/rule.logicalConstant` has an independent truncation-event
loss in Rust; it must be repaired in that package's own complete audit. Also,
the datatype `Converted` carrier merges numeric-prefix truncation and overflow
into one event, whereas Go `StrToFloat` can emit both diagnostics; its raw
non-UTF-8 string conversion boundary remains a datatype-owner issue. These
findings qualify the earlier blanket parity claim. They are not hidden by the
focused green tests and remain in the continuing loop.

The direct constraint API changes to require a statement context; every
in-repository caller has been updated. Performance remains linear in the
condition count and warning formatting occurs only for conversion events.
Full workspace tests, Bazel execution, cross-platform builds and full SQL
optimizer warning equivalence have not been established by this batch.
