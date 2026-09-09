# Complete the pinned planner rule-util package

This ExecPlan is a living document maintained under repository `PLANS.md`. The pinned Go revision is `e2788410d8d696605e8cb002585877a063ccc909`.

## Purpose / Big Picture

`pkg/planner/core/rule/util` owns expression column replacement, projection substitution, join-column classification, unique-key inference, predicate-simplification hooks, and bottom-up key derivation. Rust already integrates these operations, but its projection helper exposes a Rust-only change flag, its expression rewrite invalidates cached scalar arguments even when no replacement occurs, and its index matching has non-Go fallback behavior. Completion removes those differences and turns the two existing copy-on-write gap placeholders into executable translations of the pinned Go tests.

## Progress

- [x] (2026-08-30) Inventoried and read the complete pinned package: `misc.go` and `BUILD.bazel`; no package-local tests, fixtures, generated/platform variants, benchmarks, fuzz targets, or examples.
- [x] (2026-08-30) Read the pinned cross-package copy-on-write tests in `pkg/planner/core/operator/logicalop/logicalop_test/logical_operator_test.go`.
- [x] (2026-08-30) Corrected production behavior and removed non-source package-local tests.
- [x] (2026-08-30) Replaced stale ignored gap tests with executable source translations.
- [x] (2026-08-30) Ran WIP and Ready validation and recorded the atomic package receipt.

## Surprises & Discoveries

- Rust's `replace_column_of_expr` returns `(Expression, bool)`, but Go returns only `expression.Expression`; the boolean is used only by Rust-local tests and one production caller discards it.
- The Rust scalar rewrite always rebuilds argument storage and invalidates cached arguments. Go clones a scalar function only after the first changed argument and returns the original unchanged tree otherwise.
- Go compares `model.CIStr.L`. Rust metadata stores original strings, but `tidb_ast::CiString` already provides Go-compatible simple Unicode lowercasing; `eq_ignore_ascii_case` is insufficient.
- Two exact Go copy-on-write tests were left as empty ignored functions claiming the helper was unported, even though the production helper now exists.
- Go's projection-replacement test can observe pointer identity with `require.Same`. Rust's `Expression::Column` is an owned value and exposes no shared column pointer. The production caller consumes the replaced child projection immediately, so the planner-observable invariant is the unchanged original expression plus destination-equivalent replacement values; the source test translates that invariant directly rather than adding a fake identity token.

## Decision Log

- Decision: Make both expression helpers borrow the input and return only the translated expression, using an internal change bit solely to implement Go's copy-on-write behavior.
  Rationale: Borrowing keeps the original observable to the caller and removes the public Rust-only result.
  Date/Author: 2026-08-30 / Codex
- Decision: Preserve the package's hook functions as direct Rust calls.
  Rationale: They translate Go's import-cycle hook variables; Rust does not need mutable global function pointers to produce the same planner behavior.
  Date/Author: 2026-08-30 / Codex
- Decision: Translate Go column pointer assertions to value assertions in the copy-on-write tests.
  Rationale: Rust's closed expression enum owns column values. Adding identity-only metadata would be a workaround and changing the complete expression representation is outside this package; on the integrated projection-elimination path, the old child is consumed and pointer aliasing has no planner behavior.
  Date/Author: 2026-08-30 / Codex

## Outcomes & Retrospective

The integrated rule-util leaf now preserves scalar trees and caches when no argument changes, clones only after the first replacement, exposes the Go-shaped expression-only projection result, compares metadata names with Go-compatible `CIStr.L` semantics, and follows the source nested index scan without a Rust-only malformed-schema fallback. Five package-local ad-hoc tests absent from the pinned package were removed. The two pinned logical-operator copy-on-write placeholders are now executable and pass.

Atomic receipt: pinned package inventory `misc.go` plus `BUILD.bazel`; no package-local original tests/support/fixtures/generated/platform variants. Cross-package validation includes the complete pinned `TestReplaceColumnOfExprCopyOnWrite` and `TestResolveExprAndReplaceCopyOnWrite` behavioral invariants. Both source tests passed, 116 logical-rule tests passed, executor/server consumers compiled, formatting passed, Ready `make lint` passed, and diff checks passed.

## Context and Orientation

The Rust translation is `rust/crates/tidb-planner/src/logical/rule_util.rs`. It is consumed by CTE predicate collection, projection elimination, selection max-one-row inference, data-source key derivation, and bottom-up logical rewrites. The pinned package has no own `_test.go`, but `logical_operator_test.go` contains `TestReplaceColumnOfExprCopyOnWrite` and `TestResolveExprAndReplaceCopyOnWrite`, which directly validate this package.

## Plan of Work

Refactor both expression helpers around private `(Expression, changed)` implementations, remove the public change flag, preserve scalar caches when nothing changes, compare index column names through `CiString`, and follow Go's aligned schema indexing rather than silently treating malformed metadata as no key. Remove the package-local ad-hoc test module. Add a source-named integration test file for the two pinned logical-operator tests and delete their obsolete ignored declarations and documentation from the gap catalog.

## Concrete Steps

From `rust/`, run:

    cargo test --locked -p tidb-planner --test all rule_util_copy_on_write -- --nocapture
    cargo test --locked -p tidb-planner --lib logical::rule -- --nocapture
    cargo check --locked -p tidb-executor -p tidb-server
    cargo fmt --all -- --check

From the repository root, run:

    make lint
    git diff --check

## Validation and Acceptance

Both pinned copy-on-write scenarios must run rather than remain ignored: the source expression remains unchanged, projection replacement reuses the destination value, and hash replacement clones destination columns while preserving the source return type and `InOperand` state. Existing logical-rule tests and executor/server consumers must continue to pass.

## Idempotence and Recovery

All commands are safe to rerun. No generated files or external state are changed.

## Artifacts and Notes

Atomic package inventory: `pkg/planner/core/rule/util/misc.go` and `BUILD.bazel`. Cross-package validation sources: pinned `logical_operator_test.go` functions at lines 136 and 160.

## Interfaces and Dependencies

The package uses `tidb-expr` for expressions, columns, and schemas; `tidb-ast::CiString` for Go-compatible case-insensitive metadata identity; planner catalog types for index/column metadata; and the logical rewrite/rule modules for its hook and key-info integration.
