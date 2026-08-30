# Complete the pinned planner constraint package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. The pinned Go revision is `e2788410d8d696605e8cb002585877a063ccc909`.

## Purpose / Big Picture

The pinned `pkg/planner/core/constraint` package removes predicates that are provably true. Rust currently removes literal true predicates indirectly, but it lacks the package's schema proof that `NOT(ISNULL(column))` is redundant when the child schema declares that column NOT NULL. Completing this package gives selection simplification and join predicate pushdown the same observable condition removal as Go without adding a second planner path.

## Progress

- [x] (2026-08-30) Read and inventoried the complete pinned package: production `exprs.go` and `BUILD.bazel`; no tests, fixtures, generated artifacts, build/platform variants, benchmarks, fuzz targets, or examples.
- [x] (2026-08-30) Added one native Rust owner for both production functions and routed the ordinary simplifier and join predicate-pushdown path through it.
- [x] (2026-08-30) Added focused executable coverage for every source branch and the consuming join behavior; Ready validation passed.
- [x] (2026-08-30) Recorded the atomic package receipt; the validated change is ready to commit and push to `hparser-integration`.

## Surprises & Discoveries

- Observation: literal-true removal already happens at the end of Rust predicate simplification, but it is embedded in that rule and therefore cannot serve Go's selection and schema-aware join call sites as one owner.
  Evidence: `logical/rule_predicate_simplification.rs` filters `PredicateType::True`; there is no Rust `DeleteTrueExprsBySchema` equivalent.

## Decision Log

- Decision: Map the Go package to a root `tidb-planner::constraint` module and keep schema type lookup context-free.
  Rationale: Go passes an evaluation context only because `Column.GetType(ctx)` has a common expression interface signature; Rust's column type is static and `Schema::retrieve_column` already implements Go's identity lookup.
  Date/Author: 2026-08-30 / Codex

## Outcomes & Retrospective

The direct `pkg/planner/core/constraint` package is atomically complete against pinned Go revision `e2788410d8d696605e8cb002585877a063ccc909`. Its complete inventory is production `exprs.go` and `BUILD.bazel`; there are no original tests, fixtures, generated inputs or outputs, build-tag/platform variants, benchmarks, fuzz targets, or examples. `tidb-planner::constraint` is the single native owner. The ordinary predicate simplifier uses its plan-cache-aware literal-true deletion, and join predicate pushdown uses its exact schema proof after deduplication. Validation passed with both branch-complete owner tests, the consuming join regression, all 126 focused planner rule tests, consuming-server compilation, formatting, `make lint`, and `git diff --check`.

## Context and Orientation

Pinned `exprs.go` exports `DeleteTrueExprs` and `DeleteTrueExprsBySchema`; its private `isNullWithNotNullColumn` helper recognizes exactly one expression shape. The ordinary Rust simplifier is `rust/crates/tidb-planner/src/logical/rule_predicate_simplification.rs`. Join predicate classification ends in `LogicalJoin::predicate_push_down_local` in `rust/crates/tidb-planner/src/logical/join.rs`.

## Plan of Work

Add `rust/crates/tidb-planner/src/constraint.rs` with the two source behaviors and branch-complete unit tests. Replace the simplifier's local true filter with the shared owner. After join conditions are deduplicated, remove schema-proven `NOT(ISNULL(non-null-column))` predicates from each side using that child's schema, matching Go's call order.

## Concrete Steps

Run from `rust/` unless stated otherwise:

    cargo test --locked -p tidb-planner --lib constraint -- --nocapture
    cargo test --locked -p tidb-planner --lib rule_ -- --nocapture
    cargo check --locked -p tidb-server
    cargo fmt --all -- --check

Run `make lint` from repository root for the Ready gate, followed by `git diff --check`.

## Validation and Acceptance

Acceptance requires that ordinary constants are removed only when conversion succeeds and returns true, cached parameter/deferred constants remain, malformed/non-matching expression shapes remain, and only a schema-resolved NOT NULL column makes `NOT(ISNULL(column))` disappear. Both production call sites must use the shared owner and all focused tests must pass.

## Idempotence and Recovery

All edits are ordinary source changes. Re-running formatting, tests, checks, and lint is safe. If a behavior test fails, inspect the expression function spelling and static field flags; do not add a query-specific exception.
