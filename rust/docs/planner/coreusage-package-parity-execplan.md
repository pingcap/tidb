# Complete the pinned planner coreusage package

This ExecPlan is a living document maintained under repository `PLANS.md`. The pinned Go revision is `e2788410d8d696605e8cb002585877a063ccc909`.

## Purpose / Big Picture

`pkg/planner/util/coreusage` owns the shared aggregate-cast gate and correlated-column extraction helpers used by logical and physical planning. Rust has the correlated-tree walkers, but its aggregate gate exists only as a test-local copy and its logical schema helper applies the index rewrite that Go reserves for physical plans. Completion provides the production helper and preserves Go's logical/physical distinction without adding policy.

## Progress

- [x] (2026-08-30) Inventoried and read the complete pinned package: `cast_misc.go`, `correlated_misc.go`, and `BUILD.bazel`; there are no package-local tests, fixtures, generated/platform variants, benchmarks, fuzz targets, or examples.
- [x] (2026-08-30) Located every integrated Rust logical and physical helper and the ported external Go aggregate-cast test.
- [x] (2026-08-30) Moved the test-local aggregate cast gate into production and routed the Go-port test through it.
- [x] (2026-08-30) Corrected logical correlated-column resolution, including one shared datum binding, while retaining physical index resolution.
- [x] (2026-08-30) Ran WIP and Ready validation and recorded the atomic package receipt.

## Surprises & Discoveries

- Rust's physical helper already performs Go's `resolveIndex=true` behavior and rewires every physical expression to one shared binding cell.
- Rust's logical helper unconditionally overwrites `Column.Index`; pinned Go passes `resolveIndex=false` for logical plans.
- The Go `WrapCastForAggFuncs` test was ported with a private copy of the production helper because `coreusage` had no Rust home module.

## Decision Log

- Decision: Add one `core_usage` module for the package's aggregate helper while retaining the already-integrated logical and physical tree walkers in their owning IR modules.
  Rationale: One Go package may map to multiple Rust modules; moving mature walkers would add unrelated churn.
  Date/Author: 2026-08-30 / Codex
- Decision: Keep Rust's fallible aggregate descriptor API and return `AggDescError` from the wrapper.
  Rationale: Go's underlying call is non-fallible, while Rust's canonical descriptor method validates construction; propagating that existing error changes no successful behavior.
  Date/Author: 2026-08-30 / Codex

## Outcomes & Retrospective

The aggregate mode gate now has one production implementation and the source Go test invokes it directly. Logical correlated-column extraction now preserves the schema column's index, allocates one binding per matched schema position, and rewires every matching logical occurrence to that binding. Physical extraction retains its separate index-resolution pass. The aggregate source test, all 45 expression-rewriter tests, all 35 physical-plan tests, all 57 logical-rule tests, formatting, the server check, `make lint`, and `git diff --check` pass. This is the atomic completion receipt for pinned Go package `pkg/planner/util/coreusage`.

## Context and Orientation

The package maps to `rust/crates/tidb-planner/src/core_usage.rs`, logical correlated helpers in `expression_rewriter.rs`, and physical correlated helpers in `physical/mod.rs`. The external Go cast test is `tests/rule_inject_extra_projection_wrap_cast_source.rs` and is selected through the crate's `all` integration-test target.

## Plan of Work

Add the production aggregate wrapper with the exact Final/Partial2 exclusion, switch the source test to that wrapper, and remove its duplicate helper. Preserve each schema column's existing index in the logical resolver; keep physical rebinding unchanged. Strengthen the logical test so a non-positional schema index proves the distinction.

## Concrete Steps

From `rust/`, run:

    cargo test --locked -p tidb-planner --test all wrap_cast_for_agg_funcs_gates_final_and_partial2_modes -- --nocapture
    cargo test --locked -p tidb-planner --lib expression_rewriter::tests -- --nocapture
    cargo test --locked -p tidb-planner --lib physical::tests -- --nocapture
    cargo check --locked -p tidb-server
    cargo fmt --all -- --check

From the repository root, run:

    make lint
    git diff --check

## Validation and Acceptance

The external Go-port test must invoke the production wrapper. Logical extraction must retain the schema column's current index, while physical extraction must resolve it to the schema position and preserve one shared datum binding across every physical occurrence.

## Idempotence and Recovery

All commands are safe to rerun. No generated files or external state are changed.

## Artifacts and Notes

Atomic pinned inventory: `pkg/planner/util/coreusage/cast_misc.go`, `correlated_misc.go`, and `BUILD.bazel`.

## Interfaces and Dependencies

The package depends on aggregate descriptors, expression columns, logical and physical plan trees, schemas, and shared correlated datum bindings.
