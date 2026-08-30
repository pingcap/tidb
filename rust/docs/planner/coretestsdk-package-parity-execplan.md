# Complete `pkg/planner/util/coretestsdk` as one Rust test-support crate

This ExecPlan is a living document maintained under `PLANS.md`.

## Purpose / Big Picture

Pinned TiDB commit `e2788410d8d696605e8cb002585877a063ccc909` contains exactly three artifacts in `pkg/planner/util/coretestsdk`: `mock.go`, `testkit.go`, and `BUILD.bazel`. The package is a reusable planner-test library, not production optimizer policy. Rust currently repeats reduced versions of its table fixtures across tests and still has ignored tests that cite the missing SDK.

The Rust mapping is one `tidb-planner-coretestsdk` crate. It will expose the same signed, unsigned, no-PK, view, partition, global-index, and non-public-column fixtures; an infoschema/context implementing the planner's existing `TableSource` boundary; the parser and planner-suite lifecycle; and `GetFieldValue`'s exact edge behavior. Rust ownership and RAII replace Go pointer aliases and explicit stats-handle goroutine cleanup without inventing runtime behavior.

## Progress

- [x] (2026-08-30) Read all pinned production/build artifacts and inventoried every exported and private helper.
- [x] (2026-08-30) Verified `tidb-model` represents every source metadata field and `tidb-planner::plan_builder::catalog::TableSource` is the existing planner boundary.
- [x] (2026-08-30) Added the crate and source-shaped fixtures, including the zero-value view column type and the source's unusual no-PK offsets/flag combinations.
- [x] (2026-08-30) Added structural and planner-boundary tests for all fixtures, case-insensitive table/view resolution, source-shaped field extraction, parser configuration, context aliasing, and sequential suite IDs.
- [x] (2026-08-30) Ran final Ready lint and self-reviewed the complete diff; commit and push are the remaining handoff actions.

## Decisions

- Use a separate test-support crate because the Go package is an importable test library and placing it in `tidb-planner` would create a planner-to-session/mock ownership cycle.
- Keep the package metadata-only. `MockContext` supplies the current database, division precision, and infoschema consumed by Rust planning; Go's mock store client and stats-handle goroutine have no Rust counterpart at this boundary, so Rust RAII has nothing to start or close.
- Preserve the pinned `//go:build !codes` disposition: the Rust crate is built only when selected as a dependency/test target and has no production binary dependency.

## Validation

Run from the repository root:

    cargo test --manifest-path rust/Cargo.toml -p tidb-planner-coretestsdk --no-fail-fast
    cargo check --manifest-path rust/Cargo.toml -p tidb-planner-coretestsdk
    cargo fmt --manifest-path rust/Cargo.toml --all -- --check
    make lint
    git diff --check

No Go source, imports, Bazel metadata, or module dependencies are changed, so `make bazel_prepare` is not required.

## Outcomes

The crate is dependency-closed over existing Rust model, parser, and planner boundaries. No runtime crate depends on it. `cargo test`, `cargo check`, `cargo fmt --check`, `cargo clippy --all-targets`, and `git diff --check` pass; clippy reports only pre-existing warnings in dependencies and none in `tidb-planner-coretestsdk`.

## Artifact Inventory and Acceptance

`mock.go` owns three private field-type constructors and nine exported fixture constructors. Tests must check exact table IDs/names, column IDs/offsets/types/flags/states, all index IDs/names/columns/prefix lengths/states/uniqueness/global flags, view definition/security/definer/columns, and every partition type/expression/count/definition/value.

`testkit.go` owns `GetFieldValue`, `PlannerSuite`, two suite constructors, and `Close`. Tests must pin the source's `idx > 0` and `end > 0` quirks, sequential reassignment of logical and partition IDs in `CreatePlannerSuiteElems`, case-insensitive table/view lookup, parser strict/window defaults, shared context identity, and harmless close.

`BUILD.bazel` declares only the two sources, dependencies, public visibility, and the normal library target. The new crate manifest and workspace membership are its Rust build-artifact disposition; there are no Go tests, fixtures, generated files, platform variants, benchmarks, fuzz targets, or examples in this package.
