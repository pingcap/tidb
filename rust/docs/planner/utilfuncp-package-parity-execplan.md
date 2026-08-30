# Retire the Go-only `planner/util/utilfuncp` indirection package

This ExecPlan is a living document maintained under `PLANS.md`.

## Purpose / Big Picture

Pinned TiDB commit `e2788410d8d696605e8cb002585877a063ccc909` contains exactly three artifacts in `pkg/planner/util/utilfuncp`: `func_pointer_misc.go`, `func_pointer_misc_test.go`, and `BUILD.bazel`. The production file breaks Go import cycles with 98 assignable function variables and saves allocations while cloning cached plans with five expression-slice helpers. Neither mechanism is runtime SQL policy.

Rust has no corresponding import cycle: planner operations call their owning modules directly. Rust physical plans and expressions are owned values, so cloning a cached physical tree already deep-clones every mutable expression without a separate safe-sharing API. The existing `tidb_planner::plan_cache_constants` module instead models constants as opaque bytes plus a caller-supplied boolean. It is not connected to a real cached plan and therefore is extra behavior with no Go-equivalent input model. Completing this package means removing that synthetic module and its two artificial tests, retaining direct planner calls and owned plan cloning, and recording the disposition of every pinned artifact.

## Progress

- [x] (2026-08-30) Read all three pinned package artifacts in full and count 98 function-pointer declarations, five production helper functions, one regression test, and one Bazel target.
- [x] (2026-08-30) Trace Rust optimizer dispatch, task attachment, cost, index resolution, access-path, optimize-portal, and cached-plan clone/rebuild paths.
- [x] (2026-08-30) Remove the synthetic `PlanCacheConstant` carrier, its public module export, and both tests that exercised only that carrier.
- [x] (2026-08-30) Run WIP and Ready validation and self-review the complete diff; commit, synchronization, and push are the remaining handoff actions.

## Surprises & Discoveries

- Observation: the pinned Go package declares 98 function variables but assigns none of them. Their implementations and initialization live in planner owning packages specifically to avoid Go import cycles.
  Evidence: every non-helper production declaration begins with `var`; the only function bodies in `func_pointer_misc.go` are the five `Clone*ForPlanCache` helpers.
- Observation: Rust's cached plan is the ordinary `physical::PhysicalPlan` tree, and `PhysicalPlan::deep_clone` clones its real `tidb_expr::Expression` values before a private rebuild when private ownership is needed. Serialized prepared-cache entries rebuild the owned tree in place.
  Evidence: `rust/crates/tidb-planner/src/physical/mod.rs::PhysicalPlan::deep_clone`, `rust/crates/tidb-planner/src/physical_plan_cache.rs::rebuild_plan_for_cache`, and `rust/crates/tidb-executor/src/driver/planner_bridge.rs::CachedSelectPlan`.
- Observation: the removed Rust module had no production caller. Its only consumers were the two removed Rust-only tests, and its `Vec<u8>` payload and manual `safe_to_share` flag could not represent Go `expression.Constant` behavior.
  Evidence: `rg -n "plan_cache_constants" rust` before removal returned only the module export, the module itself, and those two tests.

## Decision Log

- Decision: do not create a Rust `utilfuncp` registry or indirect function pointers.
  Rationale: Go uses the registry solely to invert package dependencies. Rust's module/crate dependency graph already permits direct calls, and adding mutable global dispatch would add behavior and failure states absent from Rust's required architecture.
  Date/Author: 2026-08-30 / Codex.
- Decision: map the five selective Go clone helpers to Rust's owned deep clone rather than port their pointer-identity optimization.
  Rationale: Go must clone session-unsafe interface pointers while it may retain immutable pointers. Rust `Expression`, `Column`, `Constant`, and `ScalarFunction` are owned `Clone` values. Deep cloning all of them has the same cross-session isolation behavior and cannot preserve a mutable alias accidentally; selective aliasing would require changing the real expression representation to shared interior mutability and would add risk without changing SQL results.
  Date/Author: 2026-08-30 / Codex.
- Decision: delete rather than rewrite the synthetic constants shim.
  Rationale: a boolean supplied by a test is not Go's recursive `SafeToShareAcrossSession` decision and the opaque bytes are not `expression.Constant`. Keeping it would falsely claim parity and expose an unused public API.
  Date/Author: 2026-08-30 / Codex.

## Outcomes & Retrospective

No Rust package now pretends to implement Go expression sharing through a fake constant type. Real cached physical plans continue through the ordinary physical tree and its recursive rebuild path. Focused tests proved template isolation, repeat binding, and recursive reader/point/index-merge/DML rebuilding. Check, format, clippy, repository lint, and diff validation pass; clippy reports pre-existing warnings in planner dependencies and untouched planner code, with no diagnostic caused by this removal.

## Context and Orientation

`utilfuncp` means “utility function pointers.” In Go it is a dependency-inversion package: lower planner operator packages can call functions implemented in higher planner packages without importing those packages directly. Rust dispatches through methods and ordinary module functions in `rust/crates/tidb-planner/src/logical`, `physical`, `find_best_task`, `task`, `plan_cost_ver2`, and `access_path`; no initialization registry is needed.

The 98 declarations are completely dispositioned in these source-shaped groups: logical best-task and physical enumeration (`FindBestTask4BaseLogicalPlan` through `ExhaustPhysicalPlans4LogicalApply`); statistics, shard-index, and probe-count helpers (`DeriveStats4DataSource` through `GetActualProbeCntFromProbeParents`); generic/operator cost, task attachment, and index resolution (`GetPlanCost` through `Attach2Task4PhysicalIndexMergeJoin`); task comparison and access paths (`AttachPlan2Task` through `GetPossibleAccessPaths`); and the optimizer portal (`DoOptimize`). Their algorithms belong to the packages that assign them in Go, not to `utilfuncp`; Rust calls the corresponding owning implementation directly wherever that operator exists.

The five actual Go functions are `CloneExpressionsForPlanCache`, `CloneColumnsForPlanCache`, `CloneConstantsForPlanCache`, `CloneScalarFunctionsForPlanCache`, and `CloneExpression2DForPlanCache`. All return nil for nil input, return the source slice when all pointer elements are shareable, and otherwise reuse or allocate a destination while cloning unsafe elements. The sole Go test checks that a nil constant entry survives the unsafe cloning branch. Rust vectors are not nullable slices, nullable elements are represented explicitly with `Option`, and the real physical plan owns expression values rather than shared expression pointers. Consequently `PhysicalPlan::deep_clone` preserves `Option::None` structurally and deep-clones unsafe state without the Go-only all-safe allocation shortcut.

## Plan of Work

Remove `rust/crates/tidb-planner/src/plan_cache_constants.rs` and its export from `rust/crates/tidb-planner/src/lib.rs`. Remove `rust/crates/tidb-planner/tests/utilfuncp_clone_constants_nil_entry_source.rs` and `rust/difftests/planner-tests/tests/plan_cache_constants.rs`, because both validate the removed synthetic carrier rather than a real planner plan. Do not change the real physical-plan cache or add another execution route.

Validate that the deleted module has no remaining references, run the focused planner physical-cache tests that prove private clone isolation and recursive rebuild, then run formatting, checking, clippy, repository lint, and diff checks. Review the final diff for unrelated changes before committing and pushing the named branch.

## Concrete Steps

Run from `/Users/qiliu/projects/tidb`:

    rg -n "plan_cache_constants|PlanCacheConstant|clone_constants_for_plan_cache" rust --glob '!rust/docs/planner/utilfuncp-package-parity-execplan.md'
    cargo test --manifest-path rust/Cargo.toml -p tidb-planner physical_plan_cache --no-fail-fast
    cargo check --manifest-path rust/Cargo.toml -p tidb-planner
    cargo clippy --manifest-path rust/Cargo.toml -p tidb-planner --all-targets
    cargo fmt --manifest-path rust/Cargo.toml --all -- --check
    make lint
    git diff --check

The first command must return no matches. Tests and validation commands must exit zero; pre-existing dependency warnings may remain, but touched code must add none.

## Validation and Acceptance

Acceptance is behavioral: ordinary and cached execution continue to use the same real `PhysicalPlan`; a private cached rebuild leaves its template unchanged; recursive cache rebuild still updates parameterized expressions and ranges; and no callable Rust API accepts fake constant bytes or a manually asserted shareability bit. The complete pinned package inventory and architectural dispositions must remain in this plan.

No Go source, imports, Bazel metadata, or module dependencies change, so `make bazel_prepare` is not required.

## Idempotence and Recovery

Search, formatting, and validation are safe to rerun. The deleted module has no production caller, so recovery consists of restoring it only if a newly discovered real caller exists; do not replace it with another adapter. Preserve unrelated user changes and synchronize with the remote branch through a normal rebase before pushing.

## Artifacts and Notes

Pinned inventory:

    pkg/planner/util/utilfuncp/BUILD.bazel
    pkg/planner/util/utilfuncp/func_pointer_misc.go
    pkg/planner/util/utilfuncp/func_pointer_misc_test.go

There are no generated files, generation inputs, platform variants, fixtures, benchmarks, fuzz targets, or examples in this package. `BUILD.bazel` declares one public `go_library` from the production file and one short, flaky `go_test` from the test file. Removing the Rust-only module requires no Rust manifest or lockfile edit because it was an internal module, not a crate.

Validation evidence, all exiting zero:

    cargo test --manifest-path rust/Cargo.toml -p tidb-planner physical_plan_cache --no-fail-fast
    cargo test --manifest-path rust/Cargo.toml -p tidb-planner cached_plan_rebuilds_normal_and_reader_scan_trees_without_mutating_template --no-fail-fast
    cargo test --manifest-path rust/Cargo.toml -p tidb-planner cached_plan_rebuilds_the_same_tree_for_consecutive_parameter_sets --no-fail-fast
    cargo test --manifest-path rust/Cargo.toml -p tidb-planner cached_plan_rebuilds_point_batch_index_merge_and_dml_owned_trees --no-fail-fast
    cargo check --manifest-path rust/Cargo.toml -p tidb-planner
    cargo clippy --manifest-path rust/Cargo.toml -p tidb-planner --all-targets
    cargo fmt --manifest-path rust/Cargo.toml --all -- --check
    make lint
    git diff --check

The first sandboxed `make lint` attempt could not resolve `proxy.golang.org` while installing the pinned `revive` tool. The required network-enabled retry completed successfully. No Go, Bazel, or module input changed, so `make bazel_prepare` was not required.

## Interfaces and Dependencies

No new interface remains. The relevant existing interfaces are `tidb_planner::physical::PhysicalPlan::deep_clone`, `tidb_planner::physical::PhysicalPlan::rebuild_plan_for_cache_in_place`, `tidb_planner::physical::PhysicalPlan::rebuild_plan_for_cache`, the direct logical/physical optimizer dispatch in `tidb-planner`, and ordinary executor construction from that physical tree.

Revision note: this initial version records the complete pinned inventory and removal implementation. It was then updated with the completed Ready validation and exact evidence.
