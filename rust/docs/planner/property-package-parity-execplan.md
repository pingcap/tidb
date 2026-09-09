# Complete pinned `pkg/planner/property` behavior as one Rust parity unit

This ExecPlan is a living document maintained under `PLANS.md`.

## Purpose / Big Picture

Pinned TiDB commit `e2788410d8d696605e8cb002585877a063ccc909` defines the planner property package used to carry logical facts, statistics, required physical ordering and execution location, MPP partitioning, partial-order requests, and index-join runtime requirements. Rust already has partial `stats_info`, `task_type`, and `physical_property` modules, but it omits source fields and methods and also retains Rust-only classifications. After this work, those modules form one complete behavioral transcreation of the pinned Go package and its original test, and planner consumers use the completed types rather than local substitutes.

## Progress

- [x] (2026-08-30) Inventory every production, build, and test artifact in the pinned Go package.
- [x] (2026-08-30) Read all pinned Go package files before editing Rust.
- [x] (2026-08-30) Audit the existing Rust property modules and locate their planner/executor consumers.
- [x] (2026-08-30) Remove property-module classifications with no pinned Go behavior.
- [x] (2026-08-30) Add the missing logical property, statistics fields/methods, and exact task behavior.
- [x] (2026-08-30) Complete physical-property fields and methods, including partial order, hash identity, schema/task helpers, and MPP exchange enforcement.
- [x] (2026-08-30) Port the pinned Go test table and add only source-method parity regressions needed to prove the completed package.
- [x] (2026-08-30) Update all consumers and stale documentation to use the completed package behavior.
- [x] (2026-08-30) Run the Ready validation profile and self-review the atomic package diff.
- [x] (2026-08-30) Commit, synchronize, and push the package unit to `origin/hparser-integration`.

## Surprises & Discoveries

- Observation: Rust has no `LogicalProperty` type even though the Go package owns it.
  Evidence: `rg "struct LogicalProperty" rust/crates/tidb-planner/src` returns no match.
- Observation: Rust documents partial order as intentionally absent while the pinned Go physical property carries it and prioritizes it for keep-order scans.
  Evidence: `rust/crates/tidb-planner/src/physical_property.rs::need_keep_order` and the stale notes in `rust/crates/tidb-planner/src/task.rs`.
- Observation: Rust adds `Unknown` variants to both task and partition types, but pinned Go uses ordinary integer enum values and maps unknown task strings only through `String`'s default branch; partition exchange has no retained typed unknown variant.
  Evidence: pinned `task_type.go` and `physical_property.go::ToExchangeType` compared with the Rust enums.
- Observation: Rust's statistics API takes a skew ratio argument directly, whereas Go's package calls the package-level `ScaleNDVFunc` seam and has no extra method argument.
  Evidence: pinned `stats_info.go::Scale`/`ScaleByExpectCnt` and Rust `stats_info.rs` call sites.
- Observation: the existing aggregation attachment arms bypassed the already-implemented partial/final split to preserve ranges, but the pinned Go code uses that split except for its explicit double-read, root-filter, and index-merge gates.
  Evidence: pinned `pkg/planner/core/task.go::attach2Task4PhysicalStreamAgg`/`attach2Task4PhysicalHashAgg` and Rust `task.rs::attach_agg_over_cop`; removing the bypass made both aggregate pushdown regressions pass while retaining bounded scan ranges.
- Observation: an existing range test expected the raw pseudo less-than estimate, but pinned Go immediately raises that estimate through `adjustCountAfterAccess` when it is below the logical DataSource row count.
  Evidence: pinned `pkg/planner/core/stats.go:203-220`; the live test fixture has both logical and realtime counts of 100, so the source result is 100 rather than `100/3`.
- Observation: the broad planner and executor suites contain failures outside this package unit, including CTE physical-seed setup and unrelated pruning/ranger tests.
  Evidence: `cargo test -p tidb-planner --no-fail-fast` passed 819 of 824 active library tests and all 259 active transcreation tests, while the focused property/stats/aggregation tests pass; the failing symbols are recorded under Outcomes.

## Decision Log

- Decision: retain idiomatic Rust ownership while matching Go's observable value behavior; a Go pointer field maps to `Option<T>` or an owned clone where pointer identity is not part of the package contract.
  Rationale: Rust cannot safely mirror arbitrary Go pointer aliasing, but all package comparisons and planner decisions are defined by column identity, expression equality, field values, or presence.
  Date/Author: 2026-08-30 / Codex.
- Decision: remove only classifications that have no pinned Go package counterpart and migrate their consumers to the actual Go-shaped fields/results.
  Rationale: deleting a live planner distinction without replacing its source behavior would violate correctness; the user's requirement is behavioral parity, not merely fewer lines.
  Date/Author: 2026-08-30 / Codex.
- Decision: treat the complete pinned package inventory as the atomic claim, including its sole original test and Bazel build description.
  Rationale: repository policy requires one complete Go package as the minimum transcreation unit.
  Date/Author: 2026-08-30 / Codex.

## Outcomes & Retrospective

The complete pinned package inventory is represented in Rust. Logical properties, statistics version/group behavior, task values, complete column-bearing sort/partition items, partial/vector/index-join properties, hashing, cloning, schema/task helpers, and MPP exchanger decisions now follow the pinned source. Consumers no longer use a second task classification or reconstruct sort columns through a schema-only cache, and aggregation attachment uses Go's partial/final coprocessor gates.

Ready validation passed for the package unit: the focused property, statistics, aggregation, memory-accounting, compilation, formatting, Clippy, repository lint, and diff checks all exit zero. The broad planner library sweep still has five unrelated failures: `data_source_unique_index_keys_keep_nullability_and_pruning_semantics`, `union_all_child_topn_folds_the_offset_into_the_count`, `test_a_recursive_cte_without_a_union_is_refused`, `issue_40997_dnf_ranges_match_go`, and `lookup_pushdown_is_applied_only_without_keep_order`. An executor CTE test also reaches the already-named `LogicalCTE.DeriveStats: seed physical plan is nil` gap. Those failures are not hidden or weakened in this unit and remain candidates for subsequent complete package work.

## Context and Orientation

The pinned package contains `logical_property.go`, `physical_property.go`, `stats_info.go`, `task_type.go`, `physical_property_test.go`, and `BUILD.bazel`; it has no `doc.go`, generated input/output, fixtures, platform variants, benchmarks, examples, or fuzz targets. Rust's current counterparts are `rust/crates/tidb-planner/src/physical_property.rs`, `stats_info.rs`, and `task_type.rs`; `rust/crates/tidb-planner/src/lib.rs` exports them. Logical and physical planner operators consume these types under `rust/crates/tidb-planner/src/logical`, `physical`, `find_best_task`, `task.rs`, and `enforce.rs`. `tidb-expr::Schema` and `Column`, and `tidb-funcdep::FdSet`, provide the direct dependency behavior.

A physical property describes what a parent requires from a child: ordering, execution engine, row-count expectation, and MPP distribution. A logical property describes facts already derived for a logical plan. A functional dependency set records column equivalence and determination; MPP exchange elimination uses its equivalence closure.

## Plan of Work

Add `logical_property.rs` with all pinned fields and the zero-value constructor. Complete `StatsInfo` with `StatsVersion`, exact group lookup, source string/count/scale/limit behavior, and a Go-shaped NDV scaling seam. Complete `TaskType`'s pinned integer/string behavior without manufacturing planner semantics for unknown values.

In `physical_property.rs`, add the missing partial-order and vector fields, result types, source helper functions, equality/hash identity, cloning, memory accounting, schema checks, possible-child task enumeration, and exact exchanger enforcement. Remove `IndexOrderingRequirement` and any other property-module policy that is not present in the pinned package, replacing consumers with the corresponding Go physical-property match or partial-order state. Update stale comments and all exhaustive consumers.

Port the six-row `NeedMPPExchangeByEquivalence` source test without adding Rust-only behavioral expectations. Add narrow tests only for pinned methods whose prior absence could silently regress, such as limit stats, group NDV, partial-order priority, and exchanger enforcement.

## Concrete Steps

Run from `/Users/qiliu/projects/tidb`:

    cargo test --manifest-path rust/Cargo.toml -p tidb-planner physical_property --no-fail-fast
    cargo test --manifest-path rust/Cargo.toml -p tidb-planner stats_info --no-fail-fast
    cargo check --manifest-path rust/Cargo.toml -p tidb-planner -p tidb-executor
    cargo clippy --manifest-path rust/Cargo.toml -p tidb-planner -p tidb-executor --all-targets
    cargo fmt --manifest-path rust/Cargo.toml --all -- --check
    make lint
    git diff --check

Expected: focused tests and checks exit zero. Existing workspace warnings may remain, but touched code introduces no new clippy diagnostic.

## Validation and Acceptance

The exact pinned six-case functional-dependency table must produce the same exchange decisions. Additional acceptance checks exercise every completed source branch: task names, subset matching, schema coverage, child task enumeration, partial-order direction/item priority, hash identity changes for every source-hashed field, essential cloning exclusions, and exchanger decisions for Any, Broadcast, Single, Hash with equivalence, and ordered Hash without equivalence.

No Go source, import block, Bazel file, or module dependency is expected to change, so `make bazel_prepare` is not required. If the consumer audit reveals such a change, run the gate skill again and update this plan before validation.

## Idempotence and Recovery

Inspection, formatting, and validation commands are safe to rerun. All edits use `apply_patch`. Preserve unrelated user changes. If a type migration exposes a consumer that needs behavior from another incomplete package, implement only the dependency surface required by the pinned property package and record it here; do not add a fallback or parallel legacy representation.

## Artifacts and Notes

Pinned inventory:

    pkg/planner/property/BUILD.bazel
    pkg/planner/property/logical_property.go
    pkg/planner/property/physical_property.go
    pkg/planner/property/physical_property_test.go
    pkg/planner/property/stats_info.go
    pkg/planner/property/task_type.go

The Bazel target is a single Go library plus one Go test target. The Rust mapping remains in `tidb-planner` because its direct consumers already live there and splitting the package would create a dependency cycle.

## Interfaces and Dependencies

The completed Rust package exports `LogicalProperty`, `StatsInfo`, `GroupNdv`, `TaskType`, `SortItem`, `MppPartitionType`, `MppPartitionColumn`, `PhysicalProperty`, `PartialOrderInfo`, `PartialOrderMatchResult`, `IndexJoinRuntimeProp`, and the source helper functions. It depends on `tidb-expr` for columns/schema/expressions and `tidb-funcdep` for equivalence closure. It introduces no alternate planner or executor path.

Revision note: initial plan records the complete pinned inventory, current Rust gaps, dependency boundary, and Ready acceptance gates before implementation.

Revision note (2026-08-30): implementation and Ready evidence are complete; recorded the removed aggregation workaround, corrected source range-stat expectation, and unrelated broad-suite failures before commit/push.
