# Complete the pinned planner partial-index constraint package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. The pinned Go revision is `e2788410d8d696605e8cb002585877a063ccc909`.

## Purpose / Big Picture

The pinned `pkg/planner/core/partidx` package proves whether query filters imply a partial index's stored predicate. Rust carries partial-index metadata but has no owner for this proof, so it cannot make the same path-admission decision as Go. Completing the package gives the planner the same exact-match, range-implication, and plan-cache-safe `IS NOT NULL` decisions, ready for the access-path growth stage that owns partial-index pruning.

## Progress

- [x] (2026-08-30) Read and inventoried the complete pinned package: production `check_constraint.go` and `BUILD.bazel`; no tests, fixtures, generated artifacts, build/platform variants, benchmarks, fuzz targets, or examples.
- [x] (2026-08-30) Added one native Rust owner for every production branch and focused executable coverage.
- [x] (2026-08-30) Integrated partial-index pruning after logical predicate pushdown and before statistics derivation, including forced-path filtering, dynamic ranger policy, and ordinary physical-plan cache marking.
- [x] (2026-08-30) Ran the Ready validation gate and recorded the atomic package receipt.

## Surprises & Discoveries

- Observation: Rust has the same ranger primitives and datasource predicate/index metadata, but its `DataSourceAccessPath` list is intentionally empty until a still-incomplete ranger/statistics growth seam fills it.
  Evidence: `plan_builder.rs` fills `enumerated_paths` and asserts `possible_access_paths.is_empty()`; the pinned Go `CheckPartialIndexes` call runs only after the latter paths have been grown.
- Observation: the live ordinary planner grows ranges while enumerating `PossiblePath` inside `find_best_task`, so the source-faithful integration point is the post-optimization, pre-statistics datasource pass, not a second grown-path pipeline.
  Evidence: `find_best_task/dispatch.rs` detaches predicates and builds ranges inside its datasource candidate loop; `planner_bridge.rs` now runs `check_partial_index_paths` immediately before `recursive_derive_stats`.
- Observation: the broader ranger sweep has one pre-existing failure outside this package.
  Evidence: 58 ranger-filtered tests pass, while `ranger::go_cases::issue_40997_dnf_ranges_match_go` expects two three-column ranges but receives the one-column range `[["20210112","20210112"]]`; this change does not modify that case or its ranger implementation.

## Decision Log

- Decision: Port the package against the existing production ranger APIs and do not prune Rust's newborn `PossiblePath` list.
  Rationale: Go's implication proof runs immediately before range/statistics growth. Rust performs that growth during physical candidate enumeration, so pruning the candidate list immediately before statistics derivation preserves the same phase ordering without creating a second path pipeline.
  Date/Author: 2026-08-30 / Codex
- Decision: Carry `tidb_opt_prefix_index_single_scan` through the statement context and attach partial-index cache refusal to the selected ordinary index scan or point plan.
  Rationale: hard-coding the default would diverge for a live session override, and a cache-specific execution wrapper would violate the ordinary-plan execution architecture shared with Go.
  Date/Author: 2026-08-30 / Codex

## Outcomes & Retrospective

The pinned `pkg/planner/core/partidx` package is atomically complete against revision `e2788410d8d696605e8cb002585877a063ccc909`. Its complete inventory is production `check_constraint.go` and `BUILD.bazel`; there are no original tests, fixtures, generated inputs or outputs, build-tag/platform variants, benchmarks, fuzz targets, or examples. `tidb-planner::partidx` owns every source proof. The logical datasource invokes it after predicate pushdown and before statistics derivation, forced partial-index hints prune the same alternatives, the live ranger switch comes from the statement session, and cache refusal is attached to the ordinary index or point physical plan. Five focused owner/integration tests pass, executor/session and consuming-server compilation pass, formatting, `make lint`, and `git diff --check` pass. The broader ranger sweep has the separately recorded pre-existing `issue_40997_dnf_ranges_match_go` failure; 58 other ranger-filtered tests pass.

## Context and Orientation

Pinned `check_constraint.go` exports `CheckConstraints` and `AlwaysMeetConstraints`. It uses `pkg/util/ranger` to prove that comparison filters form a subset of a partial-index comparison range, and uses a deliberately narrow structural recursion to prove that a single `NOT(ISNULL(column))` predicate is always true for plan caching. Rust ranger lives under `rust/crates/tidb-planner/src/ranger`. The logical datasource is `rust/crates/tidb-planner/src/logical/data_source.rs`; its newborn and grown access-path types are in `rust/crates/tidb-planner/src/access_path.rs`.

## Plan of Work

Add `rust/crates/tidb-planner/src/partidx.rs` and export it from the planner crate. Translate exact multiset matching, comparison range union, `IS NOT NULL` range inspection, and the plan-cache structural proof without widening accepted expression shapes. Add unit tests that exercise empty predicates, duplicates, supported and unsupported shapes, implication in both directions, NULL admission, AND/OR recursion, and NULL-safe equality. Inspect the grown access-path owner before integrating; if that owner is absent, leave no disconnected replacement path and record the dependency precisely.

## Concrete Steps

Run from `rust/` unless stated otherwise:

    cargo test --locked -p tidb-planner --lib partidx -- --nocapture
    cargo test --locked -p tidb-planner --lib ranger -- --nocapture
    cargo check --locked -p tidb-server
    cargo fmt --all -- --check

Run `make lint` from repository root for the Ready gate, followed by `git diff --check`.

## Validation and Acceptance

Acceptance requires exact predicates to match as a multiset, a narrower filter range to imply a wider partial-index predicate but not the reverse, a filter range containing inclusive NULL to fail `IS NOT NULL`, and the plan-cache helper to accept only the pinned single-predicate shape with exactly the pinned AND/OR and comparison recursion. Ranger failures and unsupported shapes must fail closed.

## Idempotence and Recovery

All edits and validation commands are safe to repeat. If range tests fail, inspect the source ranger result rather than adding special cases. If the grown access-path seam is absent, document that dependency and continue with the next complete package; do not approximate pruning on newborn metadata.

## Milestones

Milestone one completes the standalone source behavior and its tests. Running the focused `partidx` test filter must pass every source branch.

Milestone two resolves production integration. The result must either invoke the owner at the same grown-path phase as Go or leave a documented, named dependency on the missing phase; it must not create a second path pipeline.

Milestone three is complete: focused tests, consuming server check, formatting, lint, and diff checks pass.

## Interfaces and Dependencies

`tidb-planner::partidx` exposes `check_constraints(opt_prefix_index_single_scan: bool, pre_predicates: &[Expression], filters: &[Expression]) -> bool` and `always_meet_constraints(pre_predicates: &[Expression], filters: &[Expression]) -> bool`. The boolean is the only source `RangerContext` value read by this package. It depends only on `tidb-expr` expression types and the existing `tidb-planner::ranger` detacher, column-range builder, and range-union implementation, matching the pinned Go package's production dependencies.
