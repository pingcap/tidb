# Complete pinned planner rule packages without Rust-only behavior

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. The pinned Go revision is `e2788410d8d696605e8cb002585877a063ccc909`.

## Purpose / Big Picture

Rust currently exposes Go's logical-rule list but several entries are missing, narrowed, or implemented through duplicate helper paths. The observable goal is that the ordinary Rust planner performs the same logical rewrites as the pinned Go planner, with the same plan shapes and SQL results, and contains no Rust-only optimizer policies. Package completion is claimed only after every production source, original test/support artifact, and build artifact in one Go package has an inventory and validation receipt.

## Progress

- [x] (2026-08-29) Inventoried pinned `pkg/planner/core/rule` and its nested, separate Go package `pkg/planner/core/rule/util`.
- [x] (2026-08-29) Wired the static `PartitionProcessor` rule into the ordinary logical and physical execution path; committed and pushed as `d6285efd11`.
- [ ] Complete `pkg/planner/core/rule/util` as the first atomic package (completed: centralized expression replacement, column-set tests, nullable-key max-one-row behavior, unique-index key derivation, flag hook, and iterative key-info portal; removed duplicate CTE/projection/index-key bodies; remaining: the two simplification hooks depend on incomplete parent-package predicate simplification).
- [ ] Audit every direct artifact in `pkg/planner/core/rule`, mapping production symbols and original tests to Rust owners.
- [ ] Implement dependency-closed missing rule bodies; when a body depends on an incomplete Go package, complete that dependency package before claiming this package.
- [x] (2026-08-29) Implemented pinned `ConstantPropagationSolver` with Go's preorder traversal, join-type sides, projection column rewrite, parent selection shape, and hard-coded unchanged flag.
- [x] (2026-08-29) Replaced the disconnected max/min classifier with pinned `MaxMinEliminator`: recursive CTE boundary, eligibility gates, nullable filtering, sort/limit construction, indexed multi-aggregate splitting, cloned subplans, and cartesian joins.
- [ ] Run the Ready validation profile and record the complete package receipt.

## Surprises & Discoveries

- Observation: Go's static partition copies retain the same numeric plan ID, because the task memo is owned by each logical plan object rather than keyed globally by ID.
  Evidence: keying Rust's memo by numeric ID reused partition p1's physical task for p2; object-identity keys and a focused regression corrected it.

- Observation: Go's `LogicalUnionAll.PruneColumns` inserts an identity projection when a child retains a condition-only column.
  Evidence: without the projection, a two-column `PartitionUnion` child emitted three columns and hash join attempted to append VARCHAR data into an INT chunk column.

- Observation: the nested directory `pkg/planner/core/rule/util` is a separate Go package and therefore a smaller valid atomic completion unit than its parent directory.
  Evidence: it has its own `package util` declaration and `BUILD.bazel` `go_library` target.

- Observation: Rust's selection max-one-row check used only `PKOrUK`, while pinned `CheckMaxOneRowCond` checks `PKOrUK` and `NullableUK`.
  Evidence: the centralized helper and focused regression now accept a fully equality-bound nullable unique key and reject partial/empty key bindings.

- Observation: postorder constant propagation is not equivalent to Go's preorder rule for nested joins.
  Evidence: a postorder walk would expose a newly created child-join Selection to its parent join in the same pass; the explicit-stack implementation snapshots candidates on entry and a regression proves the parent remains unchanged.

## Decision Log

- Decision: Close `pkg/planner/core/rule/util` before continuing the parent `rule` package.
  Rationale: repository policy requires whole Go packages as the minimum claim. The helper package is dependency-closed and lets duplicate Rust implementations be consolidated before more rule bodies consume them.
  Date/Author: 2026-08-29 / Codex

- Decision: Keep Go hooks as direct Rust functions rather than mutable process-global function variables.
  Rationale: the Go variables break an import cycle; Rust modules in one crate have no such cycle. Call behavior and signatures remain centralized without introducing mutable global state that Go does not behaviorally expose.
  Date/Author: 2026-08-29 / Codex

## Outcomes & Retrospective

Work is in progress. Static partition planning is integrated and pushed, but neither the parent `rule` package nor the nested `rule/util` package is yet claimed complete.

## Context and Orientation

The parent pinned package contains `BUILD.bazel`, thirteen production `.go` files, four original `_test.go` files, and the nested `util` package. The Rust rule driver is `rust/crates/tidb-planner/src/logical/rule.rs`; tree rewrites are in `logical/rewrite.rs`; rule-specific bodies are `logical/rule_*.rs`. Executor-owned catalog and partition-expression access is in `rust/crates/tidb-executor/src/driver/planner_bridge.rs`.

The nested pinned `pkg/planner/core/rule/util` package contains exactly `misc.go` and `BUILD.bazel`, with no package-local tests, fixtures, generated files, build-tag variants, benchmarks, fuzz targets, or examples. Its behaviors are expression/column replacement, outer/inner column-set tests, maximum-one-row key tests, unique-index key derivation, three import-cycle hooks, and bottom-up key-info traversal.

## Plan of Work

First add one Rust owner module for the complete nested helper package. Move the existing CTE replacement and projection replacement bodies into it, route selection key tests and data-source/index key derivation through it, and retain the existing iterative bottom-up key-info traversal as the Rust ownership-safe form of Go's recursive portal. Add direct tests for every helper branch because the pinned package has no original tests.

Then build a source-to-owner inventory for every direct parent-package file. Implement missing rules in Go execution order, reading the complete pinned Go file before each edit. Remove stale narrowing documentation and duplicate helper paths as their Go behavior becomes available. Validate each rule with its original Go test behavior through the closest Rust planner/session surface, but do not claim the parent package until its complete artifact inventory and Ready gates pass.

## Concrete Steps

Run from repository root unless stated otherwise:

    git show e2788410d8d696605e8cb002585877a063ccc909:pkg/planner/core/rule/util/misc.go
    cargo test --locked -p tidb-planner --lib rule_util -- --nocapture
    cargo check --locked -p tidb-session
    git diff --check

During WIP, use focused tests only. Before a package-complete claim, follow `.agents/skills/tidb-verify-profile/SKILL.md` Ready profile, including `make lint` for code changes.

## Validation and Acceptance

The nested util package is accepted when every pinned production symbol has one Rust owner, duplicate local implementations are removed, focused helper and consuming-rule tests pass, and its `BUILD.bazel` inventory is recorded. The parent rule package is accepted only when every rule selected by pinned optimizer flags runs the Go body or is excluded by the same Go condition, all four original Go test artifacts have mapped executable coverage, and ordinary SQL plans/results match the pinned behaviors.

## Idempotence and Recovery

Inventory and focused validation commands are read-only and safe to repeat. All edits are made with `apply_patch`. Existing user changes are preserved; no reset or checkout command is used. A failed focused test is fixed at the owning helper/rule rather than bypassed with an alternate execution path.

## Artifacts and Notes

The static partition slice is commit `d6285efd11` on `origin/hparser-integration`. Its focused evidence includes five `tests_partition_processor` cases, the static partition ANALYZE case, two planner regressions for union projection repair and plan-object memo identity, `cargo check --locked -p tidb-session`, and `git diff --check`.

## Interfaces and Dependencies

`tidb_expr::Expression`, `Column`, and `Schema` supply Go expression and schema behavior. `plan_builder::catalog::SourceIndex` and `logical::DataSourceColumn` carry the index/table metadata needed by `CheckIndexCanBeKey`. `logical::fold::fold_owned` is the iterative ownership-safe equivalent of Go's recursive `BuildKeyInfoPortal`. No new external dependency is required.

Revision note (2026-08-29): created after the static partition processor integration exposed duplicated and narrowed `rule/util` helpers; establishes nested `util` as the next atomic package.
