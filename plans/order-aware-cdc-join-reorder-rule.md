# Add an order-aware logical join reorder rule on top of CD-C joinorder

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

After this change, TiDB should be able to recognize a join group that has a useful ordering requirement above it, identify the leaf table whose index can preserve that order, and feed that preference into the new CD-C based join reorder implementation as a separate logical rule. The observable effect is that the planner can keep the ordered table as the leading seed of join reorder without mixing that logic into the generic `JoinReOrderSolver`.

## Progress

- [x] (2026-03-25 08:23Z) Confirmed the current working branch is effectively `master`, so the work starts from a clean baseline.
- [x] (2026-03-25 08:23Z) Confirmed the target should reuse `pkg/planner/core/joinorder`'s new `extractJoinGroup` and outer-join conflict handling rather than the old reorder path.
- [x] (2026-03-25 09:02Z) Implemented a new logical rule in `pkg/planner/core/rule_order_aware_join_reorder.go` and a CD-C helper in `pkg/planner/core/joinorder/ordered_leading.go`.
- [x] (2026-03-25 09:04Z) Registered the new rule before `JoinReOrderSolver` and propagated `TopN` ordering columns into `LogicalJoin`.
- [x] (2026-03-25 09:10Z) Added a dedicated `order_aware_join_reorder_suite` with one `through_sel=0` and one `through_sel=1` indexed ORDER BY/LIMIT case, instead of churning the existing CD-C golden suite.
- [x] (2026-03-25 09:18Z) Ran `make bazel_prepare`, compile checks, and the new targeted order-aware rule tests. `make lint` was attempted but blocked by the repository's current `revive` install target.
- [x] (2026-03-25 09:08Z) Verified that the pre-existing `TestCDCJoinReorder` / `TestJoinReorderPushSelection` golden files already drift from current planner output on this workspace, so they were not used as the acceptance gate for this feature.

## Surprises & Discoveries

- Observation: current `master` already contains the new CD-C join reorder package at `pkg/planner/core/joinorder`, including its own `extractJoinGroup`, `selConds`, and outer-join conflict detector.
  Evidence: `pkg/planner/core/joinorder/join_order.go` and `pkg/planner/core/joinorder/conflict_detector.go`.

- Observation: the old `JoinReOrderSolver` still exists and delegates to `joinorder.Optimize(...)` only when `tidb_opt_advanced_join_reorder` is enabled and the threshold path allows it.
  Evidence: `pkg/planner/core/rule_join_reorder.go`.

- Observation: `make lint` currently fails before reaching code-specific linting because `Makefile` installs `github.com/mgechev/revive@v1.2.1`, which is not an installable package path.
  Evidence: `make lint` exits from target `tools/bin/revive` with `module ... found, but does not contain package github.com/mgechev/revive`.

## Decision Log

- Decision: implement the feature as a separate logical rule that injects an internal leading preference, instead of embedding order-aware cost logic into `rule_join_reorder.go`.
  Rationale: the user explicitly asked for a new logical rule, and the CD-C path already knows how to honor leading hints while preserving outer-join legality.
  Date/Author: 2026-03-25 / Codex

- Decision: reuse the CD-C `extractJoinGroup` traversal and selection-condition capture instead of duplicating the old core reorder data structures.
  Rationale: this keeps outer-join legality and selection-through-join-group behavior aligned with the new implementation.
  Date/Author: 2026-03-25 / Codex

## Outcomes & Retrospective

The new rule now annotates an internal ordered leading preference without changing the generic join reorder implementation. The behavior is exercised through a dedicated `order_aware_join_reorder_suite`, which keeps the diff focused on the new indexed ORDER BY/LIMIT cases instead of re-recording unrelated legacy CDC golden output. The main remaining gaps are repository-level lint completion and broader legacy CDC golden validation: `make lint` is blocked by the current revive install command, and the pre-existing `TestCDCJoinReorder` / `TestJoinReorderPushSelection` expectations are already stale in this workspace.

## Context and Orientation

The existing optimizer pipeline lives in `pkg/planner/core/optimizer.go`. The legacy logical reorder rule is `pkg/planner/core/rule_join_reorder.go`. The newer CD-C based join reorder implementation lives under `pkg/planner/core/joinorder/`; its `extractJoinGroup` function collects a join subtree into a `joinGroup`, including vertices, user leading hints, join-method hints, and `Selection` conditions that were looked through during extraction.

The new rule should not replace the CD-C reorder algorithm. Instead, it should compute whether a join group has an ordered leaf worth preserving, and if so, annotate that group so that the later CD-C reorder phase starts from that ordered leaf. In this repository, the existing mechanism for “start from this leaf first” is `LEADING`, already consumed by `joinorder.buildJoinByHint`.

## Plan of Work

Add one new logical rule file under `pkg/planner/core/` and small helper(s) under `pkg/planner/core/joinorder/` as needed. The joinorder helper should expose only the minimal surface needed by the new rule: inspect the same extracted join group that CD-C uses, discover whether a single vertex matches the ORDER BY / TopN requirement with compatible equality predicates, and attach an internal leading preference to that group root when there is no user-provided leading hint already.

The new rule will run before `JoinReOrderSolver` in `pkg/planner/core/optimizer.go`. It should carry order/filter context down the plan tree, especially through `TopN`, `Selection`, and other input-order-preserving unary nodes, so it can evaluate the join group with the correct ORDER BY columns and single-table equality filters. It must stay conservative for correctness: skip groups that already carry explicit leading hints, ambiguous table aliases, or ordering expressions that are not simple columns.

Tests should extend planner rule tests, ideally alongside the existing CD-C join reorder suite, so the new rule is exercised on the same implementation path and can verify both ordering-aware inner joins and legality around outer joins.

## Concrete Steps

Work from repository root:

    sed -n '1,260p' pkg/planner/core/joinorder/join_order.go
    sed -n '1,260p' pkg/planner/core/rule_join_reorder.go
    sed -n '1,220p' pkg/planner/core/casetest/rule/rule_cdc_join_reorder_test.go

Implement the new rule and helpers, then run:

    make bazel_prepare
    go test ./pkg/planner/core/... -run 'TestCDCJoinReorder|TestJoinReorderPushSelection'

If `make bazel_prepare` changes Bazel metadata, keep those changes in the working tree.

## Validation and Acceptance

Acceptance means:

1. the new logical rule is registered and only affects the intended path;
2. when an ORDER BY / LIMIT above a join group can be preserved by one leaf table, that leaf is annotated as the leading seed for the later CD-C reorder;
3. existing CD-C join reorder tests still pass, and new regression coverage demonstrates the ordered-leaf preference without breaking outer-join legality.

## Idempotence and Recovery

The code edits are ordinary source changes and safe to reapply. `make bazel_prepare` is expected to be rerunnable. If the new rule proves too invasive, the recovery path is to remove the new rule from `optimizer.go` and revert the helper file(s), leaving existing join reorder untouched.

## Artifacts and Notes

Artifacts produced in this turn:

- diff of the new logical rule and joinorder helper;
- targeted `go test` output for planner rule tests;
- `make bazel_prepare` output showing Bazel metadata regeneration for the new files;
- `make lint` failure evidence showing the revive-install blocker.

## Interfaces and Dependencies

The new rule will implement:

    type OrderAwareJoinReorder struct {}

in `pkg/planner/core`, satisfying `base.LogicalOptRule`.

The joinorder helper is expected to expose a minimal function shaped like:

    func AnnotateOrderedLeading(root base.LogicalPlan, ordering [][]*expression.Column, parentFilters []expression.Expression) bool

or an equivalent small API that operates on the CD-C `joinGroup`.

Changed on 2026-03-25 by Codex: initialized the ExecPlan, then updated it with implementation status, validation evidence, and the current lint blocker.
