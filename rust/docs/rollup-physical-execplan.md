# Execute ROLLUP through physical Expand

This is a living ExecPlan maintained under the repository root PLANS.md. The goal is successful original ROLLUP SQL tests through the common physical executor, not removal of a refusal or a complete Go package transcreation claim.

## Purpose / Big Picture

GROUP BY WITH ROLLUP must return ordinary groups, subtotals, and a grand total, including when a window runs above those rows. The current window suite has 47 passing tests and one failing window_over_rollup. Implementing Expand should also address the original non-window ROLLUP failures without an AST execution shortcut.

## Progress

- [x] Inspected current HEAD 9f9a9198bc and reproduced merged window suite: 47 passed, 1 failed, /tmp/window-merged-rollup.log.
- [x] Identified four admission refusals in driver/planner_bridge.rs and absence of PhysicalExpand/ExpandExec.
- [x] Compared pinned Go physical Expand and serial ExpandExec behavior.
- [x] Added executor regression; /tmp/expand-exec-red.log fails at the unimplemented executor, /tmp/expand-exec-green.log passes after implementation.
- [x] Three executor tests pass in /tmp/expand-exec-boundaries.log: multi-batch/reopen/input preservation, empty/zero-width virtual rows, memory quota and close accounting.
- [x] PhysicalExpand is wired through task candidates (cop single/multi, MPP, root), task attachment, index resolution, cached parameters, correlated-column traversal, explain, and executor binding. Sort and MPP partition refusals follow Go. The generic coster sums child costs, matching inherited Go base cost; this is not a TiFlash wire-execution completion claim.
- [x] Fixed UniqueID 6: SELECT projections had not called replace_grouping_func/implicit_project_grouping_set_cols; wired both projection passes to match Go buildProjection. Expand builder now consumes bound LevelExprs positions rather than repeating ID resolution after physical projection elimination.
- [x] GROUPING construction now resolves user arguments against BlockExpand, installs marks/mode and evaluates its generated GID. Planner regression red /tmp/grouping-planner-red.log and green /tmp/grouping-planner-green3.log. Three construction/validation regressions pass, including NumericSet and argument order.
- [x] Original four ROLLUP tests pass /tmp/rollup-final-focused.log, including all window assertions, HAVING, derived expressions, empty input, duplicate group sets, zero-argument GROUPING and the index-join fence.
- [x] Go probes proved two historical test-contract errors: unordered ROLLUP changes row order across 12 identical executions (/tmp/rollup-order-go.out); derived-expression HAVING returns 1054 (/tmp/grouping-derived-having-go.out), while its projected alias works (/tmp/grouping-alias-having-go.out). Tests now preserve the full row multiset or exact source error, and add the valid alias case.
- [x] Ready checks pass: ROLLUP 4, planner aggregation 37, planner Expand 17, executor expand filter 4, windows 48, session integration 310, expression grouping 4, ONLY_FULL_GROUP_BY 8, and make lint. Updated BLOCKER_RESOLUTION_REPORT.zh-CN.md.
- [x] Integrated remote d682d96522; only the report insertion conflicted and both records were retained. Merged ROLLUP 4, windows 48, integration 310 and lint pass. Full session lib is 1592 pass / 111 fail / 209 ignored; the larger goal remains open.
- [x] Independently committed the ROLLUP category and updated the final verification receipt. Publish by normal fast-forward push; the remote ref is the authoritative publication receipt.

## Context and Orientation

Work in /tmp/tidb-hparser-current. The Go oracle is /tmp/tidb-go-master-oracle at fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85. Rust environment is RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432. No assertions or source-size gates may be weakened.

rust/crates/tidb-planner/src/plan_builder/expand.rs creates LogicalExpand. Its logical operator stores LevelExprs: one expression list for each grouping level. A level projects ordinary input values, NULL for omitted grouping columns, and generated grouping identifiers. Existing logical processing must be verified, not assumed complete. rust/crates/tidb-planner/src/logical/rule_resolve_expand.rs generates those level projections. PhysicalPlan in physical/mod.rs has no Expand variant. driver/planner_bridge.rs rejects ROLLUP at four query entry points. driver/physical_builder.rs has no Expand arm.

Go pkg/planner/core/operator/physicalop/physical_expand.go carries LevelExprs and resolves their input references. Go pkg/executor/builder.go::buildExpand creates one EvaluatorSuite per level with column swapping disabled. pkg/executor/expand.go caches one input chunk and evaluates each level against the same input before reading the next chunk. Aggregation above Expand computes totals; Expand itself does not aggregate.

## Surprises & Discoveries

The older rollup-expander-gap.md calls the logical half complete and proposes run_rollup_aggregate. Neither claim is sufficient. Go uses the common aggregate over expanded rows. Swapping projection columns corrupts the cached input for subsequent levels, so EvaluatorSuite must be constructed with avoid_column_evaluator=true.

## Decision Log

Use a genuine physical Expand operator and the existing serial evaluator rather than an AST-specific ROLLUP aggregator. This matches the pinned Go execution order and composes with windows, HAVING, DISTINCT aggregates, and subqueries. The four refusals were removed locally only after the physical path compiled, to expose actual integration failures. Partial implementation is WIP and must not be pushed or reported as fixed.

## Plan of Work

Add a sibling physical module for Expand to avoid enlarging already oversized source files unnecessarily. Carry level expressions, grouping metadata, schema, child property and stats. Wire exhaustive physical enum visitors, task dispatch, index binding, explain formatting, and cost handling by consulting pinned Go. Do not fabricate sort preservation: each input chunk is repeated across levels, so it does not preserve arbitrary global order.

Add rust/crates/tidb-executor/src/expand.rs using ExecutorMeta, one child, a cached Chunk, and an evaluator per level. Reset level position on open. Read child only after all levels of the cached chunk have been emitted. Preserve virtual row counts for zero-width outputs. Match existing memory accounting and error propagation conventions; inspect StatementMemory integration before coding. Bind from the real PhysicalExpand in driver/physical_builder.rs.

Verify GROUPING() uses generated identifiers through aggregate and projection schemas, duplicate grouping expressions remain distinct where Go requires, and empty input matches the oracle. Original tests_window::aggregates::window_over_rollup exercises subtotal windows, GROUPING partitioning and nested aggregates. Search original session ROLLUP tests and run them all, not only this window case.

## Concrete Steps

From /tmp/tidb-hparser-current, prefix Cargo commands with the Rust environment above:

    cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib window_over_rollup -- --nocapture
    cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib rollup -- --nocapture
    cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_window
    cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
    make lint
    git diff --check

For Go probes, start its binary with --store=unistore, a dedicated temporary --path, --host=127.0.0.1 -P 14841 --status=14842. Capture SQL, stdout, errors and metadata. Stop only the exact process started for the probe.

## Validation and Acceptance

The original window_over_rollup must return seven rows for four (a,b) groups: four details, two a subtotals, one grand total. ROW_NUMBER follows the window order across all seven rows. SUM(SUM(v)) OVER (PARTITION BY a) includes subtotal rows. Real data NULLs must remain distinguishable from generated NULLs through GROUPING. Test multi-chunk input, duplicate group expressions, empty input, HAVING, LIMIT, and nested query use. A green focused executor test alone is not completion. All original rollup cases and required shared planner/executor tests must be verified before reporting success.

## Idempotence and Recovery

Run tests without recording new golden outputs. Preserve other authors' changes. Keep regressions red until behavior is implemented; do not bypass admission checks in production as a standalone fix. Git worktree edits may remain WIP across continuations, with exact state recorded here.

## Artifacts and Notes

/tmp/window-merged-rollup.log records the original ROLLUP failure after the preceding window aggregate fix. /tmp/rollup-merged-tests_window.log records all 48 window tests passing after this implementation.

## Interfaces and Dependencies

PhysicalExpand should hold BasePhysicalPlan and Vec<Vec<Expression>> level expressions plus Go-required grouping metadata. ExpandExec should implement the existing Executor trait and consume a boxed child using EvaluatorSuite; do not create a second aggregation interface. Check all enum matches through compiler diagnostics and existing physical walker tests.

## Outcomes & Retrospective

Serial executor and physical path are implemented. GROUPING's missing planner construction was distinct from its already-present evaluator. The old only-full-group-by aggregate detector also treated GROUPING as an aggregate; it now follows Go's AggregateFuncExpr/ANY_VALUE detection, preserving 1111 instead of an earlier incorrect 1140. Scoped Ready checks and merged-tree verification pass. The full session suite still has 111 failures, including 3 newly observed global-state failures that pass individually. Go probe process has stopped. The broader all-failed-cases goal remains unchanged.
