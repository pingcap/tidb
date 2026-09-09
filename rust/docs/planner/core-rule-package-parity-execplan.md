# Complete pinned planner rule packages without Rust-only behavior

## Current range-quota integration batch (2026-09-08)

The Rust MAX/MIN rule now uses the session range quota and Go's hint-filtered
logical access paths. Ranger quota events share statement warning/cache state,
including recursive/DNF candidates; DNF memory accounting includes actual range
payloads. Prepared SELECT/DML preserve planning warning order and execute rejected
cache candidates once without inserting or rebuilding them. Cache hits do not
replay planning warnings; fix-control 49736 retains the forced-cache behavior.

Go authority: `f5cf8f6337612c6ae51fb6e384e4bb3469dde680`. Complete package reading
inventories and individual red/green evidence remain below and in the ranger,
variable and session inventories. This batch fixes these integration gaps; it
**does not claim complete planner/statistics/optimizer or core-rule parity**.

Ready checks executed: `make lint` passed; 64 ranger, 5 MAX/MIN, 2 shared-context,
6 SQL regression and 2 logical integration tests passed. The statement snapshot
regression passed within the expanded range filter. Expanded range tests report
36 passed, 11 failed and 2 ignored; all 11 failures have identical panic payloads
on HEAD (29 passed, 11 failed, 2 ignored). Prepared-cache module has 30 passes and
one pre-existing HashAgg panic, separately reproduced on HEAD. These failures
are limitations, not passing checks. Exact commands/logs appear in the checkpoints.

Remaining audit work includes other ranger callers, physical cacheability refusal
paths, legacy skip-cache state, and the full remaining planner/statistics packages.
The chronological checkpoints below document intermediate states; this summary
supersedes their pending statements for the changes and checks listed above.


This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. The pinned Go revision is `e2788410d8d696605e8cb002585877a063ccc909`.

## Purpose / Big Picture

Rust currently exposes Go's logical-rule list but several entries are missing, narrowed, or implemented through duplicate helper paths. The observable goal is that the ordinary Rust planner performs the same logical rewrites as the pinned Go planner, with the same plan shapes and SQL results, and contains no Rust-only optimizer policies. Package completion is claimed only after every production source, original test/support artifact, and build artifact in one Go package has an inventory and validation receipt.

## Progress

- [x] (2026-08-29) Inventoried pinned `pkg/planner/core/rule` and its nested, separate Go package `pkg/planner/core/rule/util`.
- [x] (2026-08-29) Wired the static `PartitionProcessor` rule into the ordinary logical and physical execution path; committed and pushed as `d6285efd11`.
- [x] (2026-08-30) Completed `pkg/planner/core/rule/util` as the first atomic package: every artifact and production symbol in pinned `misc.go` and `BUILD.bazel` has one Rust owner; centralized expression replacement, column-set tests, nullable-key max-one-row behavior, metadata-based unique-index key derivation, flag hook, iterative key-info portal, and both predicate-simplification hook signatures including validity-filter forwarding are integrated; duplicate CTE/projection/index-key bodies are removed; locked focused tests, the consuming session build, `make lint`, and `git diff --check` pass.
- [x] (2026-08-30) Audited every direct artifact in pinned `pkg/planner/core/rule`: `BUILD.bazel`, fourteen production files, and three original test files; no fixtures, generated inputs/outputs, build-tag/platform variants, fuzz targets, or examples exist.
- [x] (2026-08-30) Implemented the dependency-closed rule bodies and mapped every original test/benchmark behavior to an executable Rust owner. The last stale partition-pruning placeholders were replaced by production-path datetime/timestamp and thirteen-predicate range matrices plus a real five-size RANGE COLUMNS benchmark target.
- [x] (2026-08-30) Ran the final Ready validation gates and recorded the atomic `pkg/planner/core/rule` package receipt.
- [x] (2026-08-29) Implemented pinned `ConstantPropagationSolver` with Go's preorder traversal, join-type sides, projection column rewrite, parent selection shape, and hard-coded unchanged flag.
- [x] (2026-08-29) Replaced the disconnected max/min classifier with pinned `MaxMinEliminator`: recursive CTE boundary, eligibility gates, nullable filtering, sort/limit construction, indexed multi-aggregate splitting, cloned subplans, and cartesian joins.
- [x] (2026-08-29) Replaced the narrowed predicate helper and planner-local join-equivalence solver with the registered pinned `PredicateSimplification` rule and expression-owned general propagation: PushDownNot, logical-constant short circuit, IN/NE merge, redundant OR removal, impossible OR-branch pruning, DataSource recursion, plan-cache skip reasons, and session-controlled join-key retention.
- [x] (2026-08-29) Integrated pinned `PropConstForOuterJoin`: preserved-side constants, transitive equality classes, null-sensitive modes, inner `IS NOT NULL` derivation, recursive safe replacement, `allJoinLeaf`, and join-type-specific validity filters now use the ordinary join executor path.
- [x] (2026-08-29) Replaced the absent/partial outer-join anti pattern with pinned `OuterJoinToSemiJoin`: recursive selection discovery, identity-projection traversal, right-join normalization, join-predicate and NOT NULL witnesses, typed NULL restoration, and Apply/NullEQ refusal.
- [x] (2026-08-30) Replaced the detached `SchemaNode` model of `rule_column_pruning.go` with Go's post-pruning invariant over the real logical-plan tree, wired it into `ColumnPruner`, and removed the standalone model and its synthetic difftests.
- [x] (2026-08-30) Restored `PartitionProcessor.rewriteDataSource`'s `LogicalUnionScan` branch: static multi-partition rewrites now place one copied UnionScan above each partition DataSource below PartitionUnionAll, and every processed static partition source marks the plan cache with Go's reason.
- [x] (2026-08-30) Restored `PartitionProcessor.prune`'s pre-dispatch behavior: simplify both `PushedDownConds` and `AllConds` with the shared predicate simplifier, keep the two lists synchronized, and fold constant false/NULL to TableDual before invoking the partition-type pruner.
- [x] (2026-08-30) Filled `getUsedKeyPartitions`' missing non-point branch: a one-column integer KEY partition now enumerates inclusive/exclusive short ranges through the shared KEY write router when Go's range-width gate permits it, with full-range fallback at the partition-count boundary.
- [x] (2026-08-30) Removed RANGE pruning's deliberate over-inclusive `GT` boundary: an exclusive integer low endpoint now applies pinned `PruneUseBinarySearch`'s wrapping `C+1` before partition-bound comparison.
- [x] (2026-08-30) Replaced RANGE COLUMNS' point-only/full-scan fallback with pinned `multiColumnRangeColumnsPruner` behavior: normalized ranger tuple intervals now use Go's lower/upper partition-bound searches, endpoint exclusion, prefix rules, MAXVALUE handling, and per-column collations.
- [x] (2026-08-30) Preserved each ranger interval's `Collators` through the ordinary planner bridge into RANGE COLUMNS `minCmp`/`maxCmp`, and removed the range-only pruning entry so production pruning has one planner-owned path.
- [x] (2026-08-30) Replaced HASH pruning's raw-datum shortcut with pinned `getUsedHashPartitions`: points evaluate the complete partition expression, non-point enumeration is limited to a bare integer column, and BIT(flen) ranges use Go's finite-domain cap.
- [x] (2026-08-30) Replaced LIST pruning's raw-dependency/default shortcut with pinned `findUsedListPartitions`: point ranges evaluate the complete partition expression and locate only the exact, NULL, or DEFAULT owner; non-point pruning is limited to a bare column; and evaluation errors propagate to planning.
- [x] (2026-08-30) Restored scalar RANGE partition-function pruning from pinned `MakePartitionByFnCol`/`RangePruner`: exact points evaluate every valid partition expression, supported strict and non-strict monotone functions transform ranger endpoints (including relaxed inequalities), and non-monotone intervals retain the full range.
- [x] (2026-08-30) Aligned LIST COLUMNS tuple lookup with pinned `ForListColumnPruning`: complete points now choose only their collation-aware explicit owner (or DEFAULT for a gap), while partial points and intervals retain DEFAULT and compare endpoints with each partition column's collation.
- [x] (2026-08-30) Replaced LIST COLUMNS' composite-range shortcut with pinned `listPartitionPruner`: each scalar predicate is detached against its single referenced partition column, CNF intersects per-definition tuple-group identities, DNF unions them, constants preserve full/empty semantics, and DEFAULT uses Go's special `-1` group. The range-only LIST COLUMNS branch was removed so ordinary planning has one behavior owner.
- [x] (2026-08-30) Restored online DROP PARTITION overlap handling: cluster metadata retains the model's `GetOverlappingDroppingPartitionIdx` result, every static child is remapped/skipped and deduplicated before construction, explicit names are checked against the replacement, and LIST COLUMNS locations use Go's special group before CNF/DNF combination.
- [x] (2026-08-30) Restored `resolveOptimizeHint`'s INDEX_MERGE partition branch: the builder retains each `HintedIndex.Partitions` list and pinned `Restore2IndexHint` text, each static child keeps only matching/global hints before index pruning, and the parent emits Go's unknown-partition warning over the children actually constructed.
- [x] (2026-08-30) Restored `buildDataSource`'s named-partition validation before access-path enumeration: the catalog now preserves whether `GetPartitionInfo()` is non-nil, named partitions match definitions case-insensitively, unknown names return 1735 with Go's lower-cased argument, and a partition clause on a nonpartitioned table returns 1747.
- [x] (2026-08-30) Completed `resolveOptimizeHint`'s ordinary index-hint dependency: table-syntax and comment-style `USE`/`FORCE`/`IGNORE INDEX` share ordinary public-path filtering; query-block/database/alias matching, unique prefix resolution, 1176/1815 warnings, partition-scoped re-resolution, global-index removal, PRIMARY, empty USE, ignored/undetermined fallbacks, forced identities, and ORDER/NO_ORDER candidate admission are preserved; pinned `indexIsAvailableByHints` owns fast point/batch-point admission and the broader 685-line executor resolver is deleted; lookup-pushdown preserves positive/negative conflicts, all ten pinned support gates and warning texts, hint-only/affinity-force/force policy, keep-order refusal, metadata/session propagation, and execution through the ordinary physical reader; `PhysicalLocalIndexLookUp` now owns Go's cloned table subtree, fresh plan IDs, zeroed table-side statistics, handle offsets, and both flattened plan representations.
- [x] (2026-08-29) Replaced the static-only index-pruning shortcuts with the pinned `rule_prune_indexes.go` branches reachable before stats derivation: forced-path bypass, INDEX_MERGE preservation/preference, fix-control 52869, partial-index affected-column precheck, deterministic scoring, default-ten selection, and the exact safety fallback. Removed master-only clustered-prefix/internal-scoring gap stubs that are absent from the pinned tree.
- [x] (2026-08-29) Inventoried the complete pinned `pkg/planner/core/joinorder` dependency: four production files, two original test/benchmark files, and `BUILD.bazel`. Removed Rust's disconnected `ProjectionInlineShape` adapter, derived benchmark assertions, and partial/ignored test catalogs; none was a Go planner type or executable original test path.
- [x] (2026-08-29) Completed and registered `pkg/planner/core/joinorder`: real-expression substitution and equality alignment, CD-C graph/conflict rules, statistics and cumulative cost, DP and multi-start greedy enumeration, Cartesian/bushy recovery, ordered-leading index proof, nested LEADING construction, derived-table preservation, method-hint restoration, warning plumbing, and the complete pinned artifact inventory.
- [x] (2026-08-29) Ported the complete pinned `OrderAwareJoinReorder` source behavior without registering a half-pipeline: TopN/Sort order extraction, Projection/Limit/Selection propagation, mutable-selection fence, carrier-only recursion, exact DataSource index proof, and internal LEADING annotation. Focused tests cover the forward-column contract and an indexed carrier below TopN.
- [x] (2026-08-29) Read, inventoried, and ported all four pinned legacy join-reorder production sources (`rule_join_reorder.go`, DP, greedy, and projection-inline). `JoinReOrderSolver` now dispatches to the advanced or legacy implementation using the same session variable as Go; both it and `OrderAwareJoinReorder` are registered in the ordinary rule pipeline.
- [x] (2026-08-29) Wired SELECT preorder query-block offsets and current-block `sel_N`/`QB_NAME` matching into join hints, replacing the prior all-`-1` plan identity that made scoped hints inapplicable.
- [ ] Complete the statistics-collection slice (completed: full pinned `collect_column_stats_usage.go` read, base-column lineage, predicate/full-histogram classification, interesting-column pruning input, CTE traversal, `Schema.ExtractColGroups`, projection/join/apply/window group translation, `DataSource.AskedColumnGroup`, ordinary `adjustOptimizationFlags` enablement of both statistics rule points, statement operator-count publication, session predicate-usage publication, direct virtual-column dependency expansion, plan-replayer table-runtime capture under the exact two-variable gate, removal of its stale ignored gap test, direct system-schema coverage, the complete original predicate matrix before and after optimization, and the complete original full/meta histogram matrix including static/dynamic partition expansion; remaining dependency closure: the separate pinned `pkg/statistics/handle/usage` package owns sweeping and persistence of the session-local usage map and must be completed atomically before that dependency is claimed).
- [x] (2026-08-30) Completed the nested pinned `pkg/statistics/handle/usage/collector` package as the next atomic dependency unit: `collector.go`, `collector_test.go`, and `BUILD.bazel` are fully inventoried; the three original tests are executable; Rust's non-Go close-aware synchronous refusal and its test are removed; and the spawned-session path now preserves pinned Go's nil-`closeCh` behavior by accepting a synchronous delta after close while capacity remains. Locked package tests, `make lint`, and `git diff --check` pass; mutation evidence shows the removed Rust-only assertion fails against the pinned behavior.
- [x] (2026-08-30) Completed the nested pinned `pkg/statistics/handle/usage/indexusage` package: `collector.go`, `collector_test.go`, and `BUILD.bazel` are fully inventoried; all four original tests and `BenchmarkIndexCollector` have executable Rust owners; bucket boundaries, zero-row handling, wrapping counters, last-use maxima, asynchronous report/flush, index GC, and statement-level query deduplication match Go; and session delta maps now return to a shared pool after merge instead of allocating on every report. Locked tests, benchmark compilation, consuming-session compilation, `make lint`, and `git diff --check` pass.
- [x] (2026-08-30) Completed the nested pinned `pkg/statistics/handle/usage/predicatecolumn` package: `predicate_column.go` and `BUILD.bazel` are fully inventoried; there are no package-local tests or support artifacts; all/table loading, session-time-zone projection, dropped-column cleanup, predicate-ID filtering, and transactional writes match Go. Removed Rust's extra `IsIndex` write filter because the pinned function persists every supplied `TableItemID`. The new regression fails with that filter restored and passes with the parity implementation; the existing load/cleanup and timestamp-replacement regressions, consuming-server compilation, `make lint`, and `git diff --check` pass.
- [x] (2026-08-30) Completed the root pinned `pkg/statistics/handle/usage` package. Its full inventory is `BUILD.bazel`; production `index_usage.go`, `predicate_column.go`, and `session_stats_collect.go`; and support/tests `export_test.go`, `index_usage_integration_test.go`, `predicate_column_test.go`, and `session_stats_collect_test.go`. The integrated owners cover the session collector/list, table-delta and column-usage maps, sweep/reset/deferred failure merge, earliest `InitTime`, sorted 2,048-row column batches and twelve-hour throttle, sorted 100,000-table delta batches, locked-table/partition/global rules, positive/negative `stats_meta` writes, post-commit historical-meta recording, DML row/count collection, transaction lifecycle, the node-global index collector wrapper, Domain-owned scheduling, and statement/session index reporting. Point, batch-point, table, index, lookup, and IndexMerge partial readers all report from their ordinary execution owners with exact physical request receipts, pseudo-stat fallback, logical/physical IDs, clustered-handle resolution, and statement query deduplication. The complete original 10-table by 10-index GC integration lifecycle is executable in the root Rust crate.
- [x] (2026-08-30) Ran the Ready validation profile and recorded the complete root usage-package receipt.
- [x] (2026-08-30) Filled the parent-rule dependency for pinned `GcSubstituter`: the catalog bridge retains virtual generated-column ASTs, `buildDataSource` resolves them against the complete DataSource schema, the registered ordinary rule implements Go's indexed-path/type/schema/operator/CTE/TiFlash/constant and plan-cache gates, and `tidb_enable_unsafe_substitute` now reaches the rule through the statement snapshot. This is dependency progress only; the parent `pkg/planner/core/rule` package remains incomplete, and ARRAY-cast construction remains an explicit expression dependency.
- [x] (2026-08-30) Filled the parent-rule dependency for pinned `SemiJoinRewriter`: the complete 68-line wrapper and `LogicalJoin.SemiJoinRewrite` behavior are registered in Go rule order; hint/session gates, CTE boundary, warning/refusal cases, right Selection, grouping FIRST_ROW Aggregation, inner Join field transfer, identity Projection, consumed hint bit, and hard-coded unchanged flag match the pinned source. `tidb_opt_enable_semi_join_rewrite` now reaches the rule through the statement snapshot. This is dependency progress only; the parent `pkg/planner/core/rule` package remains incomplete.
- [x] (2026-08-30) Filled the parent-rule dependency for pinned `ConvertOuterToInnerJoin`: the complete rule wrapper plus BaseLogicalPlan, Selection, Projection, and LogicalJoin dispatch are registered in Go order; preorder null-rejection, left/right inner-side selection, ON/WHERE predicate routing by join type, projection substitution, promoted Apply behavior, and hard-coded unchanged flag match the pinned source. The existing expression-owned `IsNullRejected` port supplies the complete structural proof behavior. This is dependency progress only; the parent `pkg/planner/core` package remains incomplete.
- [x] (2026-08-30) Filled the parent-rule dependency for pinned `SkewDistinctAggRewriter`: the complete postorder wrapper and qualification/rewrite behavior are registered in Go order; grouped/exactly-one-DISTINCT gates, COMPLETE/no-ORDER-BY/single-column-or-constant function coverage, two-level aggregation construction, constant-DISTINCT schema allocation, ordinary COUNT-to-SUM plus final cast projection, grouping FIRST_ROW retention, hint placement, refusal identity, and hard-coded unchanged flag match the pinned source. `tidb_opt_skew_distinct_agg` now reaches every ordinary `PlanBuilder` through the statement snapshot, and the original constant-argument panic regression is executable at the SQL session boundary. This is dependency progress only; the parent `pkg/planner/core` package remains incomplete.
- [x] (2026-08-30) Filled the parent-rule dependency for pinned `OuterJoinEliminator`: the complete 420-line rule is registered in Go order, including empty-inner typed-NULL projection, left/right unique-key and access-path grounds, nullable-key NullEQ refusal, duplicate-agnostic aggregation, row-number/window uniqueness, CTE boundary, Apply/Projection correlated-column behavior, required join-condition columns, recursive successive-join elimination, and the pinned hard-coded unchanged flag. `tidb_opt_enable_no_decorrelate_in_select` now reaches the rule through the statement snapshot. The consuming regression also exposed that Rust dropped Go's `BasePhysicalJoin.IsNullEQ` from hash/merge physical plans; the shared physical metadata and ordinary executor lowering now retain it, so a refused nullable `<=>` join preserves both matching NULL rows. Stale ignored outer-join gap markers were removed. This is dependency progress only; the parent `pkg/planner/core` package remains incomplete.
- [x] (2026-08-30) Replaced the outer-join source-catalog placeholders with executable coverage: all 16 pinned `TestOuterJoinEliminator` SQL inputs reproduce the recorded one-scan/two-scan decisions, and the empty-inner typed-NULL and row-number-one branches reproduce the newer pinned casetest results. During the parent rule-package audit, removed stale ignored no-op markers that still described completed PartitionProcessor, RANGE COLUMNS, ColumnPruner, BuildKeyInfo, aggregation-elimination, predicate-simplification, and join-order owners as unported; composite gaps that still require MPP, prepared cache, or absent nested-subquery AST execution remain explicit.

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

- Observation: `ApplyPredicateSimplificationForJoin` does not always request propagation, and join-key retention is not a fixed policy.
  Evidence: pinned `LogicalJoin.PredicatePushDown` passes `propagateConstant=false` for the left-outer family and `SessionVars.AlwaysKeepJoinKey` into `PropagateConstantForJoin`; Rust previously used one cache-specific closure and hard-coded key retention on.

- Observation: outer-join propagation is over the transitive equality class, not only direct outer/inner keys.
  Evidence: pinned `propOuterJoinConstSolver.propagateColumnEQ` builds a disjoint set; the Rust regression now derives an inner predicate across a three-edge alternating equality chain.

- Observation: the existing outer-join-to-anti regression documented and asserted a deliberately partial implementation.
  Evidence: before the registered rule body, the direct case failed and the test required inner-column output to remain a left outer join; the Go rule instead inserts a typed NULL projection, so that non-parity assertion was replaced.

- Observation: the ignored pruning inventory mixed current-master behavior into a pinned-commit parity task.
  Evidence: `TestIndexPruneWithSharedClusteredPrefix`, `effectiveIndexColumnIDs`, and the internal bad-offset test do not exist at `e2788410`; the pinned rule's fallback path deliberately has no consecutive-column IDs. Those stubs were removed rather than importing newer pruning policy.

- Observation: Rust's projection-inline seed modeled a custom expression-shape API instead of Go's planner behavior.
  Evidence: pinned Go's `rule_join_reorder_projection_inline.go` consumes real `LogicalProjection`, `expression.Expression`, schemas, statistics, and plan construction; Rust's `ProjectionInlineShape` accepted effect booleans supplied by tests and was not called by the optimizer.

- Observation: Rust's existing join-order benchmark ledger changed a non-asserting Go benchmark into derived correctness assertions while leaving both original package tests ignored.
  Evidence: `core_joinorder_greedy_start_isolation_source.rs` asserted a hand-derived `sink` value rather than running Go's benchmark workload, and its `chooseBestGreedyStart`/clone-isolation functions had no production owner. The file was removed; original coverage will be colocated with the real package implementation.

- Observation: pinned `OrderAwareJoinReorder` cannot be completed as an isolated wrapper.
  Evidence: its carrier selection, index-order proof, annotation, and ordinary reorder path call the separate `pkg/planner/core/joinorder` package, whose complete pinned inventory is `conflict_detector.go`, `join_order.go`, `ordered_leading.go`, `util.go`, `join_order_test.go`, `bitset_bench_test.go`, and `BUILD.bazel`.

- Observation: Go's advanced `ConflictDetector.TryCreateCartesianCheckResult` mutates the detector even though the edge has no predicates.
  Evidence: it calls `makeEdge`, which appends the synthetic edge and advances later edge indices. Rust's first draft only returned a detached edge; the detector now records it before constructing the result.

- Observation: Go shares one `*PlanHints` object across all joins built in a query block and distinguishes conflicting LEADING hints by pointer identity.
  Evidence: `CheckAndGenerateLeadingHint` compares the pointers, while `SetNewJoinWithHint` retains the same object after reorder. Rust's builder now carries `Rc<JoinHints>` and logical joins retain that shared owner instead of cloning independent hint values.

- Observation: the pinned `JoinReOrderSolver` dispatches directly to the separate advanced `joinorder.Optimize` package only when `TiDBOptEnableAdvancedJoinReorder` is true; false selects the legacy solver in `rule_join_reorder.go`.
  Evidence: the complete pinned rule wrapper was read before registration. The advanced package will not be registered as an unconditional replacement because that would erase the session-variable behavior.

- Observation: Rust already carried query-block offsets on every logical plan and used them while matching LEADING tables, but the SELECT builder never pushed an offset.
  Evidence: ordinary plans were constructed with `select_offset() == -1`; the builder now assigns Go's preorder `sel_1`, `sel_2`, ... identities, restores the stack on every result, and focused tests prove scoped and named-block join-hint matching.

- Observation: the statistics collector's lineage walk existed, but Rust's logical interface returned no column groups for projections, joins, applies, or windows.
  Evidence: pinned `CollectColumnStatsUsage` passes `ExtractColGroups` results to each child and stores matching groups on each DataSource for index statistics; Rust now ports the expression-schema primitive and all four operator overrides, with a regression covering a two-key join while index pruning is disabled.

- Observation: the ordinary Rust planner never enabled Go's two statistics rule points.
  Evidence: the plan builder correctly omitted session-owned flags, but the executor bridge had no equivalent of Go `adjustOptimizationFlags`; an ordinary filtered query therefore left `StmtCtx.OperatorNum` at zero and never requested its analyzed column. The bridge now enables collection and the later wait together, and an end-to-end executor regression proves both effects.

- Observation: Go's column collector also snapshots each visited logical table's statistics for plan-replayer capture; this is not part of the later executor dump path.
  Evidence: pinned `CollectColumnStatsUsage` gates `recordTableRuntimeStats` on `EnablePlanReplayerCapture || EnablePlanReplayedContinuesCapture`. Rust now carries the same session-variable OR into the statement context and records both present and absent statistics entries during the ordinary collection rule.

- Observation: `LogicalProjection.PushDownTopN` computed Go's substituted by-items but discarded them before descending.
  Evidence: the pinned body assigns every `ColumnSubstitute` result back to `topN.ByItems`; Rust retained the original hidden projection column, producing an orphan TopN reference for `ORDER BY a + b LIMIT 10`. The pushed TopN now owns the substituted expression and the original collector case passes before and after optimization.

- Observation: column lineage for a set-operation output must merge every branch rather than replace the prior branch.
  Evidence: pinned `updateColMap` inserts into an existing per-output set. Rust's prior `update_column` overwrote it, so `UNION DISTINCT` attributed the output only to its last child; it now extends the existing lineage set.

- Observation: the advanced joinorder package derives each vertex's statistics while building the conflict detector.
  Evidence: pinned `ConflictDetector.Build` invokes `RecursiveDeriveStats(nil)` before `cumulativeCostByChildren`. Rust now initializes DataSource statistics before logical rules and derives each vertex with the session join-reorder threshold before reading its cost.

- Observation: the plan-aware quantified/IN/EXISTS handlers existed but ordinary WHERE construction never dispatched AST nodes to them.
  Evidence: the pinned original predicate-column matrix failed at `> ALL` in Rust's scalar-only rewriter. Direct filter subqueries now build the inner query in the outer scope and invoke the existing Go-shaped handlers; the upstream Apply, IN, EXISTS, scalar-subquery, CTE, join, window, set-operation, sort, and TopN predicate-column cases all pass before and after logical optimization.

- Observation: the pinned statistics tests do not use three nullable `INT` columns for both `t` and `t2`.
  Evidence: `coretestsdk.MockSignedTable` marks `t.a`, `t.b`, and `t.c` NOT NULL, while `MockUnsignedTable` marks `t2.a` and `t2.b` NOT NULL and makes `t2.a`/`t2.c` unsigned. A reduced nullable fixture caused predicate pushdown to synthesize legitimate `IS NOT NULL` filters and falsely appeared to make IN-subquery join keys require full histograms. Mirroring the pinned types removes that false discrepancy, and all nine original non-partition histogram cases match exactly.
- Observation: Rust's real logical tree already preserves the distinction used by Go's `p.Schema() == p.Children()[0].Schema()` assertion.
  Evidence: `LogicalPlan::schema` returns an operator-owned schema when present and otherwise returns the first child's schema by reference. Therefore `base.schema().is_none()` plus a first child is the direct ownership-equivalent check; the separate public `SchemaNode` representation was both unnecessary and disconnected from optimizer execution.
- Observation: applying the generic partition rewrite recursively below UnionScan reverses Go's required tree shape.
  Evidence: pinned `rewriteDataSource` special-cases UnionScan and turns `UnionScan -> PartitionUnionAll -> DataSource*` into `PartitionUnionAll -> (UnionScan -> DataSource)*`, preserving per-partition transaction-buffer merging. Rust previously had only the generic recursion and also omitted `SetSkipPlanCache("Static partition pruning mode")` from its real rule path.
- Observation: partition pruning is not allowed to normalize only a temporary copy of `AllConds`.
  Evidence: pinned `PartitionProcessor.prune` writes the shared simplifier's result back to both `PushedDownConds` and `AllConds` before `Conds2TableDual`; the source comment identifies later simplifier calls as a correctness risk when those lists diverge. Rust's executor callback previously applied only `PushDownNot` to a temporary `AllConds` vector.
- Observation: exact-point KEY routing is not the complete pinned KEY pruning behavior.
  Evidence: pinned `getUsedKeyPartitions` also enumerates a non-point range when there is one integer partition column and the adjusted inclusive width is smaller than `pi.Num`, de-duplicating partitions through `ForKeyPruning.LocateKeyPartition`. Rust previously returned every partition for all non-point KEY ranges.
- Observation: conservative over-scanning at an exactly representable RANGE boundary is not Go parity.
  Evidence: pinned `PruneUseBinarySearch` handles `GT` by searching against wrapping `data.C+1`; for partitions `<10`, `<20`, `MAXVALUE`, the integer interval `(9,10]` selects only the second partition. Rust's prior `range_meets_partition` intentionally retained the first partition too.
- Observation: the RANGE COLUMNS pruning gap was not blocked on missing ranger metadata.
  Evidence: the ordinary planner bridge already calls `DetachCondAndBuildRangeForPartition` with every partition column and passes its normalized tuple `IndexRange` endpoints into the executor. Rust nevertheless accepted only full-arity points and returned every partition for all intervals; the executor now applies the pinned `minCmp`/`maxCmp` searches directly to those existing endpoints.
- Observation: a RANGE COLUMNS comparison collator belongs to the ranger interval, not necessarily to the partition column's declared field type.
  Evidence: pinned `multiColumnRangeColumnsPruner` passes each `Range.Collators` vector into `minCmp` and `maxCmp`, and the ranger accepts binary-collation equality against a non-binary string column. Rust discarded that vector at the planner/executor bridge, so a binary point `"B"` could be compared case-insensitively with a lowercase `"b"` partition bound and select the wrong partition.
- Observation: Rust's HASH pruning reused the write router too early.
  Evidence: pinned Go evaluates `hashExpr` over each ranger point before taking the modulus, while Rust passed the first raw endpoint directly to `hash_partition_index`; `HASH(a+1)` therefore pruned `a=1` to partition 1 instead of partition 2. The same shortcut also enumerated ranges for compound expressions that Go treats as full range and omitted the BIT(flen) finite-domain branch from issue 22619.
- Observation: Rust's LIST pruning compared ranger endpoints directly with stored expression-domain values and unconditionally retained DEFAULT.
  Evidence: pinned Go evaluates `pruneExpr` for point ranges and calls `LocatePartition`, which selects exactly one explicit, NULL, or DEFAULT owner; it uses range lookup (and therefore possible DEFAULT gaps) only for a bare-column non-point range. Rust could misroute `LIST(a+1)` and read DEFAULT beside every exact point until the executor callback was aligned.
- Observation: scalar RANGE pruning compared the partition column's raw ranger endpoints with bounds in the partition expression's value domain.
  Evidence: pinned Go replaces the partition function's column with each predicate constant, evaluates it, and recognizes `YEAR`, `TO_DAYS`, `UNIX_TIMESTAMP`, `PLUS`, `MINUS`, supported `EXTRACT`, and `FLOOR(UNIX_TIMESTAMP(...))` monotonicity. Rust therefore sent `RANGE(a+1)` boundary points to the preceding partition and could not prune supported time-function intervals; it now evaluates points and transforms only the pinned monotone set.
- Observation: LIST COLUMNS cannot be represented by one composite ranger interval over all partition columns.
  Evidence: pinned `locatePartitionByColumn` detaches each scalar predicate against exactly its one referenced partition column, then `ListPartitionLocationHelper` intersects or unions per-definition `GroupIdxs`; this is what lets a predicate on the second column prune and prevents predicates matching different tuples in one partition from falsely intersecting. Rust's old range-only branch lost both facts. The ordinary planner bridge now owns the recursive CNF/DNF flow and the incompatible range-only LIST COLUMNS branch is gone.
- Observation: the online partition-DDL overlap policy was already ported in `tidb-model`, but the executor metadata rebuild discarded its answer before planning.
  Evidence: pinned `makeUnionAllChildren`, scalar LIST pruning, and LIST COLUMNS location pruning all call `GetOverlappingDroppingPartitionIdx`; Rust's `PartitionInfo` implements the same action/state/type-dependent mapping, while `partition_spec_from_metadata` previously retained only definitions and bounds. The cluster loaders now snapshot that existing model result and the ordinary static planner consumes it at the same three behavioral points.
- Observation: Rust's hint AST already parsed `USE_INDEX_MERGE(t PARTITION(p), idx)`, but `index_merge_hints_from_select` flattened each match to only its index-name vector.
  Evidence: pinned `resolveOptimizeHint` filters `h.HintedIndex` per physical partition and `checkHintsApplicable` reads the same partition lists for warnings. Retaining the complete consumed fields lets the existing post-partition index-pruning stage see Go's per-child hint set without a second AST-side planner.
- Observation: a nonempty partition-definition vector is not equivalent to Go's `TableInfo.GetPartitionInfo() != nil` branch.
  Evidence: pinned `buildDataSource` rejects `PARTITION (...)` on a nonpartitioned table and validates every selected name before `getPossibleAccessPaths`; Rust previously copied the names into `DataSource` without either check and inferred the partition processor flag only from a nonempty definition list. The planner catalog now carries the metadata-presence fact explicitly.
- Observation: Rust resolved ordinary index hints only inside the executor's fast-point path, while the ordinary planner enumerated every public path.
  Evidence: pinned Go runs fresh planning through `getPossibleAccessPaths` and fast planning through its deliberately smaller `indexIsAvailableByHints`; Rust's deleted `index_hints.rs` mixed those two boundaries. The planner now owns both distinct Go-shaped checks, and ordinary physical planning carries the chosen lookup-pushdown origin into the shared reader.

- Observation: exact-name-only index hint lookup is narrower than pinned Go even when every named index exists.
  Evidence: pinned `getPathByIndexName` accepts a prefix only when exactly one public index name starts with it, and returns the clustered PRIMARY table path directly. The shared Rust resolver now performs the same exact-first/unique-prefix lookup and records ORDER/NO_ORDER flags for both secondary indexes and the PRIMARY table path.

- Observation: Go deliberately does not reuse general access-path hint resolution in `TryFastPlan`.
  Evidence: pinned `indexIsAvailableByHints` recognizes only USE/FORCE/IGNORE comment hints, matches index names exactly, emits no warnings, and checks the unique index selected by the point plan. Rust's deleted executor resolver also applied ORDER/NO_ORDER, prefix resolution, warning reporting, and lookup-pushdown support before it knew which point index won; the planner now owns a direct port of the smaller Go boundary.

- Observation: lookup-pushdown eligibility is statement state, not an executor-local hint boolean.
  Evidence: pinned `checkIndexLookUpPushDownSupported` reads table encoding/global/temp/cache/MV metadata followed by isolation, replica-read, stale/snapshot, and max-keys session facts in a fixed warning order; `checkAutoForceIndexLookUpPushDown` additionally reads the three-valued policy and table affinity. Rust snapshots those facts once at the statement boundary, propagates the Go origin (`Hint` or `SysVar`) through `CopTask`, builds the pinned `PhysicalLocalIndexLookUp` tree with fresh cloned table-side plan IDs, zeros the retained table-side statistics, and records the same leaf-first/post-order flattened lists and non-natural parent map for the ordinary reader/executor path.

- Observation: IndexMerge needs no second cache- or merge-specific Rust reporter.
  Evidence: pinned Go's root `Close` loops over `partialPlans` and reads each partial scan plan ID from the shared runtime collection. Rust builds every retained partial through the ordinary TableScan/IndexScan executor; each owns that exact plan ID and reports its completed cop summaries when the IndexMerge reader closes the partial. A second root wrapper would count every partial twice.

- Observation: generated-column substitution did not need an AST-text comparison or an executor-local rewrite.
  Evidence: pinned `buildDataSource` resolves `ColumnInfo.GeneratedExpr` after installing the complete DataSource schema and stores the resulting expression on `Column.VirtualExpr`; pinned `GcSubstituter` compares that expression with ordinary logical expressions. Retaining the generated AST at the catalog seam gives Rust the same single planner-owned path.

- Observation: pinned semi-join rewriting deliberately reports no plan change even after replacing the tree.
  Evidence: `SemiJoinRewriter.Optimize` returns the recursive rewrite result with `false`, and `LogicalJoin.SemiJoinRewrite` consumes the rewrite-hint bit before its two refusal checks. Rust preserves both externally visible details instead of normalizing them to the ordinary rule convention.

- Observation: Go's `LogicalApply` comment says it inherits the base outer-to-inner method, but the concrete method set promotes the nearer `LogicalJoin.ConvertOuterToInnerJoin` implementation.
  Evidence: `LogicalApply` embeds `LogicalJoin` by value and declares no override; Go method promotion therefore selects the join receiver, whose return value is the embedded `*LogicalJoin`. Rust mirrors that concrete transition when this rule flag reaches an Apply rather than following the stale inheritance comment.

- Observation: the skew-distinct rule flag already existed in Rust's rule list and plan builder, but ordinary statements could never enable it.
  Evidence: `PlanBuilder.enable_skew_distinct_agg` was initialized only to its default while the session variable stopped at `SessionVars`; snapshotting it into `StmtContext` and assigning it at all four planner construction sites makes the pinned rule reachable through the same statement path as Go.

- Observation: skew-distinct refusal must not reconstruct the aggregation node while probing whether its descriptor rewrites are buildable.
  Evidence: pinned Go returns the original `LogicalAggregation` pointer on every refusal. Rust therefore prepares schemas, descriptors, and projection expressions read-only and consumes the node only after preparation succeeds; a focused identity regression guards that contract.

- Observation: retaining NullEQ on the logical join is insufficient if physical join metadata narrows it back to ordinary equality.
  Evidence: the new eliminator correctly refused a nullable unique-key `<=>` join and EXPLAIN retained both tables, but execution returned one rather than two NULL matches. Pinned Go stores `IsNullEQ` on `BasePhysicalJoin`; Rust carried it only for index join. Carrying the aligned boolean vector through hash/merge candidates, cached clones, and common executor construction restores the pinned result without a cache- or query-specific path.

- Observation: the final ignored partition-pruning markers described production behavior that was already present but did not execute the original Go matrices.
  Evidence: `tidb_executor::partition_pruning` already classified `to_days` and `unix_timestamp`, transformed monotone endpoints, and performed RANGE COLUMNS bound searches. Executing the exact pinned datetime/timestamp outcomes and all thirteen `TestPartitionRangeForExpr` outcomes against that owner passed; the five original benchmark sizes now compile as `tidb-executor --bench partition_pruning` instead of empty ignored tests.

## Decision Log

- Decision: Close `pkg/planner/core/rule/util` before continuing the parent `rule` package.
  Rationale: repository policy requires whole Go packages as the minimum claim. The helper package is dependency-closed and lets duplicate Rust implementations be consolidated before more rule bodies consume them.
  Date/Author: 2026-08-29 / Codex

- Decision: Keep Go hooks as direct Rust functions rather than mutable process-global function variables.
  Rationale: the Go variables break an import cycle; Rust modules in one crate have no such cycle. Call behavior and signatures remain centralized without introducing mutable global state that Go does not behaviorally expose.
  Date/Author: 2026-08-29 / Codex

- Decision: Complete the pinned `joinorder` package before wiring `OrderAwareJoinReorder` or `JoinReOrderSolver` into `RuleId::body`.
  Rationale: registering either rule without the shared conflict detector, enumeration, hint, and ordered-leading behavior would create another narrower execution path and violate package-level completion.
  Date/Author: 2026-08-29 / Codex

- Decision: Preserve the pinned legacy and advanced join-reorder implementations as separate branches selected by `TiDBOptEnableAdvancedJoinReorder`.
  Rationale: the two Go algorithms have different extraction, projection-inline, DP, greedy, Cartesian, and hint behavior. Routing both settings to one Rust solver would not be behavioral parity.
  Date/Author: 2026-08-29 / Codex

- Decision: Keep `noUnexpectedZeroColumnSchema` private to the logical rule implementation, as it is in Go, and express `intest.AssertFunc` as a debug assertion after successful pruning.
  Rationale: this checks the production plan representation during test/debug builds while avoiding a public normalized API that Go does not have.
  Date/Author: 2026-08-30 / Codex

## Outcomes & Retrospective

The pinned `pkg/planner/core/rule` package is atomically complete; the broader package-by-package planner transcreation remains in progress.

The registered predicate-simplification body and ordinary/inner/outer-join propagation are integrated as dependency work. The final utility audit additionally found and fixed two subtle divergences: the ordinary simplification hook had discarded Go's validity filter, and unique-index key strength had read expression-schema flags instead of `ColumnInfo` metadata. Mutation evidence proves the filter regression test fails when forwarding is disabled. Package completion is still withheld pending the remaining parent-rule inventory and Ready gates.

The `pkg/planner/core/rule/util` package receipt is pinned to Go revision `e2788410d8d696605e8cb002585877a063ccc909`. Its complete inventory is `misc.go` and `BUILD.bazel`; there are no package-local tests, fixtures, generated inputs/outputs, build-tag or platform variants, benchmarks, fuzz targets, or examples. Validation passed with `cargo test --locked -p tidb-planner --lib rule_util -- --nocapture`, the two consuming regressions, `cargo check --locked -p tidb-session`, `make lint`, and `git diff --check`.

The nested `pkg/statistics/handle/usage/collector` receipt is pinned to the same Go revision. Its complete inventory is `collector.go`, `collector_test.go`, and `BUILD.bazel`; there are no fixtures, generated artifacts, build-tag or platform variants, benchmarks, fuzz targets, or examples. `cargo test --locked -p tidb-stats-handle-usage-collector -- --nocapture`, `make lint`, and `git diff --check` pass.

The nested `pkg/statistics/handle/usage/indexusage` receipt is pinned to the same Go revision. Its complete inventory is `collector.go`, `collector_test.go`, and `BUILD.bazel`; the benchmark is declared inside the original test file and maps to Rust's `benches/collector.rs`; there are no fixtures, generated artifacts, build-tag or platform variants, fuzz targets, or examples. `cargo test --locked -p tidb-stats-handle-usage-indexusage -- --nocapture`, `cargo bench --locked -p tidb-stats-handle-usage-indexusage --no-run`, `cargo check --locked -p tidb-session`, `make lint`, and `git diff --check` pass.

The nested `pkg/statistics/handle/usage/predicatecolumn` receipt is pinned to the same Go revision. Its complete inventory is `predicate_column.go` and `BUILD.bazel`; there are no package-local tests, fixtures, generated inputs or outputs, build-tag or platform variants, benchmarks, fuzz targets, or examples. The native Rust owners are split across `cluster_predicate_column.rs` and `cluster_stats_write.rs`, with executable coverage in `analyze_commit_size_source.rs`. Validation passed with the three focused `cargo test --locked -p tidb-exec --test all <test-name> -- --nocapture` invocations for table-item-kind writes, load/cleanup, and timestamp replacement, `cargo check --locked -p tidb-server`, `make lint`, and `git diff --check`. Restoring the removed `IsIndex` filter made the table-item-kind regression fail, providing fail-before evidence.

The root `pkg/statistics/handle/usage` package receipt is pinned to the same Go revision. The complete inventory is `BUILD.bazel`; production `index_usage.go`, `predicate_column.go`, and `session_stats_collect.go`; and support/tests `export_test.go`, `index_usage_integration_test.go`, `predicate_column_test.go`, and `session_stats_collect_test.go`. There are no fixtures, generated inputs or outputs, build-tag/platform variants, benchmarks, fuzz targets, or examples. Native owners span `tidb-stats-handle-usage`, `tidb-exec`, `tidb-session`, `tidb-server`, and the ordinary executor readers. Validation passed with the full root usage-crate tests; focused IndexMerge and reporter tests; the original predicate usage/cleanup behaviors; persisted and predicate-column ANALYZE tests; `cargo check --locked -p tidb-server`; `cargo fmt --all -- --check`; `make lint`; and `git diff --check`.

The direct `pkg/planner/core/rule` package receipt is pinned to Go revision `e2788410d8d696605e8cb002585877a063ccc909`. Its complete inventory is `BUILD.bazel`; fourteen production files (`collect_column_stats_usage.go`, `logical_rules.go`, `rule_build_key_info.go`, `rule_collect_plan_stats.go`, `rule_column_pruning.go`, `rule_constant_propagation.go`, `rule_init.go`, `rule_join_key_type_cast.go`, `rule_max_min_eliminate.go`, `rule_order_aware_join_reorder.go`, `rule_outer_join_to_semi_join.go`, `rule_partition_processor.go`, `rule_predicate_simplification.go`, and `rule_prune_indexes.go`); and three original test files (`collect_column_stats_usage_test.go`, `rule_max_min_eliminate_test.go`, and `rule_partition_pruning_test.go`). There are no fixtures, generated inputs or outputs, build-tag/platform variants, fuzz targets, or examples. Native owners are the registered `tidb-planner` logical rule modules plus the ordinary executor partition-pruning bridge; no cache-only, query-specific, or Rust-only planning policy remains in this package. Every original test table has executable coverage, and the five original RANGE COLUMNS benchmarks map to `tidb-executor`'s `partition_pruning` benchmark target. Ready validation passed with 125 focused planner rule tests, all 19 executor partition-pruning tests, the three direct partition-range algebra tests, benchmark compilation, consuming-server compilation, formatting, `make lint`, and `git diff --check`.

## Context and Orientation

The parent pinned package contains `BUILD.bazel`, fourteen production `.go` files, three original `_test.go` files, and the nested `util` package. The Rust rule driver is `rust/crates/tidb-planner/src/logical/rule.rs`; tree rewrites are in `logical/rewrite.rs`; rule-specific bodies are `logical/rule_*.rs`. Executor-owned catalog and partition-expression access is in `rust/crates/tidb-executor/src/driver/planner_bridge.rs`.

The pinned `pkg/planner/core/joinorder` package is a direct dependency of both the parent package's ordinary join reorder and nested `rule` package's order-aware wrapper. Its complete artifact inventory is four production files (`conflict_detector.go`, `join_order.go`, `ordered_leading.go`, `util.go`), two original test/support files (`join_order_test.go`, `bitset_bench_test.go`), and `BUILD.bazel`; it has no fixtures, generated files or inputs, build-tag/platform variants, fuzz targets, or examples.

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


## Current-master re-audit checkpoint (2026-09-08)

The previous whole-package description is historical evidence, not current-master completion proof. Master `f5cf8f6337612c6ae51fb6e384e4bb3469dde680` contains nineteen direct artifacts, including the fourth test file `rule_prune_indexes_internal_test.go`. The complete current inventory, blob identities, line counts, declaration list and honest reading status are recorded in `rust/docs/planner/core-rule-reading-inventory-20260908.md`. Ten direct artifacts were read in full during this checkpoint; remaining files must be read before editing package production code.

The preceding constraint fix is pushed as `feb796539f`. Its new evaluation-context plumbing enables this package's next fix, but `logicalConstant` still ignores conversion events. The next milestone is to finish reading all direct artifacts, review the corresponding Rust owners, reproduce strict/warning/ignore classification regressions, and repair the classifier under the existing statement policy. Run Ready and update the package receipt before committing a behavior batch. No claim of complete optimizer parity is made at this checkpoint.

Reading checkpoint update: fourteen of nineteen direct artifacts have now been read in full. The remaining five are `rule_collect_plan_stats.go`, `rule_partition_processor.go`, `rule_partition_pruning_test.go`, `rule_predicate_simplification.go`, and `rule_prune_indexes.go`. Production code remains unchanged pending completion of the inventory.

Reading checkpoint update: sixteen of nineteen direct artifacts have now been read, including all statistics-load and predicate-simplification code. Preserve Go processCondition double classification when adding warning regression coverage. The partition processor, partition pruning test file and index-pruning implementation remain pending before production edits.

Reading checkpoint update: eighteen of nineteen direct artifacts are now read. All four Go test files and the complete index-pruning implementation are covered. Only the 2149-line partition processor remains; no production edit has been made in this package audit.


## Statement conversion batch (2026-09-08)


Progress: all nineteen direct artifacts (7460 lines) were read before production edits, including all 2149 partition-processor lines in bounded segments. The inventory records every Git blob and function declaration. No Go source was edited.

The Rust logicalConstant owner discarded conversion events, causing strict-mode `1garbage` to become true and `0garbage` to become false. It also omitted the second leaf classification performed by Go processCondition, dropping observable warnings. Reuse the already-audited constraint conversion adapter with the RuleContext statement evaluation context. Preserve failed conversions as Other; retain the plan-cache guard before conversion. Process leaves twice and preserve unchanged AND/OR expressions instead of unnecessarily rebuilding them.

Regression evidence: three new tests failed against the pre-fix implementation (True versus Other, removed AND operand, and zero versus two warnings). All five predicate owner tests now pass, together with six constraint tests and 57 logical-rule consumer tests. Ready `make lint` passes. Commands and remaining limitations are recorded in `rust/testport/receipts/planner_core_rule.md`.

Decision: keep one crate-private conversion adapter in constraint instead of duplicating diagnostic formatting in the rule. The adapter refactor preserves the preceding constraint batch's behavior, verified by all six tests. No new dependency or public API is introduced.

Outcome: this behavior batch is validated; the package as a whole remains under audit. Source reading is complete but is not proof that all four original Go test artifacts execute equivalently in Rust. Continue with join helper/final deletion semantics, original test mapping, constant propagation, partition processing, statistics-loading consumers, and the remaining planner/statistics packages. Underlying conversion overflow/truncation diagnostics and invalid UTF-8 behavior require their own datatype-package audit.


## Join propagation filter batch (2026-09-08)


Progress: revalidated clean worktree at pushed `35433b5de2`. The complete nineteen-artifact current-master reading inventory remains applicable. Re-read Go applyPredicateSimplificationHelper and the Rust join entry point, then inspected the propagation callback consumer without changing the expression package.

Go passes the supplied validity filter to PropagateConstant even when propagateConstant is false. Rust's ordinary path did this, but its join path passed None. A DNF containing `(a=b AND a>7) OR c=9` therefore admitted a derived `b>7` predicate rejected by the caller. The regression first failed on missing callback invocation and then, independently, on the resulting expression mismatch. Pass valid through the existing call; no new algorithm or additional behavior is introduced.

Ready evidence: all six predicate owner tests and 57 logical-rule consumer tests pass, as does make lint. Red logs are `/tmp/core-rule-join-red-20260908.log` and `/tmp/core-rule-join-shape-red-20260908.log`; green logs and commands are recorded in the package receipt. Outcome: join propagation now respects the caller's filter on this path. Continue the remaining package audit; the join final-deletion implementation and other predicate helpers are still open comparisons.


## OR conjunction preservation batch (2026-09-08)


Progress: revalidated clean state at pushed `63a4d3cda7`. The nineteen-artifact inventory remains current for the pinned Go authority. Re-read recursiveRemoveRedundantORBranch and the Rust normal-form consumer boundaries.

Go handles AND branches separately: recursively clean each conjunct, append the recomposed conjunction, and do not enter it into the non-AND deduplication map. Rust additionally hashed and removed duplicate conjunctions. Remove that Rust-only step while preserving leaf deduplication and recursive processing. The regression uses repeated AND branches containing repeated OR leaves, interleaved with a repeated ordinary leaf; Go retains three outer branches and removes duplicates inside both conjunctions.

The pre-fix run failed with two branches instead of three. During validation, the test's whole-expression comparison also exposed a test-construction type mismatch (the convenience function assigns Tiny, the normal-form composer infers its own logical result type). The test now checks the two preserved branches' ordered conjuncts and the middle leaf directly. This retains the original failing branch-count assertion and verifies the actual intended semantics without asserting that unrelated test-helper metadata is identical.

Ready: seven predicate tests, 57 logical-rule tests, make lint, rustfmt check and git diff check pass. Evidence is in the package receipt. Outcome: OR cleanup now preserves Go's conjunction branch boundary. Full package parity remains open; continue the declaration inventory and original test mapping.


## IN reconstruction batch (2026-09-08)


Progress: revalidated clean worktree at pushed `ddf4f77c1d`; the complete current-master package inventory remains applicable. Re-read Go updateInPredicate and the Rust builder, collation derivation, and IN evaluation consumer boundaries. No expression-package source was edited.

Go reconstructs the reduced IN through NewFunctionInternal. Rust directly constructed ScalarFunction with the original result metadata. A baseline with ordinary strings did not fail; an explicit utf8mb4_bin literal removed by NE exposed stale collation. The remaining column and literal use utf8mb4_general_ci, but the reduced IN retained binary comparison, so an uppercase A row returned 0 instead of the builder-derived 1. The final regression fails on this row result before the production fix and also checks expression metadata against reconstruction through the existing real builder.

Pass RuleContext into update_in and rebuild through its FunctionBuilder. Keep the existing all-values-removed special case and NULL-NE guard. If Rust's fallible construction boundary rejects rebuilding, retain both original predicates; this boundary remains an explicit limitation rather than a claim that Go's NewFunctionInternal error handling has been fully reproduced.

Ready: eight predicate tests, 57 logical-rule consumer tests and make lint pass. Formatting and diff checks pass. Logs and the precise remaining scope are recorded in the receipt. Outcome: successful IN reconstruction now derives metadata from the remaining arguments like the Go call; the full planner/statistics/optimizer goal remains active.


## Bound false OR branch batch (2026-09-08)


Progress: revalidated clean worktree at pushed `783d3f9e53`. The complete core/rule inventory remains current. Re-read Go updateOrPredicate/pruneEmptyORBranches and the called logicalop.IsConstFalse body in expression_util.go. No logicalop owner was edited or declared audited as a package.

Go unsatisfiableExpression calls IsConstFalse, which examines the bound constant value without the plan-cache guard used by logicalConstant. The caller disables plan caching after pruning a mutable branch. Rust reused logical_constant and therefore incorrectly suppressed both pruning and the required cache marker for a bound zero/NULL parameter. Use the statement-aware conversion adapter directly, preserving NULL-as-false and conversion-error handling without that extra guard.

The regression first failed because the bound false branch remained. After the fix it verifies both zero and NULL branches are removed, ordinary classification still returns Other for the parameter, and each rewrite emits the exact Go OR-simplification cache reason. Ready: nine predicate tests, 57 logical-rule tests and make lint pass; formatting and diff checks pass. Logs are in the receipt. Outcome: the rule consumer now follows the separate Go false-value and classification contracts. Full package and broader optimizer/statistics parity remain open.


## MAX/MIN cloned access-path batch (2026-09-08)


Progress: revalidated clean worktree at pushed `568aa00db6`. Re-read the complete Go MAX/MIN implementation and its sole original test, plus the complete Rust rule owner. The full nineteen-artifact package inventory remains applicable. The original Go TestMaxMinEliminateSkipsEmptyScalarAgg maps to Rust max_min_eliminate_skips_empty_scalar_aggregation and is included in this batch's passing tests.

Go cloneSubPlans copies AllPossibleAccessPaths and then resets PossibleAccessPaths to that complete list. Rust's generic DataSource clone retained a previously pruned subset. Reset the rule-specific clone's possible list from its copied all-path list before pruning the new aggregate. This avoids carrying a path decision made for the old aggregate into each split aggregate.

Regression split_source_clone_restores_all_access_paths starts with paths 11 and 22 but a pruned candidate list containing only 22. Before the fix its first clone incorrectly retained only 22. After the fix two independent clones have both paths in order; clearing one clone's lists leaves the other and the original untouched. The path candidates are used to test cloning, not to claim physical task admission or execution coverage.

Ready: three MAX/MIN owner tests and 57 logical-rule tests pass, plus make lint, formatting and diff checks. Logs are in the package receipt. Remaining MAX/MIN comparisons include handle-path early-return semantics and range-size context, and broad package parity is still unproven.


## MAX/MIN handle-path control flow (2026-09-08)


Progress: revalidated clean state at pushed `09166c4a45`; the complete core/rule inventory remains applicable. Go checkColCanUseIndex returns immediately on a matching integer handle, whether detachment succeeds completely or leaves filters. Rust's Iterator::any interpreted that false as permission to try later indexes. Replace the any closure with an ordered loop and preserve the source return versus continue distinction.

Regression integer_handle_residual_stops_before_later_covering_index constructs handle a and covering index (b,a) with b=1. Index alone succeeds; [table,index] must return false at the handle residual; [index,table] succeeds before reaching the handle. The middle assertion failed before the fix. The initial test import typo was corrected before collecting behavioral red evidence.

Ready: all four MAX/MIN tests and 57 logical-rule tests pass, together with make lint, rustfmt and diff checks. Logs are recorded in the receipt. Outcome: ordered path search now matches this Go early-return branch. Range-size context and other package declarations remain pending; broader completion is not claimed.


## Range-context and order-aware audit checkpoint (2026-09-08)


Revalidated clean worktree at pushed `69b5be8c52`. No production edit in this checkpoint. MAX/MIN passes zero to both Rust index-range construction calls. The ranger accepts range_max_size and uses it in its DNF memory fallback, so this is not simply a missing parameter in ranger. RuleContext has no range-limit field, executor StmtContext has no corresponding accessor, and session stmt_ctx snapshots do not read tidb_opt_range_max_size. The variable is registered with default 67108864; Go's setter assigns SessionVars.RangeMaxSize. A complete repair must carry that value through both statement-context construction paths, executor planner_bridge, RuleContext copies, and MAX/MIN, and test limited versus unlimited behavior. Do not hard-code the default at the planner call and call this fixed. Before edits outside the audited package, finish the relevant source-package inventories.

Read the complete Go (245 lines) and Rust order-aware join-reorder owner. Function mapping: Optimize to the trait implementation; optimizeRecursive to optimize_recursive; optimizeChildren to optimize_children plus optimize_choice_vertices for the owned-tree vertex traversal; shouldUseCDCBasedJoinReorder to should_use_cdc_based_join_reorder; extractOrderingColumns to extract_ordering_columns; sameOrderingColumns to same_ordering_columns; rewriteOrderingForProjection to rewrite_ordering_for_projection; Name to name. Forward column-only extraction, TopN/Sort replacement of inherited order, projection mapping, Limit propagation, mutable-selection barriers, filter accumulation, threshold gating, and carrier-only propagation were compared. No new confirmed owner mismatch was found. Joinorder choice/annotation and data-source ordering remain dependency comparisons, not established parity.

Focused evidence: logical::rule_order_aware_join_reorder::tests passes 2/2 in `/tmp/core-rule-order-audit-20260908.log`. This is an audit checkpoint, not a Ready behavior batch or whole-package completion claim. Next work is the missing range-limit context chain and remaining source-to-owner declarations/test mapping.


## Statement-context prerequisite inventory


Revalidated clean state at `cb54e38fc5`. Completed reading every direct artifact in Go pkg/sessionctx/stmtctx: BUILD.bazel (67 lines), main_test.go (34), stmtctx.go (1713), stmtctx_test.go (641), totaling 2455 lines. All seventeen tests, the benchmark and build targets were inspected. Exact blobs and function declarations are recorded in `rust/docs/planner/stmtctx-reading-inventory-20260908.md`.

The audit extends the range-limit fix requirements: constructor, full reset and logical-build-state restore recreate RangeFallbackHandler bound to the current plan-cache tracker and warning appender. Retry reset has different semantics. A range-budget repair must not claim complete behavior merely by delivering the byte limit while dropping warning/cache fallback effects. No production edits were made; session-variable and session package prerequisites still need inventory before cross-package changes. This is source-reading progress, not validation or completion of the range-limit fix.

Variable-package prerequisite checkpoint: revalidated clean state at f252a064aa. Enumerated every direct and nested tracked artifact with blob IDs and line counts in variable-reading-inventory-20260908.md. Eight direct artifacts are fully read; remaining entries are explicitly pending. The nextgen build-tag test is outside the ordinary BUILD test source list and must be covered separately. No behavior edit or Ready claim is made; continue the remaining files before changing the range-limit transmission chain.

Variable reading continuation at 398b4c16af: seven additional complete artifacts bring direct coverage to fifteen. Accessor validation versus hook-bypass behavior, removed-variable errors, status-provider registration/overwrite/error semantics, and TiDB statistics hooks were read with their tests. Nine direct files and nested tests remain pending, listed in the inventory. No implementation or validation claim; continue the large files before cross-package range-limit edits.

At 2d9fdf901d, completed variable.go in three bounded segments. Sixteen direct files are read. The range-limit regression requirements now explicitly distinguish parsed integer bounds warnings from int64 parse-overflow errors, and relaxed read validation from SET validation. Hook ordering and alias warning suppression were read as well. Eight direct files and nested test packages remain pending before implementation; details are in the variable inventory.

At 9f99bd62f1, completed setvar_affect.go, varsutil.go and varsutil_test.go (1443 lines total), bringing direct reading coverage to nineteen artifacts. The hint allowlist explicitly includes tidb_opt_range_max_size, requiring a statement-override regression in the eventual context transmission repair. All 34 conversion helper functions, ten tests and one test helper were inspected; details are in the inventory. Five direct large files and nested tests remain pending. No production change or Ready claim in this checkpoint.

At 620d61522f, completed noop.go and slow_log.go in seven bounded segments (1865 lines), bringing direct coverage to twenty-one artifacts. Catalog hint flags do not imply implemented optimizer behavior. Slow-log used-statistics ordering, ordinary/extra warnings, optimizer timing fields and every rule accessor/parser were read and recorded in the variable inventory. session.go, sysvar.go, sysvar_test.go and nested tests remain pending before the range-context implementation. This is prerequisite reading, not a new behavior fix or validation claim.

At fc53443c27, completed all 4013 lines of session.go in ten contiguous segments, including 228 function declarations now recorded in the inventory. Direct artifact coverage is twenty-two; sysvar.go, sysvar_test.go and nested tests remain pending. NewSessionVars leaves RangeMaxSize zero, so the pending integration must trace later variable initialization rather than attributing the catalog default to raw construction. Old-state setters read through hooks/full getters before replacing values; migration getters intentionally avoid uncached defaults. Statement-context reset/reuse and optimizer/statistics accessors were also read. No implementation or Ready validation claim.

Publication decision: the user clarified that documentation-only changes do not need pushing. Keep reading inventories, receipts and ExecPlan updates local until a Rust behavior batch is ready, then include the relevant evidence with its tested fix. Do not create or push separate documentation-only batches going forward. Previously pushed history remains intact.

Implementation WIP: added detach_index_range_with_fallback_handler and a shared optional handler in RangeDetacher. Recursive best-CNF and tail detachers retain the handler; all six CNF/tail/DNF quota exits call it at construction time. New range_quota_events_reach_shared_handler regression first failed at runtime with 'quota fallback must prevent cache admission', then passed after wiring (one selected test). Command: cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner range_quota_events_reach_shared_handler --lib -- --test-threads=1. Logs: /tmp/ranger-quota-events-red-20260908.log and /tmp/ranger-quota-events-green-20260908.log. An initial test compilation error in copy_warnings invocation was corrected before recording the runtime failure. Still required: broader recursion/DNF/ordinary-residual/zero-budget regressions; session budget and handler integration into MAX/MIN and cache admission; conversion skip-reason propagation; Ready validation and package batch review/commit/push. Existing callers still use the handler-free entry, so this is groundwork, not an integrated fix or completion claim.

WIP utility baseline completed: the recorded context::plancache::tests command passed all four selected tests (zero failures/ignored). The reusable tracker and fallback handlers pass their existing contracts, including callback-panic behavior. No new regression or integrated fallback path exists yet; Ready is not claimed.

Completed points.go (1054 lines, blob 5ad1caf7c5c87260bb22e83a7a1f3daa8aa9b541) in contiguous 1–360, 361–720 and 721–EOF segments. All four changed ranger artifacts are now refreshed; all 13 paths were re-enumerated and the other nine are unchanged from the recorded follow-up authority. Exact refresh evidence is in util_ranger.md. Reuse the existing RangeFallbackHandler through construction-time callbacks/shared references, not final-result inference. Started WIP utility handler baseline: cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-util context::plancache::tests --lib -- --test-threads=1. This baseline is not Ready or evidence that the pending integration is fixed.

Refreshed full detacher.go reading at pinned master, blob f69fa894d8e6816caf8e2bfe90db444dbad7f245, in contiguous 1–400, 401–800, 801–1220 and 1221–EOF segments. This includes EQ/IN extraction, fix-control CNF selection/intersection, recursive tail construction, all DNF quota branches, public/simple/partition entries and every shard-generated-column helper. DNF cumulative memory is tested before union; ordinary unextractable branches return the same full-range/residual shape without a quota event. Recursive candidates share handler state even if their ranges are later discarded, so observing only the final result cannot recover all side effects. Tail fanout fallback records an event before retaining access and residual conditions. AppendConditionsIfNotExist checks against the original destination, not earlier additions in the same batch. Mutable merged predicates can separately skip cache with 'some parameters may be overwritten'. points.go remains the final changed ranger file requiring refreshed reading. No Rust edit, test execution or publication.

Refreshed complete reading of Go ranger/bench_test.go (blob 8156f263d1627a6ca662c266bce9078c0b0c2ccc) and ranger/ranger.go (blob 5f3ced201ee7bf154016143adf37951c59e8ee33) at pinned master. The latter was read in contiguous 1–330, 331–700 and 701–EOF segments. Benchmark helpers build real logical plans, push down NOT, and use unlimited budgets; the two added 512-item IN benchmarks report ranges and bytes but TestBenchDaily still invokes only the original benchmark. Range conversion compacts valid intervals before quota estimation; batch storage caps each range's slices to prevent append aliasing. CNF fallback keeps the successfully built prefix and records the event; BuildColumnRange with no conditions bypasses quota. Changed detacher.go and points.go still need full refreshed reading. No Rust edit or runtime validation.

Ranger prerequisite baseline check: the existing util_ranger receipt's follow-up authority a0cdff369bd4c7060a840e3943049a79470e8af4 differs from pinned f5cf8f6337612c6ae51fb6e384e4bb3469dde680 in bench_test.go, detacher.go, points.go and ranger.go (197 insertions, 85 deletions). Do not reuse that old completion assertion without refreshing the changed source/test inventory. The combined diff output was truncated, so full changed-file reading remains pending. Current executor inspection also confirms that cached_physical_query_plan and cached DML both reject publication via StmtContext.skip_plan_cache(), whose backing state is presently a shared first-reason Option rather than the utility tracker. Handler integration must feed this actual admission decision and the existing statement warning buffer; an isolated tracker would not prevent caching. No production edit or test execution yet.

Completed prerequisite packages pkg/util/context (five artifacts, 757 lines) and pkg/util/ranger/context (three artifacts, 141 lines) at the pinned Go authority. Every production function, warning test/helper and build target was read; neither tracked package contains additional fixtures/generated/platform inputs. Exact inventory follows. RangeFallbackHandler calls SetSkipPlanCache on every event before a sync.Once capacity warning. Force-cache risk warnings can therefore repeat even when capacity warnings do not. RangerContext.Detach clones fix controls but shares the tracker and fallback handler. Rust tidb-util/src/context/plancache.rs already implements these handlers and save/restore; reuse them rather than introducing parallel state. Their integration into executor/session/planner remains open. No runtime validation or code change in this reading checkpoint.

| Go artifact | Lines | Blob | Read |
| --- | ---: | --- | --- |
| pkg/util/context/BUILD.bazel | 29 | 23f0d73c401398ab177642ecb46e3986a02075ed | complete |
| pkg/util/context/context.go | 42 | 6d0ea1d6f7ca92375b2f13935a8ef778df2b2406 | complete |
| pkg/util/context/plancache.go | 194 | d6c46cd4db81ef18001eae3748333990758b5b22 | complete |
| pkg/util/context/warn.go | 310 | b07a91d1a39245bf02bae1fcae112143cc5da9c2 | complete |
| pkg/util/context/warn_test.go | 182 | cedf4f2e7bb71d90ff3a899a9fe715e9f79610a8 | complete |
| pkg/util/ranger/context/BUILD.bazel | 30 | 68c6ace6f73c54e4a0b0935c05e7c1cd70c6fd5d | complete |
| pkg/util/ranger/context/context.go | 49 | d8cfa14d28e2758823635ea600a485fe4fea013f | complete |
| pkg/util/ranger/context/context_test.go | 62 | 40f4c6db441c0814c32e41bc4660d504783aa0e6 | complete |

Warning-source details read: SQLWarn JSON unwraps causes; CopyWarnings and TruncateWarnings return copied storage; GetWarnings aliases internal storage; AppendWarnings tests the existing length before appending the entire batch, unlike single append. No Go plancache_test.go exists in this package. Existing source tests cover warning JSON, ignore handler, copying/truncation and detached context ownership; they do not establish end-to-end fallback integration.

Rust range-path revalidation after completing variable reading: both common-handle and secondary-index calls in logical/rule_max_min_elimination.rs still pass zero. StatementVarSnapshot, executor StmtContext and RuleContext have no range-budget field. The ranger wrapper detach_cond_and_build_range returns only DetachRangeResult and drops RangeDetacher.skip_plan_cache_reason; its DNF cumulative-memory fallback branches return full ranges/residual filters without recording an observable quota-fallback event. Existing core_integration_range_fallback_and_corcol_source.rs cases are ignored empty gap tests, not execution coverage. Next repair must preserve budget plus warning/cache side effects, with quota fallback distinguished from ordinary residual predicates. Go handler authority is pkg/util/context/plancache.go:179; ranger invokes it at all quota fallbacks. Inventory/read affected prerequisite packages before edits, then add failing end-to-end and focused regressions. No Rust edit, validation or publication in this checkpoint.

Variable prerequisite reading is now complete: all 31 inventoried artifacts, including the final 743-line variable_test.go and 707-line slowlog test. Generic enum indexes must not be confused with custom TopN validation, and relaxed validation preserves some unsupported values rather than universally substituting defaults. Slowlog snapshot and zero/negative threshold cases were inspected. Resume Rust range-budget transmission analysis with this source authority; session initialization and fallback-warning/cache integration remain implementation prerequisites. This completes reading only, not the repair or package parity. No runtime tests, commit or push; documentation remains local.

Completed nested tests/session_test.go, all 1083 lines in three contiguous segments. Two nested test files remain. Global/session optimizer-variable inheritance is explicitly tested; the range-context repair should likewise exercise actual session initialization. Some source tests provide weaker evidence: the hook-context test ignores a rejected SET and the slow-log comparison helper skips statistics/warnings fields. Record these limits rather than copying assertions as proof. No Rust edits, runtime tests, Ready claim, commit or push.

Completed sysvar_test.go through EOF (2422) and both nested BUILD/TestMain pairs. All 24 direct variable artifacts are now read; three nested test files remain. Re-enumerated the 31 tracked artifacts with no additions. The selectivity test's string-versus-float NotEqual assertions are weak evidence and must not substitute for a Rust behavior regression. SkipInit tests confirm the need to preserve global-to-session initialization. Continue the three nested test files before cross-package range-context edits. This is local source-reading evidence only; no Rust change, Ready run, commit or push.

Latest test-reading cursor: sysvar_test.go through 1600, resume at 1601 inside TestTiDBAutoAnalyzeRatio. Direct complete coverage remains 23/24. Memory-limit normalization, resource-control hooks and rejected auto-analyze ratio preserving prior state were inspected. This supersedes earlier reading cursors below; no runtime or Ready validation claim.

Local sysvar continuation: read lines 1–1300 in four contiguous segments, including all helpers and catalog hooks in that interval. Resume at 1301 inside the analyze-default-TopN setter. Recorded strict-mode TiFlash exclusion, prior-statement plan-cache getters, statistics-owner hook ordering and auto-analyze custom validation. sysvar.go remains partial, with sysvar_test.go and nested tests pending; direct complete count remains twenty-two. No production or validation claim, and no documentation-only commit/push.

Next local continuation completed sysvar.go 1301–2350 in three contiguous segments. Resume at 2351 in the broadcast-join threshold-size setter. Recorded auto-analyze dependencies, column-tracking fixed behavior, cache aliases, statistics cache capacity hooks, historical-statistics prerequisites, SQL-mode/charset side effects and 32-bit GROUP_CONCAT bounds. Direct complete count remains twenty-two; no behavior fix or Ready claim. Documentation stays local.

Local continuation completed sysvar.go 2351–3370. RangeMaxSize registration is confirmed: global/session signed integer, bounds zero through MaxInt64, no skipInit/custom validation, setter uses the declared default on parse failure. The generic validation and hint contracts already read still apply. Recorded partition-prune transition warnings, exact-string analyze-version rejection, fixed index-join-build-v2 and merge-statistics-concurrency getters. Resume at 3371; tests and package completion remain pending. No Rust behavior or Ready claim, no push.

Completed sysvar.go through EOF (4404) in three final segments. Direct source coverage is twenty-three artifacts; sysvar_test.go and nested tests remain pending. Fix-control setters parse once and preserve the map on error; new-install default overrides do not include RangeMaxSize. Runtime-filter mode case sensitivity and independent fresh-stats/binding cache flags were recorded. Full variable-package prerequisite remains open until tests are read. No Rust edit or Ready claim; records stay local.

Local test-reading checkpoint: sysvar_test.go lines 1–1200 have been read in contiguous segments. Resume at 1201 inside TestTiDBServerMemoryLimit2; direct complete coverage remains 23/24 and nested tests remain pending. Tests distinguish signed overflow errors from bounded clamping, preserve unsigned LastInsertID values above MaxInt64, and verify that isolation validation uses the current session skip setting. No Rust edits or tests were executed for this checkpoint. Keep these receipt, inventory and ExecPlan changes uncommitted and unpushed until a verified Rust behavior batch is ready.


2026-09-08 Rust ranger WIP: handler-aware index detachment now carries the shared
RangeFallbackHandler through recursive candidate construction and records quota
fallback at construction time. The focused cache-admission regression failed
before wiring and passes afterward. Expanded DNF/recursive coverage exposed a
second mismatch: the DNF accumulator used a hardcoded estimate that omitted
collators and datum payloads, unlike Go Ranges.MemUsage. The composite predicate
(a = 10 and b = 40) or (a = 20 and b = 50), with quota one byte below the full
range footprint, failed before replacing the estimate with ranges_mem_usage.
Both focused tests now pass, including unlimited construction, ordinary residual
DNF without a quota event, and repeated handler calls. Logs:
/tmp/ranger-quota-events-expanded-20260908.log (red),
/tmp/ranger-quota-events-expanded-green-20260908.log (2 passed).
This is WIP: production session/MAX-MIN budget and cache/warning sink integration
remain pending; no Ready claim, commit or push. Documentation-only evidence
continues to remain local under the user's instruction.


2026-09-08 WIP continuation: all 64 ranger unit tests passed before the final
edge-case extensions (/tmp/ranger-suite-20260908.log). Added exact-budget tests
for simple DNF, composite DNF and recursive IN fanout; all retain full access
without warning at equality. Extended shared-handler coverage for forced cache.
The initial expected count of three warnings was incorrect: pinned Go detacher.go
410 and 550 attempt both the IN prefix and the column-condition retry, generating
two fallback events per build. Source inspection confirmed this construction-time
behavior; two builds now assert four risk warnings and one capacity warning,
with cache admission retained. This assertion correction is not a production
bug fix or fail-before evidence. Final focused command:
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner range_quota_events --lib -- --test-threads=1
passed both tests (/tmp/ranger-quota-forced-green-20260908.log). git diff --check
passed. Actual executor StmtContext still uses its own first-reason cache marker;
the utility tracker is not yet connected to that admission consumer. Continue
production budget/warning/cache wiring before Ready and publication. No commit
or push; documentation stays local.


2026-09-08 WIP MAX/MIN quota propagation: RuleContext now carries range_max_size
and an optional shared RangeFallbackHandler; join-reorder's context clone retains
both. check_column_can_use_index passes the same context through selections and
uses its budget/handler for common-handle and secondary-index range detachment.
Go authority: rule_max_min_eliminate.go:88 explicitly supplies SessionVars.RangeMaxSize.
The existing integer-handle/index-order test now also exercises the covering-index
path at quota 1. It failed before the call-site change with 'range quota fallback
must prevent MAX/MIN index admission' (/tmp/maxmin-quota-red-20260908.log), then
passed. Handler assertions additionally require disabled prepared-cache admission,
'in-list is too long', and two warnings. All four MAX/MIN unit tests pass:
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner rule_max_min_elimination --lib -- --test-threads=1
(/tmp/maxmin-quota-handler-20260908.log). git diff --check passes.
The executor RuleContext initializer deliberately remains a WIP scaffold with
zero budget/None handler; session snapshot, actual warning sink, and cache
admission integration are still REQUIRED before Ready/publication. No end-to-end
fix or package completion is claimed. No commit/push; keep documentary changes
local until the verified Rust batch is complete.


2026-09-08 session prerequisite: enumerated all 92 tracked artifacts under
pkg/session at pinned master, including 26 direct package artifacts and nested
packages (34,341 total lines). Exact blobs/line counts/read status are recorded
in rust/docs/planner/session-reading-inventory-20260908.md. Fully read BUILD.bazel,
OWNERS, contextimpl.go, global_init.go and main_test.go (5/26 direct). No session
source/test edit yet: complete source reading remains required before wiring
StatementVarSnapshot. This is local reading progress only, not Ready or parity.


2026-09-08 session prerequisite continuation: fully read session_nextgen_test.go
(180), testutil.go (111), advisory_locks.go (111), mock_bootstrap.go (221),
sync_upgrade.go (155), upgrade_run.go (122). Direct package reading is now 11/26.
Recorded nextgen-only coverage, prepared execution helper lifetime, advisory lock
rollback/reference counting and upgrade retry/MDL ordering in the reading inventory.
No Rust edit/test/Ready run in this reading checkpoint; no commit or push.


2026-09-08 session reading advances to 15/26 direct artifacts: session_test.go,
upgrade_test.go, txnmanager.go and tidb.go completely read (1,262 lines). Recorded
provider publication/retry ordering, parse warning behavior, LOAD DATA LOCAL
retry exclusion, transaction-abort distinctions and bootstrap test coverage in
the inventory. No new Rust edit or runtime validation; prior code remains WIP.
Receipt/ExecPlan updates remain local and unpushed.


2026-09-08 session reading checkpoint: txn.go fully read (766 lines, contiguous
1–390 and 391–EOF); direct package count 16/26. Recorded LazyTxn state transitions,
statement staging/cleanup, memory-hook reset, commit timestamp preservation,
classic/nextgen lock decisions and TSO failure behavior in the reading inventory.
No additional Rust edit, test execution, Ready claim, commit or push.


2026-09-08 reading checkpoint: tidb_test.go and bootstrap.go fully read (1,212
lines), direct session package progress 18/26. Inventory records global initial
value seeding, classic/nextgen schema differences, initialization SQL error policy
and per-statement versus nested-context test contracts. No new Rust edit or
validation in this checkpoint; documentation remains local, uncommitted/unpushed.


2026-09-08 source-reading checkpoint: upgrade_backfill_test.go completely read
(500 lines); direct session package count 19/26. Inventory records persisted-row
versus SQL getter assertions, nextgen exclusions, old binding digest fixture
construction and duplicate/unparsable-row handling. No Rust edit, Ready run,
commit or push; documentary updates remain local.


Session prerequisite continuation: nontransactional.go fully read (873 lines),
bringing direct package reading to 20/26. Inventory records temporary session
overrides, collated shard boundaries, first-job error precedence, dry-run examples
and index eligibility. Existing Rust changes remain WIP; no code edited, tests
run, commit or push in this reading checkpoint.


Session reading checkpoint: starter_bootstrap_file.go fully read (694 lines);
direct count 21/26. Inventory records restricted-SQL restoration, bootstrap versus
upgrade transaction boundaries, two version stores and conditional reset-marker
publication. No additional Rust edit, test, Ready claim, commit or push.


Session prerequisite: starter_bootstrap_file_test.go completely read (928 lines);
direct count 22/26. Recorded rollback versus partial-commit evidence, stale codec
refresh, transient PD publication retry, bounded reset and mock-CAS coverage
limits. No new Rust edit, validation, commit or push; docs stay local.


Session prerequisite cursor: bench_test.go read through line 760, covering
helpers, scan/lookup/point-get/insert/sort/join benchmarks and the beginning of
partition-pruning DDL. Resume at 761; direct completion remains 22/26. Recorded
fixed-query plan-check and row-count helper limitations. Local documentation
only; no commit, push or Ready claim.


Session reading: bench_test.go completed through EOF (2,143 lines total), direct
package completion 23/26. Recorded empty-table partition benchmark limits,
prepared compiler path, daily benchmark exclusions and unchecked pipelined
execution errors. No Ready claim, commit or push; documentation remains local.


Session source-reading cursor: bootstrap_test.go through 650, resume 651; direct
completion stays 23/26. Recorded reserved-ID validation, new-cluster optimizer
default tests and legacy test setup limitations in the inventory. Documentation
remains local; no commit, push or Ready claim.


Session prerequisite reading: bootstrap_test.go 651–1050 complete, resume 1051.
Recorded historical-default replacement versus custom-value preservation and
nonprepared-cache upgrade defaults. Direct completion 23/26; local documents
only, no Ready claim, commit or push.


Session prerequisite reading: bootstrap_test.go completed through line 2011;
direct artifacts now 24/26. session.go and upgrade_def.go remain unread in this
audit; nested packages retain their independent pending status. Inventory records
statistics/cache default migrations and distinguishes index recreation from
preservation and schema precision from timestamp-value assertions. No new Rust
edit or runtime validation in this checkpoint. Receipt and ExecPlan updates stay
local: documentation-only work is not committed or pushed. A Rust batch still
requires focused regression evidence and the Ready validation profile before
commit and push.


Session prerequisite: upgrade_def.go read contiguously through 1400, resume
1401. Recorded the ordered registry and conditional/default-rewrite distinctions,
plus historical binding rewrite behavior. Version97 range-quota compatibility
is currently comment evidence only; implementation reading remains pending.
Direct artifacts remain 24/26 complete. Local documentation only; no new Rust
edit, Ready claim, commit or push.


Session prerequisite: upgrade_def.go fully read (2345 lines), direct completion
25/26; session.go remains. Confirmed v97 backfills zero only for absent range
quota, distinct from fresh-cluster defaults. Recorded statistics-default and
binding-digest migration behavior and nextgen transaction-file inversion in the
inventory. No new Rust edit or Ready validation; documentation remains local,
uncommitted and unpushed.


Session prerequisite cursor: session.go read through 1250; resume inside retry
at 1251. Recorded cache allocation gates, transaction commit/retry exclusions,
staged temporary-table publication and replay context resets. Direct completion
25/26. Local receipt/inventory/ExecPlan updates only; no commit, push or Ready
claim.


Session source reading advanced to 1950; resume 1951. Recorded global-variable
fallback/type-validation boundaries, internal parser mode and execution error
propagation in the inventory. Direct completion remains 25/26. Documentation
only and local; no new Rust edit or Ready claim.


Session reading reached 2650; resume 2651 after compilation timing. Recorded
precompile reset/cancellation ordering and restricted-session warning cleanup.
Noted unnamed-return deferred Close-error limitation from actual code. Direct
completion remains 25/26; documentation only, kept local without commit or push.


Session reading reached 3450; resume DropPreparedStmt at 3451. Inventory records
result finishing/error precedence and prepare-dedup AST/context isolation.
Direct artifacts remain 25/26 complete. Local documentation only; no Ready
claim, commit or push.


Session reading reached 4250, resume 4251. Confirmed GetRangerCtx shares the
statement's PlanCacheTracker and RangeFallbackHandler by pointer, providing
production evidence for the pending Rust integration. Recorded close/prepare
cleanup and cached DistSQL refresh behavior. Direct completion remains 25/26;
local documentation only, no Ready claim, commit or push.


Session source reading reached 5050, resume 5051. Recorded bootstrap ordering,
post-lock version checks and core session context construction. Common-global
loading still pending; direct completion remains 25/26. Documentation is local
only with no new Ready claim, commit or push.


Session prerequisite direct-package reading complete: all 26 direct artifacts,
including session.go 6051 lines. Nested packages remain separately pending.
Confirmed common globals invoke relaxed setters only for absent session values,
with CommonGlobalLoaded set before cache fetch; recorded error and migration
ordering. Ready is not claimed and Rust integration remains unfinished. Local
documentation updates are uncommitted and unpushed per user instruction.


Rust integration WIP: added signed range quota to StatementVarSnapshot and both
statement-context construction paths, using the registered vardef default when
absent. Executor context exposes the value and planner_bridge passes it into
RuleContext instead of hardcoded zero. Shared fallback handler integration is
still unfinished. This scaffold is not validated or ready for publication;
next add focused session regressions with fail-before/pass-after evidence, wire
actual statement warning/cache consumers, then run Ready. No commit or push.


Range quota snapshot regression passes (1 test) for default 67108864, SET to 1,
0 and i64::MAX on both read/DML contexts, and preservation of the previously
built context. Green log: /tmp/session-range-quota-green-20260908.log. Removing
only the two with_range_max_size builder calls reproduces failure; red log:
/tmp/session-range-quota-red-20260908.log. Builder calls restored afterward.
Shared warning/cache tracker wiring and Ready remain pending; no publication.


Shared range fallback WIP: StmtContext owns an Arc<OnceLock> handler/tracker
bundle and a warning adapter sharing its real warning buffer. Query and DML
prepared-plan builders initialize tracking; repeated initialization does not
reenable a rejected cache. RuleContext now receives the handler; cache admission
observes tracker rejection alongside the legacy reason field. Executor check and
one clone/dedup/admission/fresh-context regression pass; logs:
/tmp/range-shared-state-check-20260908.log and
/tmp/range-shared-state-test-20260908.log. Full SQL integration regression,
fail-before proof for this wiring, force/nonprepared behavior reconciliation and
Ready remain pending. This is not complete parity and is not published.


SQL integration regression added in tests_prepared_plan_cache.rs and currently
FAILS: prepared SELECT MAX(b), MIN(b) WHERE a=? with index(a,b), quota1 returns
correct values but SHOW WARNINGS is empty. Log /tmp/range-quota-sql-test-20260908.log.
Initial single-MAX probe also lacked warnings but does not exercise the split
aggregate index check; do not count it as proof of a regression. Multiaggregate
probe still requires path tracing. bind_cached_prepared_select_for_statement
constructs a separate local statement context on cache miss and returns binding
without draining its warnings; inspect alongside actual rule triggering before
fixing. Test currently checks results/warnings; cache flag assertion remains to
be added. No Ready claim or publication.


SQL failure diagnosis: temporary probes confirm MAX/MIN sees two aggregate
functions and a Selection over DataSource, but DataSource.all_possible_access_paths
and possible_access_paths are both empty although indexes contains public visible
ab(a,b). Thus the index loop never reaches range detachment. Probe log:
/tmp/range-quota-sql-probe-20260908.log. Probes removed. Next trace production
access-path initialization/order before treating empty warnings as loss at the
session boundary. Separate cache-miss context remains a potential propagation
gap, not the established cause of this test. No Ready or publication.


Access-path diagnosis refined: Rust plan_builder.rs deliberately initializes
hint-filtered enumerated_paths and unfiltered public_enumerated_paths while
leaving costed DataSourceAccessPath lists empty. MAX/MIN reads only the latter,
so the missing connection is representation consumption, not absent enumeration.
Pinned Go logical_plan_builder.go:5220 onward copies hint-filtered possiblePaths
into AllPossibleAccessPaths before constructing DataSource. Therefore the logical
rule must consume the corresponding filtered newborn paths, preserving order;
using unfiltered public_enumerated_paths would violate hints. Do not fabricate
costed path admissions to satisfy this logical check. Rust fix remains pending;
SQL regression stays failing, no Ready or publication.


MAX/MIN now consumes filtered enumerated_paths when costed paths are absent,
retaining table/index order and integer-handle early rejection. SQL regression
now passes: repeated prepared MAX/MIN executes return correct rows, emit range
quota warnings and never report found_in_plan_cache. Previous failure log:
/tmp/range-quota-sql-test-20260908.log; green:
/tmp/range-quota-sql-path-green-20260908.log. This establishes the early-path
representation gap as the cause of the missing warning in that query. Separate
cache-miss warning propagation, hint/order regressions, force-cache behavior and
Ready still need review. No commit or push.


Added SQL hint/quota regression: IGNORE INDEX(ab) and USE INDEX() suppress the
MAX/MIN range-quota event; FORCE INDEX(ab) emits exactly one capacity warning;
quota zero preserves results and emits none. All three range_quota session tests
pass in /tmp/range-quota-hints-20260908.log. This validates use of filtered paths
and unlimited quota through SQL. Force plan-cache (distinct from FORCE INDEX),
warning propagation across cache-miss contexts and full Ready remain pending.
No commit or push.


Force-cache wiring regression: executor context with fix49736 ON previously
rejected cache on range fallback (runtime red /tmp/range-force-red.log).
start_prepared_range_tracking now transfers that fix-control value to the shared
tracker, matching pinned Go executor/select.go:1263. Both range_fallback executor
tests pass (/tmp/range-force-green.log), including two forced fallback attempts
retaining cache eligibility with three warnings (two risk warnings, one capacity
warning by count). SQL warning propagation and Ready still pending; no push.


Forced-cache SQL regression added and fails at runtime: fix49736 ON, quota1,
prepared MAX/MIN returns correct rows but SHOW WARNINGS lacks the required risk
warning (empty buffer). Evidence /tmp/forced-range-sql-red.log. This is separate
from the now-fixed early-path enumeration gap. Prepared cache miss creates a
local StmtContext in prepared_ast.rs; PreparedSelectExecution carries plan/key
lease only, while dispatch.execute_prepared_select begins another statement
boundary before execution. Planning warnings need ownership across that boundary,
including errors; simply appending before the reset would lose them again.
Current SQL test intentionally remains red pending the integration fix. No Ready
claim, commit or push.


Prepared SELECT execution lease now owns cache-miss planning warnings (separate
from retained cache entry), drained once after nested statement execution even
when it returns an error. Cache hits receive an empty warning list. Forced range
SQL regression now passes (/tmp/forced-range-sql-green.log), against prior runtime
failure /tmp/forced-range-sql-red.log. Remaining: validate repeated cache hits,
warning order/error boundaries and analogous DML/rejected-plan handling; Ready
has not run and no publication is authorized by partial test success alone.


2026-09-08 WIP warning-boundary verification: SELECT and INSERT SELECT execution
leases now carry cache-miss planning warnings independently of cache entries.
DML regression failed with an empty warning buffer before the lease transfer
(/tmp/forced-range-dml-red.log), then passed. Both SELECT and DML regressions
also execute a second time, assert a cache hit, and require no replayed planning
warnings. A separate INSERT IGNORE regression exposed reversed warning order:
execution overflow 1264 preceded planning warnings in
/tmp/forced-range-warning-order-red.log. Dispatch now drains planning warnings
immediately after begin_prepared_statement_boundary and before execution, which
preserves Go's append order and retains them on an execution error. All three
forced_range session regressions pass in
/tmp/forced-range-warning-order-green.log. Command:
`cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-session --lib forced_range`.
`git diff --check` passes. This supersedes the earlier after-execution drain.
Rejected-cache-plan warning transfer still needs investigation: bind's Option
failure discards its local planner context and ordinary execution replans. Ready
has not run; no batch commit or push. Documentation-only updates stay local.


2026-09-08 rejected SELECT candidate regression: strengthening the existing
range_quota_fallback_is_visible_to_sql_and_prepared_cache test to require
`skip prepared plan-cache: in-list is too long` failed at runtime with only the
capacity warning (/tmp/rejected-range-warning-red.log). Pinned Go
pkg/planner/core/plan_cache.go generateNewPlan keeps the newly optimized plan for
execution even when UseCache becomes false; it only skips insertion. Rust's
cached_physical_query_plan previously returned None and lost both plan and
warnings. It now returns the plan plus admission state when StmtContext rejects
caching. The SELECT binder retains a private execution lease without inserting
it into cached_plans or rebuilding its already-bound ranges. The same SQL test
passes twice with correct results, no cache hit, and the required warnings
(/tmp/rejected-range-warning-green.log). Other physical cacheability refusals
still use the old fallback and need separate review. DML's analogous rejection
path remains to fix. No Ready validation, commit or push yet; local WIP only.


2026-09-08 DML rejection continuation: INSERT SELECT regression failed before
retaining the rejected candidate (/tmp/rejected-range-dml-red.log), and passes
afterward (/tmp/rejected-range-dml-green.log). Like SELECT, DML now executes its
already-bound plan once when StmtContext rejects caching, without cache insertion
or range rebuild. Two executions verify correct inserted rows, no cache hits,
the skip-cache warning and exactly one range-capacity warning each.
Expanded prepared-plan-cache module: 30 passed, 1 failed both concurrently and
serially (/tmp/prepared-cache-batch-suite.log and
/tmp/prepared-cache-batch-serial.log). The failing existing test is
expression_and_aggregate_parameters_rebuild_on_cache_hits: parallel HashAgg reads
an empty null bitmap at tidb-chunk/src/column.rs:262. Isolated backtrace is in
/tmp/prepared-aggregate-isolated.log. A controlled HEAD baseline replacing all
modified Rust files temporarily reproduced the identical panic
(/tmp/prepared-aggregate-head-baseline.log); all WIP bytes were restored in a
finally block, and git diff --check passes. This is a verified pre-existing
failure, not a passing module result. Ready/lint and further batch review remain
pending; no commit or push. Documentation stays local until a Rust batch is ready.


2026-09-08 Ready profile execution (not a completed batch): make lint passed
(/tmp/range-batch-ready-lint.log). Targeted cargo offline/locked tests passed:
-p tidb-planner --lib ranger:: (64), --lib max_min (5),
-p tidb-executor --lib range_fallback (2),
-p tidb-session --lib tests_prepared_plan_cache::range_quota (2),
--lib forced_range (3), --lib rejected_range_dml (1).
Logs: /tmp/ranger-batch-ready.log, /tmp/maxmin-batch-ready.log,
/tmp/range-context-batch-ready.log, /tmp/quota-sql-ready.log,
/tmp/forced-sql-ready.log, /tmp/rejected-sql-ready.log.
Overbroad session --lib range_ ran unrelated surfaces: 36 passed, 11 failed,
2 ignored (/tmp/range-session-batch-ready.log). These failures are not yet
individually attributed; do not claim the suite passes. The standalone planner
test-target name was invalid; correct target is --test all with module filter.
That compilation exposed a missing pre-existing RuleContext allow_agg_push_down
initializer in core_logical_cte_topn_prune_source.rs. Added false, matching the
unit test context; the corrected integration command now passes
(/tmp/core-rule-integration-ready-green.log). Final review and failure triage
remain before batch publication. No commit/push; doc-only changes remain local.


2026-09-08 expanded-range failure attribution completed: controlled HEAD run
/tmp/range-session-head-baseline.log reports 29 passed, 11 failed, 2 ignored;
current WIP /tmp/range-session-batch-ready.log reports 36 passed, 11 failed,
2 ignored. All 11 failing names AND panic locations/payloads match exactly after
removing thread IDs. Existing failures cover join range derivation (2), partition
pruning (2), sysbench access (3), and window ranges (4). The added seven passing
tests account for the count difference. All temporary HEAD source replacements
were restored byte-for-byte by finally; git diff --check passed afterward.
These are documented baseline limitations, not new failures or passing tests.
Targeted batch checks and make lint passed as recorded above. Final source/diff
review and package-scoped commit construction remain; no publication yet.


2026-09-08 publication checkpoint: Rust integration batch dbe2add614 was committed
and pushed normally to origin/hparser-integration (fc53443c27..dbe2add614).
Rustfmt --check passed on every changed Rust file; Ready evidence and baseline
limitations are recorded above. Next iteration reviews remaining ranger callers
and physical cacheability refusal behavior before changing further Rust code.
This post-publication checkpoint is documentation-only and remains local under
the user's instruction; no separate documentation commit or push.


Next batch (2026-09-08): ranger callsite audit recorded in
ranger-callsite-audit-20260908.md. Go partition_processor uses session RangeMaxSize
for HASH/KEY and LIST/RANGE pruning; Rust partition bridge hardcoded zero.
static_partition_pruning_respects_range_quota failed with empty warnings before
wiring (/tmp/partition-quota-red.log) and passes after wiring
(/tmp/partition-quota-green.log). Added handler-aware partition entry preserving
convert_to_sort_key=false/merge_consecutive=false, and threaded StmtContext through
LIST COLUMNS CNF/DNF helpers. Cache rebuild zero-quota calls remain unchanged.
This is WIP: expand partition variants, cache interaction and unlimited-budget
coverage; then run Ready before a separate meaningful Rust commit/push.


2026-09-08 partition batch coverage: static_list_columns_quota_preserves_recursive_predicates
passes for nested AND/OR predicates over LIST COLUMNS(a,b), checking all three
expected rows, one deduplicated warning at quota1, and no warning at quota0
(/tmp/list-columns-quota-test.log). Prepared HASH regression changes both IN
parameters between executions, verifies the two correct rows each time, no cache
hit and one capacity warning (/tmp/partition-quota-prepared.log). This validates
results and admission but does not yet attribute the admission refusal to quota
rather than the static-partition cacheability gate. No such stronger claim is
made. rustfmt applied and git diff --check passes. Remaining: other partition
variants, partition wrapper option contracts, Ready tests/lint and final review.
No new commit/push; all current documentation is local.


2026-09-08 partition quota batch Ready evidence: four SQL regressions cover HASH,
KEY, RANGE, RANGE COLUMNS, LIST and recursive LIST COLUMNS; quota1 preserves rows
and emits one warning, quota0 emits none. Prepared HASH checks changing parameters
and no hit. All four passed in /tmp/partition-sql-ready.log. The original HASH
regression failed before the fix with empty warnings (/tmp/partition-quota-red.log).
All 64 ranger tests passed (/tmp/partition-ranger-ready.log), and both partition
rule tests passed (/tmp/partition-rule-ready.log). Commands use
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked
with -p tidb-session --lib tests_prepared_plan_cache::static_,
-p tidb-planner --lib ranger::, and -p tidb-planner --lib rule_partition_processor.
make lint passed (/tmp/partition-batch-ready-lint.log). rustfmt and diff checks
passed. Final review confirms partition wrapper keeps sort-key conversion and
consecutive-range merging disabled, recursive calls share one statement handler,
and cache rebuild zero-quota paths are unchanged. This batch addresses the
pkg/planner/core/rule partition quota integration; whole-package parity remains
incomplete. It includes Rust changes, so accompanying receipt updates can ship
with the meaningful fix commit under the user's publication instruction.
