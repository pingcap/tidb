# Complete pinned planner rule packages without Rust-only behavior

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. The pinned Go revision is `e2788410d8d696605e8cb002585877a063ccc909`.

## Purpose / Big Picture

Rust currently exposes Go's logical-rule list but several entries are missing, narrowed, or implemented through duplicate helper paths. The observable goal is that the ordinary Rust planner performs the same logical rewrites as the pinned Go planner, with the same plan shapes and SQL results, and contains no Rust-only optimizer policies. Package completion is claimed only after every production source, original test/support artifact, and build artifact in one Go package has an inventory and validation receipt.

## Progress

- [x] (2026-08-29) Inventoried pinned `pkg/planner/core/rule` and its nested, separate Go package `pkg/planner/core/rule/util`.
- [x] (2026-08-29) Wired the static `PartitionProcessor` rule into the ordinary logical and physical execution path; committed and pushed as `d6285efd11`.
- [x] (2026-08-30) Completed `pkg/planner/core/rule/util` as the first atomic package: every artifact and production symbol in pinned `misc.go` and `BUILD.bazel` has one Rust owner; centralized expression replacement, column-set tests, nullable-key max-one-row behavior, metadata-based unique-index key derivation, flag hook, iterative key-info portal, and both predicate-simplification hook signatures including validity-filter forwarding are integrated; duplicate CTE/projection/index-key bodies are removed; locked focused tests, the consuming session build, `make lint`, and `git diff --check` pass.
- [ ] Audit every direct artifact in `pkg/planner/core/rule`, mapping production symbols and original tests to Rust owners.
- [ ] Implement dependency-closed missing rule bodies; when a body depends on an incomplete Go package, complete that dependency package before claiming this package.
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

Work is in progress. Static partition planning is integrated and pushed. The nested `rule/util` package is the first atomically completed package against the pinned Go tree; the parent `rule` package is not claimed complete.

The registered predicate-simplification body and ordinary/inner/outer-join propagation are integrated as dependency work. The final utility audit additionally found and fixed two subtle divergences: the ordinary simplification hook had discarded Go's validity filter, and unique-index key strength had read expression-schema flags instead of `ColumnInfo` metadata. Mutation evidence proves the filter regression test fails when forwarding is disabled. Package completion is still withheld pending the remaining parent-rule inventory and Ready gates.

The `pkg/planner/core/rule/util` package receipt is pinned to Go revision `e2788410d8d696605e8cb002585877a063ccc909`. Its complete inventory is `misc.go` and `BUILD.bazel`; there are no package-local tests, fixtures, generated inputs/outputs, build-tag or platform variants, benchmarks, fuzz targets, or examples. Validation passed with `cargo test --locked -p tidb-planner --lib rule_util -- --nocapture`, the two consuming regressions, `cargo check --locked -p tidb-session`, `make lint`, and `git diff --check`.

The nested `pkg/statistics/handle/usage/collector` receipt is pinned to the same Go revision. Its complete inventory is `collector.go`, `collector_test.go`, and `BUILD.bazel`; there are no fixtures, generated artifacts, build-tag or platform variants, benchmarks, fuzz targets, or examples. `cargo test --locked -p tidb-stats-handle-usage-collector -- --nocapture`, `make lint`, and `git diff --check` pass.

The nested `pkg/statistics/handle/usage/indexusage` receipt is pinned to the same Go revision. Its complete inventory is `collector.go`, `collector_test.go`, and `BUILD.bazel`; the benchmark is declared inside the original test file and maps to Rust's `benches/collector.rs`; there are no fixtures, generated artifacts, build-tag or platform variants, fuzz targets, or examples. `cargo test --locked -p tidb-stats-handle-usage-indexusage -- --nocapture`, `cargo bench --locked -p tidb-stats-handle-usage-indexusage --no-run`, `cargo check --locked -p tidb-session`, `make lint`, and `git diff --check` pass.

The nested `pkg/statistics/handle/usage/predicatecolumn` receipt is pinned to the same Go revision. Its complete inventory is `predicate_column.go` and `BUILD.bazel`; there are no package-local tests, fixtures, generated inputs or outputs, build-tag or platform variants, benchmarks, fuzz targets, or examples. The native Rust owners are split across `cluster_predicate_column.rs` and `cluster_stats_write.rs`, with executable coverage in `analyze_commit_size_source.rs`. Validation passed with the three focused `cargo test --locked -p tidb-exec --test all <test-name> -- --nocapture` invocations for table-item-kind writes, load/cleanup, and timestamp replacement, `cargo check --locked -p tidb-server`, `make lint`, and `git diff --check`. Restoring the removed `IsIndex` filter made the table-item-kind regression fail, providing fail-before evidence.

## Context and Orientation

The parent pinned package contains `BUILD.bazel`, thirteen production `.go` files, four original `_test.go` files, and the nested `util` package. The Rust rule driver is `rust/crates/tidb-planner/src/logical/rule.rs`; tree rewrites are in `logical/rewrite.rs`; rule-specific bodies are `logical/rule_*.rs`. Executor-owned catalog and partition-expression access is in `rust/crates/tidb-executor/src/driver/planner_bridge.rs`.

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
