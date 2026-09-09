# Planner core/rule current-master reading inventory

Authority: `f5cf8f6337612c6ae51fb6e384e4bb3469dde680`. Direct package only; `util` remains a separate package.

This is a reading checkpoint, not a package parity or Ready claim. Full reading is required before production edits. The prior published constraint batch remains separate.

| Artifact | Lines | Git blob | Read fully this audit |
| --- | ---: | --- | --- |
| `pkg/planner/core/rule/BUILD.bazel` | 97 | `0370e23680967216351f4b8031e7df6f0ec06a4d` | yes |
| `pkg/planner/core/rule/collect_column_stats_usage.go` | 452 | `ebb009a4dc62fd22e0d97ff071dcde5e55ac0b66` | yes |
| `pkg/planner/core/rule/collect_column_stats_usage_test.go` | 433 | `18d22b4296dd7d34f53b7f4fa9b96108eee64955` | yes |
| `pkg/planner/core/rule/logical_rules.go` | 61 | `71ab6499a1826afee27643ad4b82267f3866063d` | yes |
| `pkg/planner/core/rule/rule_build_key_info.go` | 41 | `ea592997736df83ae04180fbb9901a94944b1622` | yes |
| `pkg/planner/core/rule/rule_collect_plan_stats.go` | 564 | `464a2de06e5ddb1184590c663acbcb94a0804f6a` | yes |
| `pkg/planner/core/rule/rule_column_pruning.go` | 72 | `44e167316e438fdbef1a67be54c320b09bd83362` | yes |
| `pkg/planner/core/rule/rule_constant_propagation.go` | 86 | `6c700663979ae09cbfae626bdd2e9154905217d1` | yes |
| `pkg/planner/core/rule/rule_init.go` | 30 | `86be352c0b8b16875d1b4a7bf3d314f3839cbd88` | yes |
| `pkg/planner/core/rule/rule_join_key_type_cast.go` | 326 | `3947b4ab16e0458b7f1af041584d9d1da7fc2e07` | yes |
| `pkg/planner/core/rule/rule_max_min_eliminate.go` | 267 | `0752f3c2ff8c1726a96b394143673a21a323f1dc` | yes |
| `pkg/planner/core/rule/rule_max_min_eliminate_test.go` | 37 | `717d8c412fdcce386f30f5c93a5b4fbee3c7f320` | yes |
| `pkg/planner/core/rule/rule_order_aware_join_reorder.go` | 245 | `478aacfb6a03d92f2a4cc295ef16e125d8d0d8d4` | yes |
| `pkg/planner/core/rule/rule_outer_join_to_semi_join.go` | 401 | `89d5571124f44e7183a8400c685cf42346b8035a` | yes |
| `pkg/planner/core/rule/rule_partition_processor.go` | 2149 | `aa10aeca23244e122920bafd8230ce31a39fdf55` | yes |
| `pkg/planner/core/rule/rule_partition_pruning_test.go` | 681 | `eb35bff5185437356161b9e60a3445033c66fe87` | yes |
| `pkg/planner/core/rule/rule_predicate_simplification.go` | 601 | `78a0f01a0262298876afd07fdde8e13f25b62ee2` | yes |
| `pkg/planner/core/rule/rule_prune_indexes.go` | 837 | `f62f7b51238b5733ade1eac8a6d3f24359a884de` | yes |
| `pkg/planner/core/rule/rule_prune_indexes_internal_test.go` | 80 | `244e0efeb91847d1e3879cd23262bfcc284cd5f3` | yes |

Total: 19 artifacts, 7460 lines. No direct fixture directory, doc.go, generated/platform variant, or additional tracked build artifact was found. BUILD.bazel declares fourteen production sources and four test files.

## Declaration inventory

These declarations were extracted to track coverage; extraction alone is not evidence that a pending file has been read.

### pkg/planner/core/rule/BUILD.bazel


### pkg/planner/core/rule/collect_column_stats_usage.go

- `newColumnStatsUsageCollector(collectIndexPruningCols bool) *columnStatsUsageCollector`
- `(c *columnStatsUsageCollector) addPredicateColumn(col *expression.Column, needFullStats bool)`
- `(c *columnStatsUsageCollector) addPredicateColumnsFromExpressions(list []expression.Expression, needFullStats bool)`
- `(c *columnStatsUsageCollector) updateColMap(col *expression.Column, relatedCols []*expression.Column)`
- `(c *columnStatsUsageCollector) updateColMapFromExpressions(col *expression.Column, list []expression.Expression)`
- `(c *columnStatsUsageCollector) collectPredicateColumnsForDataSource(askedColGroups [][]*expression.Column, ds *logicalop.DataSource)`
- `(c *columnStatsUsageCollector) collectPredicateColumnsForJoin(p *logicalop.LogicalJoin)`
- `(c *columnStatsUsageCollector) collectPredicateColumnsForUnionAll(p *logicalop.LogicalUnionAll)`
- `(c *columnStatsUsageCollector) collectInterestingColumnsForDataSource(ds *logicalop.DataSource, accumulatedJoinCols []*expression.Column, accumulatedOrderingCols []*expression.Column)`
- `(c *columnStatsUsageCollector) collectFromPlan(askedColGroups [][]*expression.Column, lp base.LogicalPlan, accumulatedJoinCols []*expression.Column, accumulatedOrderingCols []*expression.Column)`
- `CollectColumnStatsUsage(lp base.LogicalPlan) (`

### pkg/planner/core/rule/collect_column_stats_usage_test.go

- `getTblInfoByPhyID(t *testing.T, is infoschema.InfoSchema, physicalTblID int64) (*model.TableInfo, string)`
- `getColumnName(t *testing.T, is infoschema.InfoSchema, tblColID model.TableItemID, comment string) string`
- `getStatsLoadItem(t *testing.T, is infoschema.InfoSchema, item model.StatsLoadItem, comment string) string`
- `checkColumnStatsUsageForPredicates(t *testing.T, is infoschema.InfoSchema, lp base.LogicalPlan, expected []string, comment string)`
- `checkColumnStatsUsageForStatsLoad(t *testing.T, is infoschema.InfoSchema, lp base.LogicalPlan, expectedCols []string, expectedParts map[string][]string, comment string)`
- `TestSkipSystemTables(t *testing.T)`
- `TestCollectPredicateColumns(t *testing.T)`
- `TestCollectHistNeededColumns(t *testing.T)`

### pkg/planner/core/rule/logical_rules.go

- `setPredicatePushDownFlag(u uint64) uint64`

### pkg/planner/core/rule/rule_build_key_info.go

- `(*BuildKeySolver) Name() string`
- `(*BuildKeySolver) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error)`

### pkg/planner/core/rule/rule_collect_plan_stats.go

- `(c *CollectPredicateColumnsPoint) Optimize(_ context.Context, plan base.LogicalPlan) (base.LogicalPlan, bool, error)`
- `(*CollectPredicateColumnsPoint) markAtLeastOneFullStatsLoadForEachTable(`
- `(c *CollectPredicateColumnsPoint) pruneIndexesForAllDataSources(plan base.LogicalPlan) map[int64]map[int64]struct`
- `(c *CollectPredicateColumnsPoint) collectAndPruneDataSources(plan base.LogicalPlan, keptIndexIDs map[int64]map[int64]struct`
- `pruneIndexesForDataSource(ds *logicalop.DataSource, keptIndexIDs map[int64]map[int64]struct`
- `(CollectPredicateColumnsPoint) expandStatsNeededColumnsForStaticPruning(`
- `(CollectPredicateColumnsPoint) Name() string`
- `(SyncWaitStatsLoadPoint) Optimize(_ context.Context, plan base.LogicalPlan) (base.LogicalPlan, bool, error)`
- `(SyncWaitStatsLoadPoint) Name() string`
- `RequestLoadStats(ctx base.PlanContext, neededHistItems []model.StatsLoadItem, syncWait int64) error`
- `SyncWaitStatsLoad(plan base.LogicalPlan) error`
- `CollectDependingVirtualCols(tblID2Tbl map[int64]*model.TableInfo, neededItems []model.StatsLoadItem) []model.StatsLoadItem`
- `collectSyncIndices(ctx base.PlanContext,`
- `collectHistNeededItems(histNeededColumns []model.StatsLoadItem, histNeededIndices map[model.TableItemID]struct`
- `recordTableRuntimeStats(sctx base.PlanContext, tbls map[int64]struct`
- `recordSingleTableRuntimeStats(sctx base.PlanContext, tblID int64) (stats *statistics.Table, skip bool, err error)`

### pkg/planner/core/rule/rule_column_pruning.go

- `(*ColumnPruner) Optimize(_ context.Context, lp base.LogicalPlan) (base.LogicalPlan, bool, error)`
- `noUnexpectedZeroColumnSchema(p base.LogicalPlan) bool`
- `(*ColumnPruner) Name() string`

### pkg/planner/core/rule/rule_constant_propagation.go

- `(cp *ConstantPropagationSolver) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error)`
- `(cp *ConstantPropagationSolver) execOptimize(currentPlan base.LogicalPlan, parentPlan base.LogicalPlan, currentChildIdx int)`
- `(*ConstantPropagationSolver) Name() string`

### pkg/planner/core/rule/rule_init.go

- `init()`

### pkg/planner/core/rule/rule_join_key_type_cast.go

- `(*JoinKeyTypeCastRewriter) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error)`
- `(*JoinKeyTypeCastRewriter) Name() string`
- `rewriteJoinTypeCasts(p base.LogicalPlan) (base.LogicalPlan, bool)`
- `rewriteJoinEqConds(join *logicalop.LogicalJoin) bool`
- `findCastInProj(proj *logicalop.LogicalProjection, col *expression.Column, evalCtx expression.EvalContext) *projCastInfo`
- `classifyCastPair(leftInfo, rightInfo *projCastInfo) (intInfo, strInfo *projCastInfo)`

### pkg/planner/core/rule/rule_max_min_eliminate.go

- `(a *MaxMinEliminator) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error)`
- `(*MaxMinEliminator) composeAggsByInnerJoin(aggs []*logicalop.LogicalAggregation) (plan base.LogicalPlan)`
- `(a *MaxMinEliminator) checkColCanUseIndex(plan base.LogicalPlan, col *expression.Column, conditions []expression.Expression) bool`
- `(a *MaxMinEliminator) cloneSubPlans(plan base.LogicalPlan) base.LogicalPlan`
- `(a *MaxMinEliminator) splitAggFuncAndCheckIndices(agg *logicalop.LogicalAggregation) (aggs []*logicalop.LogicalAggregation, canEliminate bool)`
- `(*MaxMinEliminator) eliminateSingleMaxMin(agg *logicalop.LogicalAggregation) *logicalop.LogicalAggregation`
- `(a *MaxMinEliminator) eliminateMaxMin(p base.LogicalPlan) base.LogicalPlan`
- `(*MaxMinEliminator) Name() string`

### pkg/planner/core/rule/rule_max_min_eliminate_test.go

- `TestMaxMinEliminateSkipsEmptyScalarAgg(t *testing.T)`

### pkg/planner/core/rule/rule_order_aware_join_reorder.go

- `(r *OrderAwareJoinReorder) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error)`
- `(r *OrderAwareJoinReorder) optimizeRecursive(`
- `(r *OrderAwareJoinReorder) optimizeChildren(`
- `shouldUseCDCBasedJoinReorder(p base.LogicalPlan) bool`
- `extractOrderingColumns(items []*plannerutil.ByItems) []*expression.Column`
- `sameOrderingColumns(left, right []*expression.Column) bool`
- `rewriteOrderingForProjection(`
- `(*OrderAwareJoinReorder) Name() string`

### pkg/planner/core/rule/rule_outer_join_to_semi_join.go

- `(o *OuterJoinToSemiJoin) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error)`
- `(o *OuterJoinToSemiJoin) recursivePlan(p base.LogicalPlan) (base.LogicalPlan, bool)`
- `(o *OuterJoinToSemiJoin) dealWithSelection(p base.LogicalPlan, childIdx int, sel *logicalop.LogicalSelection) (base.LogicalPlan, bool)`
- `(o *OuterJoinToSemiJoin) startConvertOuterJoinToSemiJoin(p base.LogicalPlan, childIdx int, sel *logicalop.LogicalSelection, join *logicalop.LogicalJoin) (base.LogicalPlan, bool)`
- `ensureSelectionRoot(p base.LogicalPlan, sel *logicalop.LogicalSelection) base.LogicalPlan`
- `resetChildIfChanged(parent, oldChild, newChild base.LogicalPlan)`
- `(*OuterJoinToSemiJoin) Name() string`
- `canConvertAntiJoin(p *logicalop.LogicalJoin, selectCond []expression.Expression, selectSch *expression.Schema) (resultProj *logicalop.LogicalProjection, canConvertToAntiSemiJoin bool)`
- `validProjForConvertAntiJoin(proj *logicalop.LogicalProjection) bool`
- `joinCondNullRejectsInnerCol(innerSchSet *intset.FastIntSet, isNullColumnID int64, eq []*expression.ScalarFunction, other []expression.Expression) bool`
- `generateProjectForConvertAntiJoin(p *logicalop.LogicalJoin, innerSchSet *intset.FastIntSet, outerSchema, parentNodeSchema *expression.Schema) (proj *logicalop.LogicalProjection)`

### pkg/planner/core/rule/rule_partition_processor.go

- `(s *PartitionProcessor) Optimize(_ context.Context, lp base.LogicalPlan) (base.LogicalPlan, bool, error)`
- `(*PartitionProcessor) Name() string`
- `(s *PartitionProcessor) rewriteDataSource(lp base.LogicalPlan) (base.LogicalPlan, error)`
- `generateHashPartitionExpr(ctx base.PlanContext, pi *model.PartitionInfo, columns []*expression.Column, names types.NameSlice) (expression.Expression, error)`
- `getPartColumnsForHashPartition(hashExpr expression.Expression) ([]*expression.Column, []int)`
- `(s *PartitionProcessor) getUsedHashPartitions(ctx base.PlanContext,`
- `(s *PartitionProcessor) getUsedKeyPartitions(ctx base.PlanContext,`
- `(s *PartitionProcessor) getUsedPartitions(ctx base.PlanContext, tbl table.Table,`
- `(s *PartitionProcessor) findUsedPartitions(ctx base.PlanContext,`
- `(s *PartitionProcessor) ConvertToIntSlice(or PartitionRangeOR, pi *model.PartitionInfo, partitionNames []ast.CIStr) []int`
- `convertToRangeOr(used []int, pi *model.PartitionInfo) PartitionRangeOR`
- `(s *PartitionProcessor) PruneHashOrKeyPartition(ctx base.PlanContext, tbl table.Table, partitionNames []ast.CIStr,`
- `ReconstructTableColNames(ds *logicalop.DataSource) (types.NameSlice, error)`
- `(s *PartitionProcessor) processHashOrKeyPartition(ds *logicalop.DataSource, pi *model.PartitionInfo) (base.LogicalPlan, error)`
- `newListPartitionPruner(ctx base.PlanContext, tbl table.Table, partitionNames []ast.CIStr, s *PartitionProcessor, pruneList *tables.ForListPruning, columns []*expression.Column) *listPartitionPruner`
- `(l *listPartitionPruner) locatePartition(cond expression.Expression) (tables.ListPartitionLocation, bool, error)`
- `(l *listPartitionPruner) locatePartitionByCNFCondition(conds []expression.Expression) (tables.ListPartitionLocation, bool, error)`
- `(l *listPartitionPruner) locatePartitionByDNFCondition(conds []expression.Expression) (tables.ListPartitionLocation, bool, error)`
- `(l *listPartitionPruner) locatePartitionByColumn(cond *expression.ScalarFunction) (tables.ListPartitionLocation, bool, error)`
- `(l *listPartitionPruner) locateColumnPartitionsByCondition(cond expression.Expression, colPrune *tables.ForListColumnPruning) (tables.ListPartitionLocation, bool, error)`
- `(l *listPartitionPruner) detachCondAndBuildRange(conds []expression.Expression, exprCols ...*expression.Column) ([]*ranger.Range, error)`
- `(l *listPartitionPruner) findUsedListColumnsPartitions(conds []expression.Expression) (map[int]struct`
- `(l *listPartitionPruner) findUsedListPartitions(conds []expression.Expression) (map[int]struct`
- `(s *PartitionProcessor) findUsedListPartitions(ctx base.PlanContext, tbl table.Table, partitionNames []ast.CIStr,`
- `(s *PartitionProcessor) PruneListPartition(ctx base.PlanContext, tbl table.Table, partitionNames []ast.CIStr,`
- `(s *PartitionProcessor) prune(ds *logicalop.DataSource) (base.LogicalPlan, error)`
- `(*PartitionProcessor) FindByName(partitionNames []ast.CIStr, partitionName string) bool`
- `(lt *LessThanDataInt) Length() int`
- `(lt *LessThanDataInt) compare(ith int, v int64, unsigned bool) int`
- `(p *PartitionRange) Cmp(a PartitionRange) int`
- `GetFullRange(end int) PartitionRangeOR`
- `(or PartitionRangeOR) IntersectionRange(start, end int) PartitionRangeOR`
- `(or PartitionRangeOR) Len() int`
- `(or PartitionRangeOR) Union(x PartitionRangeOR) PartitionRangeOR`
- `(or PartitionRangeOR) simplify() PartitionRangeOR`
- `(or PartitionRangeOR) Intersection(x PartitionRangeOR) PartitionRangeOR`
- `intersectionRange(start, end, newStart, newEnd int) (s int, e int)`
- `(s *PartitionProcessor) PruneRangePartition(ctx base.PlanContext, pi *model.PartitionInfo, tbl table.PartitionedTable, conds []expression.Expression,`
- `(s *PartitionProcessor) processRangePartition(ds *logicalop.DataSource, pi *model.PartitionInfo) (base.LogicalPlan, error)`
- `(s *PartitionProcessor) processListPartition(ds *logicalop.DataSource, pi *model.PartitionInfo) (base.LogicalPlan, error)`
- `MakePartitionByFnCol(sctx base.PlanContext, columns []*expression.Column, names types.NameSlice, partitionExpr string) (*expression.Column, *expression.ScalarFunction, monotoneMode, error)`
- `minCmp(ctx base.PlanContext, lowVal []types.Datum, columnsPruner *RangeColumnsPruner, comparer []collate.Collator, lowExclude bool, gotError *bool) func(i int) bool`
- `maxCmp(ctx base.PlanContext, hiVal []types.Datum, columnsPruner *RangeColumnsPruner, comparer []collate.Collator, hiExclude bool, gotError *bool) func(i int) bool`
- `multiColumnRangeColumnsPruner(sctx base.PlanContext, exprs []expression.Expression,`
- `PartitionRangeForCNFExpr(sctx base.PlanContext, exprs []expression.Expression,`
- `PartitionRangeForExpr(sctx base.PlanContext, expr expression.Expression,`
- `(p *RangePruner) partitionRangeForExpr(sctx base.PlanContext, expr expression.Expression) (start int, end int, ok bool)`
- `(p *RangePruner) fullRange() PartitionRangeOR`
- `partitionRangeForOrExpr(sctx base.PlanContext, expr1, expr2 expression.Expression,`
- `partitionRangeColumnForInExpr(sctx base.PlanContext, args []expression.Expression,`
- `partitionRangeForInExpr(sctx base.PlanContext, args []expression.Expression,`
- `getMonotoneMode(fnName string) monotoneMode`
- `(p *RangePruner) extractDataForPrune(sctx base.PlanContext, expr expression.Expression) (DataForPrune, bool)`
- `replaceColumnWithConst(partFn *expression.ScalarFunction, con *expression.Constant) *expression.ScalarFunction`
- `opposite(op string) string`
- `relaxOP(op string) string`
- `PruneUseBinarySearch(lessThan LessThanDataInt, data DataForPrune) (start int, end int)`
- `(*PartitionProcessor) resolveAccessPaths(ds *logicalop.DataSource) error`
- `(s *PartitionProcessor) resolveOptimizeHint(ds *logicalop.DataSource, partitionName ast.CIStr) error`
- `checkTableHintsApplicableForPartition(partitions []ast.CIStr, partitionSet set.StringSet) []string`
- `appendWarnForUnknownPartitions(ctx base.PlanContext, hintName string, unknownPartitions []string)`
- `(*PartitionProcessor) checkHintsApplicable(ds *logicalop.DataSource, partitionSet set.StringSet)`
- `(s *PartitionProcessor) makeUnionAllChildren(ds *logicalop.DataSource, pi *model.PartitionInfo, or PartitionRangeOR) (base.LogicalPlan, error)`
- `(*PartitionProcessor) pruneRangeColumnsPartition(ctx base.PlanContext, conds []expression.Expression, pi *model.PartitionInfo, pe *tables.PartitionExpr, columns []*expression.Column) (PartitionRangeOR, error)`
- `makeRangeColumnPruner(columns []*expression.Column, pi *model.PartitionInfo, from *tables.ForRangeColumnsPruning, offsets []int) (*RangeColumnsPruner, error)`
- `(p *RangeColumnsPruner) fullRange() PartitionRangeOR`
- `(p *RangeColumnsPruner) getPartCol(colID int64) *expression.Column`
- `(p *RangeColumnsPruner) partitionRangeForExpr(sctx base.PlanContext, expr expression.Expression) (start int, end int, ok bool)`
- `(p *RangeColumnsPruner) pruneUseBinarySearch(sctx base.PlanContext, op string, data *expression.Constant) (start int, end int)`
- `PushDownNot(ctx expression.BuildContext, conds []expression.Expression) []expression.Expression`

### pkg/planner/core/rule/rule_partition_pruning_test.go

- `TestCanBePrune(t *testing.T)`
- `TestPruneUseBinarySearchSigned(t *testing.T)`
- `TestPruneUseBinarySearchUnSigned(t *testing.T)`
- `prepareBenchCtx(createTable string, partitionExpr string) *testCtx`
- `prepareTestCtx(t *testing.T, createTable string, partitionExpr string) *testCtx`
- `(tc *testCtx) expr(expr string) expression.Expression`
- `TestPartitionRangeForExpr(t *testing.T)`
- `TestPartitionRangeOperation(t *testing.T)`
- `TestPartitionRangePruner2VarChar(t *testing.T)`
- `TestPartitionRangePruner2CharWithCollation(t *testing.T)`
- `TestPartitionRangePruner2Date(t *testing.T)`
- `TestPartitionRangeColumnsForExpr(t *testing.T)`
- `TestPartitionRangeColumnsForExprWithSpecialCollation(t *testing.T)`
- `benchmarkRangeColumnsPruner(b *testing.B, parts int)`
- `BenchmarkRangeColumnsPruner2(b *testing.B)`
- `BenchmarkRangeColumnsPruner10(b *testing.B)`
- `BenchmarkRangeColumnsPruner100(b *testing.B)`
- `BenchmarkRangeColumnsPruner1000(b *testing.B)`
- `BenchmarkRangeColumnsPruner8000(b *testing.B)`

### pkg/planner/core/rule/rule_predicate_simplification.go

- `logicalConstant(bc base.PlanContext, cond expression.Expression) predicateType`
- `FindPredicateType(bc base.PlanContext, expr expression.Expression) (*expression.Column, predicateType)`
- `(*PredicateSimplification) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error)`
- `updateInPredicate(ctx base.PlanContext, inPredicate expression.Expression, notEQPredicate expression.Expression) (expression.Expression, bool)`
- `applyPredicateSimplificationForJoin(sctx base.PlanContext, predicates []expression.Expression,`
- `applyPredicateSimplification(sctx base.PlanContext, predicates []expression.Expression, propagateConstant bool,`
- `applyPredicateSimplificationHelper(sctx base.PlanContext, predicates []expression.Expression,`
- `mergeInAndNotEQLists(sctx base.PlanContext, predicates []expression.Expression) []expression.Expression`
- `unsatisfiableExpression(ctx base.PlanContext, p expression.Expression) bool`
- `unsatisfiable(ctx base.PlanContext, p1, p2 expression.Expression) bool`
- `binaryComparisonPredicate(predType predicateType) bool`
- `isNullInListContradiction(ctx base.PlanContext, p1 expression.Expression, p1Type predicateType, p2 expression.Expression, p2Type predicateType) bool`
- `comparisonPred(predType predicateType) predicateType`
- `prunableORBranchPredicate(predType predicateType) bool`
- `updateOrPredicate(ctx base.PlanContext, orPredicateList expression.Expression, scalarPredicatePtr expression.Expression) (expression.Expression, bool)`
- `pruneEmptyORBranches(sctx base.PlanContext, predicates []expression.Expression) []expression.Expression`
- `shortCircuitANDORLogicalConstants(sctx base.PlanContext, predicate expression.Expression, orCase bool) (expression.Expression, bool)`
- `processCondition(sctx base.PlanContext, condition expression.Expression) (expression.Expression, predicateType)`
- `shortCircuitLogicalConstants(sctx base.PlanContext, predicates []expression.Expression) []expression.Expression`
- `removeRedundantORBranch(sctx base.PlanContext, predicates []expression.Expression)`
- `recursiveRemoveRedundantORBranch(sctx base.PlanContext, predicate expression.Expression) expression.Expression`
- `(*PredicateSimplification) Name() string`

### pkg/planner/core/rule/rule_prune_indexes.go

- `ShouldPreferIndexMerge(ds *logicalop.DataSource) bool`
- `PruneIndexesByWhereAndOrder(ds *logicalop.DataSource, paths []*util.AccessPath, interestingColumns []*expression.Column, threshold int) []*util.AccessPath`
- `buildColumnRequirements(interestingColumns []*expression.Column) columnRequirements`
- `collectEqOrInBoundColIDs(conds []expression.Expression) map[int64]struct`
- `discountedHandlePrefixCols(ds *logicalop.DataSource, interestingColIDs map[int64]struct`
- `buildOrderingKey(columnIDs []int64) string`
- `buildCoveredSetKey(info indexWithScore) string`
- `isPrefixOf(short, long []int64) bool`
- `isDominatedCoverage(entry scoredIndex, state *indexSelectionState) bool`
- `effectiveIndexColumnIDs(ds *logicalop.DataSource, path *util.AccessPath) []int64`
- `scoreIndexPath(`
- `scoreAndSort(indexes []indexWithScore, req columnRequirements) []scoredIndex`
- `buildFinalResult(tablePaths, mvIndexPaths, indexMergeIndexPaths []*util.AccessPath, preferredIndexes []indexWithScore, maxToKeep int, onlyPruneZeroScore bool, req columnRequirements) []*util.AccessPath`
- `newIndexSelectionState(phase1Limit, maxToKeep int) *indexSelectionState`
- `selectIndexes(preferredScored []scoredIndex, added map[*util.AccessPath]struct`
- `shouldAddIndex(entry scoredIndex, path *util.AccessPath, req columnRequirements, state *indexSelectionState) bool`
- `recordCoverage(entry scoredIndex, state *indexSelectionState)`
- `shouldAddIndexWithConsecutive(entry scoredIndex, state *indexSelectionState) bool`
- `shouldAddIndexWithoutConsecutive(entry scoredIndex, path *util.AccessPath, req columnRequirements, state *indexSelectionState) bool`
- `findSingleInterestingColumn(path *util.AccessPath, req columnRequirements) int64`
- `calculateScoreFromCoverage(info indexWithScore, totalColumns int, isSingleScan bool) int`

### pkg/planner/core/rule/rule_prune_indexes_internal_test.go

- `TestEffectiveIndexColumnIDsWithUnresolvedColumn(t *testing.T)`
- `TestScoreIndexPathPartialIndexBadOffset(t *testing.T)`

## Findings and next steps

- The older ExecPlan inventory omitted `rule_prune_indexes_internal_test.go`; both tests have now been read.
- `logicalConstant` in the Rust predicate simplifier discards conversion events before the corrected constraint helper. Read all remaining direct package artifacts, then implement the statement-policy correction with fail-before/pass-after regressions.
- Review all Rust owners and original test mappings before making any renewed whole-package completion claim.

## Second reading checkpoint

Four more files were read in full: the 245-line order-aware join rule,
452-line statistics-column collector, its 433-line test matrix, and the
401-line outer-to-anti join rule. Fourteen of nineteen direct artifacts are
now read; the five remaining files remain explicitly pending above.

Verified Go requirements to reconcile with Rust: internal column IDs <= 0
must not request persisted statistics; logical table IDs unify partition
predicate usage while physical partition IDs are recorded separately; full
histogram demand monotonically upgrades metadata-only demand; CTE seed and
recursive plans contribute lineage and operator counts; descending ordering
prevents the order-aware leading preference; outer-to-anti conversion uses
inner FD non-null evidence before schema fallback and preserves original
nullable field types in NULL-generating projections. These observations are
source evidence, not a claim that their Rust implementations already match.

## Third reading checkpoint

Read the complete 564-line statistics-load rule and 601-line predicate
simplification rule. Sixteen of nineteen direct artifacts are now read.
The remaining files are the partition processor, partition pruning tests,
and index-pruning implementation.

Source findings: `processCondition` classifies before and after processing,
including plain constants; conversion warnings therefore depend on both
calls. Sync-load request/wait failures set the statement failure flag; pseudo
fallback additionally disables plan cache and appends a warning. Extra
analyzed-column loading happens after recording predicate usage. Virtual
column demand follows only direct dependencies and is used for expression
index demand rather than loading virtual-column histograms. Static expansion
retains original logical-table items and appends physical-partition copies.
Kept index sets are unioned across aliases only when actual pruning occurs.
These requirements remain to be reconciled against Rust; no production edits
or package completion claim were made in this reading checkpoint.

## Fourth reading checkpoint

Read all 837 lines of index pruning and all 681 lines of partition pruning
tests, including helper setup, every expected range and five benchmark entry
points (2, 10, 100, 1000, 8000 partitions). Eighteen of nineteen direct
artifacts are fully read. Only `rule_partition_processor.go` (2149 lines)
remains before production edits.

Index-pruning contracts: table and MV paths are categorized before forced-path
handling; specifically hinted merge indexes survive scoring; ordinary paths
rank by the effective declared-plus-handle key; unresolved columns break the
usable prefix. Discounted clustered-key-prefix columns still contribute to
scores, but non-covering indexes whose only coverage is that prefix are
redundant. Domination requires the same covered-column set and a selected
prefix extending the candidate prefix. Zero-score-only mode bypasses diversity
selection. Static unresolved-column fallback cannot infer consecutive order.

Partition tests preserve the disabled issue-12028 assertion as disabled source
evidence rather than claiming it as passing coverage. Active tests exercise
signed/unsigned comparisons, NULL, invalid operators, CNF/DNF intersections,
range unions, CHAR/VARCHAR collation, DATE, tuple ranges, and mixed-column
collation. These are reading findings awaiting Rust reconciliation.
