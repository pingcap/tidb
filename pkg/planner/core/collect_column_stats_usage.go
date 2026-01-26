// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/intset"
)

// columnStatsUsageCollector collects predicate columns and/or histogram-needed columns from logical plan.
// Predicate columns are the columns whose statistics are utilized when making query plans, which usually occur in where conditions, join conditions and so on.
// Histogram-needed columns are the columns whose histograms are utilized when making query plans, which usually occur in the conditions pushed down to DataSource.
// The set of histogram-needed columns is the subset of that of predicate columns.
// TODO: The collected predicate columns will be used to decide whether to load statistics for the columns. And we need some special handling for partition table
// when the prune mode is static. We can remove such handling when the static partition pruning is totally deprecated.
type columnStatsUsageCollector struct {
	// predicateCols records predicate columns.
	// The bool value indicates whether we need a full stats for it.
	// If its value is false, we just need the least meta info(like NDV) of the column in this SQL.
	predicateCols map[model.TableItemID]bool
	// colMap maps expression.Column.UniqueID to the table columns whose statistics may be utilized to calculate statistics of the column.
	// It is used for collecting predicate columns.
	// For example, in `select count(distinct a, b) as e from t`, the count of column `e` is calculated as `max(ndv(t.a), ndv(t.b))` if
	// we don't know `ndv(t.a, t.b)`(see (*LogicalAggregation).DeriveStats and getColsNDV for details). So when calculating the statistics
	// of column `e`, we may use the statistics of column `t.a` and `t.b`.
	colMap map[int64]map[model.TableItemID]struct{}
	// cols is used to store columns collected from expressions and saves some allocation.
	cols []*expression.Column

	// visitedPhysTblIDs all ds.PhysicalTableID that have been visited.
	// It's always collected, even collectHistNeededColumns is not set.
	visitedPhysTblIDs *intset.FastIntSet

	// collectVisitedTable indicates whether to collect visited table
	collectVisitedTable bool
	// visitedtbls indicates the visited table
	visitedtbls map[int64]struct{}

	// tblID2PartitionIDs is used for tables with static pruning mode.
	// Note that we've no longer suggested to use static pruning mode.
	tblID2PartitionIDs map[int64][]int64
<<<<<<< HEAD:pkg/planner/core/collect_column_stats_usage.go
=======

	// operatorNum is the number of operators in the logical plan.
	operatorNum uint64

	// interestingColsByDS tracks all columns of interest for index pruning (WHERE + JOIN + ORDERING)
	interestingColsByDS map[*logicalop.DataSource][]*expression.Column
	// Temporary storage for deduplication
	colSet map[int64]struct{}
>>>>>>> 79b2debe2a9 (planner: index pruning using existing infra (#64999)):pkg/planner/core/rule/collect_column_stats_usage.go
}

func newColumnStatsUsageCollector(enabledPlanCapture bool, collectIndexPruningCols bool) *columnStatsUsageCollector {
	set := intset.NewFastIntSet()
	collector := &columnStatsUsageCollector{
		// Pre-allocate a slice to reduce allocation, 8 doesn't have special meaning.
		cols:               make([]*expression.Column, 0, 8),
		visitedPhysTblIDs:  &set,
		tblID2PartitionIDs: make(map[int64][]int64),
	}
	collector.predicateCols = make(map[model.TableItemID]bool)
	collector.colMap = make(map[int64]map[model.TableItemID]struct{})
	if enabledPlanCapture {
		collector.collectVisitedTable = true
		collector.visitedtbls = map[int64]struct{}{}
	}
	if collectIndexPruningCols {
		collector.interestingColsByDS = make(map[*logicalop.DataSource][]*expression.Column)
		collector.colSet = make(map[int64]struct{})
	}
	return collector
}

func (c *columnStatsUsageCollector) addPredicateColumn(col *expression.Column, needFullStats bool) {
	tblColIDs, ok := c.colMap[col.UniqueID]
	if !ok {
		// It may happen if some leaf of logical plan is LogicalMemTable/LogicalShow/LogicalShowDDLJobs.
		return
	}
	for tblColID := range tblColIDs {
		fullLoad, found := c.predicateCols[tblColID]
		// It's already marked as full stats. Skip it.
		if fullLoad {
			continue
		}
		// If it's found and marked as meta stats, and the passed mark this time is also meta stats. Skip it.
		if found && !fullLoad && !needFullStats {
			continue
		}
		c.predicateCols[tblColID] = needFullStats
	}
}

func (c *columnStatsUsageCollector) addPredicateColumnsFromExpressions(list []expression.Expression, needFullStats bool) {
	cols := expression.ExtractColumnsAndCorColumnsFromExpressions(c.cols[:0], list)
	for _, col := range cols {
		c.addPredicateColumn(col, needFullStats)
	}
}

func (c *columnStatsUsageCollector) updateColMap(col *expression.Column, relatedCols []*expression.Column) {
	if _, ok := c.colMap[col.UniqueID]; !ok {
		c.colMap[col.UniqueID] = map[model.TableItemID]struct{}{}
	}
	for _, relatedCol := range relatedCols {
		tblColIDs, ok := c.colMap[relatedCol.UniqueID]
		if !ok {
			// It may happen if some leaf of logical plan is LogicalMemTable/LogicalShow/LogicalShowDDLJobs.
			continue
		}
		for tblColID := range tblColIDs {
			c.colMap[col.UniqueID][tblColID] = struct{}{}
		}
	}
}

func (c *columnStatsUsageCollector) updateColMapFromExpressions(col *expression.Column, list []expression.Expression) {
	c.updateColMap(col, expression.ExtractColumnsAndCorColumnsFromExpressions(c.cols[:0], list))
}

func (c *columnStatsUsageCollector) collectPredicateColumnsForDataSource(ds *logicalop.DataSource) {
	// Skip all system tables.
	if filter.IsSystemSchema(ds.DBName.L) {
		intest.Assert(!ds.SCtx().GetSessionVars().InRestrictedSQL, "system table should have been skipped in restricted SQL mode")
		return
	}
	// For partition tables, no matter whether it is static or dynamic pruning mode, we use table ID rather than partition ID to
	// set TableColumnID.TableID. In this way, we keep the set of predicate columns consistent between different partitions and global table.
	tblID := ds.TableInfo.ID
	if c.collectVisitedTable {
		c.visitedtbls[tblID] = struct{}{}
	}
	c.visitedPhysTblIDs.Insert(int(tblID))
	if tblID != ds.PhysicalTableID {
		c.tblID2PartitionIDs[tblID] = append(c.tblID2PartitionIDs[tblID], ds.PhysicalTableID)
	}
	for _, col := range ds.Schema().Columns {
		tblColID := model.TableItemID{TableID: tblID, ID: col.ID, IsIndex: false}
		c.colMap[col.UniqueID] = map[model.TableItemID]struct{}{tblColID: {}}
	}
	// We should use `PushedDownConds` here. `AllConds` is used for partition pruning, which doesn't need stats.
	c.addPredicateColumnsFromExpressions(ds.PushedDownConds, true)
	// Note: Interesting columns (WHERE + JOIN + ORDERING) for index pruning are collected during the same traversal
	// in collectFromPlan via collectInterestingColumnsForDataSource method.
}

func (c *columnStatsUsageCollector) collectPredicateColumnsForJoin(p *logicalop.LogicalJoin) {
	// The only schema change is merging two schemas so there is no new column.
	// Assume statistics of all the columns in EqualConditions/LeftConditions/RightConditions/OtherConditions are needed.
	exprs := make([]expression.Expression, 0, len(p.EqualConditions)+len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))
	for _, cond := range p.EqualConditions {
		exprs = append(exprs, cond)
	}
	for _, cond := range p.LeftConditions {
		exprs = append(exprs, cond)
	}
	for _, cond := range p.RightConditions {
		exprs = append(exprs, cond)
	}
	for _, cond := range p.OtherConditions {
		exprs = append(exprs, cond)
	}
	// Currently, join predicates only need meta info like NDV.
	c.addPredicateColumnsFromExpressions(exprs, false)
}

func (c *columnStatsUsageCollector) collectPredicateColumnsForUnionAll(p *logicalop.LogicalUnionAll) {
	// statistics of the ith column of UnionAll come from statistics of the ith column of each child.
	schemas := make([]*expression.Schema, 0, len(p.Children()))
	relatedCols := make([]*expression.Column, 0, len(p.Children()))
	for _, child := range p.Children() {
		schemas = append(schemas, child.Schema())
	}
	for i, col := range p.Schema().Columns {
		relatedCols = relatedCols[:0]
		for j := range p.Children() {
			relatedCols = append(relatedCols, schemas[j].Columns[i])
		}
		c.updateColMap(col, relatedCols)
	}
}

<<<<<<< HEAD:pkg/planner/core/collect_column_stats_usage.go
func (c *columnStatsUsageCollector) collectFromPlan(lp base.LogicalPlan) {
	for _, child := range lp.Children() {
		c.collectFromPlan(child)
=======
// collectInterestingColumnsForDataSource collects all interesting columns (WHERE + JOIN + ORDERING + GROUP BY) for a DataSource.
func (c *columnStatsUsageCollector) collectInterestingColumnsForDataSource(ds *logicalop.DataSource, accumulatedJoinCols []*expression.Column, accumulatedOrderingCols []*expression.Column) {
	// Clear the map for reuse instead of allocating a new one
	clear(c.colSet)
	var allCols []*expression.Column

	// Collect WHERE columns from local filter predicates
	allConds := make([]expression.Expression, 0, len(ds.PushedDownConds)+len(ds.AllConds))
	allConds = append(allConds, ds.PushedDownConds...)
	allConds = append(allConds, ds.AllConds...)

	for _, cond := range allConds {
		condCols := expression.ExtractColumns(cond)
		// Only keep columns from this DataSource (local WHERE conditions)
		allLocal := true
		for _, col := range condCols {
			if !ds.Schema().Contains(col) {
				allLocal = false
				break
			}
		}
		if allLocal {
			for _, col := range condCols {
				if _, exists := c.colSet[col.UniqueID]; !exists {
					allCols = append(allCols, col)
					c.colSet[col.UniqueID] = struct{}{}
				}
			}
		}
	}

	// Add join columns
	for _, col := range accumulatedJoinCols {
		if ds.Schema().Contains(col) {
			if _, exists := c.colSet[col.UniqueID]; !exists {
				allCols = append(allCols, col)
				c.colSet[col.UniqueID] = struct{}{}
			}
		}
	}

	// Add ordering columns
	for _, col := range accumulatedOrderingCols {
		if ds.Schema().Contains(col) {
			if _, exists := c.colSet[col.UniqueID]; !exists {
				allCols = append(allCols, col)
				c.colSet[col.UniqueID] = struct{}{}
			}
		}
	}

	c.interestingColsByDS[ds] = allCols
}

// collectFromPlan will dive into the tree to collect base column stats usage, in this process
// we also make the use of the dive process down to passing the parent operator's column groups
// requirement to notify the underlying datasource to maintain the possible group ndv.
// accumulatedJoinCols and accumulatedOrderingCols (which includes GROUP BY columns) are propagated down the tree for index pruning.
func (c *columnStatsUsageCollector) collectFromPlan(askedColGroups [][]*expression.Column, lp base.LogicalPlan, accumulatedJoinCols []*expression.Column, accumulatedOrderingCols []*expression.Column) {
	// derive the new current op's new asked column groups accordingly.
	curColGroups := lp.ExtractColGroups(askedColGroups)

	// Track columns for index pruning as we traverse
	currentJoinCols := accumulatedJoinCols
	currentOrderingCols := accumulatedOrderingCols

	// Extract join and ordering columns from this node before visiting children
	if c.interestingColsByDS != nil {
		switch x := lp.(type) {
		case *logicalop.LogicalJoin:
			// Extract join columns from EqualConditions and OtherConditions
			currentJoinCols = append(currentJoinCols, expression.ExtractColumnsFromExpressions(expression.ScalarFuncs2Exprs(x.EqualConditions), nil)...)
			currentJoinCols = append(currentJoinCols, expression.ExtractColumnsFromExpressions(x.OtherConditions, nil)...)
		case *logicalop.LogicalApply:
			currentJoinCols = append(currentJoinCols, expression.ExtractColumnsFromExpressions(expression.ScalarFuncs2Exprs(x.EqualConditions), nil)...)
			currentJoinCols = append(currentJoinCols, expression.ExtractColumnsFromExpressions(x.OtherConditions, nil)...)
		case *logicalop.LogicalSort:
			for _, item := range x.ByItems {
				currentOrderingCols = append(currentOrderingCols, expression.ExtractColumns(item.Expr)...)
			}
		case *logicalop.LogicalTopN:
			for _, item := range x.ByItems {
				currentOrderingCols = append(currentOrderingCols, expression.ExtractColumns(item.Expr)...)
			}
		case *logicalop.LogicalWindow:
			for _, item := range x.OrderBy {
				currentOrderingCols = append(currentOrderingCols, item.Col)
			}
		case *logicalop.LogicalAggregation:
			// GROUP BY columns benefit from indexes (similar to ordering)
			currentOrderingCols = append(currentOrderingCols, expression.ExtractColumnsFromExpressions(x.GroupByItems, nil)...)
			// MIN/MAX aggregates can benefit from ordered indexes
			for _, aggFunc := range x.AggFuncs {
				if aggFunc.Name == ast.AggFuncMin || aggFunc.Name == ast.AggFuncMax {
					currentOrderingCols = append(currentOrderingCols, expression.ExtractColumnsFromExpressions(aggFunc.Args, nil)...)
				}
			}
		}
>>>>>>> 79b2debe2a9 (planner: index pruning using existing infra (#64999)):pkg/planner/core/rule/collect_column_stats_usage.go
	}

	for _, child := range lp.Children() {
		// passing the new asked column groups down, along with accumulated join/ordering columns
		c.collectFromPlan(curColGroups, child, currentJoinCols, currentOrderingCols)
	}

	switch x := lp.(type) {
	case *logicalop.DataSource:
<<<<<<< HEAD:pkg/planner/core/collect_column_stats_usage.go
		c.collectPredicateColumnsForDataSource(x)
=======
		c.collectPredicateColumnsForDataSource(askedColGroups, x)
		// Collect all interesting columns (WHERE + JOIN + ORDERING) for index pruning
		if c.interestingColsByDS != nil {
			c.collectInterestingColumnsForDataSource(x, currentJoinCols, currentOrderingCols)
		}
>>>>>>> 79b2debe2a9 (planner: index pruning using existing infra (#64999)):pkg/planner/core/rule/collect_column_stats_usage.go
	case *logicalop.LogicalIndexScan:
		c.collectPredicateColumnsForDataSource(x.Source)
		c.addPredicateColumnsFromExpressions(x.AccessConds, true)
	case *logicalop.LogicalTableScan:
		c.collectPredicateColumnsForDataSource(x.Source)
		c.addPredicateColumnsFromExpressions(x.AccessConds, true)
	case *logicalop.LogicalProjection:
		// Schema change from children to self.
		schema := x.Schema()
		for i, expr := range x.Exprs {
			c.updateColMapFromExpressions(schema.Columns[i], []expression.Expression{expr})
		}
	case *logicalop.LogicalSelection:
		// Though the conditions in LogicalSelection are complex conditions which cannot be pushed down to DataSource, we still
		// regard statistics of the columns in the conditions as needed.
		c.addPredicateColumnsFromExpressions(x.Conditions, false)
	case *logicalop.LogicalAggregation:
		// Just assume statistics of all the columns in GroupByItems are needed.
		c.addPredicateColumnsFromExpressions(x.GroupByItems, false)
		// Schema change from children to self.
		schema := x.Schema()
		for i, aggFunc := range x.AggFuncs {
			c.updateColMapFromExpressions(schema.Columns[i], aggFunc.Args)
		}
	case *logicalop.LogicalWindow:
		// Statistics of the columns in LogicalWindow.PartitionBy are used in optimizeByShuffle4Window.
		// We don't use statistics of the columns in LogicalWindow.OrderBy currently.
		for _, item := range x.PartitionBy {
			c.addPredicateColumn(item.Col, false)
		}
		// Schema change from children to self.
		windowColumns := x.GetWindowResultColumns()
		for i, col := range windowColumns {
			c.updateColMapFromExpressions(col, x.WindowFuncDescs[i].Args)
		}
	case *logicalop.LogicalJoin:
		c.collectPredicateColumnsForJoin(x)
	case *logicalop.LogicalApply:
		c.collectPredicateColumnsForJoin(&x.LogicalJoin)
		// Assume statistics of correlated columns are needed.
		// Correlated columns can be found in LogicalApply.Children()[0].Schema(). Since we already visit LogicalApply.Children()[0],
		// correlated columns must have existed in columnStatsUsageCollector.colMap.
		for _, corCols := range x.CorCols {
			c.addPredicateColumn(&corCols.Column, false)
		}
	case *logicalop.LogicalSort:
		// Assume statistics of all the columns in ByItems are needed.
		for _, item := range x.ByItems {
			c.addPredicateColumnsFromExpressions([]expression.Expression{item.Expr}, false)
		}
	case *logicalop.LogicalTopN:
		// Assume statistics of all the columns in ByItems are needed.
		for _, item := range x.ByItems {
			c.addPredicateColumnsFromExpressions([]expression.Expression{item.Expr}, false)
		}
	case *logicalop.LogicalUnionAll:
		c.collectPredicateColumnsForUnionAll(x)
	case *logicalop.LogicalPartitionUnionAll:
		c.collectPredicateColumnsForUnionAll(&x.LogicalUnionAll)
	case *logicalop.LogicalCTE:
		// Visit SeedPartLogicalPlan and RecursivePartLogicalPlan first.
<<<<<<< HEAD:pkg/planner/core/collect_column_stats_usage.go
		c.collectFromPlan(x.Cte.SeedPartLogicalPlan)
		if x.Cte.RecursivePartLogicalPlan != nil {
			c.collectFromPlan(x.Cte.RecursivePartLogicalPlan)
=======
		c.collectFromPlan(nil, x.Cte.SeedPartLogicalPlan, nil, nil)
		if x.Cte.RecursivePartLogicalPlan != nil {
			c.collectFromPlan(nil, x.Cte.RecursivePartLogicalPlan, nil, nil)
>>>>>>> 79b2debe2a9 (planner: index pruning using existing infra (#64999)):pkg/planner/core/rule/collect_column_stats_usage.go
		}
		// Schema change from seedPlan/recursivePlan to self.
		columns := x.Schema().Columns
		seedColumns := x.Cte.SeedPartLogicalPlan.Schema().Columns
		var recursiveColumns []*expression.Column
		if x.Cte.RecursivePartLogicalPlan != nil {
			recursiveColumns = x.Cte.RecursivePartLogicalPlan.Schema().Columns
		}
		relatedCols := make([]*expression.Column, 0, 2)
		for i, col := range columns {
			relatedCols = append(relatedCols[:0], seedColumns[i])
			if recursiveColumns != nil {
				relatedCols = append(relatedCols, recursiveColumns[i])
			}
			c.updateColMap(col, relatedCols)
		}
		// If IsDistinct is true, then we use getColsNDV to calculate row count(see (*LogicalCTE).DeriveStat). In this case
		// statistics of all the columns are needed.
		if x.Cte.IsDistinct {
			for _, col := range columns {
				c.addPredicateColumn(col, false)
			}
		}
	case *logicalop.LogicalCTETable:
		// Schema change from seedPlan to self.
		for i, col := range x.Schema().Columns {
			c.updateColMap(col, []*expression.Column{x.SeedSchema.Columns[i]})
		}
	}
}

// CollectColumnStatsUsage collects column stats usage from logical plan.
// predicate indicates whether to collect predicate columns and histNeeded indicates whether to collect histogram-needed columns.
// The predicate columns are always collected while the histNeeded columns are depending on whether we use sync load.
// First return value: predicate columns
// Second return value: the visited table IDs(For partition table, we only record its global meta ID. The meta ID of each partition will be recorded in tblID2PartitionIDs)
// Third return value: the visited partition IDs. Used for static partition pruning.
// TODO: remove the third return value when the static partition pruning is totally deprecated.
func CollectColumnStatsUsage(lp base.LogicalPlan) (
	map[model.TableItemID]bool,
	*intset.FastIntSet,
	map[int64][]int64,
) {
<<<<<<< HEAD:pkg/planner/core/collect_column_stats_usage.go
	collector := newColumnStatsUsageCollector(lp.SCtx().GetSessionVars().IsPlanReplayerCaptureEnabled())
	collector.collectFromPlan(lp)
	if collector.collectVisitedTable {
		recordTableRuntimeStats(lp.SCtx(), collector.visitedtbls)
	}
	return collector.predicateCols, collector.visitedPhysTblIDs, collector.tblID2PartitionIDs
=======
	// Check if we need to collect index pruning columns
	threshold := lp.SCtx().GetSessionVars().OptIndexPruneThreshold
	collectIndexPruningCols := threshold >= 0

	collector := newColumnStatsUsageCollector(lp.SCtx().GetSessionVars().IsPlanReplayerCaptureEnabled(), collectIndexPruningCols)
	collector.collectFromPlan(nil, lp, nil, nil)
	if collector.collectVisitedTable {
		recordTableRuntimeStats(lp.SCtx(), collector.visitedtbls)
	}

	// Populate DataSource field with the collected interesting columns (if index pruning is enabled)
	if collectIndexPruningCols {
		for ds, cols := range collector.interestingColsByDS {
			ds.InterestingColumns = cols
		}
	}

	return collector.predicateCols, collector.visitedPhysTblIDs, collector.tblID2PartitionIDs, collector.operatorNum
>>>>>>> 79b2debe2a9 (planner: index pruning using existing infra (#64999)):pkg/planner/core/rule/collect_column_stats_usage.go
}
