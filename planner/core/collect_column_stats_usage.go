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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/model"
)

const (
	collectPredicateColumns uint64 = 1 << iota
	collectHistNeededColumns
)

// columnStatsUsageCollector collects predicate columns and/or histogram-needed columns from logical plan.
// Predicate columns are the columns whose statistics are utilized when making query plans, which usually occur in where conditions, join conditions and so on.
// Histogram-needed columns are the columns whose histograms are utilized when making query plans, which usually occur in the conditions pushed down to DataSource.
// The set of histogram-needed columns is the subset of that of predicate columns.
type columnStatsUsageCollector struct {
	// collectMode indicates whether to collect predicate columns and/or histogram-needed columns
	collectMode uint64
	// predicateCols records predicate columns.
	predicateCols map[model.TableColumnID]struct{}
	// colMap maps expression.Column.UniqueID to the table columns whose statistics may be utilized to calculate statistics of the column.
	// It is used for collecting predicate columns.
	// For example, in `select count(distinct a, b) as e from t`, the count of column `e` is calculated as `max(ndv(t.a), ndv(t.b))` if
	// we don't know `ndv(t.a, t.b)`(see (*LogicalAggregation).DeriveStats and getColsNDV for details). So when calculating the statistics
	// of column `e`, we may use the statistics of column `t.a` and `t.b`.
	colMap map[int64]map[model.TableColumnID]struct{}
	// histNeededCols records histogram-needed columns
	histNeededCols map[model.TableColumnID]struct{}
	// cols is used to store columns collected from expressions and saves some allocation.
	cols []*expression.Column
}

func newColumnStatsUsageCollector(collectMode uint64) *columnStatsUsageCollector {
	collector := &columnStatsUsageCollector{
		collectMode: collectMode,
		// Pre-allocate a slice to reduce allocation, 8 doesn't have special meaning.
		cols: make([]*expression.Column, 0, 8),
	}
	if collectMode&collectPredicateColumns != 0 {
		collector.predicateCols = make(map[model.TableColumnID]struct{})
		collector.colMap = make(map[int64]map[model.TableColumnID]struct{})
	}
	if collectMode&collectHistNeededColumns != 0 {
		collector.histNeededCols = make(map[model.TableColumnID]struct{})
	}
	return collector
}

func (c *columnStatsUsageCollector) addPredicateColumn(col *expression.Column) {
	tblColIDs, ok := c.colMap[col.UniqueID]
	if !ok {
		// It may happen if some leaf of logical plan is LogicalMemTable/LogicalShow/LogicalShowDDLJobs.
		return
	}
	for tblColID := range tblColIDs {
		c.predicateCols[tblColID] = struct{}{}
	}
}

func (c *columnStatsUsageCollector) addPredicateColumnsFromExpressions(list []expression.Expression) {
	cols := expression.ExtractColumnsAndCorColumnsFromExpressions(c.cols[:0], list)
	for _, col := range cols {
		c.addPredicateColumn(col)
	}
}

func (c *columnStatsUsageCollector) updateColMap(col *expression.Column, relatedCols []*expression.Column) {
	if _, ok := c.colMap[col.UniqueID]; !ok {
		c.colMap[col.UniqueID] = map[model.TableColumnID]struct{}{}
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

func (c *columnStatsUsageCollector) collectPredicateColumnsForDataSource(ds *DataSource) {
	// For partition tables, no matter whether it is static or dynamic pruning mode, we use table ID rather than partition ID to
	// set TableColumnID.TableID. In this way, we keep the set of predicate columns consistent between different partitions and global table.
	tblID := ds.TableInfo().ID
	for _, col := range ds.Schema().Columns {
		tblColID := model.TableColumnID{TableID: tblID, ColumnID: col.ID}
		c.colMap[col.UniqueID] = map[model.TableColumnID]struct{}{tblColID: {}}
	}
	// We should use `pushedDownConds` here. `allConds` is used for partition pruning, which doesn't need stats.
	c.addPredicateColumnsFromExpressions(ds.pushedDownConds)
}

func (c *columnStatsUsageCollector) collectPredicateColumnsForJoin(p *LogicalJoin) {
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
	c.addPredicateColumnsFromExpressions(exprs)
}

func (c *columnStatsUsageCollector) collectPredicateColumnsForUnionAll(p *LogicalUnionAll) {
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

func (c *columnStatsUsageCollector) addHistNeededColumns(ds *DataSource) {
	columns := expression.ExtractColumnsFromExpressions(c.cols[:0], ds.pushedDownConds, nil)
	for _, col := range columns {
		tblColID := model.TableColumnID{TableID: ds.physicalTableID, ColumnID: col.ID}
		c.histNeededCols[tblColID] = struct{}{}
	}
}

func (c *columnStatsUsageCollector) collectFromPlan(lp LogicalPlan) {
	for _, child := range lp.Children() {
		c.collectFromPlan(child)
	}
	if c.collectMode&collectPredicateColumns != 0 {
		switch x := lp.(type) {
		case *DataSource:
			c.collectPredicateColumnsForDataSource(x)
		case *LogicalIndexScan:
			c.collectPredicateColumnsForDataSource(x.Source)
			c.addPredicateColumnsFromExpressions(x.AccessConds)
		case *LogicalTableScan:
			c.collectPredicateColumnsForDataSource(x.Source)
			c.addPredicateColumnsFromExpressions(x.AccessConds)
		case *LogicalProjection:
			// Schema change from children to self.
			schema := x.Schema()
			for i, expr := range x.Exprs {
				c.updateColMapFromExpressions(schema.Columns[i], []expression.Expression{expr})
			}
		case *LogicalSelection:
			// Though the conditions in LogicalSelection are complex conditions which cannot be pushed down to DataSource, we still
			// regard statistics of the columns in the conditions as needed.
			c.addPredicateColumnsFromExpressions(x.Conditions)
		case *LogicalAggregation:
			// Just assume statistics of all the columns in GroupByItems are needed.
			c.addPredicateColumnsFromExpressions(x.GroupByItems)
			// Schema change from children to self.
			schema := x.Schema()
			for i, aggFunc := range x.AggFuncs {
				c.updateColMapFromExpressions(schema.Columns[i], aggFunc.Args)
			}
		case *LogicalWindow:
			// Statistics of the columns in LogicalWindow.PartitionBy are used in optimizeByShuffle4Window.
			// We don't use statistics of the columns in LogicalWindow.OrderBy currently.
			for _, item := range x.PartitionBy {
				c.addPredicateColumn(item.Col)
			}
			// Schema change from children to self.
			windowColumns := x.GetWindowResultColumns()
			for i, col := range windowColumns {
				c.updateColMapFromExpressions(col, x.WindowFuncDescs[i].Args)
			}
		case *LogicalJoin:
			c.collectPredicateColumnsForJoin(x)
		case *LogicalApply:
			c.collectPredicateColumnsForJoin(&x.LogicalJoin)
			// Assume statistics of correlated columns are needed.
			// Correlated columns can be found in LogicalApply.Children()[0].Schema(). Since we already visit LogicalApply.Children()[0],
			// correlated columns must have existed in columnStatsUsageCollector.colMap.
			for _, corCols := range x.CorCols {
				c.addPredicateColumn(&corCols.Column)
			}
		case *LogicalSort:
			// Assume statistics of all the columns in ByItems are needed.
			for _, item := range x.ByItems {
				c.addPredicateColumnsFromExpressions([]expression.Expression{item.Expr})
			}
		case *LogicalTopN:
			// Assume statistics of all the columns in ByItems are needed.
			for _, item := range x.ByItems {
				c.addPredicateColumnsFromExpressions([]expression.Expression{item.Expr})
			}
		case *LogicalUnionAll:
			c.collectPredicateColumnsForUnionAll(x)
		case *LogicalPartitionUnionAll:
			c.collectPredicateColumnsForUnionAll(&x.LogicalUnionAll)
		case *LogicalCTE:
			// Visit seedPartLogicalPlan and recursivePartLogicalPlan first.
			c.collectFromPlan(x.cte.seedPartLogicalPlan)
			if x.cte.recursivePartLogicalPlan != nil {
				c.collectFromPlan(x.cte.recursivePartLogicalPlan)
			}
			// Schema change from seedPlan/recursivePlan to self.
			columns := x.Schema().Columns
			seedColumns := x.cte.seedPartLogicalPlan.Schema().Columns
			var recursiveColumns []*expression.Column
			if x.cte.recursivePartLogicalPlan != nil {
				recursiveColumns = x.cte.recursivePartLogicalPlan.Schema().Columns
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
			if x.cte.IsDistinct {
				for _, col := range columns {
					c.addPredicateColumn(col)
				}
			}
		case *LogicalCTETable:
			// Schema change from seedPlan to self.
			for i, col := range x.Schema().Columns {
				c.updateColMap(col, []*expression.Column{x.seedSchema.Columns[i]})
			}
		}
	}
	if c.collectMode&collectHistNeededColumns != 0 {
		// Histogram-needed columns are the columns which occur in the conditions pushed down to DataSource.
		// We don't consider LogicalCTE because seedLogicalPlan and recursiveLogicalPlan haven't got logical optimization
		// yet(seedLogicalPlan and recursiveLogicalPlan are optimized in DeriveStats phase). Without logical optimization,
		// there is no condition pushed down to DataSource so no histogram-needed column can be collected.
		switch x := lp.(type) {
		case *DataSource:
			c.addHistNeededColumns(x)
		case *LogicalIndexScan:
			c.addHistNeededColumns(x.Source)
		case *LogicalTableScan:
			c.addHistNeededColumns(x.Source)
		}
	}
}

// CollectColumnStatsUsage collects column stats usage from logical plan.
// predicate indicates whether to collect predicate columns and histNeeded indicates whether to collect histogram-needed columns.
// The first return value is predicate columns(nil if predicate is false) and the second return value is histogram-needed columns(nil if histNeeded is false).
func CollectColumnStatsUsage(lp LogicalPlan, predicate, histNeeded bool) ([]model.TableColumnID, []model.TableColumnID) {
	var mode uint64
	if predicate {
		mode |= collectPredicateColumns
	}
	if histNeeded {
		mode |= collectHistNeededColumns
	}
	collector := newColumnStatsUsageCollector(mode)
	collector.collectFromPlan(lp)
	set2slice := func(set map[model.TableColumnID]struct{}) []model.TableColumnID {
		ret := make([]model.TableColumnID, 0, len(set))
		for tblColID := range set {
			ret = append(ret, tblColID)
		}
		return ret
	}
	var predicateCols, histNeededCols []model.TableColumnID
	if predicate {
		predicateCols = set2slice(collector.predicateCols)
	}
	if histNeeded {
		histNeededCols = set2slice(collector.histNeededCols)
	}
	return predicateCols, histNeededCols
}
