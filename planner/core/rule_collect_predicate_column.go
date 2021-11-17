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
	"context"
	"github.com/pingcap/tidb/expression"

	"github.com/pingcap/tidb/parser/model"
)

// predicateColumnCollector collects predicate columns from logical plan. Predicate columns are the columns whose statistics
// are utilized when making query plans, which usually occur in where conditions, join conditions and so on.
type predicateColumnCollector struct{
	// colMap maps expression.Column.UniqueID to the table columns whose statistics are utilized to calculate statistics of the column.
	colMap map[int64]map[model.TableColumnID]struct{}
	// predicateCols records predicate columns.
	predicateCols map[model.TableColumnID]struct{}
}

func (c *predicateColumnCollector) optimize(ctx context.Context, p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	c.collectPredicateColumns(p)
	return p, nil
}

func (c *predicateColumnCollector) addPredicateColumn(col *expression.Column) {
	tblColIDs, ok := c.colMap[col.UniqueID]
	if !ok {
		// It may happen if some leaf of logical plan is LogicalMemTable/LogicalShow/LogicalShowDDLJobs.
		return
	}
	for tblColID := range tblColIDs {
		c.predicateCols[tblColID] = struct{}{}
	}
}

func (c *predicateColumnCollector) addPredicateColumnsFromExpression(expr expression.Expression) {
	cols := expression.ExtractColumns(expr)
	for _, col := range cols {
		c.addPredicateColumn(col)
	}
}

func (c *predicateColumnCollector) addPredicateColumnsFromExpressions(exprs []expression.Expression) {
	cols := make([]*expression.Column, 0, len(exprs))
	cols = expression.ExtractColumnsFromExpressions(cols, exprs, nil)
	for _, col := range cols {
		c.addPredicateColumn(col)
	}
}

func (c *predicateColumnCollector) updateColMap(col *expression.Column, relatedCols []*expression.Column) {
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

func (c *predicateColumnCollector) updateColMapFromExpression(col *expression.Column, expr expression.Expression) {
	c.updateColMap(col, expression.ExtractColumns(expr))
}

func (c *predicateColumnCollector) updateColMapFromExpressions(col *expression.Column, exprs []expression.Expression) {
	relatedCols := make([]*expression.Column, 0, len(exprs))
	relatedCols = expression.ExtractColumnsFromExpressions(relatedCols, exprs, nil)
	c.updateColMap(col, relatedCols)
}

func (ds *DataSource) updateColMapAndAddPredicateColumns(c *predicateColumnCollector) {
	tblID := ds.TableInfo().ID
	for _, col := range ds.Schema().Columns {
		tblColID := model.TableColumnID{TableID: tblID, ColumnID: col.ID}
		c.colMap[col.UniqueID] = map[model.TableColumnID]struct{}{tblColID: {}}
	}
	// TODO: use ds.pushedDownConds or ds.allConds?
	c.addPredicateColumnsFromExpressions(ds.pushedDownConds)
}

func (p *LogicalJoin) updateColMapAndAddPredicateColumns(c *predicateColumnCollector) {
	// The only schema change is merging two schemas so there is no new column.
	// Assume statistics of all the columns in EqualConditions/LeftConditions/RightConditions/OtherConditions are needed.
	exprs := make([]expression.Expression, 0, len(p.EqualConditions) + len(p.LeftConditions) + len(p.RightConditions) + len(p.OtherConditions))
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

func (c *predicateColumnCollector) collectPredicateColumns(lp LogicalPlan) {
	for _, child := range lp.Children() {
		c.collectPredicateColumns(child)
	}
	switch x := lp.(type) {
	case *DataSource:
		x.updateColMapAndAddPredicateColumns(c)
	case *LogicalIndexScan:
		x.Source.updateColMapAndAddPredicateColumns(c)
		// TODO: Is it redundant to add predicate columns from LogicalIndexScan.AccessConds? Is LogicalIndexScan.AccessConds a subset of LogicalIndexScan.Source.pushedDownConds.
		c.addPredicateColumnsFromExpressions(x.AccessConds)
	case *LogicalTableScan:
		x.Source.updateColMapAndAddPredicateColumns(c)
		// TODO: Is it redundant to add predicate columns from LogicalTableScan.AccessConds? Is LogicalTableScan.AccessConds a subset of LogicalTableScan.Source.pushedDownConds.
		c.addPredicateColumnsFromExpressions(x.AccessConds)
	case *TiKVSingleGather:
		// TODO: Is it redundant?
		x.Source.updateColMapAndAddPredicateColumns(c)
	case *LogicalProjection:
		// Schema change from children to self.
		schema := x.Schema()
		for i, expr := range x.Exprs {
			c.updateColMapFromExpression(schema.Columns[i], expr)
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
	case *LogicalJoin:
		x.updateColMapAndAddPredicateColumns(c)
	case *LogicalApply:
		x.updateColMapAndAddPredicateColumns(c)
		// Assume statistics of correlated columns are needed.
		// Correlated columns can be found in LogicalApply.Children()[0].Schema(). Since we already visit LogicalApply.Children()[0],
		// correlated columns must have existed in predicateColumnCollector.colMap.
		for _, corCols := range x.CorCols {
			c.addPredicateColumn(&corCols.Column)
		}
	case *LogicalTopN:
		// Assume statistics of all the columns in ByItems are needed.
		for _, item := range x.ByItems {
			c.addPredicateColumnsFromExpression(item.Expr)
		}
	case *LogicalSort:
		// Assume statistics of all the columns in ByItems are needed.
		for _, item := range x.ByItems {
			c.addPredicateColumnsFromExpression(item.Expr)
		}
	case *LogicalUnionAll:
		// nothing to do
	case *LogicalPartitionUnionAll:
		// nothing to do

	case *LogicalLimit:
		// nothing to do
	case *LogicalMaxOneRow:
		// nothing to do
	case *LogicalTableDual:
		// nothing to do
	case *LogicalShow:
		// nothing to do
	case *LogicalShowDDLJobs:
		// nothing to do
	case *LogicalMemTable:
		// nothing to do
	case *LogicalLock:
		// nothing to do
	}

}

func (*predicateColumnCollector) name() string {
	return "collect_predicate_columns"
}
