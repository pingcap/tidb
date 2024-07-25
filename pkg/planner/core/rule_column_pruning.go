// Copyright 2016 PingCAP, Inc.
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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace/logicaltrace"
)

type columnPruner struct {
}

func (*columnPruner) optimize(_ context.Context, lp base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	lp, err := lp.PruneColumns(slices.Clone(lp.Schema().Columns), opt)
	if err != nil {
		return nil, planChanged, err
	}
	return lp, planChanged, nil
}

// PruneColumns implement the Expand OP's column pruning logic.
// logicExpand is built in the logical plan building phase, where all the column prune is not done yet. So the
// expand projection expressions is meaningless if it built at that time. (we only maintain its schema, while
// the level projection expressions construction is left to the last logical optimize rule)
//
// so when do the rule_column_pruning here, we just prune the schema is enough.
func (p *LogicalExpand) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	// Expand need those extra redundant distinct group by columns projected from underlying projection.
	// distinct GroupByCol must be used by aggregate above, to make sure this, append DistinctGroupByCol again.
	parentUsedCols = append(parentUsedCols, p.DistinctGroupByCol...)
	used := expression.GetUsedList(p.SCtx().GetExprCtx().GetEvalCtx(), parentUsedCols, p.Schema())
	prunedColumns := make([]*expression.Column, 0)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			prunedColumns = append(prunedColumns, p.Schema().Columns[i])
			p.Schema().Columns = append(p.Schema().Columns[:i], p.Schema().Columns[i+1:]...)
			p.SetOutputNames(append(p.OutputNames()[:i], p.OutputNames()[i+1:]...))
		}
	}
	logicaltrace.AppendColumnPruneTraceStep(p, prunedColumns, opt)
	// Underlying still need to keep the distinct group by columns and parent used columns.
	var err error
	p.Children()[0], err = p.Children()[0].PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func pruneByItems(p base.LogicalPlan, old []*util.ByItems, opt *optimizetrace.LogicalOptimizeOp) (byItems []*util.ByItems,
	parentUsedCols []*expression.Column) {
	prunedByItems := make([]*util.ByItems, 0)
	byItems = make([]*util.ByItems, 0, len(old))
	seen := make(map[string]struct{}, len(old))
	for _, byItem := range old {
		pruned := true
		hash := string(byItem.Expr.HashCode())
		_, hashMatch := seen[hash]
		seen[hash] = struct{}{}
		cols := expression.ExtractColumns(byItem.Expr)
		if !hashMatch {
			if len(cols) == 0 {
				if !expression.IsRuntimeConstExpr(byItem.Expr) {
					pruned = false
					byItems = append(byItems, byItem)
				}
			} else if byItem.Expr.GetType(p.SCtx().GetExprCtx().GetEvalCtx()).GetType() != mysql.TypeNull {
				pruned = false
				parentUsedCols = append(parentUsedCols, cols...)
				byItems = append(byItems, byItem)
			}
		}
		if pruned {
			prunedByItems = append(prunedByItems, byItem)
		}
	}
	logicaltrace.AppendByItemsPruneTraceStep(p, prunedByItems, opt)
	return
}

func (*columnPruner) name() string {
	return "column_prune"
}

// By add const one, we can avoid empty Projection is eliminated.
// Because in some cases, Projectoin cannot be eliminated even its output is empty.
func addConstOneForEmptyProjection(p base.LogicalPlan) {
	proj, ok := p.(*LogicalProjection)
	if !ok {
		return
	}
	if proj.Schema().Len() != 0 {
		return
	}

	constOne := expression.NewOne()
	proj.Schema().Append(&expression.Column{
		UniqueID: proj.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  constOne.GetType(p.SCtx().GetExprCtx().GetEvalCtx()),
	})
	proj.Exprs = append(proj.Exprs, &expression.Constant{
		Value:   constOne.Value,
		RetType: constOne.GetType(p.SCtx().GetExprCtx().GetEvalCtx()),
	})
}

func preferKeyColumnFromTable(dataSource *DataSource, originColumns []*expression.Column,
	originSchemaColumns []*model.ColumnInfo) (*expression.Column, *model.ColumnInfo) {
	var resultColumnInfo *model.ColumnInfo
	var resultColumn *expression.Column
	if dataSource.table.Type().IsClusterTable() && len(originColumns) > 0 {
		// use the first column.
		resultColumnInfo = originSchemaColumns[0]
		resultColumn = originColumns[0]
	} else {
		if dataSource.HandleCols != nil {
			resultColumn = dataSource.HandleCols.GetCol(0)
			resultColumnInfo = resultColumn.ToInfo()
		} else if dataSource.table.Meta().PKIsHandle {
			// dataSource.HandleCols = nil doesn't mean datasource doesn't have a intPk handle.
			// since datasource.HandleCols will be cleared in the first columnPruner.
			resultColumn = dataSource.UnMutableHandleCols.GetCol(0)
			resultColumnInfo = resultColumn.ToInfo()
		} else {
			resultColumn = dataSource.newExtraHandleSchemaCol()
			resultColumnInfo = model.NewExtraHandleColInfo()
		}
	}
	return resultColumn, resultColumnInfo
}
