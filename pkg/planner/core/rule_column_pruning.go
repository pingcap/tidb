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
	"bytes"
	"context"
	"fmt"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
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

// ExprsHasSideEffects checks if any of the expressions has side effects.
func ExprsHasSideEffects(exprs []expression.Expression) bool {
	for _, expr := range exprs {
		if exprHasSetVarOrSleep(expr) {
			return true
		}
	}
	return false
}

// exprHasSetVarOrSleep checks if the expression has SetVar function or Sleep function.
func exprHasSetVarOrSleep(expr expression.Expression) bool {
	scalaFunc, isScalaFunc := expr.(*expression.ScalarFunction)
	if !isScalaFunc {
		return false
	}
	if scalaFunc.FuncName.L == ast.SetVar || scalaFunc.FuncName.L == ast.Sleep {
		return true
	}
	for _, arg := range scalaFunc.GetArgs() {
		if exprHasSetVarOrSleep(arg) {
			return true
		}
	}
	return false
}

// PruneColumns implement the Expand OP's column pruning logic.
// logicExpand is built in the logical plan building phase, where all the column prune is not done yet. So the
// expand projection expressions is meaningless if it built at that time. (we only maintain its schema, while
// the level projection expressions construction is left to the last logical optimize rule)
//
// so when do the rule_column_pruning here, we just prune the schema is enough.
func (p *LogicalExpand) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	// Expand need those extra redundant distinct group by columns projected from underlying projection.
	// distinct GroupByCol must be used by aggregate above, to make sure this, append distinctGroupByCol again.
	parentUsedCols = append(parentUsedCols, p.distinctGroupByCol...)
	used := expression.GetUsedList(p.SCtx().GetExprCtx().GetEvalCtx(), parentUsedCols, p.Schema())
	prunedColumns := make([]*expression.Column, 0)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			prunedColumns = append(prunedColumns, p.schema.Columns[i])
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
			p.names = append(p.names[:i], p.names[i+1:]...)
		}
	}
	appendColumnPruneTraceStep(p, prunedColumns, opt)
	// Underlying still need to keep the distinct group by columns and parent used columns.
	var err error
	p.Children()[0], err = p.Children()[0].PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// PruneColumns implements base.LogicalPlan interface.
// If any expression has SetVar function or Sleep function, we do not prune it.
func (p *LogicalProjection) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	used := expression.GetUsedList(p.SCtx().GetExprCtx().GetEvalCtx(), parentUsedCols, p.schema)
	prunedColumns := make([]*expression.Column, 0)

	// for implicit projected cols, once the ancestor doesn't use it, the implicit expr will be automatically pruned here.
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] && !exprHasSetVarOrSleep(p.Exprs[i]) {
			prunedColumns = append(prunedColumns, p.schema.Columns[i])
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
			p.Exprs = append(p.Exprs[:i], p.Exprs[i+1:]...)
		}
	}
	appendColumnPruneTraceStep(p, prunedColumns, opt)
	selfUsedCols := make([]*expression.Column, 0, len(p.Exprs))
	selfUsedCols = expression.ExtractColumnsFromExpressions(selfUsedCols, p.Exprs, nil)
	var err error
	p.Children()[0], err = p.Children()[0].PruneColumns(selfUsedCols, opt)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// PruneColumns implements base.LogicalPlan interface.
func (p *LogicalSelection) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	child := p.Children()[0]
	parentUsedCols = expression.ExtractColumnsFromExpressions(parentUsedCols, p.Conditions, nil)
	var err error
	p.Children()[0], err = child.PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}
	addConstOneForEmptyProjection(p.Children()[0])
	return p, nil
}

// PruneColumns implements base.LogicalPlan interface.
func (la *LogicalAggregation) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	child := la.Children()[0]
	used := expression.GetUsedList(la.SCtx().GetExprCtx().GetEvalCtx(), parentUsedCols, la.Schema())
	prunedColumns := make([]*expression.Column, 0)
	prunedFunctions := make([]*aggregation.AggFuncDesc, 0)
	prunedGroupByItems := make([]expression.Expression, 0)

	allFirstRow := true
	allRemainFirstRow := true
	for i := len(used) - 1; i >= 0; i-- {
		if la.AggFuncs[i].Name != ast.AggFuncFirstRow {
			allFirstRow = false
		}
		if !used[i] && !ExprsHasSideEffects(la.AggFuncs[i].Args) {
			prunedColumns = append(prunedColumns, la.schema.Columns[i])
			prunedFunctions = append(prunedFunctions, la.AggFuncs[i])
			la.schema.Columns = append(la.schema.Columns[:i], la.schema.Columns[i+1:]...)
			la.AggFuncs = append(la.AggFuncs[:i], la.AggFuncs[i+1:]...)
		} else if la.AggFuncs[i].Name != ast.AggFuncFirstRow {
			allRemainFirstRow = false
		}
	}
	appendColumnPruneTraceStep(la, prunedColumns, opt)
	appendFunctionPruneTraceStep(la, prunedFunctions, opt)
	//nolint: prealloc
	var selfUsedCols []*expression.Column
	for _, aggrFunc := range la.AggFuncs {
		selfUsedCols = expression.ExtractColumnsFromExpressions(selfUsedCols, aggrFunc.Args, nil)

		var cols []*expression.Column
		aggrFunc.OrderByItems, cols = pruneByItems(la, aggrFunc.OrderByItems, opt)
		selfUsedCols = append(selfUsedCols, cols...)
	}
	if len(la.AggFuncs) == 0 || (!allFirstRow && allRemainFirstRow) {
		// If all the aggregate functions are pruned, we should add an aggregate function to maintain the info of row numbers.
		// For all the aggregate functions except `first_row`, if we have an empty table defined as t(a,b),
		// `select agg(a) from t` would always return one row, while `select agg(a) from t group by b` would return empty.
		// For `first_row` which is only used internally by tidb, `first_row(a)` would always return empty for empty input now.
		var err error
		var newAgg *aggregation.AggFuncDesc
		if allFirstRow {
			newAgg, err = aggregation.NewAggFuncDesc(la.SCtx().GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{expression.NewOne()}, false)
		} else {
			newAgg, err = aggregation.NewAggFuncDesc(la.SCtx().GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
		}
		if err != nil {
			return nil, err
		}
		la.AggFuncs = append(la.AggFuncs, newAgg)
		col := &expression.Column{
			UniqueID: la.SCtx().GetSessionVars().AllocPlanColumnID(),
			RetType:  newAgg.RetTp,
		}
		la.schema.Columns = append(la.schema.Columns, col)
	}

	if len(la.GroupByItems) > 0 {
		for i := len(la.GroupByItems) - 1; i >= 0; i-- {
			cols := expression.ExtractColumns(la.GroupByItems[i])
			if len(cols) == 0 && !exprHasSetVarOrSleep(la.GroupByItems[i]) {
				prunedGroupByItems = append(prunedGroupByItems, la.GroupByItems[i])
				la.GroupByItems = append(la.GroupByItems[:i], la.GroupByItems[i+1:]...)
			} else {
				selfUsedCols = append(selfUsedCols, cols...)
			}
		}
		// If all the group by items are pruned, we should add a constant 1 to keep the correctness.
		// Because `select count(*) from t` is different from `select count(*) from t group by 1`.
		if len(la.GroupByItems) == 0 {
			la.GroupByItems = []expression.Expression{expression.NewOne()}
		}
	}
	appendGroupByItemsPruneTraceStep(la, prunedGroupByItems, opt)
	var err error
	la.Children()[0], err = child.PruneColumns(selfUsedCols, opt)
	if err != nil {
		return nil, err
	}
	// update children[0]
	child = la.Children()[0]
	// Do an extra Projection Elimination here. This is specially for empty Projection below Aggregation.
	// This kind of Projection would cause some bugs for MPP plan and is safe to be removed.
	// This kind of Projection should be removed in Projection Elimination, but currently PrunColumnsAgain is
	// the last rule. So we specially handle this case here.
	if childProjection, isProjection := child.(*LogicalProjection); isProjection {
		if len(childProjection.Exprs) == 0 && childProjection.Schema().Len() == 0 {
			childOfChild := childProjection.Children()[0]
			la.SetChildren(childOfChild)
		}
	}
	return la, nil
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
	appendByItemsPruneTraceStep(p, prunedByItems, opt)
	return
}

// PruneColumns implements base.LogicalPlan interface.
// If any expression can view as a constant in execution stage, such as correlated column, constant,
// we do prune them. Note that we can't prune the expressions contain non-deterministic functions, such as rand().
func (ls *LogicalSort) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	var cols []*expression.Column
	ls.ByItems, cols = pruneByItems(ls, ls.ByItems, opt)
	parentUsedCols = append(parentUsedCols, cols...)
	var err error
	ls.Children()[0], err = ls.Children()[0].PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}
	return ls, nil
}

// PruneColumns implements base.LogicalPlan interface.
// If any expression can view as a constant in execution stage, such as correlated column, constant,
// we do prune them. Note that we can't prune the expressions contain non-deterministic functions, such as rand().
func (lt *LogicalTopN) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	child := lt.Children()[0]
	var cols []*expression.Column
	lt.ByItems, cols = pruneByItems(lt, lt.ByItems, opt)
	parentUsedCols = append(parentUsedCols, cols...)
	var err error
	lt.Children()[0], err = child.PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}
	return lt, nil
}

// PruneColumns implements base.LogicalPlan interface.
func (p *LogicalUnionAll) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	used := expression.GetUsedList(p.SCtx().GetExprCtx().GetEvalCtx(), parentUsedCols, p.schema)
	hasBeenUsed := false
	for i := range used {
		hasBeenUsed = hasBeenUsed || used[i]
		if hasBeenUsed {
			break
		}
	}
	if !hasBeenUsed {
		parentUsedCols = make([]*expression.Column, len(p.schema.Columns))
		copy(parentUsedCols, p.schema.Columns)
		for i := range used {
			used[i] = true
		}
	}
	var err error
	for i, child := range p.Children() {
		p.Children()[i], err = child.PruneColumns(parentUsedCols, opt)
		if err != nil {
			return nil, err
		}
	}

	prunedColumns := make([]*expression.Column, 0)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			prunedColumns = append(prunedColumns, p.schema.Columns[i])
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
		}
	}
	appendColumnPruneTraceStep(p, prunedColumns, opt)
	if hasBeenUsed {
		// It's possible that the child operator adds extra columns to the schema.
		// Currently, (*LogicalAggregation).PruneColumns() might do this.
		// But we don't need such columns, so we add an extra Projection to prune this column when this happened.
		for i, child := range p.Children() {
			if p.schema.Len() < child.Schema().Len() {
				schema := p.schema.Clone()
				exprs := make([]expression.Expression, len(p.schema.Columns))
				for j, col := range schema.Columns {
					exprs[j] = col
				}
				proj := LogicalProjection{Exprs: exprs, AvoidColumnEvaluator: true}.Init(p.SCtx(), p.QueryBlockOffset())
				proj.SetSchema(schema)

				proj.SetChildren(child)
				p.Children()[i] = proj
			}
		}
	}
	return p, nil
}

// PruneColumns implements base.LogicalPlan interface.
func (p *LogicalUnionScan) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	for i := 0; i < p.handleCols.NumCols(); i++ {
		parentUsedCols = append(parentUsedCols, p.handleCols.GetCol(i))
	}
	for _, col := range p.Schema().Columns {
		if col.ID == model.ExtraPidColID || col.ID == model.ExtraPhysTblID {
			parentUsedCols = append(parentUsedCols, col)
		}
	}
	condCols := expression.ExtractColumnsFromExpressions(nil, p.conditions, nil)
	parentUsedCols = append(parentUsedCols, condCols...)
	var err error
	p.Children()[0], err = p.Children()[0].PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// PruneColumns implements base.LogicalPlan interface.
func (ds *DataSource) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	used := expression.GetUsedList(ds.SCtx().GetExprCtx().GetEvalCtx(), parentUsedCols, ds.schema)

	exprCols := expression.ExtractColumnsFromExpressions(nil, ds.allConds, nil)
	exprUsed := expression.GetUsedList(ds.SCtx().GetExprCtx().GetEvalCtx(), exprCols, ds.schema)
	prunedColumns := make([]*expression.Column, 0)

	originSchemaColumns := ds.schema.Columns
	originColumns := ds.Columns

	ds.colsRequiringFullLen = make([]*expression.Column, 0, len(used))
	for i, col := range ds.schema.Columns {
		if used[i] || (ds.containExprPrefixUk && expression.GcColumnExprIsTidbShard(col.VirtualExpr)) {
			ds.colsRequiringFullLen = append(ds.colsRequiringFullLen, col)
		}
	}

	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] && !exprUsed[i] {
			// If ds has a shard index, and the column is generated column by `tidb_shard()`
			// it can't prune the generated column of shard index
			if ds.containExprPrefixUk &&
				expression.GcColumnExprIsTidbShard(ds.schema.Columns[i].VirtualExpr) {
				continue
			}
			prunedColumns = append(prunedColumns, ds.schema.Columns[i])
			ds.schema.Columns = append(ds.schema.Columns[:i], ds.schema.Columns[i+1:]...)
			ds.Columns = append(ds.Columns[:i], ds.Columns[i+1:]...)
		}
	}
	appendColumnPruneTraceStep(ds, prunedColumns, opt)
	addOneHandle := false
	// For SQL like `select 1 from t`, tikv's response will be empty if no column is in schema.
	// So we'll force to push one if schema doesn't have any column.
	if ds.schema.Len() == 0 {
		var handleCol *expression.Column
		var handleColInfo *model.ColumnInfo
		handleCol, handleColInfo = preferKeyColumnFromTable(ds, originSchemaColumns, originColumns)
		ds.Columns = append(ds.Columns, handleColInfo)
		ds.schema.Append(handleCol)
		addOneHandle = true
	}
	// ref: https://github.com/pingcap/tidb/issues/44579
	// when first entering columnPruner, we kept a column-a in datasource since upper agg function count(a) is used.
	//		then we mark the handleCols as nil here.
	// when second entering columnPruner, the count(a) is eliminated since it always not null. we should fill another
	// 		extra col, in this way, handle col is useful again, otherwise, _tidb_rowid will be filled.
	if ds.handleCols != nil && ds.handleCols.IsInt() && ds.schema.ColumnIndex(ds.handleCols.GetCol(0)) == -1 {
		ds.handleCols = nil
	}
	// Current DataSource operator contains all the filters on this table, and the columns used by these filters are always included
	// in the output schema. Even if they are not needed by DataSource's parent operator. Thus add a projection here to prune useless columns
	// Limit to MPP tasks, because TiKV can't benefit from this now(projection can't be pushed down to TiKV now).
	if !addOneHandle && ds.schema.Len() > len(parentUsedCols) && ds.SCtx().GetSessionVars().IsMPPEnforced() && ds.tableInfo.TiFlashReplica != nil {
		proj := LogicalProjection{
			Exprs: expression.Column2Exprs(parentUsedCols),
		}.Init(ds.SCtx(), ds.QueryBlockOffset())
		proj.SetStats(ds.StatsInfo())
		proj.SetSchema(expression.NewSchema(parentUsedCols...))
		proj.SetChildren(ds)
		return proj, nil
	}
	return ds, nil
}

// PruneColumns implements base.LogicalPlan interface.
func (p *LogicalMemTable) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	switch p.TableInfo.Name.O {
	case infoschema.TableStatementsSummary,
		infoschema.TableStatementsSummaryHistory,
		infoschema.TableSlowQuery,
		infoschema.ClusterTableStatementsSummary,
		infoschema.ClusterTableStatementsSummaryHistory,
		infoschema.ClusterTableSlowLog,
		infoschema.TableTiDBTrx,
		infoschema.ClusterTableTiDBTrx,
		infoschema.TableDataLockWaits,
		infoschema.TableDeadlocks,
		infoschema.ClusterTableDeadlocks,
		infoschema.TableTables:
	default:
		return p, nil
	}
	prunedColumns := make([]*expression.Column, 0)
	used := expression.GetUsedList(p.SCtx().GetExprCtx().GetEvalCtx(), parentUsedCols, p.schema)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] && p.schema.Len() > 1 {
			prunedColumns = append(prunedColumns, p.schema.Columns[i])
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
			p.names = append(p.names[:i], p.names[i+1:]...)
			p.Columns = append(p.Columns[:i], p.Columns[i+1:]...)
		}
	}
	appendColumnPruneTraceStep(p, prunedColumns, opt)
	return p, nil
}

// PruneColumns implements base.LogicalPlan interface.
func (p *LogicalTableDual) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	used := expression.GetUsedList(p.SCtx().GetExprCtx().GetEvalCtx(), parentUsedCols, p.Schema())
	prunedColumns := make([]*expression.Column, 0)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			prunedColumns = append(prunedColumns, p.schema.Columns[i])
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
		}
	}
	appendColumnPruneTraceStep(p, prunedColumns, opt)
	return p, nil
}

func (p *LogicalJoin) extractUsedCols(parentUsedCols []*expression.Column) (leftCols []*expression.Column, rightCols []*expression.Column) {
	for _, eqCond := range p.EqualConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(eqCond)...)
	}
	for _, leftCond := range p.LeftConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(leftCond)...)
	}
	for _, rightCond := range p.RightConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(rightCond)...)
	}
	for _, otherCond := range p.OtherConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(otherCond)...)
	}
	for _, naeqCond := range p.NAEQConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(naeqCond)...)
	}
	lChild := p.Children()[0]
	rChild := p.Children()[1]
	for _, col := range parentUsedCols {
		if lChild.Schema().Contains(col) {
			leftCols = append(leftCols, col)
		} else if rChild.Schema().Contains(col) {
			rightCols = append(rightCols, col)
		}
	}
	return leftCols, rightCols
}

func (p *LogicalJoin) mergeSchema() {
	p.schema = buildLogicalJoinSchema(p.JoinType, p)
}

// PruneColumns implements base.LogicalPlan interface.
func (p *LogicalJoin) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	leftCols, rightCols := p.extractUsedCols(parentUsedCols)

	var err error
	p.Children()[0], err = p.Children()[0].PruneColumns(leftCols, opt)
	if err != nil {
		return nil, err
	}
	addConstOneForEmptyProjection(p.Children()[0])

	p.Children()[1], err = p.Children()[1].PruneColumns(rightCols, opt)
	if err != nil {
		return nil, err
	}
	addConstOneForEmptyProjection(p.Children()[1])

	p.mergeSchema()
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		joinCol := p.schema.Columns[len(p.schema.Columns)-1]
		parentUsedCols = append(parentUsedCols, joinCol)
	}
	p.inlineProjection(parentUsedCols, opt)
	return p, nil
}

// PruneColumns implements base.LogicalPlan interface.
func (la *LogicalApply) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	leftCols, rightCols := la.extractUsedCols(parentUsedCols)
	allowEliminateApply := fixcontrol.GetBoolWithDefault(la.SCtx().GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix45822, true)
	var err error
	if allowEliminateApply && rightCols == nil && la.JoinType == LeftOuterJoin {
		applyEliminateTraceStep(la.Children()[1], opt)
		resultPlan := la.Children()[0]
		// reEnter the new child's column pruning, returning child[0] as a new child here.
		return resultPlan.PruneColumns(parentUsedCols, opt)
	}

	// column pruning for child-1.
	la.Children()[1], err = la.Children()[1].PruneColumns(rightCols, opt)
	if err != nil {
		return nil, err
	}
	addConstOneForEmptyProjection(la.Children()[1])

	la.CorCols = coreusage.ExtractCorColumnsBySchema4LogicalPlan(la.Children()[1], la.Children()[0].Schema())
	for _, col := range la.CorCols {
		leftCols = append(leftCols, &col.Column)
	}

	// column pruning for child-0.
	la.Children()[0], err = la.Children()[0].PruneColumns(leftCols, opt)
	if err != nil {
		return nil, err
	}
	addConstOneForEmptyProjection(la.Children()[0])
	la.mergeSchema()
	return la, nil
}

// PruneColumns implements base.LogicalPlan interface.
func (p *LogicalLock) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	var err error
	if !IsSelectForUpdateLockType(p.Lock.LockType) {
		// when use .baseLogicalPlan to call the PruneColumns, it means current plan itself has
		// nothing to pruning or plan change, so they resort to its children's column pruning logic.
		// so for the returned logical plan here, p is definitely determined, we just need to collect
		// those extra deeper call error in handling children's column pruning.
		_, err = p.BaseLogicalPlan.PruneColumns(parentUsedCols, opt)
		if err != nil {
			return nil, err
		}
		return p, nil
	}

	for tblID, cols := range p.tblID2Handle {
		for _, col := range cols {
			for i := 0; i < col.NumCols(); i++ {
				parentUsedCols = append(parentUsedCols, col.GetCol(i))
			}
		}
		if physTblIDCol, ok := p.tblID2PhysTblIDCol[tblID]; ok {
			// If the children include partitioned tables, there is an extra partition ID column.
			parentUsedCols = append(parentUsedCols, physTblIDCol)
		}
	}
	p.Children()[0], err = p.Children()[0].PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// PruneColumns implements base.LogicalPlan interface.
func (p *LogicalWindow) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	windowColumns := p.GetWindowResultColumns()
	cnt := 0
	for _, col := range parentUsedCols {
		used := false
		for _, windowColumn := range windowColumns {
			if windowColumn.EqualColumn(col) {
				used = true
				break
			}
		}
		if !used {
			parentUsedCols[cnt] = col
			cnt++
		}
	}
	parentUsedCols = parentUsedCols[:cnt]
	parentUsedCols = p.extractUsedCols(parentUsedCols)
	var err error
	p.Children()[0], err = p.Children()[0].PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}

	p.SetSchema(p.Children()[0].Schema().Clone())
	p.Schema().Append(windowColumns...)
	return p, nil
}

func (p *LogicalWindow) extractUsedCols(parentUsedCols []*expression.Column) []*expression.Column {
	for _, desc := range p.WindowFuncDescs {
		for _, arg := range desc.Args {
			parentUsedCols = append(parentUsedCols, expression.ExtractColumns(arg)...)
		}
	}
	for _, by := range p.PartitionBy {
		parentUsedCols = append(parentUsedCols, by.Col)
	}
	for _, by := range p.OrderBy {
		parentUsedCols = append(parentUsedCols, by.Col)
	}
	return parentUsedCols
}

// PruneColumns implements base.LogicalPlan interface.
func (p *LogicalLimit) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	if len(parentUsedCols) == 0 { // happens when LIMIT appears in UPDATE.
		return p, nil
	}

	savedUsedCols := make([]*expression.Column, len(parentUsedCols))
	copy(savedUsedCols, parentUsedCols)
	var err error
	if p.Children()[0], err = p.Children()[0].PruneColumns(parentUsedCols, opt); err != nil {
		return nil, err
	}
	p.schema = nil
	p.inlineProjection(savedUsedCols, opt)
	return p, nil
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
	proj.schema.Append(&expression.Column{
		UniqueID: proj.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  constOne.GetType(p.SCtx().GetExprCtx().GetEvalCtx()),
	})
	proj.Exprs = append(proj.Exprs, &expression.Constant{
		Value:   constOne.Value,
		RetType: constOne.GetType(p.SCtx().GetExprCtx().GetEvalCtx()),
	})
}

func appendColumnPruneTraceStep(p base.LogicalPlan, prunedColumns []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) {
	if len(prunedColumns) < 1 {
		return
	}
	s := make([]fmt.Stringer, 0, len(prunedColumns))
	for _, item := range prunedColumns {
		s = append(s, item)
	}
	appendItemPruneTraceStep(p, "columns", s, opt)
}

func appendFunctionPruneTraceStep(p base.LogicalPlan, prunedFunctions []*aggregation.AggFuncDesc, opt *optimizetrace.LogicalOptimizeOp) {
	if len(prunedFunctions) < 1 {
		return
	}
	s := make([]fmt.Stringer, 0, len(prunedFunctions))
	for _, item := range prunedFunctions {
		s = append(s, item)
	}
	appendItemPruneTraceStep(p, "aggregation functions", s, opt)
}

func appendByItemsPruneTraceStep(p base.LogicalPlan, prunedByItems []*util.ByItems, opt *optimizetrace.LogicalOptimizeOp) {
	if len(prunedByItems) < 1 {
		return
	}
	s := make([]fmt.Stringer, 0, len(prunedByItems))
	for _, item := range prunedByItems {
		s = append(s, item)
	}
	appendItemPruneTraceStep(p, "byItems", s, opt)
}

func appendGroupByItemsPruneTraceStep(p base.LogicalPlan, prunedGroupByItems []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) {
	if len(prunedGroupByItems) < 1 {
		return
	}
	s := make([]fmt.Stringer, 0, len(prunedGroupByItems))
	for _, item := range prunedGroupByItems {
		s = append(s, item)
	}
	appendItemPruneTraceStep(p, "groupByItems", s, opt)
}

func appendItemPruneTraceStep(p base.LogicalPlan, itemType string, prunedObjects []fmt.Stringer, opt *optimizetrace.LogicalOptimizeOp) {
	if len(prunedObjects) < 1 {
		return
	}
	action := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v's %v[", p.TP(), p.ID(), itemType))
		for i, item := range prunedObjects {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(item.String())
		}
		buffer.WriteString("] have been pruned")
		return buffer.String()
	}
	reason := func() string {
		return ""
	}
	opt.AppendStepToCurrent(p.ID(), p.TP(), reason, action)
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
		if dataSource.handleCols != nil {
			resultColumn = dataSource.handleCols.GetCol(0)
			resultColumnInfo = resultColumn.ToInfo()
		} else if dataSource.table.Meta().PKIsHandle {
			// dataSource.handleCols = nil doesn't mean datasource doesn't have a intPk handle.
			// since datasource.handleCols will be cleared in the first columnPruner.
			resultColumn = dataSource.unMutableHandleCols.GetCol(0)
			resultColumnInfo = resultColumn.ToInfo()
		} else {
			resultColumn = dataSource.newExtraHandleSchemaCol()
			resultColumnInfo = model.NewExtraHandleColInfo()
		}
	}
	return resultColumn, resultColumnInfo
}

// PruneColumns implements the interface of base.LogicalPlan.
// LogicalCTE just do a empty function call. It's logical optimize is indivisual phase.
func (p *LogicalCTE) PruneColumns(_ []*expression.Column, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	return p, nil
}

// PruneColumns implements the interface of base.LogicalPlan.
func (p *LogicalSequence) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	var err error
	p.Children()[p.ChildLen()-1], err = p.Children()[p.ChildLen()-1].PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func applyEliminateTraceStep(lp base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) {
	action := func() string {
		buffer := bytes.NewBufferString(
			fmt.Sprintf("%v_%v is eliminated.", lp.TP(), lp.ID()))
		return buffer.String()
	}
	reason := func() string {
		return fmt.Sprintf("%v_%v can be eliminated because it hasn't been used by it's parent.", lp.TP(), lp.ID())
	}
	opt.AppendStepToCurrent(lp.ID(), lp.TP(), reason, action)
}
