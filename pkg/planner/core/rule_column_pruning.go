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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
)

type columnPruner struct {
}

func (*columnPruner) optimize(_ context.Context, lp LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, bool, error) {
	planChanged := false
	err := lp.PruneColumns(lp.Schema().Columns, opt, lp)
	return lp, planChanged, err
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
func (p *LogicalExpand) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, _ LogicalPlan) error {
	child := p.children[0]
	// Expand need those extra redundant distinct group by columns projected from underlying projection.
	// distinct GroupByCol must be used by aggregate above, to make sure this, append distinctGroupByCol again.
	parentUsedCols = append(parentUsedCols, p.distinctGroupByCol...)
	used := expression.GetUsedList(p.SCtx(), parentUsedCols, p.Schema())
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
	return child.PruneColumns(parentUsedCols, opt, p)
}

// PruneColumns implements LogicalPlan interface.
// If any expression has SetVar function or Sleep function, we do not prune it.
func (p *LogicalProjection) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, _ LogicalPlan) error {
	child := p.children[0]
	used := expression.GetUsedList(p.SCtx(), parentUsedCols, p.schema)
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
	return child.PruneColumns(selfUsedCols, opt, p)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalSelection) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, _ LogicalPlan) error {
	child := p.children[0]
	parentUsedCols = expression.ExtractColumnsFromExpressions(parentUsedCols, p.Conditions, nil)
	return child.PruneColumns(parentUsedCols, opt, p)
}

// PruneColumns implements LogicalPlan interface.
func (la *LogicalAggregation) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, _ LogicalPlan) error {
	child := la.children[0]
	used := expression.GetUsedList(la.SCtx(), parentUsedCols, la.Schema())
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
			newAgg, err = aggregation.NewAggFuncDesc(la.SCtx(), ast.AggFuncFirstRow, []expression.Expression{expression.NewOne()}, false)
		} else {
			newAgg, err = aggregation.NewAggFuncDesc(la.SCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
		}
		if err != nil {
			return err
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
	err := child.PruneColumns(selfUsedCols, opt, la)
	if err != nil {
		return err
	}
	// Do an extra Projection Elimination here. This is specially for empty Projection below Aggregation.
	// This kind of Projection would cause some bugs for MPP plan and is safe to be removed.
	// This kind of Projection should be removed in Projection Elimination, but currently PrunColumnsAgain is
	// the last rule. So we specially handle this case here.
	if childProjection, isProjection := child.(*LogicalProjection); isProjection {
		if len(childProjection.Exprs) == 0 && childProjection.Schema().Len() == 0 {
			childOfChild := childProjection.children[0]
			la.SetChildren(childOfChild)
		}
	}
	return nil
}

func pruneByItems(p LogicalPlan, old []*util.ByItems, opt *logicalOptimizeOp) (byItems []*util.ByItems,
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
			} else if byItem.Expr.GetType().GetType() != mysql.TypeNull {
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

// PruneColumns implements LogicalPlan interface.
// If any expression can view as a constant in execution stage, such as correlated column, constant,
// we do prune them. Note that we can't prune the expressions contain non-deterministic functions, such as rand().
func (ls *LogicalSort) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, _ LogicalPlan) error {
	child := ls.children[0]
	var cols []*expression.Column
	ls.ByItems, cols = pruneByItems(ls, ls.ByItems, opt)
	parentUsedCols = append(parentUsedCols, cols...)
	return child.PruneColumns(parentUsedCols, opt, ls)
}

// PruneColumns implements LogicalPlan interface.
// If any expression can view as a constant in execution stage, such as correlated column, constant,
// we do prune them. Note that we can't prune the expressions contain non-deterministic functions, such as rand().
func (lt *LogicalTopN) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, _ LogicalPlan) error {
	child := lt.children[0]
	var cols []*expression.Column
	lt.ByItems, cols = pruneByItems(lt, lt.ByItems, opt)
	parentUsedCols = append(parentUsedCols, cols...)
	return child.PruneColumns(parentUsedCols, opt, lt)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalUnionAll) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, _ LogicalPlan) error {
	used := expression.GetUsedList(p.SCtx(), parentUsedCols, p.schema)
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
	for _, child := range p.Children() {
		err := child.PruneColumns(parentUsedCols, opt, p)
		if err != nil {
			return err
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
				proj := LogicalProjection{Exprs: exprs, AvoidColumnEvaluator: true}.Init(p.SCtx(), p.SelectBlockOffset())
				proj.SetSchema(schema)

				proj.SetChildren(child)
				p.children[i] = proj
			}
		}
	}
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalUnionScan) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, _ LogicalPlan) error {
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
	return p.children[0].PruneColumns(parentUsedCols, opt, p)
}

// PruneColumns implements LogicalPlan interface.
func (ds *DataSource) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, _ LogicalPlan) error {
	used := expression.GetUsedList(ds.SCtx(), parentUsedCols, ds.schema)

	exprCols := expression.ExtractColumnsFromExpressions(nil, ds.allConds, nil)
	exprUsed := expression.GetUsedList(ds.SCtx(), exprCols, ds.schema)
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
	// For SQL like `select 1 from t`, tikv's response will be empty if no column is in schema.
	// So we'll force to push one if schema doesn't have any column.
	if ds.schema.Len() == 0 {
		var handleCol *expression.Column
		var handleColInfo *model.ColumnInfo
		handleCol, handleColInfo = preferKeyColumnFromTable(ds, originSchemaColumns, originColumns)
		ds.Columns = append(ds.Columns, handleColInfo)
		ds.schema.Append(handleCol)
	}
	// ref: https://github.com/pingcap/tidb/issues/44579
	// when first entering columnPruner, we kept a column-a in datasource since upper agg function count(a) is used.
	//		then we mark the handleCols as nil here.
	// when second entering columnPruner, the count(a) is eliminated since it always not null. we should fill another
	// 		extra col, in this way, handle col is useful again, otherwise, _tidb_rowid will be filled.
	if ds.handleCols != nil && ds.handleCols.IsInt() && ds.schema.ColumnIndex(ds.handleCols.GetCol(0)) == -1 {
		ds.handleCols = nil
	}
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalMemTable) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, _ LogicalPlan) error {
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
		infoschema.ClusterTableDeadlocks:
	default:
		return nil
	}
	prunedColumns := make([]*expression.Column, 0)
	used := expression.GetUsedList(p.SCtx(), parentUsedCols, p.schema)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] && p.schema.Len() > 1 {
			prunedColumns = append(prunedColumns, p.schema.Columns[i])
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
			p.names = append(p.names[:i], p.names[i+1:]...)
			p.Columns = append(p.Columns[:i], p.Columns[i+1:]...)
		}
	}
	appendColumnPruneTraceStep(p, prunedColumns, opt)
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalTableDual) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, _ LogicalPlan) error {
	used := expression.GetUsedList(p.SCtx(), parentUsedCols, p.Schema())
	prunedColumns := make([]*expression.Column, 0)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			prunedColumns = append(prunedColumns, p.schema.Columns[i])
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
		}
	}
	appendColumnPruneTraceStep(p, prunedColumns, opt)
	return nil
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
	lChild := p.children[0]
	rChild := p.children[1]
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

// PruneColumns implements LogicalPlan interface.
func (p *LogicalJoin) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, _ LogicalPlan) error {
	leftCols, rightCols := p.extractUsedCols(parentUsedCols)

	err := p.children[0].PruneColumns(leftCols, opt, p)
	if err != nil {
		return err
	}
	addConstOneForEmptyProjection(p.children[0])

	err = p.children[1].PruneColumns(rightCols, opt, p)
	if err != nil {
		return err
	}
	addConstOneForEmptyProjection(p.children[1])

	p.mergeSchema()
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		joinCol := p.schema.Columns[len(p.schema.Columns)-1]
		parentUsedCols = append(parentUsedCols, joinCol)
	}
	p.inlineProjection(parentUsedCols, opt)
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (la *LogicalApply) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, parentLp LogicalPlan) error {
	leftCols, rightCols := la.extractUsedCols(parentUsedCols)
	allowEliminateApply := fixcontrol.GetBoolWithDefault(la.SCtx().GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix45822, true)
	if allowEliminateApply && rightCols == nil && la.JoinType == LeftOuterJoin {
		applyEliminateTraceStep(la.Children()[1], opt)
		parentLp.SetChildren(la.Children()[0])
	}
	err := la.children[1].PruneColumns(rightCols, opt, la)
	if err != nil {
		return err
	}
	addConstOneForEmptyProjection(la.children[1])

	la.CorCols = extractCorColumnsBySchema4LogicalPlan(la.children[1], la.children[0].Schema())
	for _, col := range la.CorCols {
		leftCols = append(leftCols, &col.Column)
	}

	err = la.children[0].PruneColumns(leftCols, opt, la)
	if err != nil {
		return err
	}
	addConstOneForEmptyProjection(la.children[0])

	la.mergeSchema()
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalLock) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, _ LogicalPlan) error {
	if !IsSelectForUpdateLockType(p.Lock.LockType) {
		return p.baseLogicalPlan.PruneColumns(parentUsedCols, opt, p)
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
	return p.children[0].PruneColumns(parentUsedCols, opt, p)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalWindow) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, _ LogicalPlan) error {
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
	err := p.children[0].PruneColumns(parentUsedCols, opt, p)
	if err != nil {
		return err
	}

	p.SetSchema(p.children[0].Schema().Clone())
	p.Schema().Append(windowColumns...)
	return nil
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

// PruneColumns implements LogicalPlan interface.
func (p *LogicalLimit) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, _ LogicalPlan) error {
	if len(parentUsedCols) == 0 { // happens when LIMIT appears in UPDATE.
		return nil
	}

	savedUsedCols := make([]*expression.Column, len(parentUsedCols))
	copy(savedUsedCols, parentUsedCols)
	if err := p.children[0].PruneColumns(parentUsedCols, opt, p); err != nil {
		return err
	}
	p.schema = nil
	p.inlineProjection(savedUsedCols, opt)
	return nil
}

func (*columnPruner) name() string {
	return "column_prune"
}

// By add const one, we can avoid empty Projection is eliminated.
// Because in some cases, Projectoin cannot be eliminated even its output is empty.
func addConstOneForEmptyProjection(p LogicalPlan) {
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
		RetType:  constOne.GetType(),
	})
	proj.Exprs = append(proj.Exprs, &expression.Constant{
		Value:   constOne.Value,
		RetType: constOne.GetType(),
	})
}

func appendColumnPruneTraceStep(p LogicalPlan, prunedColumns []*expression.Column, opt *logicalOptimizeOp) {
	if len(prunedColumns) < 1 {
		return
	}
	s := make([]fmt.Stringer, 0, len(prunedColumns))
	for _, item := range prunedColumns {
		s = append(s, item)
	}
	appendItemPruneTraceStep(p, "columns", s, opt)
}

func appendFunctionPruneTraceStep(p LogicalPlan, prunedFunctions []*aggregation.AggFuncDesc, opt *logicalOptimizeOp) {
	if len(prunedFunctions) < 1 {
		return
	}
	s := make([]fmt.Stringer, 0, len(prunedFunctions))
	for _, item := range prunedFunctions {
		s = append(s, item)
	}
	appendItemPruneTraceStep(p, "aggregation functions", s, opt)
}

func appendByItemsPruneTraceStep(p LogicalPlan, prunedByItems []*util.ByItems, opt *logicalOptimizeOp) {
	if len(prunedByItems) < 1 {
		return
	}
	s := make([]fmt.Stringer, 0, len(prunedByItems))
	for _, item := range prunedByItems {
		s = append(s, item)
	}
	appendItemPruneTraceStep(p, "byItems", s, opt)
}

func appendGroupByItemsPruneTraceStep(p LogicalPlan, prunedGroupByItems []expression.Expression, opt *logicalOptimizeOp) {
	if len(prunedGroupByItems) < 1 {
		return
	}
	s := make([]fmt.Stringer, 0, len(prunedGroupByItems))
	for _, item := range prunedGroupByItems {
		s = append(s, item)
	}
	appendItemPruneTraceStep(p, "groupByItems", s, opt)
}

func appendItemPruneTraceStep(p LogicalPlan, itemType string, prunedObjects []fmt.Stringer, opt *logicalOptimizeOp) {
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
	opt.appendStepToCurrent(p.ID(), p.TP(), reason, action)
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

// PruneColumns implements the interface of LogicalPlan.
// LogicalCTE just do a empty function call. It's logical optimize is indivisual phase.
func (*LogicalCTE) PruneColumns(_ []*expression.Column, _ *logicalOptimizeOp, _ LogicalPlan) error {
	return nil
}

// PruneColumns implements the interface of LogicalPlan.
func (p *LogicalSequence) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp, _ LogicalPlan) error {
	return p.children[len(p.children)-1].PruneColumns(parentUsedCols, opt, p)
}

func applyEliminateTraceStep(lp LogicalPlan, opt *logicalOptimizeOp) {
	action := func() string {
		buffer := bytes.NewBufferString(
			fmt.Sprintf("%v_%v is eliminated.", lp.TP(), lp.ID()))
		return buffer.String()
	}
	reason := func() string {
		return fmt.Sprintf("%v_%v can be eliminated because it hasn't been used by it's parent.", lp.TP(), lp.ID())
	}
	opt.appendStepToCurrent(lp.ID(), lp.TP(), reason, action)
}
