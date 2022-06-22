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

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/util"
)

type columnPruner struct {
}

func (s *columnPruner) optimize(ctx context.Context, lp LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	err := lp.PruneColumns(lp.Schema().Columns, opt)
	return lp, err
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

// PruneColumns implements LogicalPlan interface.
// If any expression has SetVar function or Sleep function, we do not prune it.
func (p *LogicalProjection) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp) error {
	child := p.children[0]
	used := expression.GetUsedList(parentUsedCols, p.schema)
	prunedColumns := make([]*expression.Column, 0)

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
	return child.PruneColumns(selfUsedCols, opt)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalSelection) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp) error {
	child := p.children[0]
	parentUsedCols = expression.ExtractColumnsFromExpressions(parentUsedCols, p.Conditions, nil)
	return child.PruneColumns(parentUsedCols, opt)
}

// PruneColumns implements LogicalPlan interface.
func (la *LogicalAggregation) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp) error {
	child := la.children[0]
	used := expression.GetUsedList(parentUsedCols, la.Schema())
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
			newAgg, err = aggregation.NewAggFuncDesc(la.ctx, ast.AggFuncFirstRow, []expression.Expression{expression.NewOne()}, false)
		} else {
			newAgg, err = aggregation.NewAggFuncDesc(la.ctx, ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
		}
		if err != nil {
			return err
		}
		la.AggFuncs = append(la.AggFuncs, newAgg)
		col := &expression.Column{
			UniqueID: la.ctx.GetSessionVars().AllocPlanColumnID(),
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
	err := child.PruneColumns(selfUsedCols, opt)
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
		hash := string(byItem.Expr.HashCode(nil))
		_, hashMatch := seen[hash]
		seen[hash] = struct{}{}
		cols := expression.ExtractColumns(byItem.Expr)
		if hashMatch {
			// do nothing, should be filtered
		} else if len(cols) == 0 {
			if !expression.IsRuntimeConstExpr(byItem.Expr) {
				pruned = false
				byItems = append(byItems, byItem)
			}
		} else if byItem.Expr.GetType().GetType() == mysql.TypeNull {
			// do nothing, should be filtered
		} else {
			pruned = false
			parentUsedCols = append(parentUsedCols, cols...)
			byItems = append(byItems, byItem)
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
func (ls *LogicalSort) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp) error {
	child := ls.children[0]
	var cols []*expression.Column
	ls.ByItems, cols = pruneByItems(ls, ls.ByItems, opt)
	parentUsedCols = append(parentUsedCols, cols...)
	return child.PruneColumns(parentUsedCols, opt)
}

// PruneColumns implements LogicalPlan interface.
// If any expression can view as a constant in execution stage, such as correlated column, constant,
// we do prune them. Note that we can't prune the expressions contain non-deterministic functions, such as rand().
func (lt *LogicalTopN) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp) error {
	child := lt.children[0]
	var cols []*expression.Column
	lt.ByItems, cols = pruneByItems(lt, lt.ByItems, opt)
	parentUsedCols = append(parentUsedCols, cols...)
	return child.PruneColumns(parentUsedCols, opt)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalUnionAll) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp) error {
	used := expression.GetUsedList(parentUsedCols, p.schema)
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
	}
	for _, child := range p.Children() {
		err := child.PruneColumns(parentUsedCols, opt)
		if err != nil {
			return err
		}
	}

	prunedColumns := make([]*expression.Column, 0)
	if hasBeenUsed {
		// keep the schema of LogicalUnionAll same as its children's
		used := expression.GetUsedList(p.children[0].Schema().Columns, p.schema)
		for i := len(used) - 1; i >= 0; i-- {
			if !used[i] {
				prunedColumns = append(prunedColumns, p.schema.Columns[i])
				p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
			}
		}
		appendColumnPruneTraceStep(p, prunedColumns, opt)
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
				proj := LogicalProjection{Exprs: exprs, AvoidColumnEvaluator: true}.Init(p.ctx, p.blockOffset)
				proj.SetSchema(schema)

				proj.SetChildren(child)
				p.children[i] = proj
			}
		}
	}
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalUnionScan) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp) error {
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
	return p.children[0].PruneColumns(parentUsedCols, opt)
}

// PruneColumns implements LogicalPlan interface.
func (ds *DataSource) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp) error {
	used := expression.GetUsedList(parentUsedCols, ds.schema)

	exprCols := expression.ExtractColumnsFromExpressions(nil, ds.allConds, nil)
	exprUsed := expression.GetUsedList(exprCols, ds.schema)
	prunedColumns := make([]*expression.Column, 0)

	originSchemaColumns := ds.schema.Columns
	originColumns := ds.Columns
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
		if ds.table.Type().IsClusterTable() && len(originColumns) > 0 {
			// use the first line.
			handleCol = originSchemaColumns[0]
			handleColInfo = originColumns[0]
		} else {
			if ds.handleCols != nil {
				handleCol = ds.handleCols.GetCol(0)
				handleColInfo = handleCol.ToInfo()
			} else {
				handleCol = ds.newExtraHandleSchemaCol()
				handleColInfo = model.NewExtraHandleColInfo()
			}
		}
		ds.Columns = append(ds.Columns, handleColInfo)
		ds.schema.Append(handleCol)
	}
	if ds.handleCols != nil && ds.handleCols.IsInt() && ds.schema.ColumnIndex(ds.handleCols.GetCol(0)) == -1 {
		ds.handleCols = nil
	}
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalMemTable) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp) error {
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
	used := expression.GetUsedList(parentUsedCols, p.schema)
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
func (p *LogicalTableDual) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp) error {
	used := expression.GetUsedList(parentUsedCols, p.Schema())
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
func (p *LogicalJoin) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp) error {
	leftCols, rightCols := p.extractUsedCols(parentUsedCols)

	err := p.children[0].PruneColumns(leftCols, opt)
	if err != nil {
		return err
	}
	addConstOneForEmptyProjection(p.children[0])

	err = p.children[1].PruneColumns(rightCols, opt)
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
func (la *LogicalApply) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp) error {
	leftCols, rightCols := la.extractUsedCols(parentUsedCols)

	err := la.children[1].PruneColumns(rightCols, opt)
	if err != nil {
		return err
	}
	addConstOneForEmptyProjection(la.children[1])

	la.CorCols = extractCorColumnsBySchema4LogicalPlan(la.children[1], la.children[0].Schema())
	for _, col := range la.CorCols {
		leftCols = append(leftCols, &col.Column)
	}

	err = la.children[0].PruneColumns(leftCols, opt)
	if err != nil {
		return err
	}
	addConstOneForEmptyProjection(la.children[0])

	la.mergeSchema()
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalLock) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp) error {
	if !IsSelectForUpdateLockType(p.Lock.LockType) {
		return p.baseLogicalPlan.PruneColumns(parentUsedCols, opt)
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
	return p.children[0].PruneColumns(parentUsedCols, opt)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalWindow) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp) error {
	windowColumns := p.GetWindowResultColumns()
	cnt := 0
	for _, col := range parentUsedCols {
		used := false
		for _, windowColumn := range windowColumns {
			if windowColumn.Equal(nil, col) {
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
	err := p.children[0].PruneColumns(parentUsedCols, opt)
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
func (p *LogicalLimit) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp) error {
	if len(parentUsedCols) == 0 { // happens when LIMIT appears in UPDATE.
		return nil
	}

	savedUsedCols := make([]*expression.Column, len(parentUsedCols))
	copy(savedUsedCols, parentUsedCols)
	if err := p.children[0].PruneColumns(parentUsedCols, opt); err != nil {
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
		UniqueID: proj.ctx.GetSessionVars().AllocPlanColumnID(),
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
