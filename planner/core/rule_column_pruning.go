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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/types"
)

type columnPruner struct {
}

func (s *columnPruner) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	err := lp.PruneColumns(lp.Schema().Columns)
	return lp, err
}

func getUsedList(usedCols []*expression.Column, schema *expression.Schema) []bool {
	tmpSchema := expression.NewSchema(usedCols...)
	used := make([]bool, schema.Len())
	for i, col := range schema.Columns {
		used[i] = tmpSchema.Contains(col)
	}
	return used
}

// exprsHasSideEffects checks if any of the expressions has side effects.
func exprsHasSideEffects(exprs []expression.Expression) bool {
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
func (p *LogicalProjection) PruneColumns(parentUsedCols []*expression.Column) error {
	child := p.children[0]
	used := getUsedList(parentUsedCols, p.schema)

	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] && !exprHasSetVarOrSleep(p.Exprs[i]) {
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
			p.Exprs = append(p.Exprs[:i], p.Exprs[i+1:]...)
		}
	}
	selfUsedCols := make([]*expression.Column, 0, len(p.Exprs))
	selfUsedCols = expression.ExtractColumnsFromExpressions(selfUsedCols, p.Exprs, nil)
	return child.PruneColumns(selfUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalSelection) PruneColumns(parentUsedCols []*expression.Column) error {
	child := p.children[0]
	parentUsedCols = expression.ExtractColumnsFromExpressions(parentUsedCols, p.Conditions, nil)
	return child.PruneColumns(parentUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (la *LogicalAggregation) PruneColumns(parentUsedCols []*expression.Column) error {
	child := la.children[0]
	used := getUsedList(parentUsedCols, la.Schema())

	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			la.schema.Columns = append(la.schema.Columns[:i], la.schema.Columns[i+1:]...)
			la.AggFuncs = append(la.AggFuncs[:i], la.AggFuncs[i+1:]...)
		}
	}
	var selfUsedCols []*expression.Column
	for _, aggrFunc := range la.AggFuncs {
		selfUsedCols = expression.ExtractColumnsFromExpressions(selfUsedCols, aggrFunc.Args, nil)
	}
	if len(la.AggFuncs) == 0 {
		// If all the aggregate functions are pruned, we should add an aggregate function to keep the correctness.
		one, err := aggregation.NewAggFuncDesc(la.ctx, ast.AggFuncFirstRow, []expression.Expression{expression.One}, false)
		if err != nil {
			return err
		}
		la.AggFuncs = []*aggregation.AggFuncDesc{one}
		col := &expression.Column{
			UniqueID: la.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  types.NewFieldType(mysql.TypeLonglong),
		}
		la.schema.Columns = []*expression.Column{col}
	}

	if len(la.GroupByItems) > 0 {
		for i := len(la.GroupByItems) - 1; i >= 0; i-- {
			cols := expression.ExtractColumns(la.GroupByItems[i])
			if len(cols) == 0 {
				la.GroupByItems = append(la.GroupByItems[:i], la.GroupByItems[i+1:]...)
			} else {
				selfUsedCols = append(selfUsedCols, cols...)
			}
		}
		// If all the group by items are pruned, we should add a constant 1 to keep the correctness.
		// Because `select count(*) from t` is different from `select count(*) from t group by 1`.
		if len(la.GroupByItems) == 0 {
			la.GroupByItems = []expression.Expression{expression.One}
		}
	}
	return child.PruneColumns(selfUsedCols)
}

// PruneColumns implements LogicalPlan interface.
// If any expression can view as a constant in execution stage, such as correlated column, constant,
// we do prune them. Note that we can't prune the expressions contain non-deterministic functions, such as rand().
func (ls *LogicalSort) PruneColumns(parentUsedCols []*expression.Column) error {
	child := ls.children[0]
	for i := len(ls.ByItems) - 1; i >= 0; i-- {
		cols := expression.ExtractColumns(ls.ByItems[i].Expr)
		if len(cols) == 0 {
			if expression.IsMutableEffectsExpr(ls.ByItems[i].Expr) {
				continue
			}
			ls.ByItems = append(ls.ByItems[:i], ls.ByItems[i+1:]...)
		} else if ls.ByItems[i].Expr.GetType().Tp == mysql.TypeNull {
			ls.ByItems = append(ls.ByItems[:i], ls.ByItems[i+1:]...)
		} else {
			parentUsedCols = append(parentUsedCols, cols...)
		}
	}
	return child.PruneColumns(parentUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalUnionAll) PruneColumns(parentUsedCols []*expression.Column) error {
	used := getUsedList(parentUsedCols, p.schema)

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
	} else {
		// Issue 10341: p.schema.Columns might contain table name (AsName), but p.Children()0].Schema().Columns does not.
		for i := len(used) - 1; i >= 0; i-- {
			if !used[i] {
				p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
			}
		}
	}
	for _, child := range p.Children() {
		err := child.PruneColumns(parentUsedCols)
		if err != nil {
			return err
		}
	}
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalUnionScan) PruneColumns(parentUsedCols []*expression.Column) error {
	parentUsedCols = append(parentUsedCols, p.handleCol)
	return p.children[0].PruneColumns(parentUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (ds *DataSource) PruneColumns(parentUsedCols []*expression.Column) error {
	used := getUsedList(parentUsedCols, ds.schema)

	var (
		handleCol     *expression.Column
		handleColInfo *model.ColumnInfo
	)
	if ds.handleCol != nil {
		handleCol = ds.handleCol
		handleColInfo = ds.Columns[ds.schema.ColumnIndex(handleCol)]
	}
	originSchemaColumns := ds.schema.Columns
	originColumns := ds.Columns
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			ds.schema.Columns = append(ds.schema.Columns[:i], ds.schema.Columns[i+1:]...)
			ds.Columns = append(ds.Columns[:i], ds.Columns[i+1:]...)
		}
	}
	// For SQL like `select 1 from t`, tikv's response will be empty if no column is in schema.
	// So we'll force to push one if schema doesn't have any column.
	if ds.schema.Len() == 0 {
		if ds.table.Type().IsClusterTable() && len(originColumns) > 0 {
			// use the first line.
			handleCol = originSchemaColumns[0]
			handleColInfo = originColumns[0]
		} else if handleCol == nil {
			handleCol = ds.newExtraHandleSchemaCol()
			handleColInfo = model.NewExtraHandleColInfo()
		}
		ds.Columns = append(ds.Columns, handleColInfo)
		ds.schema.Append(handleCol)
	}
	if ds.handleCol != nil && ds.schema.ColumnIndex(ds.handleCol) == -1 {
		ds.handleCol = nil
	}
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalTableDual) PruneColumns(parentUsedCols []*expression.Column) error {
	used := getUsedList(parentUsedCols, p.Schema())

	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
		}
	}
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
	lChild := p.children[0]
	rChild := p.children[1]
	composedSchema := expression.MergeSchema(lChild.Schema(), rChild.Schema())
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin {
		p.schema = lChild.Schema().Clone()
	} else if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		joinCol := p.schema.Columns[len(p.schema.Columns)-1]
		p.schema = lChild.Schema().Clone()
		p.schema.Append(joinCol)
	} else {
		p.schema = composedSchema
	}
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalJoin) PruneColumns(parentUsedCols []*expression.Column) error {
	leftCols, rightCols := p.extractUsedCols(parentUsedCols)

	err := p.children[0].PruneColumns(leftCols)
	if err != nil {
		return err
	}

	err = p.children[1].PruneColumns(rightCols)
	if err != nil {
		return err
	}

	p.mergeSchema()
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (la *LogicalApply) PruneColumns(parentUsedCols []*expression.Column) error {
	leftCols, rightCols := la.extractUsedCols(parentUsedCols)

	err := la.children[1].PruneColumns(rightCols)
	if err != nil {
		return err
	}

	la.corCols = extractCorColumnsBySchema(la.children[1], la.children[0].Schema())
	for _, col := range la.corCols {
		leftCols = append(leftCols, &col.Column)
	}

	err = la.children[0].PruneColumns(leftCols)
	if err != nil {
		return err
	}

	la.mergeSchema()
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalLock) PruneColumns(parentUsedCols []*expression.Column) error {
	if p.Lock != ast.SelectLockForUpdate && p.Lock != ast.SelectLockForUpdateNoWait {
		return p.baseLogicalPlan.PruneColumns(parentUsedCols)
	}

	for _, cols := range p.tblID2Handle {
		parentUsedCols = append(parentUsedCols, cols...)
	}
	return p.children[0].PruneColumns(parentUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalWindow) PruneColumns(parentUsedCols []*expression.Column) error {
	windowColumns := p.GetWindowResultColumns()
	len := 0
	for _, col := range parentUsedCols {
		used := false
		for _, windowColumn := range windowColumns {
			if windowColumn.Equal(nil, col) {
				used = true
				break
			}
		}
		if !used {
			parentUsedCols[len] = col
			len++
		}
	}
	parentUsedCols = parentUsedCols[:len]
	parentUsedCols = p.extractUsedCols(parentUsedCols)
	err := p.children[0].PruneColumns(parentUsedCols)
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

func (*columnPruner) name() string {
	return "column_prune"
}
