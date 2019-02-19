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
	"fmt"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
)

type columnPruner struct {
}

func (s *columnPruner) optimize(lp LogicalPlan) (LogicalPlan, error) {
	lp.PruneColumns(lp.Schema().Columns)
	return lp, nil
}

func getUsedList(usedCols []*expression.Column, schema *expression.Schema) []bool {
	used := make([]bool, schema.Len())
	for _, col := range usedCols {
		idx := schema.ColumnIndex(col)
		if idx == -1 {
			panic(fmt.Sprintf("Can't find column %s from schema %s.", col, schema))
		}
		used[idx] = true
	}
	return used
}

// exprHasSetVar checks if the expression has SetVar function.
func exprHasSetVar(expr expression.Expression) bool {
	scalaFunc, isScalaFunc := expr.(*expression.ScalarFunction)
	if !isScalaFunc {
		return false
	}
	if scalaFunc.FuncName.L == ast.SetVar {
		return true
	}
	for _, arg := range scalaFunc.GetArgs() {
		if exprHasSetVar(arg) {
			return true
		}
	}
	return false
}

// PruneColumns implements LogicalPlan interface.
// If any expression has SetVar functions, we do not prune it.
func (p *LogicalProjection) PruneColumns(parentUsedCols []*expression.Column) {
	child := p.children[0]
	used := getUsedList(parentUsedCols, p.schema)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] && !exprHasSetVar(p.Exprs[i]) {
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
			p.Exprs = append(p.Exprs[:i], p.Exprs[i+1:]...)
		}
	}
	// Prune TblID2Handle since that handle column may be pruned.
	for k, cols := range p.schema.TblID2Handle {
		if p.schema.ColumnIndex(cols[0]) == -1 {
			delete(p.schema.TblID2Handle, k)
		}
	}
	selfUsedCols := make([]*expression.Column, 0, len(p.Exprs))
	selfUsedCols = expression.ExtractColumnsFromExpressions(selfUsedCols, p.Exprs, nil)
	child.PruneColumns(selfUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalSelection) PruneColumns(parentUsedCols []*expression.Column) {
	child := p.children[0]
	parentUsedCols = expression.ExtractColumnsFromExpressions(parentUsedCols, p.Conditions, nil)
	child.PruneColumns(parentUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (la *LogicalAggregation) PruneColumns(parentUsedCols []*expression.Column) {
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
	child.PruneColumns(selfUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (ls *LogicalSort) PruneColumns(parentUsedCols []*expression.Column) {
	child := ls.children[0]
	for i := len(ls.ByItems) - 1; i >= 0; i-- {
		cols := expression.ExtractColumns(ls.ByItems[i].Expr)
		if len(cols) == 0 {
			ls.ByItems = append(ls.ByItems[:i], ls.ByItems[i+1:]...)
		} else {
			parentUsedCols = append(parentUsedCols, cols...)
		}
	}
	child.PruneColumns(parentUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalUnionAll) PruneColumns(parentUsedCols []*expression.Column) {
	used := getUsedList(parentUsedCols, p.schema)
	hasBeenUsed := false
	for i := range used {
		hasBeenUsed = hasBeenUsed || used[i]
	}
	if !hasBeenUsed {
		parentUsedCols = make([]*expression.Column, len(p.schema.Columns))
		copy(parentUsedCols, p.schema.Columns)
	}
	for _, child := range p.Children() {
		child.PruneColumns(parentUsedCols)
	}
	p.schema.Columns = p.children[0].Schema().Columns
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalUnionScan) PruneColumns(parentUsedCols []*expression.Column) {
	for _, col := range p.Schema().TblID2Handle {
		parentUsedCols = append(parentUsedCols, col[0])
	}
	p.children[0].PruneColumns(parentUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (ds *DataSource) PruneColumns(parentUsedCols []*expression.Column) {
	used := getUsedList(parentUsedCols, ds.schema)
	var (
		handleCol     *expression.Column
		handleColInfo *model.ColumnInfo
	)
	for i := len(used) - 1; i >= 0; i-- {
		if ds.tableInfo.PKIsHandle && mysql.HasPriKeyFlag(ds.Columns[i].Flag) {
			handleCol = ds.schema.Columns[i]
			handleColInfo = ds.Columns[i]
		}
		if !used[i] {
			ds.schema.Columns = append(ds.schema.Columns[:i], ds.schema.Columns[i+1:]...)
			ds.Columns = append(ds.Columns[:i], ds.Columns[i+1:]...)
		}
	}
	for k, cols := range ds.schema.TblID2Handle {
		if ds.schema.ColumnIndex(cols[0]) == -1 {
			delete(ds.schema.TblID2Handle, k)
		}
	}
	// For SQL like `select 1 from t`, tikv's response will be empty if no column is in schema.
	// So we'll force to push one if schema doesn't have any column.
	if ds.schema.Len() == 0 && !infoschema.IsMemoryDB(ds.DBName.L) {
		if handleCol == nil {
			handleCol = ds.newExtraHandleSchemaCol()
			handleColInfo = model.NewExtraHandleColInfo()
		}
		ds.Columns = append(ds.Columns, handleColInfo)
		ds.schema.Append(handleCol)
	}
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalTableDual) PruneColumns(parentUsedCols []*expression.Column) {
	used := getUsedList(parentUsedCols, p.Schema())
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
		}
	}
	for k, cols := range p.schema.TblID2Handle {
		if p.schema.ColumnIndex(cols[0]) == -1 {
			delete(p.schema.TblID2Handle, k)
		}
	}
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
func (p *LogicalJoin) PruneColumns(parentUsedCols []*expression.Column) {
	leftCols, rightCols := p.extractUsedCols(parentUsedCols)
	lChild := p.children[0]
	rChild := p.children[1]
	lChild.PruneColumns(leftCols)
	rChild.PruneColumns(rightCols)
	p.mergeSchema()
}

// PruneColumns implements LogicalPlan interface.
func (la *LogicalApply) PruneColumns(parentUsedCols []*expression.Column) {
	lChild := la.children[0]
	rChild := la.children[1]
	leftCols, rightCols := la.extractUsedCols(parentUsedCols)
	rChild.PruneColumns(rightCols)
	la.extractCorColumnsBySchema()
	for _, col := range la.corCols {
		leftCols = append(leftCols, &col.Column)
	}
	lChild.PruneColumns(leftCols)
	la.mergeSchema()
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalLock) PruneColumns(parentUsedCols []*expression.Column) {
	if p.Lock != ast.SelectLockForUpdate {
		p.baseLogicalPlan.PruneColumns(parentUsedCols)
	} else {
		for _, cols := range p.children[0].Schema().TblID2Handle {
			parentUsedCols = append(parentUsedCols, cols...)
		}
		p.children[0].PruneColumns(parentUsedCols)
	}
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalWindow) PruneColumns(parentUsedCols []*expression.Column) {
	windowColumn := p.GetWindowResultColumn()
	len := 0
	for _, col := range parentUsedCols {
		if !windowColumn.Equal(nil, col) {
			parentUsedCols[len] = col
			len++
		}
	}
	parentUsedCols = parentUsedCols[:len]
	parentUsedCols = p.extractUsedCols(parentUsedCols)
	p.children[0].PruneColumns(parentUsedCols)
	p.SetSchema(p.children[0].Schema().Clone())
	p.Schema().Append(windowColumn)
}

func (p *LogicalWindow) extractUsedCols(parentUsedCols []*expression.Column) []*expression.Column {
	for _, arg := range p.WindowFuncDesc.Args {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(arg)...)
	}
	for _, by := range p.PartitionBy {
		parentUsedCols = append(parentUsedCols, by.Col)
	}
	for _, by := range p.OrderBy {
		parentUsedCols = append(parentUsedCols, by.Col)
	}
	return parentUsedCols
}
