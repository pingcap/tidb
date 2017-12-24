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

package plan

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	log "github.com/sirupsen/logrus"
)

type columnPruner struct {
}

func (s *columnPruner) optimize(lp LogicalPlan, _ context.Context) (LogicalPlan, error) {
	lp.PruneColumns(lp.Schema().Columns)
	return lp, nil
}

func getUsedList(usedCols []*expression.Column, schema *expression.Schema) []bool {
	used := make([]bool, schema.Len())
	for _, col := range usedCols {
		idx := schema.ColumnIndex(col)
		if idx == -1 {
			log.Errorf("Can't find column %s from schema %s.", col, schema)
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
	child := p.children[0].(LogicalPlan)
	used := getUsedList(parentUsedCols, p.schema)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] && !exprHasSetVar(p.Exprs[i]) {
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
			p.Exprs = append(p.Exprs[:i], p.Exprs[i+1:]...)
		}
	}
	selfUsedCols := make([]*expression.Column, 0, len(p.Exprs))
	selfUsedCols = expression.ExtractColumnsFromExpressions(selfUsedCols, p.Exprs, nil)
	child.PruneColumns(selfUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalSelection) PruneColumns(parentUsedCols []*expression.Column) {
	child := p.children[0].(LogicalPlan)
	parentUsedCols = expression.ExtractColumnsFromExpressions(parentUsedCols, p.Conditions, nil)
	child.PruneColumns(parentUsedCols)
	p.SetSchema(child.Schema())
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalAggregation) PruneColumns(parentUsedCols []*expression.Column) {
	child := p.children[0].(LogicalPlan)
	used := getUsedList(parentUsedCols, p.schema)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
			p.AggFuncs = append(p.AggFuncs[:i], p.AggFuncs[i+1:]...)
		}
	}
	var selfUsedCols []*expression.Column
	for _, aggrFunc := range p.AggFuncs {
		selfUsedCols = expression.ExtractColumnsFromExpressions(selfUsedCols, aggrFunc.GetArgs(), nil)
	}
	if len(p.GroupByItems) > 0 {
		for i := len(p.GroupByItems) - 1; i >= 0; i-- {
			cols := expression.ExtractColumns(p.GroupByItems[i])
			if len(cols) == 0 {
				p.GroupByItems = append(p.GroupByItems[:i], p.GroupByItems[i+1:]...)
			} else {
				selfUsedCols = append(selfUsedCols, cols...)
			}
		}
		// If all the group by items are pruned, we should add a constant 1 to keep the correctness.
		// Because `select count(*) from t` is different from `select count(*) from t group by 1`.
		if len(p.GroupByItems) == 0 {
			p.GroupByItems = []expression.Expression{expression.One}
		}
	}
	child.PruneColumns(selfUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalSort) PruneColumns(parentUsedCols []*expression.Column) {
	child := p.children[0].(LogicalPlan)
	for i := len(p.ByItems) - 1; i >= 0; i-- {
		cols := expression.ExtractColumns(p.ByItems[i].Expr)
		if len(cols) == 0 {
			p.ByItems = append(p.ByItems[:i], p.ByItems[i+1:]...)
		} else {
			parentUsedCols = append(parentUsedCols, expression.ExtractColumns(p.ByItems[i].Expr)...)
		}
	}
	child.PruneColumns(parentUsedCols)
	p.SetSchema(p.children[0].Schema())
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalUnionAll) PruneColumns(parentUsedCols []*expression.Column) {
	for _, c := range p.Children() {
		child := c.(LogicalPlan)
		child.PruneColumns(parentUsedCols)
	}
	p.SetSchema(p.children[0].Schema())
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalUnionScan) PruneColumns(parentUsedCols []*expression.Column) {
	for _, col := range p.schema.TblID2Handle {
		parentUsedCols = append(parentUsedCols, col[0])
	}
	p.children[0].(LogicalPlan).PruneColumns(parentUsedCols)
	p.SetSchema(p.children[0].Schema())
}

// PruneColumns implements LogicalPlan interface.
func (p *DataSource) PruneColumns(parentUsedCols []*expression.Column) {
	used := getUsedList(parentUsedCols, p.schema)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
			p.Columns = append(p.Columns[:i], p.Columns[i+1:]...)
		}
	}
	for _, cols := range p.schema.TblID2Handle {
		if p.schema.ColumnIndex(cols[0]) == -1 {
			p.schema.TblID2Handle = nil
			break
		}
	}
	// For SQL like `select 1 from t`, tikv's response will be empty if no column is in schema.
	// So we'll force to push one if schema doesn't have any column.
	if p.schema.Len() == 0 {
		p.Columns = append(p.Columns, model.NewExtraHandleColInfo())
		p.schema.Append(p.newExtraHandleSchemaCol())
	}
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalTableDual) PruneColumns(_ []*expression.Column) {
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalExists) PruneColumns(parentUsedCols []*expression.Column) {
	p.children[0].(LogicalPlan).PruneColumns(nil)
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
	lChild := p.children[0].(LogicalPlan)
	rChild := p.children[1].(LogicalPlan)
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
	lChild := p.children[0].(LogicalPlan)
	rChild := p.children[1].(LogicalPlan)
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
	lChild := p.children[0].(LogicalPlan)
	rChild := p.children[1].(LogicalPlan)
	lChild.PruneColumns(leftCols)
	rChild.PruneColumns(rightCols)
	p.mergeSchema()
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalApply) PruneColumns(parentUsedCols []*expression.Column) {
	lChild := p.children[0].(LogicalPlan)
	rChild := p.children[1].(LogicalPlan)
	leftCols, rightCols := p.extractUsedCols(parentUsedCols)
	rChild.PruneColumns(rightCols)
	p.extractCorColumnsBySchema()
	for _, col := range p.corCols {
		leftCols = append(leftCols, &col.Column)
	}
	lChild.PruneColumns(leftCols)
	p.mergeSchema()
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalLock) PruneColumns(parentUsedCols []*expression.Column) {
	if p.Lock != ast.SelectLockForUpdate {
		p.baseLogicalPlan.PruneColumns(parentUsedCols)
	} else {
		for _, cols := range p.children[0].Schema().TblID2Handle {
			parentUsedCols = append(parentUsedCols, cols...)
		}
		p.children[0].(LogicalPlan).PruneColumns(parentUsedCols)
	}
}
