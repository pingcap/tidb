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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
)

type columnPruner struct {
}

func (s *columnPruner) optimize(lp LogicalPlan, _ context.Context, _ *idAllocator) (LogicalPlan, error) {
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

// exprHasSetVar checks if the expression has set-var function. If do, we should not prune it.
func exprHasSetVar(expr expression.Expression) bool {
	if fun, ok := expr.(*expression.ScalarFunction); ok {
		canPrune := true
		if fun.FuncName.L == ast.SetVar {
			return false
		}
		for _, arg := range fun.GetArgs() {
			canPrune = canPrune && exprHasSetVar(arg)
			if !canPrune {
				return false
			}
		}
	}
	return true
}

// PruneColumns implements LogicalPlan interface.
func (p *Projection) PruneColumns(parentUsedCols []*expression.Column) {
	child := p.children[0].(LogicalPlan)
	var selfUsedCols []*expression.Column
	used := getUsedList(parentUsedCols, p.schema)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] && exprHasSetVar(p.Exprs[i]) {
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
			p.Exprs = append(p.Exprs[:i], p.Exprs[i+1:]...)
		}
	}
	for _, expr := range p.Exprs {
		selfUsedCols = append(selfUsedCols, expression.ExtractColumns(expr)...)
	}
	child.PruneColumns(selfUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (p *Selection) PruneColumns(parentUsedCols []*expression.Column) {
	child := p.children[0].(LogicalPlan)
	for _, cond := range p.Conditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(cond)...)
	}
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
		for _, arg := range aggrFunc.GetArgs() {
			selfUsedCols = append(selfUsedCols, expression.ExtractColumns(arg)...)
		}
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
func (p *Sort) PruneColumns(parentUsedCols []*expression.Column) {
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
func (p *Union) PruneColumns(parentUsedCols []*expression.Column) {
	used := getUsedList(parentUsedCols, p.Schema())
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
		}
	}
	for _, c := range p.Children() {
		child := c.(LogicalPlan)
		schema := child.Schema()
		var newCols []*expression.Column
		for i, use := range used {
			if use {
				newCols = append(newCols, schema.Columns[i])
			}
		}
		child.PruneColumns(newCols)
	}
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
}

// PruneColumns implements LogicalPlan interface.
func (p *TableDual) PruneColumns(_ []*expression.Column) {
}

// PruneColumns implements LogicalPlan interface.
func (p *Exists) PruneColumns(parentUsedCols []*expression.Column) {
	p.children[0].(LogicalPlan).PruneColumns(nil)
}

// PruneColumns implements LogicalPlan interface.
func (p *Insert) PruneColumns(_ []*expression.Column) {
	if len(p.Children()) == 0 {
		return
	}
	child := p.children[0].(LogicalPlan)
	child.PruneColumns(child.Schema().Columns)
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
	if p.JoinType == SemiJoin {
		p.schema = lChild.Schema().Clone()
	} else if p.JoinType == LeftOuterSemiJoin {
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
func (p *Update) PruneColumns(parentUsedCols []*expression.Column) {
	p.baseLogicalPlan.PruneColumns(p.children[0].Schema().Columns)
}

// PruneColumns implements LogicalPlan interface.
func (p *Delete) PruneColumns(parentUsedCols []*expression.Column) {
	p.baseLogicalPlan.PruneColumns(p.children[0].Schema().Columns)
}

// PruneColumns implements LogicalPlan interface.
// We should not prune columns for Analyze.
func (p *Analyze) PruneColumns(parentUsedCols []*expression.Column) {

}
