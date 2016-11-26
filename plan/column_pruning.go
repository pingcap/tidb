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
	"github.com/pingcap/tidb/expression"
)

func getUsedList(usedCols []*expression.Column, schema expression.Schema) []bool {
	used := make([]bool, len(schema))
	for _, col := range usedCols {
		idx := schema.GetIndex(col)
		if idx == -1 {
			log.Errorf("Can't find column %s from schema %s.", col, schema)
		}
		used[idx] = true
	}
	return used
}

// PruneColumns implements LogicalPlan interface.
func (p *Projection) PruneColumns(parentUsedCols []*expression.Column) {
	child := p.GetChildByIndex(0).(LogicalPlan)
	var selfUsedCols []*expression.Column
	used := getUsedList(parentUsedCols, p.schema)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.schema = append(p.schema[:i], p.schema[i+1:]...)
			p.Exprs = append(p.Exprs[:i], p.Exprs[i+1:]...)
		}
	}
	for _, expr := range p.Exprs {
		selfUsedCols = append(selfUsedCols, extractColumns(expr)...)
	}
	child.PruneColumns(selfUsedCols)
	p.schema.InitIndices()
}

// PruneColumns implements LogicalPlan interface.
func (p *Selection) PruneColumns(parentUsedCols []*expression.Column) {
	child := p.GetChildByIndex(0).(LogicalPlan)
	for _, cond := range p.Conditions {
		parentUsedCols = append(parentUsedCols, extractColumns(cond)...)
	}
	child.PruneColumns(parentUsedCols)
	p.SetSchema(child.GetSchema())
}

// PruneColumns implements LogicalPlan interface.
func (p *Aggregation) PruneColumns(parentUsedCols []*expression.Column) {
	child := p.GetChildByIndex(0).(LogicalPlan)
	used := getUsedList(parentUsedCols, p.schema)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.schema = append(p.schema[:i], p.schema[i+1:]...)
			p.AggFuncs = append(p.AggFuncs[:i], p.AggFuncs[i+1:]...)
		}
	}
	var selfUsedCols []*expression.Column
	for _, aggrFunc := range p.AggFuncs {
		for _, arg := range aggrFunc.GetArgs() {
			selfUsedCols = append(selfUsedCols, extractColumns(arg)...)
		}
	}
	for _, expr := range p.GroupByItems {
		selfUsedCols = append(selfUsedCols, extractColumns(expr)...)
	}
	child.PruneColumns(selfUsedCols)
	p.schema.InitIndices()
}

// PruneColumns implements LogicalPlan interface.
func (p *Sort) PruneColumns(parentUsedCols []*expression.Column) {
	child := p.GetChildByIndex(0).(LogicalPlan)
	for _, item := range p.ByItems {
		parentUsedCols = append(parentUsedCols, extractColumns(item.Expr)...)
	}
	child.PruneColumns(parentUsedCols)
	p.SetSchema(p.GetChildByIndex(0).GetSchema())
}

// PruneColumns implements LogicalPlan interface.
func (p *Union) PruneColumns(parentUsedCols []*expression.Column) {
	used := getUsedList(parentUsedCols, p.GetSchema())
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.schema = append(p.schema[:i], p.schema[i+1:]...)
		}
	}
	for _, c := range p.GetChildren() {
		child := c.(LogicalPlan)
		schema := child.GetSchema()
		var newSchema []*expression.Column
		for i, use := range used {
			if use {
				newSchema = append(newSchema, schema[i])
			}
		}
		child.PruneColumns(newSchema)
	}
	p.schema.InitIndices()
}

// PruneColumns implements LogicalPlan interface.
func (p *DataSource) PruneColumns(parentUsedCols []*expression.Column) {
	used := getUsedList(parentUsedCols, p.schema)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.schema = append(p.schema[:i], p.schema[i+1:]...)
			p.Columns = append(p.Columns[:i], p.Columns[i+1:]...)
		}
	}
	p.schema.InitIndices()
}

// PruneColumns implements LogicalPlan interface.
func (p *TableDual) PruneColumns(_ []*expression.Column) {
}

// PruneColumns implements LogicalPlan interface.
func (p *Trim) PruneColumns(parentUsedCols []*expression.Column) {
	used := getUsedList(parentUsedCols, p.schema)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.schema = append(p.schema[:i], p.schema[i+1:]...)
		}
	}
	p.GetChildByIndex(0).(LogicalPlan).PruneColumns(parentUsedCols)
}

// PruneColumns implements LogicalPlan interface.
func (p *Exists) PruneColumns(parentUsedCols []*expression.Column) {
	p.GetChildByIndex(0).(LogicalPlan).PruneColumns(nil)
}

// PruneColumns implements LogicalPlan interface.
func (p *Insert) PruneColumns(_ []*expression.Column) {
	if len(p.GetChildren()) == 0 {
		return
	}
	child := p.GetChildByIndex(0).(LogicalPlan)
	child.PruneColumns(child.GetSchema())
}

// PruneColumns implements LogicalPlan interface.
func (p *Join) PruneColumns(parentUsedCols []*expression.Column) {
	for _, eqCond := range p.EqualConditions {
		parentUsedCols = append(parentUsedCols, extractColumns(eqCond)...)
	}
	for _, leftCond := range p.LeftConditions {
		parentUsedCols = append(parentUsedCols, extractColumns(leftCond)...)
	}
	for _, rightCond := range p.RightConditions {
		parentUsedCols = append(parentUsedCols, extractColumns(rightCond)...)
	}
	for _, otherCond := range p.OtherConditions {
		parentUsedCols = append(parentUsedCols, extractColumns(otherCond)...)
	}
	lChild := p.GetChildByIndex(0).(LogicalPlan)
	rChild := p.GetChildByIndex(1).(LogicalPlan)
	var leftCols, rightCols []*expression.Column
	for _, col := range parentUsedCols {
		if lChild.GetSchema().GetIndex(col) != -1 {
			leftCols = append(leftCols, col)
		} else if rChild.GetSchema().GetIndex(col) != -1 {
			rightCols = append(rightCols, col)
		}
	}
	lChild.PruneColumns(leftCols)
	rChild.PruneColumns(rightCols)
	composedSchema := append(lChild.GetSchema().Clone(), rChild.GetSchema().Clone()...)
	if p.JoinType == SemiJoin {
		p.schema = lChild.GetSchema().Clone()
	} else if p.JoinType == SemiJoinWithAux {
		p.schema = append(lChild.GetSchema().Clone(), p.schema[len(p.schema)-1])
	} else {
		p.schema = composedSchema
	}
	p.schema.InitIndices()
}

// PruneColumns implements LogicalPlan interface.
// e.g. For query select b.c, (select count(*) from a where a.id = b.id) from b. Its plan is Projection->Apply->TableScan.
// The schema of b is (a,b,c,id). When Pruning Apply, the parentUsedCols is (c, extra), outerSchema is (a,b,c,id).
// Then after pruning inner plan, the childOuterUsedCols schema in apply becomes (id).
// Now there're two columns in parentUsedCols, c is the column from Apply's child ---- TableScan, but extra isn't.
// So only c in parentUsedCols and id in outerSchema can be passed to TableScan.
func (p *Apply) PruneColumns(parentUsedCols []*expression.Column) {
	child := p.GetChildByIndex(0).(LogicalPlan)
	innerPlan := p.GetChildByIndex(1).(LogicalPlan)
	var usedCols []*expression.Column
	if p.Checker != nil {
		parentUsedCols = append(parentUsedCols, extractColumns(p.Checker.Condition)...)
	}
	for _, col := range parentUsedCols {
		if child.GetSchema().GetIndex(col) != -1 {
			usedCols = append(usedCols, col)
		}
	}
	innerPlan.PruneColumns(innerPlan.GetSchema())
	corCols := innerPlan.extractCorrelatedCols()
	for _, corCol := range corCols {
		idx := child.GetSchema().GetIndex(&corCol.Column)
		if idx != -1 {
			usedCols = append(usedCols, &corCol.Column)
		}
	}
	child.PruneColumns(usedCols)
	combinedSchema := append(child.GetSchema().Clone(), innerPlan.GetSchema().Clone()...)
	if p.Checker == nil {
		p.schema = combinedSchema
	} else {
		p.schema = append(child.GetSchema().Clone(), p.schema[len(p.schema)-1])
	}
	p.schema.InitIndices()
}

// PruneColumns implements LogicalPlan interface.
func (p *Update) PruneColumns(parentUsedCols []*expression.Column) {
	p.baseLogicalPlan.PruneColumns(p.GetChildByIndex(0).GetSchema())
}
