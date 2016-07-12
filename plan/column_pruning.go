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
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/expression"
)

func retrieveColumnsInExpression(expr expression.Expression, schema expression.Schema) (
	expression.Expression, error) {
	switch v := expr.(type) {
	case *expression.ScalarFunction:
		for i, arg := range v.Args {
			newExpr, err := retrieveColumnsInExpression(arg, schema)
			if err != nil {
				return nil, errors.Trace(err)
			}
			v.Args[i] = newExpr
		}
	case *expression.Column:
		if !v.Correlated {
			newColumn := schema.RetrieveColumn(v)
			if newColumn == nil {
				return nil, errors.Errorf("Can't Find column %s from schema %s.", expr.ToString(), schema.ToString())
			}
			return newColumn, nil
		}
	}
	return expr, nil
}

func makeUsedList(usedCols []*expression.Column, schema expression.Schema) []bool {
	used := make([]bool, len(schema))
	for _, col := range usedCols {
		idx := schema.GetIndex(col)
		if idx == -1 {
			log.Errorf("Can't find column %s from schema %s.", col.ToString(), schema.ToString())
		}
		used[idx] = true
	}
	return used
}

// PruneColumnsAndResolveIndices implements LogicalPlan PruneColumnsAndResolveIndices interface.
func (p *Projection) PruneColumnsAndResolveIndices(parentUsedCols []*expression.Column) ([]*expression.Column, error) {
	var selfUsedCols, outerUsedCols []*expression.Column
	used := makeUsedList(parentUsedCols, p.schema)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.schema = append(p.schema[:i], p.schema[i+1:]...)
			p.Exprs = append(p.Exprs[:i], p.Exprs[i+1:]...)
		}
	}
	p.schema.InitIndices()
	for _, expr := range p.Exprs {
		selfUsedCols, outerUsedCols = extractColumn(expr, selfUsedCols, outerUsedCols)
	}
	childOuterUsedCols, err := p.GetChildByIndex(0).(LogicalPlan).PruneColumnsAndResolveIndices(selfUsedCols)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i, expr := range p.Exprs {
		p.Exprs[i], err = retrieveColumnsInExpression(expr, p.GetChildByIndex(0).GetSchema())
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return append(childOuterUsedCols, outerUsedCols...), nil
}

// PruneColumnsAndResolveIndices implements LogicalPlan PruneColumnsAndResolveIndices interface.
func (p *Selection) PruneColumnsAndResolveIndices(parentUsedCols []*expression.Column) ([]*expression.Column, error) {
	var outerUsedCols []*expression.Column
	for _, cond := range p.Conditions {
		parentUsedCols, outerUsedCols = extractColumn(cond, parentUsedCols, outerUsedCols)
	}
	childOuterUsedCols, err := p.GetChildByIndex(0).(LogicalPlan).PruneColumnsAndResolveIndices(parentUsedCols)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.SetSchema(p.GetChildByIndex(0).GetSchema())
	for i, cond := range p.Conditions {
		p.Conditions[i], err = retrieveColumnsInExpression(cond, p.GetChildByIndex(0).GetSchema())
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return append(childOuterUsedCols, outerUsedCols...), nil
}

// PruneColumnsAndResolveIndices implements LogicalPlan PruneColumnsAndResolveIndices interface.
func (p *Aggregation) PruneColumnsAndResolveIndices(parentUsedCols []*expression.Column) ([]*expression.Column, error) {
	child := p.GetChildByIndex(0).(LogicalPlan)
	used := makeUsedList(parentUsedCols, p.schema)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.schema = append(p.schema[:i], p.schema[i+1:]...)
			p.AggFuncs = append(p.AggFuncs[:i], p.AggFuncs[i+1:]...)
		}
	}
	var selfUsedCols, outerUsedCols []*expression.Column
	for _, aggrFunc := range p.AggFuncs {
		for _, arg := range aggrFunc.GetArgs() {
			selfUsedCols, outerUsedCols = extractColumn(arg, selfUsedCols, outerUsedCols)
		}
	}
	for _, expr := range p.GroupByItems {
		selfUsedCols, outerUsedCols = extractColumn(expr, selfUsedCols, outerUsedCols)
	}
	childOuterUsedCols, err := child.PruneColumnsAndResolveIndices(selfUsedCols)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, aggrFunc := range p.AggFuncs {
		for i, arg := range aggrFunc.GetArgs() {
			var newArg expression.Expression
			newArg, err = retrieveColumnsInExpression(arg, child.GetSchema())
			if err != nil {
				return nil, errors.Trace(err)
			}
			aggrFunc.SetArgs(i, newArg)
		}
	}
	for i, expr := range p.GroupByItems {
		p.GroupByItems[i], err = retrieveColumnsInExpression(expr, child.GetSchema())
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	p.schema.InitIndices()
	return append(childOuterUsedCols, outerUsedCols...), nil
}

// PruneColumnsAndResolveIndices implements LogicalPlan PruneColumnsAndResolveIndices interface.
func (p *NewSort) PruneColumnsAndResolveIndices(parentUsedCols []*expression.Column) ([]*expression.Column, error) {
	var outerUsedCols []*expression.Column
	for _, item := range p.ByItems {
		parentUsedCols, outerUsedCols = extractColumn(item.Expr, parentUsedCols, outerUsedCols)
	}
	childOuterUsedCols, err := p.GetChildByIndex(0).(LogicalPlan).PruneColumnsAndResolveIndices(parentUsedCols)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.SetSchema(p.GetChildByIndex(0).GetSchema())
	for _, item := range p.ByItems {
		item.Expr, err = retrieveColumnsInExpression(item.Expr, p.GetChildByIndex(0).GetSchema())
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return append(childOuterUsedCols, outerUsedCols...), nil
}

// PruneColumnsAndResolveIndices implements LogicalPlan PruneColumnsAndResolveIndices interface.
func (p *NewUnion) PruneColumnsAndResolveIndices(parentUsedCols []*expression.Column) ([]*expression.Column, error) {
	var outerUsedCols []*expression.Column
	used := makeUsedList(parentUsedCols, p.GetSchema())
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.schema = append(p.schema[:i], p.schema[i+1:]...)
		}
	}
	p.schema.InitIndices()
	for _, c := range p.GetChildren() {
		child := c.(LogicalPlan)
		schema := child.GetSchema()
		var newSchema []*expression.Column
		for i, use := range used {
			if use {
				newSchema = append(newSchema, schema[i])
			}
		}
		childOuterUsedCols, err := child.PruneColumnsAndResolveIndices(newSchema)
		outerUsedCols = append(outerUsedCols, childOuterUsedCols...)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return outerUsedCols, nil
}

// PruneColumnsAndResolveIndices implements LogicalPlan PruneColumnsAndResolveIndices interface.
func (p *NewTableScan) PruneColumnsAndResolveIndices(parentUsedCols []*expression.Column) ([]*expression.Column, error) {
	used := makeUsedList(parentUsedCols, p.schema)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.schema = append(p.schema[:i], p.schema[i+1:]...)
			p.Columns = append(p.Columns[:i], p.Columns[i+1:]...)
		}
	}
	p.schema.InitIndices()
	return nil, nil
}

// PruneColumnsAndResolveIndices implements LogicalPlan PruneColumnsAndResolveIndices interface.
func (p *NewTableDual) PruneColumnsAndResolveIndices(parentUsedCols []*expression.Column) ([]*expression.Column, error) {
	return nil, nil
}

// PruneColumnsAndResolveIndices implements LogicalPlan PruneColumnsAndResolveIndices interface.
func (p *Trim) PruneColumnsAndResolveIndices(parentUsedCols []*expression.Column) ([]*expression.Column, error) {
	used := makeUsedList(parentUsedCols, p.schema)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.schema = append(p.schema[:i], p.schema[i+1:]...)
		}
	}
	childOuterUsedCols, err := p.GetChildByIndex(0).(LogicalPlan).PruneColumnsAndResolveIndices(parentUsedCols)
	return childOuterUsedCols, errors.Trace(err)
}

// PruneColumnsAndResolveIndices implements LogicalPlan PruneColumnsAndResolveIndices interface.
func (p *Exists) PruneColumnsAndResolveIndices(parentUsedCols []*expression.Column) ([]*expression.Column, error) {
	return p.GetChildByIndex(0).(LogicalPlan).PruneColumnsAndResolveIndices(nil)
}

// PruneColumnsAndResolveIndices implements LogicalPlan PruneColumnsAndResolveIndices interface.
func (p *Insert) PruneColumnsAndResolveIndices(_ []*expression.Column) ([]*expression.Column, error) {
	if len(p.GetChildren()) == 0 {
		return nil, nil
	}
	child := p.GetChildByIndex(0).(LogicalPlan)
	return child.PruneColumnsAndResolveIndices(child.GetSchema())
}

// PruneColumnsAndResolveIndices implements LogicalPlan PruneColumnsAndResolveIndices interface.
func (p *Join) PruneColumnsAndResolveIndices(parentUsedCols []*expression.Column) ([]*expression.Column, error) {
	var outerUsedCols []*expression.Column
	for _, eqCond := range p.EqualConditions {
		parentUsedCols, outerUsedCols = extractColumn(eqCond, parentUsedCols, outerUsedCols)
	}
	for _, leftCond := range p.LeftConditions {
		parentUsedCols, outerUsedCols = extractColumn(leftCond, parentUsedCols, outerUsedCols)
	}
	for _, rightCond := range p.RightConditions {
		parentUsedCols, outerUsedCols = extractColumn(rightCond, parentUsedCols, outerUsedCols)
	}
	for _, otherCond := range p.OtherConditions {
		parentUsedCols, outerUsedCols = extractColumn(otherCond, parentUsedCols, outerUsedCols)
	}
	var leftCols, rightCols []*expression.Column
	for _, col := range parentUsedCols {
		if p.GetChildByIndex(0).GetSchema().GetIndex(col) != -1 {
			leftCols = append(leftCols, col)
		} else {
			rightCols = append(rightCols, col)
		}
	}
	outerLeft, err := p.GetChildByIndex(0).(LogicalPlan).PruneColumnsAndResolveIndices(leftCols)
	outerUsedCols = append(outerUsedCols, outerLeft...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i, leftCond := range p.LeftConditions {
		p.LeftConditions[i], err = retrieveColumnsInExpression(leftCond, p.GetChildByIndex(0).GetSchema())
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	outerRight, err := p.GetChildByIndex(1).(LogicalPlan).PruneColumnsAndResolveIndices(rightCols)
	outerUsedCols = append(outerUsedCols, outerRight...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i, rightCond := range p.RightConditions {
		p.RightConditions[i], err = retrieveColumnsInExpression(rightCond, p.GetChildByIndex(1).GetSchema())
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	p.schema = append(p.GetChildByIndex(0).GetSchema().DeepCopy(), p.GetChildByIndex(1).GetSchema().DeepCopy()...)
	p.schema.InitIndices()
	for i, otherCond := range p.OtherConditions {
		p.OtherConditions[i], err = retrieveColumnsInExpression(otherCond, p.GetSchema())
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	for _, eqCond := range p.EqualConditions {
		eqCond.Args[0], err = retrieveColumnsInExpression(eqCond.Args[0], p.GetChildByIndex(0).GetSchema())
		if err != nil {
			return nil, errors.Trace(err)
		}
		eqCond.Args[1], err = retrieveColumnsInExpression(eqCond.Args[1], p.GetChildByIndex(1).GetSchema())
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return outerUsedCols, nil
}

// PruneColumnsAndResolveIndices implements LogicalPlan PruneColumnsAndResolveIndices interface.
// e.g. For query select b.c ,(select count(*) from a where a.id = b.id) from b. Its plan is Projection->Apply->TableScan.
// The schema of b is (a,b,c,id). When Pruning Apply, the parentUsedCols is (c, extra), outerSchema is (a,b,c,id).
// Then after pruning inner plan, the childOuterUsedCols schema in apply becomes (id).
// Now there're two columns in parentUsedCols, c is the column from Apply's child ---- TableScan, but extra isn't.
// So only c in parentUsedCols and id in outerSchema can be passed to TableScan.
func (p *Apply) PruneColumnsAndResolveIndices(parentUsedCols []*expression.Column) ([]*expression.Column, error) {
	child := p.GetChildByIndex(0).(LogicalPlan)
	newUsedCols := p.OuterSchema
	for _, used := range parentUsedCols {
		if p.GetChildByIndex(0).GetSchema().GetIndex(used) != -1 {
			newUsedCols = append(newUsedCols, used)
		}
	}
	if p.Checker != nil {
		condUsedCols, _ := extractColumn(p.Checker.Condition, nil, nil)
		for _, used := range condUsedCols {
			if p.GetChildByIndex(0).GetSchema().GetIndex(used) != -1 {
				newUsedCols = append(newUsedCols, used)
			}
		}
	}
	childOuterUsedCols, err := child.PruneColumnsAndResolveIndices(newUsedCols)
	for _, col := range p.OuterSchema {
		col.Index = p.GetChildByIndex(0).GetSchema().GetIndex(col)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	combinedSchema := append(p.GetChildByIndex(0).GetSchema().DeepCopy(), p.InnerPlan.GetSchema().DeepCopy()...)
	if p.Checker == nil {
		p.schema = combinedSchema
	} else {
		combinedSchema.InitIndices()
		p.Checker.Condition, err = retrieveColumnsInExpression(p.Checker.Condition, combinedSchema)
		if err != nil {
			return nil, errors.Trace(err)
		}
		p.schema = append(p.GetChildByIndex(0).GetSchema().DeepCopy(), p.schema[len(p.schema)-1])
	}
	p.schema.InitIndices()
	return append(childOuterUsedCols, p.outerColumns...), nil
}
