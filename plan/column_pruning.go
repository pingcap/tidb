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
				return nil, errors.Errorf("Can't Find column %s.", expr.ToString())
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
		used[idx] = true
	}
	return used
}

// PruneColumnsAndResolveIndices prunes unused columns and resolves index for columns.
// This function returns a bool slice representing used columns, a column slice representing outer columns and an error.
func pruneColumnsAndResolveIndices(p Plan, parentUsedCols []*expression.Column) ([]bool, []*expression.Column, error) {
	var cols, outerCols []*expression.Column
	switch v := p.(type) {
	case *Projection:
		// Prune
		var used []bool
		used = makeUsedList(parentUsedCols, p.GetSchema())
		for i := len(used) - 1; i >= 0; i-- {
			if !used[i] {
				v.schema = append(v.schema[:i], v.schema[i+1:]...)
				v.Exprs = append(v.Exprs[:i], v.Exprs[i+1:]...)
			}
		}
		v.schema.InitIndices()
		for _, expr := range v.Exprs {
			cols, outerCols = extractColumn(expr, cols, outerCols)
		}
		_, outer, err := pruneColumnsAndResolveIndices(p.GetChildByIndex(0), cols)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		for i, expr := range v.Exprs {
			v.Exprs[i], err = retrieveColumnsInExpression(expr, p.GetChildByIndex(0).GetSchema())
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
		return used, append(outer, outerCols...), nil
	case *Selection:
		cols = parentUsedCols
		for _, cond := range v.Conditions {
			cols, outerCols = extractColumn(cond, cols, outerCols)
		}
		used, outer, err := pruneColumnsAndResolveIndices(p.GetChildByIndex(0), cols)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		for i := len(used) - 1; i >= 0; i-- {
			if !used[i] {
				v.schema = append(v.schema[:i], v.schema[i+1:]...)
			}
		}
		for i, cond := range v.Conditions {
			v.Conditions[i], err = retrieveColumnsInExpression(cond, p.GetChildByIndex(0).GetSchema())
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}

		v.schema.InitIndices()
		return used, append(outer, outerCols...), nil
	case *Apply:
		return pruneApply(v, parentUsedCols)
	case *Aggregation:
		used := makeUsedList(parentUsedCols, p.GetSchema())
		for i := len(used) - 1; i >= 0; i-- {
			if !used[i] {
				v.schema = append(v.schema[:i], v.schema[i+1:]...)
				v.AggFuncs = append(v.AggFuncs[:i], v.AggFuncs[i+1:]...)
			}
		}
		for _, aggrFunc := range v.AggFuncs {
			for _, arg := range aggrFunc.GetArgs() {
				cols, outerCols = extractColumn(arg, cols, outerCols)
			}
		}
		for _, expr := range v.GroupByItems {
			cols, outerCols = extractColumn(expr, cols, outerCols)
		}
		_, outer, err := pruneColumnsAndResolveIndices(p.GetChildByIndex(0), cols)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		for _, aggrFunc := range v.AggFuncs {
			for i, arg := range aggrFunc.GetArgs() {
				var newArg expression.Expression
				newArg, err = retrieveColumnsInExpression(arg, p.GetChildByIndex(0).GetSchema())
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				aggrFunc.SetArgs(i, newArg)
			}
		}
		v.schema.InitIndices()
		return used, append(outer, outerCols...), nil
	case *NewSort:
		cols = parentUsedCols
		for _, item := range v.ByItems {
			cols, outerCols = extractColumn(item.Expr, cols, outerCols)
		}
		used, outer, err := pruneColumnsAndResolveIndices(p.GetChildByIndex(0), cols)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		for i := len(used) - 1; i >= 0; i-- {
			if !used[i] {
				v.schema = append(v.schema[:i], v.schema[i+1:]...)
			}
		}
		for _, item := range v.ByItems {
			item.Expr, err = retrieveColumnsInExpression(item.Expr, p.GetChildByIndex(0).GetSchema())
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
		v.schema.InitIndices()
		return used, append(outer, outerCols...), nil
	case *Union:
		used := makeUsedList(parentUsedCols, p.GetSchema())
		for _, child := range p.GetChildren() {
			schema := child.GetSchema()
			var newSchema []*expression.Column
			for i, use := range used {
				if use {
					newSchema = append(newSchema, schema[i])
				}
			}
			_, outer, err := pruneColumnsAndResolveIndices(child, newSchema)
			outerCols = append(outerCols, outer...)
			if err != nil {
				return used, nil, errors.Trace(err)
			}
		}
		v.schema.InitIndices()
		return used, outerCols, nil
	case *NewTableScan:
		used := makeUsedList(parentUsedCols, p.GetSchema())
		for i := len(used) - 1; i >= 0; i-- {
			if !used[i] {
				v.schema = append(v.schema[:i], v.schema[i+1:]...)
				v.Columns = append(v.Columns[:i], v.Columns[i+1:]...)
			}
		}
		v.schema.InitIndices()
		return used, nil, nil
	case *Limit, *MaxOneRow:
		used, outer, err := pruneColumnsAndResolveIndices(p.GetChildByIndex(0), parentUsedCols)
		return used, outer, errors.Trace(err)
	case *Exists:
		used, outer, err := pruneColumnsAndResolveIndices(p.GetChildByIndex(0), nil)
		return used, outer, errors.Trace(err)
	case *Join:
		cols = parentUsedCols
		for _, eqCond := range v.EqualConditions {
			cols, outerCols = extractColumn(eqCond, cols, outerCols)
		}
		for _, leftCond := range v.LeftConditions {
			cols, outerCols = extractColumn(leftCond, cols, outerCols)
		}
		for _, rightCond := range v.RightConditions {
			cols, outerCols = extractColumn(rightCond, cols, outerCols)
		}
		for _, otherCond := range v.OtherConditions {
			cols, outerCols = extractColumn(otherCond, cols, outerCols)
		}
		var leftCols, rightCols []*expression.Column
		for _, col := range cols {
			if p.GetChildByIndex(0).GetSchema().GetIndex(col) != -1 {
				leftCols = append(leftCols, col)
			} else {
				rightCols = append(rightCols, col)
			}
		}
		usedLeft, outerLeft, err := pruneColumnsAndResolveIndices(p.GetChildByIndex(0), leftCols)
		outerCols = append(outerCols, outerLeft...)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		for i, leftCond := range v.LeftConditions {
			v.LeftConditions[i], err = retrieveColumnsInExpression(leftCond, p.GetChildByIndex(0).GetSchema())
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
		usedRight, outerRight, err := pruneColumnsAndResolveIndices(p.GetChildByIndex(1), rightCols)
		outerCols = append(outerCols, outerRight...)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		for i, rightCond := range v.RightConditions {
			v.RightConditions[i], err = retrieveColumnsInExpression(rightCond, p.GetChildByIndex(1).GetSchema())
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
		used := append(usedLeft, usedRight...)
		for i := len(used) - 1; i >= 0; i-- {
			if !used[i] {
				v.schema = append(v.schema[:i], v.schema[i+1:]...)
			}
		}
		for i, otherCond := range v.OtherConditions {
			v.OtherConditions[i], err = retrieveColumnsInExpression(otherCond, p.GetSchema())
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
		for _, eqCond := range v.EqualConditions {
			eqCond.Args[0], err = retrieveColumnsInExpression(eqCond.Args[0], p.GetChildByIndex(0).GetSchema())
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			eqCond.Args[1], err = retrieveColumnsInExpression(eqCond.Args[1], p.GetChildByIndex(1).GetSchema())
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
		v.schema.InitIndices()
		return used, outerCols, nil
	default:
		return nil, nil, nil
	}
}

// e.g. For query select b.c ,(select count(*) from a where a.id = b.id) from b. Its plan is Projection->Apply->TableScan.
// The schema of b is (a,b,c,id). When Pruning Apply, the parentUsedCols is (c, extra), outerSchema is (a,b,c,id).
// Then after pruning inner plan, the outer schema in apply becomes (id).
// Now there're two columns in parentUsedCols, c is the column from Apply's child ---- TableScan, but extra isn't.
// So only c in parentUsedCols and id in outerSchema can be passed to TableScan.
func pruneApply(v *Apply, parentUsedCols []*expression.Column) ([]bool, []*expression.Column, error) {
	_, outer, err := pruneColumnsAndResolveIndices(v.InnerPlan, v.InnerPlan.GetSchema())
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	used := makeUsedList(outer, v.OuterSchema)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			v.OuterSchema = append(v.OuterSchema[:i], v.OuterSchema[i+1:]...)
		}
	}
	newUsedCols := v.OuterSchema
	for _, used := range parentUsedCols {
		if v.GetChildByIndex(0).GetSchema().GetIndex(used) != -1 {
			newUsedCols = append(newUsedCols, used)
		}
	}
	used, outer, err = pruneColumnsAndResolveIndices(v.GetChildByIndex(0), newUsedCols)
	for _, col := range v.OuterSchema {
		col.Index = v.GetChildByIndex(0).GetSchema().GetIndex(col)
	}
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			v.schema = append(v.schema[:i], v.schema[i+1:]...)
		}
	}
	v.schema.InitIndices()
	return used, outer, nil
}
