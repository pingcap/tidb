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
		newColumn := schema.RetrieveColumn(v)
		if newColumn == nil {
			return nil, errors.Errorf("Can't Find column %s.", expr.ToString())
		}
		return newColumn, nil
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
func PruneColumnsAndResolveIndices(p Plan, usedCols []*expression.Column) ([]bool, error) {
	//TODO: Currently we only implement index resolving, column pruning will be implemented later.
	var cols []*expression.Column
	switch v := p.(type) {
	case *Projection:
		// Prune
		var used []bool
		used = makeUsedList(usedCols, p.GetSchema())
		for i := len(used) - 1; i >= 0; i-- {
			if !used[i] {
				v.schema = append(v.schema[:i], v.schema[i+1:]...)
				v.Exprs = append(v.Exprs[:i], v.Exprs[i+1:]...)
			}
		}
		v.schema.InitIndices()
		for _, expr := range v.Exprs {
			cols = extractColumn(expr, cols)
		}
		_, err := PruneColumnsAndResolveIndices(p.GetChildByIndex(0), cols)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for i, expr := range v.Exprs {
			v.Exprs[i], err = retrieveColumnsInExpression(expr, p.GetChildByIndex(0).GetSchema())
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		return used, nil
	case *Selection:
		cols = usedCols
		for _, cond := range v.Conditions {
			cols = extractColumn(cond, cols)
		}
		used, err := PruneColumnsAndResolveIndices(p.GetChildByIndex(0), cols)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for i := len(used) - 1; i >= 0; i-- {
			if !used[i] {
				v.schema = append(v.schema[:i], v.schema[i+1:]...)
			}
		}
		for i, cond := range v.Conditions {
			v.Conditions[i], err = retrieveColumnsInExpression(cond, p.GetChildByIndex(0).GetSchema())
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		v.schema.InitIndices()
		return used, nil
	case *Aggregation:
		used := makeUsedList(usedCols, p.GetSchema())
		for i := len(used) - 1; i >= 0; i-- {
			if !used[i] {
				v.schema = append(v.schema[:i], v.schema[i+1:]...)
				v.AggFuncs = append(v.AggFuncs[:i], v.AggFuncs[i+1:]...)
			}
		}
		for _, aggrFunc := range v.AggFuncs {
			for _, arg := range aggrFunc.GetArgs() {
				cols = extractColumn(arg, cols)
			}
		}
		for _, expr := range v.GroupByItems {
			cols = extractColumn(expr, cols)
		}
		_, err := PruneColumnsAndResolveIndices(p.GetChildByIndex(0), cols)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, aggrFunc := range v.AggFuncs {
			for i, arg := range aggrFunc.GetArgs() {
				var newArg expression.Expression
				newArg, err = retrieveColumnsInExpression(arg, p.GetChildByIndex(0).GetSchema())
				if err != nil {
					return nil, errors.Trace(err)
				}
				aggrFunc.SetArgs(i, newArg)
			}
		}
		v.schema.InitIndices()
		return used, nil
	case *NewSort:
		cols = usedCols
		for _, item := range v.ByItems {
			cols = extractColumn(item.Expr, cols)
		}
		used, err := PruneColumnsAndResolveIndices(p.GetChildByIndex(0), cols)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for i := len(used) - 1; i >= 0; i-- {
			if !used[i] {
				v.schema = append(v.schema[:i], v.schema[i+1:]...)
			}
		}
		for _, item := range v.ByItems {
			item.Expr, err = retrieveColumnsInExpression(item.Expr, p.GetChildByIndex(0).GetSchema())
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		v.schema.InitIndices()
		return used, nil
	case *Union:
		used := makeUsedList(usedCols, p.GetSchema())
		for _, child := range p.GetChildren() {
			schema := child.GetSchema()
			var newSchema []*expression.Column
			for i, use := range used {
				if use {
					newSchema = append(newSchema, schema[i])
				}
			}
			_, err := PruneColumnsAndResolveIndices(child, newSchema)
			if err != nil {
				return used, errors.Trace(err)
			}
		}
		v.schema.InitIndices()
		return used, nil
	case *NewTableScan:
		used := makeUsedList(usedCols, p.GetSchema())
		for i := len(used) - 1; i >= 0; i-- {
			if !used[i] {
				v.schema = append(v.schema[:i], v.schema[i+1:]...)
				v.Columns = append(v.Columns[:i], v.Columns[i+1:]...)
			}
		}
		v.schema.InitIndices()
		return used, nil
	case *Limit:
		used, err := PruneColumnsAndResolveIndices(p.GetChildByIndex(0), usedCols)
		return used, errors.Trace(err)
	case *Join:
		cols = usedCols
		for _, eqCond := range v.EqualConditions {
			cols = extractColumn(eqCond, cols)
		}
		for _, leftCond := range v.LeftConditions {
			cols = extractColumn(leftCond, cols)
		}
		for _, rightCond := range v.RightConditions {
			cols = extractColumn(rightCond, cols)
		}
		for _, otherCond := range v.OtherConditions {
			cols = extractColumn(otherCond, cols)
		}
		var leftCols, rightCols []*expression.Column
		for _, col := range cols {
			if p.GetChildByIndex(0).GetSchema().GetIndex(col) != -1 {
				leftCols = append(leftCols, col)
			} else {
				rightCols = append(rightCols, col)
			}
		}
		usedLeft, err := PruneColumnsAndResolveIndices(p.GetChildByIndex(0), leftCols)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for i, leftCond := range v.LeftConditions {
			v.LeftConditions[i], err = retrieveColumnsInExpression(leftCond, p.GetChildByIndex(0).GetSchema())
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		usedRight, err := PruneColumnsAndResolveIndices(p.GetChildByIndex(1), rightCols)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for i, rightCond := range v.RightConditions {
			v.RightConditions[i], err = retrieveColumnsInExpression(rightCond, p.GetChildByIndex(1).GetSchema())
			if err != nil {
				return nil, errors.Trace(err)
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
				return nil, errors.Trace(err)
			}
		}
		for _, eqCond := range v.EqualConditions {
			eqCond.Args[0], err = retrieveColumnsInExpression(eqCond.Args[0], p.GetChildByIndex(0).GetSchema())
			if err != nil {
				return nil, errors.Trace(err)
			}
			eqCond.Args[1], err = retrieveColumnsInExpression(eqCond.Args[1], p.GetChildByIndex(1).GetSchema())
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		v.schema.InitIndices()
		return used, nil
	default:
		return nil, nil
	}
}
