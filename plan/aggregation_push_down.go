// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
)

func isDecomposable(fun expression.AggregationFunction) bool {
	switch fun.GetName() {
	case ast.AggFuncAvg, ast.AggFuncGroupConcat:
		// TODO: support avg push down.
		return false
	case ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow:
		return true
	case ast.AggFuncSum, ast.AggFuncCount:
		return !fun.IsDistinct()
	default:
		return false
	}
}

func collectAggFuncs(agg *Aggregation, join *Join) (valid bool, leftAggFuncs, rightAggFuncs []expression.AggregationFunction) {
	valid = true
	leftChild := join.GetChildByIndex(0)
	for _, aggFunc := range agg.AggFuncs {
		if isDecomposable(aggFunc) {
			fromLeft, fromRight := false, false
			var cols []*expression.Column
			for _, arg := range aggFunc.GetArgs() {
				cols, _ = extractColumn(arg, cols, nil)
			}
			for _, col := range cols {
				if leftChild.GetSchema().GetIndex(col) != -1 {
					fromLeft = true
				} else {
					fromRight = true
				}
			}
			if fromLeft && fromRight {
				return false, nil, nil
			} else if fromLeft {
				leftAggFuncs = append(leftAggFuncs, aggFunc)
			} else {
				rightAggFuncs = append(rightAggFuncs, aggFunc)
			}
		} else {
			return false, nil, nil
		}
	}
	return
}

func collectGbyCols(agg *Aggregation, join *Join) (leftGbyCols, rightGbyCols []*expression.Column) {
	leftChild := join.GetChildByIndex(0)
	for _, gbyExpr := range agg.GroupByItems {
		var cols []*expression.Column
		cols, _ = extractColumn(gbyExpr, cols, nil)
		for _, col := range cols {
			if leftChild.GetSchema().GetIndex(col) != -1 {
				leftGbyCols = append(leftGbyCols, col)
			} else {
				rightGbyCols = append(rightGbyCols, col)
			}
		}
	}
	// extract equal conditions
	for _, eqFunc := range join.EqualConditions {
		leftGbyCols = addGbyCol(leftGbyCols, eqFunc.Args[0].(*expression.Column))
		rightGbyCols = addGbyCol(rightGbyCols, eqFunc.Args[1].(*expression.Column))
	}
	for _, leftCond := range join.LeftConditions {
		var cols []*expression.Column
		cols, _ = extractColumn(leftCond, cols, nil)
		leftGbyCols = addGbyCol(leftGbyCols, cols...)
	}
	for _, rightCond := range join.RightConditions {
		var cols []*expression.Column
		cols, _ = extractColumn(rightCond, cols, nil)
		rightGbyCols = addGbyCol(rightGbyCols, cols...)
	}
	for _, otherCond := range join.OtherConditions {
		var cols []*expression.Column
		cols, _ = extractColumn(otherCond, cols, nil)
		for _, col := range cols {
			if leftChild.GetSchema().GetIndex(col) != -1 {
				leftGbyCols = addGbyCol(leftGbyCols, col)
			} else {
				rightGbyCols = addGbyCol(rightGbyCols, col)
			}
		}
	}
	return
}

func checkValidAgg(agg *Aggregation, join *Join) (valid bool,
	leftAggFuncs, rightAggFuncs []expression.AggregationFunction,
	leftGbyCols, rightGbyCols []*expression.Column) {
	valid, leftAggFuncs, rightAggFuncs = collectAggFuncs(agg, join)
	if !valid {
		return
	}
	leftGbyCols, rightGbyCols = collectGbyCols(agg, join)
	return
}

func addGbyCol(gbyCols []*expression.Column, cols ...*expression.Column) []*expression.Column {
	for _, c := range cols {
		duplicate := false
		for _, gbyCol := range gbyCols {
			if c.Equal(gbyCol) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			gbyCols = append(gbyCols, c)
		}
	}
	return gbyCols
}

func checkValidJoin(join *Join) bool {
	// TODO: Support outer join.
	return join.JoinType == InnerJoin
}

func decompose(aggFunc expression.AggregationFunction, schema expression.Schema, id string) ([]expression.AggregationFunction, expression.Schema) {
	var result []expression.AggregationFunction
	switch aggFunc.GetName() {
	case ast.AggFuncAvg:
		count := expression.NewAggFunction(ast.AggFuncCount, aggFunc.GetArgs(), false)
		sum := expression.NewAggFunction(ast.AggFuncSum, aggFunc.GetArgs(), false)
		result = []expression.AggregationFunction{count, sum}
	default:
		result = []expression.AggregationFunction{aggFunc.Clone()}
	}
	for _, aggFunc := range result {
		schema = append(schema, &expression.Column{
			ColName:  model.NewCIStr(fmt.Sprintf("join_agg_%d", len(schema))), // useless but for debug
			FromID:   id,
			Position: len(schema),
			RetType:  aggFunc.GetType(),
		})
	}
	aggFunc.SetArgs(expression.Schema2Exprs(schema[len(schema)-len(result):]))
	aggFunc.SetMode(expression.FinalMode)
	return result, schema
}

func allFirstRow(aggFuncs []expression.AggregationFunction) bool {
	for _, fun := range aggFuncs {
		if fun.GetName() != ast.AggFuncFirstRow {
			return false
		}
	}
	return true
}

func (a *aggPushDownSolver) produceNewAggPlan(aggFuncs []expression.AggregationFunction, gbyCols []*expression.Column, join *Join, childIdx int) (LogicalPlan, bool) {
	child := join.GetChildByIndex(childIdx).(LogicalPlan)
	if allFirstRow(aggFuncs) {
		return child, false
	}
	agg := &Aggregation{
		GroupByItems:    expression.Schema2Exprs(gbyCols),
		baseLogicalPlan: newBaseLogicalPlan(Agg, a.alloc),
	}
	child.SetParents(agg)
	agg.SetChildren(child)
	agg.initID()
	var newAggFuncs []expression.AggregationFunction
	schema := make(expression.Schema, 0, len(aggFuncs))
	for _, aggFunc := range aggFuncs {
		var newFuncs []expression.AggregationFunction
		newFuncs, schema = decompose(aggFunc, schema, agg.GetID())
		newAggFuncs = append(newAggFuncs, newFuncs...)
	}
	for _, gbyCol := range gbyCols {
		firstRow := expression.NewAggFunction(ast.AggFuncFirstRow, []expression.Expression{gbyCol}, false)
		newAggFuncs = append(newAggFuncs, firstRow)
		schema = append(schema, gbyCol.Clone().(*expression.Column))
	}
	agg.AggFuncs = newAggFuncs
	agg.SetSchema(schema)
	return agg, true
}

func checkCountAndSum(aggFuncs []expression.AggregationFunction) bool {
	for _, fun := range aggFuncs {
		if fun.GetName() == ast.AggFuncSum || fun.GetName() == ast.AggFuncCount {
			return true
		}
	}
	return false
}

type aggPushDownSolver struct {
	alloc *idAllocator
	ctx   context.Context
}

func (a *aggPushDownSolver) aggPushDown(p LogicalPlan) {
	if agg, ok := p.(*Aggregation); ok {
		child := agg.GetChildByIndex(0)
		if join, ok1 := child.(*Join); ok1 && checkValidJoin(join) {
			if valid, leftAggFuncs, rightAggFuns, leftGbyCols, rightGbyCols := checkValidAgg(agg, join); valid {
				var lChild, rChild LogicalPlan
				var success bool
				lChild = join.GetChildByIndex(0).(LogicalPlan)
				if checkCountAndSum(leftAggFuncs) {
					success = false
					rChild = join.GetChildByIndex(1).(LogicalPlan)
				} else {
					rChild, success = a.produceNewAggPlan(rightAggFuns, rightGbyCols, join, 1)
				}
				if !success && !checkCountAndSum(rightAggFuns) {
					// TODO: We should support eager split latter.
					lChild, _ = a.produceNewAggPlan(leftAggFuncs, leftGbyCols, join, 0)
				}
				join.SetChildren(lChild, rChild)
				lChild.SetParents(join)
				rChild.SetParents(join)
				join.SetSchema(append(lChild.GetSchema().Clone(), rChild.GetSchema().Clone()...))
			}
		}
	}
	for _, child := range p.GetChildren() {
		a.aggPushDown(child.(LogicalPlan))
	}
}
