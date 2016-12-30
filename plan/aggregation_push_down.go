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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

type aggPushDownSolver struct {
	alloc *idAllocator
	ctx   context.Context
}

// isDecomposable checks if an aggregate function is decomposable. An aggregation function $F$ is decomposable
// if there exist aggregation functions F_1 and F_2 such that F(S_1 union all S_2) = F_2(F_1(S_1),F_1(S_2)),
// where S_1 and S_2 are two sets of values. We call S_1 and S_2 partial groups.
// It's easy to see that max, min, first row is decomposable, no matter whether it's distinct, but sum(distinct) and
// count(distinct) is not.
// Currently we don't support avg and concat.
func (a *aggPushDownSolver) isDecomposable(fun expression.AggregationFunction) bool {
	switch fun.GetName() {
	case ast.AggFuncAvg, ast.AggFuncGroupConcat:
		// TODO: Support avg push down.
		return false
	case ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow:
		return true
	case ast.AggFuncSum, ast.AggFuncCount:
		return !fun.IsDistinct()
	default:
		return false
	}
}

// getAggFuncChildIdx gets which children it belongs to, 0 stands for left, 1 stands for right, -1 stands for both.
func (a *aggPushDownSolver) getAggFuncChildIdx(aggFunc expression.AggregationFunction, schema expression.Schema) int {
	fromLeft, fromRight := false, false
	var cols []*expression.Column
	for _, arg := range aggFunc.GetArgs() {
		cols = append(cols, expression.ExtractColumns(arg)...)
	}
	for _, col := range cols {
		if schema.GetColumnIndex(col) != -1 {
			fromLeft = true
		} else {
			fromRight = true
		}
	}
	if fromLeft && fromRight {
		return -1
	} else if fromLeft {
		return 0
	}
	return 1
}

// collectAggFuncs collects all aggregate functions and splits them into two parts: "leftAggFuncs" and "rightAggFuncs" whose
// arguments are all from left child or right child separately. If some aggregate functions have the arguments that have
// columns both from left and right children, the whole aggregation is forbidden to push down.
func (a *aggPushDownSolver) collectAggFuncs(agg *Aggregation, join *Join) (valid bool, leftAggFuncs, rightAggFuncs []expression.AggregationFunction) {
	valid = true
	leftChild := join.GetChildByIndex(0)
	for _, aggFunc := range agg.AggFuncs {
		if !a.isDecomposable(aggFunc) {
			return false, nil, nil
		}
		index := a.getAggFuncChildIdx(aggFunc, leftChild.GetSchema())
		switch index {
		case 0:
			leftAggFuncs = append(leftAggFuncs, aggFunc)
		case 1:
			rightAggFuncs = append(rightAggFuncs, aggFunc)
		default:
			return false, nil, nil
		}
	}
	return
}

// collectGbyCols collects all columns from gby-items and join-conditions and splits them into two parts: "leftGbyCols" and
// "rightGbyCols". e.g. For query "SELECT SUM(B.id) FROM A, B WHERE A.c1 = B.c1 AND A.c2 != B.c2 GROUP BY B.c3" , the optimized
// query should be "SELECT SUM(B.agg) FROM A, (SELECT SUM(id) as agg, c1, c2, c3 FROM B GROUP BY id, c1, c2, c3) as B
// WHERE A.c1 = B.c1 AND A.c2 != B.c2 GROUP BY B.c3". As you see, all the columns appearing in join-conditions should be
// treated as group by columns in join subquery.
func (a *aggPushDownSolver) collectGbyCols(agg *Aggregation, join *Join) (leftGbyCols, rightGbyCols []*expression.Column) {
	leftChild := join.GetChildByIndex(0)
	for _, gbyExpr := range agg.GroupByItems {
		cols := expression.ExtractColumns(gbyExpr)
		for _, col := range cols {
			if leftChild.GetSchema().GetColumnIndex(col) != -1 {
				leftGbyCols = append(leftGbyCols, col)
			} else {
				rightGbyCols = append(rightGbyCols, col)
			}
		}
	}
	// extract equal conditions
	for _, eqFunc := range join.EqualConditions {
		leftGbyCols = a.addGbyCol(leftGbyCols, eqFunc.GetArgs()[0].(*expression.Column))
		rightGbyCols = a.addGbyCol(rightGbyCols, eqFunc.GetArgs()[1].(*expression.Column))
	}
	for _, leftCond := range join.LeftConditions {
		cols := expression.ExtractColumns(leftCond)
		leftGbyCols = a.addGbyCol(leftGbyCols, cols...)
	}
	for _, rightCond := range join.RightConditions {
		cols := expression.ExtractColumns(rightCond)
		rightGbyCols = a.addGbyCol(rightGbyCols, cols...)
	}
	for _, otherCond := range join.OtherConditions {
		cols := expression.ExtractColumns(otherCond)
		for _, col := range cols {
			if leftChild.GetSchema().GetColumnIndex(col) != -1 {
				leftGbyCols = a.addGbyCol(leftGbyCols, col)
			} else {
				rightGbyCols = a.addGbyCol(rightGbyCols, col)
			}
		}
	}
	return
}

func (a *aggPushDownSolver) splitAggFuncsAndGbyCols(agg *Aggregation, join *Join) (valid bool,
	leftAggFuncs, rightAggFuncs []expression.AggregationFunction,
	leftGbyCols, rightGbyCols []*expression.Column) {
	valid, leftAggFuncs, rightAggFuncs = a.collectAggFuncs(agg, join)
	if !valid {
		return
	}
	leftGbyCols, rightGbyCols = a.collectGbyCols(agg, join)
	return
}

// addGbyCol adds a column to gbyCols. If a group by column has existed, it will not be added repeatedly.
func (a *aggPushDownSolver) addGbyCol(gbyCols []*expression.Column, cols ...*expression.Column) []*expression.Column {
	for _, c := range cols {
		duplicate := false
		for _, gbyCol := range gbyCols {
			if c.Equal(gbyCol, a.ctx) {
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

// checkValidJoin checks if this join should be pushed across.
func (a *aggPushDownSolver) checkValidJoin(join *Join) bool {
	return join.JoinType == InnerJoin || join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin
}

// decompose splits an aggregate function to two parts: a final mode function and a partial mode function. Currently
// there are no differences between partial mode and complete mode, so we can confuse them.
func (a *aggPushDownSolver) decompose(aggFunc expression.AggregationFunction, schema expression.Schema, id string) ([]expression.AggregationFunction, expression.Schema) {
	// Result is a slice because avg should be decomposed to sum and count. Currently we don't process this case.
	result := []expression.AggregationFunction{aggFunc.Clone()}
	for _, aggFunc := range result {
		schema.Append(&expression.Column{
			ColName:  model.NewCIStr(fmt.Sprintf("join_agg_%d", schema.Len())), // useless but for debug
			FromID:   id,
			Position: len(schema.Columns),
			RetType:  aggFunc.GetType(),
		})
	}
	aggFunc.SetArgs(expression.Column2Exprs(schema.Columns[len(schema.Columns)-len(result):]))
	aggFunc.SetMode(expression.FinalMode)
	return result, schema
}

func (a *aggPushDownSolver) allFirstRow(aggFuncs []expression.AggregationFunction) bool {
	for _, fun := range aggFuncs {
		if fun.GetName() != ast.AggFuncFirstRow {
			return false
		}
	}
	return true
}

// tryToPushDownAgg tries to push down an aggregate function into a join path. If all aggFuncs are first row, we won't
// process it temporarily. If not, We will add additional group by columns and first row functions. We make a new aggregation
// operator.
func (a *aggPushDownSolver) tryToPushDownAgg(aggFuncs []expression.AggregationFunction, gbyCols []*expression.Column, join *Join, childIdx int) LogicalPlan {
	child := join.GetChildByIndex(childIdx).(LogicalPlan)
	if a.allFirstRow(aggFuncs) {
		return child
	}
	agg := a.makeNewAgg(aggFuncs, gbyCols)
	child.SetParents(agg)
	agg.SetChildren(child)
	// If agg has no group-by item, it will return a default value, which may cause some bugs.
	// So here we add a group-by item forcely.
	if len(agg.GroupByItems) == 0 {
		agg.GroupByItems = []expression.Expression{&expression.Constant{
			Value:   types.NewDatum(0),
			RetType: types.NewFieldType(mysql.TypeLong)}}
	}
	if (childIdx == 0 && join.JoinType == RightOuterJoin) || (childIdx == 1 && join.JoinType == LeftOuterJoin) {
		var existsDefaultValues bool
		join.DefaultValues, existsDefaultValues = a.getDefaultValues(agg)
		if !existsDefaultValues {
			return child
		}
	}
	return agg
}

func (a *aggPushDownSolver) getDefaultValues(agg *Aggregation) ([]types.Datum, bool) {
	defaultValues := make([]types.Datum, 0, agg.GetSchema().Len())
	for _, aggFunc := range agg.AggFuncs {
		value, existsDefaultValue := aggFunc.CalculateDefaultValue(agg.children[0].GetSchema(), a.ctx)
		if !existsDefaultValue {
			return nil, false
		}
		defaultValues = append(defaultValues, value)
	}
	return defaultValues, true
}

func (a *aggPushDownSolver) checkAnyCountAndSum(aggFuncs []expression.AggregationFunction) bool {
	for _, fun := range aggFuncs {
		if fun.GetName() == ast.AggFuncSum || fun.GetName() == ast.AggFuncCount {
			return true
		}
	}
	return false
}

func (a *aggPushDownSolver) makeNewAgg(aggFuncs []expression.AggregationFunction, gbyCols []*expression.Column) *Aggregation {
	agg := &Aggregation{
		GroupByItems:    expression.Column2Exprs(gbyCols),
		baseLogicalPlan: newBaseLogicalPlan(Agg, a.alloc),
		groupByCols:     gbyCols,
	}
	agg.initIDAndContext(a.ctx)
	var newAggFuncs []expression.AggregationFunction
	schema := expression.NewSchema(make([]*expression.Column, 0, len(aggFuncs)))
	for _, aggFunc := range aggFuncs {
		var newFuncs []expression.AggregationFunction
		newFuncs, schema = a.decompose(aggFunc, schema, agg.GetID())
		newAggFuncs = append(newAggFuncs, newFuncs...)
	}
	for _, gbyCol := range gbyCols {
		firstRow := expression.NewAggFunction(ast.AggFuncFirstRow, []expression.Expression{gbyCol.Clone()}, false)
		newAggFuncs = append(newAggFuncs, firstRow)
		schema.Append(gbyCol.Clone().(*expression.Column))
	}
	agg.AggFuncs = newAggFuncs
	agg.SetSchema(schema)
	return agg
}

func (a *aggPushDownSolver) pushAggCrossUnion(agg *Aggregation, unionSchema expression.Schema, unionChild LogicalPlan) LogicalPlan {
	newAgg := &Aggregation{
		AggFuncs:        make([]expression.AggregationFunction, 0, len(agg.AggFuncs)),
		GroupByItems:    make([]expression.Expression, 0, len(agg.GroupByItems)),
		baseLogicalPlan: newBaseLogicalPlan(Agg, a.alloc),
	}
	newAgg.SetSchema(agg.schema.Clone())
	newAgg.initIDAndContext(a.ctx)
	for _, aggFunc := range agg.AggFuncs {
		newAggFunc := aggFunc.Clone()
		newArgs := make([]expression.Expression, 0, len(newAggFunc.GetArgs()))
		for _, arg := range newAggFunc.GetArgs() {
			newArgs = append(newArgs, expression.ColumnSubstitute(arg, unionSchema, expression.Column2Exprs(unionChild.GetSchema().Columns)))
		}
		newAggFunc.SetArgs(newArgs)
		newAgg.AggFuncs = append(newAgg.AggFuncs, newAggFunc)
	}
	for _, gbyExpr := range agg.GroupByItems {
		newExpr := expression.ColumnSubstitute(gbyExpr, unionSchema, expression.Column2Exprs(unionChild.GetSchema().Columns))
		newAgg.GroupByItems = append(newAgg.GroupByItems, newExpr)
	}
	newAgg.collectGroupByColumns()
	newAgg.SetChildren(unionChild)
	unionChild.SetParents(newAgg)
	return newAgg
}

// aggPushDown tries to push down aggregate functions to join paths.
func (a *aggPushDownSolver) aggPushDown(p LogicalPlan) {
	if agg, ok := p.(*Aggregation); ok {
		child := agg.GetChildByIndex(0)
		if join, ok1 := child.(*Join); ok1 && a.checkValidJoin(join) {
			if valid, leftAggFuncs, rightAggFuncs, leftGbyCols, rightGbyCols := a.splitAggFuncsAndGbyCols(agg, join); valid {
				var lChild, rChild LogicalPlan
				// If there exist count or sum functions in left join path, we can't push any
				// aggregate function into right join path.
				rightInvalid := a.checkAnyCountAndSum(leftAggFuncs)
				leftInvalid := a.checkAnyCountAndSum(rightAggFuncs)
				if rightInvalid {
					rChild = join.GetChildByIndex(1).(LogicalPlan)
				} else {
					rChild = a.tryToPushDownAgg(rightAggFuncs, rightGbyCols, join, 1)
				}
				if leftInvalid {
					lChild = join.GetChildByIndex(0).(LogicalPlan)
				} else {
					lChild = a.tryToPushDownAgg(leftAggFuncs, leftGbyCols, join, 0)
				}
				join.SetChildren(lChild, rChild)
				lChild.SetParents(join)
				rChild.SetParents(join)
				join.SetSchema(expression.MergeSchema(lChild.GetSchema(), rChild.GetSchema()))
			}
		} else if proj, ok1 := child.(*Projection); ok1 {
			// TODO: This optimization is not always reasonable. We have not supported pushing projection to kv layer yet,
			// so we must do this optimization.
			for i, gbyItem := range agg.GroupByItems {
				agg.GroupByItems[i] = expression.ColumnSubstitute(gbyItem, proj.schema, proj.Exprs)
			}
			agg.collectGroupByColumns()
			for _, aggFunc := range agg.AggFuncs {
				newArgs := make([]expression.Expression, 0, len(aggFunc.GetArgs()))
				for _, arg := range aggFunc.GetArgs() {
					newArgs = append(newArgs, expression.ColumnSubstitute(arg, proj.schema, proj.Exprs))
				}
				aggFunc.SetArgs(newArgs)
			}
			projChild := proj.children[0]
			agg.SetChildren(projChild)
			projChild.SetParents(agg)
		} else if union, ok1 := child.(*Union); ok1 {
			pushedAgg := a.makeNewAgg(agg.AggFuncs, agg.groupByCols)
			newChildren := make([]Plan, 0, len(union.children))
			for _, child := range union.children {
				newChild := a.pushAggCrossUnion(pushedAgg, union.schema, child.(LogicalPlan))
				newChildren = append(newChildren, newChild)
				newChild.SetParents(union)
			}
			union.SetChildren(newChildren...)
			union.SetSchema(pushedAgg.schema)
		}
	}
	for _, child := range p.GetChildren() {
		a.aggPushDown(child.(LogicalPlan))
	}
}
