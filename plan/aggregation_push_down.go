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
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

type aggregationOptimizer struct {
	ctx context.Context
}

// isDecomposable checks if an aggregate function is decomposable. An aggregation function $F$ is decomposable
// if there exist aggregation functions F_1 and F_2 such that F(S_1 union all S_2) = F_2(F_1(S_1),F_1(S_2)),
// where S_1 and S_2 are two sets of values. We call S_1 and S_2 partial groups.
// It's easy to see that max, min, first row is decomposable, no matter whether it's distinct, but sum(distinct) and
// count(distinct) is not.
// Currently we don't support avg and concat.
func (a *aggregationOptimizer) isDecomposable(fun aggregation.Aggregation) bool {
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
func (a *aggregationOptimizer) getAggFuncChildIdx(aggFunc aggregation.Aggregation, schema *expression.Schema) int {
	fromLeft, fromRight := false, false
	var cols []*expression.Column
	cols = expression.ExtractColumnsFromExpressions(cols, aggFunc.GetArgs(), nil)
	for _, col := range cols {
		if schema.Contains(col) {
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
func (a *aggregationOptimizer) collectAggFuncs(agg *LogicalAggregation, join *LogicalJoin) (valid bool, leftAggFuncs, rightAggFuncs []aggregation.Aggregation) {
	valid = true
	leftChild := join.children[0]
	for _, aggFunc := range agg.AggFuncs {
		if !a.isDecomposable(aggFunc) {
			return false, nil, nil
		}
		index := a.getAggFuncChildIdx(aggFunc, leftChild.Schema())
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
func (a *aggregationOptimizer) collectGbyCols(agg *LogicalAggregation, join *LogicalJoin) (leftGbyCols, rightGbyCols []*expression.Column) {
	leftChild := join.children[0]
	for _, gbyExpr := range agg.GroupByItems {
		cols := expression.ExtractColumns(gbyExpr)
		for _, col := range cols {
			if leftChild.Schema().Contains(col) {
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
			if leftChild.Schema().Contains(col) {
				leftGbyCols = a.addGbyCol(leftGbyCols, col)
			} else {
				rightGbyCols = a.addGbyCol(rightGbyCols, col)
			}
		}
	}
	return
}

func (a *aggregationOptimizer) splitAggFuncsAndGbyCols(agg *LogicalAggregation, join *LogicalJoin) (valid bool,
	leftAggFuncs, rightAggFuncs []aggregation.Aggregation,
	leftGbyCols, rightGbyCols []*expression.Column) {
	valid, leftAggFuncs, rightAggFuncs = a.collectAggFuncs(agg, join)
	if !valid {
		return
	}
	leftGbyCols, rightGbyCols = a.collectGbyCols(agg, join)
	return
}

// addGbyCol adds a column to gbyCols. If a group by column has existed, it will not be added repeatedly.
func (a *aggregationOptimizer) addGbyCol(gbyCols []*expression.Column, cols ...*expression.Column) []*expression.Column {
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
func (a *aggregationOptimizer) checkValidJoin(join *LogicalJoin) bool {
	return join.JoinType == InnerJoin || join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin
}

// decompose splits an aggregate function to two parts: a final mode function and a partial mode function. Currently
// there are no differences between partial mode and complete mode, so we can confuse them.
func (a *aggregationOptimizer) decompose(aggFunc aggregation.Aggregation, schema *expression.Schema, id int) ([]aggregation.Aggregation, *expression.Schema) {
	// Result is a slice because avg should be decomposed to sum and count. Currently we don't process this case.
	result := []aggregation.Aggregation{aggFunc.Clone()}
	for _, aggFunc := range result {
		schema.Append(&expression.Column{
			ColName:  model.NewCIStr(fmt.Sprintf("join_agg_%d", schema.Len())), // useless but for debug
			FromID:   id,
			Position: schema.Len(),
			RetType:  aggFunc.GetType(),
		})
	}
	aggFunc.SetArgs(expression.Column2Exprs(schema.Columns[schema.Len()-len(result):]))
	aggFunc.SetMode(aggregation.FinalMode)
	return result, schema
}

func (a *aggregationOptimizer) allFirstRow(aggFuncs []aggregation.Aggregation) bool {
	for _, fun := range aggFuncs {
		if fun.GetName() != ast.AggFuncFirstRow {
			return false
		}
	}
	return true
}

// tryToPushDownAgg tries to push down an aggregate function into a join path. If all aggFuncs are first row, we won't
// process it temporarily. If not, We will add additional group by columns and first row functions. We make a new aggregation operator.
// If the pushed aggregation is grouped by unique key, it's no need to push it down.
func (a *aggregationOptimizer) tryToPushDownAgg(aggFuncs []aggregation.Aggregation, gbyCols []*expression.Column, join *LogicalJoin, childIdx int) LogicalPlan {
	child := join.children[childIdx].(LogicalPlan)
	if a.allFirstRow(aggFuncs) {
		return child
	}
	// If the join is multiway-join, we forbid pushing down.
	if _, ok := join.children[childIdx].(*LogicalJoin); ok {
		return child
	}
	tmpSchema := expression.NewSchema(gbyCols...)
	for _, key := range child.Schema().Keys {
		if tmpSchema.ColumnsIndices(key) != nil {
			return child
		}
	}
	agg := a.makeNewAgg(aggFuncs, gbyCols)
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

func (a *aggregationOptimizer) getDefaultValues(agg *LogicalAggregation) ([]types.Datum, bool) {
	defaultValues := make([]types.Datum, 0, agg.Schema().Len())
	for _, aggFunc := range agg.AggFuncs {
		value, existsDefaultValue := aggFunc.CalculateDefaultValue(agg.children[0].Schema(), a.ctx)
		if !existsDefaultValue {
			return nil, false
		}
		defaultValues = append(defaultValues, value)
	}
	return defaultValues, true
}

func (a *aggregationOptimizer) checkAnyCountAndSum(aggFuncs []aggregation.Aggregation) bool {
	for _, fun := range aggFuncs {
		if fun.GetName() == ast.AggFuncSum || fun.GetName() == ast.AggFuncCount {
			return true
		}
	}
	return false
}

func (a *aggregationOptimizer) makeNewAgg(aggFuncs []aggregation.Aggregation, gbyCols []*expression.Column) *LogicalAggregation {
	agg := LogicalAggregation{
		GroupByItems: expression.Column2Exprs(gbyCols),
		groupByCols:  gbyCols,
	}.init(a.ctx)
	aggLen := len(aggFuncs) + len(gbyCols)
	newAggFuncs := make([]aggregation.Aggregation, 0, aggLen)
	schema := expression.NewSchema(make([]*expression.Column, 0, aggLen)...)
	for _, aggFunc := range aggFuncs {
		var newFuncs []aggregation.Aggregation
		newFuncs, schema = a.decompose(aggFunc, schema, agg.ID())
		newAggFuncs = append(newAggFuncs, newFuncs...)
	}
	for _, gbyCol := range gbyCols {
		firstRow := aggregation.NewAggFunction(ast.AggFuncFirstRow, []expression.Expression{gbyCol.Clone()}, false)
		newAggFuncs = append(newAggFuncs, firstRow)
		schema.Append(gbyCol.Clone().(*expression.Column))
	}
	agg.AggFuncs = newAggFuncs
	agg.SetSchema(schema)
	return agg
}

// pushAggCrossUnion will try to push the agg down to the union. If the new aggregation's group-by columns doesn't contain unique key.
// We will return the new aggregation. Otherwise we will transform the aggregation to projection.
func (a *aggregationOptimizer) pushAggCrossUnion(agg *LogicalAggregation, unionSchema *expression.Schema, unionChild LogicalPlan) LogicalPlan {
	newAgg := LogicalAggregation{
		AggFuncs:     make([]aggregation.Aggregation, 0, len(agg.AggFuncs)),
		GroupByItems: make([]expression.Expression, 0, len(agg.GroupByItems)),
	}.init(a.ctx)
	newAgg.SetSchema(agg.schema.Clone())
	for _, aggFunc := range agg.AggFuncs {
		newAggFunc := aggFunc.Clone()
		newArgs := make([]expression.Expression, 0, len(newAggFunc.GetArgs()))
		for _, arg := range newAggFunc.GetArgs() {
			newArgs = append(newArgs, expression.ColumnSubstitute(arg, unionSchema, expression.Column2Exprs(unionChild.Schema().Columns)))
		}
		newAggFunc.SetArgs(newArgs)
		newAgg.AggFuncs = append(newAgg.AggFuncs, newAggFunc)
	}
	for _, gbyExpr := range agg.GroupByItems {
		newExpr := expression.ColumnSubstitute(gbyExpr, unionSchema, expression.Column2Exprs(unionChild.Schema().Columns))
		newAgg.GroupByItems = append(newAgg.GroupByItems, newExpr)
	}
	newAgg.collectGroupByColumns()
	tmpSchema := expression.NewSchema(newAgg.groupByCols...)
	// e.g. Union distinct will add a aggregation like `select join_agg_0, join_agg_1, join_agg_2 from t group by a, b, c` above UnionAll.
	// And the pushed agg will be something like `select a, b, c, a, b, c from t group by a, b, c`. So if we just return child as join does,
	// this will cause error during executor phase.
	for _, key := range unionChild.Schema().Keys {
		if tmpSchema.ColumnsIndices(key) != nil {
			proj := a.convertAggToProj(newAgg, a.ctx)
			proj.SetChildren(unionChild)
			return proj
		}
	}
	newAgg.SetChildren(unionChild)
	return newAgg
}

func (a *aggregationOptimizer) optimize(p LogicalPlan, ctx context.Context) (LogicalPlan, error) {
	if !ctx.GetSessionVars().AllowAggPushDown {
		return p, nil
	}
	a.ctx = ctx
	a.aggPushDown(p)
	return p, nil
}

// aggPushDown tries to push down aggregate functions to join paths.
func (a *aggregationOptimizer) aggPushDown(p LogicalPlan) LogicalPlan {
	if agg, ok := p.(*LogicalAggregation); ok {
		proj := a.tryToEliminateAggregation(agg)
		if proj != nil {
			p = proj
		} else {
			child := agg.children[0]
			if join, ok1 := child.(*LogicalJoin); ok1 && a.checkValidJoin(join) {
				if valid, leftAggFuncs, rightAggFuncs, leftGbyCols, rightGbyCols := a.splitAggFuncsAndGbyCols(agg, join); valid {
					var lChild, rChild LogicalPlan
					// If there exist count or sum functions in left join path, we can't push any
					// aggregate function into right join path.
					rightInvalid := a.checkAnyCountAndSum(leftAggFuncs)
					leftInvalid := a.checkAnyCountAndSum(rightAggFuncs)
					if rightInvalid {
						rChild = join.children[1].(LogicalPlan)
					} else {
						rChild = a.tryToPushDownAgg(rightAggFuncs, rightGbyCols, join, 1)
					}
					if leftInvalid {
						lChild = join.children[0].(LogicalPlan)
					} else {
						lChild = a.tryToPushDownAgg(leftAggFuncs, leftGbyCols, join, 0)
					}
					join.SetChildren(lChild, rChild)
					join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
					join.buildKeyInfo()
					proj := a.tryToEliminateAggregation(agg)
					if proj != nil {
						p = proj
					}
				}
			} else if proj, ok1 := child.(*LogicalProjection); ok1 {
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
			} else if union, ok1 := child.(*LogicalUnionAll); ok1 {
				var gbyCols []*expression.Column
				gbyCols = expression.ExtractColumnsFromExpressions(gbyCols, agg.GroupByItems, nil)
				pushedAgg := a.makeNewAgg(agg.AggFuncs, gbyCols)
				newChildren := make([]Plan, 0, len(union.children))
				for _, child := range union.children {
					newChild := a.pushAggCrossUnion(pushedAgg, union.schema, child.(LogicalPlan))
					newChildren = append(newChildren, newChild)
				}
				union.SetChildren(newChildren...)
				union.SetSchema(pushedAgg.schema)
			}
		}
	}
	newChildren := make([]Plan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild := a.aggPushDown(child.(LogicalPlan))
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	return p
}

// tryToEliminateAggregation will eliminate aggregation grouped by unique key.
// e.g. select min(b) from t group by a. If a is a unique key, then this sql is equal to `select b from t group by a`.
// For count(expr), sum(expr), avg(expr), count(distinct expr, [expr...]) we may need to rewrite the expr. Details are shown below.
// If we can eliminate agg successful, we return a projection. Else we return a nil pointer.
func (a *aggregationOptimizer) tryToEliminateAggregation(agg *LogicalAggregation) *LogicalProjection {
	schemaByGroupby := expression.NewSchema(agg.groupByCols...)
	coveredByUniqueKey := false
	for _, key := range agg.children[0].Schema().Keys {
		if schemaByGroupby.ColumnsIndices(key) != nil {
			coveredByUniqueKey = true
			break
		}
	}
	if coveredByUniqueKey {
		// GroupByCols has unique key, so this aggregation can be removed.
		proj := a.convertAggToProj(agg, a.ctx)
		proj.SetChildren(agg.children[0])
		return proj
	}
	return nil
}

func (a *aggregationOptimizer) convertAggToProj(agg *LogicalAggregation, ctx context.Context) *LogicalProjection {
	proj := LogicalProjection{
		Exprs: make([]expression.Expression, 0, len(agg.AggFuncs)),
	}.init(a.ctx)
	for _, fun := range agg.AggFuncs {
		expr := a.rewriteExpr(fun)
		proj.Exprs = append(proj.Exprs, expr)
	}
	proj.SetSchema(agg.schema.Clone())
	return proj
}

func (a *aggregationOptimizer) rewriteCount(exprs []expression.Expression) expression.Expression {
	// If is count(expr), we will change it to if(isnull(expr), 0, 1).
	// If is count(distinct x, y, z) we will change it to if(isnull(x) or isnull(y) or isnull(z), 0, 1).
	isNullExprs := make([]expression.Expression, 0, len(exprs))
	for _, expr := range exprs {
		isNullExpr := expression.NewFunctionInternal(a.ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), expr.Clone())
		isNullExprs = append(isNullExprs, isNullExpr)
	}
	innerExpr := expression.ComposeDNFCondition(a.ctx, isNullExprs...)
	newExpr := expression.NewFunctionInternal(a.ctx, ast.If, types.NewFieldType(mysql.TypeLonglong), innerExpr, expression.Zero, expression.One)
	return newExpr
}

// See https://dev.mysql.com/doc/refman/5.7/en/group-by-functions.html
// The SUM() and AVG() functions return a DECIMAL value for exact-value arguments (integer or DECIMAL),
// and a DOUBLE value for approximate-value arguments (FLOAT or DOUBLE).
func (a *aggregationOptimizer) rewriteSumOrAvg(exprs []expression.Expression) expression.Expression {
	// FIXME: Consider the case that avg is final mode.
	expr := exprs[0].Clone()
	switch expr.GetType().Tp {
	// Integer type should be cast to decimal.
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		return expression.BuildCastFunction(a.ctx, expr, types.NewFieldType(mysql.TypeNewDecimal))
	// Double and Decimal doesn't need to be cast.
	case mysql.TypeDouble, mysql.TypeNewDecimal:
		return expr
	// Float should be cast to double. And other non-numeric type should be cast to double too.
	default:
		return expression.BuildCastFunction(a.ctx, expr, types.NewFieldType(mysql.TypeDouble))
	}
}

// rewriteExpr will rewrite the aggregate function to expression doesn't contain aggregate function.
func (a *aggregationOptimizer) rewriteExpr(aggFunc aggregation.Aggregation) expression.Expression {
	switch aggFunc.GetName() {
	case ast.AggFuncCount:
		if aggFunc.GetMode() == aggregation.FinalMode {
			return a.rewriteSumOrAvg(aggFunc.GetArgs())
		}
		return a.rewriteCount(aggFunc.GetArgs())
	case ast.AggFuncSum, ast.AggFuncAvg:
		return a.rewriteSumOrAvg(aggFunc.GetArgs())
	default:
		// Default we do nothing about expr.
		return aggFunc.GetArgs()[0].Clone()
	}
}
