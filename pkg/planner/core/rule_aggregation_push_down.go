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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"bytes"
	"context"
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/types"
)

type aggregationPushDownSolver struct {
	aggregationEliminateChecker
}

// isDecomposable checks if an aggregate function is decomposable. An aggregation function $F$ is decomposable
// if there exist aggregation functions F_1 and F_2 such that F(S_1 union all S_2) = F_2(F_1(S_1),F_1(S_2)),
// where S_1 and S_2 are two sets of values. We call S_1 and S_2 partial groups.
// For example, Max(S_1 union S_2) = Max(Max(S_1) union Max(S_2)), thus we think Max is decomposable.
// It's easy to see that max, min, first row is decomposable, no matter whether it's distinct, but sum(distinct) and
// count(distinct) is not.
// Currently we don't support avg and concat.
func (*aggregationPushDownSolver) isDecomposableWithJoin(fun *aggregation.AggFuncDesc) bool {
	if len(fun.OrderByItems) > 0 {
		return false
	}
	switch fun.Name {
	case ast.AggFuncAvg, ast.AggFuncGroupConcat, ast.AggFuncVarPop, ast.AggFuncJsonArrayagg, ast.AggFuncJsonObjectAgg, ast.AggFuncStddevPop, ast.AggFuncVarSamp, ast.AggFuncApproxPercentile, ast.AggFuncStddevSamp:
		// TODO: Support avg push down.
		return false
	case ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow:
		return true
	case ast.AggFuncSum, ast.AggFuncCount:
		return !fun.HasDistinct
	default:
		return false
	}
}

func (*aggregationPushDownSolver) isDecomposableWithUnion(fun *aggregation.AggFuncDesc) bool {
	if len(fun.OrderByItems) > 0 {
		return false
	}
	switch fun.Name {
	case ast.AggFuncGroupConcat, ast.AggFuncVarPop, ast.AggFuncJsonArrayagg, ast.AggFuncApproxPercentile, ast.AggFuncJsonObjectAgg:
		return false
	case ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow:
		return true
	case ast.AggFuncSum, ast.AggFuncCount, ast.AggFuncAvg, ast.AggFuncApproxCountDistinct:
		return true
	default:
		return false
	}
}

// getAggFuncChildIdx gets which children it belongs to.
// 0 stands for left, 1 stands for right, -1 stands for both, 2 stands for neither (e.g. count(*), sum(1) ...)
func (*aggregationPushDownSolver) getAggFuncChildIdx(aggFunc *aggregation.AggFuncDesc, lSchema, rSchema *expression.Schema) int {
	fromLeft, fromRight := false, false
	var cols []*expression.Column
	cols = expression.ExtractColumnsFromExpressions(cols, aggFunc.Args, nil)
	for _, col := range cols {
		if lSchema.Contains(col) {
			fromLeft = true
		}
		if rSchema.Contains(col) {
			fromRight = true
		}
	}
	if fromLeft && fromRight {
		return -1
	} else if fromLeft {
		return 0
	} else if fromRight {
		return 1
	}
	return 2
}

// collectAggFuncs collects all aggregate functions and splits them into two parts: "leftAggFuncs" and "rightAggFuncs" whose
// arguments are all from left child or right child separately. If some aggregate functions have the arguments that have
// columns both from left and right children, the whole aggregation is forbidden to push down.
func (a *aggregationPushDownSolver) collectAggFuncs(agg *LogicalAggregation, join *LogicalJoin) (valid bool, leftAggFuncs, rightAggFuncs []*aggregation.AggFuncDesc) {
	valid = true
	leftChild := join.Children()[0]
	rightChild := join.Children()[1]
	for _, aggFunc := range agg.AggFuncs {
		if !a.isDecomposableWithJoin(aggFunc) {
			return false, nil, nil
		}
		index := a.getAggFuncChildIdx(aggFunc, leftChild.Schema(), rightChild.Schema())
		switch index {
		case 0:
			if join.JoinType == RightOuterJoin && !a.checkAllArgsColumn(aggFunc) {
				return false, nil, nil
			}
			leftAggFuncs = append(leftAggFuncs, aggFunc)
		case 1:
			if join.JoinType == LeftOuterJoin && !a.checkAllArgsColumn(aggFunc) {
				return false, nil, nil
			}
			rightAggFuncs = append(rightAggFuncs, aggFunc)
		case 2:
			// arguments are constant
			switch join.JoinType {
			case LeftOuterJoin:
				leftAggFuncs = append(leftAggFuncs, aggFunc)
			case RightOuterJoin:
				rightAggFuncs = append(rightAggFuncs, aggFunc)
			default:
				// either left or right is fine, ideally we'd better put this to the hash build side
				rightAggFuncs = append(rightAggFuncs, aggFunc)
			}
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
func (a *aggregationPushDownSolver) collectGbyCols(agg *LogicalAggregation, join *LogicalJoin) (leftGbyCols, rightGbyCols []*expression.Column) {
	leftChild := join.Children()[0]
	ctx := agg.SCtx()
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
		leftGbyCols = a.addGbyCol(ctx, leftGbyCols, eqFunc.GetArgs()[0].(*expression.Column))
		rightGbyCols = a.addGbyCol(ctx, rightGbyCols, eqFunc.GetArgs()[1].(*expression.Column))
	}
	for _, leftCond := range join.LeftConditions {
		cols := expression.ExtractColumns(leftCond)
		leftGbyCols = a.addGbyCol(ctx, leftGbyCols, cols...)
	}
	for _, rightCond := range join.RightConditions {
		cols := expression.ExtractColumns(rightCond)
		rightGbyCols = a.addGbyCol(ctx, rightGbyCols, cols...)
	}
	for _, otherCond := range join.OtherConditions {
		cols := expression.ExtractColumns(otherCond)
		for _, col := range cols {
			if leftChild.Schema().Contains(col) {
				leftGbyCols = a.addGbyCol(ctx, leftGbyCols, col)
			} else {
				rightGbyCols = a.addGbyCol(ctx, rightGbyCols, col)
			}
		}
	}
	return
}

func (a *aggregationPushDownSolver) splitAggFuncsAndGbyCols(agg *LogicalAggregation, join *LogicalJoin) (valid bool,
	leftAggFuncs, rightAggFuncs []*aggregation.AggFuncDesc,
	leftGbyCols, rightGbyCols []*expression.Column) {
	valid, leftAggFuncs, rightAggFuncs = a.collectAggFuncs(agg, join)
	if !valid {
		return
	}
	leftGbyCols, rightGbyCols = a.collectGbyCols(agg, join)
	return
}

// addGbyCol adds a column to gbyCols. If a group by column has existed, it will not be added repeatedly.
func (*aggregationPushDownSolver) addGbyCol(ctx base.PlanContext, gbyCols []*expression.Column, cols ...*expression.Column) []*expression.Column {
	for _, c := range cols {
		duplicate := false
		for _, gbyCol := range gbyCols {
			if c.Equal(ctx.GetExprCtx().GetEvalCtx(), gbyCol) {
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
func (*aggregationPushDownSolver) checkValidJoin(join *LogicalJoin) bool {
	return join.JoinType == InnerJoin || join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin
}

// decompose splits an aggregate function to two parts: a final mode function and a partial mode function. Currently
// there are no differences between partial mode and complete mode, so we can confuse them.
func (*aggregationPushDownSolver) decompose(ctx base.PlanContext, aggFunc *aggregation.AggFuncDesc,
	schema *expression.Schema, nullGenerating bool) ([]*aggregation.AggFuncDesc, *expression.Schema) {
	// Result is a slice because avg should be decomposed to sum and count. Currently we don't process this case.
	result := []*aggregation.AggFuncDesc{aggFunc.Clone()}
	for _, aggFunc := range result {
		schema.Append(&expression.Column{
			UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  aggFunc.RetTp,
		})
	}
	cols := schema.Columns[schema.Len()-len(result):]
	aggFunc.Args = make([]expression.Expression, 0, len(cols))
	// if the partial aggregation is on the null generating side, we have to clear the NOT NULL flag
	// for the final aggregate functions' arguments
	for _, col := range cols {
		if nullGenerating {
			arg := *col
			newFieldType := *arg.RetType
			newFieldType.DelFlag(mysql.NotNullFlag)
			arg.RetType = &newFieldType
			aggFunc.Args = append(aggFunc.Args, &arg)
		} else {
			aggFunc.Args = append(aggFunc.Args, col)
		}
	}
	aggFunc.Mode = aggregation.FinalMode
	return result, schema
}

// tryToPushDownAgg tries to push down an aggregate function into a join path. If all aggFuncs are first row, we won't
// process it temporarily. If not, We will add additional group by columns and first row functions. We make a new aggregation operator.
// If the pushed aggregation is grouped by unique key, it's no need to push it down.
func (a *aggregationPushDownSolver) tryToPushDownAgg(oldAgg *LogicalAggregation, aggFuncs []*aggregation.AggFuncDesc, gbyCols []*expression.Column,
	join *LogicalJoin, childIdx int, blockOffset int, opt *optimizetrace.LogicalOptimizeOp) (_ base.LogicalPlan, err error) {
	child := join.Children()[childIdx]
	if aggregation.IsAllFirstRow(aggFuncs) {
		return child, nil
	}
	// If the join is multiway-join, we forbid pushing down.
	if _, ok := join.Children()[childIdx].(*LogicalJoin); ok {
		return child, nil
	}
	tmpSchema := expression.NewSchema(gbyCols...)
	for _, key := range child.Schema().Keys {
		if tmpSchema.ColumnsIndices(key) != nil { // gby item need to be covered by key.
			return child, nil
		}
	}
	nullGenerating := (join.JoinType == LeftOuterJoin && childIdx == 1) ||
		(join.JoinType == RightOuterJoin && childIdx == 0)
	agg, err := a.makeNewAgg(join.SCtx(), aggFuncs, gbyCols, oldAgg.PreferAggType, oldAgg.PreferAggToCop, blockOffset, nullGenerating)
	if err != nil {
		return nil, err
	}
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
			return child, nil
		}
	}
	appendAggPushDownAcrossJoinTraceStep(oldAgg, agg, aggFuncs, join, childIdx, opt)
	return agg, nil
}

func (*aggregationPushDownSolver) getDefaultValues(agg *LogicalAggregation) ([]types.Datum, bool) {
	defaultValues := make([]types.Datum, 0, agg.Schema().Len())
	for _, aggFunc := range agg.AggFuncs {
		value, existsDefaultValue := aggFunc.EvalNullValueInOuterJoin(agg.SCtx().GetExprCtx(), agg.Children()[0].Schema())
		if !existsDefaultValue {
			return nil, false
		}
		defaultValues = append(defaultValues, value)
	}
	return defaultValues, true
}

func (*aggregationPushDownSolver) checkAnyCountAndSum(aggFuncs []*aggregation.AggFuncDesc) bool {
	for _, fun := range aggFuncs {
		if fun.Name == ast.AggFuncSum || fun.Name == ast.AggFuncCount {
			return true
		}
	}
	return false
}

// checkAllArgsColumn checks whether the args in function are dedicated columns
// eg: count(*) or sum(a+1) will return false while count(a) or sum(a) will return true
func (*aggregationPushDownSolver) checkAllArgsColumn(fun *aggregation.AggFuncDesc) bool {
	for _, arg := range fun.Args {
		_, ok := arg.(*expression.Column)
		if !ok {
			return false
		}
	}
	return true
}

// TODO:
//  1. https://github.com/pingcap/tidb/issues/16355, push avg & distinct functions across join
//  2. remove this method and use splitPartialAgg instead for clean code.
func (a *aggregationPushDownSolver) makeNewAgg(ctx base.PlanContext, aggFuncs []*aggregation.AggFuncDesc,
	gbyCols []*expression.Column, preferAggType uint, preferAggToCop bool, blockOffset int, nullGenerating bool) (*LogicalAggregation, error) {
	agg := LogicalAggregation{
		GroupByItems:   expression.Column2Exprs(gbyCols),
		PreferAggType:  preferAggType,
		PreferAggToCop: preferAggToCop,
	}.Init(ctx, blockOffset)
	aggLen := len(aggFuncs) + len(gbyCols)
	newAggFuncDescs := make([]*aggregation.AggFuncDesc, 0, aggLen)
	schema := expression.NewSchema(make([]*expression.Column, 0, aggLen)...)
	for _, aggFunc := range aggFuncs {
		var newFuncs []*aggregation.AggFuncDesc
		newFuncs, schema = a.decompose(ctx, aggFunc, schema, nullGenerating)
		newAggFuncDescs = append(newAggFuncDescs, newFuncs...)
	}
	for _, gbyCol := range gbyCols {
		firstRow, err := aggregation.NewAggFuncDesc(agg.SCtx().GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{gbyCol}, false)
		if err != nil {
			return nil, err
		}
		newCol, _ := gbyCol.Clone().(*expression.Column)
		newCol.RetType = firstRow.RetTp
		newAggFuncDescs = append(newAggFuncDescs, firstRow)
		schema.Append(newCol)
	}
	agg.AggFuncs = newAggFuncDescs
	agg.SetSchema(schema)
	// TODO: Add a Projection if any argument of aggregate funcs or group by items are scalar functions.
	// agg.buildProjectionIfNecessary()
	return agg, nil
}

func (*aggregationPushDownSolver) splitPartialAgg(agg *LogicalAggregation) (pushedAgg *LogicalAggregation) {
	partial, final, _ := BuildFinalModeAggregation(agg.SCtx(), &AggInfo{
		AggFuncs:     agg.AggFuncs,
		GroupByItems: agg.GroupByItems,
		Schema:       agg.schema,
	}, false, false)
	if partial == nil {
		pushedAgg = nil
		return
	}
	agg.SetSchema(final.Schema)
	agg.AggFuncs = final.AggFuncs
	agg.GroupByItems = final.GroupByItems

	pushedAgg = LogicalAggregation{
		AggFuncs:       partial.AggFuncs,
		GroupByItems:   partial.GroupByItems,
		PreferAggType:  agg.PreferAggType,
		PreferAggToCop: agg.PreferAggToCop,
	}.Init(agg.SCtx(), agg.QueryBlockOffset())
	pushedAgg.SetSchema(partial.Schema)
	return
}

// pushAggCrossUnion will try to push the agg down to the union. If the new aggregation's group-by columns doesn't contain unique key.
// We will return the new aggregation. Otherwise we will transform the aggregation to projection.
func (*aggregationPushDownSolver) pushAggCrossUnion(agg *LogicalAggregation, unionSchema *expression.Schema, unionChild base.LogicalPlan) (base.LogicalPlan, error) {
	ctx := agg.SCtx()
	newAgg := LogicalAggregation{
		AggFuncs:       make([]*aggregation.AggFuncDesc, 0, len(agg.AggFuncs)),
		GroupByItems:   make([]expression.Expression, 0, len(agg.GroupByItems)),
		PreferAggType:  agg.PreferAggType,
		PreferAggToCop: agg.PreferAggToCop,
	}.Init(ctx, agg.QueryBlockOffset())
	newAgg.SetSchema(agg.schema.Clone())
	for _, aggFunc := range agg.AggFuncs {
		newAggFunc := aggFunc.Clone()
		newArgs := make([]expression.Expression, 0, len(newAggFunc.Args))
		for _, arg := range newAggFunc.Args {
			newArgs = append(newArgs, expression.ColumnSubstitute(ctx.GetExprCtx(), arg, unionSchema, expression.Column2Exprs(unionChild.Schema().Columns)))
		}
		newAggFunc.Args = newArgs
		newAgg.AggFuncs = append(newAgg.AggFuncs, newAggFunc)
	}
	for _, gbyExpr := range agg.GroupByItems {
		newExpr := expression.ColumnSubstitute(ctx.GetExprCtx(), gbyExpr, unionSchema, expression.Column2Exprs(unionChild.Schema().Columns))
		newAgg.GroupByItems = append(newAgg.GroupByItems, newExpr)
		// TODO: if there is a duplicated first_row function, we can delete it.
		firstRow, err := aggregation.NewAggFuncDesc(agg.SCtx().GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{gbyExpr}, false)
		if err != nil {
			return nil, err
		}
		// Update mode of new generated firstRow as other agg funcs.
		if len(agg.AggFuncs) != 0 {
			firstRow.Mode = agg.AggFuncs[0].Mode
		} else {
			firstRow.Mode = aggregation.Partial1Mode
		}
		newAgg.AggFuncs = append(newAgg.AggFuncs, firstRow)
	}
	tmpSchema := expression.NewSchema(newAgg.GetGroupByCols()...)
	// e.g. Union distinct will add a aggregation like `select join_agg_0, join_agg_1, join_agg_2 from t group by a, b, c` above UnionAll.
	// And the pushed agg will be something like `select a, b, c, a, b, c from t group by a, b, c`. So if we just return child as join does,
	// this will cause error during executor phase.
	for _, key := range unionChild.Schema().Keys {
		if tmpSchema.ColumnsIndices(key) != nil {
			if ok, proj := ConvertAggToProj(newAgg, newAgg.schema); ok {
				proj.SetChildren(unionChild)
				return proj, nil
			}
			break
		}
	}
	newAgg.SetChildren(unionChild)
	return newAgg, nil
}

func (a *aggregationPushDownSolver) optimize(_ context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	newLogicalPlan, err := a.aggPushDown(p, opt)
	return newLogicalPlan, planChanged, err
}

func (a *aggregationPushDownSolver) tryAggPushDownForUnion(union *LogicalUnionAll, agg *LogicalAggregation, opt *optimizetrace.LogicalOptimizeOp) error {
	for _, aggFunc := range agg.AggFuncs {
		if !a.isDecomposableWithUnion(aggFunc) {
			return nil
		}
	}
	pushedAgg := a.splitPartialAgg(agg)
	if pushedAgg == nil {
		return nil
	}

	// Update the agg mode for the pushed down aggregation.
	for _, aggFunc := range pushedAgg.AggFuncs {
		if aggFunc.Mode == aggregation.CompleteMode {
			aggFunc.Mode = aggregation.Partial1Mode
		} else if aggFunc.Mode == aggregation.FinalMode {
			aggFunc.Mode = aggregation.Partial2Mode
		}
	}

	newChildren := make([]base.LogicalPlan, 0, len(union.Children()))
	for _, child := range union.Children() {
		newChild, err := a.pushAggCrossUnion(pushedAgg, union.Schema(), child)
		if err != nil {
			return err
		}
		newChildren = append(newChildren, newChild)
	}
	union.SetSchema(expression.NewSchema(newChildren[0].Schema().Clone().Columns...))
	union.SetChildren(newChildren...)
	appendAggPushDownAcrossUnionTraceStep(union, agg, opt)
	return nil
}

// aggPushDown tries to push down aggregate functions to join paths.
func (a *aggregationPushDownSolver) aggPushDown(p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (_ base.LogicalPlan, err error) {
	if agg, ok := p.(*LogicalAggregation); ok {
		proj := a.tryToEliminateAggregation(agg, opt)
		if proj != nil {
			p = proj
		} else {
			child := agg.Children()[0]
			// For example, we can optimize 'select sum(a.id) from t as a,t as b where a.id = b.id;' as
			// 'select sum(agg) from (select sum(id) as agg,id from t group by id) as a, t as b where a.id = b.id;'
			// by pushing down sum aggregation functions.
			if join, ok1 := child.(*LogicalJoin); ok1 && a.checkValidJoin(join) && p.SCtx().GetSessionVars().AllowAggPushDown {
				if valid, leftAggFuncs, rightAggFuncs, leftGbyCols, rightGbyCols := a.splitAggFuncsAndGbyCols(agg, join); valid {
					var lChild, rChild base.LogicalPlan
					// If there exist count or sum functions in left join path, we can't push any
					// aggregate function into right join path.
					rightInvalid := a.checkAnyCountAndSum(leftAggFuncs)
					leftInvalid := a.checkAnyCountAndSum(rightAggFuncs)
					if rightInvalid {
						rChild = join.Children()[1]
					} else {
						rChild, err = a.tryToPushDownAgg(agg, rightAggFuncs, rightGbyCols, join, 1, agg.QueryBlockOffset(), opt)
						if err != nil {
							return nil, err
						}
					}
					if leftInvalid {
						lChild = join.Children()[0]
					} else {
						lChild, err = a.tryToPushDownAgg(agg, leftAggFuncs, leftGbyCols, join, 0, agg.QueryBlockOffset(), opt)
						if err != nil {
							return nil, err
						}
					}
					join.SetChildren(lChild, rChild)
					join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
					if join.JoinType == LeftOuterJoin {
						resetNotNullFlag(join.schema, lChild.Schema().Len(), join.schema.Len())
					} else if join.JoinType == RightOuterJoin {
						resetNotNullFlag(join.schema, 0, lChild.Schema().Len())
					}
					buildKeyInfo(join)
					// count(a) -> ifnull(col#x, 0, 1) in rewriteExpr of agg function, since col#x is already the final
					// pushed-down aggregation's result, we don't need to take every row as count 1 when they don't have
					// not-null flag in a.tryToEliminateAggregation(oldAgg, opt), which is not suitable here.
					oldCheck := a.oldAggEliminationCheck
					a.oldAggEliminationCheck = true
					proj := a.tryToEliminateAggregation(agg, opt)
					if proj != nil {
						p = proj
					}
					a.oldAggEliminationCheck = oldCheck

					// Combine the aggregation elimination logic below since new agg's child key info has changed.
					// Notice that even if we eliminate new agg below if possible, the agg's schema is inherited by proj.
					// Therefore, we don't need to set the join's schema again, just build the keyInfo again.
					changed := false
					if newAgg, ok1 := lChild.(*LogicalAggregation); ok1 {
						proj := a.tryToEliminateAggregation(newAgg, opt)
						if proj != nil {
							lChild = proj
							changed = true
						}
					}
					if newAgg, ok2 := rChild.(*LogicalAggregation); ok2 {
						proj := a.tryToEliminateAggregation(newAgg, opt)
						if proj != nil {
							rChild = proj
							changed = true
						}
					}
					if changed {
						join.SetChildren(lChild, rChild)
						buildKeyInfo(join)
					}
				}
			} else if proj, ok1 := child.(*LogicalProjection); ok1 {
				// push aggregation across projection
				// TODO: This optimization is not always reasonable. We have not supported pushing projection to kv layer yet,
				// so we must do this optimization.
				ctx := p.SCtx()
				noSideEffects := true
				newGbyItems := make([]expression.Expression, 0, len(agg.GroupByItems))
				for _, gbyItem := range agg.GroupByItems {
					_, failed, groupBy := expression.ColumnSubstituteImpl(ctx.GetExprCtx(), gbyItem, proj.schema, proj.Exprs, true)
					if failed {
						noSideEffects = false
						break
					}
					newGbyItems = append(newGbyItems, groupBy)
					if ExprsHasSideEffects(newGbyItems) {
						noSideEffects = false
						break
					}
				}
				oldAggFuncsArgs := make([][]expression.Expression, 0, len(agg.AggFuncs))
				newAggFuncsArgs := make([][]expression.Expression, 0, len(agg.AggFuncs))
				oldAggOrderItems := make([][]*util.ByItems, 0, len(agg.AggFuncs))
				newAggOrderItems := make([][]*util.ByItems, 0, len(agg.AggFuncs))
				if noSideEffects {
					for _, aggFunc := range agg.AggFuncs {
						oldAggFuncsArgs = append(oldAggFuncsArgs, aggFunc.Args)
						newArgs := make([]expression.Expression, 0, len(aggFunc.Args))
						for _, arg := range aggFunc.Args {
							_, failed, newArg := expression.ColumnSubstituteImpl(ctx.GetExprCtx(), arg, proj.schema, proj.Exprs, true)
							if failed {
								noSideEffects = false
								break
							}
							newArgs = append(newArgs, newArg)
						}
						if ExprsHasSideEffects(newArgs) {
							noSideEffects = false
							break
						}
						newAggFuncsArgs = append(newAggFuncsArgs, newArgs)
						// for ordeByItems, treat it like agg func's args, if it can be substituted by underlying projection's expression recording them temporarily.
						if len(aggFunc.OrderByItems) != 0 {
							oldAggOrderItems = append(oldAggOrderItems, aggFunc.OrderByItems)
							newOrderByItems := make([]expression.Expression, 0, len(aggFunc.OrderByItems))
							for _, oby := range aggFunc.OrderByItems {
								_, failed, byItem := expression.ColumnSubstituteImpl(ctx.GetExprCtx(), oby.Expr, proj.schema, proj.Exprs, true)
								if failed {
									noSideEffects = false
									break
								}
								newOrderByItems = append(newOrderByItems, byItem)
							}
							if ExprsHasSideEffects(newOrderByItems) {
								noSideEffects = false
								break
							}
							oneAggOrderByItems := make([]*util.ByItems, 0, len(aggFunc.OrderByItems))
							for i, obyExpr := range newOrderByItems {
								oneAggOrderByItems = append(oneAggOrderByItems, &util.ByItems{Expr: obyExpr, Desc: aggFunc.OrderByItems[i].Desc})
							}
							newAggOrderItems = append(newAggOrderItems, oneAggOrderByItems)
						} else {
							// occupy the pos for convenience of subscript index
							oldAggOrderItems = append(oldAggOrderItems, nil)
							newAggOrderItems = append(newAggOrderItems, nil)
						}
					}
				}
				ectx := p.SCtx().GetExprCtx().GetEvalCtx()
				for i, funcsArgs := range oldAggFuncsArgs {
					if !noSideEffects {
						break
					}
					for j := range funcsArgs {
						if oldAggFuncsArgs[i][j].GetType(ectx).EvalType() != newAggFuncsArgs[i][j].GetType(ectx).EvalType() {
							noSideEffects = false
							break
						}
					}
					for j, item := range newAggOrderItems[i] {
						if item == nil {
							continue
						}
						// substitution happened, check the eval type compatibility.
						if oldAggOrderItems[i][j].Expr.GetType(ectx).EvalType() != newAggOrderItems[i][j].Expr.GetType(ectx).EvalType() {
							noSideEffects = false
							break
						}
					}
				}
				if noSideEffects {
					agg.GroupByItems = newGbyItems
					for i, aggFunc := range agg.AggFuncs {
						aggFunc.Args = newAggFuncsArgs[i]
						aggFunc.OrderByItems = newAggOrderItems[i]
					}
					projChild := proj.Children()[0]
					agg.SetChildren(projChild)
					// When the origin plan tree is `Aggregation->Projection->Union All->X`, we need to merge 'Aggregation' and 'Projection' first.
					// And then push the new 'Aggregation' below the 'Union All' .
					// The final plan tree should be 'Aggregation->Union All->Aggregation->X'.
					child = projChild
					appendAggPushDownAcrossProjTraceStep(agg, proj, opt)
				}
			}
			if union, ok1 := child.(*LogicalUnionAll); ok1 && p.SCtx().GetSessionVars().AllowAggPushDown {
				err := a.tryAggPushDownForUnion(union, agg, opt)
				if err != nil {
					return nil, err
				}
			} else if union, ok1 := child.(*LogicalPartitionUnionAll); ok1 {
				err := a.tryAggPushDownForUnion(&union.LogicalUnionAll, agg, opt)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	newChildren := make([]base.LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := a.aggPushDown(child, opt)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	return p, nil
}

func (*aggregationPushDownSolver) name() string {
	return "aggregation_push_down"
}

func appendAggPushDownAcrossJoinTraceStep(oldAgg, newAgg *LogicalAggregation, aggFuncs []*aggregation.AggFuncDesc, join *LogicalJoin,
	childIdx int, opt *optimizetrace.LogicalOptimizeOp) {
	reason := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v's functions[", oldAgg.TP(), oldAgg.ID()))
		for i, aggFunc := range aggFuncs {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(aggFunc.String())
		}
		buffer.WriteString("] are decomposable with join")
		return buffer.String()
	}
	action := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v pushed down across %v_%v, ", oldAgg.TP(), oldAgg.ID(), join.TP(), join.ID()))
		fmt.Fprintf(buffer, "and %v_%v %v path becomes %v_%v", join.TP(), join.ID(), func() string {
			if childIdx == 0 {
				return "left"
			}
			return "right"
		}(), newAgg.TP(), newAgg.ID())
		return buffer.String()
	}
	opt.AppendStepToCurrent(join.ID(), join.TP(), reason, action)
}

func appendAggPushDownAcrossProjTraceStep(agg *LogicalAggregation, proj *LogicalProjection, opt *optimizetrace.LogicalOptimizeOp) {
	action := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v is eliminated, and %v_%v's functions changed into[", proj.TP(), proj.ID(), agg.TP(), agg.ID()))
		for i, aggFunc := range agg.AggFuncs {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(aggFunc.String())
		}
		buffer.WriteString("]")
		return buffer.String()
	}
	reason := func() string {
		return fmt.Sprintf("%v_%v is directly below an %v_%v and has no side effects", proj.TP(), proj.ID(), agg.TP(), agg.ID())
	}
	opt.AppendStepToCurrent(agg.ID(), agg.TP(), reason, action)
}

func appendAggPushDownAcrossUnionTraceStep(union *LogicalUnionAll, agg *LogicalAggregation, opt *optimizetrace.LogicalOptimizeOp) {
	reason := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v functions[", agg.TP(), agg.ID()))
		for i, aggFunc := range agg.AggFuncs {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(aggFunc.String())
		}
		fmt.Fprintf(buffer, "] are decomposable with %v_%v", union.TP(), union.ID())
		return buffer.String()
	}
	action := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v pushed down, and %v_%v's children changed into[", agg.TP(), agg.ID(), union.TP(), union.ID()))
		for i, child := range union.Children() {
			if i > 0 {
				buffer.WriteString(",")
			}
			fmt.Fprintf(buffer, "%v_%v", child.TP(), child.ID())
		}
		buffer.WriteString("]")
		return buffer.String()
	}
	opt.AppendStepToCurrent(union.ID(), union.TP(), reason, action)
}
