// Copyright 2018 PingCAP, Inc.
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
	"context"
	"fmt"
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/types"
)

// AggregationEliminator is used to eliminate aggregation grouped by unique key.
type AggregationEliminator struct {
	aggregationEliminateChecker
}

type aggregationEliminateChecker struct {
	// used for agg pushed down cases, for example:
	// agg -> join -> datasource1
	//             -> datasource2
	// we just make a new agg upon datasource1 or datasource2, while the old agg is still existed and waiting for elimination.
	// Note when the old agg is like below, and join is an outer join type, rewriting old agg in elimination logic has some problem.
	// eg:
	// count(a) -> ifnull(col#x, 0, 1) in rewriteExpr of agg function, since col#x is already the final pushed-down aggregation's
	// result from new join schema, we don't need to take every row as count 1 when they don't have not-null flag in a.tryToEliminateAggregation(oldAgg, opt),
	// which is not suitable here.
	oldAggEliminationCheck bool
}

// isKeyFullyCoveredByJoinColumns checks if all columns in key are uniquely covered by joinColumnPairs on the given side (0: left, 1: right)
func isKeyFullyCoveredByJoinColumns(key expression.KeyInfo, joinColumnPairs [][2]*expression.Column, side int) bool {
	if len(joinColumnPairs) != len(key) {
		return false
	}
	matched := make(map[int]bool)
	for i, keyCol := range key {
		found := false
		for _, pair := range joinColumnPairs {
			if pair[side].Equal(nil, keyCol) {
				if matched[i] {
					return false
				}
				matched[i] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return len(matched) == len(key)
}

func hasJoinEqConditionCoveringUK(keys []expression.KeyInfo, joinPairs [][2]*expression.Column, side int) bool {
	for _, key := range keys {
		if isKeyFullyCoveredByJoinColumns(key, joinPairs, side) {
			return true
		}
	}
	return false
}

// collectDataSourceUniqueKeys recursively traverses the logical plan tree to find all DataSource nodes
// and returns their primary key or unique key information (PKOrUK).
func collectDataSourceUniqueKeys(p base.LogicalPlan) [][]expression.KeyInfo {
	if ds, ok := p.(*logicalop.DataSource); ok {
		if keys := ds.Schema().PKOrUK; keys != nil {
			return [][]expression.KeyInfo{keys}
		}
		return nil
	}
	allKeys := make([][]expression.KeyInfo, 0, len(p.Children()))
	for _, child := range p.Children() {
		if childKeys := collectDataSourceUniqueKeys(child); childKeys != nil {
			allKeys = append(allKeys, childKeys...)
		}
	}
	return allKeys
}

// checkJoinChildUniqueKeyCoverage checks if a child of a join satisfies the unique key coverage condition.
func checkJoinChildUniqueKeyCoverage(child base.LogicalPlan, joinPairs [][2]*expression.Column, side int) bool {
	switch child.(type) {
	case *logicalop.DataSource, *logicalop.LogicalProjection:
		// For DataSource or Projection, we trace back to the underlying DataSources to find unique keys.
		allUniqueKeys := collectDataSourceUniqueKeys(child)
		if len(allUniqueKeys) == 0 {
			return false
		}
		isCovered := false
		for _, keys := range allUniqueKeys {
			if hasJoinEqConditionCoveringUK(keys, joinPairs, side) {
				isCovered = true
				break
			}
		}
		if !isCovered {
			return false
		}
	case *logicalop.LogicalJoin:
		// The uniqueness of a LogicalJoin child is checked by the recursive call to checkAllJoinsUniqueByEqCondition,
		// so we don't need to check its keys here. It is considered valid.
	default:
		// Any other operator type means the uniqueness is not guaranteed.
		return false
	}
	return true
}

// checkAllJoinsUniqueByEqCondition recursively checks from the bottom up whether all joins in the plan
// have their PKOrUK fully covered by join equal conditions.
// Returns false immediately if any join does not satisfy the uniqueness condition.
func checkAllJoinsUniqueByEqCondition(p base.LogicalPlan) bool {
	for _, child := range p.Children() {
		if _, ok := child.(*logicalop.LogicalJoin); ok {
			if !checkAllJoinsUniqueByEqCondition(child) {
				return false
			}
		}
	}

	join, ok := p.(*logicalop.LogicalJoin)
	if !ok {
		return false
	}

	if len(join.EqualConditions) == 0 {
		return false
	}
	joinPairs := make([][2]*expression.Column, 0, len(join.EqualConditions))
	for _, cond := range join.EqualConditions {
		if cond.FuncName.L != ast.EQ {
			return false
		}
		col1, ok1 := cond.GetArgs()[0].(*expression.Column)
		col2, ok2 := cond.GetArgs()[1].(*expression.Column)
		if !ok1 || !ok2 {
			return false
		}
		joinPairs = append(joinPairs, [2]*expression.Column{col1, col2})
	}

	if !checkJoinChildUniqueKeyCoverage(join.Children()[0], joinPairs, 0) {
		return false
	}
	if !checkJoinChildUniqueKeyCoverage(join.Children()[1], joinPairs, 1) {
		return false
	}
	return true
}

// tryToEliminateAggregation will eliminate aggregation grouped by unique key.
// e.g. select min(b) from t group by a. If a is a unique key, then this sql is equal to `select b from t group by a`.
// For count(expr), sum(expr), avg(expr), count(distinct expr, [expr...]) we may need to rewrite the expr. Details are shown below.
// If we can eliminate agg successful, we return a projection. Else we return a nil pointer.
func (a *aggregationEliminateChecker) tryToEliminateAggregation(agg *logicalop.LogicalAggregation, opt *optimizetrace.LogicalOptimizeOp) *logicalop.LogicalProjection {
	for _, af := range agg.AggFuncs {
		// TODO(issue #9968): Actually, we can rewrite GROUP_CONCAT when all the
		// arguments it accepts are promised to be NOT-NULL.
		// When it accepts only 1 argument, we can extract this argument into a
		// projection.
		// When it accepts multiple arguments, we can wrap the arguments with a
		// function CONCAT_WS and extract this function into a projection.
		// BUT, GROUP_CONCAT should truncate the final result according to the
		// system variable `group_concat_max_len`. To ensure the correctness of
		// the result, we close the elimination of GROUP_CONCAT here.
		if af.Name == ast.AggFuncGroupConcat {
			return nil
		}
	}
	schemaByGroupby := expression.NewSchema(agg.GetGroupByCols()...)
	var uniqueKey expression.KeyInfo
	coveredByUniqueKey := false
	for _, key := range agg.Children()[0].Schema().PKOrUK {
		if schemaByGroupby.ColumnsIndices(key) != nil {
			coveredByUniqueKey = true
			uniqueKey = key
			break
		}
	}
	allowEliminateAgg4Join := fixcontrol.GetBoolWithDefault(agg.SCtx().GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix61556, false)
	// Handle the multi-table join case: check if a unique key from a base table is covered and preserved through all joins.
	if !coveredByUniqueKey && allowEliminateAgg4Join {
		// The GROUP BY columns may cover a unique key from a base table.
		allDataSourceKeys := collectDataSourceUniqueKeys(agg.Children()[0])
		for _, keys := range allDataSourceKeys {
			for _, key := range keys {
				if schemaByGroupby.ColumnsIndices(key) != nil {
					coveredByUniqueKey = true
					uniqueKey = key
					break
				}
			}
			if coveredByUniqueKey {
				break
			}
		}

		if coveredByUniqueKey {
			// We have found a unique key from a data source that is covered by the GROUP BY columns.
			// Then, we need to check if the uniqueness is preserved by all intermediate join operations.
			if !checkAllJoinsUniqueByEqCondition(agg.Children()[0]) {
				coveredByUniqueKey = false
			}
		}
	}

	if coveredByUniqueKey {
		if a.oldAggEliminationCheck && !CheckCanConvertAggToProj(agg) {
			return nil
		}
		// GroupByCols has unique key, so this aggregation can be removed.
		if ok, proj := ConvertAggToProj(agg, agg.Schema()); ok {
			proj.SetChildren(agg.Children()[0])
			appendAggregationEliminateTraceStep(agg, proj, uniqueKey, opt)
			return proj
		}
	}
	return nil
}

// tryToEliminateDistinct will eliminate distinct in the aggregation function if the aggregation args
// have unique key column. see detail example in https://github.com/pingcap/tidb/issues/23436
func (*aggregationEliminateChecker) tryToEliminateDistinct(agg *logicalop.LogicalAggregation, opt *optimizetrace.LogicalOptimizeOp) {
	for _, af := range agg.AggFuncs {
		if af.HasDistinct {
			cols := make([]*expression.Column, 0, len(af.Args))
			canEliminate := true
			for _, arg := range af.Args {
				col, ok := arg.(*expression.Column)
				if !ok {
					canEliminate = false
					break
				}
				cols = append(cols, col)
			}
			if canEliminate {
				distinctByUniqueKey := false
				schemaByDistinct := expression.NewSchema(cols...)
				var uniqueKey expression.KeyInfo
				for _, key := range agg.Children()[0].Schema().PKOrUK {
					if schemaByDistinct.ColumnsIndices(key) != nil {
						distinctByUniqueKey = true
						uniqueKey = key
						break
					}
				}
				for _, key := range agg.Children()[0].Schema().NullableUK {
					if schemaByDistinct.ColumnsIndices(key) != nil {
						distinctByUniqueKey = true
						uniqueKey = key
						break
					}
				}
				if distinctByUniqueKey {
					af.HasDistinct = false
					appendDistinctEliminateTraceStep(agg, uniqueKey, af, opt)
				}
			}
		}
	}
}

func appendAggregationEliminateTraceStep(agg *logicalop.LogicalAggregation, proj *logicalop.LogicalProjection, uniqueKey expression.KeyInfo, opt *optimizetrace.LogicalOptimizeOp) {
	reason := func() string {
		return fmt.Sprintf("%s is a unique key", uniqueKey.String())
	}
	action := func() string {
		return fmt.Sprintf("%v_%v is simplified to a %v_%v", agg.TP(), agg.ID(), proj.TP(), proj.ID())
	}

	opt.AppendStepToCurrent(agg.ID(), agg.TP(), reason, action)
}

func appendDistinctEliminateTraceStep(agg *logicalop.LogicalAggregation, uniqueKey expression.KeyInfo, af *aggregation.AggFuncDesc,
	opt *optimizetrace.LogicalOptimizeOp) {
	reason := func() string {
		return fmt.Sprintf("%s is a unique key", uniqueKey.String())
	}
	action := func() string {
		return fmt.Sprintf("%s(distinct ...) is simplified to %s(...)", af.Name, af.Name)
	}
	opt.AppendStepToCurrent(agg.ID(), agg.TP(), reason, action)
}

// CheckCanConvertAggToProj check whether a special old aggregation (which has already been pushed down) to projection.
// link: issue#44795
func CheckCanConvertAggToProj(agg *logicalop.LogicalAggregation) bool {
	var mayNullSchema *expression.Schema
	if join, ok := agg.Children()[0].(*logicalop.LogicalJoin); ok {
		if join.JoinType == logicalop.LeftOuterJoin {
			mayNullSchema = join.Children()[1].Schema()
		}
		if join.JoinType == logicalop.RightOuterJoin {
			mayNullSchema = join.Children()[0].Schema()
		}
		if mayNullSchema == nil {
			return true
		}
		// once agg function args has intersection with mayNullSchema, return nil (means elimination fail)
		for _, fun := range agg.AggFuncs {
			mayNullCols := expression.ExtractColumnsFromExpressions(nil, fun.Args, func(column *expression.Column) bool {
				// collect may-null cols.
				return mayNullSchema.Contains(column)
			})
			if len(mayNullCols) != 0 {
				return false
			}
		}
	}
	return true
}

// ConvertAggToProj convert aggregation to projection.
func ConvertAggToProj(agg *logicalop.LogicalAggregation, schema *expression.Schema) (bool, *logicalop.LogicalProjection) {
	proj := logicalop.LogicalProjection{
		Exprs: make([]expression.Expression, 0, len(agg.AggFuncs)),
	}.Init(agg.SCtx(), agg.QueryBlockOffset())
	for _, fun := range agg.AggFuncs {
		ok, expr := rewriteExpr(agg.SCtx().GetExprCtx(), fun)
		if !ok {
			return false, nil
		}
		proj.Exprs = append(proj.Exprs, expr)
	}
	proj.SetSchema(schema.Clone())
	return true, proj
}

// rewriteExpr will rewrite the aggregate function to expression doesn't contain aggregate function.
func rewriteExpr(ctx expression.BuildContext, aggFunc *aggregation.AggFuncDesc) (bool, expression.Expression) {
	switch aggFunc.Name {
	case ast.AggFuncCount:
		if aggFunc.Mode == aggregation.FinalMode &&
			len(aggFunc.Args) == 1 &&
			mysql.HasNotNullFlag(aggFunc.Args[0].GetType(ctx.GetEvalCtx()).GetFlag()) {
			return true, wrapCastFunction(ctx, aggFunc.Args[0], aggFunc.RetTp)
		}
		return true, rewriteCount(ctx, aggFunc.Args, aggFunc.RetTp)
	case ast.AggFuncSum, ast.AggFuncAvg, ast.AggFuncFirstRow, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncGroupConcat:
		return true, wrapCastFunction(ctx, aggFunc.Args[0], aggFunc.RetTp)
	case ast.AggFuncBitAnd, ast.AggFuncBitOr, ast.AggFuncBitXor:
		return true, rewriteBitFunc(ctx, aggFunc.Name, aggFunc.Args[0], aggFunc.RetTp)
	default:
		return false, nil
	}
}

func rewriteCount(ctx expression.BuildContext, exprs []expression.Expression, targetTp *types.FieldType) expression.Expression {
	// If is count(expr), we will change it to if(isnull(expr), 0, 1).
	// If is count(distinct x, y, z), we will change it to if(isnull(x) or isnull(y) or isnull(z), 0, 1).
	// If is count(expr not null), we will change it to constant 1.
	isNullExprs := make([]expression.Expression, 0, len(exprs))
	for _, expr := range exprs {
		if mysql.HasNotNullFlag(expr.GetType(ctx.GetEvalCtx()).GetFlag()) {
			isNullExprs = append(isNullExprs, expression.NewZero())
		} else {
			isNullExpr := expression.NewFunctionInternal(ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), expr)
			isNullExprs = append(isNullExprs, isNullExpr)
		}
	}

	innerExpr := expression.ComposeDNFCondition(ctx, isNullExprs...)
	newExpr := expression.NewFunctionInternal(ctx, ast.If, targetTp, innerExpr, expression.NewZero(), expression.NewOne())
	return newExpr
}

func rewriteBitFunc(ctx expression.BuildContext, funcType string, arg expression.Expression, targetTp *types.FieldType) expression.Expression {
	// For not integer type. We need to cast(cast(arg as signed) as unsigned) to make the bit function work.
	innerCast := expression.WrapWithCastAsInt(ctx, arg, nil)
	outerCast := wrapCastFunction(ctx, innerCast, targetTp)
	var finalExpr expression.Expression
	if funcType != ast.AggFuncBitAnd {
		finalExpr = expression.NewFunctionInternal(ctx, ast.Ifnull, targetTp, outerCast, expression.NewZero())
	} else {
		finalExpr = expression.NewFunctionInternal(ctx, ast.Ifnull, outerCast.GetType(ctx.GetEvalCtx()), outerCast, &expression.Constant{Value: types.NewUintDatum(math.MaxUint64), RetType: targetTp})
	}
	return finalExpr
}

// wrapCastFunction will wrap a cast if the targetTp is not equal to the arg's.
func wrapCastFunction(ctx expression.BuildContext, arg expression.Expression, targetTp *types.FieldType) expression.Expression {
	if arg.GetType(ctx.GetEvalCtx()).Equal(targetTp) {
		return arg
	}
	return expression.BuildCastFunction(ctx, arg, targetTp)
}

// Optimize implements the base.LogicalOptRule.<0th> interface.
func (a *AggregationEliminator) Optimize(ctx context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	newChildren := make([]base.LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, planChanged, err := a.Optimize(ctx, child, opt)
		if err != nil {
			return nil, planChanged, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	agg, ok := p.(*logicalop.LogicalAggregation)
	if !ok {
		return p, planChanged, nil
	}
	a.tryToEliminateDistinct(agg, opt)
	if proj := a.tryToEliminateAggregation(agg, opt); proj != nil {
		return proj, planChanged, nil
	}
	return p, planChanged, nil
}

// Name implements the base.LogicalOptRule.<1st> interface.
func (*AggregationEliminator) Name() string {
	return "aggregation_eliminate"
}
