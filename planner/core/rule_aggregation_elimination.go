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

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

type aggregationEliminator struct {
	aggregationEliminateChecker
}

type aggregationEliminateChecker struct {
}

// tryToEliminateAggregation will eliminate aggregation grouped by unique key.
// e.g. select min(b) from t group by a. If a is a unique key, then this sql is equal to `select b from t group by a`.
// For count(expr), sum(expr), avg(expr), count(distinct expr, [expr...]) we may need to rewrite the expr. Details are shown below.
// If we can eliminate agg successful, we return a projection. Else we return a nil pointer.
func (a *aggregationEliminateChecker) tryToEliminateAggregation(agg *LogicalAggregation, opt *logicalOptimizeOp) *LogicalProjection {
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
	coveredByUniqueKey := false
	var uniqueKey expression.KeyInfo
	for _, key := range agg.children[0].Schema().Keys {
		if schemaByGroupby.ColumnsIndices(key) != nil {
			coveredByUniqueKey = true
			uniqueKey = key
			break
		}
	}
	if coveredByUniqueKey {
		// GroupByCols has unique key, so this aggregation can be removed.
		if ok, proj := ConvertAggToProj(agg, agg.schema); ok {
			proj.SetChildren(agg.children[0])
			appendAggregationEliminateTraceStep(agg, proj, uniqueKey, opt)
			return proj
		}
	}
	return nil
}

// tryToEliminateDistinct will eliminate distinct in the aggregation function if the aggregation args
// have unique key column. see detail example in https://github.com/pingcap/tidb/issues/23436
func (a *aggregationEliminateChecker) tryToEliminateDistinct(agg *LogicalAggregation, opt *logicalOptimizeOp) {
	for _, af := range agg.AggFuncs {
		if af.HasDistinct {
			cols := make([]*expression.Column, 0, len(af.Args))
			canEliminate := true
			for _, arg := range af.Args {
				if col, ok := arg.(*expression.Column); ok {
					cols = append(cols, col)
				} else {
					canEliminate = false
					break
				}
			}
			if canEliminate {
				distinctByUniqueKey := false
				schemaByDistinct := expression.NewSchema(cols...)
				var uniqueKey expression.KeyInfo
				for _, key := range agg.children[0].Schema().Keys {
					if schemaByDistinct.ColumnsIndices(key) != nil {
						distinctByUniqueKey = true
						uniqueKey = key
						break
					}
				}
				for _, key := range agg.children[0].Schema().UniqueKeys {
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

func appendAggregationEliminateTraceStep(agg *LogicalAggregation, proj *LogicalProjection, uniqueKey expression.KeyInfo, opt *logicalOptimizeOp) {
	reason := func() string {
		return fmt.Sprintf("%s is a unique key", uniqueKey.String())
	}
	action := func() string {
		return fmt.Sprintf("%v_%v is simplified to a %v_%v", agg.TP(), agg.ID(), proj.TP(), proj.ID())
	}

	opt.appendStepToCurrent(agg.ID(), agg.TP(), reason, action)
}

func appendDistinctEliminateTraceStep(agg *LogicalAggregation, uniqueKey expression.KeyInfo, af *aggregation.AggFuncDesc,
	opt *logicalOptimizeOp) {
	reason := func() string {
		return fmt.Sprintf("%s is a unique key", uniqueKey.String())
	}
	action := func() string {
		return fmt.Sprintf("%s(distinct ...) is simplified to %s(...)", af.Name, af.Name)
	}
	opt.appendStepToCurrent(agg.ID(), agg.TP(), reason, action)
}

// ConvertAggToProj convert aggregation to projection.
func ConvertAggToProj(agg *LogicalAggregation, schema *expression.Schema) (bool, *LogicalProjection) {
	proj := LogicalProjection{
		Exprs: make([]expression.Expression, 0, len(agg.AggFuncs)),
	}.Init(agg.ctx, agg.blockOffset)
	for _, fun := range agg.AggFuncs {
		ok, expr := rewriteExpr(agg.ctx, fun)
		if !ok {
			return false, nil
		}
		proj.Exprs = append(proj.Exprs, expr)
	}
	proj.SetSchema(schema.Clone())
	return true, proj
}

// rewriteExpr will rewrite the aggregate function to expression doesn't contain aggregate function.
func rewriteExpr(ctx sessionctx.Context, aggFunc *aggregation.AggFuncDesc) (bool, expression.Expression) {
	switch aggFunc.Name {
	case ast.AggFuncCount:
		if aggFunc.Mode == aggregation.FinalMode &&
			len(aggFunc.Args) == 1 &&
			mysql.HasNotNullFlag(aggFunc.Args[0].GetType().GetFlag()) {
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

func rewriteCount(ctx sessionctx.Context, exprs []expression.Expression, targetTp *types.FieldType) expression.Expression {
	// If is count(expr), we will change it to if(isnull(expr), 0, 1).
	// If is count(distinct x, y, z), we will change it to if(isnull(x) or isnull(y) or isnull(z), 0, 1).
	// If is count(expr not null), we will change it to constant 1.
	isNullExprs := make([]expression.Expression, 0, len(exprs))
	for _, expr := range exprs {
		if mysql.HasNotNullFlag(expr.GetType().GetFlag()) {
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

func rewriteBitFunc(ctx sessionctx.Context, funcType string, arg expression.Expression, targetTp *types.FieldType) expression.Expression {
	// For not integer type. We need to cast(cast(arg as signed) as unsigned) to make the bit function work.
	innerCast := expression.WrapWithCastAsInt(ctx, arg)
	outerCast := wrapCastFunction(ctx, innerCast, targetTp)
	var finalExpr expression.Expression
	if funcType != ast.AggFuncBitAnd {
		finalExpr = expression.NewFunctionInternal(ctx, ast.Ifnull, targetTp, outerCast, expression.NewZero())
	} else {
		finalExpr = expression.NewFunctionInternal(ctx, ast.Ifnull, outerCast.GetType(), outerCast, &expression.Constant{Value: types.NewUintDatum(math.MaxUint64), RetType: targetTp})
	}
	return finalExpr
}

// wrapCastFunction will wrap a cast if the targetTp is not equal to the arg's.
func wrapCastFunction(ctx sessionctx.Context, arg expression.Expression, targetTp *types.FieldType) expression.Expression {
	if arg.GetType().Equal(targetTp) {
		return arg
	}
	return expression.BuildCastFunction(ctx, arg, targetTp)
}

func (a *aggregationEliminator) optimize(ctx context.Context, p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := a.optimize(ctx, child, opt)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	agg, ok := p.(*LogicalAggregation)
	if !ok {
		return p, nil
	}
	a.tryToEliminateDistinct(agg, opt)
	if proj := a.tryToEliminateAggregation(agg, opt); proj != nil {
		return proj, nil
	}
	return p, nil
}

func (*aggregationEliminator) name() string {
	return "aggregation_eliminate"
}
