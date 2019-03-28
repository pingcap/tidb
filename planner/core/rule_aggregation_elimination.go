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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

type aggregationEliminator struct {
}

func (a *aggregationEliminator) canEliminate(agg *LogicalAggregation) (ok bool) {
	schemaByGroupby := expression.NewSchema(agg.groupByCols...)
	for _, key := range agg.children[0].Schema().Keys {
		if schemaByGroupby.ColumnsIndices(key) != nil {
			ok = true
			break
		}
	}
	if !ok {
		return false
	}
	for _, af := range agg.AggFuncs {
		if af.Name == ast.AggFuncGroupConcat && len(af.Args) > 1 {
			return false
		}
	}
	return true
}

// tryEliminateAgg will eliminate aggregation if `groupByCols` contains at lease
// one unique key. Else, it will do nothing.
// e.g. select min(b) from `t` group by `non-unique-col`, `unique-col`.
// Suppose `unique-col` is an unique key, this sql is equal to `select b from t
// group by a`.
// For count(expr), sum(expr), avg(expr), count(distinct expr, [expr...]) we may
// need to rewrite the expr. Details are shown below.
// If we can eliminate agg successfully, we return a projection. Else we return
// a nil pointer.
func (a *aggregationEliminator) tryEliminateAgg(agg *LogicalAggregation) (LogicalPlan, error) {
	if !a.canEliminate(agg) {
		return nil, nil
	}
	proj, err := a.convertAggToProj(agg)
	if err != nil {
		return nil, err
	}
	proj.SetChildren(agg.children[0])
	return proj, nil
}

func (a *aggregationEliminator) convertAggToProj(agg *LogicalAggregation) (*LogicalProjection, error) {
	proj := LogicalProjection{
		Exprs: make([]expression.Expression, 0, len(agg.AggFuncs)),
	}.Init(agg.ctx)
	for _, fun := range agg.AggFuncs {
		expr, err := a.rewriteExpr(agg.ctx, fun)
		if err != nil {
			return nil, err
		}
		proj.Exprs = append(proj.Exprs, expr)
	}
	proj.SetSchema(agg.schema.Clone())
	return proj, nil
}

// rewriteExpr will rewrite the aggregate function to expression doesn't contain aggregate function.
func (a *aggregationEliminator) rewriteExpr(ctx sessionctx.Context, aggFunc *aggregation.AggFuncDesc) (expr expression.Expression, err error) {
	switch aggFunc.Name {
	case ast.AggFuncCount:
		if aggFunc.Mode == aggregation.FinalMode {
			// The input value is promised to be int type if it's FinalMode.
			expr = aggFunc.Args[0]
		} else {
			expr = a.rewriteCount(ctx, aggFunc.Args, aggFunc.RetTp)
		}
	case ast.AggFuncSum, ast.AggFuncAvg, ast.AggFuncFirstRow, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncGroupConcat:
		expr = a.wrapCastFunction(ctx, aggFunc.Args[0], aggFunc.RetTp)
	case ast.AggFuncBitAnd, ast.AggFuncBitOr, ast.AggFuncBitXor:
		expr = a.rewriteBitFunc(ctx, aggFunc.Name, aggFunc.Args[0], aggFunc.RetTp)
	default:
		err = errors.Errorf("unsupported function when doing aggEliminate")
	}
	return
}

func (a *aggregationEliminator) rewriteCount(ctx sessionctx.Context, exprs []expression.Expression, targetTp *types.FieldType) expression.Expression {
	// If is count(expr), we will change it to if(isnull(expr), 0, 1).
	// If is count(distinct x, y, z) we will change it to if(isnull(x) or isnull(y) or isnull(z), 0, 1).
	// If is count(expr not null), we will change it to constant 1.
	isNullExprs := make([]expression.Expression, 0, len(exprs))
	for _, expr := range exprs {
		if mysql.HasNotNullFlag(expr.GetType().Flag) {
			isNullExprs = append(isNullExprs, expression.Zero)
		} else {
			isNullExpr := expression.NewFunctionInternal(ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), expr)
			isNullExprs = append(isNullExprs, isNullExpr)
		}
	}

	innerExpr := expression.ComposeDNFCondition(ctx, isNullExprs...)
	newExpr := expression.NewFunctionInternal(ctx, ast.If, targetTp, innerExpr, expression.Zero, expression.One)
	return newExpr
}

func (a *aggregationEliminator) rewriteBitFunc(ctx sessionctx.Context, funcType string, arg expression.Expression, targetTp *types.FieldType) expression.Expression {
	// For not integer type. We need to cast(cast(arg as signed) as unsigned) to make the bit function work.
	innerCast := expression.WrapWithCastAsInt(ctx, arg)
	outerCast := a.wrapCastFunction(ctx, innerCast, targetTp)
	var finalExpr expression.Expression
	if funcType != ast.AggFuncBitAnd {
		finalExpr = expression.NewFunctionInternal(ctx, ast.Ifnull, targetTp, outerCast, expression.Zero.Clone())
	} else {
		finalExpr = expression.NewFunctionInternal(ctx, ast.Ifnull, outerCast.GetType(), outerCast, &expression.Constant{Value: types.NewUintDatum(math.MaxUint64), RetType: targetTp})
	}
	return finalExpr
}

// wrapCastFunction will wrap a cast if the targetTp is not equal to the arg's.
func (a *aggregationEliminator) wrapCastFunction(ctx sessionctx.Context, arg expression.Expression, targetTp *types.FieldType) expression.Expression {
	if arg.GetType() == targetTp {
		return arg
	}
	return expression.BuildCastFunction(ctx, arg, targetTp)
}

func (a *aggregationEliminator) optimize(p LogicalPlan) (LogicalPlan, error) {
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := a.optimize(child)
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
	proj, err := a.tryEliminateAgg(agg)
	if proj != nil || err != nil {
		return proj, err
	}
	return p, nil
}
