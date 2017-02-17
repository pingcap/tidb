// Copyright 2017 PingCAP, Inc.
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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

type aggPruner struct {
	allocator *idAllocator
	ctx       context.Context
}

func (ap *aggPruner) optimize(lp LogicalPlan, ctx context.Context, allocator *idAllocator) (LogicalPlan, error) {
	ap.ctx = ctx
	ap.allocator = allocator
	return ap.eliminateAggregation(lp)
}

// eliminateAggregation will eliminate aggregation grouped by unique key.
// e.g. select min(b) from t group by a. If a is a unique key, then this sql is equal to `select b from t group by a`.
// For count(expr), sum(expr), avg(expr), count(distinct expr, [expr...]) we may need to rewrite the expr. Details are shown below.
func (ap *aggPruner) eliminateAggregation(p LogicalPlan) (LogicalPlan, error) {
	retPlan := p
	if agg, ok := p.(*Aggregation); ok {
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
			proj := &Projection{
				Exprs:           make([]expression.Expression, 0, len(agg.AggFuncs)),
				baseLogicalPlan: newBaseLogicalPlan(Proj, ap.allocator),
			}
			proj.self = proj
			proj.initIDAndContext(ap.ctx)
			for _, fun := range agg.AggFuncs {
				expr, err := ap.rewriteExpr(fun.GetArgs(), fun.GetName())
				if err != nil {
					return nil, errors.Trace(err)
				}
				proj.Exprs = append(proj.Exprs, expr)
			}
			proj.SetSchema(agg.schema.Clone())
			proj.SetParents(p.Parents()...)
			for _, child := range p.Children() {
				child.SetParents(proj)
			}
			retPlan = proj
		}
	}
	newChildren := make([]Plan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := ap.eliminateAggregation(child.(LogicalPlan))
		if err != nil {
			return nil, errors.Trace(err)
		}
		newChildren = append(newChildren, newChild)
	}
	retPlan.SetChildren(newChildren...)
	return retPlan, nil
}

// rewriteExpr will rewrite the aggregate function to expression doesn't contain aggregate function.
func (ap *aggPruner) rewriteExpr(exprs []expression.Expression, funcName string) (newExpr expression.Expression, err error) {
	switch funcName {
	case ast.AggFuncCount:
		// If is count(expr), we will change it to if(isnull(expr), 0, 1).
		// If is count(distinct x, y, z) we will change it to if(isnull(x) or isnull(y) or isnull(z), 0, 1).
		isNullExprs := make([]expression.Expression, 0, len(exprs))
		for _, expr := range exprs {
			isNullExpr, err := expression.NewFunction(ap.ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), expr.Clone())
			if err != nil {
				return nil, errors.Trace(err)
			}
			isNullExprs = append(isNullExprs, isNullExpr)
		}
		innerExpr := expression.ComposeDNFCondition(ap.ctx, isNullExprs...)
		newExpr, err = expression.NewFunction(ap.ctx, ast.If, types.NewFieldType(mysql.TypeLonglong), innerExpr, expression.Zero, expression.One)
		if err != nil {
			return nil, errors.Trace(err)
		}
	// See https://dev.mysql.com/doc/refman/5.7/en/group-by-functions.html
	// The SUM() and AVG() functions return a DECIMAL value for exact-value arguments (integer or DECIMAL),
	// and a DOUBLE value for approximate-value arguments (FLOAT or DOUBLE).
	case ast.AggFuncSum, ast.AggFuncAvg:
		expr := exprs[0].Clone()
		switch expr.GetType().Tp {
		// Integer type should be cast to decimal.
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			newExpr = expression.NewCastFunc(types.NewFieldType(mysql.TypeNewDecimal), expr, ap.ctx)
		// Double and Decimal doesn't need to be cast.
		case mysql.TypeDouble, mysql.TypeNewDecimal:
			newExpr = expr
		// Float should be cast to double. And other non-numeric type should be cast to double too.
		default:
			newExpr = expression.NewCastFunc(types.NewFieldType(mysql.TypeDouble), expr, ap.ctx)
		}
	default:
		// Default we do nothing about expr.
		newExpr = exprs[0].Clone()
	}
	return
}
