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

// eliminateAggregation will eliminate aggregation grouped by unique key.
// e.g. select min(b) from t group by a. If a is a unique key, then this sql is equal to `select b from t group by a`.
// For count(expr), sum(expr), avg(expr), we may need to rewrite the expr. Details is shown below.
func (ap *aggPruner) eliminateAggregation(p LogicalPlan) error {
	for _, child := range p.GetChildren() {
		if agg, ok := child.(*Aggregation); ok {
			schemaByGroupby := expression.NewSchema(agg.groupByCols)
			coveredByUniqueKey := false
			for _, key := range agg.schema.Keys {
				if schemaByGroupby.GetColumnsIndices(key) != nil {
					coveredByUniqueKey = true
					break
				}
			}
			if !coveredByUniqueKey {
				continue
			}
			// GroupByCols has unique key. So this aggregation can be removed.
			proj := &Projection{
				Exprs:           make([]expression.Expression, 0, len(agg.AggFuncs)),
				baseLogicalPlan: newBaseLogicalPlan(Proj, ap.allocator),
			}
			proj.self = proj
			proj.initIDAndContext(ap.ctx)
			for _, fun := range agg.AggFuncs {
				expr, err := ap.rewriteExpr(fun.GetArgs()[0].Clone(), fun.GetName())
				if err != nil {
					return errors.Trace(err)
				}
				proj.Exprs = append(proj.Exprs, expr)
			}
			proj.SetSchema(agg.schema.Clone())
			InsertPlan(p, agg, proj)
			RemovePlan(agg)
		}
	}
	for _, child := range p.GetChildren() {
		err := ap.eliminateAggregation(child.(LogicalPlan))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (ap *aggPruner) rewriteExpr(expr expression.Expression, funcName string) (newExpr expression.Expression, err error) {
	switch funcName {
	case ast.AggFuncCount:
		// If is count(expr), we will change it to if(isnull(expr), 0, 1).
		isNullExpr, err := expression.NewFunction(ast.IsNull, types.NewFieldType(mysql.TypeTiny), expr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		zero := &expression.Constant{
			Value:   types.NewIntDatum(0),
			RetType: types.NewFieldType(mysql.TypeLonglong),
		}
		one := &expression.Constant{
			Value:   types.NewIntDatum(1),
			RetType: types.NewFieldType(mysql.TypeLonglong),
		}
		newExpr, err = expression.NewFunction(ast.If, zero.RetType, isNullExpr, zero, one)
		if err != nil {
			return nil, errors.Trace(err)
		}
	case ast.AggFuncSum:
		// Numeric type do nothing. Others will cast to decimal.
		switch expr.GetType().Tp {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		case mysql.TypeDouble, mysql.TypeFloat, mysql.TypeDecimal, mysql.TypeNewDecimal:
			newExpr = expr
		default:
			newExpr, err = expression.NewCastFunc(types.NewFieldType(mysql.TypeNewDecimal), expr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	case ast.AggFuncAvg:
		switch expr.GetType().Tp {
		// Float-point number or fixed-point numnber will do nothing, Others will cast to decimal.
		case mysql.TypeDouble, mysql.TypeFloat, mysql.TypeDecimal, mysql.TypeNewDecimal:
			newExpr = expr
		default:
			newExpr, err = expression.NewCastFunc(types.NewFieldType(mysql.TypeNewDecimal), expr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	default:
		// Default we do nothing about expr.
		newExpr = expr
	}
	return
}
