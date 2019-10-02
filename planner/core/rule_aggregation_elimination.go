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
	"context"
	"math"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

type aggregationEliminator struct {
	aggregationEliminateChecker
}

type aggregationEliminateChecker struct {
}

// nestedAggPattern stores nested LogicalAggregations, so they can be accessed easily.
type nestedAggPattern struct {
	outer *LogicalAggregation
	proj  *LogicalProjection
	inner *LogicalAggregation
	// isTrivial indicates if there are operators(like LogicalSelection/LogicalLimit) between 2 aggs.
	isTrivial bool
}

// tryToEliminateAggregationByUniqueKey will eliminate aggregation grouped by unique key.
// e.g. select min(b) from t group by a. If a is a unique key, then this sql is equal to `select b from t group by a`.
// For count(expr), sum(expr), avg(expr), count(distinct expr, [expr...]) we may need to rewrite the expr. Details are shown below.
// If we can eliminate agg successful, we return a projection. Else we return a nil pointer.
func (a *aggregationEliminateChecker) tryToEliminateAggregationByUniqueKey(agg *LogicalAggregation) *LogicalProjection {
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
		proj := a.convertAggToProj(agg)
		proj.SetChildren(agg.children[0])
		return proj
	}
	return nil
}

func (a *aggregationEliminateChecker) convertAggToProj(agg *LogicalAggregation) *LogicalProjection {
	proj := LogicalProjection{
		Exprs: make([]expression.Expression, 0, len(agg.AggFuncs)),
	}.Init(agg.ctx, agg.blockOffset)
	for _, fun := range agg.AggFuncs {
		expr := a.rewriteExpr(agg.ctx, fun)
		proj.Exprs = append(proj.Exprs, expr)
	}
	proj.SetSchema(agg.schema.Clone())
	return proj
}

// rewriteExpr will rewrite the aggregate function to expression doesn't contain aggregate function.
func (a *aggregationEliminateChecker) rewriteExpr(ctx sessionctx.Context, aggFunc *aggregation.AggFuncDesc) expression.Expression {
	switch aggFunc.Name {
	case ast.AggFuncCount:
		if aggFunc.Mode == aggregation.FinalMode {
			return a.wrapCastFunction(ctx, aggFunc.Args[0], aggFunc.RetTp)
		}
		return a.rewriteCount(ctx, aggFunc.Args, aggFunc.RetTp)
	case ast.AggFuncSum, ast.AggFuncAvg, ast.AggFuncFirstRow, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncGroupConcat:
		return a.wrapCastFunction(ctx, aggFunc.Args[0], aggFunc.RetTp)
	case ast.AggFuncBitAnd, ast.AggFuncBitOr, ast.AggFuncBitXor:
		return a.rewriteBitFunc(ctx, aggFunc.Name, aggFunc.Args[0], aggFunc.RetTp)
	default:
		panic("Unsupported function")
	}
}

func (a *aggregationEliminateChecker) rewriteCount(ctx sessionctx.Context, exprs []expression.Expression, targetTp *types.FieldType) expression.Expression {
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

func (a *aggregationEliminateChecker) rewriteBitFunc(ctx sessionctx.Context, funcType string, arg expression.Expression, targetTp *types.FieldType) expression.Expression {
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
func (a *aggregationEliminateChecker) wrapCastFunction(ctx sessionctx.Context, arg expression.Expression, targetTp *types.FieldType) expression.Expression {
	if arg.GetType() == targetTp {
		return arg
	}
	return expression.BuildCastFunction(ctx, arg, targetTp)
}

// tryToEliminateAggregationByMapping tries to eliminate aggregation by combining two aggregations into one.
// Compared with tryToEliminateAggregationByUniqueKey, this rule handles two more types of cases:
//
// 1. Nested aggregations with same group-by items, while the items contains scalar functions rather than raw columns, for example:
//  `select p, max(dt) from (select a + b as p, count(d) as dt from t group by a + b) tt group by p`
//  can be rewritten as
//  `select a + b as p, count(d) as `max(dt)` from t group by a + b`
//
// 2. Group by items of outer aggregation are super set of nested aggregation, for example:
//  `select at, max(bt) from (select a as at, max(b) as bt from t group by a, c) tt group by at`
//  can be rewritten as
//  `select a as at, max(b) as `max(bt)` from t group by a`
//
// We map definition of outer aggregation to schema of inner aggregation and check if the rules can be applied.
func (a *aggregationEliminateChecker) tryToEliminateAggregationByMapping(la *LogicalAggregation) LogicalPlan {
	ptn := &nestedAggPattern{outer: la, isTrivial: true}
	collectNestedAggPattern(la, ptn)

	if ptn.inner == nil {
		return nil
	}
	exprMap, aggMap := genColMaps(ptn)

	// Map group-by items in outer aggregation to the schema of inner aggregation, so the group-by items can be compared.
	var items []expression.Expression
	if exprMap == nil {
		items = ptn.outer.GroupByItems
	} else {
		_, items = exprSubstitute(la.ctx, ptn.outer.GroupByItems, exprMap, func(i interface{}) expression.Expression {
			e, ok := i.(expression.Expression)
			if !ok {
				return nil
			}
			return e
		})
	}
	_, items = exprSubstitute(la.ctx, items, aggMap, func(i interface{}) expression.Expression {
		f, ok := i.(*aggregation.AggFuncDesc)
		if !ok {
			return nil
		}
		// Outer group-by items cannot be aggregated result of inner aggregation.
		if f.Name == ast.AggFuncFirstRow && len(f.Args) == 1 {
			return f.Args[0]
		}
		return nil
	})
	if items == nil {
		return nil
	}

	for _, item := range items {
		if !expression.EqualContains(la.ctx, ptn.inner.GroupByItems, item) {
			return nil
		}
	}
	if len(items) == len(ptn.inner.GroupByItems) {
		// Inner/outer aggregations have same group-by items, we can simply convert agg to proj.
		proj := a.convertAggToProj(la)
		proj.SetChildren(la.children[0])
		return proj
	}

	// Outer group-by items are subset of inner ones, which is very similar to `Partial` and `Final` AggFunctionMode.
	// In such cases, we try to merge each pair of inner/outer aggregate function, and then eliminate the
	// nested aggregation.
	if !ptn.isTrivial {
		// There are constraints(like limit/having/where clause) between the aggregations, cannot merge the aggregations.
		return nil
	}

	newFuncs := make([]*aggregation.AggFuncDesc, len(ptn.outer.AggFuncs))
	for idx, fun := range ptn.outer.AggFuncs {
		if len(fun.Args) != 1 {
			// Only aggregate functions with 1 param can be handled
			return nil
		}
		var exprs []expression.Expression
		if exprMap == nil {
			exprs = fun.Args
		} else {
			_, exprs = exprSubstitute(la.ctx, fun.Args, exprMap, func(i interface{}) expression.Expression {
				e, ok := i.(expression.Expression)
				if !ok {
					return nil
				}
				return e
			})
		}
		expr := exprs[0]

		// notExtraProj indicates if there is no extra `LogicalProjection` between outer/inner aggregated functions.
		// For example, if we have a `sum(a) + 1 as sum_a` and `sum(sum_a)`, the plus expression prevents the optimizations.
		_, noExtraProj := expr.(*expression.Column)
		// isGbItem indicates if expr is the group-by item of inner aggregation.
		// For example, select max(at), sum(bt) from (select a as at, count(b) as bt from t group by a) as tt,
		// in such case column `at` is an alias of `a` and is the group-by item of inner aggregation.
		isGbItem := expression.EqualContains(la.ctx, ptn.inner.GroupByItems, expr)
		if !noExtraProj && !isGbItem {
			return nil
		}

		cols := expression.ExtractColumns(expr)
		if len(cols) != 1 {
			// We have an outer aggregate function that refers to more than 1 inner aggregate function, for example,
			// `... (sum(b) + count(c)) as col ... `, cannot optimize such cases.
			return nil
		}

		col := cols[0]
		innerFun := aggMap[string(col.HashCode(nil))].(*aggregation.AggFuncDesc)
		if len(innerFun.Args) != 1 {
			return nil
		}
		// Generalize isGbItem: aggregate function in inner aggregation can also be treated as group by item.
		// For example, select max(at), sum(bt) from (select max(a) as at, count(b) as bt from t group by a) as tt,
		// in such case column `at` is an alias of `max(a)`, and `max(a)` is equivalent of `a`(`firstrow(a)`), which is the group by
		// item of inner aggregation.
		isGbItem = isGbItem || expression.EqualContains(la.ctx, ptn.inner.GroupByItems, innerFun.Args[0])

		_, combinedExprs := exprSubstitute(la.ctx, exprs, aggMap, func(i interface{}) expression.Expression {
			f, ok := i.(*aggregation.AggFuncDesc)
			if !ok {
				return nil
			}
			return f.Args[0]
		})
		newFunc := tryToCombineAggFunc(fun, innerFun, isGbItem, combinedExprs[0])
		if newFunc == nil {
			return nil
		}
		newFuncs[idx] = newFunc
	}

	// All agg functions are combined, now combine two aggregation plans.
	la.AggFuncs = newFuncs
	la.GroupByItems = items
	la.collectGroupByColumns()
	la.SetChildren(ptn.inner.Children()...)
	return la
}

func (a *aggregationEliminateChecker) tryToEliminateAggregation(agg *LogicalAggregation) LogicalPlan {
	if proj := a.tryToEliminateAggregationByUniqueKey(agg); proj != nil {
		return proj
	}
	var ok bool
	var result LogicalPlan
	for {
		if plan := a.tryToEliminateAggregationByMapping(agg); plan != nil {
			result = plan
			if agg, ok = plan.(*LogicalAggregation); ok {
				continue
			}
		}
		return result
	}
}

func (a *aggregationEliminator) optimize(ctx context.Context, p LogicalPlan) (LogicalPlan, error) {
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := a.optimize(ctx, child)
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
	if proj := a.tryToEliminateAggregation(agg); proj != nil {
		return proj, nil
	}
	return p, nil
}

func (*aggregationEliminator) name() string {
	return "aggregation_eliminate"
}

func collectNestedAggPattern(lp LogicalPlan, ptn *nestedAggPattern) {
	if len(lp.Children()) != 1 {
		return
	}
	child := lp.Children()[0]
	switch v := child.(type) {
	case *LogicalAggregation:
		ptn.inner = v
		return
	case *LogicalProjection:
		if ptn.proj != nil {
			return
		}
		ptn.proj = v
	case *LogicalSort:
		// Ignore sort between aggregations.
	default:
		ptn.isTrivial = false
	}
	collectNestedAggPattern(child, ptn)
}

// tryToCombineAggFunc checks the types of inner/outer aggregate function and check if they can be combined as one based on their semantics.
// for example, since max(max(PARTIAL)) can be combined as max(TOTAL), we can combine inner max() and outer max() as a final max()
// `innerIsGbItem` indicates if the inner aggregate function is aggregating group-by items(values are de-duplicated).
func tryToCombineAggFunc(outer, inner *aggregation.AggFuncDesc, innerIsGbItem bool, expr expression.Expression) *aggregation.AggFuncDesc {
	combined := outer.Clone()
	combined.Args[0] = expr

	if innerIsGbItem {
		// If the item being aggregated in outer aggregation is group-by item of inner aggregation. We can only do elimination on very
		// few conditions.
		// For example, `select sum(at) from (select count(a) as at, b as bt from t group by a, b) as t group by bt`
		// AggFunc `count(a)` is always 1 and `sum(at)` is distinct number of `a`. We only consider the simplest case here because for
		// other cases, there is no common rule to indicate the semantics.
		if (inner.Name == ast.AggFuncFirstRow || inner.Name == ast.AggFuncMax || inner.Name == ast.AggFuncMin) &&
			(outer.Name == ast.AggFuncFirstRow || outer.Name == ast.AggFuncMax || outer.Name == ast.AggFuncMin) {
			return combined
		}
		// TODO: add type cast to support more cases
	}

	switch {
	case inner.Name == ast.AggFuncFirstRow && outer.Name == ast.AggFuncFirstRow:
	case inner.Name == ast.AggFuncMax && outer.Name == ast.AggFuncMax:
	case inner.Name == ast.AggFuncMin && outer.Name == ast.AggFuncMin:
	case inner.Name == ast.AggFuncBitAnd && outer.Name == ast.AggFuncBitAnd:
	case inner.Name == ast.AggFuncBitOr && outer.Name == ast.AggFuncBitOr:
	case inner.Name == ast.AggFuncBitXor && outer.Name == ast.AggFuncBitXor:
		if inner.HasDistinct || outer.HasDistinct {
			return nil
		}
	case inner.Name == ast.AggFuncSum && outer.Name == ast.AggFuncSum:
		if inner.HasDistinct || outer.HasDistinct {
			return nil
		}
	case inner.Name == ast.AggFuncCount && outer.Name == ast.AggFuncSum:
		// TODO: add cast to support `sum(count(_)) -> count(_)`
		return nil
	default:
		return nil
	}
	return combined
}

// genColMaps generates exprMap(`col -> definition expr`) and aggMap(`col -> definition aggFunc`) from column definitions.
func genColMaps(ptn *nestedAggPattern) (map[string]interface{}, map[string]interface{}) {
	var exprMap map[string]interface{}
	if ptn.proj != nil {
		exprMap = make(map[string]interface{}, len(ptn.proj.Schema().Columns))
		for idx, col := range ptn.proj.Schema().Columns {
			exprMap[string(col.HashCode(nil))] = ptn.proj.Exprs[idx]
		}
	}
	aggMap := make(map[string]interface{}, len(ptn.inner.Schema().Columns))
	for idx, col := range ptn.inner.Schema().Columns {
		aggMap[string(col.HashCode(nil))] = ptn.inner.AggFuncs[idx]
	}

	return exprMap, aggMap
}

// cowExprs is a wrapper of slice of expression.Expression, which provides copy-on-write functionality
type cowExprs struct {
	copied bool
	e      []expression.Expression
}

func fromSlice(e []expression.Expression) *cowExprs {
	return &cowExprs{copied: false, e: e}
}

func (c *cowExprs) getSlice() (bool, []expression.Expression) {
	return c.copied, c.e
}

func (c *cowExprs) length() int {
	return len(c.e)
}

func (c *cowExprs) get(i int) expression.Expression {
	return c.e[i]
}

func (c *cowExprs) set(i int, e expression.Expression) {
	if !c.copied {
		c.copied = true
		newExprs := make([]expression.Expression, len(c.e))
		copy(newExprs, c.e)
		c.e = newExprs
	}
	c.e[i] = e
}

// exprSubstitute tries to substitutes expressions occurred in given `exprs` with values in `exprMap`.
// If the substitution happened, it returns true and the substituted expressions, otherwise it returns false and original exprs.
// This function returns:
//  * true / substituted exprs if the substitution happens.
//  * false / original exprs if substitution doesn't happen.
//  * false / nil if predicate is not nil and it failed.
func exprSubstitute(
	ctx sessionctx.Context,
	exprs []expression.Expression,
	m map[string]interface{},
	f func(interface{}) expression.Expression) (bool, []expression.Expression) {
	cow := fromSlice(exprs)
	for i := 0; i < cow.length(); i++ {
		expr := cow.get(i)
		if e, ok := m[string(expr.HashCode(ctx.GetSessionVars().StmtCtx))]; ok {
			if newExpr := f(e); newExpr != nil {
				cow.set(i, newExpr)
				continue
			}
			return false, nil
		}
		sf, ok := expr.(*expression.ScalarFunction)
		if !ok {
			continue
		}
		if replaced, subExprs := exprSubstitute(ctx, sf.GetArgs(), m, f); replaced {
			newSf := expression.NewFunctionInternal(ctx, sf.FuncName.L, sf.GetType(), subExprs...)
			cow.set(i, newSf)
		} else if subExprs == nil {
			return false, nil
		}
	}
	return cow.getSlice()
}
