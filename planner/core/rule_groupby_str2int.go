// Copyright 2022 PingCAP, Inc.
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

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

// This rule trys to convert an Aggregation on string column to Aggregation on int column.
// There are several requirements that have to be met so that this rule can be applied:
// 1. There is a Selection under Aggregation
// 2. The Selection has equality filter on one of the group keys
// 3. The filtered group key must be string type
// 4. The filter condition must be disjoint of IN or EQ scalar function
//
// e.g.
//  SELECT a, b, max(c) FROM foo WHERE a in ('x', 'y', 'z') GROUP BY a,b;
//
// will be transformed into
//  SELECT CASE WHEN a=0 THEN 'x' WHEN a=1 THEN 'y' WHEN a=2 THEN 'z' END as a, b, max
//  FROM (
//		SELECT a, b, max(c)
//		FROM (
//			 SELECT CASE WHEN a='x' THEN 0 WHEN a='y' THEN 1 WHEN a='z' THEN 2 END as a, b, c
//			 FROM foo
//			 WHERE a in ('x', 'y', 'z')
//		) t GROUP BY a,b
//  ) t;
//
// NB: This rule doesn't guarantee to yield better alternative, especially when
//  1. The IN list constant values are too many, hence the comparison may take a lot more time.
//  2. There is an index on the string group key that can be utilized by top Aggregation, whereas
//     the alternative will break the order property of the index.
//
// For the moment, we don't consider other special cases, such as the filter condition is below
// the child of Aggregation, unless we can pull up the filter from child operators.
//
// Ideally, the optimization should be done in query executor automatically, without intervention
// of optimizer, which requires that there is dictionary encoding in the storage engine and the
// execution engine can do the transformation on the fly.
//
// This rule is turned off by default. Use with caution.
type groupbyStr2IntConverter struct {
}

func (a *groupbyStr2IntConverter) tryToConvertGroubyStr2Int(
	agg *LogicalAggregation, sel *LogicalSelection,
	mapping map[string]*expression.Column, opt *logicalOptimizeOp) *LogicalProjection {
	if len(sel.Conditions) != 1 {
		return nil
	}
	cond := sel.Conditions[0]
	filter, ok := cond.(*expression.ScalarFunction)
	if !ok {
		return nil
	}
	dnfExprs := expression.FlattenDNFConditions(filter)
	ok, colExpr, consts := extractStrConstsFromExpr(dnfExprs)
	if !ok {
		return nil
	}

	filterOnGroupKey := false
	var groupKey expression.Expression
	// now check the filter column or expression is in the group keys
	for _, groupKey = range agg.GroupByItems {
		if groupKey.Equal(nil, colExpr) {
			filterOnGroupKey = true
			break
		}
	}

	// the filter column is not on group key
	if !filterOnGroupKey {
		return nil
	}

	// CASE WHEN expression that translates string to int
	lowerCaseExpr, err := makeCaseWhenFunc(agg.ctx, types.NewFieldType(mysql.TypeLong), colExpr, consts...)
	if err != nil {
		return nil
	}

	// create project on top of filter that transforms string to int
	lowerProj, newCol := createProjectOnFilter(sel, groupKey, lowerCaseExpr)

	// aggregate on int column
	newAgg := createAggOnProject(agg, lowerProj, groupKey, newCol)

	// CASE WHEN expression that translates int back to string
	upperCaseExpr, err := makeCaseWhenFunc(agg.ctx, groupKey.GetType(), newCol, consts...)
	if newAgg == nil || err != nil {
		return nil
	}

	// final Project that translate int group key to string
	topProj, caseCol := createProjectOnAgg(newAgg, newCol, upperCaseExpr)

	mapping[string(groupKey.HashCode(nil))] = caseCol

	return topProj
}

// extractStrConstsFromExpr extracts column expression and string constants from a list of disjoint expressions, e.g.
// a = '1' or a = '2' or a in ('3', '4') or a in ('5', '6', '7')
// will return a and ['1', '2', '3', '4', '5', '6', '7']
//
// CONCAT(a, b) = 'xxx' or CONCAT(a, b) = 'yyy' or CONCAT(a,b) in ('zzz', 'xyz')
// will return CONCAT(a, b) and ['xxx', 'yyy', 'zzz', 'xyz']
func extractStrConstsFromExpr(exprs []expression.Expression) (bool, expression.Expression, []expression.Constant) {
	var leftExpr expression.Expression
	var consts []expression.Constant

	for _, expr := range exprs {
		scfun, ok := expr.(*expression.ScalarFunction)
		if !ok {
			return false, nil, nil
		}
		var col expression.Expression
		if scfun.FuncName.L == ast.EQ {
			// for case: foo.col1 = 'xxx'
			col = scfun.GetArgs()[0]
			v, ok := scfun.GetArgs()[1].(*expression.Constant)
			if !ok {
				// for case: 'xxx' = foo.col1
				col = scfun.GetArgs()[1]
				v, ok = scfun.GetArgs()[0].(*expression.Constant)
				if !ok {
					return false, nil, nil
				}
			}
			// we only care about string constants
			if types.IsString(v.RetType.Tp) {
				consts = append(consts, *v)
			} else {
				return false, nil, nil
			}
		} else if scfun.FuncName.L == ast.In {
			col = scfun.GetArgs()[0]
			for i := 1; i < len(scfun.GetArgs()); i++ {
				v, ok := scfun.GetArgs()[i].(*expression.Constant)
				if !ok {
					return false, nil, nil
				}
				// we only care about string constants
				if types.IsString(v.RetType.Tp) {
					consts = append(consts, *v)
				} else {
					return false, nil, nil
				}
			}
		} else {
			return false, nil, nil
		}

		// make sure the conditions are using the same column or expression
		if leftExpr == nil {
			leftExpr = col
		} else if !leftExpr.Equal(nil, col) {
			return false, nil, nil
		}
	}

	return true, leftExpr, consts
}

// makeCaseWhenFunc creates a CASE WHEN... expression.
//
// If the retType is TypeLong, col is foo.a, strConsts is ['aaa', 'bbb', 'ccc'],
// it will generate the following expression:
// CASE WHEN foo.a='aaa' THEN 0 WHEN foo.a='bbb' THEN 1 WHEN foo.a='ccc' THEN 2 END
//
// If the retType is NOT TypeLong, col is foo.a, strConsts is ['aaa', 'bbb', 'ccc'],
// it will generate the following expression:
// CASE WHEN foo.a=0 THEN 'aaa' WHEN foo.a=1 THEN 'bbb' WHEN foo.a=2 THEN 'ccc' END
func makeCaseWhenFunc(ctx sessionctx.Context, retType *types.FieldType, col expression.Expression,
	strConsts ...expression.Constant) (expression.Expression, error) {
	size := len(strConsts)
	args := make([]expression.Expression, size*2)
	intConsts := make([]expression.Constant, size)

	for i := 0; i < size; i++ {
		intConsts[i] = *expression.NewInt(i)
	}

	// constants that we originally had
	srcConsts := strConsts
	// constants that we will have
	dstConsts := intConsts

	// this means we need to translate int constants to string constants
	if retType.Tp != mysql.TypeLong {
		srcConsts = intConsts
		dstConsts = strConsts
	}

	// create WHEN...THEN... pairs
	for i := 0; i < size; i++ {
		exp, err := expression.NewFunction(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), col, &srcConsts[i])
		if err != nil {
			return exp, err
		}
		args[i*2] = exp
		args[i*2+1] = &dstConsts[i]
	}

	return expression.NewFunction(ctx, ast.Case, retType, args...)
}

// createProjectOnFilter creates a lower Project on top of Selection. The new Project
// will translate string constants into int constants starting from 0.
func createProjectOnFilter(sel *LogicalSelection, groupKey expression.Expression,
	projExpr expression.Expression) (*LogicalProjection, *expression.Column) {
	found := false
	proj := LogicalProjection{
		Exprs: make([]expression.Expression, 0, sel.Schema().Len()),
	}.Init(sel.ctx, sel.blockOffset)

	newCol := &expression.Column{
		UniqueID: sel.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  projExpr.GetType(),
	}

	projSchema := make([]*expression.Column, 0, sel.Schema().Len())

	for _, column := range sel.Schema().Columns {
		// if the agg key is a column
		if column.Equal(sel.ctx, groupKey) {
			proj.Exprs = append(proj.Exprs, projExpr)
			projSchema = append(projSchema, newCol)
			found = true
		} else {
			proj.Exprs = append(proj.Exprs, column)
			projSchema = append(projSchema, column)
		}
	}
	// if agg key is a scalar function
	if !found {
		proj.Exprs = append(proj.Exprs, projExpr)
		projSchema = append(projSchema, newCol)
	}

	proj.SetSchema(expression.NewSchema(projSchema...))
	proj.SetChildren(sel)
	return proj, newCol
}

// createAggOnProject creates an Agg on top of generated Projection, the new Agg replace
// string agg key with int agg key.
func createAggOnProject(agg *LogicalAggregation, proj *LogicalProjection,
	groupKey expression.Expression, newCol *expression.Column) *LogicalAggregation {
	newAgg := LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, len(agg.AggFuncs)),
		GroupByItems: make([]expression.Expression, 0, len(agg.GroupByItems)),
		aggHints:     agg.aggHints,
	}.Init(agg.ctx, agg.blockOffset)

	for _, aggfunc := range agg.AggFuncs {
		newAf := aggfunc.Clone()
		for i, arg := range newAf.Args {
			if arg.Equal(agg.ctx, groupKey) {
				if i != 0 || newAf.Name != "firstrow" {
					return nil
				}
				newAf.Args[i] = newCol
				newAf.RetTp = types.NewFieldType(mysql.TypeLong)
			}
		}
		newAgg.AggFuncs = append(newAgg.AggFuncs, newAf)
	}

	for _, item := range agg.GroupByItems {
		if item.Equal(agg.ctx, groupKey) {
			newAgg.GroupByItems = append(newAgg.GroupByItems, newCol)
		} else {
			newAgg.GroupByItems = append(newAgg.GroupByItems, item)
		}
	}

	aggSchema := make([]*expression.Column, 0, agg.Schema().Len())
	for _, column := range agg.Schema().Columns {
		if groupKey.Equal(agg.ctx, column) {
			aggSchema = append(aggSchema, newCol)
		} else {
			aggSchema = append(aggSchema, column)
		}
	}
	newAgg.SetSchema(expression.NewSchema(aggSchema...))
	newAgg.SetChildren(proj)
	return newAgg
}

// createProjectOnAgg creates a Project on top of Aggregate.
// That Project will translate int back to string.
func createProjectOnAgg(newAgg *LogicalAggregation,
	groupKey expression.Expression, caseExpr expression.Expression) (*LogicalProjection, *expression.Column) {
	found := false
	proj := LogicalProjection{
		Exprs: make([]expression.Expression, 0, newAgg.Schema().Len()),
	}.Init(newAgg.ctx, newAgg.blockOffset)

	newCol := &expression.Column{
		UniqueID: newAgg.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  caseExpr.GetType(),
	}

	projSchema := make([]*expression.Column, 0, newAgg.Schema().Len())

	for _, column := range newAgg.Schema().Columns {
		// if the agg key is a column
		if column.Equal(newAgg.ctx, groupKey) {
			proj.Exprs = append(proj.Exprs, caseExpr)
			projSchema = append(projSchema, newCol)
			found = true
		} else {
			proj.Exprs = append(proj.Exprs, column)
			projSchema = append(projSchema, column)
		}
	}

	if !found {
		return nil, nil
	}

	proj.SetSchema(expression.NewSchema(projSchema...))
	proj.SetChildren(newAgg)
	return proj, newCol
}

func (a *groupbyStr2IntConverter) recursiveOptimize(ctx context.Context, p LogicalPlan,
	mapping map[string]*expression.Column, opt *logicalOptimizeOp) (LogicalPlan, error) {
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := a.recursiveOptimize(ctx, child, mapping, opt)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)

	// we have to update the parent's reference to the child columns
	p.replaceExprColumns(mapping)
	for _, dst := range p.Schema().Columns {
		resolveColumnAndReplace(dst, mapping)
	}

	agg, ok := p.(*LogicalAggregation)
	if !ok {
		return p, nil
	}
	sel, ok := agg.children[0].(*LogicalSelection)
	if !ok {
		return p, nil
	}
	if proj := a.tryToConvertGroubyStr2Int(agg, sel, mapping, opt); proj != nil {
		return proj, nil
	}
	return p, nil
}

func (a *groupbyStr2IntConverter) optimize(ctx context.Context, p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	mapping := make(map[string]*expression.Column)
	plan, err := a.recursiveOptimize(ctx, p, mapping, opt)
	return plan, err
}

func (*groupbyStr2IntConverter) name() string {
	return "groupby_str2int"
}
