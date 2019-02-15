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
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

// injectExtraProjection is used to extract the expressions of specific
// operators into a physical Projection operator and inject the Projection below
// the operators. Thus we can accelerate the expression evaluation by eager
// evaluation.
func injectExtraProjection(plan PhysicalPlan) PhysicalPlan {
	return NewProjInjector().inject(plan)
}

type projInjector struct {
}

// NewProjInjector builds a projInjector.
func NewProjInjector() *projInjector {
	return &projInjector{}
}

func (pe *projInjector) inject(plan PhysicalPlan) PhysicalPlan {
	for _, child := range plan.Children() {
		pe.inject(child)
	}

	switch p := plan.(type) {
	case *PhysicalHashAgg:
		plan = injectProjBelowAgg(plan, p.AggFuncs, p.GroupByItems)
	case *PhysicalStreamAgg:
		plan = injectProjBelowAgg(plan, p.AggFuncs, p.GroupByItems)
	case *PhysicalSort:
		plan = injectProjBelowSort(p, p.ByItems)
	case *PhysicalTopN:
		plan = injectProjBelowSort(p, p.ByItems)
	}
	return plan
}

// injectProjBelowAgg injects a ProjOperator below AggOperator. If all the args
// of `aggFuncs`, and all the item of `groupByItems` are columns or constants,
// we do not need to build the `proj`.
func injectProjBelowAgg(aggPlan PhysicalPlan, aggFuncs []*aggregation.AggFuncDesc, groupByItems []expression.Expression) PhysicalPlan {
	hasScalarFunc := false

	// If the mode is FinalMode, we do not need to wrap cast upon the args,
	// since the types of the args are already the expected.
	if len(aggFuncs) > 0 && aggFuncs[0].Mode != aggregation.FinalMode {
		wrapCastForAggArgs(aggPlan.context(), aggFuncs)
	}

	for i := 0; !hasScalarFunc && i < len(aggFuncs); i++ {
		for _, arg := range aggFuncs[i].Args {
			_, isScalarFunc := arg.(*expression.ScalarFunction)
			hasScalarFunc = hasScalarFunc || isScalarFunc
		}
	}
	for i := 0; !hasScalarFunc && i < len(groupByItems); i++ {
		_, isScalarFunc := groupByItems[i].(*expression.ScalarFunction)
		hasScalarFunc = hasScalarFunc || isScalarFunc
	}
	if !hasScalarFunc {
		return aggPlan
	}

	projSchemaCols := make([]*expression.Column, 0, len(aggFuncs)+len(groupByItems))
	projExprs := make([]expression.Expression, 0, cap(projSchemaCols))
	cursor := 0

	for _, f := range aggFuncs {
		for i, arg := range f.Args {
			if _, isCnst := arg.(*expression.Constant); isCnst {
				continue
			}
			projExprs = append(projExprs, arg)
			newArg := &expression.Column{
				RetType: arg.GetType(),
				ColName: model.NewCIStr(fmt.Sprintf("col_%d", len(projSchemaCols))),
				Index:   cursor,
			}
			projSchemaCols = append(projSchemaCols, newArg)
			f.Args[i] = newArg
			cursor++
		}
	}

	for i, item := range groupByItems {
		if _, isCnst := item.(*expression.Constant); isCnst {
			continue
		}
		projExprs = append(projExprs, item)
		newArg := &expression.Column{
			UniqueID: aggPlan.context().GetSessionVars().AllocPlanColumnID(),
			RetType:  item.GetType(),
			ColName:  model.NewCIStr(fmt.Sprintf("col_%d", len(projSchemaCols))),
			Index:    cursor,
		}
		projSchemaCols = append(projSchemaCols, newArg)
		groupByItems[i] = newArg
		cursor++
	}

	child := aggPlan.Children()[0]
	prop := aggPlan.getChildReqProps(0).Clone()
	proj := PhysicalProjection{
		Exprs:                projExprs,
		AvoidColumnEvaluator: false,
	}.init(aggPlan.context(), child.statsInfo().ScaleByExpectCnt(prop.ExpectedCnt), prop)
	proj.SetSchema(expression.NewSchema(projSchemaCols...))
	proj.SetChildren(child)

	aggPlan.SetChildren(proj)
	return aggPlan
}

// wrapCastForAggArgs wraps the args of an aggregate function with a cast function.
func wrapCastForAggArgs(ctx sessionctx.Context, funcs []*aggregation.AggFuncDesc) {
	for _, f := range funcs {
		// We do not need to wrap cast upon these functions,
		// since the EvalXXX method called by the arg is determined by the corresponding arg type.
		if f.Name == ast.AggFuncCount || f.Name == ast.AggFuncMin || f.Name == ast.AggFuncMax || f.Name == ast.AggFuncFirstRow {
			continue
		}
		var castFunc func(ctx sessionctx.Context, expr expression.Expression) expression.Expression
		switch retTp := f.RetTp; retTp.EvalType() {
		case types.ETInt:
			castFunc = expression.WrapWithCastAsInt
		case types.ETReal:
			castFunc = expression.WrapWithCastAsReal
		case types.ETString:
			castFunc = expression.WrapWithCastAsString
		case types.ETDecimal:
			castFunc = expression.WrapWithCastAsDecimal
		default:
			panic("should never happen in executorBuilder.wrapCastForAggArgs")
		}
		for i := range f.Args {
			f.Args[i] = castFunc(ctx, f.Args[i])
			if f.Name != ast.AggFuncAvg && f.Name != ast.AggFuncSum {
				continue
			}
			// After wrapping cast on the argument, flen etc. may not the same
			// as the type of the aggregation function. The following part set
			// the type of the argument exactly as the type of the aggregation
			// function.
			// Note: If the `Tp` of argument is the same as the `Tp` of the
			// aggregation function, it will not wrap cast function on it
			// internally. The reason of the special handling for `Column` is
			// that the `RetType` of `Column` refers to the `infoschema`, so we
			// need to set a new variable for it to avoid modifying the
			// definition in `infoschema`.
			if col, ok := f.Args[i].(*expression.Column); ok {
				col.RetType = types.NewFieldType(col.RetType.Tp)
			}
			// originTp is used when the the `Tp` of column is TypeFloat32 while
			// the type of the aggregation function is TypeFloat64.
			originTp := f.Args[i].GetType().Tp
			*(f.Args[i].GetType()) = *(f.RetTp)
			f.Args[i].GetType().Tp = originTp
		}
	}
}

// injectProjBelowSort extracts the ScalarFunctions of `orderByItems` into a
// PhysicalProjection and injects it below PhysicalTopN/PhysicalSort. The schema
// of PhysicalSort and PhysicalTopN are the same as the schema of their
// children. When a projection is injected as the child of PhysicalSort and
// PhysicalTopN, some extra columns will be added into the schema of the
// Projection, thus we need to add another Projection upon them to prune the
// redundant columns.
func injectProjBelowSort(p PhysicalPlan, orderByItems []*ByItems) PhysicalPlan {
	hasScalarFunc, numOrderByItems := false, len(orderByItems)
	for i := 0; !hasScalarFunc && i < numOrderByItems; i++ {
		_, isScalarFunc := orderByItems[i].Expr.(*expression.ScalarFunction)
		hasScalarFunc = hasScalarFunc || isScalarFunc
	}
	if !hasScalarFunc {
		return p
	}

	topProjExprs := make([]expression.Expression, 0, p.Schema().Len())
	for i := range p.Schema().Columns {
		col := p.Schema().Columns[i]
		col.Index = i
		topProjExprs = append(topProjExprs, col)
	}
	topProj := PhysicalProjection{
		Exprs:                topProjExprs,
		AvoidColumnEvaluator: false,
	}.init(p.context(), p.statsInfo(), nil)
	topProj.SetSchema(p.Schema().Clone())
	topProj.SetChildren(p)

	childPlan := p.Children()[0]
	bottomProjSchemaCols := make([]*expression.Column, 0, len(childPlan.Schema().Columns)+numOrderByItems)
	bottomProjExprs := make([]expression.Expression, 0, len(childPlan.Schema().Columns)+numOrderByItems)
	for i, col := range childPlan.Schema().Columns {
		col.Index = i
		bottomProjSchemaCols = append(bottomProjSchemaCols, col)
		bottomProjExprs = append(bottomProjExprs, col)
	}

	for _, item := range orderByItems {
		itemExpr := item.Expr
		if _, isScalarFunc := itemExpr.(*expression.ScalarFunction); !isScalarFunc {
			continue
		}
		bottomProjExprs = append(bottomProjExprs, itemExpr)
		newArg := &expression.Column{
			UniqueID: p.context().GetSessionVars().AllocPlanColumnID(),
			RetType:  itemExpr.GetType(),
			ColName:  model.NewCIStr(fmt.Sprintf("col_%d", len(bottomProjSchemaCols))),
			Index:    len(bottomProjSchemaCols),
		}
		bottomProjSchemaCols = append(bottomProjSchemaCols, newArg)
		item.Expr = newArg
	}

	childProp := p.getChildReqProps(0).Clone()
	bottomProj := PhysicalProjection{
		Exprs:                bottomProjExprs,
		AvoidColumnEvaluator: false,
	}.init(p.context(), childPlan.statsInfo().ScaleByExpectCnt(childProp.ExpectedCnt), childProp)
	bottomProj.SetSchema(expression.NewSchema(bottomProjSchemaCols...))
	bottomProj.SetChildren(childPlan)
	p.SetChildren(bottomProj)

	if origChildProj, isChildProj := childPlan.(*PhysicalProjection); isChildProj {
		refine4NeighbourProj(bottomProj, origChildProj)
	}
	return topProj
}
