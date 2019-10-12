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

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
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
	for i, child := range plan.Children() {
		plan.Children()[i] = pe.inject(child)
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

// wrapCastForAggFunc wraps the args of an aggregate function with a cast function.
// If the mode is FinalMode or Partial2Mode, we do not need to wrap cast upon the args,
// since the types of the args are already the expected.
func wrapCastForAggFuncs(sctx sessionctx.Context, aggFuncs []*aggregation.AggFuncDesc) {
	for i := range aggFuncs {
		if aggFuncs[i].Mode != aggregation.FinalMode && aggFuncs[i].Mode != aggregation.Partial2Mode {
			aggFuncs[i].WrapCastForAggArgs(sctx)
		}
	}
}

// injectProjBelowAgg injects a ProjOperator below AggOperator. If all the args
// of `aggFuncs`, and all the item of `groupByItems` are columns or constants,
// we do not need to build the `proj`.
func injectProjBelowAgg(aggPlan PhysicalPlan, aggFuncs []*aggregation.AggFuncDesc, groupByItems []expression.Expression) PhysicalPlan {
	hasScalarFunc := false

	wrapCastForAggFuncs(aggPlan.SCtx(), aggFuncs)
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
				UniqueID: aggPlan.SCtx().GetSessionVars().AllocPlanColumnID(),
				RetType:  arg.GetType(),
				ColName:  model.NewCIStr(fmt.Sprintf("col_%d", len(projSchemaCols))),
				Index:    cursor,
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
			UniqueID: aggPlan.SCtx().GetSessionVars().AllocPlanColumnID(),
			RetType:  item.GetType(),
			ColName:  model.NewCIStr(fmt.Sprintf("col_%d", len(projSchemaCols))),
			Index:    cursor,
		}
		projSchemaCols = append(projSchemaCols, newArg)
		groupByItems[i] = newArg
		cursor++
	}

	child := aggPlan.Children()[0]
	prop := aggPlan.GetChildReqProps(0).Clone()
	proj := PhysicalProjection{
		Exprs:                projExprs,
		AvoidColumnEvaluator: false,
	}.Init(aggPlan.SCtx(), child.statsInfo().ScaleByExpectCnt(prop.ExpectedCnt), aggPlan.SelectBlockOffset(), prop)
	proj.SetSchema(expression.NewSchema(projSchemaCols...))
	proj.SetChildren(child)

	aggPlan.SetChildren(proj)
	return aggPlan
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
		col := p.Schema().Columns[i].Clone().(*expression.Column)
		col.Index = i
		topProjExprs = append(topProjExprs, col)
	}
	topProj := PhysicalProjection{
		Exprs:                topProjExprs,
		AvoidColumnEvaluator: false,
	}.Init(p.SCtx(), p.statsInfo(), p.SelectBlockOffset(), nil)
	topProj.SetSchema(p.Schema().Clone())
	topProj.SetChildren(p)

	childPlan := p.Children()[0]
	bottomProjSchemaCols := make([]*expression.Column, 0, len(childPlan.Schema().Columns)+numOrderByItems)
	bottomProjExprs := make([]expression.Expression, 0, len(childPlan.Schema().Columns)+numOrderByItems)
	for i, col := range childPlan.Schema().Columns {
		newCol := col.Clone().(*expression.Column)
		newCol.Index = i
		bottomProjSchemaCols = append(bottomProjSchemaCols, newCol)
		bottomProjExprs = append(bottomProjExprs, newCol)
	}

	for _, item := range orderByItems {
		itemExpr := item.Expr
		if _, isScalarFunc := itemExpr.(*expression.ScalarFunction); !isScalarFunc {
			continue
		}
		bottomProjExprs = append(bottomProjExprs, itemExpr)
		newArg := &expression.Column{
			UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
			RetType:  itemExpr.GetType(),
			ColName:  model.NewCIStr(fmt.Sprintf("col_%d", len(bottomProjSchemaCols))),
			Index:    len(bottomProjSchemaCols),
		}
		bottomProjSchemaCols = append(bottomProjSchemaCols, newArg)
		item.Expr = newArg
	}

	childProp := p.GetChildReqProps(0).Clone()
	bottomProj := PhysicalProjection{
		Exprs:                bottomProjExprs,
		AvoidColumnEvaluator: false,
	}.Init(p.SCtx(), childPlan.statsInfo().ScaleByExpectCnt(childProp.ExpectedCnt), p.SelectBlockOffset(), childProp)
	bottomProj.SetSchema(expression.NewSchema(bottomProjSchemaCols...))
	bottomProj.SetChildren(childPlan)
	p.SetChildren(bottomProj)

	if origChildProj, isChildProj := childPlan.(*PhysicalProjection); isChildProj {
		refine4NeighbourProj(bottomProj, origChildProj)
	}

	return topProj
}
