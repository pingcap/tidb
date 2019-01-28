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

	// The schema of PhysicalSort and PhysicalTopN are the same as the schema of
	// their children. When a projection is injected as the child of
	// PhysicalSort and PhysicalTopN, some extra columns will be added into the
	// schema of the Projection, thus we need to add another Projection upon
	// them to prune the redundant columns.
	var origSchema *expression.Schema
	needProj2PruneColumns := false
	switch p := plan.(type) {
	case *PhysicalHashAgg:
		plan = injectProjBelowAgg(plan, p.AggFuncs, p.GroupByItems)
	case *PhysicalStreamAgg:
		plan = injectProjBelowAgg(plan, p.AggFuncs, p.GroupByItems)
	case *PhysicalSort:
		origSchema = plan.Schema().Clone()
		plan, needProj2PruneColumns = injectProjBelowSort(p, p.ByItems)
	case *PhysicalTopN:
		origSchema = plan.Schema().Clone()
		plan, needProj2PruneColumns = injectProjBelowSort(p, p.ByItems)
	}
	if !needProj2PruneColumns {
		return plan
	}
	prop := plan.GetChildReqProps(0).Clone()
	projExprs := make([]expression.Expression, 0, origSchema.Len())
	for i := 0; i < origSchema.Len(); i++ {
		col := plan.Schema().Columns[i]
		col.Index = i
		projExprs = append(projExprs, col)
	}
	proj := PhysicalProjection{
		Exprs:                projExprs,
		AvoidColumnEvaluator: false,
	}.Init(plan.context(), plan.statsInfo().ScaleByExpectCnt(prop.ExpectedCnt), prop)
	proj.SetSchema(origSchema)
	proj.SetChildren(plan)
	return proj
}

// injectProjBelowAgg injects a ProjOperator below AggOperator. If all the args
// of `aggFuncs`, and all the item of `groupByItems` are columns or constants,
// we do not need to build the `proj`.
func injectProjBelowAgg(aggPlan PhysicalPlan, aggFuncs []*aggregation.AggFuncDesc, groupByItems []expression.Expression) PhysicalPlan {
	hasScalarFunc := false

	// If the mode is FinalMode, we do not need to wrap cast upon the args,
	// since the types of the args are already the expected.
	if len(aggFuncs) > 0 && aggFuncs[0].Mode != aggregation.FinalMode {
		for _, agg := range aggFuncs {
			agg.WrapCastForAggArgs(aggPlan.context())
		}
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
			RetType: item.GetType(),
			ColName: model.NewCIStr(fmt.Sprintf("col_%d", len(projSchemaCols))),
			Index:   cursor,
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
	}.Init(aggPlan.context(), child.statsInfo().ScaleByExpectCnt(prop.ExpectedCnt), prop)
	proj.SetSchema(expression.NewSchema(projSchemaCols...))
	proj.SetChildren(child)

	aggPlan.SetChildren(proj)
	return aggPlan
}

func injectProjBelowSort(p PhysicalPlan, orderByItems []*ByItems) (PhysicalPlan, bool) {
	hasScalarFunc, numOrderByItems := false, len(orderByItems)
	for i := 0; !hasScalarFunc && i < numOrderByItems; i++ {
		_, isScalarFunc := orderByItems[i].Expr.(*expression.ScalarFunction)
		hasScalarFunc = hasScalarFunc || isScalarFunc
	}
	if !hasScalarFunc {
		return p, false
	}

	childPlan := p.Children()[0]
	projSchemaCols := make([]*expression.Column, 0, len(childPlan.Schema().Columns)+numOrderByItems)
	projExprs := make([]expression.Expression, 0, len(childPlan.Schema().Columns)+numOrderByItems)
	for i, col := range childPlan.Schema().Columns {
		col.Index = i
		projSchemaCols = append(projSchemaCols, col)
		projExprs = append(projExprs, col)
	}

	for _, item := range orderByItems {
		itemExpr := item.Expr
		if _, isCnst := itemExpr.(*expression.Constant); isCnst {
			continue
		}
		projExprs = append(projExprs, itemExpr)
		newArg := &expression.Column{
			RetType: itemExpr.GetType(),
			ColName: model.NewCIStr(fmt.Sprintf("col_%d", len(projSchemaCols))),
			Index:   len(projSchemaCols),
		}
		projSchemaCols = append(projSchemaCols, newArg)
		item.Expr = newArg
	}

	prop := p.GetChildReqProps(0).Clone()
	proj := PhysicalProjection{
		Exprs:                projExprs,
		AvoidColumnEvaluator: false,
	}.Init(p.context(), childPlan.statsInfo().ScaleByExpectCnt(prop.ExpectedCnt), prop)
	proj.SetSchema(expression.NewSchema(projSchemaCols...))
	proj.SetChildren(childPlan)
	p.SetChildren(proj)
	return p, true
}
