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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
)

// InjectExtraProjection is used to extract the expressions of specific
// operators into a physical Projection operator and inject the Projection below
// the operators. Thus we can accelerate the expression evaluation by eager
// evaluation.
// This function will be called in two situations:
// 1. In postOptimize.
// 2. TiDB can be used as a coprocessor, when a plan tree been pushed down to
// TiDB, we need to inject extra projections for the plan tree as well.
func InjectExtraProjection(plan base.PhysicalPlan) base.PhysicalPlan {
	failpoint.Inject("DisableProjectionPostOptimization", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(plan)
		}
	})

	return NewProjInjector().inject(plan)
}

type projInjector struct {
}

// NewProjInjector builds a projInjector.
func NewProjInjector() *projInjector {
	return &projInjector{}
}

func (pe *projInjector) inject(plan base.PhysicalPlan) base.PhysicalPlan {
	for i, child := range plan.Children() {
		plan.Children()[i] = pe.inject(child)
	}

	if tr, ok := plan.(*PhysicalTableReader); ok && tr.StoreType == kv.TiFlash {
		tr.tablePlan = pe.inject(tr.tablePlan)
		tr.TablePlans = flattenPushDownPlan(tr.tablePlan)
	}

	switch p := plan.(type) {
	case *PhysicalHashAgg:
		plan = InjectProjBelowAgg(plan, p.AggFuncs, p.GroupByItems)
	case *PhysicalStreamAgg:
		plan = InjectProjBelowAgg(plan, p.AggFuncs, p.GroupByItems)
	case *PhysicalSort:
		plan = InjectProjBelowSort(p, p.ByItems)
	case *PhysicalTopN:
		plan = InjectProjBelowSort(p, p.ByItems)
	case *NominalSort:
		plan = TurnNominalSortIntoProj(p, p.OnlyColumn, p.ByItems)
	case *PhysicalUnionAll:
		plan = injectProjBelowUnion(p)
	}
	return plan
}

func injectProjBelowUnion(un *PhysicalUnionAll) *PhysicalUnionAll {
	if !un.mpp {
		return un
	}
	for i, ch := range un.children {
		exprs := make([]expression.Expression, len(ch.Schema().Columns))
		needChange := false
		for i, dstCol := range un.schema.Columns {
			dstType := dstCol.RetType
			srcCol := ch.Schema().Columns[i]
			srcCol.Index = i
			srcType := srcCol.RetType
			if !srcType.Equal(dstType) || !(mysql.HasNotNullFlag(dstType.GetFlag()) == mysql.HasNotNullFlag(srcType.GetFlag())) {
				exprs[i] = expression.BuildCastFunction4Union(un.SCtx().GetExprCtx(), srcCol, dstType)
				needChange = true
			} else {
				exprs[i] = srcCol
			}
		}
		if needChange {
			proj := PhysicalProjection{
				Exprs: exprs,
			}.Init(un.SCtx(), ch.StatsInfo(), 0)
			proj.SetSchema(un.schema.Clone())
			proj.SetChildren(ch)
			un.children[i] = proj
		}
	}
	return un
}

// InjectProjBelowAgg injects a ProjOperator below AggOperator. So that All
// scalar functions in aggregation may speed up by vectorized evaluation in
// the `proj`. If all the args of `aggFuncs`, and all the item of `groupByItems`
// are columns or constants, we do not need to build the `proj`.
func InjectProjBelowAgg(aggPlan base.PhysicalPlan, aggFuncs []*aggregation.AggFuncDesc, groupByItems []expression.Expression) base.PhysicalPlan {
	hasScalarFunc := false

	coreusage.WrapCastForAggFuncs(aggPlan.SCtx().GetExprCtx(), aggFuncs)
	for i := 0; !hasScalarFunc && i < len(aggFuncs); i++ {
		for _, arg := range aggFuncs[i].Args {
			_, isScalarFunc := arg.(*expression.ScalarFunction)
			hasScalarFunc = hasScalarFunc || isScalarFunc
		}
		for _, byItem := range aggFuncs[i].OrderByItems {
			_, isScalarFunc := byItem.Expr.(*expression.ScalarFunction)
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

	ectx := aggPlan.SCtx().GetExprCtx().GetEvalCtx()
	for _, f := range aggFuncs {
		for i, arg := range f.Args {
			if _, isCnst := arg.(*expression.Constant); isCnst {
				continue
			}
			projExprs = append(projExprs, arg)
			newArg := &expression.Column{
				UniqueID: aggPlan.SCtx().GetSessionVars().AllocPlanColumnID(),
				RetType:  arg.GetType(ectx),
				Index:    cursor,
			}
			projSchemaCols = append(projSchemaCols, newArg)
			f.Args[i] = newArg
			cursor++
		}
		for _, byItem := range f.OrderByItems {
			if _, isCnst := byItem.Expr.(*expression.Constant); isCnst {
				continue
			}
			projExprs = append(projExprs, byItem.Expr)
			newArg := &expression.Column{
				UniqueID: aggPlan.SCtx().GetSessionVars().AllocPlanColumnID(),
				RetType:  byItem.Expr.GetType(ectx),
				Index:    cursor,
			}
			projSchemaCols = append(projSchemaCols, newArg)
			byItem.Expr = newArg
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
			RetType:  item.GetType(ectx),
			Index:    cursor,
		}
		projSchemaCols = append(projSchemaCols, newArg)
		groupByItems[i] = newArg
		cursor++
	}

	child := aggPlan.Children()[0]
	prop := aggPlan.GetChildReqProps(0).CloneEssentialFields()
	proj := PhysicalProjection{
		Exprs:                projExprs,
		AvoidColumnEvaluator: false,
	}.Init(aggPlan.SCtx(), child.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), aggPlan.QueryBlockOffset(), prop)
	proj.SetSchema(expression.NewSchema(projSchemaCols...))
	proj.SetChildren(child)

	aggPlan.SetChildren(proj)
	return aggPlan
}

// InjectProjBelowSort extracts the ScalarFunctions of `orderByItems` into a
// PhysicalProjection and injects it below PhysicalTopN/PhysicalSort. The schema
// of PhysicalSort and PhysicalTopN are the same as the schema of their
// children. When a projection is injected as the child of PhysicalSort and
// PhysicalTopN, some extra columns will be added into the schema of the
// Projection, thus we need to add another Projection upon them to prune the
// redundant columns.
func InjectProjBelowSort(p base.PhysicalPlan, orderByItems []*util.ByItems) base.PhysicalPlan {
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
	}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), nil)
	topProj.SetSchema(p.Schema().Clone())
	topProj.SetChildren(p)

	childPlan := p.Children()[0]
	bottomProjSchemaCols := make([]*expression.Column, 0, len(childPlan.Schema().Columns)+numOrderByItems)
	bottomProjExprs := make([]expression.Expression, 0, len(childPlan.Schema().Columns)+numOrderByItems)
	for _, col := range childPlan.Schema().Columns {
		newCol := col.Clone().(*expression.Column)
		newCol.Index = childPlan.Schema().ColumnIndex(newCol)
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
			RetType:  itemExpr.GetType(p.SCtx().GetExprCtx().GetEvalCtx()),
			Index:    len(bottomProjSchemaCols),
		}
		bottomProjSchemaCols = append(bottomProjSchemaCols, newArg)
		item.Expr = newArg
	}

	childProp := p.GetChildReqProps(0).CloneEssentialFields()
	bottomProj := PhysicalProjection{
		Exprs:                bottomProjExprs,
		AvoidColumnEvaluator: false,
	}.Init(p.SCtx(), childPlan.StatsInfo().ScaleByExpectCnt(childProp.ExpectedCnt), p.QueryBlockOffset(), childProp)
	bottomProj.SetSchema(expression.NewSchema(bottomProjSchemaCols...))
	bottomProj.SetChildren(childPlan)
	p.SetChildren(bottomProj)

	if origChildProj, isChildProj := childPlan.(*PhysicalProjection); isChildProj {
		refine4NeighbourProj(bottomProj, origChildProj)
	}
	refine4NeighbourProj(topProj, bottomProj)

	return topProj
}

// TurnNominalSortIntoProj will turn nominal sort into two projections. This is to check if the scalar functions will
// overflow.
func TurnNominalSortIntoProj(p base.PhysicalPlan, onlyColumn bool, orderByItems []*util.ByItems) base.PhysicalPlan {
	if onlyColumn {
		return p.Children()[0]
	}

	numOrderByItems := len(orderByItems)
	childPlan := p.Children()[0]

	bottomProjSchemaCols := make([]*expression.Column, 0, len(childPlan.Schema().Columns)+numOrderByItems)
	bottomProjExprs := make([]expression.Expression, 0, len(childPlan.Schema().Columns)+numOrderByItems)
	for _, col := range childPlan.Schema().Columns {
		newCol := col.Clone().(*expression.Column)
		newCol.Index = childPlan.Schema().ColumnIndex(newCol)
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
			RetType:  itemExpr.GetType(p.SCtx().GetExprCtx().GetEvalCtx()),
			Index:    len(bottomProjSchemaCols),
		}
		bottomProjSchemaCols = append(bottomProjSchemaCols, newArg)
	}

	childProp := p.GetChildReqProps(0).CloneEssentialFields()
	bottomProj := PhysicalProjection{
		Exprs:                bottomProjExprs,
		AvoidColumnEvaluator: false,
	}.Init(p.SCtx(), childPlan.StatsInfo().ScaleByExpectCnt(childProp.ExpectedCnt), p.QueryBlockOffset(), childProp)
	bottomProj.SetSchema(expression.NewSchema(bottomProjSchemaCols...))
	bottomProj.SetChildren(childPlan)

	topProjExprs := make([]expression.Expression, 0, childPlan.Schema().Len())
	for i := range childPlan.Schema().Columns {
		col := childPlan.Schema().Columns[i].Clone().(*expression.Column)
		col.Index = i
		topProjExprs = append(topProjExprs, col)
	}
	topProj := PhysicalProjection{
		Exprs:                topProjExprs,
		AvoidColumnEvaluator: false,
	}.Init(p.SCtx(), childPlan.StatsInfo().ScaleByExpectCnt(childProp.ExpectedCnt), p.QueryBlockOffset(), childProp)
	topProj.SetSchema(childPlan.Schema().Clone())
	topProj.SetChildren(bottomProj)

	if origChildProj, isChildProj := childPlan.(*PhysicalProjection); isChildProj {
		refine4NeighbourProj(bottomProj, origChildProj)
	}
	refine4NeighbourProj(topProj, bottomProj)

	return topProj
}
