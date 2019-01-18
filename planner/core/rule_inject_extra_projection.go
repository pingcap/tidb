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

// injectExtraProjection is used to extract the expressions of specific operators into a
// physical Projection operator and inject the Projection below the operators.
// Thus we can accelerate the expression evaluation by eager evaluation.
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
	}
	return plan
}

// injectProjBelowAgg injects a ProjOperator below AggOperator.
// If all the args of `aggFuncs`, and all the item of `groupByItems`
// are columns or constants, we do not need to build the `proj`.
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
