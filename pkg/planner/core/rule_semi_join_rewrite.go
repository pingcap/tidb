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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	h "github.com/pingcap/tidb/pkg/util/hint"
)

// semiJoinRewriter rewrites semi join to inner join with aggregation.
// Note: This rewriter is only used for exists subquery.
// And it also requires the hint `SEMI_JOIN_REWRITE` to be set.
// For example:
//
//	select * from t where exists (select /*+ SEMI_JOIN_REWRITE() */ * from s where s.a = t.a);
//
// will be rewriten to:
//
//	select * from t join (select a from s group by a) s on t.a = s.a;
type semiJoinRewriter struct {
}

func (smj *semiJoinRewriter) optimize(_ context.Context, p base.LogicalPlan, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	newLogicalPlan, err := smj.recursivePlan(p)
	return newLogicalPlan, planChanged, err
}

func (*semiJoinRewriter) name() string {
	return "semi_join_rewrite"
}

func (smj *semiJoinRewriter) recursivePlan(p base.LogicalPlan) (base.LogicalPlan, error) {
	if _, ok := p.(*LogicalCTE); ok {
		return p, nil
	}
	newChildren := make([]base.LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := smj.recursivePlan(child)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	join, ok := p.(*LogicalJoin)
	// If it's not a join, or not a (outer) semi join. We just return it since no optimization is needed.
	// Actually the check of the preferRewriteSemiJoin is a superset of checking the join type. We remain them for a better understanding.
	if !ok || !(join.JoinType == SemiJoin || join.JoinType == LeftOuterSemiJoin) || (join.preferJoinType&h.PreferRewriteSemiJoin == 0) {
		return p, nil
	}
	// The preferRewriteSemiJoin flag only be used here. We should reset it in order to not affect other parts.
	join.preferJoinType &= ^h.PreferRewriteSemiJoin

	if join.JoinType == LeftOuterSemiJoin {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning("SEMI_JOIN_REWRITE() is inapplicable for LeftOuterSemiJoin.")
		return p, nil
	}

	// If we have jumped the above if condition. We can make sure that the current join is a non-correlated one.

	// If there's left condition or other condition, we cannot rewrite
	if len(join.LeftConditions) > 0 || len(join.OtherConditions) > 0 {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning("SEMI_JOIN_REWRITE() is inapplicable for SemiJoin with left conditions or other conditions.")
		return p, nil
	}

	innerChild := join.Children()[1]

	// If there's right conditions:
	//   - If it's semi join, then right condition should be pushed.
	//   - If it's outer semi join, then it still should be pushed since the outer join should not remain any cond of the inner side.
	// But the aggregation we added may block the predicate push down since we've not maintained the functional dependency to pass the equiv class to guide the push down.
	// So we create a selection before we build the aggregation.
	if len(join.RightConditions) > 0 {
		sel := LogicalSelection{Conditions: make([]expression.Expression, len(join.RightConditions))}.Init(p.SCtx(), innerChild.QueryBlockOffset())
		copy(sel.Conditions, join.RightConditions)
		sel.SetChildren(innerChild)
		innerChild = sel
	}

	subAgg := LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, len(join.EqualConditions)),
		GroupByItems: make([]expression.Expression, 0, len(join.EqualConditions)),
	}.Init(p.SCtx(), p.Children()[1].QueryBlockOffset())

	aggOutputCols := make([]*expression.Column, 0, len(join.EqualConditions))
	for i := range join.EqualConditions {
		innerCol := join.EqualConditions[i].GetArgs()[1].(*expression.Column)
		firstRow, err := aggregation.NewAggFuncDesc(join.SCtx().GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{innerCol}, false)
		if err != nil {
			return nil, err
		}
		subAgg.AggFuncs = append(subAgg.AggFuncs, firstRow)
		subAgg.GroupByItems = append(subAgg.GroupByItems, innerCol)
		aggOutputCols = append(aggOutputCols, innerCol)
	}
	subAgg.SetChildren(innerChild)
	subAgg.SetSchema(expression.NewSchema(aggOutputCols...))
	subAgg.buildSelfKeyInfo(subAgg.Schema())

	innerJoin := LogicalJoin{
		JoinType:        InnerJoin,
		hintInfo:        join.hintInfo,
		preferJoinType:  join.preferJoinType,
		preferJoinOrder: join.preferJoinOrder,
		EqualConditions: make([]*expression.ScalarFunction, 0, len(join.EqualConditions)),
	}.Init(p.SCtx(), p.QueryBlockOffset())
	innerJoin.SetChildren(join.Children()[0], subAgg)
	innerJoin.SetSchema(expression.MergeSchema(join.Children()[0].Schema(), subAgg.schema))
	innerJoin.AttachOnConds(expression.ScalarFuncs2Exprs(join.EqualConditions))

	proj := LogicalProjection{
		Exprs: expression.Column2Exprs(join.Children()[0].Schema().Columns),
	}.Init(p.SCtx(), p.QueryBlockOffset())
	proj.SetChildren(innerJoin)
	proj.SetSchema(join.Children()[0].Schema())

	return proj, nil
}
