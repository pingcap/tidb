// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser/ast"
)

type semiJoinRewriter struct {
}

func (smj *semiJoinRewriter) optimize(_ context.Context, p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	return smj.recursivePlan(p)
}

func (smj *semiJoinRewriter) name() string {
	return "semi_join_rewrite"
}

func (smj *semiJoinRewriter) recursivePlan(p LogicalPlan) (LogicalPlan, error) {
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
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
	if !ok || !(join.JoinType == SemiJoin || join.JoinType == LeftOuterSemiJoin) || (join.preferJoinType&preferRewriteSemiJoin == 0) {
		return p, nil
	}

	if join.JoinType == LeftOuterSemiJoin {
		p.SCtx().GetSessionVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack("The EXISTS/IN subquery is too complicated to apply the SEMI_JOIN_REWRITE hint."))
		return p, nil
	}

	// If we have jumped the above if condition. We can make sure that the current join is a non-correlated one.

	// If there's left condition or other condition, we cannot rewrite
	if len(join.LeftConditions) > 0 || len(join.OtherConditions) > 0 {
		p.SCtx().GetSessionVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack("The EXISTS/IN subquery is too complicated to apply the SEMI_JOIN_REWRITE hint."))
		return p, nil
	}

	innerChild := join.Children()[1]

	subAgg := LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, len(join.EqualConditions)),
		GroupByItems: make([]expression.Expression, 0, len(join.EqualConditions)),
	}.Init(p.SCtx(), p.Children()[1].SelectBlockOffset())

	aggOutputCols := make([]*expression.Column, 0, len(join.EqualConditions))
	for i := range join.EqualConditions {
		innerCol := join.EqualConditions[i].GetArgs()[1].(*expression.Column)
		firstRow, err := aggregation.NewAggFuncDesc(join.SCtx(), ast.AggFuncFirstRow, []expression.Expression{innerCol}, false)
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
		EqualConditions: make([]*expression.ScalarFunction, 0, len(join.EqualConditions)),
	}.Init(p.SCtx(), p.SelectBlockOffset())
	innerJoin.SetChildren(join.Children()[0], subAgg)
	innerJoin.SetSchema(expression.MergeSchema(join.Children()[0].Schema(), subAgg.schema))
	innerJoin.AttachOnConds(expression.ScalarFuncs2Exprs(join.EqualConditions))

	proj := LogicalProjection{
		Exprs: expression.Column2Exprs(join.Children()[0].Schema().Columns),
	}.Init(p.SCtx(), p.SelectBlockOffset())
	proj.SetChildren(innerJoin)
	proj.SetSchema(join.Children()[0].Schema())

	return proj, nil
}
