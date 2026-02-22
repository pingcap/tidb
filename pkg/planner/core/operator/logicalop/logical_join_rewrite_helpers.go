// Copyright 2024 PingCAP, Inc.
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

package logicalop

import (
	"math/bits"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	utilhint "github.com/pingcap/tidb/pkg/util/hint"
)

func (p *LogicalJoin) getProj(idx int) *LogicalProjection {
	child := p.Children()[idx]
	proj, ok := child.(*LogicalProjection)
	if ok {
		return proj
	}
	proj = LogicalProjection{Exprs: make([]expression.Expression, 0, child.Schema().Len())}.Init(p.SCtx(), child.QueryBlockOffset())
	for _, col := range child.Schema().Columns {
		proj.Exprs = append(proj.Exprs, col)
	}
	proj.SetSchema(child.Schema().Clone())
	proj.SetChildren(child)
	p.Children()[idx] = proj
	return proj
}

// outerJoinPropConst propagates constant equal and column equal conditions over outer join or anti semi join.
func (p *LogicalJoin) outerJoinPropConst(predicates []expression.Expression, vaildExprFunc expression.VaildConstantPropagationExpressionFuncType) []expression.Expression {
	children := p.Children()
	innerTable := children[1]
	outerTable := children[0]
	if p.JoinType == base.RightOuterJoin {
		innerTable, outerTable = outerTable, innerTable
	}
	lenJoinConds := len(p.EqualConditions) + len(p.LeftConditions) + len(p.RightConditions) + len(p.OtherConditions)
	joinConds := make([]expression.Expression, 0, lenJoinConds)
	for _, equalCond := range p.EqualConditions {
		joinConds = append(joinConds, equalCond)
	}
	joinConds = append(joinConds, p.LeftConditions...)
	joinConds = append(joinConds, p.RightConditions...)
	joinConds = append(joinConds, p.OtherConditions...)
	p.EqualConditions = nil
	p.LeftConditions = nil
	p.RightConditions = nil
	p.OtherConditions = nil
	nullSensitive := p.JoinType == base.AntiLeftOuterSemiJoin || p.JoinType == base.LeftOuterSemiJoin || p.JoinType == base.AntiSemiJoin
	exprCtx := p.SCtx().GetExprCtx()
	outerTableSchema := outerTable.Schema()
	innerTableSchema := innerTable.Schema()
	joinConds, predicates = expression.PropConstForOuterJoin(exprCtx, joinConds, predicates, outerTableSchema,
		innerTableSchema, p.SCtx().GetSessionVars().AlwaysKeepJoinKey, nullSensitive, vaildExprFunc)
	p.AttachOnConds(joinConds)
	return predicates
}

func mergeOnClausePredicates(p *LogicalJoin, predicates []expression.Expression) []expression.Expression {
	combinedCond := make([]expression.Expression, 0,
		len(p.LeftConditions)+len(p.RightConditions)+
			len(p.EqualConditions)+len(p.OtherConditions)+
			len(predicates))
	combinedCond = append(combinedCond, p.LeftConditions...)
	combinedCond = append(combinedCond, p.RightConditions...)
	combinedCond = append(combinedCond, expression.ScalarFuncs2Exprs(p.EqualConditions)...)
	combinedCond = append(combinedCond, p.OtherConditions...)
	combinedCond = append(combinedCond, predicates...)
	return combinedCond
}

// SemiJoinRewrite rewrites semi join to inner join with aggregation.
// Note: This rewriter is only used for exists subquery.
// And it also requires the hint `SEMI_JOIN_REWRITE` or variable tidb_opt_enable_sem_join_rewrite
// to be set.
// For example:
//
//	select * from t where exists (select /*+ SEMI_JOIN_REWRITE() */ * from s where s.a = t.a);
//
// will be rewriten to:
//
//	select * from t join (select a from s group by a) s on t.a = s.a;
func (p *LogicalJoin) SemiJoinRewrite() (base.LogicalPlan, error) {
	// If it's not a join, or not a (outer) semi join. We just return it since no optimization is needed.
	// Actually the check of the preferRewriteSemiJoin is a superset of checking the join type. We remain them for a better understanding.
	if !(p.JoinType == base.SemiJoin || p.JoinType == base.LeftOuterSemiJoin) {
		return p.Self(), nil
	}
	if _, ok := p.Self().(*LogicalApply); ok {
		return p.Self(), nil
	}
	// Get by hint or session variable.
	if (p.PreferJoinType&utilhint.PreferRewriteSemiJoin) == 0 && !p.SCtx().GetSessionVars().EnableSemiJoinRewrite {
		return p.Self(), nil
	}
	// The preferRewriteSemiJoin flag only be used here. We should reset it in order to not affect other parts.
	p.PreferJoinType &= ^utilhint.PreferRewriteSemiJoin

	if p.JoinType == base.LeftOuterSemiJoin {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning("SEMI_JOIN_REWRITE() is inapplicable for LeftOuterSemiJoin.")
		return p.Self(), nil
	}

	// If we have jumped the above if condition. We can make sure that the current join is a non-correlated one.

	// If there's left condition or other condition, we cannot rewrite
	if len(p.LeftConditions) > 0 || len(p.OtherConditions) > 0 {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning("SEMI_JOIN_REWRITE() is inapplicable for SemiJoin with left conditions or other conditions.")
		return p.Self(), nil
	}

	innerChild := p.Children()[1]

	// If there's right conditions:
	//   - If it's semi join, then right condition should be pushed.
	//   - If it's outer semi join, then it still should be pushed since the outer join should not remain any cond of the inner side.
	// But the aggregation we added may block the predicate push down since we've not maintained the functional dependency to pass the equiv class to guide the push down.
	// So we create a selection before we build the aggregation.
	if len(p.RightConditions) > 0 {
		sel := LogicalSelection{Conditions: make([]expression.Expression, len(p.RightConditions))}.Init(p.SCtx(), innerChild.QueryBlockOffset())
		copy(sel.Conditions, p.RightConditions)
		sel.SetChildren(innerChild)
		innerChild = sel
	}

	subAgg := LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, len(p.EqualConditions)),
		GroupByItems: make([]expression.Expression, 0, len(p.EqualConditions)),
	}.Init(p.SCtx(), p.Children()[1].QueryBlockOffset())

	aggOutputCols := make([]*expression.Column, 0, len(p.EqualConditions))
	for i := range p.EqualConditions {
		innerCol := p.EqualConditions[i].GetArgs()[1].(*expression.Column)
		firstRow, err := aggregation.NewAggFuncDesc(p.SCtx().GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{innerCol}, false)
		if err != nil {
			return nil, err
		}
		subAgg.AggFuncs = append(subAgg.AggFuncs, firstRow)
		subAgg.GroupByItems = append(subAgg.GroupByItems, innerCol)
		aggOutputCols = append(aggOutputCols, innerCol)
	}
	subAgg.SetChildren(innerChild)
	subAgg.SetSchema(expression.NewSchema(aggOutputCols...))
	subAgg.BuildSelfKeyInfo(subAgg.Schema())
	innerJoin := LogicalJoin{
		JoinType:        base.InnerJoin,
		HintInfo:        p.HintInfo,
		PreferJoinType:  p.PreferJoinType,
		PreferJoinOrder: p.PreferJoinOrder,
		EqualConditions: make([]*expression.ScalarFunction, 0, len(p.EqualConditions)),
	}.Init(p.SCtx(), p.QueryBlockOffset())
	innerJoin.SetChildren(p.Children()[0], subAgg)
	innerJoin.SetSchema(expression.MergeSchema(p.Children()[0].Schema().Clone(), subAgg.Schema().Clone()))
	innerJoin.AttachOnConds(expression.ScalarFuncs2Exprs(p.EqualConditions))
	proj := LogicalProjection{
		Exprs: expression.Column2Exprs(p.Children()[0].Schema().Columns),
	}.Init(p.SCtx(), p.QueryBlockOffset())
	proj.SetChildren(innerJoin)
	proj.SetSchema(p.Children()[0].Schema().Clone())
	return proj, nil
}

// containDifferentJoinTypes checks whether `PreferJoinType` contains different
// join types.
func containDifferentJoinTypes(preferJoinType uint) bool {
	preferJoinType &= ^utilhint.PreferNoHashJoin
	preferJoinType &= ^utilhint.PreferNoMergeJoin
	preferJoinType &= ^utilhint.PreferNoIndexJoin
	preferJoinType &= ^utilhint.PreferNoIndexHashJoin
	preferJoinType &= ^utilhint.PreferNoIndexMergeJoin

	inlMask := utilhint.PreferRightAsINLJInner ^ utilhint.PreferLeftAsINLJInner
	inlhjMask := utilhint.PreferRightAsINLHJInner ^ utilhint.PreferLeftAsINLHJInner
	inlmjMask := utilhint.PreferRightAsINLMJInner ^ utilhint.PreferLeftAsINLMJInner
	hjRightBuildMask := utilhint.PreferRightAsHJBuild ^ utilhint.PreferLeftAsHJProbe
	hjLeftBuildMask := utilhint.PreferLeftAsHJBuild ^ utilhint.PreferRightAsHJProbe

	mppMask := utilhint.PreferShuffleJoin ^ utilhint.PreferBCJoin
	mask := inlMask ^ inlhjMask ^ inlmjMask ^ hjRightBuildMask ^ hjLeftBuildMask
	onesCount := bits.OnesCount(preferJoinType & ^mask & ^mppMask)
	if onesCount > 1 || onesCount == 1 && preferJoinType&mask > 0 {
		return true
	}

	cnt := 0
	if preferJoinType&inlMask > 0 {
		cnt++
	}
	if preferJoinType&inlhjMask > 0 {
		cnt++
	}
	if preferJoinType&inlmjMask > 0 {
		cnt++
	}
	if preferJoinType&hjLeftBuildMask > 0 {
		cnt++
	}
	if preferJoinType&hjRightBuildMask > 0 {
		cnt++
	}
	return cnt > 1
}

func setPreferredJoinTypeFromOneSide(preferJoinType uint, isLeft bool) (resJoinType uint) {
	if preferJoinType == 0 {
		return
	}
	if preferJoinType&utilhint.PreferINLJ > 0 {
		preferJoinType &= ^utilhint.PreferINLJ
		if isLeft {
			resJoinType |= utilhint.PreferLeftAsINLJInner
		} else {
			resJoinType |= utilhint.PreferRightAsINLJInner
		}
	}
	if preferJoinType&utilhint.PreferINLHJ > 0 {
		preferJoinType &= ^utilhint.PreferINLHJ
		if isLeft {
			resJoinType |= utilhint.PreferLeftAsINLHJInner
		} else {
			resJoinType |= utilhint.PreferRightAsINLHJInner
		}
	}
	if preferJoinType&utilhint.PreferINLMJ > 0 {
		preferJoinType &= ^utilhint.PreferINLMJ
		if isLeft {
			resJoinType |= utilhint.PreferLeftAsINLMJInner
		} else {
			resJoinType |= utilhint.PreferRightAsINLMJInner
		}
	}
	if preferJoinType&utilhint.PreferHJBuild > 0 {
		preferJoinType &= ^utilhint.PreferHJBuild
		if isLeft {
			resJoinType |= utilhint.PreferLeftAsHJBuild
		} else {
			resJoinType |= utilhint.PreferRightAsHJBuild
		}
	}
	if preferJoinType&utilhint.PreferHJProbe > 0 {
		preferJoinType &= ^utilhint.PreferHJProbe
		if isLeft {
			resJoinType |= utilhint.PreferLeftAsHJProbe
		} else {
			resJoinType |= utilhint.PreferRightAsHJProbe
		}
	}
	resJoinType |= preferJoinType
	return
}

// DeriveOtherConditions given a LogicalJoin, check the OtherConditions to see if we can derive more
// conditions for left/right child pushdown.
func DeriveOtherConditions(
	p *LogicalJoin, leftSchema *expression.Schema, rightSchema *expression.Schema,
	deriveLeft bool, deriveRight bool) (
	leftCond []expression.Expression, rightCond []expression.Expression) {
	isOuterSemi := (p.JoinType == base.LeftOuterSemiJoin) || (p.JoinType == base.AntiLeftOuterSemiJoin)
	ctx := p.SCtx()
	exprCtx := ctx.GetExprCtx()
	for _, expr := range p.OtherConditions {
		if deriveLeft {
			leftRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(exprCtx, expr, leftSchema)
			if leftRelaxedCond != nil {
				leftCond = append(leftCond, leftRelaxedCond)
			}
			notNullExpr := deriveNotNullExpr(ctx, expr, leftSchema)
			if notNullExpr != nil {
				leftCond = append(leftCond, notNullExpr)
			}
		}
		if deriveRight {
			rightRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(exprCtx, expr, rightSchema)
			if rightRelaxedCond != nil {
				rightCond = append(rightCond, rightRelaxedCond)
			}
			// For LeftOuterSemiJoin and AntiLeftOuterSemiJoin, we can actually generate
			// `col is not null` according to expressions in `OtherConditions` now, but we
			// are putting column equal condition converted from `in (subq)` into
			// `OtherConditions`(@sa https://github.com/pingcap/tidb/pull/9051), then it would
			// cause wrong results, so we disable this optimization for outer semi joins now.
			// TODO enable this optimization for outer semi joins later by checking whether
			// condition in `OtherConditions` is converted from `in (subq)`.
			if isOuterSemi {
				continue
			}
			notNullExpr := deriveNotNullExpr(ctx, expr, rightSchema)
			if notNullExpr != nil {
				rightCond = append(rightCond, notNullExpr)
			}
		}
	}
	return
}

// deriveNotNullExpr generates a new expression `not(isnull(col))` given `col1 op col2`,
// in which `col` is in specified schema. Caller guarantees that only one of `col1` or
// `col2` is in schema.
func deriveNotNullExpr(ctx base.PlanContext, expr expression.Expression, schema *expression.Schema) expression.Expression {
	binop, ok := expr.(*expression.ScalarFunction)
	if !ok || len(binop.GetArgs()) != 2 {
		return nil
	}
	arg0, arg1, ok := expression.IsColOpCol(binop)
	if !ok {
		return nil
	}
	childCol := schema.RetrieveColumn(arg0)
	if childCol == nil {
		childCol = schema.RetrieveColumn(arg1)
	}
	if util.IsNullRejected(ctx, schema, expr, true) && !mysql.HasNotNullFlag(childCol.RetType.GetFlag()) {
		return expression.BuildNotNullExpr(ctx.GetExprCtx(), childCol)
	}
	return nil
}

// BuildLogicalJoinSchema builds the schema for join operator.
func BuildLogicalJoinSchema(joinType base.JoinType, join base.LogicalPlan) *expression.Schema {
	leftSchema := join.Children()[0].Schema()
	switch joinType {
	case base.SemiJoin, base.AntiSemiJoin:
		return leftSchema.Clone()
	case base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin:
		newSchema := leftSchema.Clone()
		newSchema.Append(join.Schema().Columns[join.Schema().Len()-1])
		return newSchema
	}
	newSchema := expression.MergeSchema(leftSchema, join.Children()[1].Schema())
	if joinType == base.LeftOuterJoin {
		util.ResetNotNullFlag(newSchema, leftSchema.Len(), newSchema.Len())
	} else if joinType == base.RightOuterJoin {
		util.ResetNotNullFlag(newSchema, 0, leftSchema.Len())
	}
	return newSchema
}
