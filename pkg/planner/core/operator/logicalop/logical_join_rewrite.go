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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	utilhint "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
)

func (p *LogicalJoin) getGroupNDVs(childStats []*property.StatsInfo) []property.GroupNDV {
	outerIdx := int(-1)
	if p.JoinType == base.LeftOuterJoin || p.JoinType == base.LeftOuterSemiJoin || p.JoinType == base.AntiLeftOuterSemiJoin {
		outerIdx = 0
	} else if p.JoinType == base.RightOuterJoin {
		outerIdx = 1
	}
	if outerIdx >= 0 {
		return childStats[outerIdx].GroupNDVs
	}
	return nil
}

// PreferAny checks whether the join type is in the joinFlags.
func (p *LogicalJoin) PreferAny(joinFlags ...uint) bool {
	for _, flag := range joinFlags {
		if p.PreferJoinType&flag > 0 {
			return true
		}
	}
	return false
}

// This function is only used with inner join and semi join.
func (p *LogicalJoin) isVaildConstantPropagationExpressionWithInnerJoinOrSemiJoin(expr expression.Expression) bool {
	return p.isVaildConstantPropagationExpression(expr, true, true, true, true)
}

// This function is only used in LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin, AntiSemiJoin
func (p *LogicalJoin) isVaildConstantPropagationExpressionForLeftOuterJoinAndAntiSemiJoin(expr expression.Expression) bool {
	return p.isVaildConstantPropagationExpression(expr, false, false, false, true)
}

// This function is only used in RightOuterJoin
func (p *LogicalJoin) isVaildConstantPropagationExpressionForRightOuterJoin(expr expression.Expression) bool {
	return p.isVaildConstantPropagationExpression(expr, false, false, true, false)
}

// isVaildConstantPropagationExpression is to judge whether the expression is created by PropagationContant is vaild.
//
// Some expressions are not suitable for constant propagation. After constant propagation,
// these expressions will only become a projection, increasing the computational load without
// being able to filter data directly from the data source.
//
// `deriveLeft` and `driveRight` are used in conjunction with `extractOnCondition`.
//
// `canLeftPushDown` and `canRightPushDown` are used to mark that for some joins,
// the left or right condition will not be pushed down. For these conditions that cannot be pushed down,
// we can reject the new expressions from constant propagation.
func (p *LogicalJoin) isVaildConstantPropagationExpression(cond expression.Expression, deriveLeft, deriveRight, canLeftPushDown, canRightPushDown bool) bool {
	_, leftCond, rightCond, otherCond := p.extractOnCondition([]expression.Expression{cond}, deriveLeft, deriveRight)
	if len(otherCond) > 0 {
		// a new expression which is created by constant propagation, is a other condtion, we don't put it
		// into our final result.
		return false
	}
	intest.Assert(len(leftCond) == 0 || len(rightCond) == 0, "An expression cannot be both a left and a right condition at the same time.")
	// When the expression is a left/right condition, we want it to filter more of the underlying data.
	if len(leftCond) > 0 {
		// If this expression's columns is in the same table. We will push it down.
		if canLeftPushDown && p.isAllUniqueIDInTheSameLeaf(cond) {
			return true
		}
		return false
	}
	if len(rightCond) > 0 {
		// If this expression's columns is in the same table. We will push it down.
		if canRightPushDown && p.isAllUniqueIDInTheSameLeaf(cond) {
			return true
		}
		return false
	}
	return true
}

// ExtractOnCondition divide conditions in CNF of join node into 4 groups.
// These conditions can be where conditions, join conditions, or collection of both.
// If deriveLeft/deriveRight is set, we would try to derive more conditions for left/right plan.
func (p *LogicalJoin) ExtractOnCondition(
	conditions []expression.Expression,
	leftSchema *expression.Schema,
	rightSchema *expression.Schema,
	deriveLeft bool,
	deriveRight bool) (eqCond []*expression.ScalarFunction, leftCond []expression.Expression,
	rightCond []expression.Expression, otherCond []expression.Expression) {
	ctx := p.SCtx()
	for _, expr := range conditions {
		// For queries like `select a in (select a from s where s.b = t.b) from t`,
		// if subquery is empty caused by `s.b = t.b`, the result should always be
		// false even if t.a is null or s.a is null. To make this join "empty aware",
		// we should differentiate `t.a = s.a` from other column equal conditions, so
		// we put it into OtherConditions instead of EqualConditions of join.
		if expression.IsEQCondFromIn(expr) {
			otherCond = append(otherCond, expr)
			continue
		}
		binop, ok := expr.(*expression.ScalarFunction)
		if ok && len(binop.GetArgs()) == 2 {
			arg0, arg1, ok := expression.IsColOpCol(binop)
			if ok {
				leftCol := leftSchema.RetrieveColumn(arg0)
				rightCol := rightSchema.RetrieveColumn(arg1)
				if leftCol == nil || rightCol == nil {
					leftCol = leftSchema.RetrieveColumn(arg1)
					rightCol = rightSchema.RetrieveColumn(arg0)
					arg0, arg1 = arg1, arg0
				}
				if leftCol != nil && rightCol != nil {
					if deriveLeft {
						if util.IsNullRejected(ctx, leftSchema, expr, true) && !mysql.HasNotNullFlag(leftCol.RetType.GetFlag()) {
							notNullExpr := expression.BuildNotNullExpr(ctx.GetExprCtx(), leftCol)
							leftCond = append(leftCond, notNullExpr)
						}
					}
					if deriveRight {
						if util.IsNullRejected(ctx, rightSchema, expr, true) && !mysql.HasNotNullFlag(rightCol.RetType.GetFlag()) {
							notNullExpr := expression.BuildNotNullExpr(ctx.GetExprCtx(), rightCol)
							rightCond = append(rightCond, notNullExpr)
						}
					}
					switch binop.FuncName.L {
					case ast.EQ, ast.NullEQ:
						cond := expression.NewFunctionInternal(ctx.GetExprCtx(), binop.FuncName.L, types.NewFieldType(mysql.TypeTiny), arg0, arg1)
						eqCond = append(eqCond, cond.(*expression.ScalarFunction))
						continue
					}
				}
			}
		}
		columns := expression.ExtractColumns(expr)
		// `columns` may be empty, if the condition is like `correlated_column op constant`, or `constant`,
		// push this kind of constant condition down according to join type.
		if len(columns) == 0 {
			// The IsMutableEffectsExpr check is primarily designed to prevent mutable expressions
			// like rand() > 0.5 from being pushed down; instead, such expressions should remain
			// in other conditions.
			// Checking len(columns) == 0 first is to let filter like rand() > tbl.col
			// to be able pushdown as left or right condition
			if expression.IsMutableEffectsExpr(expr) {
				otherCond = append(otherCond, expr)
				continue
			}
			leftCond, rightCond = p.pushDownConstExpr(expr, leftCond, rightCond, deriveLeft || deriveRight)
			continue
		}
		allFromLeft, allFromRight := true, true
		for _, col := range columns {
			if !leftSchema.Contains(col) {
				allFromLeft = false
			}
			if !rightSchema.Contains(col) {
				allFromRight = false
			}
		}
		if allFromRight {
			rightCond = append(rightCond, expr)
		} else if allFromLeft {
			leftCond = append(leftCond, expr)
		} else {
			// Relax expr to two supersets: leftRelaxedCond and rightRelaxedCond, the expression now is
			// `expr AND leftRelaxedCond AND rightRelaxedCond`. Motivation is to push filters down to
			// children as much as possible.
			if deriveLeft {
				leftRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(ctx.GetExprCtx(), expr, leftSchema)
				if leftRelaxedCond != nil {
					leftCond = append(leftCond, leftRelaxedCond)
				}
			}
			if deriveRight {
				rightRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(ctx.GetExprCtx(), expr, rightSchema)
				if rightRelaxedCond != nil {
					rightCond = append(rightCond, rightRelaxedCond)
				}
			}
			otherCond = append(otherCond, expr)
		}
	}
	return
}

// pushDownConstExpr checks if the condition is from filter condition, if true, push it down to both
// children of join, whatever the join type is; if false, push it down to inner child of outer join,
// and both children of non-outer-join.
func (p *LogicalJoin) pushDownConstExpr(expr expression.Expression, leftCond []expression.Expression,
	rightCond []expression.Expression, filterCond bool) (_, _ []expression.Expression) {
	switch p.JoinType {
	case base.LeftOuterJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin:
		if filterCond {
			leftCond = append(leftCond, expr)
			// Append the expr to right join condition instead of `rightCond`, to make it able to be
			// pushed down to children of join.
			p.RightConditions = append(p.RightConditions, expr)
		} else {
			rightCond = append(rightCond, expr)
		}
	case base.RightOuterJoin:
		if filterCond {
			rightCond = append(rightCond, expr)
			p.LeftConditions = append(p.LeftConditions, expr)
		} else {
			leftCond = append(leftCond, expr)
		}
	case base.SemiJoin, base.InnerJoin:
		leftCond = append(leftCond, expr)
		rightCond = append(rightCond, expr)
	case base.AntiSemiJoin:
		if filterCond {
			leftCond = append(leftCond, expr)
		}
		rightCond = append(rightCond, expr)
	}
	return leftCond, rightCond
}

func (p *LogicalJoin) extractOnCondition(conditions []expression.Expression, deriveLeft bool,
	deriveRight bool) (eqCond []*expression.ScalarFunction, leftCond []expression.Expression,
	rightCond []expression.Expression, otherCond []expression.Expression) {
	child := p.Children()
	rightSchema := child[1].Schema()
	leftSchema := child[0].Schema()
	return p.ExtractOnCondition(conditions, leftSchema, rightSchema, deriveLeft, deriveRight)
}

// SetPreferredJoinTypeAndOrder sets the preferred join type and order for the LogicalJoin.
func (p *LogicalJoin) SetPreferredJoinTypeAndOrder(hintInfo *utilhint.PlanHints) {
	if hintInfo == nil {
		return
	}

	lhsAlias := util.ExtractTableAlias(p.Children()[0], p.QueryBlockOffset())
	rhsAlias := util.ExtractTableAlias(p.Children()[1], p.QueryBlockOffset())
	if hintInfo.IfPreferMergeJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferMergeJoin
		p.LeftPreferJoinType |= utilhint.PreferMergeJoin
	}
	if hintInfo.IfPreferMergeJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferMergeJoin
		p.RightPreferJoinType |= utilhint.PreferMergeJoin
	}
	if hintInfo.IfPreferNoMergeJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoMergeJoin
		p.LeftPreferJoinType |= utilhint.PreferNoMergeJoin
	}
	if hintInfo.IfPreferNoMergeJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoMergeJoin
		p.RightPreferJoinType |= utilhint.PreferNoMergeJoin
	}
	if hintInfo.IfPreferBroadcastJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferBCJoin
		p.LeftPreferJoinType |= utilhint.PreferBCJoin
	}
	if hintInfo.IfPreferBroadcastJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferBCJoin
		p.RightPreferJoinType |= utilhint.PreferBCJoin
	}
	if hintInfo.IfPreferShuffleJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferShuffleJoin
		p.LeftPreferJoinType |= utilhint.PreferShuffleJoin
	}
	if hintInfo.IfPreferShuffleJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferShuffleJoin
		p.RightPreferJoinType |= utilhint.PreferShuffleJoin
	}
	if hintInfo.IfPreferHashJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferHashJoin
		p.LeftPreferJoinType |= utilhint.PreferHashJoin
	}
	if hintInfo.IfPreferHashJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferHashJoin
		p.RightPreferJoinType |= utilhint.PreferHashJoin
	}
	if hintInfo.IfPreferNoHashJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoHashJoin
		p.LeftPreferJoinType |= utilhint.PreferNoHashJoin
	}
	if hintInfo.IfPreferNoHashJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoHashJoin
		p.RightPreferJoinType |= utilhint.PreferNoHashJoin
	}
	if hintInfo.IfPreferINLJ(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferLeftAsINLJInner
		p.LeftPreferJoinType |= utilhint.PreferINLJ
	}
	if hintInfo.IfPreferINLJ(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferRightAsINLJInner
		p.RightPreferJoinType |= utilhint.PreferINLJ
	}
	if hintInfo.IfPreferINLHJ(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferLeftAsINLHJInner
		p.LeftPreferJoinType |= utilhint.PreferINLHJ
	}
	if hintInfo.IfPreferINLHJ(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferRightAsINLHJInner
		p.RightPreferJoinType |= utilhint.PreferINLHJ
	}
	if hintInfo.IfPreferINLMJ(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferLeftAsINLMJInner
		p.LeftPreferJoinType |= utilhint.PreferINLMJ
	}
	if hintInfo.IfPreferINLMJ(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferRightAsINLMJInner
		p.RightPreferJoinType |= utilhint.PreferINLMJ
	}
	if hintInfo.IfPreferNoIndexJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoIndexJoin
		p.LeftPreferJoinType |= utilhint.PreferNoIndexJoin
	}
	if hintInfo.IfPreferNoIndexJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoIndexJoin
		p.RightPreferJoinType |= utilhint.PreferNoIndexJoin
	}
	if hintInfo.IfPreferNoIndexHashJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoIndexHashJoin
		p.LeftPreferJoinType |= utilhint.PreferNoIndexHashJoin
	}
	if hintInfo.IfPreferNoIndexHashJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoIndexHashJoin
		p.RightPreferJoinType |= utilhint.PreferNoIndexHashJoin
	}
	if hintInfo.IfPreferNoIndexMergeJoin(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoIndexMergeJoin
		p.LeftPreferJoinType |= utilhint.PreferNoIndexMergeJoin
	}
	if hintInfo.IfPreferNoIndexMergeJoin(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferNoIndexMergeJoin
		p.RightPreferJoinType |= utilhint.PreferNoIndexMergeJoin
	}
	if hintInfo.IfPreferHJBuild(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferLeftAsHJBuild
		p.LeftPreferJoinType |= utilhint.PreferHJBuild
	}
	if hintInfo.IfPreferHJBuild(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferRightAsHJBuild
		p.RightPreferJoinType |= utilhint.PreferHJBuild
	}
	if hintInfo.IfPreferHJProbe(lhsAlias) {
		p.PreferJoinType |= utilhint.PreferLeftAsHJProbe
		p.LeftPreferJoinType |= utilhint.PreferHJProbe
	}
	if hintInfo.IfPreferHJProbe(rhsAlias) {
		p.PreferJoinType |= utilhint.PreferRightAsHJProbe
		p.RightPreferJoinType |= utilhint.PreferHJProbe
	}
	hasConflict := false
	if !p.SCtx().GetSessionVars().EnableAdvancedJoinHint || p.SCtx().GetSessionVars().StmtCtx.StraightJoinOrder {
		if containDifferentJoinTypes(p.PreferJoinType) {
			hasConflict = true
		}
	} else if p.SCtx().GetSessionVars().EnableAdvancedJoinHint {
		if containDifferentJoinTypes(p.LeftPreferJoinType) || containDifferentJoinTypes(p.RightPreferJoinType) {
			hasConflict = true
		}
	}
	if hasConflict {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
			"Join hints are conflict, you can only specify one type of join")
		p.PreferJoinType = 0
	}
	// set the join order
	if hintInfo.LeadingJoinOrder != nil {
		p.PreferJoinOrder = hintInfo.MatchTableName([]*utilhint.HintedTable{lhsAlias, rhsAlias}, hintInfo.LeadingJoinOrder)
	}
	// set hintInfo for further usage if this hint info can be used.
	if p.PreferJoinType != 0 || p.PreferJoinOrder {
		p.HintInfo = hintInfo
	}
}

// SetPreferredJoinType generates hint information for the logicalJoin based on the hint information of its left and right children.
func (p *LogicalJoin) SetPreferredJoinType() {
	if p.LeftPreferJoinType == 0 && p.RightPreferJoinType == 0 {
		return
	}
	p.PreferJoinType = setPreferredJoinTypeFromOneSide(p.LeftPreferJoinType, true) | setPreferredJoinTypeFromOneSide(p.RightPreferJoinType, false)
	if containDifferentJoinTypes(p.PreferJoinType) {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
			"Join hints conflict after join reorder phase, you can only specify one type of join")
		p.PreferJoinType = 0
	}
}

// updateEQCond will extract the arguments of a equal condition that connect two expressions.
func (p *LogicalJoin) updateEQCond() {
	lChild, rChild := p.Children()[0], p.Children()[1]
	var lKeys, rKeys []expression.Expression
	var lNAKeys, rNAKeys []expression.Expression
	// We need two steps here:
	// step1: try best to extract normal EQ condition from OtherCondition to join EqualConditions.
	for i := len(p.OtherConditions) - 1; i >= 0; i-- {
		need2Remove := false
		if eqCond, ok := p.OtherConditions[i].(*expression.ScalarFunction); ok && eqCond.FuncName.L == ast.EQ {
			// If it is a column equal condition converted from `[not] in (subq)`, do not move it
			// to EqualConditions, and keep it in OtherConditions. Reference comments in `extractOnCondition`
			// for detailed reasons.
			if expression.IsEQCondFromIn(eqCond) {
				continue
			}
			lExpr, rExpr := eqCond.GetArgs()[0], eqCond.GetArgs()[1]
			if expression.ExprFromSchema(lExpr, lChild.Schema()) && expression.ExprFromSchema(rExpr, rChild.Schema()) {
				lKeys = append(lKeys, lExpr)
				rKeys = append(rKeys, rExpr)
				need2Remove = true
			} else if expression.ExprFromSchema(lExpr, rChild.Schema()) && expression.ExprFromSchema(rExpr, lChild.Schema()) {
				lKeys = append(lKeys, rExpr)
				rKeys = append(rKeys, lExpr)
				need2Remove = true
			}
		}
		if need2Remove {
			p.OtherConditions = slices.Delete(p.OtherConditions, i, i+1)
		}
	}
	// eg: explain select * from t1, t3 where t1.a+1 = t3.a;
	// tidb only accept the join key in EqualCondition as a normal column (join OP take granted for that)
	// so once we found the left and right children's schema can supply the all columns in complicated EQ condition that used by left/right key.
	// we will add a layer of projection here to convert the complicated expression of EQ's left or right side to be a normal column.
	adjustKeyForm := func(leftKeys, rightKeys []expression.Expression, isNA bool) {
		if len(leftKeys) > 0 {
			needLProj, needRProj := false, false
			for i := range leftKeys {
				_, lOk := leftKeys[i].(*expression.Column)
				_, rOk := rightKeys[i].(*expression.Column)
				needLProj = needLProj || !lOk
				needRProj = needRProj || !rOk
			}

			var lProj, rProj *LogicalProjection
			if needLProj {
				lProj = p.getProj(0)
			}
			if needRProj {
				rProj = p.getProj(1)
			}
			for i := range leftKeys {
				lKey, rKey := leftKeys[i], rightKeys[i]
				if lProj != nil {
					lKey = lProj.AppendExpr(lKey)
				}
				if rProj != nil {
					rKey = rProj.AppendExpr(rKey)
				}
				eqCond := expression.NewFunctionInternal(p.SCtx().GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), lKey, rKey)
				if isNA {
					p.NAEQConditions = append(p.NAEQConditions, eqCond.(*expression.ScalarFunction))
				} else {
					p.EqualConditions = append(p.EqualConditions, eqCond.(*expression.ScalarFunction))
				}
			}
		}
	}
	adjustKeyForm(lKeys, rKeys, false)

	// Step2: when step1 is finished, then we can determine whether we need to extract NA-EQ from OtherCondition to NAEQConditions.
	// when there are still no EqualConditions, let's try to be a NAAJ.
	// todo: by now, when there is already a normal EQ condition, just keep NA-EQ as other-condition filters above it.
	// eg: select * from stu where stu.name not in (select name from exam where exam.stu_id = stu.id);
	// combination of <stu.name NAEQ exam.name> and <exam.stu_id EQ stu.id> for join key is little complicated for now.
	canBeNAAJ := (p.JoinType == base.AntiSemiJoin || p.JoinType == base.AntiLeftOuterSemiJoin) && len(p.EqualConditions) == 0
	if canBeNAAJ && p.SCtx().GetSessionVars().OptimizerEnableNAAJ {
		var otherCond expression.CNFExprs
		for i := range p.OtherConditions {
			eqCond, ok := p.OtherConditions[i].(*expression.ScalarFunction)
			if ok && eqCond.FuncName.L == ast.EQ && expression.IsEQCondFromIn(eqCond) {
				// here must be a EQCondFromIn.
				lExpr, rExpr := eqCond.GetArgs()[0], eqCond.GetArgs()[1]
				if expression.ExprFromSchema(lExpr, lChild.Schema()) && expression.ExprFromSchema(rExpr, rChild.Schema()) {
					lNAKeys = append(lNAKeys, lExpr)
					rNAKeys = append(rNAKeys, rExpr)
				} else if expression.ExprFromSchema(lExpr, rChild.Schema()) && expression.ExprFromSchema(rExpr, lChild.Schema()) {
					lNAKeys = append(lNAKeys, rExpr)
					rNAKeys = append(rNAKeys, lExpr)
				}
				continue
			}
			otherCond = append(otherCond, p.OtherConditions[i])
		}
		p.OtherConditions = otherCond
		// here is for cases like: select (a+1, b*3) not in (select a,b from t2) from t1.
		adjustKeyForm(lNAKeys, rNAKeys, true)
	}
}

