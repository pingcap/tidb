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

package old

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/memo"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intset"
)

type outerJoinEliminator struct {
}

func (*outerJoinEliminator) prepareForEliminateOuterJoin(joinExpr *memo.GroupExpr) (ok bool, innerChildIdx int, outerGroup *memo.Group, innerGroup *memo.Group, outerUniqueIDs intset.FastIntSet) {
	join := joinExpr.ExprNode.(*logicalop.LogicalJoin)

	switch join.JoinType {
	case base.LeftOuterJoin:
		innerChildIdx = 1
	case base.RightOuterJoin:
		innerChildIdx = 0
	default:
		ok = false
		return
	}
	outerGroup = joinExpr.Children[1^innerChildIdx]
	innerGroup = joinExpr.Children[innerChildIdx]

	outerUniqueIDs = intset.NewFastIntSet()
	for _, outerCol := range outerGroup.Prop.Schema.Columns {
		outerUniqueIDs.Insert(int(outerCol.UniqueID))
	}

	ok = true
	return
}

// check whether one of unique keys sets is contained by inner join keys.
func (*outerJoinEliminator) isInnerJoinKeysContainUniqueKey(innerGroup *memo.Group, joinKeys *expression.Schema, innerNullEQKeys intset.FastIntSet) (bool, error) {
	// builds UniqueKey info of innerGroup.
	innerGroup.BuildKeyInfo()
	for _, keyInfo := range innerGroup.Prop.Schema.PKOrUK {
		joinKeysContainKeyInfo := true
		for _, col := range keyInfo {
			if !joinKeys.Contains(col) {
				joinKeysContainKeyInfo = false
				break
			}
		}
		if joinKeysContainKeyInfo {
			return true, nil
		}
	}
	for _, keyInfo := range innerGroup.Prop.Schema.NullableUK {
		joinKeysContainKeyInfo := true
		for _, col := range keyInfo {
			if !joinKeys.Contains(col) {
				joinKeysContainKeyInfo = false
				break
			}
			if innerNullEQKeys.Has(int(col.UniqueID)) {
				joinKeysContainKeyInfo = false
				break
			}
		}
		if joinKeysContainKeyInfo {
			return true, nil
		}
	}
	return false, nil
}

// EliminateOuterJoinBelowAggregation eliminate the outer join which below aggregation.
type EliminateOuterJoinBelowAggregation struct {
	baseRule
	outerJoinEliminator
}

// NewRuleEliminateOuterJoinBelowAggregation creates a new Transformation EliminateOuterJoinBelowAggregation.
// The pattern of this rule is `Aggregation->Join->X`.
func NewRuleEliminateOuterJoinBelowAggregation() Transformation {
	rule := &EliminateOuterJoinBelowAggregation{}
	rule.pattern = pattern.BuildPattern(
		pattern.OperandAggregation,
		pattern.EngineTiDBOnly,
		pattern.NewPattern(pattern.OperandJoin, pattern.EngineTiDBOnly),
	)
	return rule
}

// Match implements Transformation interface.
func (*EliminateOuterJoinBelowAggregation) Match(expr *memo.ExprIter) bool {
	joinType := expr.Children[0].GetExpr().ExprNode.(*logicalop.LogicalJoin).JoinType
	return joinType == base.LeftOuterJoin || joinType == base.RightOuterJoin
}

// OnTransform implements Transformation interface.
// This rule tries to eliminate outer join which below aggregation.
func (r *EliminateOuterJoinBelowAggregation) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	agg := old.GetExpr().ExprNode.(*logicalop.LogicalAggregation)
	joinExpr := old.Children[0].GetExpr()
	join := joinExpr.ExprNode.(*logicalop.LogicalJoin)

	ok, innerChildIdx, outerGroup, innerGroup, outerUniqueIDs := r.prepareForEliminateOuterJoin(joinExpr)
	if !ok {
		return nil, false, false, nil
	}

	// only when agg only use the columns from outer table can eliminate outer join.
	if !ruleutil.IsColsAllFromOuterTable(agg.GetUsedCols(), &outerUniqueIDs) {
		return nil, false, false, nil
	}
	// outer join elimination with duplicate agnostic aggregate functions.
	_, aggCols := logicalop.GetDupAgnosticAggCols(agg, nil)
	if len(aggCols) > 0 {
		newAggExpr := memo.NewGroupExpr(agg)
		newAggExpr.SetChildren(outerGroup)
		return []*memo.GroupExpr{newAggExpr}, true, false, nil
	}
	// outer join elimination without duplicate agnostic aggregate functions.
	innerJoinKeys := join.ExtractJoinKeys(innerChildIdx)
	innerNullEQKeys := intset.NewFastIntSet()
	for _, eqCond := range join.EqualConditions {
		if eqCond.FuncName.L != ast.NullEQ {
			continue
		}
		innerKey := eqCond.GetArgs()[innerChildIdx].(*expression.Column)
		innerNullEQKeys.Insert(int(innerKey.UniqueID))
	}
	contain, err := r.isInnerJoinKeysContainUniqueKey(innerGroup, innerJoinKeys, innerNullEQKeys)
	if err != nil {
		return nil, false, false, err
	}
	if contain {
		newAggExpr := memo.NewGroupExpr(agg)
		newAggExpr.SetChildren(outerGroup)
		return []*memo.GroupExpr{newAggExpr}, true, false, nil
	}

	return nil, false, false, nil
}

// EliminateOuterJoinBelowProjection eliminate the outer join which below projection.
type EliminateOuterJoinBelowProjection struct {
	baseRule
	outerJoinEliminator
}

// NewRuleEliminateOuterJoinBelowProjection creates a new Transformation EliminateOuterJoinBelowProjection.
// The pattern of this rule is `Projection->Join->X`.
func NewRuleEliminateOuterJoinBelowProjection() Transformation {
	rule := &EliminateOuterJoinBelowProjection{}
	rule.pattern = pattern.BuildPattern(
		pattern.OperandProjection,
		pattern.EngineTiDBOnly,
		pattern.NewPattern(pattern.OperandJoin, pattern.EngineTiDBOnly),
	)
	return rule
}

// Match implements Transformation interface.
func (*EliminateOuterJoinBelowProjection) Match(expr *memo.ExprIter) bool {
	joinType := expr.Children[0].GetExpr().ExprNode.(*logicalop.LogicalJoin).JoinType
	return joinType == base.LeftOuterJoin || joinType == base.RightOuterJoin
}

// OnTransform implements Transformation interface.
// This rule tries to eliminate outer join which below projection.
func (r *EliminateOuterJoinBelowProjection) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	proj := old.GetExpr().ExprNode.(*logicalop.LogicalProjection)
	joinExpr := old.Children[0].GetExpr()
	join := joinExpr.ExprNode.(*logicalop.LogicalJoin)

	ok, innerChildIdx, outerGroup, innerGroup, outerUniqueIDs := r.prepareForEliminateOuterJoin(joinExpr)
	if !ok {
		return nil, false, false, nil
	}

	// only when proj only use the columns from outer table can eliminate outer join.
	if !ruleutil.IsColsAllFromOuterTable(proj.GetUsedCols(), &outerUniqueIDs) {
		return nil, false, false, nil
	}

	innerJoinKeys := join.ExtractJoinKeys(innerChildIdx)
	innerNullEQKeys := intset.NewFastIntSet()
	for _, eqCond := range join.EqualConditions {
		if eqCond.FuncName.L != ast.NullEQ {
			continue
		}
		innerKey := eqCond.GetArgs()[innerChildIdx].(*expression.Column)
		innerNullEQKeys.Insert(int(innerKey.UniqueID))
	}
	contain, err := r.isInnerJoinKeysContainUniqueKey(innerGroup, innerJoinKeys, innerNullEQKeys)
	if err != nil {
		return nil, false, false, err
	}
	if contain {
		newProjExpr := memo.NewGroupExpr(proj)
		newProjExpr.SetChildren(outerGroup)
		return []*memo.GroupExpr{newProjExpr}, true, false, nil
	}

	return nil, false, false, nil
}

// TransformAggregateCaseToSelection convert Agg(case when) to Agg->Selection.
type TransformAggregateCaseToSelection struct {
	baseRule
}

// NewRuleTransformAggregateCaseToSelection creates a new Transformation TransformAggregateCaseToSelection.
// The pattern of this rule is `Agg->X`.
func NewRuleTransformAggregateCaseToSelection() Transformation {
	rule := &TransformAggregateCaseToSelection{}
	rule.pattern = pattern.BuildPattern(
		pattern.OperandAggregation,
		pattern.EngineTiDBOnly,
	)
	return rule
}

// Match implements Transformation interface.
func (r *TransformAggregateCaseToSelection) Match(expr *memo.ExprIter) bool {
	agg := expr.GetExpr().ExprNode.(*logicalop.LogicalAggregation)
	return agg.IsCompleteModeAgg() && len(agg.GroupByItems) == 0 && len(agg.AggFuncs) == 1 && len(agg.AggFuncs[0].Args) == 1 && r.isTwoOrThreeArgCase(agg.AggFuncs[0].Args[0])
}

// OnTransform implements Transformation interface.
// This rule tries to convert Agg(case when) to Agg->Selection.
func (r *TransformAggregateCaseToSelection) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	agg := old.GetExpr().ExprNode.(*logicalop.LogicalAggregation)

	ok, newConditions, newAggFuncs := r.transform(agg)
	if !ok {
		return nil, false, false, nil
	}

	newSel := logicalop.LogicalSelection{Conditions: newConditions}.Init(agg.SCtx(), agg.QueryBlockOffset())
	newSelExpr := memo.NewGroupExpr(newSel)
	newSelExpr.SetChildren(old.GetExpr().Children...)
	newSelGroup := memo.NewGroupWithSchema(newSelExpr, old.GetExpr().Children[0].Prop.Schema)

	newAgg := logicalop.LogicalAggregation{
		AggFuncs:     newAggFuncs,
		GroupByItems: agg.GroupByItems,
	}.Init(agg.SCtx(), agg.QueryBlockOffset())
	newAgg.CopyAggHints(agg)
	newAggExpr := memo.NewGroupExpr(newAgg)
	newAggExpr.SetChildren(newSelGroup)
	return []*memo.GroupExpr{newAggExpr}, true, false, nil
}

func (r *TransformAggregateCaseToSelection) transform(agg *logicalop.LogicalAggregation) (ok bool, newConditions []expression.Expression, newAggFuncs []*aggregation.AggFuncDesc) {
	aggFuncDesc := agg.AggFuncs[0]
	aggFuncName := aggFuncDesc.Name
	ctx := agg.SCtx()

	caseFunc := aggFuncDesc.Args[0].(*expression.ScalarFunction)
	conditionFromCase := caseFunc.GetArgs()[0]
	caseArgs := caseFunc.GetArgs()
	caseArgsNum := len(caseArgs)

	// `case when a>0 then null else a end` should be converted to `case when !(a>0) then a else null end`.
	var nullFlip = caseArgsNum == 3 && caseArgs[1].Equal(ctx.GetExprCtx().GetEvalCtx(), expression.NewNull()) && !caseArgs[2].Equal(ctx.GetExprCtx().GetEvalCtx(), expression.NewNull())
	// `case when a>0 then 0 else a end` should be converted to `case when !(a>0) then a else 0 end`.
	var zeroFlip = !nullFlip && caseArgsNum == 3 && caseArgs[1].Equal(ctx.GetExprCtx().GetEvalCtx(), expression.NewZero())

	var outputIdx int
	if nullFlip || zeroFlip {
		outputIdx = 2
		newConditions = []expression.Expression{expression.NewFunctionInternal(ctx.GetExprCtx(), ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), conditionFromCase)}
	} else {
		outputIdx = 1
		newConditions = expression.SplitCNFItems(conditionFromCase)
	}

	if aggFuncDesc.HasDistinct {
		// Just one style supported:
		//   COUNT(DISTINCT CASE WHEN x = 'foo' THEN y END)
		// =>
		//   newAggFuncDesc: COUNT(DISTINCT y), newCondition: x = 'foo'

		if aggFuncName == ast.AggFuncCount && r.isOnlyOneNotNull(ctx.GetExprCtx().GetEvalCtx(), caseArgs, caseArgsNum, outputIdx) {
			newAggFuncDesc := aggFuncDesc.Clone()
			newAggFuncDesc.Args = []expression.Expression{caseArgs[outputIdx]}
			return true, newConditions, []*aggregation.AggFuncDesc{newAggFuncDesc}
		}
		return false, nil, nil
	}

	// Two styles supported:
	//
	// A1: AGG(CASE WHEN x = 'foo' THEN cnt END)
	//   => newAggFuncDesc: AGG(cnt), newCondition: x = 'foo'
	// A2: SUM(CASE WHEN x = 'foo' THEN cnt ELSE 0 END)
	//   => newAggFuncDesc: SUM(cnt), newCondition: x = 'foo'

	switch {
	case r.allowsSelection(aggFuncName) && (caseArgsNum == 2 || caseArgs[3-outputIdx].Equal(ctx.GetExprCtx().GetEvalCtx(), expression.NewNull())), // Case A1
		aggFuncName == ast.AggFuncSum && caseArgsNum == 3 && caseArgs[3-outputIdx].Equal(ctx.GetExprCtx().GetEvalCtx(), expression.NewZero()): // Case A2
		newAggFuncDesc := aggFuncDesc.Clone()
		newAggFuncDesc.Args = []expression.Expression{caseArgs[outputIdx]}
		return true, newConditions, []*aggregation.AggFuncDesc{newAggFuncDesc}
	default:
		return false, nil, nil
	}
}

func (*TransformAggregateCaseToSelection) allowsSelection(aggFuncName string) bool {
	return aggFuncName != ast.AggFuncFirstRow
}

func (*TransformAggregateCaseToSelection) isOnlyOneNotNull(ctx expression.EvalContext, args []expression.Expression, argsNum int, outputIdx int) bool {
	return !args[outputIdx].Equal(ctx, expression.NewNull()) && (argsNum == 2 || args[3-outputIdx].Equal(ctx, expression.NewNull()))
}

// TransformAggregateCaseToSelection only support `case when cond then var end` and `case when cond then var1 else var2 end`.
func (*TransformAggregateCaseToSelection) isTwoOrThreeArgCase(expr expression.Expression) bool {
	scalarFunc, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	return scalarFunc.FuncName.L == ast.Case && (len(scalarFunc.GetArgs()) == 2 || len(scalarFunc.GetArgs()) == 3)
}

// TransformAggToProj convert Agg to Proj.
type TransformAggToProj struct {
	baseRule
}

// NewRuleTransformAggToProj creates a new Transformation TransformAggToProj.
// The pattern of this rule is `Agg`.
func NewRuleTransformAggToProj() Transformation {
	rule := &TransformAggToProj{}
	rule.pattern = pattern.BuildPattern(
		pattern.OperandAggregation,
		pattern.EngineTiDBOnly,
	)
	return rule
}

// Match implements Transformation interface.
func (*TransformAggToProj) Match(expr *memo.ExprIter) bool {
	agg := expr.GetExpr().ExprNode.(*logicalop.LogicalAggregation)

	if !agg.IsCompleteModeAgg() {
		return false
	}

	for _, af := range agg.AggFuncs {
		// TODO(issue #9968): same as rule_aggregation_elimination.go -> tryToEliminateAggregation.
		// waiting for (issue #14616): `nullable` information.
		if af.Name == ast.AggFuncGroupConcat {
			return false
		}
	}

	childGroup := expr.GetExpr().Children[0]
	childGroup.BuildKeyInfo()
	schemaByGroupby := expression.NewSchema(agg.GetGroupByCols()...)
	for _, key := range childGroup.Prop.Schema.PKOrUK {
		if schemaByGroupby.ColumnsIndices(key) != nil {
			return true
		}
	}

	return false
}

// OnTransform implements Transformation interface.
// This rule tries to convert agg to proj.
func (*TransformAggToProj) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	agg := old.GetExpr().ExprNode.(*logicalop.LogicalAggregation)
	if ok, proj := plannercore.ConvertAggToProj(agg, old.GetExpr().Schema()); ok {
		newProjExpr := memo.NewGroupExpr(proj)
		newProjExpr.SetChildren(old.GetExpr().Children...)
		return []*memo.GroupExpr{newProjExpr}, true, false, nil
	}

	return nil, false, false, nil
}

// InjectProjectionBelowTopN injects two Projections below and upon TopN if TopN's ByItems
// contain ScalarFunctions.
