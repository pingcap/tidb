// Copyright 2025 PingCAP, Inc.
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

package agg

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	corebase "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
)

var _ rule.Rule = &XFAggregationCaseWhen2Selection{}

// XFAggregationCaseWhen2Selection try to convert case when from agg to selection downward.
type XFAggregationCaseWhen2Selection struct {
	*rule.BaseRule
}

// NewXFAggregationCaseWhen2Selection creates a new XFDeCorrelateSimpleApply rule.
func NewXFAggregationCaseWhen2Selection() *XFAggregationCaseWhen2Selection {
	pa := pattern.NewPattern(pattern.OperandAggregation, pattern.EngineTiDBOnly)
	pa.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly))
	return &XFAggregationCaseWhen2Selection{
		BaseRule: rule.NewBaseRule(rule.XFAggregationCaseWhen2Selection, pa),
	}
}

// ID implement the Rule interface.
func (*XFAggregationCaseWhen2Selection) ID() uint {
	return uint(rule.XFAggregationCaseWhen2Selection)
}

// PreCheck implements the Rule interface.
func (xf *XFAggregationCaseWhen2Selection) PreCheck(aggGE corebase.LogicalPlan) bool {
	agg := aggGE.GetWrappedLogicalPlan().(*logicalop.LogicalAggregation)
	return agg.IsCompleteModeAgg() && len(agg.GroupByItems) == 0 && len(agg.AggFuncs) == 1 &&
		len(agg.AggFuncs[0].Args) == 1 && xf.isTwoOrThreeArgCaseWhenFunc(agg.AggFuncs[0].Args[0])
}

// XForm implements the Rule interface.
func (xf *XFAggregationCaseWhen2Selection) XForm(aggGE corebase.LogicalPlan) ([]corebase.LogicalPlan, bool, error) {
	aggOp := aggGE.GetWrappedLogicalPlan().(*logicalop.LogicalAggregation)
	childGE := aggGE.Children()[0]
	newConditions, newAggFuncs, ok := xf.transform(aggOp)
	if !ok {
		return nil, false, nil
	}
	// remove means whether the intermediary apply should be removed from memo
	newSel := logicalop.LogicalSelection{Conditions: newConditions}.Init(aggOp.SCtx(), aggOp.QueryBlockOffset())
	newSel.SetChildren(childGE)
	// new top aggregation.
	newAgg := logicalop.LogicalAggregation{
		AggFuncs:     newAggFuncs,
		GroupByItems: aggOp.GroupByItems,
	}.Init(aggOp.SCtx(), aggOp.QueryBlockOffset())
	// schema sharing, COW when it's necessary.
	newAgg.SetSchema(aggOp.Schema())
	newAgg.CopyAggHints(aggOp)
	newAgg.SetChildren(newSel)
	// return the new agg back.
	return []corebase.LogicalPlan{newAgg}, true, nil
}

// isTwoOrThreeArgCaseWhenFunc only support `case when cond then var end` and `case when cond then var1 else var2 end`.
func (*XFAggregationCaseWhen2Selection) isTwoOrThreeArgCaseWhenFunc(expr expression.Expression) bool {
	scalarFunc, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	return scalarFunc.FuncName.L == ast.Case && (len(scalarFunc.GetArgs()) == 2 || len(scalarFunc.GetArgs()) == 3)
}

func (xf *XFAggregationCaseWhen2Selection) transform(agg *logicalop.LogicalAggregation) (
	newConditions []expression.Expression, newAggFuncs []*aggregation.AggFuncDesc, ok bool) {

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
		newF, err := expression.NewFunction(ctx.GetExprCtx(), ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), conditionFromCase)
		if err != nil {
			return nil, nil, false
		}
		newConditions = []expression.Expression{newF}
	} else {
		outputIdx = 1
		newConditions = expression.SplitCNFItems(conditionFromCase)
	}

	if aggFuncDesc.HasDistinct {
		// Just one style supported:
		//   COUNT(DISTINCT CASE WHEN x = 'foo' THEN y END)
		// =>
		//   newAggFuncDesc: COUNT(DISTINCT y), newCondition: x = 'foo'
		if aggFuncName == ast.AggFuncCount && xf.isOnlyOneNotNull(ctx.GetExprCtx().GetEvalCtx(), caseArgs, caseArgsNum, outputIdx) {
			newAggFuncDesc := aggFuncDesc.Clone()
			newAggFuncDesc.Args = []expression.Expression{caseArgs[outputIdx]}
			return newConditions, []*aggregation.AggFuncDesc{newAggFuncDesc}, true
		}
		return nil, nil, false
	}

	// Two styles supported:
	//
	// A1: AGG(CASE WHEN x = 'foo' THEN cnt END)
	//   => newAggFuncDesc: AGG(cnt), newCondition: x = 'foo'
	// A2: SUM(CASE WHEN x = 'foo' THEN cnt ELSE 0 END)
	//   => newAggFuncDesc: SUM(cnt), newCondition: x = 'foo'
	switch {
	case xf.allowsSelection(aggFuncName) && (caseArgsNum == 2 || caseArgs[3-outputIdx].Equal(ctx.GetExprCtx().GetEvalCtx(), expression.NewNull())), // Case A1
		aggFuncName == ast.AggFuncSum && caseArgsNum == 3 && caseArgs[3-outputIdx].Equal(ctx.GetExprCtx().GetEvalCtx(), expression.NewZero()): // Case A2
		newAggFuncDesc := aggFuncDesc.Clone()
		newAggFuncDesc.Args = []expression.Expression{caseArgs[outputIdx]}
		return newConditions, []*aggregation.AggFuncDesc{newAggFuncDesc}, true
	default:
		return nil, nil, false
	}
}

func (*XFAggregationCaseWhen2Selection) allowsSelection(aggFuncName string) bool {
	return aggFuncName != ast.AggFuncFirstRow
}

func (*XFAggregationCaseWhen2Selection) isOnlyOneNotNull(ctx expression.EvalContext, args []expression.Expression, argsNum int, outputIdx int) bool {
	// when args num is equal to 2, AND
	// the output arg should not be null, AND
	// the other arg should be null.
	return !args[outputIdx].Equal(ctx, expression.NewNull()) && (argsNum == 2 || args[3-outputIdx].Equal(ctx, expression.NewNull()))
}
