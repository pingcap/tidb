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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"bytes"
	"context"
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
)

type ppdSolver struct{}

// exprPrefixAdder is the wrapper struct to add tidb_shard(x) = val for `OrigConds`
// `cols` is the index columns for a unique shard index
type exprPrefixAdder struct {
	sctx      base.PlanContext
	OrigConds []expression.Expression
	cols      []*expression.Column
	lengths   []int
}

func (*ppdSolver) optimize(_ context.Context, lp base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	_, p := lp.PredicatePushDown(nil, opt)
	return p, planChanged, nil
}

func addSelection(p base.LogicalPlan, child base.LogicalPlan, conditions []expression.Expression, chIdx int, opt *optimizetrace.LogicalOptimizeOp) {
	if len(conditions) == 0 {
		p.Children()[chIdx] = child
		return
	}
	conditions = expression.PropagateConstant(p.SCtx().GetExprCtx(), conditions)
	// Return table dual when filter is constant false or null.
	dual := Conds2TableDual(child, conditions)
	if dual != nil {
		p.Children()[chIdx] = dual
		appendTableDualTraceStep(child, dual, conditions, opt)
		return
	}

	conditions = DeleteTrueExprs(p, conditions)
	if len(conditions) == 0 {
		p.Children()[chIdx] = child
		return
	}
	selection := LogicalSelection{Conditions: conditions}.Init(p.SCtx(), p.QueryBlockOffset())
	selection.SetChildren(child)
	p.Children()[chIdx] = selection
	appendAddSelectionTraceStep(p, child, selection, opt)
}

func splitSetGetVarFunc(filters []expression.Expression) ([]expression.Expression, []expression.Expression) {
	canBePushDown := make([]expression.Expression, 0, len(filters))
	canNotBePushDown := make([]expression.Expression, 0, len(filters))
	for _, expr := range filters {
		if expression.HasGetSetVarFunc(expr) {
			canNotBePushDown = append(canNotBePushDown, expr)
		} else {
			canBePushDown = append(canBePushDown, expr)
		}
	}
	return canBePushDown, canNotBePushDown
}

// PredicatePushDown implements base.LogicalPlan PredicatePushDown interface.
func (p *LogicalSelection) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	predicates = DeleteTrueExprs(p, predicates)
	p.Conditions = DeleteTrueExprs(p, p.Conditions)
	var child base.LogicalPlan
	var retConditions []expression.Expression
	var originConditions []expression.Expression
	canBePushDown, canNotBePushDown := splitSetGetVarFunc(p.Conditions)
	originConditions = canBePushDown
	retConditions, child = p.Children()[0].PredicatePushDown(append(canBePushDown, predicates...), opt)
	retConditions = append(retConditions, canNotBePushDown...)
	exprCtx := p.SCtx().GetExprCtx()
	if len(retConditions) > 0 {
		p.Conditions = expression.PropagateConstant(exprCtx, retConditions)
		// Return table dual when filter is constant false or null.
		dual := Conds2TableDual(p, p.Conditions)
		if dual != nil {
			appendTableDualTraceStep(p, dual, p.Conditions, opt)
			return nil, dual
		}
		return nil, p
	}
	appendSelectionPredicatePushDownTraceStep(p, originConditions, opt)
	return nil, child
}

// PredicatePushDown implements base.LogicalPlan PredicatePushDown interface.
func (p *LogicalUnionScan) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	retainedPredicates, _ := p.Children()[0].PredicatePushDown(predicates, opt)
	p.conditions = make([]expression.Expression, 0, len(predicates))
	p.conditions = append(p.conditions, predicates...)
	// The conditions in UnionScan is only used for added rows, so parent Selection should not be removed.
	return retainedPredicates, p
}

// PredicatePushDown implements base.LogicalPlan PredicatePushDown interface.
func (ds *DataSource) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	predicates = expression.PropagateConstant(ds.SCtx().GetExprCtx(), predicates)
	predicates = DeleteTrueExprs(ds, predicates)
	// Add tidb_shard() prefix to the condtion for shard index in some scenarios
	// TODO: remove it to the place building logical plan
	predicates = ds.AddPrefix4ShardIndexes(ds.SCtx(), predicates)
	ds.allConds = predicates
	ds.pushedDownConds, predicates = expression.PushDownExprs(GetPushDownCtx(ds.SCtx()), predicates, kv.UnSpecified)
	appendDataSourcePredicatePushDownTraceStep(ds, opt)
	return predicates, ds
}

// PredicatePushDown implements base.LogicalPlan PredicatePushDown interface.
func (p *LogicalTableDual) PredicatePushDown(predicates []expression.Expression, _ *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	return predicates, p
}

// PredicatePushDown implements base.LogicalPlan PredicatePushDown interface.
func (p *LogicalJoin) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) (ret []expression.Expression, retPlan base.LogicalPlan) {
	var equalCond []*expression.ScalarFunction
	var leftPushCond, rightPushCond, otherCond, leftCond, rightCond []expression.Expression
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		predicates = p.outerJoinPropConst(predicates)
		dual := Conds2TableDual(p, predicates)
		if dual != nil {
			appendTableDualTraceStep(p, dual, predicates, opt)
			return ret, dual
		}
		// Handle where conditions
		predicates = expression.ExtractFiltersFromDNFs(p.SCtx().GetExprCtx(), predicates)
		// Only derive left where condition, because right where condition cannot be pushed down
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(predicates, true, false)
		leftCond = leftPushCond
		// Handle join conditions, only derive right join condition, because left join condition cannot be pushed down
		_, derivedRightJoinCond := DeriveOtherConditions(
			p, p.Children()[0].Schema(), p.Children()[1].Schema(), false, true)
		rightCond = append(p.RightConditions, derivedRightJoinCond...)
		p.RightConditions = nil
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, rightPushCond...)
	case RightOuterJoin:
		predicates = p.outerJoinPropConst(predicates)
		dual := Conds2TableDual(p, predicates)
		if dual != nil {
			appendTableDualTraceStep(p, dual, predicates, opt)
			return ret, dual
		}
		// Handle where conditions
		predicates = expression.ExtractFiltersFromDNFs(p.SCtx().GetExprCtx(), predicates)
		// Only derive right where condition, because left where condition cannot be pushed down
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(predicates, false, true)
		rightCond = rightPushCond
		// Handle join conditions, only derive left join condition, because right join condition cannot be pushed down
		derivedLeftJoinCond, _ := DeriveOtherConditions(
			p, p.Children()[0].Schema(), p.Children()[1].Schema(), true, false)
		leftCond = append(p.LeftConditions, derivedLeftJoinCond...)
		p.LeftConditions = nil
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, leftPushCond...)
	case SemiJoin, InnerJoin:
		tempCond := make([]expression.Expression, 0, len(p.LeftConditions)+len(p.RightConditions)+len(p.EqualConditions)+len(p.OtherConditions)+len(predicates))
		tempCond = append(tempCond, p.LeftConditions...)
		tempCond = append(tempCond, p.RightConditions...)
		tempCond = append(tempCond, expression.ScalarFuncs2Exprs(p.EqualConditions)...)
		tempCond = append(tempCond, p.OtherConditions...)
		tempCond = append(tempCond, predicates...)
		tempCond = expression.ExtractFiltersFromDNFs(p.SCtx().GetExprCtx(), tempCond)
		tempCond = expression.PropagateConstant(p.SCtx().GetExprCtx(), tempCond)
		// Return table dual when filter is constant false or null.
		dual := Conds2TableDual(p, tempCond)
		if dual != nil {
			appendTableDualTraceStep(p, dual, tempCond, opt)
			return ret, dual
		}
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(tempCond, true, true)
		p.LeftConditions = nil
		p.RightConditions = nil
		p.EqualConditions = equalCond
		p.OtherConditions = otherCond
		leftCond = leftPushCond
		rightCond = rightPushCond
	case AntiSemiJoin:
		predicates = expression.PropagateConstant(p.SCtx().GetExprCtx(), predicates)
		// Return table dual when filter is constant false or null.
		dual := Conds2TableDual(p, predicates)
		if dual != nil {
			appendTableDualTraceStep(p, dual, predicates, opt)
			return ret, dual
		}
		// `predicates` should only contain left conditions or constant filters.
		_, leftPushCond, rightPushCond, _ = p.extractOnCondition(predicates, true, true)
		// Do not derive `is not null` for anti join, since it may cause wrong results.
		// For example:
		// `select * from t t1 where t1.a not in (select b from t t2)` does not imply `t2.b is not null`,
		// `select * from t t1 where t1.a not in (select a from t t2 where t1.b = t2.b` does not imply `t1.b is not null`,
		// `select * from t t1 where not exists (select * from t t2 where t2.a = t1.a)` does not imply `t1.a is not null`,
		leftCond = leftPushCond
		rightCond = append(p.RightConditions, rightPushCond...)
		p.RightConditions = nil
	}
	leftCond = expression.RemoveDupExprs(leftCond)
	rightCond = expression.RemoveDupExprs(rightCond)
	leftRet, lCh := p.Children()[0].PredicatePushDown(leftCond, opt)
	rightRet, rCh := p.Children()[1].PredicatePushDown(rightCond, opt)
	utilfuncp.AddSelection(p, lCh, leftRet, 0, opt)
	utilfuncp.AddSelection(p, rCh, rightRet, 1, opt)
	p.updateEQCond()
	buildKeyInfo(p)
	return ret, p.Self()
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
			p.OtherConditions = append(p.OtherConditions[:i], p.OtherConditions[i+1:]...)
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
					lKey = lProj.appendExpr(lKey)
				}
				if rProj != nil {
					rKey = rProj.appendExpr(rKey)
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
	canBeNAAJ := (p.JoinType == AntiSemiJoin || p.JoinType == AntiLeftOuterSemiJoin) && len(p.EqualConditions) == 0
	if canBeNAAJ && p.SCtx().GetSessionVars().OptimizerEnableNAAJ {
		var otherCond expression.CNFExprs
		for i := 0; i < len(p.OtherConditions); i++ {
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

func (p *LogicalProjection) appendExpr(expr expression.Expression) *expression.Column {
	if col, ok := expr.(*expression.Column); ok {
		return col
	}
	expr = expression.ColumnSubstitute(p.SCtx().GetExprCtx(), expr, p.schema, p.Exprs)
	p.Exprs = append(p.Exprs, expr)

	col := &expression.Column{
		UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  expr.GetType(p.SCtx().GetExprCtx().GetEvalCtx()).Clone(),
	}
	col.SetCoercibility(expr.Coercibility())
	col.SetRepertoire(expr.Repertoire())
	p.schema.Append(col)
	// reset ParseToJSONFlag in order to keep the flag away from json column
	if col.GetStaticType().GetType() == mysql.TypeJSON {
		col.GetStaticType().DelFlag(mysql.ParseToJSONFlag)
	}
	return col
}

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

// BreakDownPredicates breaks down predicates into two sets: canBePushed and cannotBePushed. It also maps columns to projection schema.
func BreakDownPredicates(p *LogicalProjection, predicates []expression.Expression) ([]expression.Expression, []expression.Expression) {
	canBePushed := make([]expression.Expression, 0, len(predicates))
	canNotBePushed := make([]expression.Expression, 0, len(predicates))
	exprCtx := p.SCtx().GetExprCtx()
	for _, cond := range predicates {
		substituted, hasFailed, newFilter := expression.ColumnSubstituteImpl(exprCtx, cond, p.Schema(), p.Exprs, true)
		if substituted && !hasFailed && !expression.HasGetSetVarFunc(newFilter) {
			canBePushed = append(canBePushed, newFilter)
		} else {
			canNotBePushed = append(canNotBePushed, cond)
		}
	}
	return canBePushed, canNotBePushed
}

// PredicatePushDown implements base.LogicalPlan PredicatePushDown interface.
func (p *LogicalExpand) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) (ret []expression.Expression, retPlan base.LogicalPlan) {
	// Note that, grouping column related predicates can't be pushed down, since grouping column has nullability change after Expand OP itself.
	// condition related with grouping column shouldn't be pushed down through it.
	// currently, since expand is adjacent to aggregate, any filter above aggregate wanted to be push down through expand only have two cases:
	// 		1. agg function related filters. (these condition is always above aggregate)
	// 		2. group-by item related filters. (there condition is always related with grouping sets columns, which can't be pushed down)
	// As a whole, we banned all the predicates pushing-down logic here that remained in Expand OP, and constructing a new selection above it if any.
	remained, child := p.BaseLogicalPlan.PredicatePushDown(nil, opt)
	return append(remained, predicates...), child
}

// PredicatePushDown implements base.LogicalPlan PredicatePushDown interface.
func (p *LogicalProjection) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) (ret []expression.Expression, retPlan base.LogicalPlan) {
	for _, expr := range p.Exprs {
		if expression.HasAssignSetVarFunc(expr) {
			_, child := p.BaseLogicalPlan.PredicatePushDown(nil, opt)
			return predicates, child
		}
	}
	canBePushed, canNotBePushed := BreakDownPredicates(p, predicates)
	remained, child := p.BaseLogicalPlan.PredicatePushDown(canBePushed, opt)
	return append(remained, canNotBePushed...), child
}

// PredicatePushDown implements base.LogicalPlan PredicatePushDown interface.
func (p *LogicalUnionAll) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) (ret []expression.Expression, retPlan base.LogicalPlan) {
	for i, proj := range p.Children() {
		newExprs := make([]expression.Expression, 0, len(predicates))
		newExprs = append(newExprs, predicates...)
		retCond, newChild := proj.PredicatePushDown(newExprs, opt)
		utilfuncp.AddSelection(p, newChild, retCond, i, opt)
	}
	return nil, p
}

// pushDownPredicatesForAggregation split a condition to two parts, can be pushed-down or can not be pushed-down below aggregation.
func (la *LogicalAggregation) pushDownPredicatesForAggregation(cond expression.Expression, groupByColumns *expression.Schema, exprsOriginal []expression.Expression) ([]expression.Expression, []expression.Expression) {
	var condsToPush []expression.Expression
	var ret []expression.Expression
	switch cond.(type) {
	case *expression.Constant:
		condsToPush = append(condsToPush, cond)
		// Consider SQL list "select sum(b) from t group by a having 1=0". "1=0" is a constant predicate which should be
		// retained and pushed down at the same time. Because we will get a wrong query result that contains one column
		// with value 0 rather than an empty query result.
		ret = append(ret, cond)
	case *expression.ScalarFunction:
		extractedCols := expression.ExtractColumns(cond)
		ok := true
		for _, col := range extractedCols {
			if !groupByColumns.Contains(col) {
				ok = false
				break
			}
		}
		if ok {
			newFunc := expression.ColumnSubstitute(la.SCtx().GetExprCtx(), cond, la.Schema(), exprsOriginal)
			condsToPush = append(condsToPush, newFunc)
		} else {
			ret = append(ret, cond)
		}
	default:
		ret = append(ret, cond)
	}
	return condsToPush, ret
}

// pushDownPredicatesForAggregation split a CNF condition to two parts, can be pushed-down or can not be pushed-down below aggregation.
// It would consider the CNF.
// For example,
// (a > 1 or avg(b) > 1) and (a < 3), and `avg(b) > 1` can't be pushed-down.
// Then condsToPush: a < 3, ret: a > 1 or avg(b) > 1
func (la *LogicalAggregation) pushDownCNFPredicatesForAggregation(cond expression.Expression, groupByColumns *expression.Schema, exprsOriginal []expression.Expression) ([]expression.Expression, []expression.Expression) {
	var condsToPush []expression.Expression
	var ret []expression.Expression
	subCNFItem := expression.SplitCNFItems(cond)
	if len(subCNFItem) == 1 {
		return la.pushDownPredicatesForAggregation(subCNFItem[0], groupByColumns, exprsOriginal)
	}
	exprCtx := la.SCtx().GetExprCtx()
	for _, item := range subCNFItem {
		condsToPushForItem, retForItem := la.pushDownDNFPredicatesForAggregation(item, groupByColumns, exprsOriginal)
		if len(condsToPushForItem) > 0 {
			condsToPush = append(condsToPush, expression.ComposeDNFCondition(exprCtx, condsToPushForItem...))
		}
		if len(retForItem) > 0 {
			ret = append(ret, expression.ComposeDNFCondition(exprCtx, retForItem...))
		}
	}
	return condsToPush, ret
}

// pushDownDNFPredicatesForAggregation split a DNF condition to two parts, can be pushed-down or can not be pushed-down below aggregation.
// It would consider the DNF.
// For example,
// (a > 1 and avg(b) > 1) or (a < 3), and `avg(b) > 1` can't be pushed-down.
// Then condsToPush: (a < 3) and (a > 1), ret: (a > 1 and avg(b) > 1) or (a < 3)
func (la *LogicalAggregation) pushDownDNFPredicatesForAggregation(cond expression.Expression, groupByColumns *expression.Schema, exprsOriginal []expression.Expression) ([]expression.Expression, []expression.Expression) {
	//nolint: prealloc
	var condsToPush []expression.Expression
	var ret []expression.Expression
	subDNFItem := expression.SplitDNFItems(cond)
	if len(subDNFItem) == 1 {
		return la.pushDownPredicatesForAggregation(subDNFItem[0], groupByColumns, exprsOriginal)
	}
	exprCtx := la.SCtx().GetExprCtx()
	for _, item := range subDNFItem {
		condsToPushForItem, retForItem := la.pushDownCNFPredicatesForAggregation(item, groupByColumns, exprsOriginal)
		if len(condsToPushForItem) <= 0 {
			return nil, []expression.Expression{cond}
		}
		condsToPush = append(condsToPush, expression.ComposeCNFCondition(exprCtx, condsToPushForItem...))
		if len(retForItem) > 0 {
			ret = append(ret, expression.ComposeCNFCondition(exprCtx, retForItem...))
		}
	}
	if len(ret) == 0 {
		// All the condition can be pushed down.
		return []expression.Expression{cond}, nil
	}
	dnfPushDownCond := expression.ComposeDNFCondition(exprCtx, condsToPush...)
	// Some condition can't be pushed down, we need to keep all the condition.
	return []expression.Expression{dnfPushDownCond}, []expression.Expression{cond}
}

// splitCondForAggregation splits the condition into those who can be pushed and others.
func (la *LogicalAggregation) splitCondForAggregation(predicates []expression.Expression) ([]expression.Expression, []expression.Expression) {
	var condsToPush []expression.Expression
	var ret []expression.Expression
	exprsOriginal := make([]expression.Expression, 0, len(la.AggFuncs))
	for _, fun := range la.AggFuncs {
		exprsOriginal = append(exprsOriginal, fun.Args[0])
	}
	groupByColumns := expression.NewSchema(la.GetGroupByCols()...)
	// It's almost the same as pushDownCNFPredicatesForAggregation, except that the condition is a slice.
	for _, cond := range predicates {
		subCondsToPush, subRet := la.pushDownDNFPredicatesForAggregation(cond, groupByColumns, exprsOriginal)
		if len(subCondsToPush) > 0 {
			condsToPush = append(condsToPush, subCondsToPush...)
		}
		if len(subRet) > 0 {
			ret = append(ret, subRet...)
		}
	}
	return condsToPush, ret
}

// PredicatePushDown implements base.LogicalPlan PredicatePushDown interface.
func (la *LogicalAggregation) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	condsToPush, ret := la.splitCondForAggregation(predicates)
	la.BaseLogicalPlan.PredicatePushDown(condsToPush, opt)
	return ret, la
}

// PredicatePushDown implements base.LogicalPlan PredicatePushDown interface.
func (p *LogicalLimit) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	// Limit forbids any condition to push down.
	p.BaseLogicalPlan.PredicatePushDown(nil, opt)
	return predicates, p
}

// PredicatePushDown implements base.LogicalPlan PredicatePushDown interface.
func (p *LogicalMaxOneRow) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	// MaxOneRow forbids any condition to push down.
	p.BaseLogicalPlan.PredicatePushDown(nil, opt)
	return predicates, p
}

// DeriveOtherConditions given a LogicalJoin, check the OtherConditions to see if we can derive more
// conditions for left/right child pushdown.
func DeriveOtherConditions(
	p *LogicalJoin, leftSchema *expression.Schema, rightSchema *expression.Schema,
	deriveLeft bool, deriveRight bool) (
	leftCond []expression.Expression, rightCond []expression.Expression) {
	isOuterSemi := (p.JoinType == LeftOuterSemiJoin) || (p.JoinType == AntiLeftOuterSemiJoin)
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
	arg0, lOK := binop.GetArgs()[0].(*expression.Column)
	arg1, rOK := binop.GetArgs()[1].(*expression.Column)
	if !lOK || !rOK {
		return nil
	}
	childCol := schema.RetrieveColumn(arg0)
	if childCol == nil {
		childCol = schema.RetrieveColumn(arg1)
	}
	if util.IsNullRejected(ctx, schema, expr) && !mysql.HasNotNullFlag(childCol.RetType.GetFlag()) {
		return expression.BuildNotNullExpr(ctx.GetExprCtx(), childCol)
	}
	return nil
}

// Conds2TableDual builds a LogicalTableDual if cond is constant false or null.
func Conds2TableDual(p base.LogicalPlan, conds []expression.Expression) base.LogicalPlan {
	if len(conds) != 1 {
		return nil
	}
	con, ok := conds[0].(*expression.Constant)
	if !ok {
		return nil
	}
	sc := p.SCtx().GetSessionVars().StmtCtx
	if expression.MaybeOverOptimized4PlanCache(p.SCtx().GetExprCtx(), []expression.Expression{con}) {
		return nil
	}
	if isTrue, err := con.Value.ToBool(sc.TypeCtxOrDefault()); (err == nil && isTrue == 0) || con.Value.IsNull() {
		dual := LogicalTableDual{}.Init(p.SCtx(), p.QueryBlockOffset())
		dual.SetSchema(p.Schema())
		return dual
	}
	return nil
}

// DeleteTrueExprs deletes the surely true expressions
func DeleteTrueExprs(p base.LogicalPlan, conds []expression.Expression) []expression.Expression {
	newConds := make([]expression.Expression, 0, len(conds))
	for _, cond := range conds {
		con, ok := cond.(*expression.Constant)
		if !ok {
			newConds = append(newConds, cond)
			continue
		}
		if expression.MaybeOverOptimized4PlanCache(p.SCtx().GetExprCtx(), []expression.Expression{con}) {
			newConds = append(newConds, cond)
			continue
		}
		sc := p.SCtx().GetSessionVars().StmtCtx
		if isTrue, err := con.Value.ToBool(sc.TypeCtx()); err == nil && isTrue == 1 {
			continue
		}
		newConds = append(newConds, cond)
	}
	return newConds
}

// outerJoinPropConst propagates constant equal and column equal conditions over outer join.
func (p *LogicalJoin) outerJoinPropConst(predicates []expression.Expression) []expression.Expression {
	outerTable := p.Children()[0]
	innerTable := p.Children()[1]
	if p.JoinType == RightOuterJoin {
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
	nullSensitive := p.JoinType == AntiLeftOuterSemiJoin || p.JoinType == LeftOuterSemiJoin
	joinConds, predicates = expression.PropConstOverOuterJoin(p.SCtx().GetExprCtx(), joinConds, predicates, outerTable.Schema(), innerTable.Schema(), nullSensitive)
	p.AttachOnConds(joinConds)
	return predicates
}

// GetPartitionByCols extracts 'partition by' columns from the Window.
func (p *LogicalWindow) GetPartitionByCols() []*expression.Column {
	partitionCols := make([]*expression.Column, 0, len(p.PartitionBy))
	for _, partitionItem := range p.PartitionBy {
		partitionCols = append(partitionCols, partitionItem.Col)
	}
	return partitionCols
}

// PredicatePushDown implements base.LogicalPlan PredicatePushDown interface.
func (p *LogicalWindow) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	canBePushed := make([]expression.Expression, 0, len(predicates))
	canNotBePushed := make([]expression.Expression, 0, len(predicates))
	partitionCols := expression.NewSchema(p.GetPartitionByCols()...)
	for _, cond := range predicates {
		// We can push predicate beneath Window, only if all of the
		// extractedCols are part of partitionBy columns.
		if expression.ExprFromSchema(cond, partitionCols) {
			canBePushed = append(canBePushed, cond)
		} else {
			canNotBePushed = append(canNotBePushed, cond)
		}
	}
	p.BaseLogicalPlan.PredicatePushDown(canBePushed, opt)
	return canNotBePushed, p
}

// PredicatePushDown implements base.LogicalPlan PredicatePushDown interface.
func (p *LogicalMemTable) PredicatePushDown(predicates []expression.Expression, _ *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	if p.Extractor != nil {
		predicates = p.Extractor.Extract(p.SCtx(), p.schema, p.names, predicates)
	}
	return predicates, p.Self()
}

func (*ppdSolver) name() string {
	return "predicate_push_down"
}

func appendTableDualTraceStep(replaced base.LogicalPlan, dual base.LogicalPlan, conditions []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v is replaced by %v_%v", replaced.TP(), replaced.ID(), dual.TP(), dual.ID())
	}
	reason := func() string {
		buffer := bytes.NewBufferString("The conditions[")
		for i, cond := range conditions {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(cond.String())
		}
		buffer.WriteString("] are constant false or null")
		return buffer.String()
	}
	opt.AppendStepToCurrent(dual.ID(), dual.TP(), reason, action)
}

func appendSelectionPredicatePushDownTraceStep(p *LogicalSelection, conditions []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v is removed", p.TP(), p.ID())
	}
	reason := func() string {
		return ""
	}
	if len(conditions) > 0 {
		reason = func() string {
			buffer := bytes.NewBufferString("The conditions[")
			for i, cond := range conditions {
				if i > 0 {
					buffer.WriteString(",")
				}
				buffer.WriteString(cond.String())
			}
			fmt.Fprintf(buffer, "] in %v_%v are pushed down", p.TP(), p.ID())
			return buffer.String()
		}
	}
	opt.AppendStepToCurrent(p.ID(), p.TP(), reason, action)
}

func appendDataSourcePredicatePushDownTraceStep(ds *DataSource, opt *optimizetrace.LogicalOptimizeOp) {
	if len(ds.pushedDownConds) < 1 {
		return
	}
	reason := func() string {
		return ""
	}
	action := func() string {
		buffer := bytes.NewBufferString("The conditions[")
		for i, cond := range ds.pushedDownConds {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(cond.String())
		}
		fmt.Fprintf(buffer, "] are pushed down across %v_%v", ds.TP(), ds.ID())
		return buffer.String()
	}
	opt.AppendStepToCurrent(ds.ID(), ds.TP(), reason, action)
}

func appendAddSelectionTraceStep(p base.LogicalPlan, child base.LogicalPlan, sel *LogicalSelection, opt *optimizetrace.LogicalOptimizeOp) {
	reason := func() string {
		return ""
	}
	action := func() string {
		return fmt.Sprintf("add %v_%v to connect %v_%v and %v_%v", sel.TP(), sel.ID(), p.TP(), p.ID(), child.TP(), child.ID())
	}
	opt.AppendStepToCurrent(sel.ID(), sel.TP(), reason, action)
}

// AddPrefix4ShardIndexes add expression prefix for shard index. e.g. an index is test.uk(tidb_shard(a), a).
// It transforms the sql "SELECT * FROM test WHERE a = 10" to
// "SELECT * FROM test WHERE tidb_shard(a) = val AND a = 10", val is the value of tidb_shard(10).
// It also transforms the sql "SELECT * FROM test WHERE a IN (10, 20, 30)" to
// "SELECT * FROM test WHERE tidb_shard(a) = val1 AND a = 10 OR tidb_shard(a) = val2 AND a = 20"
// @param[in] conds            the original condtion of this datasource
// @retval - the new condition after adding expression prefix
func (ds *DataSource) AddPrefix4ShardIndexes(sc base.PlanContext, conds []expression.Expression) []expression.Expression {
	if !ds.containExprPrefixUk {
		return conds
	}

	var err error
	newConds := conds

	for _, path := range ds.possibleAccessPaths {
		if !path.IsUkShardIndexPath {
			continue
		}
		newConds, err = ds.addExprPrefixCond(sc, path, newConds)
		if err != nil {
			logutil.BgLogger().Error("Add tidb_shard expression failed",
				zap.Error(err),
				zap.Uint64("connection id", sc.GetSessionVars().ConnectionID),
				zap.String("database name", ds.DBName.L),
				zap.String("table name", ds.tableInfo.Name.L),
				zap.String("index name", path.Index.Name.L))
			return conds
		}
	}

	return newConds
}

func (ds *DataSource) addExprPrefixCond(sc base.PlanContext, path *util.AccessPath,
	conds []expression.Expression) ([]expression.Expression, error) {
	idxCols, idxColLens :=
		expression.IndexInfo2PrefixCols(ds.Columns, ds.schema.Columns, path.Index)
	if len(idxCols) == 0 {
		return conds, nil
	}

	adder := &exprPrefixAdder{
		sctx:      sc,
		OrigConds: conds,
		cols:      idxCols,
		lengths:   idxColLens,
	}

	return adder.addExprPrefix4ShardIndex()
}

// AddExprPrefix4ShardIndex
// if original condition is a LogicOr expression, such as `WHERE a = 1 OR a = 10`,
// call the function AddExprPrefix4DNFCond to add prefix expression tidb_shard(a) = xxx for shard index.
// Otherwise, if the condition is  `WHERE a = 1`, `WHERE a = 1 AND b = 10`, `WHERE a IN (1, 2, 3)`......,
// call the function AddExprPrefix4CNFCond to add prefix expression for shard index.
func (adder *exprPrefixAdder) addExprPrefix4ShardIndex() ([]expression.Expression, error) {
	if len(adder.OrigConds) == 1 {
		if sf, ok := adder.OrigConds[0].(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			return adder.addExprPrefix4DNFCond(sf)
		}
	}
	return adder.addExprPrefix4CNFCond(adder.OrigConds)
}

// AddExprPrefix4CNFCond
// add the prefix expression for CNF condition, e.g. `WHERE a = 1`, `WHERE a = 1 AND b = 10`, ......
// @param[in] conds        the original condtion of the datasoure. e.g. `WHERE t1.a = 1 AND t1.b = 10 AND t2.a = 20`.
//
//	if current datasource is `t1`, conds is {t1.a = 1, t1.b = 10}. if current datasource is
//	`t2`, conds is {t2.a = 20}
//
// @return  -     the new condition after adding expression prefix
func (adder *exprPrefixAdder) addExprPrefix4CNFCond(conds []expression.Expression) ([]expression.Expression, error) {
	newCondtionds, err := ranger.AddExpr4EqAndInCondition(adder.sctx.GetRangerCtx(),
		conds, adder.cols)

	return newCondtionds, err
}

// AddExprPrefix4DNFCond
// add the prefix expression for DNF condition, e.g. `WHERE a = 1 OR a = 10`, ......
// The condition returned is `WHERE (tidb_shard(a) = 214 AND a = 1) OR (tidb_shard(a) = 142 AND a = 10)`
// @param[in] condition    the original condtion of the datasoure. e.g. `WHERE a = 1 OR a = 10`. condtion is `a = 1 OR a = 10`
// @return 	 -          the new condition after adding expression prefix. It's still a LogicOr expression.
func (adder *exprPrefixAdder) addExprPrefix4DNFCond(condition *expression.ScalarFunction) ([]expression.Expression, error) {
	var err error
	dnfItems := expression.FlattenDNFConditions(condition)
	newAccessItems := make([]expression.Expression, 0, len(dnfItems))

	exprCtx := adder.sctx.GetExprCtx()
	for _, item := range dnfItems {
		if sf, ok := item.(*expression.ScalarFunction); ok {
			var accesses []expression.Expression
			if sf.FuncName.L == ast.LogicAnd {
				cnfItems := expression.FlattenCNFConditions(sf)
				accesses, err = adder.addExprPrefix4CNFCond(cnfItems)
				if err != nil {
					return []expression.Expression{condition}, err
				}
				newAccessItems = append(newAccessItems, expression.ComposeCNFCondition(exprCtx, accesses...))
			} else if sf.FuncName.L == ast.EQ || sf.FuncName.L == ast.In {
				// only add prefix expression for EQ or IN function
				accesses, err = adder.addExprPrefix4CNFCond([]expression.Expression{sf})
				if err != nil {
					return []expression.Expression{condition}, err
				}
				newAccessItems = append(newAccessItems, expression.ComposeCNFCondition(exprCtx, accesses...))
			} else {
				newAccessItems = append(newAccessItems, item)
			}
		} else {
			newAccessItems = append(newAccessItems, item)
		}
	}

	return []expression.Expression{expression.ComposeDNFCondition(exprCtx, newAccessItems...)}, nil
}

// PredicatePushDown implements base.LogicalPlan PredicatePushDown interface.
func (p *LogicalCTE) PredicatePushDown(predicates []expression.Expression, _ *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	if p.cte.recursivePartLogicalPlan != nil {
		// Doesn't support recursive CTE yet.
		return predicates, p.Self()
	}
	if !p.cte.isOuterMostCTE {
		return predicates, p.Self()
	}
	pushedPredicates := make([]expression.Expression, len(predicates))
	copy(pushedPredicates, predicates)
	// The filter might change the correlated status of the cte.
	// We forbid the push down that makes the change for now.
	// Will support it later.
	if !p.cte.IsInApply {
		for i := len(pushedPredicates) - 1; i >= 0; i-- {
			if len(expression.ExtractCorColumns(pushedPredicates[i])) == 0 {
				continue
			}
			pushedPredicates = append(pushedPredicates[0:i], pushedPredicates[i+1:]...)
		}
	}
	if len(pushedPredicates) == 0 {
		p.cte.pushDownPredicates = append(p.cte.pushDownPredicates, expression.NewOne())
		return predicates, p.Self()
	}
	newPred := make([]expression.Expression, 0, len(predicates))
	for i := range pushedPredicates {
		newPred = append(newPred, pushedPredicates[i].Clone())
		ResolveExprAndReplace(newPred[i], p.cte.ColumnMap)
	}
	p.cte.pushDownPredicates = append(p.cte.pushDownPredicates, expression.ComposeCNFCondition(p.SCtx().GetExprCtx(), newPred...))
	return predicates, p.Self()
}

// PredicatePushDown implements the base.LogicalPlan interface.
// Currently, we only maintain the main query tree.
func (p *LogicalSequence) PredicatePushDown(predicates []expression.Expression, op *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan) {
	lastIdx := p.ChildLen() - 1
	remained, newLastChild := p.Children()[lastIdx].PredicatePushDown(predicates, op)
	p.SetChild(lastIdx, newLastChild)
	return remained, p
}
