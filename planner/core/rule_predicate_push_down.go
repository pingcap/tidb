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

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

type ppdSolver struct{}

// exprPrefixAdder is the wrapper struct to add tidb_shard(x) = val for `OrigConds`
// `cols` is the index columns for a unique shard index
type exprPrefixAdder struct {
	sctx      sessionctx.Context
	OrigConds []expression.Expression
	cols      []*expression.Column
	lengths   []int
}

func (s *ppdSolver) optimize(ctx context.Context, lp LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	_, p := lp.PredicatePushDown(nil, opt)
	return p, nil
}

func addSelection(p LogicalPlan, child LogicalPlan, conditions []expression.Expression, chIdx int, opt *logicalOptimizeOp) {
	if len(conditions) == 0 {
		p.Children()[chIdx] = child
		return
	}
	conditions = expression.PropagateConstant(p.SCtx(), conditions)
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
	selection := LogicalSelection{Conditions: conditions}.Init(p.SCtx(), p.SelectBlockOffset())
	selection.SetChildren(child)
	p.Children()[chIdx] = selection
	appendAddSelectionTraceStep(p, child, selection, opt)
}

// PredicatePushDown implements LogicalPlan interface.
func (p *baseLogicalPlan) PredicatePushDown(predicates []expression.Expression, opt *logicalOptimizeOp) ([]expression.Expression, LogicalPlan) {
	if len(p.children) == 0 {
		return predicates, p.self
	}
	child := p.children[0]
	rest, newChild := child.PredicatePushDown(predicates, opt)
	addSelection(p.self, newChild, rest, 0, opt)
	return nil, p.self
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

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalSelection) PredicatePushDown(predicates []expression.Expression, opt *logicalOptimizeOp) ([]expression.Expression, LogicalPlan) {
	predicates = DeleteTrueExprs(p, predicates)
	p.Conditions = DeleteTrueExprs(p, p.Conditions)
	var child LogicalPlan
	var retConditions []expression.Expression
	var originConditions []expression.Expression
	canBePushDown, canNotBePushDown := splitSetGetVarFunc(p.Conditions)
	originConditions = canBePushDown
	retConditions, child = p.children[0].PredicatePushDown(append(canBePushDown, predicates...), opt)
	retConditions = append(retConditions, canNotBePushDown...)
	if len(retConditions) > 0 {
		p.Conditions = expression.PropagateConstant(p.ctx, retConditions)
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

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalUnionScan) PredicatePushDown(predicates []expression.Expression, opt *logicalOptimizeOp) ([]expression.Expression, LogicalPlan) {
	retainedPredicates, _ := p.children[0].PredicatePushDown(predicates, opt)
	p.conditions = make([]expression.Expression, 0, len(predicates))
	p.conditions = append(p.conditions, predicates...)
	// The conditions in UnionScan is only used for added rows, so parent Selection should not be removed.
	return retainedPredicates, p
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (ds *DataSource) PredicatePushDown(predicates []expression.Expression, opt *logicalOptimizeOp) ([]expression.Expression, LogicalPlan) {
	predicates = expression.PropagateConstant(ds.ctx, predicates)
	predicates = DeleteTrueExprs(ds, predicates)
	// Add tidb_shard() prefix to the condtion for shard index in some scenarios
	// TODO: remove it to the place building logical plan
	predicates = ds.AddPrefix4ShardIndexes(ds.ctx, predicates)
	ds.allConds = predicates
	ds.pushedDownConds, predicates = expression.PushDownExprs(ds.ctx.GetSessionVars().StmtCtx, predicates, ds.ctx.GetClient(), kv.UnSpecified)
	appendDataSourcePredicatePushDownTraceStep(ds, opt)
	return predicates, ds
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalTableDual) PredicatePushDown(predicates []expression.Expression, opt *logicalOptimizeOp) ([]expression.Expression, LogicalPlan) {
	return predicates, p
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalJoin) PredicatePushDown(predicates []expression.Expression, opt *logicalOptimizeOp) (ret []expression.Expression, retPlan LogicalPlan) {
	simplifyOuterJoin(p, predicates)
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
		predicates = expression.ExtractFiltersFromDNFs(p.ctx, predicates)
		// Only derive left where condition, because right where condition cannot be pushed down
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(predicates, true, false)
		leftCond = leftPushCond
		// Handle join conditions, only derive right join condition, because left join condition cannot be pushed down
		_, derivedRightJoinCond := DeriveOtherConditions(
			p, p.children[0].Schema(), p.children[1].Schema(), false, true)
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
		predicates = expression.ExtractFiltersFromDNFs(p.ctx, predicates)
		// Only derive right where condition, because left where condition cannot be pushed down
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(predicates, false, true)
		rightCond = rightPushCond
		// Handle join conditions, only derive left join condition, because right join condition cannot be pushed down
		derivedLeftJoinCond, _ := DeriveOtherConditions(
			p, p.children[0].Schema(), p.children[1].Schema(), true, false)
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
		tempCond = expression.ExtractFiltersFromDNFs(p.ctx, tempCond)
		tempCond = expression.PropagateConstant(p.ctx, tempCond)
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
		predicates = expression.PropagateConstant(p.ctx, predicates)
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
	leftCond = expression.RemoveDupExprs(p.ctx, leftCond)
	rightCond = expression.RemoveDupExprs(p.ctx, rightCond)
	leftRet, lCh := p.children[0].PredicatePushDown(leftCond, opt)
	rightRet, rCh := p.children[1].PredicatePushDown(rightCond, opt)
	addSelection(p, lCh, leftRet, 0, opt)
	addSelection(p, rCh, rightRet, 1, opt)
	p.updateEQCond()
	buildKeyInfo(p)
	return ret, p.self
}

// updateEQCond will extract the arguments of a equal condition that connect two expressions.
func (p *LogicalJoin) updateEQCond() {
	lChild, rChild := p.children[0], p.children[1]
	var lKeys, rKeys []expression.Expression
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
	if len(lKeys) > 0 {
		needLProj, needRProj := false, false
		for i := range lKeys {
			_, lOk := lKeys[i].(*expression.Column)
			_, rOk := rKeys[i].(*expression.Column)
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
		for i := range lKeys {
			lKey, rKey := lKeys[i], rKeys[i]
			if lProj != nil {
				lKey = lProj.appendExpr(lKey)
			}
			if rProj != nil {
				rKey = rProj.appendExpr(rKey)
			}
			eqCond := expression.NewFunctionInternal(p.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), lKey, rKey)
			p.EqualConditions = append(p.EqualConditions, eqCond.(*expression.ScalarFunction))
		}
	}
}

func (p *LogicalProjection) appendExpr(expr expression.Expression) *expression.Column {
	if col, ok := expr.(*expression.Column); ok {
		return col
	}
	expr = expression.ColumnSubstitute(expr, p.schema, p.Exprs)
	p.Exprs = append(p.Exprs, expr)

	col := &expression.Column{
		UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  expr.GetType().Clone(),
	}
	col.SetCoercibility(expr.Coercibility())
	col.SetRepertoire(expr.Repertoire())
	p.schema.Append(col)
	// reset ParseToJSONFlag in order to keep the flag away from json column
	if col.GetType().GetType() == mysql.TypeJSON {
		col.GetType().DelFlag(mysql.ParseToJSONFlag)
	}
	return col
}

func (p *LogicalJoin) getProj(idx int) *LogicalProjection {
	child := p.children[idx]
	proj, ok := child.(*LogicalProjection)
	if ok {
		return proj
	}
	proj = LogicalProjection{Exprs: make([]expression.Expression, 0, child.Schema().Len())}.Init(p.ctx, child.SelectBlockOffset())
	for _, col := range child.Schema().Columns {
		proj.Exprs = append(proj.Exprs, col)
	}
	proj.SetSchema(child.Schema().Clone())
	proj.SetChildren(child)
	p.children[idx] = proj
	return proj
}

// simplifyOuterJoin transforms "LeftOuterJoin/RightOuterJoin" to "InnerJoin" if possible.
func simplifyOuterJoin(p *LogicalJoin, predicates []expression.Expression) {
	if p.JoinType != LeftOuterJoin && p.JoinType != RightOuterJoin && p.JoinType != InnerJoin {
		return
	}

	innerTable := p.children[0]
	outerTable := p.children[1]
	if p.JoinType == LeftOuterJoin {
		innerTable, outerTable = outerTable, innerTable
	}

	// first simplify embedded outer join.
	if innerPlan, ok := innerTable.(*LogicalJoin); ok {
		simplifyOuterJoin(innerPlan, predicates)
	}
	if outerPlan, ok := outerTable.(*LogicalJoin); ok {
		simplifyOuterJoin(outerPlan, predicates)
	}

	if p.JoinType == InnerJoin {
		return
	}
	// then simplify embedding outer join.
	canBeSimplified := false
	for _, expr := range predicates {
		// avoid the case where the expr only refers to the schema of outerTable
		if expression.ExprFromSchema(expr, outerTable.Schema()) {
			continue
		}
		isOk := isNullRejected(p.ctx, innerTable.Schema(), expr)
		if isOk {
			canBeSimplified = true
			break
		}
	}
	if canBeSimplified {
		p.JoinType = InnerJoin
	}
}

// isNullRejected check whether a condition is null-rejected
// A condition would be null-rejected in one of following cases:
// If it is a predicate containing a reference to an inner table that evaluates to UNKNOWN or FALSE when one of its arguments is NULL.
// If it is a conjunction containing a null-rejected condition as a conjunct.
// If it is a disjunction of null-rejected conditions.
func isNullRejected(ctx sessionctx.Context, schema *expression.Schema, expr expression.Expression) bool {
	expr = expression.PushDownNot(ctx, expr)
	if expression.ContainOuterNot(expr) {
		return false
	}
	sc := ctx.GetSessionVars().StmtCtx
	sc.InNullRejectCheck = true
	result := expression.EvaluateExprWithNull(ctx, schema, expr)
	sc.InNullRejectCheck = false
	x, ok := result.(*expression.Constant)
	if !ok {
		return false
	}
	if x.Value.IsNull() {
		return true
	} else if isTrue, err := x.Value.ToBool(sc); err == nil && isTrue == 0 {
		return true
	}
	return false
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalProjection) PredicatePushDown(predicates []expression.Expression, opt *logicalOptimizeOp) (ret []expression.Expression, retPlan LogicalPlan) {
	canBePushed := make([]expression.Expression, 0, len(predicates))
	canNotBePushed := make([]expression.Expression, 0, len(predicates))
	for _, expr := range p.Exprs {
		if expression.HasAssignSetVarFunc(expr) {
			_, child := p.baseLogicalPlan.PredicatePushDown(nil, opt)
			return predicates, child
		}
	}
	for _, cond := range predicates {
		newFilter := expression.ColumnSubstitute(cond, p.Schema(), p.Exprs)
		if !expression.HasGetSetVarFunc(newFilter) {
			canBePushed = append(canBePushed, newFilter)
		} else {
			canNotBePushed = append(canNotBePushed, cond)
		}
	}
	remained, child := p.baseLogicalPlan.PredicatePushDown(canBePushed, opt)
	return append(remained, canNotBePushed...), child
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalUnionAll) PredicatePushDown(predicates []expression.Expression, opt *logicalOptimizeOp) (ret []expression.Expression, retPlan LogicalPlan) {
	for i, proj := range p.children {
		newExprs := make([]expression.Expression, 0, len(predicates))
		newExprs = append(newExprs, predicates...)
		retCond, newChild := proj.PredicatePushDown(newExprs, opt)
		addSelection(p, newChild, retCond, i, opt)
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
			newFunc := expression.ColumnSubstitute(cond, la.Schema(), exprsOriginal)
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
	for _, item := range subCNFItem {
		condsToPushForItem, retForItem := la.pushDownDNFPredicatesForAggregation(item, groupByColumns, exprsOriginal)
		if len(condsToPushForItem) > 0 {
			condsToPush = append(condsToPush, expression.ComposeDNFCondition(la.ctx, condsToPushForItem...))
		}
		if len(retForItem) > 0 {
			ret = append(ret, expression.ComposeDNFCondition(la.ctx, retForItem...))
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
	var condsToPush []expression.Expression
	var ret []expression.Expression
	subDNFItem := expression.SplitDNFItems(cond)
	if len(subDNFItem) == 1 {
		return la.pushDownPredicatesForAggregation(subDNFItem[0], groupByColumns, exprsOriginal)
	}
	for _, item := range subDNFItem {
		condsToPushForItem, retForItem := la.pushDownCNFPredicatesForAggregation(item, groupByColumns, exprsOriginal)
		if len(condsToPushForItem) > 0 {
			condsToPush = append(condsToPush, expression.ComposeCNFCondition(la.ctx, condsToPushForItem...))
		} else {
			return nil, []expression.Expression{cond}
		}
		if len(retForItem) > 0 {
			ret = append(ret, expression.ComposeCNFCondition(la.ctx, retForItem...))
		}
	}
	if len(ret) == 0 {
		// All the condition can be pushed down.
		return []expression.Expression{cond}, nil
	}
	dnfPushDownCond := expression.ComposeDNFCondition(la.ctx, condsToPush...)
	// Some condition can't be pushed down, we need to keep all the condition.
	return []expression.Expression{dnfPushDownCond}, []expression.Expression{cond}
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (la *LogicalAggregation) PredicatePushDown(predicates []expression.Expression, opt *logicalOptimizeOp) (ret []expression.Expression, retPlan LogicalPlan) {
	var condsToPush []expression.Expression
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
	la.baseLogicalPlan.PredicatePushDown(condsToPush, opt)
	return ret, la
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalLimit) PredicatePushDown(predicates []expression.Expression, opt *logicalOptimizeOp) ([]expression.Expression, LogicalPlan) {
	// Limit forbids any condition to push down.
	p.baseLogicalPlan.PredicatePushDown(nil, opt)
	return predicates, p
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalMaxOneRow) PredicatePushDown(predicates []expression.Expression, opt *logicalOptimizeOp) ([]expression.Expression, LogicalPlan) {
	// MaxOneRow forbids any condition to push down.
	p.baseLogicalPlan.PredicatePushDown(nil, opt)
	return predicates, p
}

// DeriveOtherConditions given a LogicalJoin, check the OtherConditions to see if we can derive more
// conditions for left/right child pushdown.
func DeriveOtherConditions(
	p *LogicalJoin, leftSchema *expression.Schema, rightSchema *expression.Schema,
	deriveLeft bool, deriveRight bool) (
	leftCond []expression.Expression, rightCond []expression.Expression) {

	isOuterSemi := (p.JoinType == LeftOuterSemiJoin) || (p.JoinType == AntiLeftOuterSemiJoin)
	for _, expr := range p.OtherConditions {
		if deriveLeft {
			leftRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(expr, leftSchema)
			if leftRelaxedCond != nil {
				leftCond = append(leftCond, leftRelaxedCond)
			}
			notNullExpr := deriveNotNullExpr(expr, leftSchema)
			if notNullExpr != nil {
				leftCond = append(leftCond, notNullExpr)
			}
		}
		if deriveRight {
			rightRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(expr, rightSchema)
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
			notNullExpr := deriveNotNullExpr(expr, rightSchema)
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
func deriveNotNullExpr(expr expression.Expression, schema *expression.Schema) expression.Expression {
	binop, ok := expr.(*expression.ScalarFunction)
	if !ok || len(binop.GetArgs()) != 2 {
		return nil
	}
	ctx := binop.GetCtx()
	arg0, lOK := binop.GetArgs()[0].(*expression.Column)
	arg1, rOK := binop.GetArgs()[1].(*expression.Column)
	if !lOK || !rOK {
		return nil
	}
	childCol := schema.RetrieveColumn(arg0)
	if childCol == nil {
		childCol = schema.RetrieveColumn(arg1)
	}
	if isNullRejected(ctx, schema, expr) && !mysql.HasNotNullFlag(childCol.RetType.GetFlag()) {
		return expression.BuildNotNullExpr(ctx, childCol)
	}
	return nil
}

// Conds2TableDual builds a LogicalTableDual if cond is constant false or null.
func Conds2TableDual(p LogicalPlan, conds []expression.Expression) LogicalPlan {
	if len(conds) != 1 {
		return nil
	}
	con, ok := conds[0].(*expression.Constant)
	if !ok {
		return nil
	}
	sc := p.SCtx().GetSessionVars().StmtCtx
	if expression.MaybeOverOptimized4PlanCache(p.SCtx(), []expression.Expression{con}) {
		return nil
	}
	if isTrue, err := con.Value.ToBool(sc); (err == nil && isTrue == 0) || con.Value.IsNull() {
		dual := LogicalTableDual{}.Init(p.SCtx(), p.SelectBlockOffset())
		dual.SetSchema(p.Schema())
		return dual
	}
	return nil
}

// DeleteTrueExprs deletes the surely true expressions
func DeleteTrueExprs(p LogicalPlan, conds []expression.Expression) []expression.Expression {
	newConds := make([]expression.Expression, 0, len(conds))
	for _, cond := range conds {
		con, ok := cond.(*expression.Constant)
		if !ok {
			newConds = append(newConds, cond)
			continue
		}
		if expression.MaybeOverOptimized4PlanCache(p.SCtx(), []expression.Expression{con}) {
			newConds = append(newConds, cond)
			continue
		}
		sc := p.SCtx().GetSessionVars().StmtCtx
		if isTrue, err := con.Value.ToBool(sc); err == nil && isTrue == 1 {
			continue
		}
		newConds = append(newConds, cond)
	}
	return newConds
}

// outerJoinPropConst propagates constant equal and column equal conditions over outer join.
func (p *LogicalJoin) outerJoinPropConst(predicates []expression.Expression) []expression.Expression {
	outerTable := p.children[0]
	innerTable := p.children[1]
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
	joinConds, predicates = expression.PropConstOverOuterJoin(p.ctx, joinConds, predicates, outerTable.Schema(), innerTable.Schema(), nullSensitive)
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

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalWindow) PredicatePushDown(predicates []expression.Expression, opt *logicalOptimizeOp) ([]expression.Expression, LogicalPlan) {
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
	p.baseLogicalPlan.PredicatePushDown(canBePushed, opt)
	return canNotBePushed, p
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalMemTable) PredicatePushDown(predicates []expression.Expression, opt *logicalOptimizeOp) ([]expression.Expression, LogicalPlan) {
	if p.Extractor != nil {
		predicates = p.Extractor.Extract(p.ctx, p.schema, p.names, predicates)
	}
	return predicates, p.self
}

func (*ppdSolver) name() string {
	return "predicate_push_down"
}

func appendTableDualTraceStep(replaced LogicalPlan, dual LogicalPlan, conditions []expression.Expression, opt *logicalOptimizeOp) {
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
	opt.appendStepToCurrent(dual.ID(), dual.TP(), reason, action)
}

func appendSelectionPredicatePushDownTraceStep(p *LogicalSelection, conditions []expression.Expression, opt *logicalOptimizeOp) {
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
			buffer.WriteString(fmt.Sprintf("] in %v_%v are pushed down", p.TP(), p.ID()))
			return buffer.String()
		}
	}
	opt.appendStepToCurrent(p.ID(), p.TP(), reason, action)
}

func appendDataSourcePredicatePushDownTraceStep(ds *DataSource, opt *logicalOptimizeOp) {
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
		buffer.WriteString(fmt.Sprintf("] are pushed down across %v_%v", ds.TP(), ds.ID()))
		return buffer.String()
	}
	opt.appendStepToCurrent(ds.ID(), ds.TP(), reason, action)
}

func appendAddSelectionTraceStep(p LogicalPlan, child LogicalPlan, sel *LogicalSelection, opt *logicalOptimizeOp) {
	reason := func() string {
		return ""
	}
	action := func() string {
		return fmt.Sprintf("add %v_%v to connect %v_%v and %v_%v", sel.TP(), sel.ID(), p.TP(), p.ID(), child.TP(), child.ID())
	}
	opt.appendStepToCurrent(sel.ID(), sel.TP(), reason, action)
}

// AddPrefix4ShardIndexes add expression prefix for shard index. e.g. an index is test.uk(tidb_shard(a), a).
// It transforms the sql "SELECT * FROM test WHERE a = 10" to
// "SELECT * FROM test WHERE tidb_shard(a) = val AND a = 10", val is the value of tidb_shard(10).
// It also transforms the sql "SELECT * FROM test WHERE a IN (10, 20, 30)" to
// "SELECT * FROM test WHERE tidb_shard(a) = val1 AND a = 10 OR tidb_shard(a) = val2 AND a = 20"
// @param[in] conds            the original condtion of this datasource
// @retval - the new condition after adding expression prefix
func (ds *DataSource) AddPrefix4ShardIndexes(sc sessionctx.Context, conds []expression.Expression) []expression.Expression {
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

func (ds *DataSource) addExprPrefixCond(sc sessionctx.Context, path *util.AccessPath,
	conds []expression.Expression) ([]expression.Expression, error) {
	IdxCols, IdxColLens :=
		expression.IndexInfo2PrefixCols(ds.Columns, ds.schema.Columns, path.Index)
	if len(IdxCols) == 0 {
		return conds, nil
	}

	adder := &exprPrefixAdder{
		sctx:      sc,
		OrigConds: conds,
		cols:      IdxCols,
		lengths:   IdxColLens,
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
//                         if current datasource is `t1`, conds is {t1.a = 1, t1.b = 10}. if current datasource is
//                         `t2`, conds is {t2.a = 20}
// @return  -     the new condition after adding expression prefix
func (adder *exprPrefixAdder) addExprPrefix4CNFCond(conds []expression.Expression) ([]expression.Expression, error) {
	newCondtionds, err := ranger.AddExpr4EqAndInCondition(adder.sctx,
		conds, adder.cols)

	return newCondtionds, err
}

// AddExprPrefix4DNFCond
// add the prefix expression for DNF condition, e.g. `WHERE a = 1 OR a = 10`, ......
// The condition returned is `WHERE (tidb_shard(a) = 214 AND a = 1) OR (tidb_shard(a) = 142 AND a = 10)`
// @param[in] condition    the original condtion of the datasoure. e.g. `WHERE a = 1 OR a = 10`.
//                          condtion is `a = 1 OR a = 10`
// @return 	 -          the new condition after adding expression prefix. It's still a LogicOr expression.
func (adder *exprPrefixAdder) addExprPrefix4DNFCond(condition *expression.ScalarFunction) ([]expression.Expression, error) {
	var err error
	dnfItems := expression.FlattenDNFConditions(condition)
	newAccessItems := make([]expression.Expression, 0, len(dnfItems))

	for _, item := range dnfItems {
		if sf, ok := item.(*expression.ScalarFunction); ok {
			var accesses []expression.Expression
			if sf.FuncName.L == ast.LogicAnd {
				cnfItems := expression.FlattenCNFConditions(sf)
				accesses, err = adder.addExprPrefix4CNFCond(cnfItems)
				if err != nil {
					return []expression.Expression{condition}, err
				}
				newAccessItems = append(newAccessItems, expression.ComposeCNFCondition(adder.sctx, accesses...))
			} else if sf.FuncName.L == ast.EQ || sf.FuncName.L == ast.In {
				// only add prefix expression for EQ or IN function
				accesses, err = adder.addExprPrefix4CNFCond([]expression.Expression{sf})
				if err != nil {
					return []expression.Expression{condition}, err
				}
				newAccessItems = append(newAccessItems, expression.ComposeCNFCondition(adder.sctx, accesses...))
			} else {
				newAccessItems = append(newAccessItems, item)
			}
		} else {
			newAccessItems = append(newAccessItems, item)
		}
	}

	return []expression.Expression{expression.ComposeDNFCondition(adder.sctx, newAccessItems...)}, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalCTE) PredicatePushDown(predicates []expression.Expression, opt *logicalOptimizeOp) ([]expression.Expression, LogicalPlan) {
	if p.cte.recursivePartLogicalPlan != nil {
		// Doesn't support recursive CTE yet.
		return predicates, p.self
	}
	if !p.isOuterMostCTE {
		return predicates, p.self
	}
	if len(predicates) == 0 {
		p.cte.pushDownPredicates = append(p.cte.pushDownPredicates, expression.NewOne())
		return predicates, p.self
	}
	newPred := make([]expression.Expression, 0, len(predicates))
	for i := range predicates {
		newPred = append(newPred, predicates[i].Clone())
		ResolveExprAndReplace(newPred[i], p.cte.ColumnMap)
	}
	p.cte.pushDownPredicates = append(p.cte.pushDownPredicates, expression.ComposeCNFCondition(p.ctx, newPred...))
	return predicates, p.self
}
