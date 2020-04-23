// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

type ppdSolver struct{}

func (s *ppdSolver) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	_, p := lp.PredicatePushDown(nil)
	return p, nil
}

func addSelection(p LogicalPlan, child LogicalPlan, conditions []expression.Expression, chIdx int) {
	if len(conditions) == 0 {
		p.Children()[chIdx] = child
		return
	}
	conditions = expression.PropagateConstant(p.SCtx(), conditions)
	// Return table dual when filter is constant false or null.
	dual := Conds2TableDual(child, conditions)
	if dual != nil {
		p.Children()[chIdx] = dual
		return
	}
	selection := LogicalSelection{Conditions: conditions}.Init(p.SCtx(), p.SelectBlockOffset())
	selection.SetChildren(child)
	p.Children()[chIdx] = selection
}

// PredicatePushDown implements LogicalPlan interface.
func (p *baseLogicalPlan) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	if len(p.children) == 0 {
		return predicates, p.self
	}
	child := p.children[0]
	rest, newChild := child.PredicatePushDown(predicates)
	addSelection(p.self, newChild, rest, 0)
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
func (p *LogicalSelection) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	canBePushDown, canNotBePushDown := splitSetGetVarFunc(p.Conditions)
	retConditions, child := p.children[0].PredicatePushDown(append(canBePushDown, predicates...))
	retConditions = append(retConditions, canNotBePushDown...)
	if len(retConditions) > 0 {
		p.Conditions = expression.PropagateConstant(p.ctx, retConditions)
		// Return table dual when filter is constant false or null.
		dual := Conds2TableDual(p, p.Conditions)
		if dual != nil {
			return nil, dual
		}
		return nil, p
	}
	return nil, child
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalUnionScan) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	retainedPredicates, _ := p.children[0].PredicatePushDown(predicates)
	p.conditions = make([]expression.Expression, 0, len(predicates))
	p.conditions = append(p.conditions, predicates...)
	// The conditions in UnionScan is only used for added rows, so parent Selection should not be removed.
	return retainedPredicates, p
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (ds *DataSource) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	ds.allConds = predicates
	ds.pushedDownConds, predicates = expression.PushDownExprs(ds.ctx.GetSessionVars().StmtCtx, predicates, ds.ctx.GetClient(), kv.UnSpecified)
	return predicates, ds
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalTableDual) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	return predicates, p
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalJoin) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan) {
	simplifyOuterJoin(p, predicates)
	var equalCond []*expression.ScalarFunction
	var leftPushCond, rightPushCond, otherCond, leftCond, rightCond []expression.Expression
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		predicates = p.outerJoinPropConst(predicates)
		dual := Conds2TableDual(p, predicates)
		if dual != nil {
			return ret, dual
		}
		// Handle where conditions
		predicates = expression.ExtractFiltersFromDNFs(p.ctx, predicates)
		// Only derive left where condition, because right where condition cannot be pushed down
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(predicates, true, false)
		leftCond = leftPushCond
		// Handle join conditions, only derive right join condition, because left join condition cannot be pushed down
		_, derivedRightJoinCond := DeriveOtherConditions(p, p.children[0].Schema(), p.children[1].Schema(), false, true)
		rightCond = append(p.RightConditions, derivedRightJoinCond...)
		p.RightConditions = nil
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, rightPushCond...)
	case RightOuterJoin:
		predicates = p.outerJoinPropConst(predicates)
		dual := Conds2TableDual(p, predicates)
		if dual != nil {
			return ret, dual
		}
		// Handle where conditions
		predicates = expression.ExtractFiltersFromDNFs(p.ctx, predicates)
		// Only derive right where condition, because left where condition cannot be pushed down
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(predicates, false, true)
		rightCond = rightPushCond
		// Handle join conditions, only derive left join condition, because right join condition cannot be pushed down
		derivedLeftJoinCond, _ := DeriveOtherConditions(p, p.children[0].Schema(), p.children[1].Schema(), true, false)
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
	leftRet, lCh := p.children[0].PredicatePushDown(leftCond)
	rightRet, rCh := p.children[1].PredicatePushDown(rightCond)
	addSelection(p, lCh, leftRet, 0)
	addSelection(p, rCh, rightRet, 1)
	p.updateEQCond()
	p.mergeSchema()
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
		RetType:  expr.GetType(),
	}
	p.schema.Append(col)
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
func (p *LogicalProjection) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan) {
	canBePushed := make([]expression.Expression, 0, len(predicates))
	canNotBePushed := make([]expression.Expression, 0, len(predicates))
	for _, expr := range p.Exprs {
		if expression.HasAssignSetVarFunc(expr) {
			_, child := p.baseLogicalPlan.PredicatePushDown(nil)
			return predicates, child
		}
	}
	for _, cond := range predicates {
		newFilter := expression.ColumnSubstitute(cond, p.Schema(), p.Exprs)
		if !expression.HasGetSetVarFunc(newFilter) {
			canBePushed = append(canBePushed, expression.ColumnSubstitute(cond, p.Schema(), p.Exprs))
		} else {
			canNotBePushed = append(canNotBePushed, cond)
		}
	}
	remained, child := p.baseLogicalPlan.PredicatePushDown(canBePushed)
	return append(remained, canNotBePushed...), child
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalUnionAll) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan) {
	for i, proj := range p.children {
		newExprs := make([]expression.Expression, 0, len(predicates))
		newExprs = append(newExprs, predicates...)
		retCond, newChild := proj.PredicatePushDown(newExprs)
		addSelection(p, newChild, retCond, i)
	}
	return nil, p
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (la *LogicalAggregation) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan) {
	var condsToPush []expression.Expression
	exprsOriginal := make([]expression.Expression, 0, len(la.AggFuncs))
	for _, fun := range la.AggFuncs {
		exprsOriginal = append(exprsOriginal, fun.Args[0])
	}
	groupByColumns := expression.NewSchema(la.groupByCols...)
	for _, cond := range predicates {
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
	}
	la.baseLogicalPlan.PredicatePushDown(condsToPush)
	return ret, la
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalLimit) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	// Limit forbids any condition to push down.
	p.baseLogicalPlan.PredicatePushDown(nil)
	return predicates, p
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalMaxOneRow) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	// MaxOneRow forbids any condition to push down.
	p.baseLogicalPlan.PredicatePushDown(nil)
	return predicates, p
}

// DeriveOtherConditions given a LogicalJoin, check the OtherConditions to see if we can derive more
// conditions for left/right child pushdown.
func DeriveOtherConditions(
	p *LogicalJoin,
	leftSchema *expression.Schema,
	rightSchema *expression.Schema,
	deriveLeft bool,
	deriveRight bool) (leftCond []expression.Expression,
	rightCond []expression.Expression) {
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
	if isNullRejected(ctx, schema, expr) && !mysql.HasNotNullFlag(childCol.RetType.Flag) {
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
	if expression.ContainMutableConst(p.SCtx(), []expression.Expression{con}) {
		return nil
	}
	if isTrue, err := con.Value.ToBool(sc); (err == nil && isTrue == 0) || con.Value.IsNull() {
		dual := LogicalTableDual{}.Init(p.SCtx(), p.SelectBlockOffset())
		dual.SetSchema(p.Schema())
		return dual
	}
	return nil
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
func (p *LogicalWindow) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
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
	p.baseLogicalPlan.PredicatePushDown(canBePushed)
	return canNotBePushed, p
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalMemTable) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	if p.Extractor != nil {
		predicates = p.Extractor.Extract(p.ctx, p.schema, p.names, predicates)
	}
	return predicates, p.self
}

func (*ppdSolver) name() string {
	return "predicate_push_down"
}
