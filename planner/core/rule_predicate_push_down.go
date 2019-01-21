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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

type ppdSolver struct{}

func (s *ppdSolver) optimize(lp LogicalPlan) (LogicalPlan, error) {
	_, p := lp.PredicatePushDown(nil)
	return p, nil
}

func addSelection(p LogicalPlan, child LogicalPlan, conditions []expression.Expression, chIdx int) {
	if len(conditions) == 0 {
		p.Children()[chIdx] = child
		return
	}
	conditions = expression.PropagateConstant(p.context(), conditions)
	// Return table dual when filter is constant false or null.
	dual := conds2TableDual(child, conditions)
	if dual != nil {
		p.Children()[chIdx] = dual
		return
	}
	selection := LogicalSelection{Conditions: conditions}.Init(p.context())
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
		dual := conds2TableDual(p, p.Conditions)
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
	_, ds.pushedDownConds, predicates = expression.ExpressionsToPB(ds.ctx.GetSessionVars().StmtCtx, predicates, ds.ctx.GetClient())
	return predicates, ds
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalTableDual) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	return predicates, p
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalJoin) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan) {
	simplifyOuterJoin(p, predicates)
	leftPlan := p.children[0]
	rightPlan := p.children[1]
	var equalCond []*expression.ScalarFunction
	var leftPushCond, rightPushCond, otherCond, leftCond, rightCond []expression.Expression
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		predicates = p.outerJoinPropConst(predicates)
		dual := conds2TableDual(p, predicates)
		if dual != nil {
			return ret, dual
		}
		// Handle where conditions
		predicates = expression.ExtractFiltersFromDNFs(p.ctx, predicates)
		// Only derive left where condition, because right where condition cannot be pushed down
		equalCond, leftPushCond, rightPushCond, otherCond = extractOnCondition(predicates, leftPlan, rightPlan, true, false)
		leftCond = leftPushCond
		// Handle join conditions, only derive right join condition, because left join condition cannot be pushed down
		_, derivedRightJoinCond := deriveOtherConditions(p, false, true)
		rightCond = append(p.RightConditions, derivedRightJoinCond...)
		p.RightConditions = nil
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, rightPushCond...)
	case RightOuterJoin:
		predicates = p.outerJoinPropConst(predicates)
		dual := conds2TableDual(p, predicates)
		if dual != nil {
			return ret, dual
		}
		// Handle where conditions
		predicates = expression.ExtractFiltersFromDNFs(p.ctx, predicates)
		// Only derive right where condition, because left where condition cannot be pushed down
		equalCond, leftPushCond, rightPushCond, otherCond = extractOnCondition(predicates, leftPlan, rightPlan, false, true)
		rightCond = rightPushCond
		// Handle join conditions, only derive left join condition, because right join condition cannot be pushed down
		derivedLeftJoinCond, _ := deriveOtherConditions(p, true, false)
		leftCond = append(p.LeftConditions, derivedLeftJoinCond...)
		p.LeftConditions = nil
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, leftPushCond...)
	case SemiJoin, AntiSemiJoin, InnerJoin:
		tempCond := make([]expression.Expression, 0, len(p.LeftConditions)+len(p.RightConditions)+len(p.EqualConditions)+len(p.OtherConditions)+len(predicates))
		tempCond = append(tempCond, p.LeftConditions...)
		tempCond = append(tempCond, p.RightConditions...)
		tempCond = append(tempCond, expression.ScalarFuncs2Exprs(p.EqualConditions)...)
		tempCond = append(tempCond, p.OtherConditions...)
		tempCond = append(tempCond, predicates...)
		tempCond = expression.ExtractFiltersFromDNFs(p.ctx, tempCond)
		tempCond = expression.PropagateConstant(p.ctx, tempCond)
		// Return table dual when filter is constant false or null. Not applicable to AntiSemiJoin.
		// TODO: For AntiSemiJoin, we can use outer plan to substitute LogicalJoin actually.
		if p.JoinType != AntiSemiJoin {
			dual := conds2TableDual(p, tempCond)
			if dual != nil {
				return ret, dual
			}
		}
		equalCond, leftPushCond, rightPushCond, otherCond = extractOnCondition(tempCond, leftPlan, rightPlan, true, true)
		p.LeftConditions = nil
		p.RightConditions = nil
		p.EqualConditions = equalCond
		p.OtherConditions = otherCond
		leftCond = leftPushCond
		rightCond = rightPushCond
	}
	leftRet, lCh := leftPlan.PredicatePushDown(leftCond)
	rightRet, rCh := rightPlan.PredicatePushDown(rightCond)
	addSelection(p, lCh, leftRet, 0)
	addSelection(p, rCh, rightRet, 1)
	p.updateEQCond()
	for _, eqCond := range p.EqualConditions {
		p.LeftJoinKeys = append(p.LeftJoinKeys, eqCond.GetArgs()[0].(*expression.Column))
		p.RightJoinKeys = append(p.RightJoinKeys, eqCond.GetArgs()[1].(*expression.Column))
	}
	p.mergeSchema()
	p.buildKeyInfo()
	return ret, p.self
}

// updateEQCond will extract the arguments of a equal condition that connect two expressions.
func (p *LogicalJoin) updateEQCond() {
	lChild, rChild := p.children[0], p.children[1]
	var lKeys, rKeys []expression.Expression
	for i := len(p.OtherConditions) - 1; i >= 0; i-- {
		need2Remove := false
		if eqCond, ok := p.OtherConditions[i].(*expression.ScalarFunction); ok && eqCond.FuncName.L == ast.EQ {
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
		lProj := p.getProj(0)
		rProj := p.getProj(1)
		for i := range lKeys {
			lKey := lProj.appendExpr(lKeys[i])
			rKey := rProj.appendExpr(rKeys[i])
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
		ColName:  model.NewCIStr(expr.String()),
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
	proj = LogicalProjection{Exprs: make([]expression.Expression, 0, child.Schema().Len())}.Init(p.ctx)
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
	expr = expression.PushDownNot(nil, expr, false)
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

// getGbyColIndex gets the column's index in the group-by columns.
func (la *LogicalAggregation) getGbyColIndex(col *expression.Column) int {
	return expression.NewSchema(la.groupByCols...).ColumnIndex(col)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (la *LogicalAggregation) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan) {
	var condsToPush []expression.Expression
	exprsOriginal := make([]expression.Expression, 0, len(la.AggFuncs))
	for _, fun := range la.AggFuncs {
		exprsOriginal = append(exprsOriginal, fun.Args[0])
	}
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
				if la.getGbyColIndex(col) == -1 {
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

// deriveOtherConditions given a LogicalJoin, check the OtherConditions to see if we can derive more
// conditions for left/right child pushdown.
func deriveOtherConditions(p *LogicalJoin, deriveLeft bool, deriveRight bool) (leftCond []expression.Expression,
	rightCond []expression.Expression) {
	leftPlan := p.children[0]
	rightPlan := p.children[1]
	for _, expr := range p.OtherConditions {
		if deriveLeft {
			leftRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(expr, leftPlan.Schema())
			if leftRelaxedCond != nil {
				leftCond = append(leftCond, leftRelaxedCond)
			}
		}
		if deriveRight {
			rightRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(expr, rightPlan.Schema())
			if rightRelaxedCond != nil {
				rightCond = append(rightCond, rightRelaxedCond)
			}
		}
	}
	return
}

// conds2TableDual builds a LogicalTableDual if cond is constant false or null.
func conds2TableDual(p LogicalPlan, conds []expression.Expression) LogicalPlan {
	if len(conds) != 1 {
		return nil
	}
	con, ok := conds[0].(*expression.Constant)
	if !ok {
		return nil
	}
	sc := p.context().GetSessionVars().StmtCtx
	if isTrue, err := con.Value.ToBool(sc); (err == nil && isTrue == 0) || con.Value.IsNull() {
		dual := LogicalTableDual{}.Init(p.context())
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
	joinConds, predicates = expression.PropConstOverOuterJoin(p.ctx, joinConds, predicates, outerTable.Schema(), innerTable.Schema())
	p.attachOnConds(joinConds)
	return predicates
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalWindow) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	// Window function forbids any condition to push down.
	p.baseLogicalPlan.PredicatePushDown(nil)
	return predicates, p
}
