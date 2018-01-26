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

package plan

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
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
	selection := LogicalSelection{Conditions: conditions}.init(p.context())
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

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalSelection) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	retConditions, child := p.children[0].PredicatePushDown(append(p.Conditions, predicates...))
	if len(retConditions) > 0 {
		p.Conditions = expression.PropagateConstant(p.ctx, retConditions)
		return nil, p
	}
	return nil, child
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalUnionScan) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	p.children[0].PredicatePushDown(predicates)
	p.conditions = predicates
	return nil, p
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (ds *DataSource) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	_, ds.pushedDownConds, predicates = expression.ExpressionsToPB(ds.ctx.GetSessionVars().StmtCtx, predicates, ds.ctx.GetClient())
	return predicates, ds
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalTableDual) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan) {
	return predicates, p
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalJoin) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan) {
	outerJoinSimplify(p, predicates)
	groups, valid := tryToGetJoinGroup(p)
	if valid {
		e := joinReOrderSolver{ctx: p.ctx}
		e.reorderJoin(groups, predicates)
		newJoin := e.resultJoin
		return newJoin.PredicatePushDown(predicates)
	}
	var leftCond, rightCond []expression.Expression
	leftPlan := p.children[0]
	rightPlan := p.children[1]
	var (
		equalCond                              []*expression.ScalarFunction
		leftPushCond, rightPushCond, otherCond []expression.Expression
	)
	if p.JoinType != InnerJoin {
		equalCond, leftPushCond, rightPushCond, otherCond = extractOnCondition(predicates, leftPlan, rightPlan)
	} else {
		tempCond := make([]expression.Expression, 0, len(p.LeftConditions)+len(p.RightConditions)+len(p.EqualConditions)+len(p.OtherConditions)+len(predicates))
		tempCond = append(tempCond, p.LeftConditions...)
		tempCond = append(tempCond, p.RightConditions...)
		tempCond = append(tempCond, expression.ScalarFuncs2Exprs(p.EqualConditions)...)
		tempCond = append(tempCond, p.OtherConditions...)
		tempCond = append(tempCond, predicates...)
		equalCond, leftPushCond, rightPushCond, otherCond = extractOnCondition(expression.PropagateConstant(p.ctx, tempCond), leftPlan, rightPlan)
	}
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		rightCond = p.RightConditions
		p.RightConditions = nil
		leftCond = leftPushCond
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, rightPushCond...)
	case RightOuterJoin:
		leftCond = p.LeftConditions
		p.LeftConditions = nil
		rightCond = rightPushCond
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, leftPushCond...)
	case SemiJoin, AntiSemiJoin:
		_, leftPushCond, rightPushCond, _ = extractOnCondition(predicates, leftPlan, rightPlan)
		leftCond = append(p.LeftConditions, leftPushCond...)
		rightCond = append(p.RightConditions, rightPushCond...)
		p.LeftConditions = nil
		p.RightConditions = nil
	case InnerJoin:
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
		FromID:   p.id,
		Position: p.schema.Len(),
		ColName:  model.NewCIStr(expr.String()),
		RetType:  expr.GetType(),
	}
	p.schema.Append(col)
	return col.Clone().(*expression.Column)
}

func (p *LogicalJoin) getProj(idx int) *LogicalProjection {
	child := p.children[idx]
	proj, ok := child.(*LogicalProjection)
	if ok {
		return proj
	}
	proj = LogicalProjection{Exprs: make([]expression.Expression, 0, child.Schema().Len())}.init(p.ctx)
	for _, col := range child.Schema().Columns {
		proj.Exprs = append(proj.Exprs, col.Clone())
	}
	proj.SetSchema(child.Schema().Clone())
	proj.SetChildren(child)
	p.children[idx] = proj
	return proj
}

// outerJoinSimplify simplifies outer join.
func outerJoinSimplify(p *LogicalJoin, predicates []expression.Expression) {
	var innerTable, outerTable LogicalPlan
	child1 := p.children[0]
	child2 := p.children[1]
	var fullConditions []expression.Expression
	if p.JoinType == LeftOuterJoin {
		innerTable = child2
		outerTable = child1
	} else if p.JoinType == RightOuterJoin || p.JoinType == InnerJoin {
		innerTable = child1
		outerTable = child2
	} else {
		return
	}
	// first simplify embedded outer join.
	// When trying to simplify an embedded outer join operation in a query,
	// we must take into account the join condition for the embedding outer join together with the WHERE condition.
	if innerPlan, ok := innerTable.(*LogicalJoin); ok {
		fullConditions = concatOnAndWhereConds(p, predicates)
		outerJoinSimplify(innerPlan, fullConditions)
	}
	if outerPlan, ok := outerTable.(*LogicalJoin); ok {
		if fullConditions != nil {
			fullConditions = concatOnAndWhereConds(p, predicates)
		}
		outerJoinSimplify(outerPlan, fullConditions)
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
func isNullRejected(ctx context.Context, schema *expression.Schema, expr expression.Expression) bool {
	result := expression.EvaluateExprWithNull(ctx, schema, expr)
	x, ok := result.(*expression.Constant)
	if !ok {
		return false
	}
	sc := ctx.GetSessionVars().StmtCtx
	if x.Value.IsNull() {
		return true
	} else if isTrue, err := x.Value.ToBool(sc); err != nil || isTrue == 0 {
		return true
	}
	return false
}

// concatOnAndWhereConds concatenate ON conditions with WHERE conditions.
func concatOnAndWhereConds(join *LogicalJoin, predicates []expression.Expression) []expression.Expression {
	equalConds, leftConds, rightConds, otherConds := join.EqualConditions, join.LeftConditions, join.RightConditions, join.OtherConditions
	ans := make([]expression.Expression, 0, len(equalConds)+len(leftConds)+len(rightConds)+len(predicates))
	for _, v := range equalConds {
		ans = append(ans, v)
	}
	ans = append(ans, leftConds...)
	ans = append(ans, rightConds...)
	ans = append(ans, otherConds...)
	ans = append(ans, predicates...)
	return ans
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalProjection) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan) {
	var push = make([]expression.Expression, 0, p.Schema().Len())
	for _, cond := range predicates {
		push = append(push, expression.ColumnSubstitute(cond, p.Schema(), p.Exprs))
	}
	return p.baseLogicalPlan.PredicatePushDown(push)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalUnionAll) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan) {
	for i, proj := range p.children {
		newExprs := make([]expression.Expression, 0, len(predicates))
		for _, cond := range predicates {
			newExprs = append(newExprs, cond.Clone())
		}
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
				newFunc := expression.ColumnSubstitute(cond.Clone(), la.Schema(), exprsOriginal)
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
