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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

type ppdSolver struct{}

func (s *ppdSolver) optimize(lp LogicalPlan, _ context.Context, _ *idAllocator) (LogicalPlan, error) {
	_, p, err := lp.PredicatePushDown(nil)
	return p, errors.Trace(err)
}

func addSelection(p Plan, child LogicalPlan, conditions []expression.Expression, allocator *idAllocator) error {
	conditions = expression.PropagateConstant(p.context(), conditions)
	selection := Selection{Conditions: conditions}.init(allocator, p.context())
	selection.SetSchema(child.Schema().Clone())
	return InsertPlan(p, child, selection)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Selection) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	retConditions, child, err := p.children[0].(LogicalPlan).PredicatePushDown(append(p.Conditions, predicates...))
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if len(retConditions) > 0 {
		p.Conditions = expression.PropagateConstant(p.ctx, retConditions)
		return nil, p, nil
	}
	err = RemovePlan(p)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return nil, child, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *DataSource) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	_, p.pushedDownConds, predicates = expression.ExpressionsToPB(p.ctx.GetSessionVars().StmtCtx, predicates, p.ctx.GetClient())
	return predicates, p, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *TableDual) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	return predicates, p, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalJoin) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan, err error) {
	err = outerJoinSimplify(p, predicates)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	groups, valid := tryToGetJoinGroup(p)
	if valid {
		e := joinReOrderSolver{allocator: p.allocator, ctx: p.ctx}
		e.reorderJoin(groups, predicates)
		newJoin := e.resultJoin
		if len(p.parents) > 0 {
			parent := p.parents[0]
			newJoin.SetParents(parent)
			err = parent.ReplaceChild(p, newJoin)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
		return newJoin.PredicatePushDown(predicates)
	}
	var leftCond, rightCond []expression.Expression
	retPlan = p
	leftPlan := p.children[0].(LogicalPlan)
	rightPlan := p.children[1].(LogicalPlan)
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
	leftRet, _, err1 := leftPlan.PredicatePushDown(leftCond)
	if err1 != nil {
		return nil, nil, errors.Trace(err1)
	}
	rightRet, _, err2 := rightPlan.PredicatePushDown(rightCond)
	if err2 != nil {
		return nil, nil, errors.Trace(err2)
	}
	if len(leftRet) > 0 {
		err2 = addSelection(p, leftPlan, leftRet, p.allocator)
		if err2 != nil {
			return nil, nil, errors.Trace(err2)
		}
	}
	if len(rightRet) > 0 {
		err2 = addSelection(p, rightPlan, rightRet, p.allocator)
		if err2 != nil {
			return nil, nil, errors.Trace(err2)
		}
	}
	p.updateEQCond()
	for _, eqCond := range p.EqualConditions {
		p.LeftJoinKeys = append(p.LeftJoinKeys, eqCond.GetArgs()[0].(*expression.Column))
		p.RightJoinKeys = append(p.RightJoinKeys, eqCond.GetArgs()[1].(*expression.Column))
	}
	p.mergeSchema()
	p.buildKeyInfo()
	return
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

func (p *Projection) appendExpr(expr expression.Expression) *expression.Column {
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

func (p *LogicalJoin) getProj(idx int) *Projection {
	child := p.children[idx]
	proj, ok := child.(*Projection)
	if ok {
		return proj
	}
	proj = Projection{Exprs: make([]expression.Expression, 0, child.Schema().Len())}.init(p.allocator, p.ctx)
	for _, col := range child.Schema().Columns {
		proj.Exprs = append(proj.Exprs, col.Clone())
	}
	proj.SetSchema(child.Schema().Clone())
	setParentAndChildren(proj, child)
	proj.SetParents(p)
	p.children[idx] = proj
	return proj
}

// outerJoinSimplify simplifies outer join.
func outerJoinSimplify(p *LogicalJoin, predicates []expression.Expression) error {
	var innerTable, outerTable LogicalPlan
	child1 := p.children[0].(LogicalPlan)
	child2 := p.children[1].(LogicalPlan)
	var fullConditions []expression.Expression
	if p.JoinType == LeftOuterJoin {
		innerTable = child2
		outerTable = child1
	} else if p.JoinType == RightOuterJoin || p.JoinType == InnerJoin {
		innerTable = child1
		outerTable = child2
	} else {
		return nil
	}
	// first simplify embedded outer join.
	// When trying to simplify an embedded outer join operation in a query,
	// we must take into account the join condition for the embedding outer join together with the WHERE condition.
	if innerPlan, ok := innerTable.(*LogicalJoin); ok {
		fullConditions = concatOnAndWhereConds(p, predicates)
		err := outerJoinSimplify(innerPlan, fullConditions)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if outerPlan, ok := outerTable.(*LogicalJoin); ok {
		if fullConditions != nil {
			fullConditions = concatOnAndWhereConds(p, predicates)
		}
		err := outerJoinSimplify(outerPlan, fullConditions)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if p.JoinType == InnerJoin {
		return nil
	}
	// then simplify embedding outer join.
	canBeSimplified := false
	for _, expr := range predicates {
		isOk, err := isNullRejected(p.ctx, innerTable.Schema(), expr)
		if err != nil {
			return errors.Trace(err)
		}
		if isOk {
			canBeSimplified = true
			break
		}
	}
	if canBeSimplified {
		p.JoinType = InnerJoin
	}
	return nil
}

// isNullRejected check whether a condition is null-rejected
// A condition would be null-rejected in one of following cases:
// If it is a predicate containing a reference to an inner table that evaluates to UNKNOWN or FALSE when one of its arguments is NULL.
// If it is a conjunction containing a null-rejected condition as a conjunct.
// If it is a disjunction of null-rejected conditions.
func isNullRejected(ctx context.Context, schema *expression.Schema, expr expression.Expression) (bool, error) {
	result, err := expression.EvaluateExprWithNull(ctx, schema, expr)
	if err != nil {
		return false, errors.Trace(err)
	}
	x, ok := result.(*expression.Constant)
	if !ok {
		return false, nil
	}
	sc := ctx.GetSessionVars().StmtCtx
	if x.Value.IsNull() {
		return true, nil
	} else if isTrue, err := x.Value.ToBool(sc); err != nil || isTrue == 0 {
		return true, errors.Trace(err)
	}
	return false, nil
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
func (p *Projection) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan, err error) {
	retPlan = p
	var push = make([]expression.Expression, 0, p.Schema().Len())
	for _, cond := range predicates {
		push = append(push, expression.ColumnSubstitute(cond, p.Schema(), p.Exprs))
	}
	child := p.children[0].(LogicalPlan)
	restConds, _, err1 := child.PredicatePushDown(push)
	if err1 != nil {
		return nil, nil, errors.Trace(err1)
	}
	if len(restConds) > 0 {
		err1 = addSelection(p, child, restConds, p.allocator)
		if err1 != nil {
			return nil, nil, errors.Trace(err1)
		}
	}
	return
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Union) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan, err error) {
	retPlan = p
	for _, proj := range p.children {
		newExprs := make([]expression.Expression, 0, len(predicates))
		for _, cond := range predicates {
			newCond := expression.ColumnSubstitute(cond, p.Schema(), expression.Column2Exprs(proj.Schema().Columns))
			newExprs = append(newExprs, newCond)
		}
		retCond, _, err := proj.(LogicalPlan).PredicatePushDown(newExprs)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if len(retCond) != 0 {
			err = addSelection(p, proj.(LogicalPlan), retCond, p.allocator)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
	}
	return
}

// getGbyColIndex gets the column's index in the group-by columns.
func (p *LogicalAggregation) getGbyColIndex(col *expression.Column) int {
	return expression.NewSchema(p.groupByCols...).ColumnIndex(col)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *LogicalAggregation) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan, err error) {
	retPlan = p
	var exprsOriginal []expression.Expression
	var condsToPush []expression.Expression
	for _, fun := range p.AggFuncs {
		exprsOriginal = append(exprsOriginal, fun.GetArgs()[0])
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
				if p.getGbyColIndex(col) == -1 {
					ok = false
					break
				}
			}
			if ok {
				newFunc := expression.ColumnSubstitute(cond.Clone(), p.Schema(), exprsOriginal)
				condsToPush = append(condsToPush, newFunc)
			} else {
				ret = append(ret, cond)
			}
		default:
			ret = append(ret, cond)
		}
	}
	_, _, err = p.baseLogicalPlan.PredicatePushDown(condsToPush)
	return ret, retPlan, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Limit) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	// Limit forbids any condition to push down.
	_, _, err := p.baseLogicalPlan.PredicatePushDown(nil)
	return predicates, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *MaxOneRow) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	// MaxOneRow forbids any condition to push down.
	_, _, err := p.baseLogicalPlan.PredicatePushDown(nil)
	return predicates, p, errors.Trace(err)
}
