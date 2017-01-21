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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
)

func addSelection(p Plan, child LogicalPlan, conditions []expression.Expression, allocator *idAllocator) error {
	conditions = expression.PropagateConstant(p.context(), conditions)
	selection := &Selection{
		Conditions:      conditions,
		baseLogicalPlan: newBaseLogicalPlan(Sel, allocator)}
	selection.self = selection
	selection.initIDAndContext(p.context())
	selection.SetSchema(child.GetSchema().Clone())
	return InsertPlan(p, child, selection)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Selection) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	retConditions, child, err := p.GetChildByIndex(0).(LogicalPlan).PredicatePushDown(append(p.Conditions, predicates...))
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
	return predicates, p, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *TableDual) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	return predicates, p, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Join) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan, err error) {
	err = outerJoinSimplify(p, predicates)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	groups, valid := tryToGetJoinGroup(p)
	if valid {
		e := joinReOrderSolver{allocator: p.allocator}
		e.reorderJoin(groups, predicates)
		newJoin := e.resultJoin
		parent := p.parents[0]
		newJoin.SetParents(parent)
		parent.ReplaceChild(p, newJoin)
		return newJoin.PredicatePushDown(predicates)
	}
	var leftCond, rightCond []expression.Expression
	retPlan = p
	leftPlan := p.GetChildByIndex(0).(LogicalPlan)
	rightPlan := p.GetChildByIndex(1).(LogicalPlan)
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
	case LeftOuterJoin, SemiJoinWithAux:
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
	case SemiJoin:
		equalCond, leftPushCond, rightPushCond, otherCond = extractOnCondition(predicates, leftPlan, rightPlan)
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
	return
}

// outerJoinSimplify simplifies outer join.
func outerJoinSimplify(p *Join, predicates []expression.Expression) error {
	var innerTable, outerTable LogicalPlan
	child1 := p.GetChildByIndex(0).(LogicalPlan)
	child2 := p.GetChildByIndex(1).(LogicalPlan)
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
	if innerPlan, ok := innerTable.(*Join); ok {
		fullConditions = concatOnAndWhereConds(p, predicates)
		err := outerJoinSimplify(innerPlan, fullConditions)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if outerPlan, ok := outerTable.(*Join); ok {
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
		isOk, err := isNullRejected(p.ctx, innerTable.GetSchema(), expr)
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
func isNullRejected(ctx context.Context, schema expression.Schema, expr expression.Expression) (bool, error) {
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
func concatOnAndWhereConds(join *Join, predicates []expression.Expression) []expression.Expression {
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
	var push []expression.Expression
	for _, cond := range predicates {
		canSubstitute := true
		extractedCols := expression.ExtractColumns(cond)
		for _, col := range extractedCols {
			id := p.GetSchema().GetColumnIndex(col)
			if _, ok := p.Exprs[id].(*expression.ScalarFunction); ok {
				canSubstitute = false
				break
			}
		}
		if canSubstitute {
			push = append(push, expression.ColumnSubstitute(cond, p.GetSchema(), p.Exprs))
		} else {
			ret = append(ret, cond)
		}
	}
	child := p.GetChildByIndex(0).(LogicalPlan)
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
			newCond := expression.ColumnSubstitute(cond, p.GetSchema(), expression.Column2Exprs(proj.GetSchema().Columns))
			newExprs = append(newExprs, newCond)
		}
		retCond, _, err := proj.(LogicalPlan).PredicatePushDown(newExprs)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if len(retCond) != 0 {
			addSelection(p, proj.(LogicalPlan), retCond, p.allocator)
		}
	}
	return
}

// getGbyColIndex gets the column's index in the group-by columns.
func (p *Aggregation) getGbyColIndex(col *expression.Column) int {
	return expression.NewSchema(p.groupByCols).GetColumnIndex(col)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Aggregation) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan, err error) {
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
				newFunc := expression.ColumnSubstitute(cond.Clone(), p.GetSchema(), exprsOriginal)
				condsToPush = append(condsToPush, newFunc)
			} else {
				ret = append(ret, cond)
			}
		default:
			ret = append(ret, cond)
		}
	}
	p.baseLogicalPlan.PredicatePushDown(condsToPush)
	return
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Apply) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan, err error) {
	child := p.GetChildByIndex(0).(LogicalPlan)
	var push []expression.Expression
	for _, cond := range predicates {
		extractedCols := expression.ExtractColumns(cond)
		canPush := true
		for _, col := range extractedCols {
			if child.GetSchema().GetColumnIndex(col) == -1 {
				canPush = false
				break
			}
		}
		if canPush {
			push = append(push, cond)
		} else {
			ret = append(ret, cond)
		}
	}
	childRet, _, err := child.PredicatePushDown(push)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	_, p.children[1], err = p.children[1].(LogicalPlan).PredicatePushDown(nil)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return append(ret, childRet...), p, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Limit) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	// Limit forbids any condition to push down.
	_, _, err := p.baseLogicalPlan.PredicatePushDown(nil)
	return predicates, p, errors.Trace(err)
}
