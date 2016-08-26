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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

func addSelection(p Plan, child LogicalPlan, conditions []expression.Expression, allocator *idAllocator) error {
	selection := &Selection{
		Conditions:      conditions,
		baseLogicalPlan: newBaseLogicalPlan(Sel, allocator)}
	selection.initID()
	selection.SetSchema(child.GetSchema().DeepCopy())
	return InsertPlan(p, child, selection)
}

// columnSubstitute substitutes the columns in filter to expressions in select fields.
// e.g. select * from (select b as a from t) k where a < 10 => select * from (select b as a from t where b < 10) k.
func columnSubstitute(expr expression.Expression, schema expression.Schema, newExprs []expression.Expression) expression.Expression {
	switch v := expr.(type) {
	case *expression.Column:
		id := schema.GetIndex(v)
		if id == -1 {
			log.Errorf("Can't find columns %s in schema %s", v.ToString(), schema.ToString())
		}
		return newExprs[id]
	case *expression.ScalarFunction:
		for i, arg := range v.Args {
			v.Args[i] = columnSubstitute(arg, schema, newExprs)
		}
	}
	return expr
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Selection) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retP LogicalPlan, err error) {
	conditions := p.Conditions
	retConditions, child, err1 := p.GetChildByIndex(0).(LogicalPlan).PredicatePushDown(append(conditions, predicates...))
	if err1 != nil {
		return nil, nil, errors.Trace(err1)
	}
	if len(retConditions) > 0 {
		p.Conditions = retConditions
		retP = p
	} else {
		err1 = RemovePlan(p)
		if err1 != nil {
			return nil, nil, errors.Trace(err1)
		}
		retP = child
	}
	return
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *DataSource) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	return predicates, p, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *NewTableDual) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
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
	equalCond, leftPushCond, rightPushCond, otherCond := extractOnCondition(predicates, leftPlan, rightPlan)
	if p.JoinType == LeftOuterJoin || p.JoinType == SemiJoinWithAux {
		rightCond = p.RightConditions
		p.RightConditions = nil
		leftCond = leftPushCond
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, rightPushCond...)
	} else if p.JoinType == RightOuterJoin {
		leftCond = p.LeftConditions
		p.LeftConditions = nil
		rightCond = rightPushCond
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, leftPushCond...)
	} else {
		leftCond = append(p.LeftConditions, leftPushCond...)
		rightCond = append(p.RightConditions, rightPushCond...)
		p.LeftConditions = nil
		p.RightConditions = nil
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
	if p.JoinType == InnerJoin {
		p.EqualConditions = append(p.EqualConditions, equalCond...)
		p.OtherConditions = append(p.OtherConditions, otherCond...)
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
		isOk, err := isNullRejected(innerTable.GetSchema(), expr)
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
func isNullRejected(schema expression.Schema, expr expression.Expression) (bool, error) {
	result, err := calculateResultOfExpression(schema, expr)
	if err != nil {
		return false, errors.Trace(err)
	}
	x, ok := result.(*expression.Constant)
	if !ok {
		return false, nil
	}
	if x.Value.IsNull() {
		return true, nil
	} else if isTrue, err := x.Value.ToBool(); err != nil || isTrue == 0 {
		return true, errors.Trace(err)
	}
	return false, nil
}

// calculateResultOfExpression set inner table columns in a expression as null and calculate the finally result of the scalar function.
func calculateResultOfExpression(schema expression.Schema, expr expression.Expression) (expression.Expression, error) {
	switch x := expr.(type) {
	case *expression.ScalarFunction:
		var err error
		args := make([]expression.Expression, len(x.Args))
		for i, arg := range x.Args {
			args[i], err = calculateResultOfExpression(schema, arg)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		return expression.NewFunction(x.FuncName.L, types.NewFieldType(mysql.TypeTiny), args...)
	case *expression.Column:
		if schema.GetIndex(x) == -1 {
			return x, nil
		}
		constant := &expression.Constant{Value: types.Datum{}}
		constant.Value.SetNull()
		return constant, nil
	default:
		return x.DeepCopy(), nil
	}
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
		extractedCols, _ := extractColumn(cond, nil, nil)
		for _, col := range extractedCols {
			id := p.GetSchema().GetIndex(col)
			if _, ok := p.Exprs[id].(*expression.ScalarFunction); ok {
				canSubstitute = false
				break
			}
		}
		if canSubstitute {
			push = append(push, columnSubstitute(cond, p.GetSchema(), p.Exprs))
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
func (p *NewUnion) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan, err error) {
	retPlan = p
	for _, proj := range p.children {
		newExprs := make([]expression.Expression, 0, len(predicates))
		for _, cond := range predicates {
			newCond := columnSubstitute(cond.DeepCopy(), p.GetSchema(), expression.Schema2Exprs(proj.GetSchema()))
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

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Aggregation) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	// TODO: implement aggregation push down.
	var condsToPush []expression.Expression
	for _, cond := range predicates {
		if _, ok := cond.(*expression.Constant); ok {
			condsToPush = append(condsToPush, cond)
		}
	}
	p.baseLogicalPlan.PredicatePushDown(condsToPush)
	return predicates, p, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Apply) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan LogicalPlan, err error) {
	child := p.GetChildByIndex(0).(LogicalPlan)
	var push []expression.Expression
	for _, cond := range predicates {
		extractedCols, _ := extractColumn(cond, nil, nil)
		canPush := true
		for _, col := range extractedCols {
			if child.GetSchema().GetIndex(col) == -1 {
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
	return append(ret, childRet...), p, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Limit) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	// Limit forbids any condition to push down.
	_, _, err := p.baseLogicalPlan.PredicatePushDown(nil)
	return predicates, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *NewSort) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Trim) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *MaxOneRow) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Exists) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Distinct) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Insert) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *SelectLock) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *NewUpdate) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *NewDelete) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	ret, _, err := p.baseLogicalPlan.PredicatePushDown(predicates)
	return ret, p, errors.Trace(err)
}
