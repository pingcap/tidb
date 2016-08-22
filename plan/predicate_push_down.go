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
	//TODO: add null rejecter.
	rejectNull(p)	//https://dev.mysql.com/doc/refman/5.7/en/outer-join-simplification.html
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

// rejectNull simplify outer join
func rejectNull(p *Join) {

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
