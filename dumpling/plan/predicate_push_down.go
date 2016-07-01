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
	"github.com/pingcap/tidb/expression"
)

func addSelection(p LogicalPlan, child LogicalPlan, conditions []expression.Expression, allocator *idAllocator) error {
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
		return newExprs[id]
	case *expression.ScalarFunction:
		for i, arg := range v.Args {
			v.Args[i] = columnSubstitute(arg, schema, newExprs)
		}
	}
	return expr
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Selection) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, err error) {
	conditions := p.Conditions
	retConditions, err1 := p.GetChildByIndex(0).(LogicalPlan).PredicatePushDown(append(conditions, predicates...))
	if err1 != nil {
		return nil, errors.Trace(err1)
	}
	if len(retConditions) > 0 {
		p.Conditions = retConditions
	} else {
		if len(p.GetParents()) == 0 {
			return ret, nil
		}
		err1 = RemovePlan(p)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
	}
	return
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *NewTableScan) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, error) {
	return predicates, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *NewTableDual) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, error) {
	return predicates, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Join) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, err error) {
	//TODO: add null rejecter.
	var leftCond, rightCond []expression.Expression
	leftPlan := p.GetChildByIndex(0).(LogicalPlan)
	rightPlan := p.GetChildByIndex(1).(LogicalPlan)
	equalCond, leftPushCond, rightPushCond, otherCond := extractOnCondition(predicates, leftPlan, rightPlan)
	if p.JoinType == LeftOuterJoin {
		rightCond = p.RightConditions
		leftCond = leftPushCond
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, rightPushCond...)
	} else if p.JoinType == RightOuterJoin {
		leftCond = p.LeftConditions
		rightCond = rightPushCond
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, leftPushCond...)
	} else {
		leftCond = append(p.LeftConditions, leftPushCond...)
		rightCond = append(p.RightConditions, rightPushCond...)
	}
	leftRet, err1 := leftPlan.PredicatePushDown(leftCond)
	if err1 != nil {
		return nil, errors.Trace(err1)
	}
	rightRet, err2 := rightPlan.PredicatePushDown(rightCond)
	if err2 != nil {
		return nil, errors.Trace(err2)
	}
	if len(leftRet) > 0 {
		err2 = addSelection(p, leftPlan, leftRet, p.allocator)
		if err2 != nil {
			return nil, errors.Trace(err2)
		}
	}
	if len(rightRet) > 0 {
		err2 = addSelection(p, rightPlan, rightRet, p.allocator)
		if err2 != nil {
			return nil, errors.Trace(err2)
		}
	}
	if p.JoinType == InnerJoin {
		p.EqualConditions = append(p.EqualConditions, equalCond...)
		p.OtherConditions = append(p.OtherConditions, otherCond...)
	}
	return
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Projection) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, err error) {
	if len(p.GetChildren()) == 0 {
		return predicates, nil
	}
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
	restConds, err1 := child.PredicatePushDown(push)
	if err1 != nil {
		return nil, errors.Trace(err1)
	}
	if len(restConds) > 0 {
		err1 = addSelection(p, child, restConds, p.allocator)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
	}
	return
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *NewUnion) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, err error) {
	for _, proj := range p.Selects {
		newExprs := make([]expression.Expression, 0, len(predicates))
		for _, cond := range predicates {
			newCond := columnSubstitute(cond.DeepCopy(), p.GetSchema(), expression.Schema2Exprs(proj.GetSchema()))
			newExprs = append(newExprs, newCond)
		}
		retCond, err := proj.PredicatePushDown(newExprs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(retCond) != 0 {
			addSelection(p, proj, retCond, p.allocator)
		}
	}
	return
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Aggregation) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, err error) {
	// TODO: implement aggregation push down.
	return predicates, nil
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *Apply) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, err error) {
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
	childRet, err := child.PredicatePushDown(push)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return append(ret, childRet...), nil

}
