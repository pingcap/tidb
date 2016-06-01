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
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
)

func addFilter(p Plan, child Plan, conditions []expression.Expression) error {
	filter := &Selection{Conditions: conditions}
	filter.SetSchema(child.GetSchema())
	return InsertPlan(p, child, filter)
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

// PredicatePushDown applies predicate push down to all kinds of plans, except aggregation and union.
func PredicatePushDown(p Plan, predicates []expression.Expression) (ret []expression.Expression, err error) {
	switch v := p.(type) {
	case *NewTableScan:
		return predicates, nil
	case *Selection:
		conditions := v.Conditions
		retConditions, err1 := PredicatePushDown(p.GetChildByIndex(0), append(conditions, predicates...))
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if len(retConditions) > 0 {
			v.Conditions = retConditions
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
	case *Join:
		//TODO: add null rejecter.
		var leftCond, rightCond []expression.Expression
		leftPlan := v.GetChildByIndex(0)
		rightPlan := v.GetChildByIndex(1)
		equalCond, leftPushCond, rightPushCond, otherCond := extractOnCondition(predicates, leftPlan, rightPlan)
		if v.JoinType == LeftOuterJoin {
			rightCond = v.RightConditions
			leftCond = leftPushCond
			ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
			ret = append(ret, rightPushCond...)
		} else if v.JoinType == RightOuterJoin {
			leftCond = v.LeftConditions
			rightCond = rightPushCond
			ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
			ret = append(ret, leftPushCond...)
		} else {
			leftCond = append(v.LeftConditions, leftPushCond...)
			rightCond = append(v.RightConditions, rightPushCond...)
		}
		leftRet, err1 := PredicatePushDown(leftPlan, leftCond)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		rightRet, err2 := PredicatePushDown(rightPlan, rightCond)
		if err2 != nil {
			return nil, errors.Trace(err2)
		}
		if len(leftRet) > 0 {
			err2 = addFilter(p, leftPlan, leftRet)
			if err2 != nil {
				return nil, errors.Trace(err2)
			}
		}
		if len(rightRet) > 0 {
			err2 = addFilter(p, rightPlan, rightRet)
			if err2 != nil {
				return nil, errors.Trace(err2)
			}
		}
		if v.JoinType == InnerJoin {
			v.EqualConditions = append(v.EqualConditions, equalCond...)
			v.OtherConditions = append(v.OtherConditions, otherCond...)
		}
		return
	case *Projection:
		if len(v.GetChildren()) == 0 {
			return predicates, nil
		}
		var push []expression.Expression
		for _, cond := range predicates {
			canSubstitute := true
			extractedCols := extractColumn(cond, make([]*expression.Column, 0))
			for _, col := range extractedCols {
				id := v.GetSchema().GetIndex(col)
				if _, ok := v.exprs[id].(*expression.ScalarFunction); ok {
					canSubstitute = false
					break
				}
			}
			if canSubstitute {
				push = append(push, columnSubstitute(cond, v.GetSchema(), v.exprs))
			} else {
				ret = append(ret, cond)
			}
		}
		restConds, err1 := PredicatePushDown(v.GetChildByIndex(0), push)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if len(restConds) > 0 {
			err1 = addFilter(v, v.GetChildByIndex(0), restConds)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
		}
		return
	case *Sort, *Limit, *Distinct:
		rest, err1 := PredicatePushDown(p.GetChildByIndex(0), predicates)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if len(rest) > 0 {
			err1 = addFilter(p, p.GetChildByIndex(0), rest)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
		}
		return
	case *Union:
		for _, proj := range v.Selects {
			newExprs := make([]expression.Expression, 0, len(predicates))
			for _, cond := range predicates {
				newCond := columnSubstitute(cond.DeepCopy(), v.GetSchema(), expression.Schema2Exprs(proj.GetSchema()))
				newExprs = append(newExprs, newCond)
			}
			retCond, err := PredicatePushDown(proj, newExprs)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if len(retCond) != 0 {
				addFilter(v, proj, retCond)
			}
		}
		return
	//TODO: support aggregation.
	default:
		return predicates, errors.Errorf("Unkown type %T.", v)
	}
}
