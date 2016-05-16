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
	"github.com/pingcap/tidb/ast"
)

func addFilter(p Plan, child Plan, conditions []ast.ExprNode) error {
	filter := &Filter{Conditions: conditions}
	return InsertPlan(child, p, filter)
}

// columnSubstituor substitutes the columns in filter to expressions in select fields.
// e.g. Filter(a < 10) <- SelectFields(1 as a) ==> Select(1 as a) <- Filter(1 < 10).
type columnSubstitutor struct {
	fields []*ast.ResultField
}

func (cl *columnSubstitutor) Enter(inNode ast.Node) (node ast.Node, ok bool) {
	switch v := inNode.(type) {
	case *ast.ColumnNameExpr:
		match := false
		for _, field := range cl.fields {
			if field == v.Refer {
				switch field.Expr.(type) {
				case *ast.ColumnNameExpr:
				case *ast.ValueExpr:
				default:
					return nil, false
				}
				match = true
			}
		}
		return node, match
	}
	return inNode, true
}

func (cl *columnSubstitutor) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	switch v := inNode.(type) {
	case *ast.ColumnNameExpr:
		for _, field := range cl.fields {
			if v.Refer == field {
				return field.Expr, true
			}
		}
	}
	return inNode, true
}

// PredicatePushDown applies predicate push down to all kinds of plans, except aggregation and union.
func PredicatePushDown(p Plan, predicates []ast.ExprNode) (ret []ast.ExprNode, err error) {
	if len(p.GetChildren()) == 0 {
		return predicates, nil
	}
	switch v := p.(type) {
	case *TableScan:
		v.attachCondition(predicates)
		return predicates, nil
	case *Filter:
		conditions := v.Conditions
		retConditions, err1 := PredicatePushDown(p.GetChildByIndex(0), append(conditions, predicates...))
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if len(retConditions) > 0 {
			v.Conditions = retConditions
		} else {
			err1 = RemovePlan(p)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
		}
		return ret, nil
	case *Join:
		//TODO: add null rejecter
		var leftCond, rightCond []ast.ExprNode
		leftPlan := v.GetChildByIndex(0)
		rightPlan := v.GetChildByIndex(1)
		equalCond, leftPushCond, rightPushCond, otherCond := extractOnCondition(predicates, leftPlan, rightPlan)
		if v.JoinType == LeftOuterJoin {
			rightCond = v.RightConditions
			leftCond = leftPushCond
			ret = append(equalCond, otherCond...)
			ret = append(ret, rightPushCond...)
		} else if v.JoinType == RightOuterJoin {
			leftCond = v.LeftConditions
			rightCond = rightPushCond
			ret = append(equalCond, otherCond...)
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
		err2 = addFilter(p, leftPlan, leftRet)
		if err2 != nil {
			return nil, errors.Trace(err2)
		}
		err2 = addFilter(p, rightPlan, rightRet)
		if err2 != nil {
			return nil, errors.Trace(err2)
		}
		if v.JoinType == InnerJoin {
			v.EqualConditions = append(v.EqualConditions, equalCond...)
			v.OtherConditions = append(v.OtherConditions, otherCond...)
		}
		return ret, nil
	case *SelectFields:
		cs := &columnSubstitutor{fields: v.Fields()}
		var push []ast.ExprNode
		for _, cond := range predicates {
			cond, ok := cond.Accept(cs)
			cond1, _ := cond.(ast.ExprNode)
			if ok {
				push = append(push, cond1)
			} else {
				ret = append(ret, cond1)
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
		return ret, nil
	case *Sort, *Limit, *Distinct:
		rest, err1 := PredicatePushDown(p.GetChildByIndex(0), predicates)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if len(rest) > 0 {
			addFilter(p, p.GetChildByIndex(0), rest)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
		}
		return ret, nil
	default:
		//TODO: support union and sub queries when abandon result field.
		for _, child := range v.GetChildren() {
			_, err = PredicatePushDown(child, []ast.ExprNode{})
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		return predicates, nil
	}
}
