// Copyright 2015 PingCAP, Inc.
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

func removePlan(p Plan) error {
	parents := p.GetParents()
	children := p.GetChildren()
	if len(parents) != 1 || len(children) != 1 {
		return SystemInternalErrorType.Gen("can't remove this plan")
	}
	parent, child := parents[0], children[0]
	err := parent.ReplaceChild(p, child)
	if err != nil {
		return errors.Trace(err)
	}
	err = child.ReplaceParent(p, parent)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func addFilter(p Plan, child Plan, conditions []ast.ExprNode) error {
	filter := &Filter{Conditions: conditions}
	filter.AddChild(child)
	filter.AddParent(p)
	err := child.ReplaceParent(p, filter)
	if err != nil {
		return errors.Trace(err)
	}
	err = p.ReplaceChild(child, filter)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

type columnSubstitute struct {
	fields []*ast.ResultField
}

func (cl *columnSubstitute) Enter(inNode ast.Node) (node ast.Node, ok bool) {
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

func (cl *columnSubstitute) Leave(inNode ast.Node) (node ast.Node, ok bool) {
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
func containsSubq(cond ast.ExprNode) (ok bool) {
	switch v := cond.(type) {
	case *ast.ValueExpr, *ast.ColumnNameExpr:
		return false
	case *ast.FuncCallExpr:
		for _, arg := range v.Args {
			if containsSubq(arg) {
				return true
			}
		}
		return false
	case *ast.BinaryOperationExpr:
		return containsSubq(v.L) || containsSubq(v.R)
	case *ast.UnaryOperationExpr:
		return containsSubq(v.V)
	default:
		return true
	}
}

func extractCond(conds []ast.ExprNode) (ret []ast.ExprNode, subq []ast.ExprNode) {
	for _, cond := range conds {
		if containsSubq(cond) {
			subq = append(subq, cond)
		} else {
			ret = append(ret, cond)
		}
	}
	return ret, subq
}

// PredicatePushDown applies predicate push down to all kinds of plans, expected aggregation and union.
func PredicatePushDown(p Plan, predicates []ast.ExprNode) (ret []ast.ExprNode, err error) {
	if len(p.GetChildren()) == 0 {
		return predicates, nil
	}
	switch v := p.(type) {
	case *TableScan:
		v.attachCondition(predicates)
		return predicates, nil
	case *Filter:
		conditions, subq := extractCond(v.Conditions)
		retConditions, err1 := PredicatePushDown(p.GetChildByIndex(0), append(conditions, predicates...))
		retConditions = append(retConditions, subq...)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if len(retConditions) > 0 {
			v.Conditions = retConditions
		} else {
			err1 = removePlan(p)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
		}
		return ret, nil
	case *Join:
		//todo: add null rejecter
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
		cs := &columnSubstitute{fields: v.Fields()}
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
		//todo: support union and sub queries when abandon result field.
		for _, child := range v.GetChildren() {
			_, err = PredicatePushDown(child, []ast.ExprNode{})
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		return predicates, nil
	}
}
