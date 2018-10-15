// Copyright 2018 PingCAP, Inc.
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

package cascades

import (
	"container/list"
)

// ExprIter enumerates all the equivalent expressions in the group according to
// the expression pattern.
type ExprIter struct {
	// The group and element field solely identify a group expression.
	group   *Group
	element *list.Element

	// operand is the node of the pattern tree. The operand type of the group
	// expression must be matched with it, otherwise the group expression is
	// ignored during the iteration.
	operand int

	// children is used to iterate the child expressions.
	children []*ExprIter
}

func (iter *ExprIter) Next() bool {
	// Iterate child firstly.
	for i := range iter.children {
		if iter.children[i].Next() {
			return true
		}
	}

	// It's root node.
	if iter.group == nil {
		return false
	}

	// Otherwise, iterate itself to find more matched equivalent expressions.
	for iter.element.Next(); iter.element != nil; iter.element.Next() {
		expr := iter.element.Value.(*GroupExpr)
		exprOperand := expr.exprNode.GetOperand()

		if !iter.operand.match(exprOperand) {
			continue
		}

		allMatched := true
		for i := range iter.children {
			if !iter.children[i].Reset(expr.children[i]) {
				allMatched = false
				break
			}
		}
		if allMatched {
			return true
		}
	}
	return false
}

func (iter *ExprIter) Reset(g *Group) bool {
	for elem := g.equivalents.Front(); elem != nil; elem.Next() {
		expr := elem.Value.(*GroupExpr)
		if !iter.operand.match(expr.exprNode.GetOperand()) {
			continue
		}

		allMatched := true
		for i := range expr.children {
			if !iter.children[i].Reset(expr.children[i]) {
				allMatched = false
				break
			}
		}
		if allMatched {
			iter.group = g
			iter.element = elem
			return true
		}
	}
	return false
}

func NewExprIter(expr *GroupExpr, p *Pattern) *ExprIter {
	if !p.operand.match(expr.exprNode.GetOperand()) {
		return nil
	}

	iter := &ExprIter{operand: p.operand}
	for i := range p.children {
		childIter := newChildExprIter(expr.children[i], p.children[i])
		if childIter == nil {
			return nil
		}
		iter.children = append(iter.children, childIter)
	}
	return iter
}

func newChildExprIter(childGroup *Group, childPattern *Pattern) *ExprIter {
	for elem := childGroup.equivalents.Front(); elem != nil; elem = elem.Next() {
		childIter := NewExprIter(elem.Value.(*GroupExpr), childPattern)
		if childIter == nil {
			continue
		}
		childIter.group = childGroup
		childIter.element = elem
		return childIter
	}
	return nil
}
