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

	// Indicates whether the current group expression binded by the iterator
	// matches the pattern after the creation or iteration of the group.
	foundMatch bool

	// operand is the node of the pattern tree. The operand type of the group
	// expression must be matched with it, otherwise the group expression is
	// ignored during the iteration.
	operand Operand

	// children is used to iterate the child expressions.
	children []*ExprIter
}

// Next returns the next group expression matches the pattern.
func (iter *ExprIter) Next() {
	// Iterate child firstly.
	for i := range iter.children {
		iter.children[i].Next()
		if iter.children[i].OK() {
			iter.foundMatch = true
			return
		}
	}

	// It's root node.
	if iter.group == nil {
		iter.foundMatch = false
		return
	}

	// Otherwise, iterate itself to find more matched equivalent expressions.
	for iter.element.Next(); iter.element != nil; iter.element.Next() {
		expr := iter.element.Value.(*GroupExpr)
		exprOperand := GetOperand(expr.exprNode)

		if !iter.operand.match(exprOperand) {
			continue
		}

		iter.foundMatch = true
		for i := range iter.children {
			if !iter.children[i].Reset(expr.children[i]) {
				iter.foundMatch = false
				break
			}
		}
		if iter.foundMatch {
			return
		}
	}
	iter.foundMatch = false
	return
}

// OK returns whether the iterator founds a group expression matches the pattern.
func (iter *ExprIter) OK() bool {
	return iter.foundMatch
}

// Reset resets the iterator to the first matched group expression.
func (iter *ExprIter) Reset(g *Group) bool {
	for elem := g.equivalents.Front(); elem != nil; elem.Next() {
		expr := elem.Value.(*GroupExpr)
		if !iter.operand.match(GetOperand(expr.exprNode)) {
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

// NewExprIter creates the iterator on the group which iterates the group
// expressions matches the pattern.
func NewExprIter(expr *GroupExpr, p *Pattern) *ExprIter {
	if !p.operand.match(GetOperand(expr.exprNode)) {
		return nil
	}

	iter := &ExprIter{operand: p.operand, foundMatch: true}
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
