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
	// group and element solely identify a group expression.
	group   *Group
	element *list.Element

	// matched indicates whether the current group expression binded by the
	// iterator matches the pattern after the creation or iteration.
	matched bool

	// operand is the node of the pattern tree. The operand type of the group
	// expression must be matched with it.
	operand Operand

	// children is used to iterate the child expressions.
	children []*ExprIter
}

// Next returns the next group expression matches the pattern.
func (iter *ExprIter) Next() (found bool) {
	defer func() {
		iter.matched = found
	}()

	// Iterate child firstly.
	for i := len(iter.children) - 1; i >= 0; i-- {
		if !iter.children[i].Next() {
			continue
		}

		for j := i + 1; j < len(iter.children); j++ {
			iter.children[j].Reset()
		}
		return true
	}

	// It's root node.
	if iter.group == nil {
		return false
	}

	// Otherwise, iterate itself to find more matched equivalent expressions.
	for elem := iter.element.Next(); elem != nil; elem = elem.Next() {
		expr := elem.Value.(*GroupExpr)
		exprOperand := GetOperand(expr.exprNode)

		if !iter.operand.match(exprOperand) {
			// All the equivalents which have the same operand are continuously
			// stored in the list. Once the current equivalent can not match
			// the operand, the rest can not, either.
			return false
		}

		if len(iter.children) != len(expr.children) {
			continue
		}

		allMatched := true
		for i := range iter.children {
			iter.children[i].group = expr.children[i]
			if !iter.children[i].Reset() {
				allMatched = false
				break
			}
		}

		if allMatched {
			iter.element = elem
			return true
		}
	}
	return false
}

// Matched returns whether the iterator founds a group expression matches the
// pattern.
func (iter *ExprIter) Matched() bool {
	return iter.matched
}

// Reset resets the iterator to the first matched group expression.
func (iter *ExprIter) Reset() (findMatch bool) {
	defer func() { iter.matched = findMatch }()

	for elem := iter.group.GetFirstElem(iter.operand); elem != nil; elem = elem.Next() {
		expr := elem.Value.(*GroupExpr)
		exprOperand := GetOperand(expr.exprNode)
		if !iter.operand.match(exprOperand) {
			break
		}

		if len(expr.children) != len(iter.children) {
			continue
		}

		allMatched := true
		for i := range iter.children {
			iter.children[i].group = expr.children[i]
			if !iter.children[i].Reset() {
				allMatched = false
				break
			}
		}
		if allMatched {
			iter.element = elem
			return true
		}
	}
	return false
}

// NewExprIterFromGroupElem creates the iterator on the group element.
func NewExprIterFromGroupElem(elem *list.Element, p *Pattern) *ExprIter {
	expr := elem.Value.(*GroupExpr)
	if !p.operand.match(GetOperand(expr.exprNode)) {
		return nil
	}
	iter := newExprIterFromGroupExpr(expr, p)
	if iter != nil {
		iter.element = elem
	}
	return iter
}

// newExprIterFromGroupExpr creates the iterator on the group expression.
func newExprIterFromGroupExpr(expr *GroupExpr, p *Pattern) *ExprIter {
	if len(p.children) != len(expr.children) {
		return nil
	}

	iter := &ExprIter{operand: p.operand, matched: true}
	for i := range p.children {
		childIter := newExprIterFromGroup(expr.children[i], p.children[i])
		if childIter == nil {
			return nil
		}
		iter.children = append(iter.children, childIter)
	}
	return iter
}

// newExprIterFromGroup creates the iterator on the group.
func newExprIterFromGroup(g *Group, p *Pattern) *ExprIter {
	for elem := g.GetFirstElem(p.operand); elem != nil; elem = elem.Next() {
		expr := elem.Value.(*GroupExpr)
		if !p.operand.match(GetOperand(expr.exprNode)) {
			return nil
		}
		iter := newExprIterFromGroupExpr(expr, p)
		if iter != nil {
			iter.group, iter.element = g, elem
			return iter
		}
	}
	return nil
}
