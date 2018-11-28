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
	"fmt"
)

// Group is short for expression group, which is used to store all the
// logically equivalent expressions. It's a set of GroupExpr.
type Group struct {
	equivalents *list.List

	firstExpr    map[Operand]*list.Element
	fingerprints map[string]*list.Element

	explored        bool
	selfFingerprint string
}

// NewGroup creates a new Group.
func NewGroup(e *GroupExpr) *Group {
	g := &Group{
		equivalents:  list.New(),
		fingerprints: make(map[string]*list.Element),
		firstExpr:    make(map[Operand]*list.Element),
	}
	g.Insert(e)
	return g
}

// FingerPrint returns the unique fingerprint of the group.
func (g *Group) FingerPrint() string {
	if g.selfFingerprint == "" {
		g.selfFingerprint = fmt.Sprintf("%p", g)
	}
	return g.selfFingerprint
}

// Insert a nonexistent group expression.
func (g *Group) Insert(e *GroupExpr) bool {
	if g.Exists(e) {
		return false
	}

	operand := GetOperand(e.exprNode)
	var newEquiv *list.Element
	mark, hasMark := g.firstExpr[operand]
	if hasMark {
		newEquiv = g.equivalents.InsertAfter(e, mark)
	} else {
		newEquiv = g.equivalents.PushBack(e)
		g.firstExpr[operand] = newEquiv
	}
	g.fingerprints[e.FingerPrint()] = newEquiv
	return true
}

// Delete an existing group expression.
func (g *Group) Delete(e *GroupExpr) {
	fingerprint := e.FingerPrint()
	equiv, ok := g.fingerprints[fingerprint]
	if !ok {
		return // Can not find the target GroupExpr.
	}

	g.equivalents.Remove(equiv)
	delete(g.fingerprints, fingerprint)

	operand := GetOperand(equiv.Value.(*GroupExpr).exprNode)
	if g.firstExpr[operand] != equiv {
		return // The target GroupExpr is not the first element of the same operand.
	}

	nextElem := equiv.Next()
	if nextElem != nil && GetOperand(nextElem.Value.(*GroupExpr).exprNode) == operand {
		g.firstExpr[operand] = nextElem
		return // The first element of the same operand has been changed.
	}
	delete(g.firstExpr, operand)
}

// Exists checks whether a group expression existed in a Group.
func (g *Group) Exists(e *GroupExpr) bool {
	_, ok := g.fingerprints[e.FingerPrint()]
	return ok
}

// GetFirstElem returns the first group expression which matches the operand.
// Return a nil pointer if there isn't.
func (g *Group) GetFirstElem(operand Operand) *list.Element {
	if operand == OperandAny {
		return g.equivalents.Front()
	}
	return g.firstExpr[operand]
}
