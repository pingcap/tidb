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

package memo

import (
	"container/list"
	"fmt"

	"github.com/pingcap/tidb/planner/property"
)

// Group is short for expression Group, which is used to store all the
// logically equivalent expressions. It's a set of GroupExpr.
type Group struct {
	Equivalents *list.List

	FirstExpr    map[Operand]*list.Element
	Fingerprints map[string]*list.Element

	Explored        bool
	SelfFingerprint string

	ImplMap map[string]Implementation
	Prop    *property.LogicalProperty
}

// NewGroup creates a new Group.
func NewGroup(e *GroupExpr) *Group {
	g := &Group{
		Equivalents:  list.New(),
		Fingerprints: make(map[string]*list.Element),
		FirstExpr:    make(map[Operand]*list.Element),
		ImplMap:      make(map[string]Implementation),
	}
	g.Insert(e)
	return g
}

// FingerPrint returns the unique fingerprint of the Group.
func (g *Group) FingerPrint() string {
	if g.SelfFingerprint == "" {
		g.SelfFingerprint = fmt.Sprintf("%p", g)
	}
	return g.SelfFingerprint
}

// Insert a nonexistent Group expression.
func (g *Group) Insert(e *GroupExpr) bool {
	if g.Exists(e) {
		return false
	}

	operand := GetOperand(e.ExprNode)
	var newEquiv *list.Element
	mark, hasMark := g.FirstExpr[operand]
	if hasMark {
		newEquiv = g.Equivalents.InsertAfter(e, mark)
	} else {
		newEquiv = g.Equivalents.PushBack(e)
		g.FirstExpr[operand] = newEquiv
	}
	g.Fingerprints[e.FingerPrint()] = newEquiv
	return true
}

// Delete an existing Group expression.
func (g *Group) Delete(e *GroupExpr) {
	fingerprint := e.FingerPrint()
	equiv, ok := g.Fingerprints[fingerprint]
	if !ok {
		return // Can not find the target GroupExpr.
	}

	g.Equivalents.Remove(equiv)
	delete(g.Fingerprints, fingerprint)

	operand := GetOperand(equiv.Value.(*GroupExpr).ExprNode)
	if g.FirstExpr[operand] != equiv {
		return // The target GroupExpr is not the first Element of the same Operand.
	}

	nextElem := equiv.Next()
	if nextElem != nil && GetOperand(nextElem.Value.(*GroupExpr).ExprNode) == operand {
		g.FirstExpr[operand] = nextElem
		return // The first Element of the same Operand has been changed.
	}
	delete(g.FirstExpr, operand)
}

// Exists checks whether a Group expression existed in a Group.
func (g *Group) Exists(e *GroupExpr) bool {
	_, ok := g.Fingerprints[e.FingerPrint()]
	return ok
}

// GetFirstElem returns the first Group expression which matches the Operand.
// Return a nil pointer if there isn't.
func (g *Group) GetFirstElem(operand Operand) *list.Element {
	if operand == OperandAny {
		return g.Equivalents.Front()
	}
	return g.FirstExpr[operand]
}

// GetImpl returns the best Implementation satisfy the physical property.
func (g *Group) GetImpl(prop *property.PhysicalProperty) Implementation {
	key := prop.HashCode()
	return g.ImplMap[string(key)]
}

// InsertImpl inserts the best Implementation satisfy the physical property.
func (g *Group) InsertImpl(prop *property.PhysicalProperty, impl Implementation) {
	key := prop.HashCode()
	g.ImplMap[string(key)] = impl
}
