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

	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/property"
)

// EngineType is determined by whether it's above or below `Gather`s.
// Plan will choose the different engine to be implemented/executed on according to its EngineType.
// Different engine may support different operators with different cost, so we should design
// different transformation and implementation rules for each engine.
type EngineType uint

const (
	// EngineTiDB stands for groups which is above `Gather`s and will be executed in TiDB layer.
	EngineTiDB EngineType = 1 << iota
	// EngineTiKV stands for groups which is below `Gather`s and will be executed in TiKV layer.
	EngineTiKV
	// EngineTiFlash stands for groups which is below `Gather`s and will be executed in TiFlash layer.
	EngineTiFlash
)

// EngineTypeSet is the bit set of EngineTypes.
type EngineTypeSet uint

const (
	// EngineTiDBOnly is the EngineTypeSet for EngineTiDB only.
	EngineTiDBOnly = EngineTypeSet(EngineTiDB)
	// EngineTiKVOnly is the EngineTypeSet for EngineTiKV only.
	EngineTiKVOnly = EngineTypeSet(EngineTiKV)
	// EngineTiFlashOnly is the EngineTypeSet for EngineTiFlash only.
	EngineTiFlashOnly = EngineTypeSet(EngineTiFlash)
	// EngineTiKVOrTiFlash is the EngineTypeSet for (EngineTiKV | EngineTiFlash).
	EngineTiKVOrTiFlash = EngineTypeSet(EngineTiKV | EngineTiFlash)
	// EngineAll is the EngineTypeSet for all of the EngineTypes.
	EngineAll = EngineTypeSet(EngineTiDB | EngineTiKV | EngineTiFlash)
)

// Contains checks whether the EngineTypeSet contains the EngineType.
func (e EngineTypeSet) Contains(tp EngineType) bool {
	return uint(e)&uint(tp) != 0
}

// String implements fmt.Stringer interface.
func (e EngineType) String() string {
	switch e {
	case EngineTiDB:
		return "EngineTiDB"
	case EngineTiKV:
		return "EngineTiKV"
	case EngineTiFlash:
		return "EngineTiFlash"
	}
	return "UnknownEngineType"
}

// ExploreMark is uses to mark whether a Group or GroupExpr has
// been fully explored by a transformation rule batch.
type ExploreMark int

// SetExplored sets the roundth bit.
func (m *ExploreMark) SetExplored(round int) {
	*m |= 1 << round
}

// SetUnexplored unsets the roundth bit.
func (m *ExploreMark) SetUnexplored(round int) {
	*m &= ^(1 << round)
}

// Explored returns whether the roundth bit has been set.
func (m *ExploreMark) Explored(round int) bool {
	return *m&(1<<round) != 0
}

// Group is short for expression Group, which is used to store all the
// logically equivalent expressions. It's a set of GroupExpr.
type Group struct {
	Equivalents *list.List

	FirstExpr    map[Operand]*list.Element
	Fingerprints map[string]*list.Element

	ImplMap map[string]Implementation
	Prop    *property.LogicalProperty

	EngineType EngineType

	SelfFingerprint string

	// ExploreMark is uses to mark whether this Group has been explored
	// by a transformation rule batch in a certain round.
	ExploreMark

	//hasBuiltKeyInfo indicates whether this group has called `BuildKeyInfo`.
	// BuildKeyInfo is lazily called when a rule needs information of
	// unique key or maxOneRow (in LogicalProp). For each Group, we only need
	// to collect these information once.
	hasBuiltKeyInfo bool
}

// NewGroupWithSchema creates a new Group with given schema.
func NewGroupWithSchema(e *GroupExpr, s *expression.Schema) *Group {
	prop := &property.LogicalProperty{Schema: expression.NewSchema(s.Columns...)}
	g := &Group{
		Equivalents:  list.New(),
		Fingerprints: make(map[string]*list.Element),
		FirstExpr:    make(map[Operand]*list.Element),
		ImplMap:      make(map[string]Implementation),
		Prop:         prop,
		EngineType:   EngineTiDB,
	}
	g.Insert(e)
	return g
}

// SetEngineType sets the engine type of the group.
func (g *Group) SetEngineType(e EngineType) *Group {
	g.EngineType = e
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
	if e == nil || g.Exists(e) {
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
	e.Group = g
	return true
}

// Delete an existing Group expression.
func (g *Group) Delete(e *GroupExpr) {
	fingerprint := e.FingerPrint()
	equiv, ok := g.Fingerprints[fingerprint]
	if !ok {
		return // Can not find the target GroupExpr.
	}

	operand := GetOperand(equiv.Value.(*GroupExpr).ExprNode)
	if g.FirstExpr[operand] == equiv {
		// The target GroupExpr is the first Element of the same Operand.
		// We need to change the FirstExpr to the next Expr, or delete the FirstExpr.
		nextElem := equiv.Next()
		if nextElem != nil && GetOperand(nextElem.Value.(*GroupExpr).ExprNode) == operand {
			g.FirstExpr[operand] = nextElem
		} else {
			// There is no more GroupExpr of the Operand, so we should
			// delete the FirstExpr of this Operand.
			delete(g.FirstExpr, operand)
		}
	}

	g.Equivalents.Remove(equiv)
	delete(g.Fingerprints, fingerprint)
	e.Group = nil
}

// DeleteAll deletes all of the GroupExprs in the Group.
func (g *Group) DeleteAll() {
	g.Equivalents = list.New()
	g.Fingerprints = make(map[string]*list.Element)
	g.FirstExpr = make(map[Operand]*list.Element)
	g.SelfFingerprint = ""
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

// Convert2GroupExpr converts a logical plan to a GroupExpr.
func Convert2GroupExpr(node plannercore.LogicalPlan) *GroupExpr {
	e := NewGroupExpr(node)
	e.Children = make([]*Group, 0, len(node.Children()))
	for _, child := range node.Children() {
		childGroup := Convert2Group(child)
		e.Children = append(e.Children, childGroup)
	}
	return e
}

// Convert2Group converts a logical plan to a Group.
func Convert2Group(node plannercore.LogicalPlan) *Group {
	e := Convert2GroupExpr(node)
	g := NewGroupWithSchema(e, node.Schema())
	// Stats property for `Group` would be computed after exploration phase.
	return g
}

// BuildKeyInfo recursively builds UniqueKey and MaxOneRow info in the LogicalProperty.
func (g *Group) BuildKeyInfo() {
	if g.hasBuiltKeyInfo {
		return
	}
	g.hasBuiltKeyInfo = true

	e := g.Equivalents.Front().Value.(*GroupExpr)
	childSchema := make([]*expression.Schema, len(e.Children))
	childMaxOneRow := make([]bool, len(e.Children))
	for i := range e.Children {
		e.Children[i].BuildKeyInfo()
		childSchema[i] = e.Children[i].Prop.Schema
		childMaxOneRow[i] = e.Children[i].Prop.MaxOneRow
	}
	if len(childSchema) == 1 {
		// For UnaryPlan(such as Selection, Limit ...), we can set the child's unique key as its unique key.
		// If the GroupExpr is a schemaProducer, schema.Keys will be reset below in `BuildKeyInfo()`.
		g.Prop.Schema.Keys = childSchema[0].Keys
	}
	e.ExprNode.BuildKeyInfo(g.Prop.Schema, childSchema)
	g.Prop.MaxOneRow = e.ExprNode.MaxOneRow() || plannercore.HasMaxOneRow(e.ExprNode, childMaxOneRow)
}
