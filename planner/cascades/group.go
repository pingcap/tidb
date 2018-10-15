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
	"fmt"

	plannercore "github.com/pingcap/tidb/planner/core"
)

// Group is short for expression group, which is used to store all the
// logically equivalent expressions. It's a set of GroupExpr.
type Group struct {
	equivalents     map[string]*GroupExpr
	explored        bool
	selfFingerprint string
}

// NewGroup creates a new Group.
func NewGroup(e *GroupExpr) *Group {
	g := &Group{equivalents: make(map[string]*GroupExpr)}
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

// Insert a nonexistent group exxpression.
func (g *Group) Insert(e *GroupExpr) {
	g.equivalents[e.FingerPrint()] = e
}

// Delete an existing group expression.
func (g *Group) Delete(e *GroupExpr) {
	fingerprint := e.FingerPrint()
	if _, ok := g.equivalents[fingerprint]; ok {
		delete(g.equivalents, fingerprint)
	}
}

// Exists checks whether a group expression existed in a Group.
func (g *Group) Exists(e *GroupExpr) bool {
	_, ok := g.equivalents[e.FingerPrint()]
	return ok
}

// Convert2Group converts a logical plan to expression groups.
func Convert2Group(node plannercore.LogicalPlan) *Group {
	e := NewGroupExpr(node)
	e.children = make([]*Group, 0, len(node.Children()))
	for _, child := range node.Children() {
		childGroup := Convert2Group(child)
		e.children = append(e.children, childGroup)
	}
	return NewGroup(e)
}
