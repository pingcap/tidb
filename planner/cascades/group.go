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
	equivalents  *list.List
	fingerprints map[string]*list.Element

	explored        bool
	selfFingerprint string
}

// NewGroup creates a new Group.
func NewGroup(e *GroupExpr) *Group {
	g := &Group{
		equivalents:  list.New(),
		fingerprints: make(map[string]*list.Element),
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
	newEquiv := g.equivalents.PushBack(e)
	g.fingerprints[e.FingerPrint()] = newEquiv
	return true
}

// Delete an existing group expression.
func (g *Group) Delete(e *GroupExpr) {
	fingerprint := e.FingerPrint()
	if equiv, ok := g.fingerprints[fingerprint]; ok {
		g.equivalents.Remove(equiv)
		delete(g.fingerprints, fingerprint)
	}
}

// Exists checks whether a group expression existed in a Group.
func (g *Group) Exists(e *GroupExpr) bool {
	_, ok := g.fingerprints[e.FingerPrint()]
	return ok
}
