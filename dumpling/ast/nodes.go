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

package ast

// txtNode is the struct implements partial node interface.
// can be embeded by other nodes.
type txtNode struct {
	txt string
}

// SetText implements Node interface.
func (bn *txtNode) SetText(text string) {
	bn.txt = text
}

// Text implements Node interface.
func (bn *txtNode) Text() string {
	return bn.txt
}

// VariableAssignment is a variable assignment struct.
type VariableAssignment struct {
	txtNode
	Name     string
	Value    Expression
	IsGlobal bool
	IsSystem bool
}

// Accept implements Node interface.
func (va *VariableAssignment) Accept(v Visitor) (Node, bool) {
	if !v.Enter(va) {
		return va, false
	}
	node, ok := va.Value.Accept(v)
	if !ok {
		return va, false
	}
	va.Value = node.(Expression)
	return v.Leave(va)
}

// SetStmt is the statement to set variables.
type SetStmt struct {
	txtNode
	// Variables is the list of variable assignment.
	Variables []*VariableAssignment
}

// Accept implements Node interface.
func (set *SetStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(set) {
		return set, false
	}
	for i, val := range set.Variables {
		node, ok := val.Accept(v)
		if !ok {
			return set, false
		}
		set.Variables[i] = node.(*VariableAssignment)
	}
	return v.Leave(set)
}
