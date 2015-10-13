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

import (
	"github.com/pingcap/tidb/table"
)

var (
	_ Node = &SelectNode{}
	_ Node = &JoinNode{}
	_ Node = &TableRef{}
	_ Node = &TableSource{}
)

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

// JoinType is join type, including cross/left/right/full.
type JoinType int

const (
	// CrossJoin is cross join type.
	CrossJoin JoinType = iota + 1
	// LeftJoin is left Join type.
	LeftJoin
	// RightJoin is right Join type.
	RightJoin
)

// JoinNode represents table join.
type JoinNode struct {
	txtNode

	// Left table can be TableSource or JoinNode.
	Left Node
	// Right table can be TableSource or JoinNode or nil.
	Right Node
	// Tp represents join type.
	Tp JoinType
}

// Accept implements Node Accept interface.
func (jn *JoinNode) Accept(v Visitor) (Node, bool) {
	if !v.Enter(jn) {
		return jn, false
	}
	node, ok := jn.Left.Accept(v)
	if !ok {
		return jn, false
	}
	jn.Left = node
	node, ok = jn.Right.Accept(v)
	if !ok {
		return jn, false
	}
	jn.Right = node
	return v.Leave(jn)
}

// TableRef represents a reference to actual table.
type TableRef struct {
	txtNode
	// Ident is the table identifier.
	Ident table.Ident
}

// Accept implements Node Accept interface.
func (tr *TableRef) Accept(v Visitor) (Node, bool) {
	if !v.Enter(tr) {
		return tr, false
	}
	return v.Leave(tr)
}

// TableSource represents table source with a name.
type TableSource struct {
	txtNode

	// Source is the source of the data, can be a TableRef,
	// a SubQuery, or a JoinNode.
	Source Node

	// Name is the alias name of the table source.
	Name string
}

// Accept implements Node Accept interface.
func (ts *TableSource) Accept(v Visitor) (Node, bool) {
	if !v.Enter(ts) {
		return ts, false
	}
	node, ok := ts.Source.Accept(v)
	if !ok {
		return ts, false
	}
	ts.Source = node
	return v.Leave(ts)
}

// SelectNode represents the select query node.
type SelectNode struct {
	txtNode

	// Distinct represents if the select has distinct option.
	Distinct bool
	// Fields is the select expression list.
	Fields []Expression
	// From is the from clause of the query.
	From *JoinNode
	// Where is the where clause in select statement.
	Where Expression
	// GroupBy is the group by expression list.
	GroupBy []Expression
	// Having is the having condition.
	Having Expression
	// OrderBy is the odering expression list.
	OrderBy []Expression
	// Offset is the offset value.
	Offset int
	// Limit is the limit value.
	Limit int
}

// Accept implements Node Accept interface.
func (sn *SelectNode) Accept(v Visitor) (Node, bool) {
	if !v.Enter(sn) {
		return sn, false
	}
	for i, val := range sn.Fields {
		node, ok := val.Accept(v)
		if !ok {
			return sn, false
		}
		sn.Fields[i] = node.(Expression)
	}
	node, ok := sn.From.Accept(v)
	if !ok {
		return sn, false
	}
	sn.From = node.(*JoinNode)

	node, ok = sn.Where.Accept(v)
	if !ok {
		return sn, false
	}
	sn.Where = node.(Expression)

	for i, val := range sn.GroupBy {
		node, ok = val.Accept(v)
		if !ok {
			return sn, false
		}
		sn.GroupBy[i] = node.(Expression)
	}

	node, ok = sn.Having.Accept(v)
	if !ok {
		return sn, false
	}
	sn.Having = node.(Expression)

	for i, val := range sn.OrderBy {
		node, ok = val.Accept(v)
		if !ok {
			return sn, false
		}
		sn.OrderBy[i] = node.(Expression)
	}

	return v.Leave(sn)
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
