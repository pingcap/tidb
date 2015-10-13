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
	"github.com/pingcap/tidb/model"
)

var (
	_ Node = &JoinNode{}
	_ Node = &TableRef{}
	_ Node = &TableSource{}
	_ Node = &SelectNode{}
	_ Node = &Assignment{}
	_ Node = &InsertIntoStmt{}
	_ Node = &DeleteStmt{}
	_ Node = &UpdateStmt{}
	_ Node = &TruncateTableStmt{}
	_ Node = &UnionStmt{}
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

type TableIdent struct {
	Schema model.CIStr
	Name   model.CIStr
}

// TableRef represents a reference to actual table.
type TableRef struct {
	txtNode

	Ident TableIdent
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

// LockType is select lock type.
type LockType int

// Select Lock Type.
const (
	SelectLockNone LockType = iota
	SelectLockForUpdate
	SelectLockInShareMode
)

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
	// Lock is the lock type
	LockTp LockType
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

// Assignment is the expression for assignment, like a = 1.
type Assignment struct {
	txtNode
	// Column is the column reference to be assigned.
	Column *ColumnRef
	// Expr is the expression assigning to ColName.
	Expr Expression
}

// Accept implements Node Accept interface.
func (as *Assignment) Accept(v Visitor) (Node, bool) {
	if !v.Enter(as) {
		return as, false
	}
	node, ok := as.Column.Accept(v)
	if !ok {
		return as, false
	}
	as.Column = node.(*ColumnRef)
	node, ok = as.Expr.Accept(v)
	if !ok {
		return as, false
	}
	as.Expr = node.(Expression)
	return v.Leave(as)
}

// InsertIntoStmt is a statement to insert new rows into an existing table.
// See: https://dev.mysql.com/doc/refman/5.7/en/insert.html
type InsertIntoStmt struct {
	txtNode

	Columns     []*ColumnRef
	Lists       [][]Expression
	Table       *TableRef
	Setlist     []*Assignment
	Priority    int
	OnDuplicate []*Assignment
}

// Accept implements Node Accept interface.
func (in *InsertIntoStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(in) {
		return in, false
	}
	for i, val := range in.Columns {
		node, ok := val.Accept(v)
		if !ok {
			return in, false
		}
		in.Columns[i] = node.(*ColumnRef)
	}
	for i, list := range in.Lists {
		for j, val := range list {
			node, ok := val.Accept(v)
			if !ok {
				return in, false
			}
			in.Lists[i][j] = node.(Expression)
		}
	}
	for i, val := range in.Setlist {
		node, ok := val.Accept(v)
		if !ok {
			return in, false
		}
		in.Setlist[i] = node.(*Assignment)
	}
	for i, val := range in.OnDuplicate {
		node, ok := val.Accept(v)
		if !ok {
			return in, false
		}
		in.OnDuplicate[i] = node.(*Assignment)
	}
	return v.Leave(in)
}

// DeleteStmt is a statement to delete rows from table.
// See: https://dev.mysql.com/doc/refman/5.7/en/delete.html
type DeleteStmt struct {
	txtNode

	Tables      []*TableRef
	Where       Expression
	Order       []Expression
	Limit       int
	LowPriority bool
	Ignore      bool
	Quick       bool
	MultiTable  bool
	BeforeFrom  bool
}

// Accept implements Node Accept interface.
func (de *DeleteStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(de) {
		return de, false
	}
	for i, val := range de.Tables {
		node, ok := val.Accept(v)
		if !ok {
			return de, false
		}
		de.Tables[i] = node.(*TableRef)
	}

	node, ok := de.Where.Accept(v)
	if !ok {
		return de, false
	}
	de.Where = node.(Expression)
	for i, val := range de.Order {
		node, ok = val.Accept(v)
		if !ok {
			return de, false
		}
		de.Order[i] = node.(Expression)
	}
	return v.Leave(de)
}

// UpdateStmt is a statement to update columns of existing rows in tables with new values.
// See: https://dev.mysql.com/doc/refman/5.7/en/update.html
type UpdateStmt struct {
	txtNode

	TableRefs     *JoinNode
	List          []*Assignment
	Where         Expression
	Order         []Expression
	Limit         int
	LowPriority   bool
	Ignore        bool
	MultipleTable bool
}

// Accept implements Node Accept interface.
func (up *UpdateStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(up) {
		return up, false
	}
	node, ok := up.TableRefs.Accept(v)
	if !ok {
		return up, false
	}
	up.TableRefs = node.(*JoinNode)
	for i, val := range up.List {
		node, ok = val.Accept(v)
		if !ok {
			return up, false
		}
		up.List[i] = node.(*Assignment)
	}
	node, ok = up.Where.Accept(v)
	if !ok {
		return up, false
	}
	up.Where = node.(Expression)
	for i, val := range up.Order {
		node, ok = val.Accept(v)
		if !ok {
			return up, false
		}
		up.Order[i] = node.(Expression)
	}
	return v.Leave(up)
}

// TruncateTableStmt is a statement to empty a table completely.
// See: https://dev.mysql.com/doc/refman/5.7/en/truncate-table.html
type TruncateTableStmt struct {
	txtNode

	Table *TableRef
}

// UnionStmt is a statement to combine results from multiple SelectStmts.
// See: https://dev.mysql.com/doc/refman/5.7/en/union.html
type UnionStmt struct {
	txtNode

	Distincts []bool
	Selects   []*SelectNode
	Limit     int
	Offset    int
	OrderBy   []Expression
}
