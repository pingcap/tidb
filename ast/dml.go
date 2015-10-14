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
	_ DMLNode = &InsertStmt{}
	_ DMLNode = &DeleteStmt{}
	_ DMLNode = &UpdateStmt{}
	_ DMLNode = &SelectStmt{}
	_ Node    = &Join{}
	_ Node    = &Union{}
	_ Node    = &TableRef{}
	_ Node    = &TableSource{}
	_ Node    = &Assignment{}
)

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

// Join represents table join.
type Join struct {
	node

	// Left table can be TableSource or JoinNode.
	Left Node
	// Right table can be TableSource or JoinNode or nil.
	Right Node
	// Tp represents join type.
	Tp JoinType
}

// Accept implements Node Accept interface.
func (j *Join) Accept(v Visitor) (Node, bool) {
	if !v.Enter(j) {
		return j, false
	}
	node, ok := j.Left.Accept(v)
	if !ok {
		return j, false
	}
	j.Left = node
	if j.Right != nil {
		node, ok = j.Right.Accept(v)
		if !ok {
			return j, false
		}
		j.Right = node
	}
	return v.Leave(j)
}

// TableRef represents a reference to actual table.
type TableRef struct {
	node

	Schema model.CIStr
	Name   model.CIStr
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
	node

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

// Union represents union select statement.
type Union struct {
	node

	Select *SelectStmt
}

// Accept implements Node Accept interface.
func (u *Union) Accept(v Visitor) (Node, bool) {
	if !v.Enter(u) {
		return u, false
	}
	node, ok := u.Select.Accept(v)
	if !ok {
		return u, false
	}
	u.Select = node.(*SelectStmt)
	return v.Leave(u)
}

// SelectLockType is the lock type for SelectStmt.
type SelectLockType int

// Select lock types.
const (
	SelectLockNone SelectLockType = iota
	SelectLockForUpdate
	SelectLockInShareMode
)

// SelectStmt represents the select query node.
type SelectStmt struct {
	dmlNode

	// Distinct represents if the select has distinct option.
	Distinct bool
	// Fields is the select expression list.
	Fields []ExprNode
	// From is the from clause of the query.
	From *Join
	// Where is the where clause in select statement.
	Where ExprNode
	// GroupBy is the group by expression list.
	GroupBy []ExprNode
	// Having is the having condition.
	Having ExprNode
	// OrderBy is the odering expression list.
	OrderBy []ExprNode
	// Offset is the offset value.
	Offset int
	// Limit is the limit value.
	Limit int
	// Lock is the lock type
	LockTp SelectLockType
	// Unions is the union select statement.
	Unions []*Union
}

// Accept implements Node Accept interface.
func (sn *SelectStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(sn) {
		return sn, false
	}
	for i, val := range sn.Fields {
		node, ok := val.Accept(v)
		if !ok {
			return sn, false
		}
		sn.Fields[i] = node.(ExprNode)
	}
	if sn.From != nil {
		node, ok := sn.From.Accept(v)
		if !ok {
			return sn, false
		}
		sn.From = node.(*Join)
	}

	if sn.Where != nil {
		node, ok := sn.Where.Accept(v)
		if !ok {
			return sn, false
		}
		sn.Where = node.(ExprNode)
	}

	for i, val := range sn.GroupBy {
		node, ok := val.Accept(v)
		if !ok {
			return sn, false
		}
		sn.GroupBy[i] = node.(ExprNode)
	}
	if sn.Having != nil {
		node, ok := sn.Having.Accept(v)
		if !ok {
			return sn, false
		}
		sn.Having = node.(ExprNode)
	}

	for i, val := range sn.OrderBy {
		node, ok := val.Accept(v)
		if !ok {
			return sn, false
		}
		sn.OrderBy[i] = node.(ExprNode)
	}

	for i, val := range sn.Unions {
		node, ok := val.Accept(v)
		if !ok {
			return sn, false
		}
		sn.Unions[i] = node.(*Union)
	}
	return v.Leave(sn)
}

// Assignment is the expression for assignment, like a = 1.
type Assignment struct {
	node
	// Column is the column reference to be assigned.
	Column *ColumnRefExpr
	// Expr is the expression assigning to ColName.
	Expr ExprNode
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
	as.Column = node.(*ColumnRefExpr)
	node, ok = as.Expr.Accept(v)
	if !ok {
		return as, false
	}
	as.Expr = node.(ExprNode)
	return v.Leave(as)
}

// InsertStmt is a statement to insert new rows into an existing table.
// See: https://dev.mysql.com/doc/refman/5.7/en/insert.html
type InsertStmt struct {
	dmlNode

	Columns     []*ColumnRefExpr
	Lists       [][]ExprNode
	Table       *TableRef
	Setlist     []*Assignment
	Priority    int
	OnDuplicate []*Assignment
}

// Accept implements Node Accept interface.
func (in *InsertStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(in) {
		return in, false
	}
	for i, val := range in.Columns {
		node, ok := val.Accept(v)
		if !ok {
			return in, false
		}
		in.Columns[i] = node.(*ColumnRefExpr)
	}
	for i, list := range in.Lists {
		for j, val := range list {
			node, ok := val.Accept(v)
			if !ok {
				return in, false
			}
			in.Lists[i][j] = node.(ExprNode)
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
	dmlNode

	Tables      []*TableRef
	Where       ExprNode
	Order       []ExprNode
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

	if de.Where != nil {
		node, ok := de.Where.Accept(v)
		if !ok {
			return de, false
		}
		de.Where = node.(ExprNode)
	}

	for i, val := range de.Order {
		node, ok := val.Accept(v)
		if !ok {
			return de, false
		}
		de.Order[i] = node.(ExprNode)
	}
	return v.Leave(de)
}

// UpdateStmt is a statement to update columns of existing rows in tables with new values.
// See: https://dev.mysql.com/doc/refman/5.7/en/update.html
type UpdateStmt struct {
	dmlNode

	TableRefs     *Join
	List          []*Assignment
	Where         ExprNode
	Order         []ExprNode
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
	up.TableRefs = node.(*Join)
	for i, val := range up.List {
		node, ok = val.Accept(v)
		if !ok {
			return up, false
		}
		up.List[i] = node.(*Assignment)
	}
	if up.Where != nil {
		node, ok = up.Where.Accept(v)
		if !ok {
			return up, false
		}
		up.Where = node.(ExprNode)
	}

	for i, val := range up.Order {
		node, ok = val.Accept(v)
		if !ok {
			return up, false
		}
		up.Order[i] = node.(ExprNode)
	}
	return v.Leave(up)
}
