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
	_ Node    = &TableName{}
	_ Node    = &TableSource{}
	_ Node    = &Assignment{}
	_ Node    = &Limit{}
	_ Node    = &WildCardField{}
	_ Node    = &SelectField{}
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
	// On represents join on condition.
	On ExprNode
}

// Accept implements Node Accept interface.
func (j *Join) Accept(v Visitor) (Node, bool) {
	if !v.Enter(j) {
		return j, v.OK()
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
	if j.On != nil {
		node, ok = j.On.Accept(v)
		if !ok {
			return j, false
		}
		j.On = node.(ExprNode)
	}
	return v.Leave(j)
}

// TableName represents a table name.
type TableName struct {
	node

	Schema model.CIStr
	Name   model.CIStr
}

// Accept implements Node Accept interface.
func (tr *TableName) Accept(v Visitor) (Node, bool) {
	if !v.Enter(tr) {
		return tr, v.OK()
	}
	return v.Leave(tr)
}

// TableSource represents table source with a name.
type TableSource struct {
	node

	// Source is the source of the data, can be a TableName,
	// a SubQuery, or a JoinNode.
	Source Node

	// Alias is the alias name of the table source.
	Alias string
}

// Accept implements Node Accept interface.
func (ts *TableSource) Accept(v Visitor) (Node, bool) {
	if !v.Enter(ts) {
		return ts, v.OK()
	}
	node, ok := ts.Source.Accept(v)
	if !ok {
		return ts, false
	}
	ts.Source = node
	return v.Leave(ts)
}

// UnionClause represents a single "UNION SELECT ..." or "UNION (SELECT ...)" clause.
type UnionClause struct {
	node

	Distinct bool
	Select   *SelectStmt
}

// Accept implements Node Accept interface.
func (uc *UnionClause) Accept(v Visitor) (Node, bool) {
	if !v.Enter(uc) {
		return uc, v.OK()
	}
	node, ok := uc.Select.Accept(v)
	if !ok {
		return uc, false
	}
	uc.Select = node.(*SelectStmt)
	return v.Leave(uc)
}

// SelectLockType is the lock type for SelectStmt.
type SelectLockType int

// Select lock types.
const (
	SelectLockNone SelectLockType = iota
	SelectLockForUpdate
	SelectLockInShareMode
)

// WildCardField is a special type of select field content.
type WildCardField struct {
	node

	Table *TableName
}

// Accept implements Node Accept interface.
func (wf *WildCardField) Accept(v Visitor) (Node, bool) {
	if !v.Enter(wf) {
		return wf, v.OK()
	}
	if wf.Table != nil {
		node, ok := wf.Table.Accept(v)
		if !ok {
			return wf, false
		}
		wf.Table = node.(*TableName)
	}
	return v.Leave(wf)
}

// SelectField represents fields in select statement.
// There are two type of select field: wildcard
// and expression with optional alias name.
type SelectField struct {
	node

	// If WildCard is not nil, Expr will be nil.
	WildCard *WildCardField
	// If Expr is not nil, WildCard will be nil.
	Expr ExprNode
	// Alias name for Expr.
	Alias string
}

// Accept implements Node Accept interface.
func (sf *SelectField) Accept(v Visitor) (Node, bool) {
	if !v.Enter(sf) {
		return sf, v.OK()
	}
	if sf.Expr != nil {
		node, ok := sf.Expr.Accept(v)
		if !ok {
			return sf, false
		}
		sf.Expr = node.(ExprNode)
	}
	return v.Leave(sf)
}

// OrderByItem represents a single order by item.
type OrderByItem struct {
	node

	Expr ExprNode
	Desc bool
}

// Accept implements Node Accept interface.
func (ob *OrderByItem) Accept(v Visitor) (Node, bool) {
	if !v.Enter(ob) {
		return ob, v.OK()
	}
	node, ok := ob.Expr.Accept(v)
	if !ok {
		return ob, false
	}
	ob.Expr = node.(ExprNode)
	return v.Leave(ob)
}

// SelectStmt represents the select query node.
type SelectStmt struct {
	dmlNode

	// Distinct represents if the select has distinct option.
	Distinct bool
	// Fields is the select expression list.
	Fields []*SelectField
	// From is the from clause of the query.
	From *Join
	// Where is the where clause in select statement.
	Where ExprNode
	// GroupBy is the group by expression list.
	GroupBy []ExprNode
	// Having is the having condition.
	Having ExprNode
	// OrderBy is the odering expression list.
	OrderBy []*OrderByItem
	// Limit is the limit clause.
	Limit *Limit
	// Lock is the lock type
	LockTp SelectLockType

	// Union clauses.
	Unions []*UnionClause
	// Order by for union select.
	UnionOrderBy []*OrderByItem
	// Limit for union select.
	UnionLimit *Limit
}

// Accept implements Node Accept interface.
func (sn *SelectStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(sn) {
		return sn, v.OK()
	}
	for i, val := range sn.Fields {
		node, ok := val.Accept(v)
		if !ok {
			return sn, false
		}
		sn.Fields[i] = node.(*SelectField)
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
		sn.OrderBy[i] = node.(*OrderByItem)
	}

	if sn.Limit != nil {
		node, ok := sn.Limit.Accept(v)
		if !ok {
			return sn, false
		}
		sn.Limit = node.(*Limit)
	}
	return v.Leave(sn)
}

// Assignment is the expression for assignment, like a = 1.
type Assignment struct {
	node
	// Column is the column name to be assigned.
	Column *ColumnName
	// Expr is the expression assigning to ColName.
	Expr ExprNode
}

// Accept implements Node Accept interface.
func (as *Assignment) Accept(v Visitor) (Node, bool) {
	if !v.Enter(as) {
		return as, v.OK()
	}
	node, ok := as.Column.Accept(v)
	if !ok {
		return as, false
	}
	as.Column = node.(*ColumnName)
	node, ok = as.Expr.Accept(v)
	if !ok {
		return as, false
	}
	as.Expr = node.(ExprNode)
	return v.Leave(as)
}

// Priority const values.
// See: https://dev.mysql.com/doc/refman/5.7/en/insert.html
const (
	NoPriority = iota
	LowPriority
	HighPriority
	DelayedPriority
)

// InsertStmt is a statement to insert new rows into an existing table.
// See: https://dev.mysql.com/doc/refman/5.7/en/insert.html
type InsertStmt struct {
	dmlNode

	Columns     []*ColumnName
	Lists       [][]ExprNode
	Table       *TableName
	Setlist     []*Assignment
	Priority    int
	OnDuplicate []*Assignment
	Select      *SelectStmt
}

// Accept implements Node Accept interface.
func (in *InsertStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(in) {
		return in, v.OK()
	}
	for i, val := range in.Columns {
		node, ok := val.Accept(v)
		if !ok {
			return in, false
		}
		in.Columns[i] = node.(*ColumnName)
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
	if in.Select != nil {
		node, ok := in.Select.Accept(v)
		if !ok {
			return in, false
		}
		in.Select = node.(*SelectStmt)
	}
	return v.Leave(in)
}

// DeleteStmt is a statement to delete rows from table.
// See: https://dev.mysql.com/doc/refman/5.7/en/delete.html
type DeleteStmt struct {
	dmlNode

	TableRefs   *Join
	Tables      []*TableName
	Where       ExprNode
	Order       []*OrderByItem
	Limit       *Limit
	LowPriority bool
	Ignore      bool
	Quick       bool
	MultiTable  bool
	BeforeFrom  bool
}

// Accept implements Node Accept interface.
func (de *DeleteStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(de) {
		return de, v.OK()
	}

	node, ok := de.TableRefs.Accept(v)
	if !ok {
		return de, false
	}
	de.TableRefs = node.(*Join)

	for i, val := range de.Tables {
		node, ok = val.Accept(v)
		if !ok {
			return de, false
		}
		de.Tables[i] = node.(*TableName)
	}

	if de.Where != nil {
		node, ok = de.Where.Accept(v)
		if !ok {
			return de, false
		}
		de.Where = node.(ExprNode)
	}

	for i, val := range de.Order {
		node, ok = val.Accept(v)
		if !ok {
			return de, false
		}
		de.Order[i] = node.(*OrderByItem)
	}

	node, ok = de.Limit.Accept(v)
	if !ok {
		return de, false
	}
	de.Limit = node.(*Limit)
	return v.Leave(de)
}

// UpdateStmt is a statement to update columns of existing rows in tables with new values.
// See: https://dev.mysql.com/doc/refman/5.7/en/update.html
type UpdateStmt struct {
	dmlNode

	TableRefs     *Join
	List          []*Assignment
	Where         ExprNode
	Order         []*OrderByItem
	Limit         *Limit
	LowPriority   bool
	Ignore        bool
	MultipleTable bool
}

// Accept implements Node Accept interface.
func (up *UpdateStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(up) {
		return up, v.OK()
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
		up.Order[i] = node.(*OrderByItem)
	}
	node, ok = up.Limit.Accept(v)
	if !ok {
		return up, false
	}
	up.Limit = node.(*Limit)
	return v.Leave(up)
}

// Limit is the limit clause.
type Limit struct {
	node

	Offset uint64
	Count  uint64
}

// Accept implements Node Accept interface.
func (l *Limit) Accept(v Visitor) (Node, bool) {
	if !v.Enter(l) {
		return l, v.OK()
	}
	return v.Leave(l)
}
