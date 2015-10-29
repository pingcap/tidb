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
	_ DMLNode = &UnionStmt{}
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
	resultSetNode

	// Left table can be TableSource or JoinNode.
	Left ResultSetNode
	// Right table can be TableSource or JoinNode or nil.
	Right ResultSetNode
	// Tp represents join type.
	Tp JoinType
	// On represents join on condition.
	On *OnCondition
}

// Accept implements Node Accept interface.
func (nod *Join) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*Join)
	node, ok := nod.Left.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Left = node.(ResultSetNode)
	if nod.Right != nil {
		node, ok = nod.Right.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Right = node.(ResultSetNode)
	}
	if nod.On != nil {
		node, ok = nod.On.Accept(v)
		if !ok {
			return nod, false
		}
		nod.On = node.(*OnCondition)
	}
	return v.Leave(nod)
}

// TableName represents a table name.
type TableName struct {
	node
	resultSetNode

	Schema model.CIStr
	Name   model.CIStr

	DBInfo    *model.DBInfo
	TableInfo *model.TableInfo
}

// Accept implements Node Accept interface.
func (nod *TableName) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*TableName)
	return v.Leave(nod)
}

// TableSource represents table source with a name.
type TableSource struct {
	node

	// Source is the source of the data, can be a TableName,
	// a SelectStmt, a UnionStmt, or a JoinNode.
	Source ResultSetNode

	// AsName is the as name of the table source.
	AsName model.CIStr
}

// Accept implements Node Accept interface.
func (nod *TableSource) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*TableSource)
	node, ok := nod.Source.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Source = node.(ResultSetNode)
	return v.Leave(nod)
}

// OnCondition represetns JOIN on condition.
type OnCondition struct {
	node

	Expr ExprNode
}

// Accept implements Node Accept interface.
func (nod *OnCondition) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*OnCondition)
	node, ok := nod.Expr.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Expr = node.(ExprNode)
	return v.Leave(nod)
}

// SetResultFields implements ResultSet interface.
func (nod *TableSource) SetResultFields(rfs []*ResultField) {
	nod.Source.SetResultFields(rfs)
}

// GetResultFields implements ResultSet interface.
func (nod *TableSource) GetResultFields() []*ResultField {
	return nod.Source.GetResultFields()
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

	Table  model.CIStr
	Schema model.CIStr
}

// Accept implements Node Accept interface.
func (nod *WildCardField) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*WildCardField)
	return v.Leave(nod)
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
	// AsName name for Expr.
	AsName model.CIStr
}

// Accept implements Node Accept interface.
func (nod *SelectField) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*SelectField)
	if nod.Expr != nil {
		node, ok := nod.Expr.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Expr = node.(ExprNode)
	}
	return v.Leave(nod)
}

// FieldList represents field list in select statement.
type FieldList struct {
	node

	Fields []*SelectField
}

// Accept implements Node Accept interface.
func (nod *FieldList) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*FieldList)
	for i, val := range nod.Fields {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Fields[i] = node.(*SelectField)
	}
	return v.Leave(nod)
}

// TableRefsClause represents table references clause in dml statement.
type TableRefsClause struct {
	node

	TableRefs *Join
}

// Accept implements Node Accept interface.
func (nod *TableRefsClause) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*TableRefsClause)
	node, ok := nod.TableRefs.Accept(v)
	if !ok {
		return nod, false
	}
	nod.TableRefs = node.(*Join)
	return v.Leave(nod)
}

// ByItem represents an item in order by or group by.
type ByItem struct {
	node

	Expr ExprNode
	Desc bool
}

// Accept implements Node Accept interface.
func (nod *ByItem) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*ByItem)
	node, ok := nod.Expr.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Expr = node.(ExprNode)
	return v.Leave(nod)
}

// GroupByClause represents group by clause.
type GroupByClause struct {
	node
	Items []*ByItem
}

// Accept implements Node Accept interface.
func (nod *GroupByClause) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*GroupByClause)
	for i, val := range nod.Items {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Items[i] = node.(*ByItem)
	}
	return v.Leave(nod)
}

// HavingClause represents having clause.
type HavingClause struct {
	node
	Expr ExprNode
}

// Accept implements Node Accept interface.
func (nod *HavingClause) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*HavingClause)
	node, ok := nod.Expr.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Expr = node.(ExprNode)
	return v.Leave(nod)
}

// OrderByClause represents order by clause.
type OrderByClause struct {
	node
	Items    []*ByItem
	ForUnion bool
}

// Accept implements Node Accept interface.
func (nod *OrderByClause) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*OrderByClause)
	for i, val := range nod.Items {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Items[i] = node.(*ByItem)
	}
	return v.Leave(nod)
}

// SelectStmt represents the select query node.
// See: https://dev.mysql.com/doc/refman/5.7/en/select.html
type SelectStmt struct {
	dmlNode
	resultSetNode

	// Distinct represents if the select has distinct option.
	Distinct bool
	// From is the from clause of the query.
	From *TableRefsClause
	// Where is the where clause in select statement.
	Where ExprNode
	// Fields is the select expression list.
	Fields *FieldList
	// GroupBy is the group by expression list.
	GroupBy *GroupByClause
	// Having is the having condition.
	Having *HavingClause
	// OrderBy is the ordering expression list.
	OrderBy *OrderByClause
	// Limit is the limit clause.
	Limit *Limit
	// Lock is the lock type
	LockTp SelectLockType
}

// Accept implements Node Accept interface.
func (nod *SelectStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*SelectStmt)

	if nod.From != nil {
		node, ok := nod.From.Accept(v)
		if !ok {
			return nod, false
		}
		nod.From = node.(*TableRefsClause)
	}

	if nod.Where != nil {
		node, ok := nod.Where.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Where = node.(ExprNode)
	}

	if nod.Fields != nil {
		node, ok := nod.Fields.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Fields = node.(*FieldList)
	}

	if nod.GroupBy != nil {
		node, ok := nod.GroupBy.Accept(v)
		if !ok {
			return nod, false
		}
		nod.GroupBy = node.(*GroupByClause)
	}

	if nod.Having != nil {
		node, ok := nod.Having.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Having = node.(*HavingClause)
	}

	if nod.OrderBy != nil {
		node, ok := nod.OrderBy.Accept(v)
		if !ok {
			return nod, false
		}
		nod.OrderBy = node.(*OrderByClause)
	}

	if nod.Limit != nil {
		node, ok := nod.Limit.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Limit = node.(*Limit)
	}
	return v.Leave(nod)
}

// UnionClause represents a single "UNION SELECT ..." or "UNION (SELECT ...)" clause.
type UnionClause struct {
	node

	Distinct bool
	Select   *SelectStmt
}

// Accept implements Node Accept interface.
func (nod *UnionClause) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*UnionClause)
	node, ok := nod.Select.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Select = node.(*SelectStmt)
	return v.Leave(nod)
}

// UnionStmt represents "union statement"
// See: https://dev.mysql.com/doc/refman/5.7/en/union.html
type UnionStmt struct {
	dmlNode
	resultSetNode

	Distinct bool
	Selects  []*SelectStmt
	OrderBy  *OrderByClause
	Limit    *Limit
}

// Accept implements Node Accept interface.
func (nod *UnionStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*UnionStmt)
	for i, val := range nod.Selects {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Selects[i] = node.(*SelectStmt)
	}
	if nod.OrderBy != nil {
		node, ok := nod.OrderBy.Accept(v)
		if !ok {
			return nod, false
		}
		nod.OrderBy = node.(*OrderByClause)
	}
	if nod.Limit != nil {
		node, ok := nod.Limit.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Limit = node.(*Limit)
	}
	return v.Leave(nod)
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
func (nod *Assignment) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*Assignment)
	node, ok := nod.Column.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Column = node.(*ColumnName)
	node, ok = nod.Expr.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Expr = node.(ExprNode)
	return v.Leave(nod)
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

	Table       *TableRefsClause
	Columns     []*ColumnName
	Lists       [][]ExprNode
	Setlist     []*Assignment
	Priority    int
	OnDuplicate []*Assignment
	Select      ResultSetNode
}

// Accept implements Node Accept interface.
func (nod *InsertStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*InsertStmt)

	node, ok := nod.Table.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Table = node.(*TableRefsClause)

	for i, val := range nod.Columns {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Columns[i] = node.(*ColumnName)
	}
	for i, list := range nod.Lists {
		for j, val := range list {
			node, ok := val.Accept(v)
			if !ok {
				return nod, false
			}
			nod.Lists[i][j] = node.(ExprNode)
		}
	}
	for i, val := range nod.Setlist {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Setlist[i] = node.(*Assignment)
	}
	for i, val := range nod.OnDuplicate {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.OnDuplicate[i] = node.(*Assignment)
	}
	if nod.Select != nil {
		node, ok := nod.Select.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Select = node.(ResultSetNode)
	}
	return v.Leave(nod)
}

// DeleteStmt is a statement to delete rows from table.
// See: https://dev.mysql.com/doc/refman/5.7/en/delete.html
type DeleteStmt struct {
	dmlNode

	// Used in both single table and multiple table delete statement.
	TableRefs *TableRefsClause
	// Only used in multiple table delete statement.
	Tables      []*TableName
	Where       ExprNode
	Order       []*ByItem
	Limit       *Limit
	LowPriority bool
	Ignore      bool
	Quick       bool
	MultiTable  bool
	BeforeFrom  bool
}

// Accept implements Node Accept interface.
func (nod *DeleteStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*DeleteStmt)

	node, ok := nod.TableRefs.Accept(v)
	if !ok {
		return nod, false
	}
	nod.TableRefs = node.(*TableRefsClause)

	for i, val := range nod.Tables {
		node, ok = val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Tables[i] = node.(*TableName)
	}

	if nod.Where != nil {
		node, ok = nod.Where.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Where = node.(ExprNode)
	}

	for i, val := range nod.Order {
		node, ok = val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Order[i] = node.(*ByItem)
	}

	node, ok = nod.Limit.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Limit = node.(*Limit)
	return v.Leave(nod)
}

// UpdateStmt is a statement to update columns of existing rows in tables with new values.
// See: https://dev.mysql.com/doc/refman/5.7/en/update.html
type UpdateStmt struct {
	dmlNode

	TableRefs     *TableRefsClause
	List          []*Assignment
	Where         ExprNode
	Order         []*ByItem
	Limit         *Limit
	LowPriority   bool
	Ignore        bool
	MultipleTable bool
}

// Accept implements Node Accept interface.
func (nod *UpdateStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*UpdateStmt)
	node, ok := nod.TableRefs.Accept(v)
	if !ok {
		return nod, false
	}
	nod.TableRefs = node.(*TableRefsClause)
	for i, val := range nod.List {
		node, ok = val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.List[i] = node.(*Assignment)
	}
	if nod.Where != nil {
		node, ok = nod.Where.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Where = node.(ExprNode)
	}

	for i, val := range nod.Order {
		node, ok = val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Order[i] = node.(*ByItem)
	}
	node, ok = nod.Limit.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Limit = node.(*Limit)
	return v.Leave(nod)
}

// Limit is the limit clause.
type Limit struct {
	node

	Offset uint64
	Count  uint64
}

// Accept implements Node Accept interface.
func (nod *Limit) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*Limit)
	return v.Leave(nod)
}
