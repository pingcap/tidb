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
	"github.com/pingcap/tidb/util/types"
)

var (
	_ DDLNode = &CreateDatabaseStmt{}
	_ DDLNode = &DropDatabaseStmt{}
	_ DDLNode = &CreateTableStmt{}
	_ DDLNode = &DropTableStmt{}
	_ DDLNode = &CreateIndexStmt{}
	_ DDLNode = &DropTableStmt{}
	_ DDLNode = &AlterTableStmt{}
	_ DDLNode = &TruncateTableStmt{}
	_ Node    = &IndexColName{}
	_ Node    = &ReferenceDef{}
	_ Node    = &ColumnOption{}
	_ Node    = &Constraint{}
	_ Node    = &ColumnDef{}
	_ Node    = &ColumnPosition{}
	_ Node    = &AlterTableSpec{}
)

// CharsetOpt is used for parsing charset option from SQL.
type CharsetOpt struct {
	Chs string
	Col string
}

// CreateDatabaseStmt is a statement to create a database.
// See: https://dev.mysql.com/doc/refman/5.7/en/create-database.html
type CreateDatabaseStmt struct {
	ddlNode

	IfNotExists bool
	Name        string
	Opt         *CharsetOpt
}

// Accept implements Node Accept interface.
func (cd *CreateDatabaseStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(cd) {
		return cd, false
	}
	return v.Leave(cd)
}

// DropDatabaseStmt is a statement to drop a database and all tables in the database.
// See: https://dev.mysql.com/doc/refman/5.7/en/drop-database.html
type DropDatabaseStmt struct {
	ddlNode

	IfExists bool
	Name     string
}

// Accept implements Node Accept interface.
func (dd *DropDatabaseStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(dd) {
		return dd, false
	}
	return v.Leave(dd)
}

// IndexColName is used for parsing index column name from SQL.
type IndexColName struct {
	node

	Column *ColumnRefExpr
	Length int
}

// Accept implements Node Accept interface.
func (ic *IndexColName) Accept(v Visitor) (Node, bool) {
	if !v.Enter(ic) {
		return ic, false
	}
	node, ok := ic.Column.Accept(v)
	if !ok {
		return ic, false
	}
	ic.Column = node.(*ColumnRefExpr)
	return v.Leave(ic)
}

// ReferenceDef is used for parsing foreign key reference option from SQL.
// See: http://dev.mysql.com/doc/refman/5.7/en/create-table-foreign-keys.html
type ReferenceDef struct {
	node

	Table         *TableRef
	IndexColNames []*IndexColName
}

// Accept implements Node Accept interface.
func (rd *ReferenceDef) Accept(v Visitor) (Node, bool) {
	if !v.Enter(rd) {
		return rd, false
	}
	node, ok := rd.Table.Accept(v)
	if !ok {
		return rd, false
	}
	rd.Table = node.(*TableRef)
	for i, val := range rd.IndexColNames {
		node, ok = val.Accept(v)
		if !ok {
			return rd, false
		}
		rd.IndexColNames[i] = node.(*IndexColName)
	}
	return v.Leave(rd)
}

// ColumnOptionType is the type for ColumnOption.
type ColumnOptionType int

// ColumnOption types.
const (
	ColumnOptionNoOption ColumnOptionType = iota
	ColumnOptionPrimaryKey
	ColumnOptionNotNull
	ColumnOptionAutoIncrement
	ColumnOptionDefaultValue
	ColumnOptionUniq
	ColumnOptionIndex
	ColumnOptionUniqIndex
	ColumnOptionKey
	ColumnOptionUniqKey
	ColumnOptionNull
	ColumnOptionOnUpdate // For Timestamp and Datetime only.
	ColumnOptionFulltext
	ColumnOptionComment
)

// ColumnOption is used for parsing column constraint info from SQL.
type ColumnOption struct {
	node

	Tp ColumnOptionType
	// The value For Default or On Update.
	Val ExprNode
}

// Accept implements Node Accept interface.
func (co *ColumnOption) Accept(v Visitor) (Node, bool) {
	if !v.Enter(co) {
		return co, false
	}
	if co.Val != nil {
		node, ok := co.Val.Accept(v)
		if !ok {
			return co, false
		}
		co.Val = node.(ExprNode)
	}
	return v.Leave(co)
}

// ConstraintType is the type for Constraint.
type ConstraintType int

// ConstraintTypes
const (
	ConstraintNoConstraint ConstraintType = iota
	ConstraintPrimaryKey
	ConstraintKey
	ConstraintIndex
	ConstraintUniq
	ConstraintUniqKey
	ConstraintUniqIndex
	ConstraintForeignKey
)

// Constraint is constraint for table definition.
type Constraint struct {
	node

	Tp   ConstraintType
	Name string

	// Used for PRIMARY KEY, UNIQUE, ......
	Keys []*IndexColName

	// Used for foreign key.
	Refer *ReferenceDef
}

// Accept implements Node Accept interface.
func (tc *Constraint) Accept(v Visitor) (Node, bool) {
	if !v.Enter(tc) {
		return tc, false
	}
	for i, val := range tc.Keys {
		node, ok := val.Accept(v)
		if !ok {
			return tc, false
		}
		tc.Keys[i] = node.(*IndexColName)
	}
	if tc.Refer != nil {
		node, ok := tc.Refer.Accept(v)
		if !ok {
			return tc, false
		}
		tc.Refer = node.(*ReferenceDef)
	}
	return v.Leave(tc)
}

// ColumnDef is used for parsing column definition from SQL.
type ColumnDef struct {
	node

	Name    string
	Tp      *types.FieldType
	Options []*ColumnOption
}

// Accept implements Node Accept interface.
func (cd *ColumnDef) Accept(v Visitor) (Node, bool) {
	if !v.Enter(cd) {
		return cd, false
	}
	for i, val := range cd.Options {
		node, ok := val.Accept(v)
		if !ok {
			return cd, false
		}
		cd.Options[i] = node.(*ColumnOption)
	}
	return v.Leave(cd)
}

// CreateTableStmt is a statement to create a table.
// See: https://dev.mysql.com/doc/refman/5.7/en/create-table.html
type CreateTableStmt struct {
	ddlNode

	IfNotExists bool
	Table       *TableRef
	Cols        []*ColumnDef
	Constraints []*Constraint
	Options     []*TableOption
}

// Accept implements Node Accept interface.
func (ct *CreateTableStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(ct) {
		return ct, false
	}
	node, ok := ct.Table.Accept(v)
	if !ok {
		return ct, false
	}
	ct.Table = node.(*TableRef)
	for i, val := range ct.Cols {
		node, ok = val.Accept(v)
		if !ok {
			return ct, false
		}
		ct.Cols[i] = node.(*ColumnDef)
	}
	for i, val := range ct.Constraints {
		node, ok = val.Accept(v)
		if !ok {
			return ct, false
		}
		ct.Constraints[i] = node.(*Constraint)
	}
	return v.Leave(ct)
}

// DropTableStmt is a statement to drop one or more tables.
// See: https://dev.mysql.com/doc/refman/5.7/en/drop-table.html
type DropTableStmt struct {
	ddlNode

	IfExists  bool
	TableRefs []*TableRef
}

// Accept implements Node Accept interface.
func (dt *DropTableStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(dt) {
		return dt, false
	}
	for i, val := range dt.TableRefs {
		node, ok := val.Accept(v)
		if !ok {
			return dt, false
		}
		dt.TableRefs[i] = node.(*TableRef)
	}
	return v.Leave(dt)
}

// CreateIndexStmt is a statement to create an index.
// See: https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type CreateIndexStmt struct {
	ddlNode

	IndexName     string
	Table         *TableRef
	Unique        bool
	IndexColNames []*IndexColName
}

// Accept implements Node Accept interface.
func (ci *CreateIndexStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(ci) {
		return ci, false
	}
	node, ok := ci.Table.Accept(v)
	if !ok {
		return ci, false
	}
	ci.Table = node.(*TableRef)
	for i, val := range ci.IndexColNames {
		node, ok = val.Accept(v)
		if !ok {
			return ci, false
		}
		ci.IndexColNames[i] = node.(*IndexColName)
	}
	return v.Leave(ci)
}

// DropIndexStmt is a statement to drop the index.
// See: https://dev.mysql.com/doc/refman/5.7/en/drop-index.html
type DropIndexStmt struct {
	ddlNode

	IfExists  bool
	IndexName string
}

// Accept implements Node Accept interface.
func (di *DropIndexStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(di) {
		return di, false
	}
	return v.Leave(di)
}

// TableOptionType is the type for TableOption
type TableOptionType int

// TableOption types.
const (
	TableOptionNone TableOptionType = iota
	TableOptionEngine
	TableOptionCharset
	TableOptionCollate
	TableOptionAutoIncrement
	TableOptionComment
	TableOptionAvgRowLength
	TableOptionCheckSum
	TableOptionCompression
	TableOptionConnection
	TableOptionPassword
	TableOptionKeyBlockSize
	TableOptionMaxRows
	TableOptionMinRows
)

// TableOption is used for parsing table option from SQL.
type TableOption struct {
	Tp        TableOptionType
	StrValue  string
	UintValue uint64
}

// ColumnPositionType is the type for ColumnPosition.
type ColumnPositionType int

// ColumnPosition Types
const (
	ColumnPositionNone ColumnPositionType = iota
	ColumnPositionFirst
	ColumnPositionAfter
)

// ColumnPosition represent the position of the newly added column
type ColumnPosition struct {
	node
	// ColumnPositionNone | ColumnPositionFirst | ColumnPositionAfter
	Tp ColumnPositionType
	// RelativeColumn is the column the newly added column after if type is ColumnPositionAfter
	RelativeColumn *ColumnRefExpr
}

// Accept implements Node Accept interface.
func (cp *ColumnPosition) Accept(v Visitor) (Node, bool) {
	if !v.Enter(cp) {
		return cp, false
	}
	node, ok := cp.RelativeColumn.Accept(v)
	if !ok {
		return cp, false
	}
	cp.RelativeColumn = node.(*ColumnRefExpr)
	return v.Leave(cp)
}

// AlterTableType is the type for AlterTableSpec.
type AlterTableType int

// AlterTable types.
const (
	AlterTableOption AlterTableType = iota + 1
	AlterTableAddColumn
	AlterTableAddConstraint
	AlterTableDropColumn
	AlterTableDropPrimaryKey
	AlterTableDropIndex
	AlterTableDropForeignKey

// TODO: Add more actions
)

// AlterTableSpec represents alter table specification.
type AlterTableSpec struct {
	node

	Tp         AlterTableType
	Name       string
	Constraint *Constraint
	TableOpts  []*TableOption
	Column     *ColumnDef
	Position   *ColumnPosition
}

// Accept implements Node Accept interface.
func (as *AlterTableSpec) Accept(v Visitor) (Node, bool) {
	if !v.Enter(as) {
		return as, false
	}
	if as.Constraint != nil {
		node, ok := as.Constraint.Accept(v)
		if !ok {
			return as, false
		}
		as.Constraint = node.(*Constraint)
	}
	if as.Column != nil {
		node, ok := as.Column.Accept(v)
		if !ok {
			return as, false
		}
		as.Column = node.(*ColumnDef)
	}
	if as.Position != nil {
		node, ok := as.Position.Accept(v)
		if !ok {
			return as, false
		}
		as.Position = node.(*ColumnPosition)
	}
	return v.Leave(as)
}

// AlterTableStmt is a statement to change the structure of a table.
// See: https://dev.mysql.com/doc/refman/5.7/en/alter-table.html
type AlterTableStmt struct {
	ddlNode

	Table *TableRef
	Specs []*AlterTableSpec
}

// Accept implements Node Accept interface.
func (at *AlterTableStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(at) {
		return at, false
	}
	node, ok := at.Table.Accept(v)
	if !ok {
		return at, false
	}
	at.Table = node.(*TableRef)
	for i, val := range at.Specs {
		node, ok = val.Accept(v)
		if !ok {
			return at, false
		}
		at.Specs[i] = node.(*AlterTableSpec)
	}
	return v.Leave(at)
}

// TruncateTableStmt is a statement to empty a table completely.
// See: https://dev.mysql.com/doc/refman/5.7/en/truncate-table.html
type TruncateTableStmt struct {
	ddlNode

	Table *TableRef
}

// Accept implements Node Accept interface.
func (ts *TruncateTableStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(ts) {
		return ts, false
	}
	node, ok := ts.Table.Accept(v)
	if !ok {
		return ts, false
	}
	ts.Table = node.(*TableRef)
	return v.Leave(ts)
}
