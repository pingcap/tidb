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

// FloatOpt is used for parsing floating-point type option from SQL.
// TODO: add reference doc.
type FloatOpt struct {
	Flen    int
	Decimal int
}

// CharsetOpt is used for parsing charset option from SQL.
type CharsetOpt struct {
	Chs string
	Col string
}

// DatabaseOptionType is the type for database options.
type DatabaseOptionType int

// Database option types.
const (
	DatabaseOptionNone DatabaseOptionType = iota
	DatabaseOptionCharset
	DatabaseOptionCollate
)

// DatabaseOption represents database option.
type DatabaseOption struct {
	node

	Tp    DatabaseOptionType
	Value string
}

// CreateDatabaseStmt is a statement to create a database.
// See: https://dev.mysql.com/doc/refman/5.7/en/create-database.html
type CreateDatabaseStmt struct {
	ddlNode

	IfNotExists bool
	Name        string
	Options     []*DatabaseOption
}

// Accept implements Node Accept interface.
func (cd *CreateDatabaseStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(cd) {
		return cd, v.OK()
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
		return dd, v.OK()
	}
	return v.Leave(dd)
}

// IndexColName is used for parsing index column name from SQL.
type IndexColName struct {
	node

	Column *ColumnName
	Length int
}

// Accept implements Node Accept interface.
func (ic *IndexColName) Accept(v Visitor) (Node, bool) {
	if !v.Enter(ic) {
		return ic, v.OK()
	}
	node, ok := ic.Column.Accept(v)
	if !ok {
		return ic, false
	}
	ic.Column = node.(*ColumnName)
	return v.Leave(ic)
}

// ReferenceDef is used for parsing foreign key reference option from SQL.
// See: http://dev.mysql.com/doc/refman/5.7/en/create-table-foreign-keys.html
type ReferenceDef struct {
	node

	Table         *TableName
	IndexColNames []*IndexColName
}

// Accept implements Node Accept interface.
func (rd *ReferenceDef) Accept(v Visitor) (Node, bool) {
	if !v.Enter(rd) {
		return rd, v.OK()
	}
	node, ok := rd.Table.Accept(v)
	if !ok {
		return rd, false
	}
	rd.Table = node.(*TableName)
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
	Expr ExprNode
}

// Accept implements Node Accept interface.
func (co *ColumnOption) Accept(v Visitor) (Node, bool) {
	if !v.Enter(co) {
		return co, v.OK()
	}
	if co.Expr != nil {
		node, ok := co.Expr.Accept(v)
		if !ok {
			return co, false
		}
		co.Expr = node.(ExprNode)
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
	ConstraintFulltext
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
		return tc, v.OK()
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

	Name    *ColumnName
	Tp      *types.FieldType
	Options []*ColumnOption
}

// Accept implements Node Accept interface.
func (cd *ColumnDef) Accept(v Visitor) (Node, bool) {
	if !v.Enter(cd) {
		return cd, v.OK()
	}
	node, ok := cd.Name.Accept(v)
	if !ok {
		return cd, false
	}
	cd.Name = node.(*ColumnName)
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
	Table       *TableName
	Cols        []*ColumnDef
	Constraints []*Constraint
	Options     []*TableOption
}

// Accept implements Node Accept interface.
func (ct *CreateTableStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(ct) {
		return ct, v.OK()
	}
	node, ok := ct.Table.Accept(v)
	if !ok {
		return ct, false
	}
	ct.Table = node.(*TableName)
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

	IfExists bool
	Tables   []*TableName
}

// Accept implements Node Accept interface.
func (dt *DropTableStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(dt) {
		return dt, v.OK()
	}
	for i, val := range dt.Tables {
		node, ok := val.Accept(v)
		if !ok {
			return dt, false
		}
		dt.Tables[i] = node.(*TableName)
	}
	return v.Leave(dt)
}

// CreateIndexStmt is a statement to create an index.
// See: https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type CreateIndexStmt struct {
	ddlNode

	IndexName     string
	Table         *TableName
	Unique        bool
	IndexColNames []*IndexColName
}

// Accept implements Node Accept interface.
func (ci *CreateIndexStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(ci) {
		return ci, v.OK()
	}
	node, ok := ci.Table.Accept(v)
	if !ok {
		return ci, false
	}
	ci.Table = node.(*TableName)
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
	Table     *TableName
}

// Accept implements Node Accept interface.
func (di *DropIndexStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(di) {
		return di, v.OK()
	}
	node, ok := di.Table.Accept(v)
	if !ok {
		return di, false
	}
	di.Table = node.(*TableName)
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
	RelativeColumn *ColumnName
}

// Accept implements Node Accept interface.
func (cp *ColumnPosition) Accept(v Visitor) (Node, bool) {
	if !v.Enter(cp) {
		return cp, v.OK()
	}
	if cp.RelativeColumn != nil {
		node, ok := cp.RelativeColumn.Accept(v)
		if !ok {
			return cp, false
		}
		cp.RelativeColumn = node.(*ColumnName)
	}
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
	Options    []*TableOption
	Column     *ColumnDef
	ColumnName *ColumnName
	Position   *ColumnPosition
}

// Accept implements Node Accept interface.
func (as *AlterTableSpec) Accept(v Visitor) (Node, bool) {
	if !v.Enter(as) {
		return as, v.OK()
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
	if as.ColumnName != nil {
		node, ok := as.ColumnName.Accept(v)
		if !ok {
			return as, false
		}
		as.ColumnName = node.(*ColumnName)
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

	Table *TableName
	Specs []*AlterTableSpec
}

// Accept implements Node Accept interface.
func (at *AlterTableStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(at) {
		return at, v.OK()
	}
	node, ok := at.Table.Accept(v)
	if !ok {
		return at, false
	}
	at.Table = node.(*TableName)
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

	Table *TableName
}

// Accept implements Node Accept interface.
func (ts *TruncateTableStmt) Accept(v Visitor) (Node, bool) {
	if !v.Enter(ts) {
		return ts, v.OK()
	}
	node, ok := ts.Table.Accept(v)
	if !ok {
		return ts, false
	}
	ts.Table = node.(*TableName)
	return v.Leave(ts)
}
