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
func (nod *CreateDatabaseStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*CreateDatabaseStmt)
	return v.Leave(nod)
}

// DropDatabaseStmt is a statement to drop a database and all tables in the database.
// See: https://dev.mysql.com/doc/refman/5.7/en/drop-database.html
type DropDatabaseStmt struct {
	ddlNode

	IfExists bool
	Name     string
}

// Accept implements Node Accept interface.
func (nod *DropDatabaseStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*DropDatabaseStmt)
	return v.Leave(nod)
}

// IndexColName is used for parsing index column name from SQL.
type IndexColName struct {
	node

	Column *ColumnName
	Length int
}

// Accept implements Node Accept interface.
func (nod *IndexColName) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*IndexColName)
	node, ok := nod.Column.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Column = node.(*ColumnName)
	return v.Leave(nod)
}

// ReferenceDef is used for parsing foreign key reference option from SQL.
// See: http://dev.mysql.com/doc/refman/5.7/en/create-table-foreign-keys.html
type ReferenceDef struct {
	node

	Table         *TableName
	IndexColNames []*IndexColName
}

// Accept implements Node Accept interface.
func (nod *ReferenceDef) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*ReferenceDef)
	node, ok := nod.Table.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Table = node.(*TableName)
	for i, val := range nod.IndexColNames {
		node, ok = val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.IndexColNames[i] = node.(*IndexColName)
	}
	return v.Leave(nod)
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
func (nod *ColumnOption) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*ColumnOption)
	if nod.Expr != nil {
		node, ok := nod.Expr.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Expr = node.(ExprNode)
	}
	return v.Leave(nod)
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
func (nod *Constraint) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*Constraint)
	for i, val := range nod.Keys {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Keys[i] = node.(*IndexColName)
	}
	if nod.Refer != nil {
		node, ok := nod.Refer.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Refer = node.(*ReferenceDef)
	}
	return v.Leave(nod)
}

// ColumnDef is used for parsing column definition from SQL.
type ColumnDef struct {
	node

	Name    *ColumnName
	Tp      *types.FieldType
	Options []*ColumnOption
}

// Accept implements Node Accept interface.
func (nod *ColumnDef) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*ColumnDef)
	node, ok := nod.Name.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Name = node.(*ColumnName)
	for i, val := range nod.Options {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Options[i] = node.(*ColumnOption)
	}
	return v.Leave(nod)
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
func (nod *CreateTableStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*CreateTableStmt)
	node, ok := nod.Table.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Table = node.(*TableName)
	for i, val := range nod.Cols {
		node, ok = val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Cols[i] = node.(*ColumnDef)
	}
	for i, val := range nod.Constraints {
		node, ok = val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Constraints[i] = node.(*Constraint)
	}
	return v.Leave(nod)
}

// DropTableStmt is a statement to drop one or more tables.
// See: https://dev.mysql.com/doc/refman/5.7/en/drop-table.html
type DropTableStmt struct {
	ddlNode

	IfExists bool
	Tables   []*TableName
}

// Accept implements Node Accept interface.
func (nod *DropTableStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*DropTableStmt)
	for i, val := range nod.Tables {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Tables[i] = node.(*TableName)
	}
	return v.Leave(nod)
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
func (nod *CreateIndexStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*CreateIndexStmt)
	node, ok := nod.Table.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Table = node.(*TableName)
	for i, val := range nod.IndexColNames {
		node, ok = val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.IndexColNames[i] = node.(*IndexColName)
	}
	return v.Leave(nod)
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
func (nod *DropIndexStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*DropIndexStmt)
	node, ok := nod.Table.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Table = node.(*TableName)
	return v.Leave(nod)
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
func (nod *ColumnPosition) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*ColumnPosition)
	if nod.RelativeColumn != nil {
		node, ok := nod.RelativeColumn.Accept(v)
		if !ok {
			return nod, false
		}
		nod.RelativeColumn = node.(*ColumnName)
	}
	return v.Leave(nod)
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
func (nod *AlterTableSpec) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*AlterTableSpec)
	if nod.Constraint != nil {
		node, ok := nod.Constraint.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Constraint = node.(*Constraint)
	}
	if nod.Column != nil {
		node, ok := nod.Column.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Column = node.(*ColumnDef)
	}
	if nod.ColumnName != nil {
		node, ok := nod.ColumnName.Accept(v)
		if !ok {
			return nod, false
		}
		nod.ColumnName = node.(*ColumnName)
	}
	if nod.Position != nil {
		node, ok := nod.Position.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Position = node.(*ColumnPosition)
	}
	return v.Leave(nod)
}

// AlterTableStmt is a statement to change the structure of a table.
// See: https://dev.mysql.com/doc/refman/5.7/en/alter-table.html
type AlterTableStmt struct {
	ddlNode

	Table *TableName
	Specs []*AlterTableSpec
}

// Accept implements Node Accept interface.
func (nod *AlterTableStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*AlterTableStmt)
	node, ok := nod.Table.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Table = node.(*TableName)
	for i, val := range nod.Specs {
		node, ok = val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Specs[i] = node.(*AlterTableSpec)
	}
	return v.Leave(nod)
}

// TruncateTableStmt is a statement to empty a table completely.
// See: https://dev.mysql.com/doc/refman/5.7/en/truncate-table.html
type TruncateTableStmt struct {
	ddlNode

	Table *TableName
}

// Accept implements Node Accept interface.
func (nod *TruncateTableStmt) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*TruncateTableStmt)
	node, ok := nod.Table.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Table = node.(*TableName)
	return v.Leave(nod)
}
