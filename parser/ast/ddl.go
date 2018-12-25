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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/types"
)

var (
	_ DDLNode = &AlterTableStmt{}
	_ DDLNode = &CreateDatabaseStmt{}
	_ DDLNode = &CreateIndexStmt{}
	_ DDLNode = &CreateTableStmt{}
	_ DDLNode = &CreateViewStmt{}
	_ DDLNode = &DropDatabaseStmt{}
	_ DDLNode = &DropIndexStmt{}
	_ DDLNode = &DropTableStmt{}
	_ DDLNode = &RenameTableStmt{}
	_ DDLNode = &TruncateTableStmt{}

	_ Node = &AlterTableSpec{}
	_ Node = &ColumnDef{}
	_ Node = &ColumnOption{}
	_ Node = &ColumnPosition{}
	_ Node = &Constraint{}
	_ Node = &IndexColName{}
	_ Node = &ReferenceDef{}
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
	Tp    DatabaseOptionType
	Value string
}

// Restore implements Node interface.
func (n *DatabaseOption) Restore(ctx *RestoreCtx) error {
	switch n.Tp {
	case DatabaseOptionCharset:
		ctx.WriteKeyWord("CHARACTER SET")
		ctx.WritePlain(" = ")
		ctx.WritePlain(n.Value)
	case DatabaseOptionCollate:
		ctx.WriteKeyWord("COLLATE")
		ctx.WritePlain(" = ")
		ctx.WritePlain(n.Value)
	default:
		return errors.Errorf("invalid DatabaseOptionType: %d", n.Tp)
	}
	return nil
}

// CreateDatabaseStmt is a statement to create a database.
// See https://dev.mysql.com/doc/refman/5.7/en/create-database.html
type CreateDatabaseStmt struct {
	ddlNode

	IfNotExists bool
	Name        string
	Options     []*DatabaseOption
}

// Restore implements Node interface.
func (n *CreateDatabaseStmt) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord("CREATE DATABASE ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	ctx.WriteName(n.Name)
	for _, option := range n.Options {
		ctx.WritePlain(" ")
		err := option.Restore(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateDatabaseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateDatabaseStmt)
	return v.Leave(n)
}

// DropDatabaseStmt is a statement to drop a database and all tables in the database.
// See https://dev.mysql.com/doc/refman/5.7/en/drop-database.html
type DropDatabaseStmt struct {
	ddlNode

	IfExists bool
	Name     string
}

// Restore implements Node interface.
func (n *DropDatabaseStmt) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord("DROP DATABASE ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	ctx.WriteName(n.Name)
	return nil
}

// Accept implements Node Accept interface.
func (n *DropDatabaseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropDatabaseStmt)
	return v.Leave(n)
}

// IndexColName is used for parsing index column name from SQL.
type IndexColName struct {
	node

	Column *ColumnName
	Length int
}

// Restore implements Node interface.
func (n *IndexColName) Restore(ctx *RestoreCtx) error {
	if err := n.Column.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while splicing IndexColName")
	}
	if n.Length > 0 {
		ctx.WritePlainf("(%d)", n.Length)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *IndexColName) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*IndexColName)
	node, ok := n.Column.Accept(v)
	if !ok {
		return n, false
	}
	n.Column = node.(*ColumnName)
	return v.Leave(n)
}

// ReferenceDef is used for parsing foreign key reference option from SQL.
// See http://dev.mysql.com/doc/refman/5.7/en/create-table-foreign-keys.html
type ReferenceDef struct {
	node

	Table         *TableName
	IndexColNames []*IndexColName
	OnDelete      *OnDeleteOpt
	OnUpdate      *OnUpdateOpt
}

// Restore implements Node interface.
func (n *ReferenceDef) Restore(ctx *RestoreCtx) error {
	if n.Table != nil {
		ctx.WriteKeyWord("REFERENCES ")
		if err := n.Table.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing ReferenceDef")
		}
	}
	ctx.WritePlain("(")
	for i, indexColNames := range n.IndexColNames {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		if err := indexColNames.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing IndexColNames: [%v]", i)
		}
	}
	ctx.WritePlain(")")
	if n.OnDelete.ReferOpt != ReferOptionNoOption {
		ctx.WritePlain(" ")
		if err := n.OnDelete.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing OnDelete")
		}
	}
	if n.OnUpdate.ReferOpt != ReferOptionNoOption {
		ctx.WritePlain(" ")
		if err := n.OnUpdate.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing OnUpdate")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ReferenceDef) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ReferenceDef)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	for i, val := range n.IndexColNames {
		node, ok = val.Accept(v)
		if !ok {
			return n, false
		}
		n.IndexColNames[i] = node.(*IndexColName)
	}
	onDelete, ok := n.OnDelete.Accept(v)
	if !ok {
		return n, false
	}
	n.OnDelete = onDelete.(*OnDeleteOpt)
	onUpdate, ok := n.OnUpdate.Accept(v)
	if !ok {
		return n, false
	}
	n.OnUpdate = onUpdate.(*OnUpdateOpt)
	return v.Leave(n)
}

// ReferOptionType is the type for refer options.
type ReferOptionType int

// Refer option types.
const (
	ReferOptionNoOption ReferOptionType = iota
	ReferOptionRestrict
	ReferOptionCascade
	ReferOptionSetNull
	ReferOptionNoAction
)

// String implements fmt.Stringer interface.
func (r ReferOptionType) String() string {
	switch r {
	case ReferOptionRestrict:
		return "RESTRICT"
	case ReferOptionCascade:
		return "CASCADE"
	case ReferOptionSetNull:
		return "SET NULL"
	case ReferOptionNoAction:
		return "NO ACTION"
	}
	return ""
}

// OnDeleteOpt is used for optional on delete clause.
type OnDeleteOpt struct {
	node
	ReferOpt ReferOptionType
}

// Restore implements Node interface.
func (n *OnDeleteOpt) Restore(ctx *RestoreCtx) error {
	if n.ReferOpt != ReferOptionNoOption {
		ctx.WriteKeyWord("ON DELETE ")
		ctx.WriteKeyWord(n.ReferOpt.String())
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *OnDeleteOpt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*OnDeleteOpt)
	return v.Leave(n)
}

// OnUpdateOpt is used for optional on update clause.
type OnUpdateOpt struct {
	node
	ReferOpt ReferOptionType
}

// Restore implements Node interface.
func (n *OnUpdateOpt) Restore(ctx *RestoreCtx) error {
	if n.ReferOpt != ReferOptionNoOption {
		ctx.WriteKeyWord("ON UPDATE ")
		ctx.WriteKeyWord(n.ReferOpt.String())
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *OnUpdateOpt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*OnUpdateOpt)
	return v.Leave(n)
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
	ColumnOptionUniqKey
	ColumnOptionNull
	ColumnOptionOnUpdate // For Timestamp and Datetime only.
	ColumnOptionFulltext
	ColumnOptionComment
	ColumnOptionGenerated
	ColumnOptionReference
)

// ColumnOption is used for parsing column constraint info from SQL.
type ColumnOption struct {
	node

	Tp ColumnOptionType
	// Expr is used for ColumnOptionDefaultValue/ColumnOptionOnUpdateColumnOptionGenerated.
	// For ColumnOptionDefaultValue or ColumnOptionOnUpdate, it's the target value.
	// For ColumnOptionGenerated, it's the target expression.
	Expr ExprNode
	// Stored is only for ColumnOptionGenerated, default is false.
	Stored bool
	// Refer is used for foreign key.
	Refer *ReferenceDef
}

// Restore implements Node interface.
func (n *ColumnOption) Restore(ctx *RestoreCtx) error {
	return errors.New("Not implemented")
}

// Accept implements Node Accept interface.
func (n *ColumnOption) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ColumnOption)
	if n.Expr != nil {
		node, ok := n.Expr.Accept(v)
		if !ok {
			return n, false
		}
		n.Expr = node.(ExprNode)
	}
	return v.Leave(n)
}

// IndexOption is the index options.
//    KEY_BLOCK_SIZE [=] value
//  | index_type
//  | WITH PARSER parser_name
//  | COMMENT 'string'
// See http://dev.mysql.com/doc/refman/5.7/en/create-table.html
type IndexOption struct {
	node

	KeyBlockSize uint64
	Tp           model.IndexType
	Comment      string
}

// Restore implements Node interface.
func (n *IndexOption) Restore(ctx *RestoreCtx) error {
	hasPrevOption := false
	if n.KeyBlockSize > 0 {
		ctx.WriteKeyWord("KEY_BLOCK_SIZE")
		ctx.WritePlainf("=%d", n.KeyBlockSize)
		hasPrevOption = true
	}

	if n.Tp != model.IndexTypeInvalid {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("USING ")
		ctx.WritePlain(n.Tp.String())
		hasPrevOption = true
	}

	if n.Comment != "" {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("COMMENT ")
		ctx.WriteString(n.Comment)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *IndexOption) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*IndexOption)
	return v.Leave(n)
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

	Keys []*IndexColName // Used for PRIMARY KEY, UNIQUE, ......

	Refer *ReferenceDef // Used for foreign key.

	Option *IndexOption // Index Options
}

// Restore implements Node interface.
func (n *Constraint) Restore(ctx *RestoreCtx) error {
	return errors.New("Not implemented")
}

// Accept implements Node Accept interface.
func (n *Constraint) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*Constraint)
	for i, val := range n.Keys {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Keys[i] = node.(*IndexColName)
	}
	if n.Refer != nil {
		node, ok := n.Refer.Accept(v)
		if !ok {
			return n, false
		}
		n.Refer = node.(*ReferenceDef)
	}
	if n.Option != nil {
		node, ok := n.Option.Accept(v)
		if !ok {
			return n, false
		}
		n.Option = node.(*IndexOption)
	}
	return v.Leave(n)
}

// ColumnDef is used for parsing column definition from SQL.
type ColumnDef struct {
	node

	Name    *ColumnName
	Tp      *types.FieldType
	Options []*ColumnOption
}

// Restore implements Node interface.
func (n *ColumnDef) Restore(ctx *RestoreCtx) error {
	return errors.New("Not implemented")
}

// Accept implements Node Accept interface.
func (n *ColumnDef) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ColumnDef)
	node, ok := n.Name.Accept(v)
	if !ok {
		return n, false
	}
	n.Name = node.(*ColumnName)
	for i, val := range n.Options {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Options[i] = node.(*ColumnOption)
	}
	return v.Leave(n)
}

// CreateTableStmt is a statement to create a table.
// See https://dev.mysql.com/doc/refman/5.7/en/create-table.html
type CreateTableStmt struct {
	ddlNode

	IfNotExists bool
	Table       *TableName
	ReferTable  *TableName
	Cols        []*ColumnDef
	Constraints []*Constraint
	Options     []*TableOption
	Partition   *PartitionOptions
	OnDuplicate OnDuplicateCreateTableSelectType
	Select      ResultSetNode
}

// Restore implements Node interface.
func (n *CreateTableStmt) Restore(ctx *RestoreCtx) error {
	return errors.New("Not implemented")
}

// Accept implements Node Accept interface.
func (n *CreateTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateTableStmt)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	if n.ReferTable != nil {
		node, ok = n.ReferTable.Accept(v)
		if !ok {
			return n, false
		}
		n.ReferTable = node.(*TableName)
	}
	for i, val := range n.Cols {
		node, ok = val.Accept(v)
		if !ok {
			return n, false
		}
		n.Cols[i] = node.(*ColumnDef)
	}
	for i, val := range n.Constraints {
		node, ok = val.Accept(v)
		if !ok {
			return n, false
		}
		n.Constraints[i] = node.(*Constraint)
	}
	if n.Select != nil {
		node, ok := n.Select.Accept(v)
		if !ok {
			return n, false
		}
		n.Select = node.(ResultSetNode)
	}

	return v.Leave(n)
}

// DropTableStmt is a statement to drop one or more tables.
// See https://dev.mysql.com/doc/refman/5.7/en/drop-table.html
type DropTableStmt struct {
	ddlNode

	IfExists bool
	Tables   []*TableName
	IsView   bool
}

// Restore implements Node interface.
func (n *DropTableStmt) Restore(ctx *RestoreCtx) error {
	if n.IsView {
		ctx.WriteKeyWord("DROP VIEW ")
	} else {
		ctx.WriteKeyWord("DROP TABLE ")
	}
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}

	for index, table := range n.Tables {
		if index != 0 {
			ctx.WritePlain(", ")
		}
		if err := table.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore DropTableStmt.Tables "+string(index))
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *DropTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropTableStmt)
	for i, val := range n.Tables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}
	return v.Leave(n)
}

// RenameTableStmt is a statement to rename a table.
// See http://dev.mysql.com/doc/refman/5.7/en/rename-table.html
type RenameTableStmt struct {
	ddlNode

	OldTable *TableName
	NewTable *TableName

	// TableToTables is only useful for syncer which depends heavily on tidb parser to do some dirty work for now.
	// TODO: Refactor this when you are going to add full support for multiple schema changes.
	TableToTables []*TableToTable
}

// Restore implements Node interface.
func (n *RenameTableStmt) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord("RENAME TABLE ")
	for index, table2table := range n.TableToTables {
		if index != 0 {
			ctx.WritePlain(", ")
		}
		if err := table2table.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore RenameTableStmt.TableToTables")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *RenameTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RenameTableStmt)
	node, ok := n.OldTable.Accept(v)
	if !ok {
		return n, false
	}
	n.OldTable = node.(*TableName)
	node, ok = n.NewTable.Accept(v)
	if !ok {
		return n, false
	}
	n.NewTable = node.(*TableName)

	for i, t := range n.TableToTables {
		node, ok := t.Accept(v)
		if !ok {
			return n, false
		}
		n.TableToTables[i] = node.(*TableToTable)
	}

	return v.Leave(n)
}

// TableToTable represents renaming old table to new table used in RenameTableStmt.
type TableToTable struct {
	node
	OldTable *TableName
	NewTable *TableName
}

// Restore implements Node interface.
func (n *TableToTable) Restore(ctx *RestoreCtx) error {
	if err := n.OldTable.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore TableToTable.OldTable")
	}
	ctx.WriteKeyWord(" TO ")
	if err := n.NewTable.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore TableToTable.NewTable")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *TableToTable) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TableToTable)
	node, ok := n.OldTable.Accept(v)
	if !ok {
		return n, false
	}
	n.OldTable = node.(*TableName)
	node, ok = n.NewTable.Accept(v)
	if !ok {
		return n, false
	}
	n.NewTable = node.(*TableName)
	return v.Leave(n)
}

// CreateViewStmt is a statement to create a View.
// See https://dev.mysql.com/doc/refman/5.7/en/create-view.html
type CreateViewStmt struct {
	ddlNode

	OrReplace   bool
	ViewName    *TableName
	Cols        []model.CIStr
	Select      StmtNode
	Algorithm   model.ViewAlgorithm
	Definer     *auth.UserIdentity
	Security    model.ViewSecurity
	CheckOption model.ViewCheckOption
}

// Restore implements Node interface.
func (n *CreateViewStmt) Restore(ctx *RestoreCtx) error {
	return errors.New("Not implemented")
}

// Accept implements Node Accept interface.
func (n *CreateViewStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateViewStmt)
	node, ok := n.ViewName.Accept(v)
	if !ok {
		return n, false
	}
	n.ViewName = node.(*TableName)
	selnode, ok := n.Select.Accept(v)
	if !ok {
		return n, false
	}
	n.Select = selnode.(*SelectStmt)
	return v.Leave(n)
}

// CreateIndexStmt is a statement to create an index.
// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type CreateIndexStmt struct {
	ddlNode

	IndexName     string
	Table         *TableName
	Unique        bool
	IndexColNames []*IndexColName
	IndexOption   *IndexOption
}

// Restore implements Node interface.
func (n *CreateIndexStmt) Restore(ctx *RestoreCtx) error {
	return errors.New("Not implemented")
}

// Accept implements Node Accept interface.
func (n *CreateIndexStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateIndexStmt)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	for i, val := range n.IndexColNames {
		node, ok = val.Accept(v)
		if !ok {
			return n, false
		}
		n.IndexColNames[i] = node.(*IndexColName)
	}
	if n.IndexOption != nil {
		node, ok := n.IndexOption.Accept(v)
		if !ok {
			return n, false
		}
		n.IndexOption = node.(*IndexOption)
	}
	return v.Leave(n)
}

// DropIndexStmt is a statement to drop the index.
// See https://dev.mysql.com/doc/refman/5.7/en/drop-index.html
type DropIndexStmt struct {
	ddlNode

	IfExists  bool
	IndexName string
	Table     *TableName
}

// Restore implements Node interface.
func (n *DropIndexStmt) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord("DROP INDEX ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	ctx.WriteName(n.IndexName)
	ctx.WriteKeyWord(" ON ")

	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while add index")
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *DropIndexStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropIndexStmt)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	return v.Leave(n)
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
	TableOptionDelayKeyWrite
	TableOptionRowFormat
	TableOptionStatsPersistent
	TableOptionShardRowID
	TableOptionPackKeys
)

// RowFormat types
const (
	RowFormatDefault uint64 = iota + 1
	RowFormatDynamic
	RowFormatFixed
	RowFormatCompressed
	RowFormatRedundant
	RowFormatCompact
)

// OnDuplicateCreateTableSelectType is the option that handle unique key values in 'CREATE TABLE ... SELECT'.
// See https://dev.mysql.com/doc/refman/5.7/en/create-table-select.html
type OnDuplicateCreateTableSelectType int

// OnDuplicateCreateTableSelect types
const (
	OnDuplicateCreateTableSelectError OnDuplicateCreateTableSelectType = iota
	OnDuplicateCreateTableSelectIgnore
	OnDuplicateCreateTableSelectReplace
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
	// Tp is either ColumnPositionNone, ColumnPositionFirst or ColumnPositionAfter.
	Tp ColumnPositionType
	// RelativeColumn is the column the newly added column after if type is ColumnPositionAfter
	RelativeColumn *ColumnName
}

// Restore implements Node interface.
func (n *ColumnPosition) Restore(ctx *RestoreCtx) error {
	return errors.New("Not implemented")
}

// Accept implements Node Accept interface.
func (n *ColumnPosition) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ColumnPosition)
	if n.RelativeColumn != nil {
		node, ok := n.RelativeColumn.Accept(v)
		if !ok {
			return n, false
		}
		n.RelativeColumn = node.(*ColumnName)
	}
	return v.Leave(n)
}

// AlterTableType is the type for AlterTableSpec.
type AlterTableType int

// AlterTable types.
const (
	AlterTableOption AlterTableType = iota + 1
	AlterTableAddColumns
	AlterTableAddConstraint
	AlterTableDropColumn
	AlterTableDropPrimaryKey
	AlterTableDropIndex
	AlterTableDropForeignKey
	AlterTableModifyColumn
	AlterTableChangeColumn
	AlterTableRenameTable
	AlterTableAlterColumn
	AlterTableLock
	AlterTableAlgorithm
	AlterTableRenameIndex
	AlterTableForce
	AlterTableAddPartitions
	AlterTableCoalescePartitions
	AlterTableDropPartition
	AlterTableTruncatePartition

	// TODO: Add more actions
)

// LockType is the type for AlterTableSpec.
// See https://dev.mysql.com/doc/refman/5.7/en/alter-table.html#alter-table-concurrency
type LockType byte

// Lock Types.
const (
	LockTypeNone LockType = iota + 1
	LockTypeDefault
	LockTypeShared
	LockTypeExclusive
)

// AlterTableSpec represents alter table specification.
type AlterTableSpec struct {
	node

	Tp              AlterTableType
	Name            string
	Constraint      *Constraint
	Options         []*TableOption
	NewTable        *TableName
	NewColumns      []*ColumnDef
	OldColumnName   *ColumnName
	Position        *ColumnPosition
	LockType        LockType
	Comment         string
	FromKey         model.CIStr
	ToKey           model.CIStr
	PartDefinitions []*PartitionDefinition
	Num             uint64
}

// Restore implements Node interface.
func (n *AlterTableSpec) Restore(ctx *RestoreCtx) error {
	return errors.New("Not implemented")
}

// Accept implements Node Accept interface.
func (n *AlterTableSpec) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterTableSpec)
	if n.Constraint != nil {
		node, ok := n.Constraint.Accept(v)
		if !ok {
			return n, false
		}
		n.Constraint = node.(*Constraint)
	}
	if n.NewTable != nil {
		node, ok := n.NewTable.Accept(v)
		if !ok {
			return n, false
		}
		n.NewTable = node.(*TableName)
	}
	for _, col := range n.NewColumns {
		node, ok := col.Accept(v)
		if !ok {
			return n, false
		}
		col = node.(*ColumnDef)
	}
	if n.OldColumnName != nil {
		node, ok := n.OldColumnName.Accept(v)
		if !ok {
			return n, false
		}
		n.OldColumnName = node.(*ColumnName)
	}
	if n.Position != nil {
		node, ok := n.Position.Accept(v)
		if !ok {
			return n, false
		}
		n.Position = node.(*ColumnPosition)
	}
	return v.Leave(n)
}

// AlterTableStmt is a statement to change the structure of a table.
// See https://dev.mysql.com/doc/refman/5.7/en/alter-table.html
type AlterTableStmt struct {
	ddlNode

	Table *TableName
	Specs []*AlterTableSpec
}

// Restore implements Node interface.
func (n *AlterTableStmt) Restore(ctx *RestoreCtx) error {
	return errors.New("Not implemented")
}

// Accept implements Node Accept interface.
func (n *AlterTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterTableStmt)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	for i, val := range n.Specs {
		node, ok = val.Accept(v)
		if !ok {
			return n, false
		}
		n.Specs[i] = node.(*AlterTableSpec)
	}
	return v.Leave(n)
}

// TruncateTableStmt is a statement to empty a table completely.
// See https://dev.mysql.com/doc/refman/5.7/en/truncate-table.html
type TruncateTableStmt struct {
	ddlNode

	Table *TableName
}

// Restore implements Node interface.
func (n *TruncateTableStmt) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord("TRUNCATE TABLE ")
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore TruncateTableStmt.Table")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *TruncateTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TruncateTableStmt)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	return v.Leave(n)
}

// PartitionDefinition defines a single partition.
type PartitionDefinition struct {
	Name     model.CIStr
	LessThan []ExprNode
	MaxValue bool
	Comment  string
}

// PartitionOptions specifies the partition options.
type PartitionOptions struct {
	Tp          model.PartitionType
	Expr        ExprNode
	ColumnNames []*ColumnName
	Definitions []*PartitionDefinition
	Num         uint64
}
