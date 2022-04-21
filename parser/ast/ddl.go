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
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/parser/tidb"
	"github.com/pingcap/tidb/parser/types"
)

var (
	_ DDLNode = &AlterTableStmt{}
	_ DDLNode = &AlterSequenceStmt{}
	_ DDLNode = &AlterPlacementPolicyStmt{}
	_ DDLNode = &CreateDatabaseStmt{}
	_ DDLNode = &CreateIndexStmt{}
	_ DDLNode = &CreateTableStmt{}
	_ DDLNode = &CreateViewStmt{}
	_ DDLNode = &CreateSequenceStmt{}
	_ DDLNode = &CreatePlacementPolicyStmt{}
	_ DDLNode = &DropDatabaseStmt{}
	_ DDLNode = &DropIndexStmt{}
	_ DDLNode = &DropTableStmt{}
	_ DDLNode = &DropSequenceStmt{}
	_ DDLNode = &DropPlacementPolicyStmt{}
	_ DDLNode = &RenameTableStmt{}
	_ DDLNode = &TruncateTableStmt{}
	_ DDLNode = &RepairTableStmt{}

	_ Node = &AlterTableSpec{}
	_ Node = &ColumnDef{}
	_ Node = &ColumnOption{}
	_ Node = &ColumnPosition{}
	_ Node = &Constraint{}
	_ Node = &IndexPartSpecification{}
	_ Node = &ReferenceDef{}
)

// CharsetOpt is used for parsing charset option from SQL.
type CharsetOpt struct {
	Chs string
	Col string
}

// NullString represents a string that may be nil.
type NullString struct {
	String string
	Empty  bool // Empty is true if String is empty backtick.
}

// DatabaseOptionType is the type for database options.
type DatabaseOptionType int

// Database option types.
const (
	DatabaseOptionNone DatabaseOptionType = iota
	DatabaseOptionCharset
	DatabaseOptionCollate
	DatabaseOptionEncryption
	DatabaseSetTiFlashReplica
	DatabaseOptionPlacementPolicy = DatabaseOptionType(PlacementOptionPolicy)
)

// DatabaseOption represents database option.
type DatabaseOption struct {
	Tp             DatabaseOptionType
	Value          string
	UintValue      uint64
	TiFlashReplica *TiFlashReplicaSpec
}

// Restore implements Node interface.
func (n *DatabaseOption) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case DatabaseOptionCharset:
		ctx.WriteKeyWord("CHARACTER SET")
		ctx.WritePlain(" = ")
		ctx.WritePlain(n.Value)
	case DatabaseOptionCollate:
		ctx.WriteKeyWord("COLLATE")
		ctx.WritePlain(" = ")
		ctx.WritePlain(n.Value)
	case DatabaseOptionEncryption:
		ctx.WriteKeyWord("ENCRYPTION")
		ctx.WritePlain(" = ")
		ctx.WriteString(n.Value)
	case DatabaseOptionPlacementPolicy:
		placementOpt := PlacementOption{
			Tp:        PlacementOptionPolicy,
			UintValue: n.UintValue,
			StrValue:  n.Value,
		}
		return placementOpt.Restore(ctx)
	case DatabaseSetTiFlashReplica:
		ctx.WriteKeyWord("SET TIFLASH REPLICA ")
		ctx.WritePlainf("%d", n.TiFlashReplica.Count)
		if len(n.TiFlashReplica.Labels) == 0 {
			break
		}
		ctx.WriteKeyWord(" LOCATION LABELS ")
		for i, v := range n.TiFlashReplica.Labels {
			if i > 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteString(v)
		}
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
func (n *CreateDatabaseStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE DATABASE ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	ctx.WriteName(n.Name)
	for i, option := range n.Options {
		ctx.WritePlain(" ")
		err := option.Restore(ctx)
		if err != nil {
			return errors.Annotatef(err, "An error occurred while splicing CreateDatabaseStmt DatabaseOption: [%v]", i)
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

// AlterDatabaseStmt is a statement to change the structure of a database.
// See https://dev.mysql.com/doc/refman/5.7/en/alter-database.html
type AlterDatabaseStmt struct {
	ddlNode

	Name                 string
	AlterDefaultDatabase bool
	Options              []*DatabaseOption
}

// Restore implements Node interface.
func (n *AlterDatabaseStmt) Restore(ctx *format.RestoreCtx) error {
	if ctx.Flags.HasSkipPlacementRuleForRestoreFlag() && n.isAllPlacementOptions() {
		return nil
	}
	// If all options placement options and RestoreTiDBSpecialComment flag is on,
	// we should restore the whole node in special comment. For example, the restore result should be:
	// /*T![placement] ALTER DATABASE `db1` PLACEMENT POLICY = `p1` */
	// instead of
	// ALTER DATABASE `db1` /*T![placement] PLACEMENT POLICY = `p1` */
	// because altering a database without any options is not a legal syntax in mysql
	if n.isAllPlacementOptions() && ctx.Flags.HasTiDBSpecialCommentFlag() {
		return restorePlacementStmtInSpecialComment(ctx, n)
	}

	ctx.WriteKeyWord("ALTER DATABASE")
	if !n.AlterDefaultDatabase {
		ctx.WritePlain(" ")
		ctx.WriteName(n.Name)
	}
	for i, option := range n.Options {
		ctx.WritePlain(" ")
		err := option.Restore(ctx)
		if err != nil {
			return errors.Annotatef(err, "An error occurred while splicing AlterDatabaseStmt DatabaseOption: [%v]", i)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterDatabaseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterDatabaseStmt)
	return v.Leave(n)
}

func (n *AlterDatabaseStmt) isAllPlacementOptions() bool {
	for _, n := range n.Options {
		switch n.Tp {
		case DatabaseOptionPlacementPolicy:
		default:
			return false
		}
	}
	return true
}

// DropDatabaseStmt is a statement to drop a database and all tables in the database.
// See https://dev.mysql.com/doc/refman/5.7/en/drop-database.html
type DropDatabaseStmt struct {
	ddlNode

	IfExists bool
	Name     string
}

// Restore implements Node interface.
func (n *DropDatabaseStmt) Restore(ctx *format.RestoreCtx) error {
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

// IndexPartSpecifications is used for parsing index column name or index expression from SQL.
type IndexPartSpecification struct {
	node

	Column *ColumnName
	Length int
	Expr   ExprNode
}

// Restore implements Node interface.
func (n *IndexPartSpecification) Restore(ctx *format.RestoreCtx) error {
	if n.Expr != nil {
		ctx.WritePlain("(")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing IndexPartSpecifications")
		}
		ctx.WritePlain(")")
		return nil
	}
	if err := n.Column.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while splicing IndexPartSpecifications")
	}
	if n.Length > 0 {
		ctx.WritePlainf("(%d)", n.Length)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *IndexPartSpecification) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*IndexPartSpecification)
	if n.Expr != nil {
		node, ok := n.Expr.Accept(v)
		if !ok {
			return n, false
		}
		n.Expr = node.(ExprNode)
		return v.Leave(n)
	}
	node, ok := n.Column.Accept(v)
	if !ok {
		return n, false
	}
	n.Column = node.(*ColumnName)
	return v.Leave(n)
}

// MatchType is the type for reference match type.
type MatchType int

// match type
const (
	MatchNone MatchType = iota
	MatchFull
	MatchPartial
	MatchSimple
)

// ReferenceDef is used for parsing foreign key reference option from SQL.
// See http://dev.mysql.com/doc/refman/5.7/en/create-table-foreign-keys.html
type ReferenceDef struct {
	node

	Table                   *TableName
	IndexPartSpecifications []*IndexPartSpecification
	OnDelete                *OnDeleteOpt
	OnUpdate                *OnUpdateOpt
	Match                   MatchType
}

// Restore implements Node interface.
func (n *ReferenceDef) Restore(ctx *format.RestoreCtx) error {
	if n.Table != nil {
		ctx.WriteKeyWord("REFERENCES ")
		if err := n.Table.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing ReferenceDef")
		}
	}

	if n.IndexPartSpecifications != nil {
		ctx.WritePlain("(")
		for i, indexColNames := range n.IndexPartSpecifications {
			if i > 0 {
				ctx.WritePlain(", ")
			}
			if err := indexColNames.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while splicing IndexPartSpecifications: [%v]", i)
			}
		}
		ctx.WritePlain(")")
	}

	if n.Match != MatchNone {
		ctx.WriteKeyWord(" MATCH ")
		switch n.Match {
		case MatchFull:
			ctx.WriteKeyWord("FULL")
		case MatchPartial:
			ctx.WriteKeyWord("PARTIAL")
		case MatchSimple:
			ctx.WriteKeyWord("SIMPLE")
		}
	}
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
	if n.Table != nil {
		node, ok := n.Table.Accept(v)
		if !ok {
			return n, false
		}
		n.Table = node.(*TableName)
	}
	for i, val := range n.IndexPartSpecifications {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.IndexPartSpecifications[i] = node.(*IndexPartSpecification)
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
	ReferOptionSetDefault
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
	case ReferOptionSetDefault:
		return "SET DEFAULT"
	}
	return ""
}

// OnDeleteOpt is used for optional on delete clause.
type OnDeleteOpt struct {
	node
	ReferOpt ReferOptionType
}

// Restore implements Node interface.
func (n *OnDeleteOpt) Restore(ctx *format.RestoreCtx) error {
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
func (n *OnUpdateOpt) Restore(ctx *format.RestoreCtx) error {
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
	ColumnOptionCollate
	ColumnOptionCheck
	ColumnOptionColumnFormat
	ColumnOptionStorage
	ColumnOptionAutoRandom
)

var (
	invalidOptionForGeneratedColumn = map[ColumnOptionType]struct{}{
		ColumnOptionAutoIncrement: {},
		ColumnOptionOnUpdate:      {},
		ColumnOptionDefaultValue:  {},
	}
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
	Refer               *ReferenceDef
	StrValue            string
	AutoRandomBitLength int
	// Enforced is only for Check, default is true.
	Enforced bool
	// Name is only used for Check Constraint name.
	ConstraintName string
	PrimaryKeyTp   model.PrimaryKeyType
}

// Restore implements Node interface.
func (n *ColumnOption) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case ColumnOptionNoOption:
		return nil
	case ColumnOptionPrimaryKey:
		ctx.WriteKeyWord("PRIMARY KEY")
		pkTp := n.PrimaryKeyTp.String()
		if len(pkTp) != 0 {
			ctx.WritePlain(" ")
			_ = ctx.WriteWithSpecialComments(tidb.FeatureIDClusteredIndex, func() error {
				ctx.WriteKeyWord(pkTp)
				return nil
			})
		}
	case ColumnOptionNotNull:
		ctx.WriteKeyWord("NOT NULL")
	case ColumnOptionAutoIncrement:
		ctx.WriteKeyWord("AUTO_INCREMENT")
	case ColumnOptionDefaultValue:
		ctx.WriteKeyWord("DEFAULT ")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing ColumnOption DefaultValue Expr")
		}
	case ColumnOptionUniqKey:
		ctx.WriteKeyWord("UNIQUE KEY")
	case ColumnOptionNull:
		ctx.WriteKeyWord("NULL")
	case ColumnOptionOnUpdate:
		ctx.WriteKeyWord("ON UPDATE ")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing ColumnOption ON UPDATE Expr")
		}
	case ColumnOptionFulltext:
		return errors.New("TiDB Parser ignore the `ColumnOptionFulltext` type now")
	case ColumnOptionComment:
		ctx.WriteKeyWord("COMMENT ")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing ColumnOption COMMENT Expr")
		}
	case ColumnOptionGenerated:
		ctx.WriteKeyWord("GENERATED ALWAYS AS")
		ctx.WritePlain("(")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing ColumnOption GENERATED ALWAYS Expr")
		}
		ctx.WritePlain(")")
		if n.Stored {
			ctx.WriteKeyWord(" STORED")
		} else {
			ctx.WriteKeyWord(" VIRTUAL")
		}
	case ColumnOptionReference:
		if err := n.Refer.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing ColumnOption ReferenceDef")
		}
	case ColumnOptionCollate:
		if n.StrValue == "" {
			return errors.New("Empty ColumnOption COLLATE")
		}
		ctx.WriteKeyWord("COLLATE ")
		ctx.WritePlain(n.StrValue)
	case ColumnOptionCheck:
		if n.ConstraintName != "" {
			ctx.WriteKeyWord("CONSTRAINT ")
			ctx.WriteName(n.ConstraintName)
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("CHECK")
		ctx.WritePlain("(")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Trace(err)
		}
		ctx.WritePlain(")")
		if n.Enforced {
			ctx.WriteKeyWord(" ENFORCED")
		} else {
			ctx.WriteKeyWord(" NOT ENFORCED")
		}
	case ColumnOptionColumnFormat:
		ctx.WriteKeyWord("COLUMN_FORMAT ")
		ctx.WriteKeyWord(n.StrValue)
	case ColumnOptionStorage:
		ctx.WriteKeyWord("STORAGE ")
		ctx.WriteKeyWord(n.StrValue)
	case ColumnOptionAutoRandom:
		_ = ctx.WriteWithSpecialComments(tidb.FeatureIDAutoRandom, func() error {
			ctx.WriteKeyWord("AUTO_RANDOM")
			if n.AutoRandomBitLength != types.UnspecifiedLength {
				ctx.WritePlainf("(%d)", n.AutoRandomBitLength)
			}
			return nil
		})
	default:
		return errors.New("An error occurred while splicing ColumnOption")
	}
	return nil
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

// IndexVisibility is the option for index visibility.
type IndexVisibility int

// IndexVisibility options.
const (
	IndexVisibilityDefault IndexVisibility = iota
	IndexVisibilityVisible
	IndexVisibilityInvisible
)

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
	ParserName   model.CIStr
	Visibility   IndexVisibility
	PrimaryKeyTp model.PrimaryKeyType
}

// Restore implements Node interface.
func (n *IndexOption) Restore(ctx *format.RestoreCtx) error {
	hasPrevOption := false
	if n.PrimaryKeyTp != model.PrimaryKeyTypeDefault {
		_ = ctx.WriteWithSpecialComments(tidb.FeatureIDClusteredIndex, func() error {
			ctx.WriteKeyWord(n.PrimaryKeyTp.String())
			return nil
		})
		hasPrevOption = true
	}
	if n.KeyBlockSize > 0 {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
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

	if len(n.ParserName.O) > 0 {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("WITH PARSER ")
		ctx.WriteName(n.ParserName.O)
		hasPrevOption = true
	}

	if n.Comment != "" {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("COMMENT ")
		ctx.WriteString(n.Comment)
		hasPrevOption = true
	}

	if n.Visibility != IndexVisibilityDefault {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		switch n.Visibility {
		case IndexVisibilityVisible:
			ctx.WriteKeyWord("VISIBLE")
		case IndexVisibilityInvisible:
			ctx.WriteKeyWord("INVISIBLE")
		}
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
	ConstraintCheck
)

// Constraint is constraint for table definition.
type Constraint struct {
	node

	// only supported by MariaDB 10.0.2+ (ADD {INDEX|KEY}, ADD FOREIGN KEY),
	// see https://mariadb.com/kb/en/library/alter-table/
	IfNotExists bool

	Tp   ConstraintType
	Name string

	Keys []*IndexPartSpecification // Used for PRIMARY KEY, UNIQUE, ......

	Refer *ReferenceDef // Used for foreign key.

	Option *IndexOption // Index Options

	Expr ExprNode // Used for Check

	Enforced bool // Used for Check

	InColumn bool // Used for Check

	InColumnName string // Used for Check
	IsEmptyIndex bool   // Used for Check
}

// Restore implements Node interface.
func (n *Constraint) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case ConstraintNoConstraint:
		return nil
	case ConstraintPrimaryKey:
		ctx.WriteKeyWord("PRIMARY KEY")
	case ConstraintKey:
		ctx.WriteKeyWord("KEY")
		if n.IfNotExists {
			ctx.WriteKeyWord(" IF NOT EXISTS")
		}
	case ConstraintIndex:
		ctx.WriteKeyWord("INDEX")
		if n.IfNotExists {
			ctx.WriteKeyWord(" IF NOT EXISTS")
		}
	case ConstraintUniq:
		ctx.WriteKeyWord("UNIQUE")
	case ConstraintUniqKey:
		ctx.WriteKeyWord("UNIQUE KEY")
	case ConstraintUniqIndex:
		ctx.WriteKeyWord("UNIQUE INDEX")
	case ConstraintFulltext:
		ctx.WriteKeyWord("FULLTEXT")
	case ConstraintCheck:
		if n.Name != "" {
			ctx.WriteKeyWord("CONSTRAINT ")
			ctx.WriteName(n.Name)
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("CHECK")
		ctx.WritePlain("(")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Trace(err)
		}
		ctx.WritePlain(") ")
		if n.Enforced {
			ctx.WriteKeyWord("ENFORCED")
		} else {
			ctx.WriteKeyWord("NOT ENFORCED")
		}
		return nil
	}

	if n.Tp == ConstraintForeignKey {
		ctx.WriteKeyWord("CONSTRAINT ")
		if n.Name != "" {
			ctx.WriteName(n.Name)
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("FOREIGN KEY ")
		if n.IfNotExists {
			ctx.WriteKeyWord("IF NOT EXISTS ")
		}
	} else if n.Name != "" || n.IsEmptyIndex {
		ctx.WritePlain(" ")
		ctx.WriteName(n.Name)
	}

	ctx.WritePlain("(")
	for i, keys := range n.Keys {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		if err := keys.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing Constraint Keys: [%v]", i)
		}
	}
	ctx.WritePlain(")")

	if n.Refer != nil {
		ctx.WritePlain(" ")
		if err := n.Refer.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing Constraint Refer")
		}
	}

	if n.Option != nil {
		ctx.WritePlain(" ")
		if err := n.Option.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing Constraint Option")
		}
	}

	return nil
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
		n.Keys[i] = node.(*IndexPartSpecification)
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
	if n.Expr != nil {
		node, ok := n.Expr.Accept(v)
		if !ok {
			return n, false
		}
		n.Expr = node.(ExprNode)
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
func (n *ColumnDef) Restore(ctx *format.RestoreCtx) error {
	if err := n.Name.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while splicing ColumnDef Name")
	}
	if n.Tp != nil {
		ctx.WritePlain(" ")
		if err := n.Tp.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing ColumnDef Type")
		}
	}
	for i, options := range n.Options {
		ctx.WritePlain(" ")
		if err := options.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing ColumnDef ColumnOption: [%v]", i)
		}
	}
	return nil
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

// Validate checks if a column definition is legal.
// For example, generated column definitions that contain such
// column options as `ON UPDATE`, `AUTO_INCREMENT`, `DEFAULT`
// are illegal.
func (n *ColumnDef) Validate() bool {
	generatedCol := false
	illegalOpt4gc := false
	for _, opt := range n.Options {
		if opt.Tp == ColumnOptionGenerated {
			generatedCol = true
		}
		_, found := invalidOptionForGeneratedColumn[opt.Tp]
		illegalOpt4gc = illegalOpt4gc || found
	}
	return !(generatedCol && illegalOpt4gc)
}

type TemporaryKeyword int

const (
	TemporaryNone TemporaryKeyword = iota
	TemporaryGlobal
	TemporaryLocal
)

// CreateTableStmt is a statement to create a table.
// See https://dev.mysql.com/doc/refman/5.7/en/create-table.html
type CreateTableStmt struct {
	ddlNode

	IfNotExists bool
	TemporaryKeyword
	// Meanless when TemporaryKeyword is not TemporaryGlobal.
	// ON COMMIT DELETE ROWS => true
	// ON COMMIT PRESERVE ROW => false
	OnCommitDelete bool
	Table          *TableName
	ReferTable     *TableName
	Cols           []*ColumnDef
	Constraints    []*Constraint
	Options        []*TableOption
	Partition      *PartitionOptions
	OnDuplicate    OnDuplicateKeyHandlingType
	Select         ResultSetNode
}

// Restore implements Node interface.
func (n *CreateTableStmt) Restore(ctx *format.RestoreCtx) error {
	switch n.TemporaryKeyword {
	case TemporaryNone:
		ctx.WriteKeyWord("CREATE TABLE ")
	case TemporaryGlobal:
		ctx.WriteKeyWord("CREATE GLOBAL TEMPORARY TABLE ")
	case TemporaryLocal:
		ctx.WriteKeyWord("CREATE TEMPORARY TABLE ")
	}
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}

	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while splicing CreateTableStmt Table")
	}

	if n.ReferTable != nil {
		ctx.WriteKeyWord(" LIKE ")
		if err := n.ReferTable.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing CreateTableStmt ReferTable")
		}
	}
	lenCols := len(n.Cols)
	lenConstraints := len(n.Constraints)
	if lenCols+lenConstraints > 0 {
		ctx.WritePlain(" (")
		for i, col := range n.Cols {
			if i > 0 {
				ctx.WritePlain(",")
			}
			if err := col.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while splicing CreateTableStmt ColumnDef: [%v]", i)
			}
		}
		for i, constraint := range n.Constraints {
			if i > 0 || lenCols >= 1 {
				ctx.WritePlain(",")
			}
			if err := constraint.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while splicing CreateTableStmt Constraints: [%v]", i)
			}
		}
		ctx.WritePlain(")")
	}

	for i, option := range n.Options {
		ctx.WritePlain(" ")
		if err := option.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing CreateTableStmt TableOption: [%v]", i)
		}
	}

	if n.Partition != nil {
		ctx.WritePlain(" ")
		if err := n.Partition.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing CreateTableStmt Partition")
		}
	}

	if n.Select != nil {
		switch n.OnDuplicate {
		case OnDuplicateKeyHandlingError:
			ctx.WriteKeyWord(" AS ")
		case OnDuplicateKeyHandlingIgnore:
			ctx.WriteKeyWord(" IGNORE AS ")
		case OnDuplicateKeyHandlingReplace:
			ctx.WriteKeyWord(" REPLACE AS ")
		}

		if err := n.Select.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing CreateTableStmt Select")
		}
	}

	if n.TemporaryKeyword == TemporaryGlobal {
		if n.OnCommitDelete {
			ctx.WriteKeyWord(" ON COMMIT DELETE ROWS")
		} else {
			ctx.WriteKeyWord(" ON COMMIT PRESERVE ROWS")
		}
	}

	return nil
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
	if n.Partition != nil {
		node, ok := n.Partition.Accept(v)
		if !ok {
			return n, false
		}
		n.Partition = node.(*PartitionOptions)
	}

	return v.Leave(n)
}

// DropTableStmt is a statement to drop one or more tables.
// See https://dev.mysql.com/doc/refman/5.7/en/drop-table.html
type DropTableStmt struct {
	ddlNode

	IfExists         bool
	Tables           []*TableName
	IsView           bool
	TemporaryKeyword // make sense ONLY if/when IsView == false
}

// Restore implements Node interface.
func (n *DropTableStmt) Restore(ctx *format.RestoreCtx) error {
	if n.IsView {
		ctx.WriteKeyWord("DROP VIEW ")
	} else {
		switch n.TemporaryKeyword {
		case TemporaryNone:
			ctx.WriteKeyWord("DROP TABLE ")
		case TemporaryGlobal:
			ctx.WriteKeyWord("DROP GLOBAL TEMPORARY TABLE ")
		case TemporaryLocal:
			ctx.WriteKeyWord("DROP TEMPORARY TABLE ")
		}
	}
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}

	for index, table := range n.Tables {
		if index != 0 {
			ctx.WritePlain(", ")
		}
		if err := table.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore DropTableStmt.Tables[%d]", index)
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

// DropPlacementPolicyStmt is a statement to drop a Policy.
type DropPlacementPolicyStmt struct {
	ddlNode

	IfExists   bool
	PolicyName model.CIStr
}

// Restore implements Restore interface.
func (n *DropPlacementPolicyStmt) Restore(ctx *format.RestoreCtx) error {
	if ctx.Flags.HasTiDBSpecialCommentFlag() {
		return restorePlacementStmtInSpecialComment(ctx, n)
	}

	ctx.WriteKeyWord("DROP PLACEMENT POLICY ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	ctx.WriteName(n.PolicyName.O)
	return nil
}

func (n *DropPlacementPolicyStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropPlacementPolicyStmt)
	return v.Leave(n)
}

// DropSequenceStmt is a statement to drop a Sequence.
type DropSequenceStmt struct {
	ddlNode

	IfExists  bool
	Sequences []*TableName
}

// Restore implements Node interface.
func (n *DropSequenceStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP SEQUENCE ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	for i, sequence := range n.Sequences {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := sequence.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore DropSequenceStmt.Sequences[%d]", i)
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *DropSequenceStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropSequenceStmt)
	for i, val := range n.Sequences {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Sequences[i] = node.(*TableName)
	}
	return v.Leave(n)
}

// RenameTableStmt is a statement to rename a table.
// See http://dev.mysql.com/doc/refman/5.7/en/rename-table.html
type RenameTableStmt struct {
	ddlNode

	TableToTables []*TableToTable
}

// Restore implements Node interface.
func (n *RenameTableStmt) Restore(ctx *format.RestoreCtx) error {
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
func (n *TableToTable) Restore(ctx *format.RestoreCtx) error {
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
	SchemaCols  []model.CIStr
	Algorithm   model.ViewAlgorithm
	Definer     *auth.UserIdentity
	Security    model.ViewSecurity
	CheckOption model.ViewCheckOption
}

// Restore implements Node interface.
func (n *CreateViewStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	if n.OrReplace {
		ctx.WriteKeyWord("OR REPLACE ")
	}
	ctx.WriteKeyWord("ALGORITHM")
	ctx.WritePlain(" = ")
	ctx.WriteKeyWord(n.Algorithm.String())
	ctx.WriteKeyWord(" DEFINER")
	ctx.WritePlain(" = ")

	// todo Use n.Definer.Restore(ctx) to replace this part
	if n.Definer.CurrentUser {
		ctx.WriteKeyWord("current_user")
	} else {
		ctx.WriteName(n.Definer.Username)
		if n.Definer.Hostname != "" {
			ctx.WritePlain("@")
			ctx.WriteName(n.Definer.Hostname)
		}
	}

	ctx.WriteKeyWord(" SQL SECURITY ")
	ctx.WriteKeyWord(n.Security.String())
	ctx.WriteKeyWord(" VIEW ")

	if err := n.ViewName.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while create CreateViewStmt.ViewName")
	}

	for i, col := range n.Cols {
		if i == 0 {
			ctx.WritePlain(" (")
		} else {
			ctx.WritePlain(",")
		}
		ctx.WriteName(col.O)
		if i == len(n.Cols)-1 {
			ctx.WritePlain(")")
		}
	}

	ctx.WriteKeyWord(" AS ")

	if err := n.Select.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while create CreateViewStmt.Select")
	}

	if n.CheckOption != model.CheckOptionCascaded {
		ctx.WriteKeyWord(" WITH ")
		ctx.WriteKeyWord(n.CheckOption.String())
		ctx.WriteKeyWord(" CHECK OPTION")
	}
	return nil
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
	n.Select = selnode.(StmtNode)
	return v.Leave(n)
}

// CreatePlacementPolicyStmt is a statement to create a policy.
type CreatePlacementPolicyStmt struct {
	ddlNode

	OrReplace        bool
	IfNotExists      bool
	PolicyName       model.CIStr
	PlacementOptions []*PlacementOption
}

// Restore implements Node interface.
func (n *CreatePlacementPolicyStmt) Restore(ctx *format.RestoreCtx) error {
	if ctx.Flags.HasTiDBSpecialCommentFlag() {
		return restorePlacementStmtInSpecialComment(ctx, n)
	}

	ctx.WriteKeyWord("CREATE ")
	if n.OrReplace {
		ctx.WriteKeyWord("OR REPLACE ")
	}
	ctx.WriteKeyWord("PLACEMENT POLICY ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	ctx.WriteName(n.PolicyName.O)
	for i, option := range n.PlacementOptions {
		ctx.WritePlain(" ")
		if err := option.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing CreatePlacementPolicy TableOption: [%v]", i)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CreatePlacementPolicyStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreatePlacementPolicyStmt)
	return v.Leave(n)
}

// CreateSequenceStmt is a statement to create a Sequence.
type CreateSequenceStmt struct {
	ddlNode

	// TODO : support or replace if need : care for it will conflict on temporaryOpt.
	IfNotExists bool
	Name        *TableName
	SeqOptions  []*SequenceOption
	TblOptions  []*TableOption
}

// Restore implements Node interface.
func (n *CreateSequenceStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	ctx.WriteKeyWord("SEQUENCE ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	if err := n.Name.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while create CreateSequenceStmt.Name")
	}
	for i, option := range n.SeqOptions {
		ctx.WritePlain(" ")
		if err := option.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing CreateSequenceStmt SequenceOption: [%v]", i)
		}
	}
	for i, option := range n.TblOptions {
		ctx.WritePlain(" ")
		if err := option.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing CreateSequenceStmt TableOption: [%v]", i)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateSequenceStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateSequenceStmt)
	node, ok := n.Name.Accept(v)
	if !ok {
		return n, false
	}
	n.Name = node.(*TableName)
	return v.Leave(n)
}

// IndexLockAndAlgorithm stores the algorithm option and the lock option.
type IndexLockAndAlgorithm struct {
	node

	LockTp      LockType
	AlgorithmTp AlgorithmType
}

// Restore implements Node interface.
func (n *IndexLockAndAlgorithm) Restore(ctx *format.RestoreCtx) error {
	hasPrevOption := false
	if n.AlgorithmTp != AlgorithmTypeDefault {
		ctx.WriteKeyWord("ALGORITHM")
		ctx.WritePlain(" = ")
		ctx.WriteKeyWord(n.AlgorithmTp.String())
		hasPrevOption = true
	}

	if n.LockTp != LockTypeDefault {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("LOCK")
		ctx.WritePlain(" = ")
		ctx.WriteKeyWord(n.LockTp.String())
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *IndexLockAndAlgorithm) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*IndexLockAndAlgorithm)
	return v.Leave(n)
}

// IndexKeyType is the type for index key.
type IndexKeyType int

// Index key types.
const (
	IndexKeyTypeNone IndexKeyType = iota
	IndexKeyTypeUnique
	IndexKeyTypeSpatial
	IndexKeyTypeFullText
)

// CreateIndexStmt is a statement to create an index.
// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type CreateIndexStmt struct {
	ddlNode

	// only supported by MariaDB 10.0.2+,
	// see https://mariadb.com/kb/en/library/create-index/
	IfNotExists bool

	IndexName               string
	Table                   *TableName
	IndexPartSpecifications []*IndexPartSpecification
	IndexOption             *IndexOption
	KeyType                 IndexKeyType
	LockAlg                 *IndexLockAndAlgorithm
}

// Restore implements Node interface.
func (n *CreateIndexStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	switch n.KeyType {
	case IndexKeyTypeUnique:
		ctx.WriteKeyWord("UNIQUE ")
	case IndexKeyTypeSpatial:
		ctx.WriteKeyWord("SPATIAL ")
	case IndexKeyTypeFullText:
		ctx.WriteKeyWord("FULLTEXT ")
	}
	ctx.WriteKeyWord("INDEX ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	ctx.WriteName(n.IndexName)
	ctx.WriteKeyWord(" ON ")
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateIndexStmt.Table")
	}

	ctx.WritePlain(" (")
	for i, indexColName := range n.IndexPartSpecifications {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := indexColName.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreateIndexStmt.IndexPartSpecifications: [%v]", i)
		}
	}
	ctx.WritePlain(")")

	if n.IndexOption.Tp != model.IndexTypeInvalid || n.IndexOption.KeyBlockSize > 0 || n.IndexOption.Comment != "" || len(n.IndexOption.ParserName.O) > 0 || n.IndexOption.Visibility != IndexVisibilityDefault {
		ctx.WritePlain(" ")
		if err := n.IndexOption.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore CreateIndexStmt.IndexOption")
		}
	}

	if n.LockAlg != nil {
		ctx.WritePlain(" ")
		if err := n.LockAlg.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore CreateIndexStmt.LockAlg")
		}
	}

	return nil
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
	for i, val := range n.IndexPartSpecifications {
		node, ok = val.Accept(v)
		if !ok {
			return n, false
		}
		n.IndexPartSpecifications[i] = node.(*IndexPartSpecification)
	}
	if n.IndexOption != nil {
		node, ok := n.IndexOption.Accept(v)
		if !ok {
			return n, false
		}
		n.IndexOption = node.(*IndexOption)
	}
	if n.LockAlg != nil {
		node, ok := n.LockAlg.Accept(v)
		if !ok {
			return n, false
		}
		n.LockAlg = node.(*IndexLockAndAlgorithm)
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
	LockAlg   *IndexLockAndAlgorithm
}

// Restore implements Node interface.
func (n *DropIndexStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP INDEX ")
	if n.IfExists {
		_ = ctx.WriteWithSpecialComments("", func() error {
			ctx.WriteKeyWord("IF EXISTS ")
			return nil
		})
	}
	ctx.WriteName(n.IndexName)
	ctx.WriteKeyWord(" ON ")

	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while add index")
	}

	if n.LockAlg != nil {
		ctx.WritePlain(" ")
		if err := n.LockAlg.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore CreateIndexStmt.LockAlg")
		}
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
	if n.LockAlg != nil {
		node, ok := n.LockAlg.Accept(v)
		if !ok {
			return n, false
		}
		n.LockAlg = node.(*IndexLockAndAlgorithm)
	}
	return v.Leave(n)
}

// LockTablesStmt is a statement to lock tables.
type LockTablesStmt struct {
	ddlNode

	TableLocks []TableLock
}

// TableLock contains the table name and lock type.
type TableLock struct {
	Table *TableName
	Type  model.TableLockType
}

// Accept implements Node Accept interface.
func (n *LockTablesStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*LockTablesStmt)
	for i := range n.TableLocks {
		node, ok := n.TableLocks[i].Table.Accept(v)
		if !ok {
			return n, false
		}
		n.TableLocks[i].Table = node.(*TableName)
	}
	return v.Leave(n)
}

// Restore implements Node interface.
func (n *LockTablesStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("LOCK TABLES ")
	for i, tl := range n.TableLocks {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := tl.Table.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while add index")
		}
		ctx.WriteKeyWord(" " + tl.Type.String())
	}
	return nil
}

// UnlockTablesStmt is a statement to unlock tables.
type UnlockTablesStmt struct {
	ddlNode
}

// Accept implements Node Accept interface.
func (n *UnlockTablesStmt) Accept(v Visitor) (Node, bool) {
	_, _ = v.Enter(n)
	return v.Leave(n)
}

// Restore implements Node interface.
func (n *UnlockTablesStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("UNLOCK TABLES")
	return nil
}

// CleanupTableLockStmt is a statement to cleanup table lock.
type CleanupTableLockStmt struct {
	ddlNode

	Tables []*TableName
}

// Accept implements Node Accept interface.
func (n *CleanupTableLockStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CleanupTableLockStmt)
	for i := range n.Tables {
		node, ok := n.Tables[i].Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}
	return v.Leave(n)
}

// Restore implements Node interface.
func (n *CleanupTableLockStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ADMIN CLEANUP TABLE LOCK ")
	for i, v := range n.Tables {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CleanupTableLockStmt.Tables[%d]", i)
		}
	}
	return nil
}

// RepairTableStmt is a statement to repair tableInfo.
type RepairTableStmt struct {
	ddlNode
	Table      *TableName
	CreateStmt *CreateTableStmt
}

// Accept implements Node Accept interface.
func (n *RepairTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RepairTableStmt)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	node, ok = n.CreateStmt.Accept(v)
	if !ok {
		return n, false
	}
	n.CreateStmt = node.(*CreateTableStmt)
	return v.Leave(n)
}

// Restore implements Node interface.
func (n *RepairTableStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ADMIN REPAIR TABLE ")
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotatef(err, "An error occurred while restore RepairTableStmt.table : [%v]", n.Table)
	}
	ctx.WritePlain(" ")
	if err := n.CreateStmt.Restore(ctx); err != nil {
		return errors.Annotatef(err, "An error occurred while restore RepairTableStmt.createStmt : [%v]", n.CreateStmt)
	}
	return nil
}

// PlacementOptionType is the type for PlacementOption
type PlacementOptionType int

// PlacementOption types.
const (
	PlacementOptionPrimaryRegion PlacementOptionType = 0x3000 + iota
	PlacementOptionRegions
	PlacementOptionFollowerCount
	PlacementOptionVoterCount
	PlacementOptionLearnerCount
	PlacementOptionSchedule
	PlacementOptionConstraints
	PlacementOptionLeaderConstraints
	PlacementOptionLearnerConstraints
	PlacementOptionFollowerConstraints
	PlacementOptionVoterConstraints
	PlacementOptionPolicy
)

// PlacementOption is used for parsing placement option.
type PlacementOption struct {
	Tp        PlacementOptionType
	StrValue  string
	UintValue uint64
}

func (n *PlacementOption) Restore(ctx *format.RestoreCtx) error {
	if ctx.Flags.HasSkipPlacementRuleForRestoreFlag() {
		return nil
	}
	fn := func() error {
		switch n.Tp {
		case PlacementOptionPrimaryRegion:
			ctx.WriteKeyWord("PRIMARY_REGION ")
			ctx.WritePlain("= ")
			ctx.WriteString(n.StrValue)
		case PlacementOptionRegions:
			ctx.WriteKeyWord("REGIONS ")
			ctx.WritePlain("= ")
			ctx.WriteString(n.StrValue)
		case PlacementOptionFollowerCount:
			ctx.WriteKeyWord("FOLLOWERS ")
			ctx.WritePlain("= ")
			ctx.WritePlainf("%d", n.UintValue)
		case PlacementOptionVoterCount:
			ctx.WriteKeyWord("VOTERS ")
			ctx.WritePlain("= ")
			ctx.WritePlainf("%d", n.UintValue)
		case PlacementOptionLearnerCount:
			ctx.WriteKeyWord("LEARNERS ")
			ctx.WritePlain("= ")
			ctx.WritePlainf("%d", n.UintValue)
		case PlacementOptionSchedule:
			ctx.WriteKeyWord("SCHEDULE ")
			ctx.WritePlain("= ")
			ctx.WriteString(n.StrValue)
		case PlacementOptionConstraints:
			ctx.WriteKeyWord("CONSTRAINTS ")
			ctx.WritePlain("= ")
			ctx.WriteString(n.StrValue)
		case PlacementOptionLeaderConstraints:
			ctx.WriteKeyWord("LEADER_CONSTRAINTS ")
			ctx.WritePlain("= ")
			ctx.WriteString(n.StrValue)
		case PlacementOptionFollowerConstraints:
			ctx.WriteKeyWord("FOLLOWER_CONSTRAINTS ")
			ctx.WritePlain("= ")
			ctx.WriteString(n.StrValue)
		case PlacementOptionVoterConstraints:
			ctx.WriteKeyWord("VOTER_CONSTRAINTS ")
			ctx.WritePlain("= ")
			ctx.WriteString(n.StrValue)
		case PlacementOptionLearnerConstraints:
			ctx.WriteKeyWord("LEARNER_CONSTRAINTS ")
			ctx.WritePlain("= ")
			ctx.WriteString(n.StrValue)
		case PlacementOptionPolicy:
			ctx.WriteKeyWord("PLACEMENT POLICY ")
			ctx.WritePlain("= ")
			ctx.WriteName(n.StrValue)
		default:
			return errors.Errorf("invalid PlacementOption: %d", n.Tp)
		}
		return nil
	}
	// WriteSpecialComment
	return ctx.WriteWithSpecialComments(tidb.FeatureIDPlacement, fn)
}

type StatsOptionType int

const (
	StatsOptionBuckets StatsOptionType = 0x5000 + iota
	StatsOptionTopN
	StatsOptionColsChoice
	StatsOptionColList
	StatsOptionSampleRate
)

// TableOptionType is the type for TableOption
type TableOptionType int

// TableOption types.
const (
	TableOptionNone TableOptionType = iota
	TableOptionEngine
	TableOptionCharset
	TableOptionCollate
	TableOptionAutoIdCache
	TableOptionAutoIncrement
	TableOptionAutoRandomBase
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
	TableOptionStatsAutoRecalc
	TableOptionShardRowID
	TableOptionPreSplitRegion
	TableOptionPackKeys
	TableOptionTablespace
	TableOptionNodegroup
	TableOptionDataDirectory
	TableOptionIndexDirectory
	TableOptionStorageMedia
	TableOptionStatsSamplePages
	TableOptionSecondaryEngine
	TableOptionSecondaryEngineNull
	TableOptionInsertMethod
	TableOptionTableCheckSum
	TableOptionUnion
	TableOptionEncryption
	TableOptionPlacementPolicy = TableOptionType(PlacementOptionPolicy)
	TableOptionStatsBuckets    = TableOptionType(StatsOptionBuckets)
	TableOptionStatsTopN       = TableOptionType(StatsOptionTopN)
	TableOptionStatsColsChoice = TableOptionType(StatsOptionColsChoice)
	TableOptionStatsColList    = TableOptionType(StatsOptionColList)
	TableOptionStatsSampleRate = TableOptionType(StatsOptionSampleRate)
)

// RowFormat types
const (
	RowFormatDefault uint64 = iota + 1
	RowFormatDynamic
	RowFormatFixed
	RowFormatCompressed
	RowFormatRedundant
	RowFormatCompact
	TokuDBRowFormatDefault
	TokuDBRowFormatFast
	TokuDBRowFormatSmall
	TokuDBRowFormatZlib
	TokuDBRowFormatQuickLZ
	TokuDBRowFormatLzma
	TokuDBRowFormatSnappy
	TokuDBRowFormatUncompressed
)

// OnDuplicateKeyHandlingType is the option that handle unique key values in 'CREATE TABLE ... SELECT' or `LOAD DATA`.
// See https://dev.mysql.com/doc/refman/5.7/en/create-table-select.html
// See https://dev.mysql.com/doc/refman/5.7/en/load-data.html
type OnDuplicateKeyHandlingType int

// OnDuplicateKeyHandling types
const (
	OnDuplicateKeyHandlingError OnDuplicateKeyHandlingType = iota
	OnDuplicateKeyHandlingIgnore
	OnDuplicateKeyHandlingReplace
)

const (
	TableOptionCharsetWithoutConvertTo uint64 = 0
	TableOptionCharsetWithConvertTo    uint64 = 1
)

// TableOption is used for parsing table option from SQL.
type TableOption struct {
	Tp         TableOptionType
	Default    bool
	StrValue   string
	UintValue  uint64
	BoolValue  bool
	Value      ValueExpr
	TableNames []*TableName
}

func (n *TableOption) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case TableOptionEngine:
		ctx.WriteKeyWord("ENGINE ")
		ctx.WritePlain("= ")
		if n.StrValue != "" {
			ctx.WritePlain(n.StrValue)
		} else {
			ctx.WritePlain("''")
		}
	case TableOptionCharset:
		if n.UintValue == TableOptionCharsetWithConvertTo {
			ctx.WriteKeyWord("CONVERT TO ")
		} else {
			ctx.WriteKeyWord("DEFAULT ")
		}
		ctx.WriteKeyWord("CHARACTER SET ")
		if n.UintValue == TableOptionCharsetWithoutConvertTo {
			ctx.WriteKeyWord("= ")
		}
		if n.Default {
			ctx.WriteKeyWord("DEFAULT")
		} else {
			ctx.WriteKeyWord(n.StrValue)
		}
	case TableOptionCollate:
		ctx.WriteKeyWord("DEFAULT COLLATE ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord(n.StrValue)
	case TableOptionAutoIncrement:
		if n.BoolValue {
			_ = ctx.WriteWithSpecialComments(tidb.FeatureIDForceAutoInc, func() error {
				ctx.WriteKeyWord("FORCE")
				return nil
			})
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("AUTO_INCREMENT ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionAutoIdCache:
		_ = ctx.WriteWithSpecialComments(tidb.FeatureIDAutoIDCache, func() error {
			ctx.WriteKeyWord("AUTO_ID_CACHE ")
			ctx.WritePlain("= ")
			ctx.WritePlainf("%d", n.UintValue)
			return nil
		})
	case TableOptionAutoRandomBase:
		if n.BoolValue {
			_ = ctx.WriteWithSpecialComments(tidb.FeatureIDForceAutoInc, func() error {
				ctx.WriteKeyWord("FORCE")
				return nil
			})
			ctx.WritePlain(" ")
		}
		_ = ctx.WriteWithSpecialComments(tidb.FeatureIDAutoRandomBase, func() error {
			ctx.WriteKeyWord("AUTO_RANDOM_BASE ")
			ctx.WritePlain("= ")
			ctx.WritePlainf("%d", n.UintValue)
			return nil
		})
	case TableOptionComment:
		ctx.WriteKeyWord("COMMENT ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionAvgRowLength:
		ctx.WriteKeyWord("AVG_ROW_LENGTH ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionCheckSum:
		ctx.WriteKeyWord("CHECKSUM ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionCompression:
		ctx.WriteKeyWord("COMPRESSION ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionConnection:
		ctx.WriteKeyWord("CONNECTION ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionPassword:
		ctx.WriteKeyWord("PASSWORD ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionKeyBlockSize:
		ctx.WriteKeyWord("KEY_BLOCK_SIZE ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionMaxRows:
		ctx.WriteKeyWord("MAX_ROWS ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionMinRows:
		ctx.WriteKeyWord("MIN_ROWS ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionDelayKeyWrite:
		ctx.WriteKeyWord("DELAY_KEY_WRITE ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionRowFormat:
		ctx.WriteKeyWord("ROW_FORMAT ")
		ctx.WritePlain("= ")
		switch n.UintValue {
		case RowFormatDefault:
			ctx.WriteKeyWord("DEFAULT")
		case RowFormatDynamic:
			ctx.WriteKeyWord("DYNAMIC")
		case RowFormatFixed:
			ctx.WriteKeyWord("FIXED")
		case RowFormatCompressed:
			ctx.WriteKeyWord("COMPRESSED")
		case RowFormatRedundant:
			ctx.WriteKeyWord("REDUNDANT")
		case RowFormatCompact:
			ctx.WriteKeyWord("COMPACT")
		case TokuDBRowFormatDefault:
			ctx.WriteKeyWord("TOKUDB_DEFAULT")
		case TokuDBRowFormatFast:
			ctx.WriteKeyWord("TOKUDB_FAST")
		case TokuDBRowFormatSmall:
			ctx.WriteKeyWord("TOKUDB_SMALL")
		case TokuDBRowFormatZlib:
			ctx.WriteKeyWord("TOKUDB_ZLIB")
		case TokuDBRowFormatQuickLZ:
			ctx.WriteKeyWord("TOKUDB_QUICKLZ")
		case TokuDBRowFormatLzma:
			ctx.WriteKeyWord("TOKUDB_LZMA")
		case TokuDBRowFormatSnappy:
			ctx.WriteKeyWord("TOKUDB_SNAPPY")
		case TokuDBRowFormatUncompressed:
			ctx.WriteKeyWord("TOKUDB_UNCOMPRESSED")
		default:
			return errors.Errorf("invalid TableOption: TableOptionRowFormat: %d", n.UintValue)
		}
	case TableOptionStatsPersistent:
		// TODO: not support
		ctx.WriteKeyWord("STATS_PERSISTENT ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord("DEFAULT")
		ctx.WritePlain(" /* TableOptionStatsPersistent is not supported */ ")
	case TableOptionStatsAutoRecalc:
		ctx.WriteKeyWord("STATS_AUTO_RECALC ")
		ctx.WritePlain("= ")
		if n.Default {
			ctx.WriteKeyWord("DEFAULT")
		} else {
			ctx.WritePlainf("%d", n.UintValue)
		}
	case TableOptionShardRowID:
		_ = ctx.WriteWithSpecialComments(tidb.FeatureIDTiDB, func() error {
			ctx.WriteKeyWord("SHARD_ROW_ID_BITS ")
			ctx.WritePlainf("= %d", n.UintValue)
			return nil
		})
	case TableOptionPreSplitRegion:
		_ = ctx.WriteWithSpecialComments(tidb.FeatureIDTiDB, func() error {
			ctx.WriteKeyWord("PRE_SPLIT_REGIONS ")
			ctx.WritePlainf("= %d", n.UintValue)
			return nil
		})
	case TableOptionPackKeys:
		// TODO: not support
		ctx.WriteKeyWord("PACK_KEYS ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord("DEFAULT")
		ctx.WritePlain(" /* TableOptionPackKeys is not supported */ ")
	case TableOptionTablespace:
		ctx.WriteKeyWord("TABLESPACE ")
		ctx.WritePlain("= ")
		ctx.WriteName(n.StrValue)
	case TableOptionNodegroup:
		ctx.WriteKeyWord("NODEGROUP ")
		ctx.WritePlainf("= %d", n.UintValue)
	case TableOptionDataDirectory:
		ctx.WriteKeyWord("DATA DIRECTORY ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionIndexDirectory:
		ctx.WriteKeyWord("INDEX DIRECTORY ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionStorageMedia:
		ctx.WriteKeyWord("STORAGE ")
		ctx.WriteKeyWord(n.StrValue)
	case TableOptionStatsSamplePages:
		ctx.WriteKeyWord("STATS_SAMPLE_PAGES ")
		ctx.WritePlain("= ")
		if n.Default {
			ctx.WriteKeyWord("DEFAULT")
		} else {
			ctx.WritePlainf("%d", n.UintValue)
		}
	case TableOptionSecondaryEngine:
		ctx.WriteKeyWord("SECONDARY_ENGINE ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionSecondaryEngineNull:
		ctx.WriteKeyWord("SECONDARY_ENGINE ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord("NULL")
	case TableOptionInsertMethod:
		ctx.WriteKeyWord("INSERT_METHOD ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionTableCheckSum:
		ctx.WriteKeyWord("TABLE_CHECKSUM ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionUnion:
		ctx.WriteKeyWord("UNION ")
		ctx.WritePlain("= (")
		for i, tableName := range n.TableNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			tableName.Restore(ctx)
		}
		ctx.WritePlain(")")
	case TableOptionEncryption:
		ctx.WriteKeyWord("ENCRYPTION ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionPlacementPolicy:
		if ctx.Flags.HasSkipPlacementRuleForRestoreFlag() {
			return nil
		}
		placementOpt := PlacementOption{
			Tp:        PlacementOptionPolicy,
			UintValue: n.UintValue,
			StrValue:  n.StrValue,
		}
		return placementOpt.Restore(ctx)
	case TableOptionStatsBuckets:
		ctx.WriteKeyWord("STATS_BUCKETS ")
		ctx.WritePlain("= ")
		if n.Default {
			ctx.WriteKeyWord("DEFAULT")
		} else {
			ctx.WritePlainf("%d", n.UintValue)
		}
	case TableOptionStatsTopN:
		ctx.WriteKeyWord("STATS_TOPN ")
		ctx.WritePlain("= ")
		if n.Default {
			ctx.WriteKeyWord("DEFAULT")
		} else {
			ctx.WritePlainf("%d", n.UintValue)
		}
	case TableOptionStatsSampleRate:
		ctx.WriteKeyWord("STATS_SAMPLE_RATE ")
		ctx.WritePlain("= ")
		if n.Default {
			ctx.WriteKeyWord("DEFAULT")
		} else {
			ctx.WritePlainf("%v", n.Value.GetValue())
		}
	case TableOptionStatsColsChoice:
		ctx.WriteKeyWord("STATS_COL_CHOICE ")
		ctx.WritePlain("= ")
		if n.Default {
			ctx.WriteKeyWord("DEFAULT")
		} else {
			ctx.WriteString(n.StrValue)
		}
	case TableOptionStatsColList:
		ctx.WriteKeyWord("STATS_COL_LIST ")
		ctx.WritePlain("= ")
		if n.Default {
			ctx.WriteKeyWord("DEFAULT")
		} else {
			ctx.WriteString(n.StrValue)
		}
	default:
		return errors.Errorf("invalid TableOption: %d", n.Tp)
	}
	return nil
}

// SequenceOptionType is the type for SequenceOption
type SequenceOptionType int

// SequenceOption types.
const (
	SequenceOptionNone SequenceOptionType = iota
	SequenceOptionIncrementBy
	SequenceStartWith
	SequenceNoMinValue
	SequenceMinValue
	SequenceNoMaxValue
	SequenceMaxValue
	SequenceNoCache
	SequenceCache
	SequenceNoCycle
	SequenceCycle
	// SequenceRestart is only used in alter sequence statement.
	SequenceRestart
	SequenceRestartWith
)

// SequenceOption is used for parsing sequence option from SQL.
type SequenceOption struct {
	Tp       SequenceOptionType
	IntValue int64
}

func (n *SequenceOption) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case SequenceOptionIncrementBy:
		ctx.WriteKeyWord("INCREMENT BY ")
		ctx.WritePlainf("%d", n.IntValue)
	case SequenceStartWith:
		ctx.WriteKeyWord("START WITH ")
		ctx.WritePlainf("%d", n.IntValue)
	case SequenceNoMinValue:
		ctx.WriteKeyWord("NO MINVALUE")
	case SequenceMinValue:
		ctx.WriteKeyWord("MINVALUE ")
		ctx.WritePlainf("%d", n.IntValue)
	case SequenceNoMaxValue:
		ctx.WriteKeyWord("NO MAXVALUE")
	case SequenceMaxValue:
		ctx.WriteKeyWord("MAXVALUE ")
		ctx.WritePlainf("%d", n.IntValue)
	case SequenceNoCache:
		ctx.WriteKeyWord("NOCACHE")
	case SequenceCache:
		ctx.WriteKeyWord("CACHE ")
		ctx.WritePlainf("%d", n.IntValue)
	case SequenceNoCycle:
		ctx.WriteKeyWord("NOCYCLE")
	case SequenceCycle:
		ctx.WriteKeyWord("CYCLE")
	case SequenceRestart:
		ctx.WriteKeyWord("RESTART")
	case SequenceRestartWith:
		ctx.WriteKeyWord("RESTART WITH ")
		ctx.WritePlainf("%d", n.IntValue)
	default:
		return errors.Errorf("invalid SequenceOption: %d", n.Tp)
	}
	return nil
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
func (n *ColumnPosition) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case ColumnPositionNone:
		// do nothing
	case ColumnPositionFirst:
		ctx.WriteKeyWord("FIRST")
	case ColumnPositionAfter:
		ctx.WriteKeyWord("AFTER ")
		if err := n.RelativeColumn.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore ColumnPosition.RelativeColumn")
		}
	default:
		return errors.Errorf("invalid ColumnPositionType: %d", n.Tp)
	}
	return nil
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
	AlterTableRenameColumn
	AlterTableRenameTable
	AlterTableAlterColumn
	AlterTableLock
	AlterTableWriteable
	AlterTableAlgorithm
	AlterTableRenameIndex
	AlterTableForce
	AlterTableAddPartitions
	// A tombstone for `AlterTableAlterPartition`. It will never be used anymore.
	// Just left a tombstone here to keep the enum number unchanged.
	__DEPRECATED_AlterTableAlterPartition
	AlterTablePartitionAttributes
	AlterTablePartitionOptions
	AlterTableCoalescePartitions
	AlterTableDropPartition
	AlterTableTruncatePartition
	AlterTablePartition
	AlterTableEnableKeys
	AlterTableDisableKeys
	AlterTableRemovePartitioning
	AlterTableWithValidation
	AlterTableWithoutValidation
	AlterTableSecondaryLoad
	AlterTableSecondaryUnload
	AlterTableRebuildPartition
	AlterTableReorganizePartition
	AlterTableCheckPartitions
	AlterTableExchangePartition
	AlterTableOptimizePartition
	AlterTableRepairPartition
	AlterTableImportPartitionTablespace
	AlterTableDiscardPartitionTablespace
	AlterTableAlterCheck
	AlterTableDropCheck
	AlterTableImportTablespace
	AlterTableDiscardTablespace
	AlterTableIndexInvisible
	// TODO: Add more actions
	AlterTableOrderByColumns
	// AlterTableSetTiFlashReplica uses to set the table TiFlash replica.
	AlterTableSetTiFlashReplica
	// A tombstone for `AlterTablePlacement`. It will never be used anymore.
	// Just left a tombstone here to keep the enum number unchanged.
	__DEPRECATED_AlterTablePlacement
	AlterTableAddStatistics
	AlterTableDropStatistics
	AlterTableAttributes
	AlterTableCache
	AlterTableNoCache
	AlterTableStatsOptions
)

// LockType is the type for AlterTableSpec.
// See https://dev.mysql.com/doc/refman/5.7/en/alter-table.html#alter-table-concurrency
type LockType byte

func (n LockType) String() string {
	switch n {
	case LockTypeNone:
		return "NONE"
	case LockTypeDefault:
		return "DEFAULT"
	case LockTypeShared:
		return "SHARED"
	case LockTypeExclusive:
		return "EXCLUSIVE"
	}
	return ""
}

// Lock Types.
const (
	LockTypeNone LockType = iota + 1
	LockTypeDefault
	LockTypeShared
	LockTypeExclusive
)

// AlgorithmType is the algorithm of the DDL operations.
// See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html#alter-table-performance.
type AlgorithmType byte

// DDL algorithms.
// For now, TiDB only supported inplace and instance algorithms. If the user specify `copy`,
// will get an error.
const (
	AlgorithmTypeDefault AlgorithmType = iota
	AlgorithmTypeCopy
	AlgorithmTypeInplace
	AlgorithmTypeInstant
)

func (a AlgorithmType) String() string {
	switch a {
	case AlgorithmTypeDefault:
		return "DEFAULT"
	case AlgorithmTypeCopy:
		return "COPY"
	case AlgorithmTypeInplace:
		return "INPLACE"
	case AlgorithmTypeInstant:
		return "INSTANT"
	default:
		return "DEFAULT"
	}
}

// AlterTableSpec represents alter table specification.
type AlterTableSpec struct {
	node

	// only supported by MariaDB 10.0.2+ (DROP COLUMN, CHANGE COLUMN, MODIFY COLUMN, DROP INDEX, DROP FOREIGN KEY, DROP PARTITION)
	// see https://mariadb.com/kb/en/library/alter-table/
	IfExists bool

	// only supported by MariaDB 10.0.2+ (ADD COLUMN, ADD PARTITION)
	// see https://mariadb.com/kb/en/library/alter-table/
	IfNotExists bool

	NoWriteToBinlog bool
	OnAllPartitions bool

	Tp               AlterTableType
	Name             string
	IndexName        model.CIStr
	Constraint       *Constraint
	Options          []*TableOption
	OrderByList      []*AlterOrderItem
	NewTable         *TableName
	NewColumns       []*ColumnDef
	NewConstraints   []*Constraint
	OldColumnName    *ColumnName
	NewColumnName    *ColumnName
	Position         *ColumnPosition
	LockType         LockType
	Algorithm        AlgorithmType
	Comment          string
	FromKey          model.CIStr
	ToKey            model.CIStr
	Partition        *PartitionOptions
	PartitionNames   []model.CIStr
	PartDefinitions  []*PartitionDefinition
	WithValidation   bool
	Num              uint64
	Visibility       IndexVisibility
	TiFlashReplica   *TiFlashReplicaSpec
	Writeable        bool
	Statistics       *StatisticsSpec
	AttributesSpec   *AttributesSpec
	StatsOptionsSpec *StatsOptionsSpec
}

type TiFlashReplicaSpec struct {
	Count  uint64
	Labels []string
}

// AlterOrderItem represents an item in order by at alter table stmt.
type AlterOrderItem struct {
	node
	Column *ColumnName
	Desc   bool
}

// Restore implements Node interface.
func (n *AlterOrderItem) Restore(ctx *format.RestoreCtx) error {
	if err := n.Column.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore AlterOrderItem.Column")
	}
	if n.Desc {
		ctx.WriteKeyWord(" DESC")
	}
	return nil
}

func (n *AlterTableSpec) IsAllPlacementRule() bool {
	switch n.Tp {
	case AlterTablePartitionAttributes, AlterTablePartitionOptions, AlterTableOption, AlterTableAttributes:
		for _, o := range n.Options {
			if o.Tp != TableOptionPlacementPolicy {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// Restore implements Node interface.
func (n *AlterTableSpec) Restore(ctx *format.RestoreCtx) error {
	if n.IsAllPlacementRule() && ctx.Flags.HasSkipPlacementRuleForRestoreFlag() {
		return nil
	}
	switch n.Tp {
	case AlterTableSetTiFlashReplica:
		ctx.WriteKeyWord("SET TIFLASH REPLICA ")
		ctx.WritePlainf("%d", n.TiFlashReplica.Count)
		if len(n.TiFlashReplica.Labels) == 0 {
			break
		}
		ctx.WriteKeyWord(" LOCATION LABELS ")
		for i, v := range n.TiFlashReplica.Labels {
			if i > 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteString(v)
		}
	case AlterTableAddStatistics:
		ctx.WriteKeyWord("ADD STATS_EXTENDED ")
		if n.IfNotExists {
			ctx.WriteKeyWord("IF NOT EXISTS ")
		}
		ctx.WriteName(n.Statistics.StatsName)
		switch n.Statistics.StatsType {
		case StatsTypeCardinality:
			ctx.WriteKeyWord(" CARDINALITY(")
		case StatsTypeDependency:
			ctx.WriteKeyWord(" DEPENDENCY(")
		case StatsTypeCorrelation:
			ctx.WriteKeyWord(" CORRELATION(")
		}
		for i, col := range n.Statistics.Columns {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := col.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore AddStatisticsSpec.Columns: [%v]", i)
			}
		}
		ctx.WritePlain(")")
	case AlterTableDropStatistics:
		ctx.WriteKeyWord("DROP STATS_EXTENDED ")
		if n.IfExists {
			ctx.WriteKeyWord("IF EXISTS ")
		}
		ctx.WriteName(n.Statistics.StatsName)
	case AlterTableOption:
		switch {
		case len(n.Options) == 2 && n.Options[0].Tp == TableOptionCharset && n.Options[1].Tp == TableOptionCollate:
			if n.Options[0].UintValue == TableOptionCharsetWithConvertTo {
				ctx.WriteKeyWord("CONVERT TO ")
			}
			ctx.WriteKeyWord("CHARACTER SET ")
			if n.Options[0].Default {
				ctx.WriteKeyWord("DEFAULT")
			} else {
				ctx.WriteKeyWord(n.Options[0].StrValue)
			}
			ctx.WriteKeyWord(" COLLATE ")
			ctx.WriteKeyWord(n.Options[1].StrValue)
		case n.Options[0].Tp == TableOptionCharset && n.Options[0].Default:
			if n.Options[0].UintValue == TableOptionCharsetWithConvertTo {
				ctx.WriteKeyWord("CONVERT TO ")
			}
			ctx.WriteKeyWord("CHARACTER SET DEFAULT")
		default:
			for i, opt := range n.Options {
				if i != 0 {
					ctx.WritePlain(" ")
				}
				if err := opt.Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.Options[%d]", i)
				}
			}
		}
	case AlterTableAddColumns:
		ctx.WriteKeyWord("ADD COLUMN ")
		if n.IfNotExists {
			ctx.WriteKeyWord("IF NOT EXISTS ")
		}
		if n.Position != nil && len(n.NewColumns) == 1 {
			if err := n.NewColumns[0].Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.NewColumns[%d]", 0)
			}
			if n.Position.Tp != ColumnPositionNone {
				ctx.WritePlain(" ")
			}
			if err := n.Position.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore AlterTableSpec.Position")
			}
		} else {
			lenCols := len(n.NewColumns)
			ctx.WritePlain("(")
			for i, col := range n.NewColumns {
				if i != 0 {
					ctx.WritePlain(", ")
				}
				if err := col.Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.NewColumns[%d]", i)
				}
			}
			for i, constraint := range n.NewConstraints {
				if i != 0 || lenCols >= 1 {
					ctx.WritePlain(", ")
				}
				if err := constraint.Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.NewConstraints[%d]", i)
				}
			}
			ctx.WritePlain(")")
		}
	case AlterTableAddConstraint:
		ctx.WriteKeyWord("ADD ")
		if err := n.Constraint.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.Constraint")
		}
	case AlterTableDropColumn:
		ctx.WriteKeyWord("DROP COLUMN ")
		if n.IfExists {
			ctx.WriteKeyWord("IF EXISTS ")
		}
		if err := n.OldColumnName.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.OldColumnName")
		}
	// TODO: RestrictOrCascadeOpt not support
	case AlterTableDropPrimaryKey:
		ctx.WriteKeyWord("DROP PRIMARY KEY")
	case AlterTableDropIndex:
		ctx.WriteKeyWord("DROP INDEX ")
		if n.IfExists {
			ctx.WriteKeyWord("IF EXISTS ")
		}
		ctx.WriteName(n.Name)
	case AlterTableDropForeignKey:
		ctx.WriteKeyWord("DROP FOREIGN KEY ")
		if n.IfExists {
			ctx.WriteKeyWord("IF EXISTS ")
		}
		ctx.WriteName(n.Name)
	case AlterTableModifyColumn:
		ctx.WriteKeyWord("MODIFY COLUMN ")
		if n.IfExists {
			ctx.WriteKeyWord("IF EXISTS ")
		}
		if err := n.NewColumns[0].Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.NewColumns[0]")
		}
		if n.Position.Tp != ColumnPositionNone {
			ctx.WritePlain(" ")
		}
		if err := n.Position.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.Position")
		}
	case AlterTableChangeColumn:
		ctx.WriteKeyWord("CHANGE COLUMN ")
		if n.IfExists {
			ctx.WriteKeyWord("IF EXISTS ")
		}
		if err := n.OldColumnName.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.OldColumnName")
		}
		ctx.WritePlain(" ")
		if err := n.NewColumns[0].Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.NewColumns[0]")
		}
		if n.Position.Tp != ColumnPositionNone {
			ctx.WritePlain(" ")
		}
		if err := n.Position.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.Position")
		}
	case AlterTableRenameColumn:
		ctx.WriteKeyWord("RENAME COLUMN ")
		if err := n.OldColumnName.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.OldColumnName")
		}
		ctx.WriteKeyWord(" TO ")
		if err := n.NewColumnName.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.NewColumnName")
		}
	case AlterTableRenameTable:
		ctx.WriteKeyWord("RENAME AS ")
		if err := n.NewTable.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.NewTable")
		}
	case AlterTableAlterColumn:
		ctx.WriteKeyWord("ALTER COLUMN ")
		if err := n.NewColumns[0].Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.NewColumns[0]")
		}
		if len(n.NewColumns[0].Options) == 1 {
			ctx.WriteKeyWord("SET DEFAULT ")
			expr := n.NewColumns[0].Options[0].Expr
			if valueExpr, ok := expr.(ValueExpr); ok {
				if err := valueExpr.Restore(ctx); err != nil {
					return errors.Annotate(err, "An error occurred while restore AlterTableSpec.NewColumns[0].Options[0].Expr")
				}
			} else {
				ctx.WritePlain("(")
				if err := expr.Restore(ctx); err != nil {
					return errors.Annotate(err, "An error occurred while restore AlterTableSpec.NewColumns[0].Options[0].Expr")
				}
				ctx.WritePlain(")")
			}
		} else {
			ctx.WriteKeyWord(" DROP DEFAULT")
		}
	case AlterTableLock:
		ctx.WriteKeyWord("LOCK ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord(n.LockType.String())
	case AlterTableWriteable:
		ctx.WriteKeyWord("READ ")
		if n.Writeable {
			ctx.WriteKeyWord("WRITE")
		} else {
			ctx.WriteKeyWord("ONLY")
		}
	case AlterTableOrderByColumns:
		ctx.WriteKeyWord("ORDER BY ")
		for i, alterOrderItem := range n.OrderByList {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := alterOrderItem.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.OrderByList[%d]", i)
			}
		}
	case AlterTableAlgorithm:
		ctx.WriteKeyWord("ALGORITHM ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord(n.Algorithm.String())
	case AlterTableRenameIndex:
		ctx.WriteKeyWord("RENAME INDEX ")
		ctx.WriteName(n.FromKey.O)
		ctx.WriteKeyWord(" TO ")
		ctx.WriteName(n.ToKey.O)
	case AlterTableForce:
		// TODO: not support
		ctx.WriteKeyWord("FORCE")
		ctx.WritePlain(" /* AlterTableForce is not supported */ ")
	case AlterTableAddPartitions:
		ctx.WriteKeyWord("ADD PARTITION")
		if n.IfNotExists {
			ctx.WriteKeyWord(" IF NOT EXISTS")
		}
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord(" NO_WRITE_TO_BINLOG")
		}
		if n.PartDefinitions != nil {
			ctx.WritePlain(" (")
			for i, def := range n.PartDefinitions {
				if i != 0 {
					ctx.WritePlain(", ")
				}
				if err := def.Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.PartDefinitions[%d]", i)
				}
			}
			ctx.WritePlain(")")
		} else if n.Num != 0 {
			ctx.WriteKeyWord(" PARTITIONS ")
			ctx.WritePlainf("%d", n.Num)
		}
	case AlterTablePartitionOptions:
		restoreWithoutSpecialComment := func() error {
			origFlags := ctx.Flags
			defer func() {
				ctx.Flags = origFlags
			}()
			ctx.Flags &= ^format.RestoreTiDBSpecialComment
			ctx.WriteKeyWord("PARTITION ")
			ctx.WriteName(n.PartitionNames[0].O)
			ctx.WritePlain(" ")

			for i, opt := range n.Options {
				if i != 0 {
					ctx.WritePlain(" ")
				}
				if err := opt.Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.Options[%d] for PARTITION `%s`", i, n.PartitionNames[0].O)
				}
			}
			return nil
		}

		var err error
		if ctx.Flags.HasTiDBSpecialCommentFlag() {
			// AlterTablePartitionOptions now only supports placement options, so add put all options to special comment
			err = ctx.WriteWithSpecialComments(tidb.FeatureIDPlacement, restoreWithoutSpecialComment)
		} else {
			err = restoreWithoutSpecialComment()
		}

		if err != nil {
			return err
		}
	case AlterTablePartitionAttributes:
		ctx.WriteKeyWord("PARTITION ")
		ctx.WriteName(n.PartitionNames[0].O)
		ctx.WritePlain(" ")

		spec := n.AttributesSpec
		if err := spec.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.AttributesSpec")
		}
	case AlterTableCoalescePartitions:
		ctx.WriteKeyWord("COALESCE PARTITION ")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
		}
		ctx.WritePlainf("%d", n.Num)
	case AlterTableDropPartition:
		ctx.WriteKeyWord("DROP PARTITION ")
		if n.IfExists {
			ctx.WriteKeyWord("IF EXISTS ")
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableTruncatePartition:
		ctx.WriteKeyWord("TRUNCATE PARTITION ")
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableCheckPartitions:
		ctx.WriteKeyWord("CHECK PARTITION ")
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableOptimizePartition:
		ctx.WriteKeyWord("OPTIMIZE PARTITION ")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
		}
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableRepairPartition:
		ctx.WriteKeyWord("REPAIR PARTITION ")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
		}
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableImportPartitionTablespace:
		ctx.WriteKeyWord("IMPORT PARTITION ")
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
		} else {
			for i, name := range n.PartitionNames {
				if i != 0 {
					ctx.WritePlain(",")
				}
				ctx.WriteName(name.O)
			}
		}
		ctx.WriteKeyWord(" TABLESPACE")
	case AlterTableDiscardPartitionTablespace:
		ctx.WriteKeyWord("DISCARD PARTITION ")
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
		} else {
			for i, name := range n.PartitionNames {
				if i != 0 {
					ctx.WritePlain(",")
				}
				ctx.WriteName(name.O)
			}
		}
		ctx.WriteKeyWord(" TABLESPACE")
	case AlterTablePartition:
		if err := n.Partition.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterTableSpec.Partition")
		}
	case AlterTableEnableKeys:
		ctx.WriteKeyWord("ENABLE KEYS")
	case AlterTableDisableKeys:
		ctx.WriteKeyWord("DISABLE KEYS")
	case AlterTableRemovePartitioning:
		ctx.WriteKeyWord("REMOVE PARTITIONING")
	case AlterTableWithValidation:
		ctx.WriteKeyWord("WITH VALIDATION")
	case AlterTableWithoutValidation:
		ctx.WriteKeyWord("WITHOUT VALIDATION")
	case AlterTableRebuildPartition:
		ctx.WriteKeyWord("REBUILD PARTITION ")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
		}
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableReorganizePartition:
		ctx.WriteKeyWord("REORGANIZE PARTITION")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord(" NO_WRITE_TO_BINLOG")
		}
		if n.OnAllPartitions {
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			} else {
				ctx.WritePlain(" ")
			}
			ctx.WriteName(name.O)
		}
		ctx.WriteKeyWord(" INTO ")
		if n.PartDefinitions != nil {
			ctx.WritePlain("(")
			for i, def := range n.PartDefinitions {
				if i != 0 {
					ctx.WritePlain(", ")
				}
				if err := def.Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.PartDefinitions[%d]", i)
				}
			}
			ctx.WritePlain(")")
		}
	case AlterTableExchangePartition:
		ctx.WriteKeyWord("EXCHANGE PARTITION ")
		ctx.WriteName(n.PartitionNames[0].O)
		ctx.WriteKeyWord(" WITH TABLE ")
		n.NewTable.Restore(ctx)
		if !n.WithValidation {
			ctx.WriteKeyWord(" WITHOUT VALIDATION")
		}
	case AlterTableSecondaryLoad:
		ctx.WriteKeyWord("SECONDARY_LOAD")
	case AlterTableSecondaryUnload:
		ctx.WriteKeyWord("SECONDARY_UNLOAD")
	case AlterTableAlterCheck:
		ctx.WriteKeyWord("ALTER CHECK ")
		ctx.WriteName(n.Constraint.Name)
		if !n.Constraint.Enforced {
			ctx.WriteKeyWord(" NOT")
		}
		ctx.WriteKeyWord(" ENFORCED")
	case AlterTableDropCheck:
		ctx.WriteKeyWord("DROP CHECK ")
		ctx.WriteName(n.Constraint.Name)
	case AlterTableImportTablespace:
		ctx.WriteKeyWord("IMPORT TABLESPACE")
	case AlterTableDiscardTablespace:
		ctx.WriteKeyWord("DISCARD TABLESPACE")
	case AlterTableIndexInvisible:
		ctx.WriteKeyWord("ALTER INDEX ")
		ctx.WriteName(n.IndexName.O)
		switch n.Visibility {
		case IndexVisibilityVisible:
			ctx.WriteKeyWord(" VISIBLE")
		case IndexVisibilityInvisible:
			ctx.WriteKeyWord(" INVISIBLE")
		}
	case AlterTableAttributes:
		spec := n.AttributesSpec
		if err := spec.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.AttributesSpec")
		}
	case AlterTableCache:
		ctx.WriteKeyWord("CACHE")
	case AlterTableNoCache:
		ctx.WriteKeyWord("NOCACHE")
	case AlterTableStatsOptions:
		spec := n.StatsOptionsSpec
		if err := spec.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterTableSpec.StatsOptionsSpec")
		}

	default:
		// TODO: not support
		ctx.WritePlainf(" /* AlterTableType(%d) is not supported */ ", n.Tp)
	}
	return nil
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
	for i, col := range n.NewColumns {
		node, ok := col.Accept(v)
		if !ok {
			return n, false
		}
		n.NewColumns[i] = node.(*ColumnDef)
	}
	for i, constraint := range n.NewConstraints {
		node, ok := constraint.Accept(v)
		if !ok {
			return n, false
		}
		n.NewConstraints[i] = node.(*Constraint)
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
	if n.Partition != nil {
		node, ok := n.Partition.Accept(v)
		if !ok {
			return n, false
		}
		n.Partition = node.(*PartitionOptions)
	}
	for _, def := range n.PartDefinitions {
		if !def.acceptInPlace(v) {
			return n, false
		}
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

func (n *AlterTableStmt) HaveOnlyPlacementOptions() bool {
	for _, n := range n.Specs {
		if n.Tp == AlterTablePartitionOptions {
			if !n.IsAllPlacementRule() {
				return false
			}
		} else {
			return false
		}

	}
	return true
}

// Restore implements Node interface.
func (n *AlterTableStmt) Restore(ctx *format.RestoreCtx) error {
	if ctx.Flags.HasSkipPlacementRuleForRestoreFlag() && n.HaveOnlyPlacementOptions() {
		return nil
	}
	ctx.WriteKeyWord("ALTER TABLE ")
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore AlterTableStmt.Table")
	}
	var specs []*AlterTableSpec
	for _, spec := range n.Specs {
		if !(spec.IsAllPlacementRule() && ctx.Flags.HasSkipPlacementRuleForRestoreFlag()) {
			specs = append(specs, spec)
		}
	}
	for i, spec := range specs {
		if i == 0 || spec.Tp == AlterTablePartition || spec.Tp == AlterTableRemovePartitioning || spec.Tp == AlterTableImportTablespace || spec.Tp == AlterTableDiscardTablespace {
			ctx.WritePlain(" ")
		} else {
			ctx.WritePlain(", ")
		}
		if err := spec.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterTableStmt.Specs[%d]", i)
		}
	}
	return nil
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
func (n *TruncateTableStmt) Restore(ctx *format.RestoreCtx) error {
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

var (
	ErrNoParts                              = terror.ClassDDL.NewStd(mysql.ErrNoParts)
	ErrPartitionColumnList                  = terror.ClassDDL.NewStd(mysql.ErrPartitionColumnList)
	ErrPartitionRequiresValues              = terror.ClassDDL.NewStd(mysql.ErrPartitionRequiresValues)
	ErrPartitionsMustBeDefined              = terror.ClassDDL.NewStd(mysql.ErrPartitionsMustBeDefined)
	ErrPartitionWrongNoPart                 = terror.ClassDDL.NewStd(mysql.ErrPartitionWrongNoPart)
	ErrPartitionWrongNoSubpart              = terror.ClassDDL.NewStd(mysql.ErrPartitionWrongNoSubpart)
	ErrPartitionWrongValues                 = terror.ClassDDL.NewStd(mysql.ErrPartitionWrongValues)
	ErrRowSinglePartitionField              = terror.ClassDDL.NewStd(mysql.ErrRowSinglePartitionField)
	ErrSubpartition                         = terror.ClassDDL.NewStd(mysql.ErrSubpartition)
	ErrSystemVersioningWrongPartitions      = terror.ClassDDL.NewStd(mysql.ErrSystemVersioningWrongPartitions)
	ErrTooManyValues                        = terror.ClassDDL.NewStd(mysql.ErrTooManyValues)
	ErrWrongPartitionTypeExpectedSystemTime = terror.ClassDDL.NewStd(mysql.ErrWrongPartitionTypeExpectedSystemTime)
	ErrUnknownCharacterSet                  = terror.ClassDDL.NewStd(mysql.ErrUnknownCharacterSet)
)

type SubPartitionDefinition struct {
	Name    model.CIStr
	Options []*TableOption
}

func (spd *SubPartitionDefinition) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SUBPARTITION ")
	ctx.WriteName(spd.Name.O)
	for i, opt := range spd.Options {
		ctx.WritePlain(" ")
		if err := opt.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore SubPartitionDefinition.Options[%d]", i)
		}
	}
	return nil
}

type PartitionDefinitionClause interface {
	restore(ctx *format.RestoreCtx) error
	acceptInPlace(v Visitor) bool
	// Validate checks if the clause is consistent with the given options.
	// `pt` can be 0 and `columns` can be -1 to skip checking the clause against
	// the partition type or number of columns in the expression list.
	Validate(pt model.PartitionType, columns int) error
}

type PartitionDefinitionClauseNone struct{}

func (n *PartitionDefinitionClauseNone) restore(ctx *format.RestoreCtx) error {
	return nil
}

func (n *PartitionDefinitionClauseNone) acceptInPlace(v Visitor) bool {
	return true
}

func (n *PartitionDefinitionClauseNone) Validate(pt model.PartitionType, columns int) error {
	switch pt {
	case 0:
	case model.PartitionTypeRange:
		return ErrPartitionRequiresValues.GenWithStackByArgs("RANGE", "LESS THAN")
	case model.PartitionTypeList:
		return ErrPartitionRequiresValues.GenWithStackByArgs("LIST", "IN")
	case model.PartitionTypeSystemTime:
		return ErrSystemVersioningWrongPartitions
	}
	return nil
}

type PartitionDefinitionClauseLessThan struct {
	Exprs []ExprNode
}

func (n *PartitionDefinitionClauseLessThan) restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(" VALUES LESS THAN ")
	ctx.WritePlain("(")
	for i, expr := range n.Exprs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := expr.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore PartitionDefinitionClauseLessThan.Exprs[%d]", i)
		}
	}
	ctx.WritePlain(")")
	return nil
}

func (n *PartitionDefinitionClauseLessThan) acceptInPlace(v Visitor) bool {
	for i, expr := range n.Exprs {
		newExpr, ok := expr.Accept(v)
		if !ok {
			return false
		}
		n.Exprs[i] = newExpr.(ExprNode)
	}
	return true
}

func (n *PartitionDefinitionClauseLessThan) Validate(pt model.PartitionType, columns int) error {
	switch pt {
	case model.PartitionTypeRange, 0:
	default:
		return ErrPartitionWrongValues.GenWithStackByArgs("RANGE", "LESS THAN")
	}

	switch {
	case columns == 0 && len(n.Exprs) != 1:
		return ErrTooManyValues.GenWithStackByArgs("RANGE")
	case columns > 0 && len(n.Exprs) != columns:
		return ErrPartitionColumnList
	}
	return nil
}

type PartitionDefinitionClauseIn struct {
	Values [][]ExprNode
}

func (n *PartitionDefinitionClauseIn) restore(ctx *format.RestoreCtx) error {
	// we special-case an empty list of values to mean MariaDB's "DEFAULT" clause.
	if len(n.Values) == 0 {
		ctx.WriteKeyWord(" DEFAULT")
		return nil
	}

	ctx.WriteKeyWord(" VALUES IN ")
	ctx.WritePlain("(")
	for i, valList := range n.Values {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if len(valList) == 1 {
			if err := valList[0].Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore PartitionDefinitionClauseIn.Values[%d][0]", i)
			}
		} else {
			ctx.WritePlain("(")
			for j, val := range valList {
				if j != 0 {
					ctx.WritePlain(", ")
				}
				if err := val.Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore PartitionDefinitionClauseIn.Values[%d][%d]", i, j)
				}
			}
			ctx.WritePlain(")")
		}
	}
	ctx.WritePlain(")")
	return nil
}

func (n *PartitionDefinitionClauseIn) acceptInPlace(v Visitor) bool {
	for _, valList := range n.Values {
		for j, val := range valList {
			newVal, ok := val.Accept(v)
			if !ok {
				return false
			}
			valList[j] = newVal.(ExprNode)
		}
	}
	return true
}

func (n *PartitionDefinitionClauseIn) Validate(pt model.PartitionType, columns int) error {
	switch pt {
	case model.PartitionTypeList, 0:
	default:
		return ErrPartitionWrongValues.GenWithStackByArgs("LIST", "IN")
	}

	if len(n.Values) == 0 {
		return nil
	}

	expectedColCount := len(n.Values[0])
	for _, val := range n.Values[1:] {
		if len(val) != expectedColCount {
			return ErrPartitionColumnList
		}
	}

	switch {
	case columns == 0 && expectedColCount != 1:
		return ErrRowSinglePartitionField
	case columns > 0 && expectedColCount != columns:
		return ErrPartitionColumnList
	}
	return nil
}

type PartitionDefinitionClauseHistory struct {
	Current bool
}

func (n *PartitionDefinitionClauseHistory) restore(ctx *format.RestoreCtx) error {
	if n.Current {
		ctx.WriteKeyWord(" CURRENT")
	} else {
		ctx.WriteKeyWord(" HISTORY")
	}
	return nil
}

func (n *PartitionDefinitionClauseHistory) acceptInPlace(v Visitor) bool {
	return true
}

func (n *PartitionDefinitionClauseHistory) Validate(pt model.PartitionType, columns int) error {
	switch pt {
	case 0, model.PartitionTypeSystemTime:
	default:
		return ErrWrongPartitionTypeExpectedSystemTime
	}

	return nil
}

// PartitionDefinition defines a single partition.
type PartitionDefinition struct {
	Name    model.CIStr
	Clause  PartitionDefinitionClause
	Options []*TableOption
	Sub     []*SubPartitionDefinition
}

// Comment returns the comment option given to this definition.
// The second return value indicates if the comment option exists.
func (n *PartitionDefinition) Comment() (string, bool) {
	for _, opt := range n.Options {
		if opt.Tp == TableOptionComment {
			return opt.StrValue, true
		}
	}
	return "", false
}

func (n *PartitionDefinition) acceptInPlace(v Visitor) bool {
	return n.Clause.acceptInPlace(v)
}

// Restore implements Node interface.
func (n *PartitionDefinition) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("PARTITION ")
	ctx.WriteName(n.Name.O)

	if err := n.Clause.restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore PartitionDefinition.Clause")
	}

	for i, opt := range n.Options {
		ctx.WritePlain(" ")
		if err := opt.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore PartitionDefinition.Options[%d]", i)
		}
	}

	if len(n.Sub) > 0 {
		ctx.WritePlain(" (")
		for i, spd := range n.Sub {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := spd.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore PartitionDefinition.Sub[%d]", i)
			}
		}
		ctx.WritePlain(")")
	}

	return nil
}

// PartitionMethod describes how partitions or subpartitions are constructed.
type PartitionMethod struct {
	// Tp is the type of the partition function
	Tp model.PartitionType
	// Linear is a modifier to the HASH and KEY type for choosing a different
	// algorithm
	Linear bool
	// Expr is an expression used as argument of HASH, RANGE, LIST and
	// SYSTEM_TIME types
	Expr ExprNode
	// ColumnNames is a list of column names used as argument of KEY,
	// RANGE COLUMNS and LIST COLUMNS types
	ColumnNames []*ColumnName
	// Unit is a time unit used as argument of SYSTEM_TIME type
	Unit TimeUnitType
	// Limit is a row count used as argument of the SYSTEM_TIME type
	Limit uint64

	// Num is the number of (sub)partitions required by the method.
	Num uint64

	// KeyAlgorithm is the optional hash algorithm type for `PARTITION BY [LINEAR] KEY` syntax.
	KeyAlgorithm *PartitionKeyAlgorithm
}

type PartitionKeyAlgorithm struct {
	Type uint64
}

// Restore implements the Node interface
func (n *PartitionMethod) Restore(ctx *format.RestoreCtx) error {
	if n.Linear {
		ctx.WriteKeyWord("LINEAR ")
	}
	ctx.WriteKeyWord(n.Tp.String())

	if n.KeyAlgorithm != nil {
		ctx.WriteKeyWord(" ALGORITHM")
		ctx.WritePlainf(" = %d", n.KeyAlgorithm.Type)
	}

	switch {
	case n.Tp == model.PartitionTypeSystemTime:
		if n.Expr != nil && n.Unit != TimeUnitInvalid {
			ctx.WriteKeyWord(" INTERVAL ")
			if err := n.Expr.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore PartitionMethod.Expr")
			}
			ctx.WritePlain(" ")
			ctx.WriteKeyWord(n.Unit.String())
		}

	case n.Expr != nil:
		ctx.WritePlain(" (")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore PartitionMethod.Expr")
		}
		ctx.WritePlain(")")

	default:
		if n.Tp == model.PartitionTypeRange || n.Tp == model.PartitionTypeList {
			ctx.WriteKeyWord(" COLUMNS")
		}
		ctx.WritePlain(" (")
		for i, col := range n.ColumnNames {
			if i > 0 {
				ctx.WritePlain(",")
			}
			if err := col.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while splicing PartitionMethod.ColumnName[%d]", i)
			}
		}
		ctx.WritePlain(")")
	}

	if n.Limit > 0 {
		ctx.WriteKeyWord(" LIMIT ")
		ctx.WritePlainf("%d", n.Limit)
	}

	return nil
}

// acceptInPlace is like Node.Accept but does not allow replacing the node itself.
func (n *PartitionMethod) acceptInPlace(v Visitor) bool {
	if n.Expr != nil {
		expr, ok := n.Expr.Accept(v)
		if !ok {
			return false
		}
		n.Expr = expr.(ExprNode)
	}
	for i, colName := range n.ColumnNames {
		newColName, ok := colName.Accept(v)
		if !ok {
			return false
		}
		n.ColumnNames[i] = newColName.(*ColumnName)
	}
	return true
}

// PartitionOptions specifies the partition options.
type PartitionOptions struct {
	node
	PartitionMethod
	Sub         *PartitionMethod
	Definitions []*PartitionDefinition
}

// Validate checks if the partition is well-formed.
func (n *PartitionOptions) Validate() error {
	// if both a partition list and the partition numbers are specified, their values must match
	if n.Num != 0 && len(n.Definitions) != 0 && n.Num != uint64(len(n.Definitions)) {
		return ErrPartitionWrongNoPart
	}
	// now check the subpartition count
	if len(n.Definitions) > 0 {
		// ensure the subpartition count for every partitions are the same
		// then normalize n.Num and n.Sub.Num so equality comparison works.
		n.Num = uint64(len(n.Definitions))

		subDefCount := len(n.Definitions[0].Sub)
		for _, pd := range n.Definitions[1:] {
			if len(pd.Sub) != subDefCount {
				return ErrPartitionWrongNoSubpart
			}
		}
		if n.Sub != nil {
			if n.Sub.Num != 0 && subDefCount != 0 && n.Sub.Num != uint64(subDefCount) {
				return ErrPartitionWrongNoSubpart
			}
			if subDefCount != 0 {
				n.Sub.Num = uint64(subDefCount)
			}
		} else if subDefCount != 0 {
			return ErrSubpartition
		}
	}

	switch n.Tp {
	case model.PartitionTypeHash, model.PartitionTypeKey:
		if n.Num == 0 {
			n.Num = 1
		}
	case model.PartitionTypeRange, model.PartitionTypeList:
		if len(n.Definitions) == 0 {
			return ErrPartitionsMustBeDefined.GenWithStackByArgs(n.Tp)
		}
	case model.PartitionTypeSystemTime:
		if len(n.Definitions) < 2 {
			return ErrSystemVersioningWrongPartitions
		}
	}

	for _, pd := range n.Definitions {
		// ensure the partition definition types match the methods,
		// e.g. RANGE partitions only allows VALUES LESS THAN
		if err := pd.Clause.Validate(n.Tp, len(n.ColumnNames)); err != nil {
			return err
		}
	}

	return nil
}

func (n *PartitionOptions) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("PARTITION BY ")
	if err := n.PartitionMethod.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore PartitionOptions.PartitionMethod")
	}

	if n.Num > 0 && len(n.Definitions) == 0 {
		ctx.WriteKeyWord(" PARTITIONS ")
		ctx.WritePlainf("%d", n.Num)
	}

	if n.Sub != nil {
		ctx.WriteKeyWord(" SUBPARTITION BY ")
		if err := n.Sub.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore PartitionOptions.Sub")
		}
		if n.Sub.Num > 0 {
			ctx.WriteKeyWord(" SUBPARTITIONS ")
			ctx.WritePlainf("%d", n.Sub.Num)
		}
	}

	if len(n.Definitions) > 0 {
		ctx.WritePlain(" (")
		for i, def := range n.Definitions {
			if i > 0 {
				ctx.WritePlain(",")
			}
			if err := def.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore PartitionOptions.Definitions[%d]", i)
			}
		}
		ctx.WritePlain(")")
	}

	return nil
}

func (n *PartitionOptions) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*PartitionOptions)
	if !n.PartitionMethod.acceptInPlace(v) {
		return n, false
	}
	if n.Sub != nil && !n.Sub.acceptInPlace(v) {
		return n, false
	}
	for _, def := range n.Definitions {
		if !def.acceptInPlace(v) {
			return n, false
		}
	}
	return v.Leave(n)
}

// RecoverTableStmt is a statement to recover dropped table.
type RecoverTableStmt struct {
	ddlNode

	JobID  int64
	Table  *TableName
	JobNum int64
}

// Restore implements Node interface.
func (n *RecoverTableStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RECOVER TABLE ")
	if n.JobID != 0 {
		ctx.WriteKeyWord("BY JOB ")
		ctx.WritePlainf("%d", n.JobID)
	} else {
		if err := n.Table.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing RecoverTableStmt Table")
		}
		if n.JobNum > 0 {
			ctx.WritePlainf(" %d", n.JobNum)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *RecoverTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*RecoverTableStmt)
	if n.Table != nil {
		node, ok := n.Table.Accept(v)
		if !ok {
			return n, false
		}
		n.Table = node.(*TableName)
	}
	return v.Leave(n)
}

// FlashBackTableStmt is a statement to restore a dropped/truncate table.
type FlashBackTableStmt struct {
	ddlNode

	Table   *TableName
	NewName string
}

// Restore implements Node interface.
func (n *FlashBackTableStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("FLASHBACK TABLE ")
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while splicing RecoverTableStmt Table")
	}
	if len(n.NewName) > 0 {
		ctx.WriteKeyWord(" TO ")
		ctx.WriteName(n.NewName)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *FlashBackTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*FlashBackTableStmt)
	if n.Table != nil {
		node, ok := n.Table.Accept(v)
		if !ok {
			return n, false
		}
		n.Table = node.(*TableName)
	}
	return v.Leave(n)
}

type AttributesSpec struct {
	node

	Attributes string
	Default    bool
}

func (n *AttributesSpec) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ATTRIBUTES")
	ctx.WritePlain("=")
	if n.Default {
		ctx.WriteKeyWord("DEFAULT")
		return nil
	}
	ctx.WriteString(n.Attributes)
	return nil
}

func (n *AttributesSpec) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AttributesSpec)
	return v.Leave(n)
}

type StatsOptionsSpec struct {
	node

	StatsOptions string
	Default      bool
}

func (n *StatsOptionsSpec) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("STATS_OPTIONS")
	ctx.WritePlain("=")
	if n.Default {
		ctx.WriteKeyWord("DEFAULT")
		return nil
	}
	ctx.WriteString(n.StatsOptions)
	return nil
}

func (n *StatsOptionsSpec) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*StatsOptionsSpec)
	return v.Leave(n)
}

// AlterPlacementPolicyStmt is a statement to alter placement policy option.
type AlterPlacementPolicyStmt struct {
	ddlNode

	PolicyName       model.CIStr
	IfExists         bool
	PlacementOptions []*PlacementOption
}

func (n *AlterPlacementPolicyStmt) Restore(ctx *format.RestoreCtx) error {
	if ctx.Flags.HasSkipPlacementRuleForRestoreFlag() {
		return nil
	}
	if ctx.Flags.HasTiDBSpecialCommentFlag() {
		return restorePlacementStmtInSpecialComment(ctx, n)
	}

	ctx.WriteKeyWord("ALTER PLACEMENT POLICY ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	ctx.WriteName(n.PolicyName.O)
	for i, option := range n.PlacementOptions {
		ctx.WritePlain(" ")
		if err := option.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing AlterPlacementPolicyStmt TableOption: [%v]", i)
		}
	}
	return nil
}

func (n *AlterPlacementPolicyStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterPlacementPolicyStmt)
	return v.Leave(n)
}

// AlterSequenceStmt is a statement to alter sequence option.
type AlterSequenceStmt struct {
	ddlNode

	// sequence name
	Name *TableName

	IfExists   bool
	SeqOptions []*SequenceOption
}

func (n *AlterSequenceStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER SEQUENCE ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	if err := n.Name.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore AlterSequenceStmt.Table")
	}
	for i, option := range n.SeqOptions {
		ctx.WritePlain(" ")
		if err := option.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing AlterSequenceStmt SequenceOption: [%v]", i)
		}
	}
	return nil
}

func (n *AlterSequenceStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterSequenceStmt)
	node, ok := n.Name.Accept(v)
	if !ok {
		return n, false
	}
	n.Name = node.(*TableName)
	return v.Leave(n)
}

func restorePlacementStmtInSpecialComment(ctx *format.RestoreCtx, n DDLNode) error {
	origFlags := ctx.Flags
	defer func() {
		ctx.Flags = origFlags
	}()

	ctx.Flags |= format.RestoreTiDBSpecialComment
	return ctx.WriteWithSpecialComments(tidb.FeatureIDPlacement, func() error {
		ctx.Flags &= ^format.RestoreTiDBSpecialComment
		return n.Restore(ctx)
	})
}
