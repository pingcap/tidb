package ast

import (
	"github.com/pingcap/tidb/util/types"
)

var (
	_ Node = &CreateDatabaseStmt{}
	_ Node = &DropDatabaseStmt{}
	_ Node = &IndexColName{}
	_ Node = &ReferenceDef{}
	_ Node = &ConstraintOpt{}
	_ Node = &TableConstraint{}
	_ Node = &ColumnDef{}
	_ Node = &CreateTableOption{}
	_ Node = &CreateTableStmt{}
	_ Node = &DropTableStmt{}
	_ Node = &CreateIndexStmt{}
	_ Node = &DropTableStmt{}
	_ Node = &AlterTableOpt{}
	_ Node = &ColumnPosition{}
	_ Node = &AlterSpecification{}
	_ Node = &AlterTableStmt{}
)

// CharsetOpt is used for parsing charset option from SQL.
type CharsetOpt struct {
	Chs string
	Col string
}

// CreateDatabaseStmt is a statement to create a database.
// See: https://dev.mysql.com/doc/refman/5.7/en/create-database.html
type CreateDatabaseStmt struct {
	txtNode

	IfNotExists bool
	Name        string
	Opt         *CharsetOpt
}

// DropDatabaseStmt is a statement to drop a database and all tables in the database.
// See: https://dev.mysql.com/doc/refman/5.7/en/drop-database.html
type DropDatabaseStmt struct {
	txtNode

	IfExists bool
	Name     string
}

// IndexColName is used for parsing index column name from SQL.
type IndexColName struct {
	Column *ColumnRef
	Length int
}

// ReferenceDef is used for parsing foreign key reference option from SQL.
// See: http://dev.mysql.com/doc/refman/5.7/en/create-table-foreign-keys.html
type ReferenceDef struct {
	TableIdent    *TableRef
	IndexColNames []*IndexColName
}

// Constraints.
const (
	ConstrNoConstr = iota
	ConstrPrimaryKey
	ConstrForeignKey
	ConstrNotNull
	ConstrAutoIncrement
	ConstrDefaultValue
	ConstrUniq
	ConstrIndex
	ConstrUniqIndex
	ConstrKey
	ConstrUniqKey
	ConstrNull
	ConstrOnUpdate
	ConstrFulltext
	ConstrComment
)

// ConstraintOpt is used for parsing column constraint info from SQL.
type ConstraintOpt struct {
	Tp     int
	Bvalue bool
	Evalue Expression
}

// Table Options.
const (
	TblOptNone = iota
	TblOptEngine
	TblOptCharset
	TblOptCollate
	TblOptAutoIncrement
	TblOptComment
	TblOptAvgRowLength
	TblOptCheckSum
	TblOptCompression
	TblOptConnection
	TblOptPassword
	TblOptKeyBlockSize
	TblOptMaxRows
	TblOptMinRows
)

// TableConstraint is constraint for table definition.
type TableConstraint struct {
	Tp         int
	ConstrName string

	// Used for PRIMARY KEY, UNIQUE, ......
	Keys []*IndexColName

	// Used for foreign key.
	Refer *ReferenceDef
}

// ColumnDef is used for parsing column definition from SQL.
type ColumnDef struct {
	Name        string
	Tp          *types.FieldType
	Constraints []*ConstraintOpt
}

// CreateTableOption is the collection of table options.
type CreateTableOption struct {
	Engine        string
	Charset       string
	Collate       string
	AutoIncrement uint64
}

// CreateTableStmt is a statement to create a table.
// See: https://dev.mysql.com/doc/refman/5.7/en/create-table.html
type CreateTableStmt struct {
	txtNode

	IfNotExists bool
	Ident       TableIdent
	Cols        []*ColumnDef
	Constraints []*TableConstraint
	Opt         *CreateTableOption
}

// DropTableStmt is a statement to drop one or more tables.
// See: https://dev.mysql.com/doc/refman/5.7/en/drop-table.html
type DropTableStmt struct {
	txtNode

	IfExists  bool
	TableRefs []*TableRef
}

// CreateIndexStmt is a statement to create an index.
// See: https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type CreateIndexStmt struct {
	txtNode

	IndexName     string
	Table         *TableRef
	Unique        bool
	IndexColNames []*IndexColName
}

// DropIndexStmt is a statement to drop the index.
// See: https://dev.mysql.com/doc/refman/5.7/en/drop-index.html
type DropIndexStmt struct {
	IfExists  bool
	IndexName string

	Text string
}

// AlterTableOpt is used for parsing table option from SQL.
type AlterTableOpt struct {
	Tp        int
	StrValue  string
	UintValue uint64
}

// ColumnPosition Types
const (
	ColumnPositionNone int = iota
	ColumnPositionFirst
	ColumnPositionAfter
)

// ColumnPosition represent the position of the newly added column
type ColumnPosition struct {
	// ColumnPositionNone | ColumnPositionFirst | ColumnPositionAfter
	Type int
	// RelativeColumn is the column the newly added column after if type is ColumnPositionAfter
	RelativeColumn *ColumnRef
}

// AlterSpecification alter table specification
type AlterSpecification struct {
	Action     int
	Name       string
	Constraint *TableConstraint
	TableOpts  []*AlterTableOpt
	Column     *ColumnDef
	Position   *ColumnPosition
}

// AlterTableStmt is a statement to change the structure of a table.
// See: https://dev.mysql.com/doc/refman/5.7/en/alter-table.html
type AlterTableStmt struct {
	txtNode

	Ident TableIdent
	Specs []*AlterSpecification
}
