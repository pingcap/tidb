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

package model

import (
	"strings"
	"time"

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

// SchemaState is the state for schema elements.
type SchemaState byte

const (
	// StateNone means this schema element is absent and can't be used.
	StateNone SchemaState = iota
	// StateDeleteOnly means we can only delete items for this schema element.
	StateDeleteOnly
	// StateWriteOnly means we can use any write operation on this schema element,
	// but outer can't read the changed data.
	StateWriteOnly
	// StateWriteReorganization means we are re-organizating whole data after write only state.
	StateWriteReorganization
	// StateDeleteReorganization means we are re-organizating whole data after delete only state.
	StateDeleteReorganization
	// StatePublic means this schema element is ok for all write and read operations.
	StatePublic
)

// String implements fmt.Stringer interface.
func (s SchemaState) String() string {
	switch s {
	case StateDeleteOnly:
		return "delete only"
	case StateWriteOnly:
		return "write only"
	case StateWriteReorganization:
		return "write reorganization"
	case StateDeleteReorganization:
		return "delete reorganization"
	case StatePublic:
		return "public"
	default:
		return "none"
	}
}

// ColumnInfo provides meta data describing of a table column.
type ColumnInfo struct {
	ID                  int64               `json:"id"`
	Name                CIStr               `json:"name"`
	Offset              int                 `json:"offset"`
	OriginDefaultValue  interface{}         `json:"origin_default"`
	DefaultValue        interface{}         `json:"default"`
	GeneratedExprString string              `json:"generated_expr_string"`
	GeneratedStored     bool                `json:"generated_stored"`
	Dependences         map[string]struct{} `json:"dependences"`
	types.FieldType     `json:"type"`
	State               SchemaState `json:"state"`
	Comment             string      `json:"comment"`
}

// Clone clones ColumnInfo.
func (c *ColumnInfo) Clone() *ColumnInfo {
	nc := *c
	return &nc
}

// IsGenerated returns true if the column is generated column.
func (c *ColumnInfo) IsGenerated() bool {
	return len(c.GeneratedExprString) != 0
}

// ExtraHandleID is the column ID of column which we need to append to schema to occupy the handle's position
// for use of execution phase.
const ExtraHandleID = -1

// ExtraHandleName is the name of ExtraHandle Column.
var ExtraHandleName = NewCIStr("_rowid")

// TableInfo provides meta data describing a DB table.
type TableInfo struct {
	ID      int64  `json:"id"`
	Name    CIStr  `json:"name"`
	Charset string `json:"charset"`
	Collate string `json:"collate"`
	// Columns are listed in the order in which they appear in the schema.
	Columns     []*ColumnInfo `json:"cols"`
	Indices     []*IndexInfo  `json:"index_info"`
	ForeignKeys []*FKInfo     `json:"fk_info"`
	State       SchemaState   `json:"state"`
	PKIsHandle  bool          `json:"pk_is_handle"`
	Comment     string        `json:"comment"`
	AutoIncID   int64         `json:"auto_inc_id"`
	MaxColumnID int64         `json:"max_col_id"`
	MaxIndexID  int64         `json:"max_idx_id"`
	// UpdateTS is used to record the timestamp of updating the table's schema information.
	// These changing schema operations don't include 'truncate table' and 'rename table'.
	UpdateTS uint64 `json:"update_timestamp"`
	// OldSchemaID :
	// Because auto increment ID has schemaID as prefix,
	// We need to save original schemaID to keep autoID unchanged
	// while renaming a table from one database to another.
	// TODO: Remove it.
	// Now it only uses for compatibility with the old version that already uses this field.
	OldSchemaID int64 `json:"old_schema_id,omitempty"`

	// ShardRowIDBits specify if the implicit row ID is sharded.
	ShardRowIDBits uint64
}

// GetUpdateTime gets the table's updating time.
func (t *TableInfo) GetUpdateTime() time.Time {
	return tsConvert2Time(t.UpdateTS)
}

// GetDBID returns the schema ID that is used to create an allocator.
// TODO: Remove it after removing OldSchemaID.
func (t *TableInfo) GetDBID(dbID int64) int64 {
	if t.OldSchemaID != 0 {
		return t.OldSchemaID
	}
	return dbID
}

// Clone clones TableInfo.
func (t *TableInfo) Clone() *TableInfo {
	nt := *t
	nt.Columns = make([]*ColumnInfo, len(t.Columns))
	nt.Indices = make([]*IndexInfo, len(t.Indices))
	nt.ForeignKeys = make([]*FKInfo, len(t.ForeignKeys))

	for i := range t.Columns {
		nt.Columns[i] = t.Columns[i].Clone()
	}

	for i := range t.Indices {
		nt.Indices[i] = t.Indices[i].Clone()
	}

	for i := range t.ForeignKeys {
		nt.ForeignKeys[i] = t.ForeignKeys[i].Clone()
	}

	return &nt
}

// GetPkName will return the pk name if pk exists.
func (t *TableInfo) GetPkName() CIStr {
	if t.PKIsHandle {
		for _, colInfo := range t.Columns {
			if mysql.HasPriKeyFlag(colInfo.Flag) {
				return colInfo.Name
			}
		}
	}
	return CIStr{}
}

// GetPkColInfo gets the ColumnInfo of pk if exists.
// Make sure PkIsHandle checked before call this method.
func (t *TableInfo) GetPkColInfo() *ColumnInfo {
	for _, colInfo := range t.Columns {
		if mysql.HasPriKeyFlag(colInfo.Flag) {
			return colInfo
		}
	}
	return nil
}

// Cols returns the columns of the table in public state.
func (t *TableInfo) Cols() []*ColumnInfo {
	publicColumns := make([]*ColumnInfo, len(t.Columns))
	maxOffset := -1
	for _, col := range t.Columns {
		if col.State != StatePublic {
			continue
		}
		publicColumns[col.Offset] = col
		if maxOffset < col.Offset {
			maxOffset = col.Offset
		}
	}
	return publicColumns[0 : maxOffset+1]
}

// NewExtraHandleColInfo mocks a column info for extra handle column.
func NewExtraHandleColInfo() *ColumnInfo {
	colInfo := &ColumnInfo{
		ID:   ExtraHandleID,
		Name: ExtraHandleName,
	}
	colInfo.Flag = mysql.PriKeyFlag
	return colInfo
}

// ColumnIsInIndex checks whether c is included in any indices of t.
func (t *TableInfo) ColumnIsInIndex(c *ColumnInfo) bool {
	for _, index := range t.Indices {
		for _, column := range index.Columns {
			if column.Name.L == c.Name.L {
				return true
			}
		}
	}
	return false
}

// IndexColumn provides index column info.
type IndexColumn struct {
	Name   CIStr `json:"name"`   // Index name
	Offset int   `json:"offset"` // Index offset
	// Length of prefix when using column prefix
	// for indexing;
	// UnspecifedLength if not using prefix indexing
	Length int `json:"length"`
}

// Clone clones IndexColumn.
func (i *IndexColumn) Clone() *IndexColumn {
	ni := *i
	return &ni
}

// IndexType is the type of index
type IndexType int

// String implements Stringer interface.
func (t IndexType) String() string {
	switch t {
	case IndexTypeBtree:
		return "BTREE"
	case IndexTypeHash:
		return "HASH"
	default:
		return ""
	}
}

// IndexTypes
const (
	IndexTypeInvalid IndexType = iota
	IndexTypeBtree
	IndexTypeHash
)

// IndexInfo provides meta data describing a DB index.
// It corresponds to the statement `CREATE INDEX Name ON Table (Column);`
// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type IndexInfo struct {
	ID      int64          `json:"id"`
	Name    CIStr          `json:"idx_name"`   // Index name.
	Table   CIStr          `json:"tbl_name"`   // Table name.
	Columns []*IndexColumn `json:"idx_cols"`   // Index columns.
	Unique  bool           `json:"is_unique"`  // Whether the index is unique.
	Primary bool           `json:"is_primary"` // Whether the index is primary key.
	State   SchemaState    `json:"state"`
	Comment string         `json:"comment"`    // Comment
	Tp      IndexType      `json:"index_type"` // Index type: Btree or Hash
}

// Clone clones IndexInfo.
func (index *IndexInfo) Clone() *IndexInfo {
	ni := *index
	ni.Columns = make([]*IndexColumn, len(index.Columns))
	for i := range index.Columns {
		ni.Columns[i] = index.Columns[i].Clone()
	}
	return &ni
}

// HasPrefixIndex returns whether any columns of this index uses prefix length.
func (index *IndexInfo) HasPrefixIndex() bool {
	for _, ic := range index.Columns {
		if ic.Length != types.UnspecifiedLength {
			return true
		}
	}
	return false
}

// FKInfo provides meta data describing a foreign key constraint.
type FKInfo struct {
	ID       int64       `json:"id"`
	Name     CIStr       `json:"fk_name"`
	RefTable CIStr       `json:"ref_table"`
	RefCols  []CIStr     `json:"ref_cols"`
	Cols     []CIStr     `json:"cols"`
	OnDelete int         `json:"on_delete"`
	OnUpdate int         `json:"on_update"`
	State    SchemaState `json:"state"`
}

// Clone clones FKInfo.
func (fk *FKInfo) Clone() *FKInfo {
	nfk := *fk

	nfk.RefCols = make([]CIStr, len(fk.RefCols))
	nfk.Cols = make([]CIStr, len(fk.Cols))
	copy(nfk.RefCols, fk.RefCols)
	copy(nfk.Cols, fk.Cols)

	return &nfk
}

// DBInfo provides meta data describing a DB.
type DBInfo struct {
	ID      int64        `json:"id"`      // Database ID
	Name    CIStr        `json:"db_name"` // DB name.
	Charset string       `json:"charset"`
	Collate string       `json:"collate"`
	Tables  []*TableInfo `json:"-"` // Tables in the DB.
	State   SchemaState  `json:"state"`
}

// Clone clones DBInfo.
func (db *DBInfo) Clone() *DBInfo {
	newInfo := *db
	newInfo.Tables = make([]*TableInfo, len(db.Tables))
	for i := range db.Tables {
		newInfo.Tables[i] = db.Tables[i].Clone()
	}
	return &newInfo
}

// CIStr is case insensitive string.
type CIStr struct {
	O string `json:"O"` // Original string.
	L string `json:"L"` // Lower case string.
}

// String implements fmt.Stringer interface.
func (cis CIStr) String() string {
	return cis.O
}

// NewCIStr creates a new CIStr.
func NewCIStr(s string) (cs CIStr) {
	cs.O = s
	cs.L = strings.ToLower(s)
	return
}
