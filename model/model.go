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

	"github.com/pingcap/tidb/util/types"
)

// ColumnInfo provides meta data describing of a table column.
type ColumnInfo struct {
	ID              int64       `json:"id"`
	Name            CIStr       `json:"name"` // Column Name.
	Offset          int         `json:"offset"`
	DefaultValue    interface{} `json:"default"` // Default Value.
	types.FieldType `json:"type"`
}

// TableInfo provides meta data describing a DB table.
type TableInfo struct {
	// Table name.
	ID      int64  `json:"id"`
	Name    CIStr  `json:"name"`
	Charset string `json:"charset"`
	Collate string `json:"collate"`
	// Columns are listed in the order in which they appear in the schema.
	Columns []*ColumnInfo `json:"cols"`
	Indices []*IndexInfo  `json:"index_info"`
}

// IndexColumn provides index column info.
type IndexColumn struct {
	Name   CIStr `json:"name"`   // Index name
	Offset int   `json:"offset"` // Index offset
	Length int   `json:"length"` // Index length
}

// IndexInfo provides meta data describing a DB index.
// It corresponds to the statement `CREATE INDEX Name ON Table (Column);`
// See: https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type IndexInfo struct {
	Name    CIStr          `json:"idx_name"`   // Index name.
	Table   CIStr          `json:"tbl_name"`   // Table name.
	Columns []*IndexColumn `json:"idx_cols"`   // Index columns.
	Unique  bool           `json:"is_unique"`  // Whether the index is unique.
	Primary bool           `json:"is_primary"` // Whether the index is primary key.
}

// DBInfo provides meta data describing a DB.
type DBInfo struct {
	ID      int64        `json:"id"`      // Database ID
	Name    CIStr        `json:"db_name"` // DB name.
	Charset string       `json:"charset"`
	Collate string       `json:"collate"`
	Tables  []*TableInfo `json:"tables"` // Tables in the DB.
}

// CIStr is case insensitve string.
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
