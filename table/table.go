// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package table

import (
	"fmt"

	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/db"
)

// RecordIterFunc is used for low-level record iteration.
type RecordIterFunc func(h int64, rec []interface{}, cols []*column.Col) (more bool, err error)

// Table is used to retrieve and modify rows in table.
type Table interface {
	// IterRecords iterates records in the table and call fn.
	IterRecords(ctx context.Context, startKey string, cols []*column.Col, fn RecordIterFunc) error

	// RowWithCols returns a row that contains the given cols.
	RowWithCols(ctx context.Context, h int64, cols []*column.Col) ([]interface{}, error)

	// Row returns a row for all columns.
	Row(ctx context.Context, h int64) ([]interface{}, error)

	// RemoveRow removes the row of handle h.
	RemoveRow(ctx context.Context, h int64) error

	// RemoveRowIndex removes an index of a row.
	RemoveRowIndex(ctx context.Context, h int64, vals []interface{}, idx *column.IndexedCol) error

	// RemoveRowAllIndex removes all the indices of a row.
	RemoveRowAllIndex(ctx context.Context, h int64, rec []interface{}) error

	// BuildIndexForRow builds an index for a row.
	BuildIndexForRow(ctx context.Context, h int64, vals []interface{}, idx *column.IndexedCol) error

	// TableName returns table name.
	TableName() model.CIStr

	// Cols returns the columns of the table.
	Cols() []*column.Col

	// Indices returns the indices of the table.
	Indices() []*column.IndexedCol

	// AddIndex appends the index to the table, for internal usage and test.
	AddIndex(*column.IndexedCol)

	// FindIndexByColName finds the index by column name.
	FindIndexByColName(name string) *column.IndexedCol

	// KeyPrefix returns the key prefix string.
	KeyPrefix() string

	// IndexPrefix returns the index prefix string.
	IndexPrefix() string

	// FirstKey returns the first key string.
	FirstKey() string

	// RecordKey returns the key in KV storage for the column.
	RecordKey(h int64, col *column.Col) []byte

	// Truncate truncates the table.
	Truncate(ctx context.Context) (err error)

	// AddRecord inserts a row into the table.
	AddRecord(ctx context.Context, r []interface{}) (recordID int64, err error)

	// UpdateRecord updates a row in the table.
	UpdateRecord(ctx context.Context, h int64, currData []interface{}, newData []interface{}, touched []bool) error

	// TableID returns the ID of the table.
	TableID() int64

	// EncodeValue encodes a go value to bytes.
	EncodeValue(raw interface{}) ([]byte, error)

	// DecodeValue decodes bytes to go value.
	DecodeValue(data []byte, col *column.Col) (interface{}, error)

	// AllocAutoID allocates an auto_increment ID for a new row.
	AllocAutoID() (int64, error)

	// Meta returns TableInfo.
	Meta() *model.TableInfo

	// LockRow locks a row.
	// If update is true, set row lock key to current txn.
	LockRow(ctx context.Context, h int64, update bool) error

	// ColumnOffset gets the column offset in whole columns with column name.
	ColumnOffset(name string) (int, error)
}

// TableFromMeta builds a table.Table from *model.TableInfo.
// Currently, it is assigned to tables.TableFromMeta in tidb package's init function.
var TableFromMeta func(schema string, alloc autoid.Allocator, tblInfo *model.TableInfo) Table

// Ident is the table identifier composed of schema name and table name.
// TODO: Move out
type Ident struct {
	Schema model.CIStr
	Name   model.CIStr
}

// Full returns an Ident which set schema to the current schema if it is empty.
func (i Ident) Full(ctx context.Context) (full Ident) {
	full.Name = i.Name
	full.Schema = i.Schema
	if i.Schema.O != "" {
		full.Schema = i.Schema
	} else {
		full.Schema = model.NewCIStr(db.GetCurrentSchema(ctx))
	}
	return
}

// String implements fmt.Stringer interface
func (i Ident) String() string {
	if i.Schema.O == "" {
		return i.Name.O
	}
	return fmt.Sprintf("%s.%s", i.Schema, i.Name)
}
