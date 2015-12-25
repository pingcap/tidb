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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/db"
)

// RecordIterFunc is used for low-level record iteration.
type RecordIterFunc func(h int64, rec []interface{}, cols []*column.Col) (more bool, err error)

// Table is used to retrieve and modify rows in table.
type Table interface {
	// IterRecords iterates records in the table and calls fn.
	IterRecords(retriever kv.Retriever, startKey kv.Key, cols []*column.Col, fn RecordIterFunc) error

	// RowWithCols returns a row that contains the given cols.
	RowWithCols(retriever kv.Retriever, h int64, cols []*column.Col) ([]interface{}, error)

	// Row returns a row for all columns.
	Row(ctx context.Context, h int64) ([]interface{}, error)

	// RemoveRowIndex removes an index of a row.
	RemoveRowIndex(rm kv.RetrieverMutator, h int64, vals []interface{}, idx *column.IndexedCol) error

	// BuildIndexForRow builds an index for a row.
	BuildIndexForRow(rm kv.RetrieverMutator, h int64, vals []interface{}, idx *column.IndexedCol) error

	// TableName returns table name.
	TableName() model.CIStr

	// Cols returns the columns of the table which is used in select.
	Cols() []*column.Col

	// Indices returns the indices of the table.
	Indices() []*column.IndexedCol

	// AddIndex appends the index to the table, for internal usage and test.
	AddIndex(*column.IndexedCol)

	// FindIndexByColName finds the index by column name.
	FindIndexByColName(name string) *column.IndexedCol

	// RecordPrefix returns the record key prefix.
	RecordPrefix() kv.Key

	// IndexPrefix returns the index key prefix.
	IndexPrefix() kv.Key

	// FirstKey returns the first key.
	FirstKey() kv.Key

	// RecordKey returns the key in KV storage for the column.
	RecordKey(h int64, col *column.Col) kv.Key

	// Truncate truncates the table.
	Truncate(rm kv.RetrieverMutator) (err error)

	// AddRecord inserts a row into the table. Is h is 0, it will alloc an unique id inside.
	AddRecord(ctx context.Context, r []interface{}, h int64) (recordID int64, err error)

	// UpdateRecord updates a row in the table.
	UpdateRecord(ctx context.Context, h int64, currData []interface{}, newData []interface{}, touched map[int]bool) error

	// RemoveRecord removes a row in the table.
	RemoveRecord(ctx context.Context, h int64, r []interface{}) error

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

	// SetColValue sets the column value.
	// If the column is untouched, we don't need to do this.
	SetColValue(rm kv.RetrieverMutator, key []byte, data interface{}) error

	// LockRow locks a row.
	LockRow(ctx context.Context, h int64) error
}

// TableFromMeta builds a table.Table from *model.TableInfo.
// Currently, it is assigned to tables.TableFromMeta in tidb package's init function.
var TableFromMeta func(alloc autoid.Allocator, tblInfo *model.TableInfo) (Table, error)

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
