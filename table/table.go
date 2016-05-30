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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
)

var (
	// ErrNoDefaultValue is used when insert a row, the column value is not given, and the column has not null flag
	// and it doesn't have a default value.
	ErrNoDefaultValue = terror.ClassTable.New(codeNoDefaultValue, "Field doesn't have a default value")
)

const (
	codeNoDefaultValue = 1
)

// RecordIterFunc is used for low-level record iteration.
type RecordIterFunc func(h int64, rec []types.Datum, cols []*Column) (more bool, err error)

// Table is used to retrieve and modify rows in table.
type Table interface {
	// IterRecords iterates records in the table and calls fn.
	IterRecords(ctx context.Context, startKey kv.Key, cols []*Column, fn RecordIterFunc) error

	// RowWithCols returns a row that contains the given cols.
	RowWithCols(ctx context.Context, h int64, cols []*Column) ([]types.Datum, error)

	// Row returns a row for all columns.
	Row(ctx context.Context, h int64) ([]types.Datum, error)

	// Cols returns the columns of the table which is used in select.
	Cols() []*Column

	// Indices returns the indices of the table.
	Indices() []*IndexedColumn

	// RecordPrefix returns the record key prefix.
	RecordPrefix() kv.Key

	// IndexPrefix returns the index key prefix.
	IndexPrefix() kv.Key

	// FirstKey returns the first key.
	FirstKey() kv.Key

	// RecordKey returns the key in KV storage for the column.
	RecordKey(h int64, col *Column) kv.Key

	// Truncate truncates the table.
	Truncate(ctx context.Context) (err error)

	// AddRecord inserts a row into the table.
	AddRecord(ctx context.Context, r []types.Datum) (recordID int64, err error)

	// UpdateRecord updates a row in the table.
	UpdateRecord(ctx context.Context, h int64, currData []types.Datum, newData []types.Datum, touched map[int]bool) error

	// RemoveRecord removes a row in the table.
	RemoveRecord(ctx context.Context, h int64, r []types.Datum) error

	// AllocAutoID allocates an auto_increment ID for a new row.
	AllocAutoID() (int64, error)

	// RebaseAutoID rebases the auto_increment ID base.
	// If allocIDs is true, it will allocate some IDs and save to the cache.
	// If allocIDs is false, it will not allocate IDs.
	RebaseAutoID(newBase int64, allocIDs bool) error

	// Meta returns TableInfo.
	Meta() *model.TableInfo

	// LockRow locks a row.
	LockRow(ctx context.Context, h int64, forRead bool) error

	// Seek returns the handle greater or equal to h.
	Seek(ctx context.Context, h int64) (handle int64, found bool, err error)
}

// TableFromMeta builds a table.Table from *model.TableInfo.
// Currently, it is assigned to tables.TableFromMeta in tidb package's init function.
var TableFromMeta func(alloc autoid.Allocator, tblInfo *model.TableInfo) (Table, error)

// MockTableFromMeta only serves for test.
var MockTableFromMeta func(tableInfo *model.TableInfo) Table
