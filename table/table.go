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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
)

var (
	// errNoDefaultValue is used when insert a row, the column value is not given, and the column has not null flag
	// and it doesn't have a default value.
	errNoDefaultValue  = terror.ClassTable.New(codeNoDefaultValue, "field doesn't have a default value")
	errColumnCantNull  = terror.ClassTable.New(codeColumnCantNull, "column can not be null")
	errUnknownColumn   = terror.ClassTable.New(codeUnknownColumn, "unknown column")
	errDuplicateColumn = terror.ClassTable.New(codeDuplicateColumn, "duplicate column")

	errGetDefaultFailed = terror.ClassTable.New(codeGetDefaultFailed, "get default value fail")

	// ErrIndexOutBound returns for index column offset out of bound.
	ErrIndexOutBound = terror.ClassTable.New(codeIndexOutBound, "index column offset out of bound")
	// ErrUnsupportedOp returns for unsupported operation.
	ErrUnsupportedOp = terror.ClassTable.New(codeUnsupportedOp, "operation not supported")
	// ErrRowNotFound returns for row not found.
	ErrRowNotFound = terror.ClassTable.New(codeRowNotFound, "can not find the row")
	// ErrTableStateCantNone returns for table none state.
	ErrTableStateCantNone = terror.ClassTable.New(codeTableStateCantNone, "table can not be in none state")
	// ErrColumnStateCantNone returns for column none state.
	ErrColumnStateCantNone = terror.ClassTable.New(codeColumnStateCantNone, "column can not be in none state")
	// ErrColumnStateNonPublic returns for column non-public state.
	ErrColumnStateNonPublic = terror.ClassTable.New(codeColumnStateNonPublic, "can not use non-public column")
	// ErrIndexStateCantNone returns for index none state.
	ErrIndexStateCantNone = terror.ClassTable.New(codeIndexStateCantNone, "index can not be in none state")
	// ErrInvalidRecordKey returns for invalid record key.
	ErrInvalidRecordKey = terror.ClassTable.New(codeInvalidRecordKey, "invalid record key")
	// ErrInvalidUTF8Value returns for inserting invalid UTF8 value to a utf8 column.
	ErrInvalidUTF8Value = terror.ClassTable.New(codeInvalidUTF8Value, "invalid utf8 value")
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

	// WritableCols returns columns of the table in writable states.
	// Writable states includes Public, WriteOnly, WriteOnlyReorganization.
	WritableCols() []*Column

	// Indices returns the indices of the table.
	Indices() []Index

	// RecordPrefix returns the record key prefix.
	RecordPrefix() kv.Key

	// IndexPrefix returns the index key prefix.
	IndexPrefix() kv.Key

	// FirstKey returns the first key.
	FirstKey() kv.Key

	// RecordKey returns the key in KV storage for the row.
	RecordKey(h int64) kv.Key

	// AddRecord inserts a row into the table.
	AddRecord(ctx context.Context, r []types.Datum) (recordID int64, err error)

	// UpdateRecord updates a row in the table.
	UpdateRecord(ctx context.Context, h int64, currData []types.Datum, newData []types.Datum, touched map[int]bool) error

	// RemoveRecord removes a row in the table.
	RemoveRecord(ctx context.Context, h int64, r []types.Datum) error

	// AllocAutoID allocates an auto_increment ID for a new row.
	AllocAutoID() (int64, error)

	// Allocator returns Allocator.
	Allocator() autoid.Allocator

	// RebaseAutoID rebases the auto_increment ID base.
	// If allocIDs is true, it will allocate some IDs and save to the cache.
	// If allocIDs is false, it will not allocate IDs.
	RebaseAutoID(newBase int64, allocIDs bool) error

	// Meta returns TableInfo.
	Meta() *model.TableInfo

	// Seek returns the handle greater or equal to h.
	Seek(ctx context.Context, h int64) (handle int64, found bool, err error)
}

// TableFromMeta builds a table.Table from *model.TableInfo.
// Currently, it is assigned to tables.TableFromMeta in tidb package's init function.
var TableFromMeta func(alloc autoid.Allocator, tblInfo *model.TableInfo) (Table, error)

// MockTableFromMeta only serves for test.
var MockTableFromMeta func(tableInfo *model.TableInfo) Table

// Table error codes.
const (
	codeGetDefaultFailed     = 1
	codeIndexOutBound        = 2
	codeUnsupportedOp        = 3
	codeRowNotFound          = 4
	codeTableStateCantNone   = 5
	codeColumnStateCantNone  = 6
	codeColumnStateNonPublic = 7
	codeIndexStateCantNone   = 8
	codeInvalidRecordKey     = 9
	codeInvalidUTF8Value     = 10

	codeColumnCantNull  = 1048
	codeUnknownColumn   = 1054
	codeDuplicateColumn = 1110
	codeNoDefaultValue  = 1364
)

func init() {
	tableMySQLErrCodes := map[terror.ErrCode]uint16{
		codeColumnCantNull:  mysql.ErrBadNull,
		codeUnknownColumn:   mysql.ErrBadField,
		codeDuplicateColumn: mysql.ErrFieldSpecifiedTwice,
		codeNoDefaultValue:  mysql.ErrNoDefaultForField,
	}
	terror.ErrClassToMySQLCodes[terror.ClassTable] = tableMySQLErrCodes
}
