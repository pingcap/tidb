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
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

// Type , the type of table, store data in different ways.
type Type int16

const (
	// NormalTable , store data in tikv, mocktikv and so on.
	NormalTable Type = iota
	// VirtualTable , store no data, just extract data from the memory struct.
	VirtualTable
	// ClusterTable , contain the `VirtualTable` in the all cluster tidb nodes.
	ClusterTable
)

// IsNormalTable checks whether the table is a normal table type.
func (tp Type) IsNormalTable() bool {
	return tp == NormalTable
}

// IsVirtualTable checks whether the table is a virtual table type.
func (tp Type) IsVirtualTable() bool {
	return tp == VirtualTable
}

// IsClusterTable checks whether the table is a cluster table type.
func (tp Type) IsClusterTable() bool {
	return tp == ClusterTable
}

const (
	// DirtyTableAddRow is the constant for dirty table operation type.
	DirtyTableAddRow = iota
	// DirtyTableDeleteRow is the constant for dirty table operation type.
	DirtyTableDeleteRow
)

var (
	// ErrColumnCantNull is used for inserting null to a not null column.
	ErrColumnCantNull  = terror.ClassTable.New(mysql.ErrBadNull, mysql.MySQLErrName[mysql.ErrBadNull])
	errUnknownColumn   = terror.ClassTable.New(mysql.ErrBadField, mysql.MySQLErrName[mysql.ErrBadField])
	errDuplicateColumn = terror.ClassTable.New(mysql.ErrFieldSpecifiedTwice, mysql.MySQLErrName[mysql.ErrFieldSpecifiedTwice])

	errGetDefaultFailed = terror.ClassTable.New(mysql.ErrFieldGetDefaultFailed, mysql.MySQLErrName[mysql.ErrFieldGetDefaultFailed])

	// ErrNoDefaultValue is used when insert a row, the column value is not given, and the column has not null flag
	// and it doesn't have a default value.
	ErrNoDefaultValue = terror.ClassTable.New(mysql.ErrNoDefaultForField, mysql.MySQLErrName[mysql.ErrNoDefaultForField])
	// ErrIndexOutBound returns for index column offset out of bound.
	ErrIndexOutBound = terror.ClassTable.New(mysql.ErrIndexOutBound, mysql.MySQLErrName[mysql.ErrIndexOutBound])
	// ErrUnsupportedOp returns for unsupported operation.
	ErrUnsupportedOp = terror.ClassTable.New(mysql.ErrUnsupportedOp, mysql.MySQLErrName[mysql.ErrUnsupportedOp])
	// ErrRowNotFound returns for row not found.
	ErrRowNotFound = terror.ClassTable.New(mysql.ErrRowNotFound, mysql.MySQLErrName[mysql.ErrRowNotFound])
	// ErrTableStateCantNone returns for table none state.
	ErrTableStateCantNone = terror.ClassTable.New(mysql.ErrTableStateCantNone, mysql.MySQLErrName[mysql.ErrTableStateCantNone])
	// ErrColumnStateCantNone returns for column none state.
	ErrColumnStateCantNone = terror.ClassTable.New(mysql.ErrColumnStateCantNone, mysql.MySQLErrName[mysql.ErrColumnStateCantNone])
	// ErrColumnStateNonPublic returns for column non-public state.
	ErrColumnStateNonPublic = terror.ClassTable.New(mysql.ErrColumnStateNonPublic, mysql.MySQLErrName[mysql.ErrColumnStateNonPublic])
	// ErrIndexStateCantNone returns for index none state.
	ErrIndexStateCantNone = terror.ClassTable.New(mysql.ErrIndexStateCantNone, mysql.MySQLErrName[mysql.ErrIndexStateCantNone])
	// ErrInvalidRecordKey returns for invalid record key.
	ErrInvalidRecordKey = terror.ClassTable.New(mysql.ErrInvalidRecordKey, mysql.MySQLErrName[mysql.ErrInvalidRecordKey])
	// ErrTruncatedWrongValueForField returns for truncate wrong value for field.
	ErrTruncatedWrongValueForField = terror.ClassTable.New(mysql.ErrTruncatedWrongValueForField, mysql.MySQLErrName[mysql.ErrTruncatedWrongValueForField])
	// ErrUnknownPartition returns unknown partition error.
	ErrUnknownPartition = terror.ClassTable.New(mysql.ErrUnknownPartition, mysql.MySQLErrName[mysql.ErrUnknownPartition])
	// ErrNoPartitionForGivenValue returns table has no partition for value.
	ErrNoPartitionForGivenValue = terror.ClassTable.New(mysql.ErrNoPartitionForGivenValue, mysql.MySQLErrName[mysql.ErrNoPartitionForGivenValue])
	// ErrLockOrActiveTransaction returns when execute unsupported statement in a lock session or an active transaction.
	ErrLockOrActiveTransaction = terror.ClassTable.New(mysql.ErrLockOrActiveTransaction, mysql.MySQLErrName[mysql.ErrLockOrActiveTransaction])
)

// RecordIterFunc is used for low-level record iteration.
type RecordIterFunc func(h int64, rec []types.Datum, cols []*Column) (more bool, err error)

// AddRecordOpt contains the options will be used when adding a record.
type AddRecordOpt struct {
	CreateIdxOpt
	IsUpdate bool
}

// AddRecordOption is defined for the AddRecord() method of the Table interface.
type AddRecordOption interface {
	ApplyOn(*AddRecordOpt)
}

// ApplyOn implements the AddRecordOption interface, so any CreateIdxOptFunc
// can be passed as the optional argument to the table.AddRecord method.
func (f CreateIdxOptFunc) ApplyOn(opt *AddRecordOpt) {
	f(&opt.CreateIdxOpt)
}

// IsUpdate is a defined value for AddRecordOptFunc.
var IsUpdate AddRecordOption = isUpdate{}

type isUpdate struct{}

func (i isUpdate) ApplyOn(opt *AddRecordOpt) {
	opt.IsUpdate = true
}

// Table is used to retrieve and modify rows in table.
type Table interface {
	// IterRecords iterates records in the table and calls fn.
	IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*Column, fn RecordIterFunc) error

	// RowWithCols returns a row that contains the given cols.
	RowWithCols(ctx sessionctx.Context, h int64, cols []*Column) ([]types.Datum, error)

	// Row returns a row for all columns.
	Row(ctx sessionctx.Context, h int64) ([]types.Datum, error)

	// Cols returns the columns of the table which is used in select, including hidden columns.
	Cols() []*Column

	// VisibleCols returns the columns of the table which is used in select, excluding hidden columns.
	VisibleCols() []*Column

	// HiddenCols returns the hidden columns of the table.
	HiddenCols() []*Column

	// WritableCols returns columns of the table in writable states.
	// Writable states includes Public, WriteOnly, WriteOnlyReorganization.
	WritableCols() []*Column

	// Indices returns the indices of the table.
	Indices() []Index

	// WritableIndices returns write-only and public indices of the table.
	WritableIndices() []Index

	// DeletableIndices returns delete-only, write-only and public indices of the table.
	DeletableIndices() []Index

	// RecordPrefix returns the record key prefix.
	RecordPrefix() kv.Key

	// IndexPrefix returns the index key prefix.
	IndexPrefix() kv.Key

	// FirstKey returns the first key.
	FirstKey() kv.Key

	// RecordKey returns the key in KV storage for the row.
	RecordKey(h int64) kv.Key

	// AddRecord inserts a row which should contain only public columns
	AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...AddRecordOption) (recordID int64, err error)

	// UpdateRecord updates a row which should contain only writable columns.
	UpdateRecord(ctx sessionctx.Context, h int64, currData, newData []types.Datum, touched []bool) error

	// RemoveRecord removes a row in the table.
	RemoveRecord(ctx sessionctx.Context, h int64, r []types.Datum) error

	// AllocHandle allocates a handle for a new row.
	AllocHandle(ctx sessionctx.Context) (int64, error)

	// AllocHandleIDs allocates multiple handle for rows.
	AllocHandleIDs(ctx sessionctx.Context, n uint64) (int64, int64, error)

	// Allocator returns Allocator.
	Allocator(ctx sessionctx.Context) autoid.Allocator

	// RebaseAutoID rebases the auto_increment ID base.
	// If allocIDs is true, it will allocate some IDs and save to the cache.
	// If allocIDs is false, it will not allocate IDs.
	RebaseAutoID(ctx sessionctx.Context, newBase int64, allocIDs bool) error

	// Meta returns TableInfo.
	Meta() *model.TableInfo

	// Seek returns the handle greater or equal to h.
	Seek(ctx sessionctx.Context, h int64) (handle int64, found bool, err error)

	// Type returns the type of table
	Type() Type
}

// AllocAutoIncrementValue allocates an auto_increment value for a new row.
func AllocAutoIncrementValue(ctx context.Context, t Table, sctx sessionctx.Context) (int64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("table.AllocAutoIncrementValue", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	_, max, err := t.Allocator(sctx).Alloc(t.Meta().ID, uint64(1))
	if err != nil {
		return 0, err
	}
	return max, err
}

// AllocBatchAutoIncrementValue allocates batch auto_increment value (min and max] for rows.
func AllocBatchAutoIncrementValue(ctx context.Context, t Table, sctx sessionctx.Context, N int) (int64, int64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("table.AllocBatchAutoIncrementValue", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	return t.Allocator(sctx).Alloc(t.Meta().ID, uint64(N))
}

// PhysicalTable is an abstraction for two kinds of table representation: partition or non-partitioned table.
// PhysicalID is a ID that can be used to construct a key ranges, all the data in the key range belongs to the corresponding PhysicalTable.
// For a non-partitioned table, its PhysicalID equals to its TableID; For a partition of a partitioned table, its PhysicalID is the partition's ID.
type PhysicalTable interface {
	Table
	GetPhysicalID() int64
}

// PartitionedTable is a Table, and it has a GetPartition() method.
// GetPartition() gets the partition from a partition table by a physical table ID,
type PartitionedTable interface {
	Table
	GetPartition(physicalID int64) PhysicalTable
	GetPartitionByRow(sessionctx.Context, []types.Datum) (Table, error)
}

// TableFromMeta builds a table.Table from *model.TableInfo.
// Currently, it is assigned to tables.TableFromMeta in tidb package's init function.
var TableFromMeta func(alloc autoid.Allocator, tblInfo *model.TableInfo) (Table, error)

// MockTableFromMeta only serves for test.
var MockTableFromMeta func(tableInfo *model.TableInfo) Table

// Slice is used for table sorting.
type Slice []Table

func (s Slice) Len() int { return len(s) }

func (s Slice) Less(i, j int) bool {
	return s[i].Meta().Name.O < s[j].Meta().Name.O
}

func (s Slice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func init() {
	tableMySQLErrCodes := map[terror.ErrCode]uint16{
		mysql.ErrBadNull:                     mysql.ErrBadNull,
		mysql.ErrBadField:                    mysql.ErrBadField,
		mysql.ErrFieldSpecifiedTwice:         mysql.ErrFieldSpecifiedTwice,
		mysql.ErrNoDefaultForField:           mysql.ErrNoDefaultForField,
		mysql.ErrTruncatedWrongValueForField: mysql.ErrTruncatedWrongValueForField,
		mysql.ErrUnknownPartition:            mysql.ErrUnknownPartition,
		mysql.ErrNoPartitionForGivenValue:    mysql.ErrNoPartitionForGivenValue,
		mysql.ErrLockOrActiveTransaction:     mysql.ErrLockOrActiveTransaction,
		mysql.ErrIndexOutBound:               mysql.ErrIndexOutBound,
		mysql.ErrColumnStateNonPublic:        mysql.ErrColumnStateNonPublic,
		mysql.ErrFieldGetDefaultFailed:       mysql.ErrFieldGetDefaultFailed,
		mysql.ErrUnsupportedOp:               mysql.ErrUnsupportedOp,
		mysql.ErrRowNotFound:                 mysql.ErrRowNotFound,
		mysql.ErrTableStateCantNone:          mysql.ErrTableStateCantNone,
		mysql.ErrColumnStateCantNone:         mysql.ErrColumnStateCantNone,
		mysql.ErrIndexStateCantNone:          mysql.ErrIndexStateCantNone,
		mysql.ErrInvalidRecordKey:            mysql.ErrInvalidRecordKey,
	}
	terror.ErrClassToMySQLCodes[terror.ClassTable] = tableMySQLErrCodes
}
