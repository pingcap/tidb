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
	"github.com/pingcap/parser/terror"
	mysql "github.com/pingcap/tidb/errno"
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
	ErrColumnCantNull = terror.ClassTable.New(mysql.ErrBadNull, mysql.MySQLErrName[mysql.ErrBadNull])
	// ErrUnknownColumn is returned when accessing an unknown column.
	ErrUnknownColumn   = terror.ClassTable.New(mysql.ErrBadField, mysql.MySQLErrName[mysql.ErrBadField])
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
	// ErrSequenceHasRunOut returns when sequence has run out.
	ErrSequenceHasRunOut = terror.ClassTable.New(mysql.ErrSequenceRunOut, mysql.MySQLErrName[mysql.ErrSequenceRunOut])
	// ErrRowDoesNotMatchGivenPartitionSet returns when the destination partition conflict with the partition selection.
	ErrRowDoesNotMatchGivenPartitionSet = terror.ClassTable.NewStd(mysql.ErrRowDoesNotMatchGivenPartitionSet)
	// ErrWarnDataTruncated returns for data truncated warning.
	ErrWarnDataTruncated = terror.ClassTable.New(mysql.WarnDataTruncated, mysql.MySQLErrName[mysql.WarnDataTruncated])
)

// RecordIterFunc is used for low-level record iteration.
type RecordIterFunc func(h kv.Handle, rec []types.Datum, cols []*Column) (more bool, err error)

// AddRecordOpt contains the options will be used when adding a record.
type AddRecordOpt struct {
	CreateIdxOpt
	IsUpdate      bool
	ReserveAutoID int
}

// AddRecordOption is defined for the AddRecord() method of the Table interface.
type AddRecordOption interface {
	ApplyOn(*AddRecordOpt)
}

// WithReserveAutoIDHint tells the AddRecord operation to reserve a batch of auto ID in the stmtctx.
type WithReserveAutoIDHint int

// ApplyOn implements the AddRecordOption interface.
func (n WithReserveAutoIDHint) ApplyOn(opt *AddRecordOpt) {
	opt.ReserveAutoID = int(n)
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
	RowWithCols(ctx sessionctx.Context, h kv.Handle, cols []*Column) ([]types.Datum, error)

	// Row returns a row for all columns.
	Row(ctx sessionctx.Context, h kv.Handle) ([]types.Datum, error)

	// Cols returns the columns of the table which is used in select, including hidden columns.
	Cols() []*Column

	// VisibleCols returns the columns of the table which is used in select, excluding hidden columns.
	VisibleCols() []*Column

	// HiddenCols returns the hidden columns of the table.
	HiddenCols() []*Column

	// WritableCols returns columns of the table in writable states.
	// Writable states includes Public, WriteOnly, WriteOnlyReorganization.
	WritableCols() []*Column

	// FullHiddenColsAndVisibleCols returns hidden columns in all states and unhidden columns in public states.
	FullHiddenColsAndVisibleCols() []*Column

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
	RecordKey(h kv.Handle) kv.Key

	// AddRecord inserts a row which should contain only public columns
	AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...AddRecordOption) (recordID kv.Handle, err error)

	// UpdateRecord updates a row which should contain only writable columns.
	UpdateRecord(ctx context.Context, sctx sessionctx.Context, h kv.Handle, currData, newData []types.Datum, touched []bool) error

	// RemoveRecord removes a row in the table.
	RemoveRecord(ctx sessionctx.Context, h kv.Handle, r []types.Datum) error

	// Allocators returns all allocators.
	Allocators(ctx sessionctx.Context) autoid.Allocators

	// RebaseAutoID rebases the auto_increment ID base.
	// If allocIDs is true, it will allocate some IDs and save to the cache.
	// If allocIDs is false, it will not allocate IDs.
	RebaseAutoID(ctx sessionctx.Context, newBase int64, allocIDs bool, tp autoid.AllocatorType) error

	// Meta returns TableInfo.
	Meta() *model.TableInfo

	// Seek returns the handle greater or equal to h.
	Seek(ctx sessionctx.Context, h kv.Handle) (handle kv.Handle, found bool, err error)

	// Type returns the type of table
	Type() Type
}

// AllocAutoIncrementValue allocates an auto_increment value for a new row.
func AllocAutoIncrementValue(ctx context.Context, t Table, sctx sessionctx.Context) (int64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("table.AllocAutoIncrementValue", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	increment := sctx.GetSessionVars().AutoIncrementIncrement
	offset := sctx.GetSessionVars().AutoIncrementOffset
	_, max, err := t.Allocators(sctx).Get(autoid.RowIDAllocType).Alloc(t.Meta().ID, uint64(1), int64(increment), int64(offset))
	if err != nil {
		return 0, err
	}
	return max, err
}

// AllocBatchAutoIncrementValue allocates batch auto_increment value for rows, returning firstID, increment and err.
// The caller can derive the autoID by adding increment to firstID for N-1 times.
func AllocBatchAutoIncrementValue(ctx context.Context, t Table, sctx sessionctx.Context, N int) (firstID int64, increment int64, err error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("table.AllocBatchAutoIncrementValue", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	increment = int64(sctx.GetSessionVars().AutoIncrementIncrement)
	offset := int64(sctx.GetSessionVars().AutoIncrementOffset)
	min, max, err := t.Allocators(sctx).Get(autoid.RowIDAllocType).Alloc(t.Meta().ID, uint64(N), increment, offset)
	if err != nil {
		return min, max, err
	}
	// SeekToFirstAutoIDUnSigned seeks to first autoID. Because AutoIncrement always allocate from 1,
	// signed and unsigned value can be unified as the unsigned handle.
	nr := int64(autoid.SeekToFirstAutoIDUnSigned(uint64(min), uint64(increment), uint64(offset)))
	return nr, increment, nil
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
	GetPartitionByRow(sessionctx.Context, []types.Datum) (PhysicalTable, error)
}

// TableFromMeta builds a table.Table from *model.TableInfo.
// Currently, it is assigned to tables.TableFromMeta in tidb package's init function.
var TableFromMeta func(allocators autoid.Allocators, tblInfo *model.TableInfo) (Table, error)

// MockTableFromMeta only serves for test.
var MockTableFromMeta func(tableInfo *model.TableInfo) Table
