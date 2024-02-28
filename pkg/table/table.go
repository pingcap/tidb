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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package table

import (
	"context"
	"time"

	mysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table/briefapi"
	tbctx "github.com/pingcap/tidb/pkg/table/context"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

var (
	// ErrColumnCantNull is used for inserting null to a not null column.
	ErrColumnCantNull = dbterror.ClassTable.NewStd(mysql.ErrBadNull)
	// ErrUnknownColumn is returned when accessing an unknown column.
	ErrUnknownColumn   = dbterror.ClassTable.NewStd(mysql.ErrBadField)
	errDuplicateColumn = dbterror.ClassTable.NewStd(mysql.ErrFieldSpecifiedTwice)

	// ErrWarnNullToNotnull is like ErrColumnCantNull but it's used in LOAD DATA
	ErrWarnNullToNotnull = dbterror.ClassExecutor.NewStd(mysql.ErrWarnNullToNotnull)

	errGetDefaultFailed = dbterror.ClassTable.NewStd(mysql.ErrFieldGetDefaultFailed)

	// ErrNoDefaultValue is used when insert a row, the column value is not given, and the column has not null flag
	// and it doesn't have a default value.
	ErrNoDefaultValue = dbterror.ClassTable.NewStd(mysql.ErrNoDefaultForField)
	// ErrIndexOutBound returns for index column offset out of bound.
	ErrIndexOutBound = dbterror.ClassTable.NewStd(mysql.ErrIndexOutBound)
	// ErrUnsupportedOp returns for unsupported operation.
	ErrUnsupportedOp = dbterror.ClassTable.NewStd(mysql.ErrUnsupportedOp)
	// ErrRowNotFound returns for row not found.
	ErrRowNotFound = dbterror.ClassTable.NewStd(mysql.ErrRowNotFound)
	// ErrTableStateCantNone returns for table none state.
	ErrTableStateCantNone = dbterror.ClassTable.NewStd(mysql.ErrTableStateCantNone)
	// ErrColumnStateCantNone returns for column none state.
	ErrColumnStateCantNone = dbterror.ClassTable.NewStd(mysql.ErrColumnStateCantNone)
	// ErrColumnStateNonPublic returns for column non-public state.
	ErrColumnStateNonPublic = dbterror.ClassTable.NewStd(mysql.ErrColumnStateNonPublic)
	// ErrIndexStateCantNone returns for index none state.
	ErrIndexStateCantNone = dbterror.ClassTable.NewStd(mysql.ErrIndexStateCantNone)
	// ErrInvalidRecordKey returns for invalid record key.
	ErrInvalidRecordKey = dbterror.ClassTable.NewStd(mysql.ErrInvalidRecordKey)
	// ErrTruncatedWrongValueForField returns for truncate wrong value for field.
	ErrTruncatedWrongValueForField = dbterror.ClassTable.NewStd(mysql.ErrTruncatedWrongValueForField)
	// ErrUnknownPartition returns unknown partition error.
	ErrUnknownPartition = dbterror.ClassTable.NewStd(mysql.ErrUnknownPartition)
	// ErrNoPartitionForGivenValue returns table has no partition for value.
	ErrNoPartitionForGivenValue = dbterror.ClassTable.NewStd(mysql.ErrNoPartitionForGivenValue)
	// ErrLockOrActiveTransaction returns when execute unsupported statement in a lock session or an active transaction.
	ErrLockOrActiveTransaction = dbterror.ClassTable.NewStd(mysql.ErrLockOrActiveTransaction)
	// ErrSequenceHasRunOut returns when sequence has run out.
	ErrSequenceHasRunOut = dbterror.ClassTable.NewStd(mysql.ErrSequenceRunOut)
	// ErrRowDoesNotMatchGivenPartitionSet returns when the destination partition conflict with the partition selection.
	ErrRowDoesNotMatchGivenPartitionSet = dbterror.ClassTable.NewStd(mysql.ErrRowDoesNotMatchGivenPartitionSet)
	// ErrTempTableFull returns a table is full error, it's used by temporary table now.
	ErrTempTableFull = dbterror.ClassTable.NewStd(mysql.ErrRecordFileFull)
	// ErrOptOnCacheTable returns when exec unsupported opt at cache mode
	ErrOptOnCacheTable = dbterror.ClassDDL.NewStd(mysql.ErrOptOnCacheTable)
	// ErrCheckConstraintViolated return when check constraint is violated.
	ErrCheckConstraintViolated = dbterror.ClassTable.NewStd(mysql.ErrCheckConstraintViolated)
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

// MutateContext is used to when mutating a table.
type MutateContext = tbctx.MutateContext

// AllocatorContext is used to provide context for method `table.Allocators`.
type AllocatorContext = tbctx.AllocatorContext

// Type is an alias of briefapi.Type
type Type = briefapi.Type

// Table is an alias of briefapi.Table
type Table = briefapi.Table

// Mutator is used to retrieve and modify rows in table.
type Mutator interface {
	Table

	// AddRecord inserts a row which should contain only public columns
	AddRecord(ctx MutateContext, r []types.Datum, opts ...AddRecordOption) (recordID kv.Handle, err error)

	// UpdateRecord updates a row which should contain only writable columns.
	UpdateRecord(gctx context.Context, ctx MutateContext, h kv.Handle, currData, newData []types.Datum, touched []bool) error

	// RemoveRecord removes a row in the table.
	RemoveRecord(ctx MutateContext, h kv.Handle, r []types.Datum) error

	IndexMutators() []IndexMutator
}

// AllocAutoIncrementValue allocates an auto_increment value for a new row.
func AllocAutoIncrementValue(ctx context.Context, t Mutator, sctx sessionctx.Context) (int64, error) {
	r, ctx := tracing.StartRegionEx(ctx, "table.AllocAutoIncrementValue")
	defer r.End()
	increment := sctx.GetSessionVars().AutoIncrementIncrement
	offset := sctx.GetSessionVars().AutoIncrementOffset
	alloc := t.Allocators(sctx.GetTableCtx()).Get(autoid.AutoIncrementType)
	_, max, err := alloc.Alloc(ctx, uint64(1), int64(increment), int64(offset))
	if err != nil {
		return 0, err
	}
	return max, err
}

// AllocBatchAutoIncrementValue allocates batch auto_increment value for rows, returning firstID, increment and err.
// The caller can derive the autoID by adding increment to firstID for N-1 times.
func AllocBatchAutoIncrementValue(ctx context.Context, t Mutator, sctx sessionctx.Context, N int) (firstID int64, increment int64, err error) {
	increment = int64(sctx.GetSessionVars().AutoIncrementIncrement)
	offset := int64(sctx.GetSessionVars().AutoIncrementOffset)
	alloc := t.Allocators(sctx.GetTableCtx()).Get(autoid.AutoIncrementType)
	min, max, err := alloc.Alloc(ctx, uint64(N), increment, offset)
	if err != nil {
		return min, max, err
	}
	// SeekToFirstAutoIDUnSigned seeks to first autoID. Because AutoIncrement always allocate from 1,
	// signed and unsigned value can be unified as the unsigned handle.
	nr := int64(autoid.SeekToFirstAutoIDUnSigned(uint64(min), uint64(increment), uint64(offset)))
	return nr, increment, nil
}

// PhysicalTable is an alias of briefapi.PhysicalTable
type PhysicalTable = briefapi.PhysicalTable

type PhysicalTableMutator interface {
	Mutator
	PhysicalTable
}

// PartitionedTable is an alias of briefapi.PartitionedTable
type PartitionedTable = briefapi.PartitionedTable

// PartitionedTableMutator is used to mutate a partitioned table.
type PartitionedTableMutator interface {
	Mutator
	PartitionedTable
	GetPartitionForMutate(pid int64) PhysicalTableMutator
	GetPartitionByRow(expression.BuildContext, []types.Datum) (PhysicalTableMutator, error)
	CheckForExchangePartition(ctx expression.BuildContext, pi *model.PartitionInfo, r []types.Datum, partID, ntID int64) error
}

// TableFromMeta builds a table.Table from *model.TableInfo.
// Currently, it is assigned to tables.TableFromMeta in tidb package's init function.
var TableFromMeta func(allocators autoid.Allocators, tblInfo *model.TableInfo) (Mutator, error)

// MockTableFromMeta only serves for test.
var MockTableFromMeta func(tableInfo *model.TableInfo) Mutator

// CachedTable is a Table, and it has a UpdateLockForRead() method
// UpdateLockForRead() according to the reasons for not meeting the read conditions, update the lock information,
// And at the same time reload data from the original table.
type CachedTable interface {
	Mutator

	Init(exec sqlexec.SQLExecutor) error

	// TryReadFromCache checks if the cache table is readable.
	TryReadFromCache(ts uint64, leaseDuration time.Duration) (kv.MemBuffer, bool)

	// UpdateLockForRead if you cannot meet the conditions of the read buffer,
	// you need to update the lock information and read the data from the original table
	UpdateLockForRead(ctx context.Context, store kv.Storage, ts uint64, leaseDuration time.Duration)

	// WriteLockAndKeepAlive first obtain the write lock, then it renew the lease to keep the lock alive.
	// 'exit' is a channel to tell the keep alive goroutine to exit.
	// The result is sent to the 'wg' channel.
	WriteLockAndKeepAlive(ctx context.Context, exit chan struct{}, leasePtr *uint64, wg chan error)
}
