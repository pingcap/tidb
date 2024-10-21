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

	"github.com/bits-and-blooms/bitset"
	mysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table/tblctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

// Type is used to distinguish between different tables that store data in different ways.
type Type int16

const (
	// NormalTable stores data in tikv, mocktikv and so on.
	NormalTable Type = iota
	// VirtualTable stores no data, just extract data from the memory struct.
	VirtualTable
	// ClusterTable contains the `VirtualTable` in the all cluster tidb nodes.
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

// commonMutateOpt is the common options for mutating a table.
type commonMutateOpt struct {
	ctx                        context.Context
	dupKeyCheck                DupKeyCheckMode
	pessimisticLazyDupKeyCheck PessimisticLazyDupKeyCheckMode
}

// Ctx returns the go context in the option
func (opt *commonMutateOpt) Ctx() context.Context {
	return opt.ctx
}

// DupKeyCheck returns the DupKeyCheckMode in the option
func (opt *commonMutateOpt) DupKeyCheck() DupKeyCheckMode {
	return opt.dupKeyCheck
}

// PessimisticLazyDupKeyCheck returns the PessimisticLazyDupKeyCheckMode in the option
func (opt *commonMutateOpt) PessimisticLazyDupKeyCheck() PessimisticLazyDupKeyCheckMode {
	return opt.pessimisticLazyDupKeyCheck
}

// AddRecordOpt contains the options will be used when adding a record.
type AddRecordOpt struct {
	commonMutateOpt
	isUpdate      bool
	reserveAutoID int
}

// NewAddRecordOpt creates a new AddRecordOpt with options.
func NewAddRecordOpt(opts ...AddRecordOption) *AddRecordOpt {
	opt := &AddRecordOpt{}
	for _, o := range opts {
		o.applyAddRecordOpt(opt)
	}
	return opt
}

// IsUpdate indicates whether the `AddRecord` operation is in an update statement.
func (opt *AddRecordOpt) IsUpdate() bool {
	return opt.isUpdate
}

// ReserveAutoID indicates the auto id count that should be reserved.
func (opt *AddRecordOpt) ReserveAutoID() int {
	return opt.reserveAutoID
}

// GetCreateIdxOpt creates a CreateIdxOpt.
func (opt *AddRecordOpt) GetCreateIdxOpt() *CreateIdxOpt {
	return &CreateIdxOpt{commonMutateOpt: opt.commonMutateOpt}
}

// AddRecordOption is defined for the AddRecord() method of the Table interface.
type AddRecordOption interface {
	applyAddRecordOpt(*AddRecordOpt)
}

// UpdateRecordOpt contains the options will be used when updating a record.
type UpdateRecordOpt struct {
	commonMutateOpt
	// skipWriteUntouchedIndices is an option to skip write untouched indices when updating a record.
	skipWriteUntouchedIndices bool
}

// NewUpdateRecordOpt creates a new UpdateRecordOpt with options.
func NewUpdateRecordOpt(opts ...UpdateRecordOption) *UpdateRecordOpt {
	opt := &UpdateRecordOpt{}
	for _, o := range opts {
		o.applyUpdateRecordOpt(opt)
	}
	return opt
}

// SkipWriteUntouchedIndices indicates whether to skip write untouched indices when updating a record.
func (opt *UpdateRecordOpt) SkipWriteUntouchedIndices() bool {
	return opt.skipWriteUntouchedIndices
}

// GetAddRecordOpt creates a AddRecordOpt.
func (opt *UpdateRecordOpt) GetAddRecordOpt() *AddRecordOpt {
	return &AddRecordOpt{commonMutateOpt: opt.commonMutateOpt}
}

// GetCreateIdxOpt creates a CreateIdxOpt.
func (opt *UpdateRecordOpt) GetCreateIdxOpt() *CreateIdxOpt {
	return &CreateIdxOpt{commonMutateOpt: opt.commonMutateOpt}
}

// UpdateRecordOption is defined for the UpdateRecord() method of the Table interface.
type UpdateRecordOption interface {
	applyUpdateRecordOpt(*UpdateRecordOpt)
}

// RemoveRecordOpt contains the options will be used when removing a record.
type RemoveRecordOpt struct {
	indexesLayoutOffset IndexesLayout
	columnSize          *ColumnsSizeHelper
}

// HasIndexesLayout returns whether the RemoveRecordOpt has indexes layout.
func (opt *RemoveRecordOpt) HasIndexesLayout() bool {
	return opt.indexesLayoutOffset != nil
}

// GetIndexesLayout returns the IndexesLayout of the RemoveRecordOpt.
func (opt *RemoveRecordOpt) GetIndexesLayout() IndexesLayout {
	return opt.indexesLayoutOffset
}

// GetIndexLayout returns the IndexRowLayoutOption for the specified index.
func (opt *RemoveRecordOpt) GetIndexLayout(indexID int64) IndexRowLayoutOption {
	return opt.indexesLayoutOffset[indexID]
}

// GetColumnSizeOpt returns the ColumnSizeOption of the RemoveRecordOpt.
func (opt *RemoveRecordOpt) GetColumnSizeOpt() *ColumnsSizeHelper {
	return opt.columnSize
}

// NewRemoveRecordOpt creates a new RemoveRecordOpt with options.
func NewRemoveRecordOpt(opts ...RemoveRecordOption) *RemoveRecordOpt {
	opt := &RemoveRecordOpt{}
	for _, o := range opts {
		o.applyRemoveRecordOpt(opt)
	}
	return opt
}

// RemoveRecordOption is defined for the RemoveRecord() method of the Table interface.
type RemoveRecordOption interface {
	applyRemoveRecordOpt(*RemoveRecordOpt)
}

// ExtraPartialRowOption is the combined one of IndexesLayout and ColumnSizeOption.
type ExtraPartialRowOption struct {
	IndexesRowLayout  IndexesLayout
	ColumnsSizeHelper *ColumnsSizeHelper
}

func (e *ExtraPartialRowOption) applyRemoveRecordOpt(opt *RemoveRecordOpt) {
	opt.indexesLayoutOffset = e.IndexesRowLayout
	opt.columnSize = e.ColumnsSizeHelper
}

// IndexRowLayoutOption is the option for index row layout.
// It is used to specify the order of the index columns in the row.
type IndexRowLayoutOption []int

// IndexesLayout is used to specify the layout of the indexes.
// It's mapping from index ID to the layout of the index.
type IndexesLayout map[int64]IndexRowLayoutOption

// GetIndexLayout returns the layout of the specified index.
func (idx IndexesLayout) GetIndexLayout(idxID int64) IndexRowLayoutOption {
	if idx == nil {
		return nil
	}
	return idx[idxID]
}

// ColumnsSizeHelper records the column size information.
// We're updating the total column size and total row size used in table statistics when doing DML.
// If the column is pruned when doing DML, we can't get the accurate size of the column. So we need the estimated avg size.
//   - If the column is not pruned, we can calculate its acurate size by the real data.
//   - Otherwise, we use the estimated avg size given by table statistics and field type information.
type ColumnsSizeHelper struct {
	// NotPruned is a bitset to record the columns that are not pruned.
	// The ith bit is 1 means the ith public column is not pruned.
	NotPruned *bitset.BitSet
	// If the column is pruned, we use the estimated avg size. They are stored by their ordinal in the table.
	// The ith element is the estimated size of the ith pruned public column.
	AvgSizes []float64
	// If the column is not pruned, we use the accurate size. They are stored by their ordinal in the pruned row.
	// The ith element is the position of the ith public column in the pruned row.
	PublicColsLayout []int
}

// CommonMutateOptFunc is a function to provide common options for mutating a table.
type CommonMutateOptFunc func(*commonMutateOpt)

// ApplyAddRecordOpt implements the AddRecordOption interface.
func (f CommonMutateOptFunc) applyAddRecordOpt(opt *AddRecordOpt) {
	f(&opt.commonMutateOpt)
}

// ApplyUpdateRecordOpt implements the UpdateRecordOption interface.
func (f CommonMutateOptFunc) applyUpdateRecordOpt(opt *UpdateRecordOpt) {
	f(&opt.commonMutateOpt)
}

// ApplyCreateIdxOpt implements the CreateIdxOption interface.
func (f CommonMutateOptFunc) applyCreateIdxOpt(opt *CreateIdxOpt) {
	f(&opt.commonMutateOpt)
}

// WithCtx returns a CommonMutateOptFunc.
// This option is used to pass context.Context.
func WithCtx(ctx context.Context) CommonMutateOptFunc {
	return func(opt *commonMutateOpt) {
		opt.ctx = ctx
	}
}

// WithReserveAutoIDHint tells the AddRecord operation to reserve a batch of auto ID in the stmtctx.
type WithReserveAutoIDHint int

// ApplyAddRecordOpt implements the AddRecordOption interface.
func (n WithReserveAutoIDHint) applyAddRecordOpt(opt *AddRecordOpt) {
	opt.reserveAutoID = int(n)
}

// IsUpdate is a defined value for AddRecordOptFunc.
var IsUpdate AddRecordOption = isUpdate{}

type isUpdate struct{}

func (i isUpdate) applyAddRecordOpt(opt *AddRecordOpt) {
	opt.isUpdate = true
}

// skipWriteUntouchedIndices implements UpdateRecordOption.
type skipWriteUntouchedIndices struct{}

func (skipWriteUntouchedIndices) applyUpdateRecordOpt(opt *UpdateRecordOpt) {
	opt.skipWriteUntouchedIndices = true
}

// SkipWriteUntouchedIndices is an option to skip write untouched options when updating a record.
// If there are no later queries in the transaction that need to read the untouched indices,
// you can use this option to improve performance.
// However, it is not safe to use it in an explicit txn or the updated table has some foreign key constraints.
// Because the following read operations in the same txn may not get the correct data with the current implementation.
// See:
// - https://github.com/pingcap/tidb/pull/12609
// - https://github.com/pingcap/tidb/issues/39419
var SkipWriteUntouchedIndices UpdateRecordOption = skipWriteUntouchedIndices{}

// DupKeyCheckMode indicates how to check the duplicated key when adding/updating a record/index.
type DupKeyCheckMode uint8

const (
	// DupKeyCheckInPlace indicates to check the duplicated key in place, both in the memory buffer and storage.
	DupKeyCheckInPlace DupKeyCheckMode = iota
	// DupKeyCheckLazy indicates to check the duplicated key lazily.
	// It means only checking the duplicated key in the memory buffer and checking keys in storage will be postponed
	// to the subsequence stage such as lock or commit phase.
	DupKeyCheckLazy
	// DupKeyCheckSkip indicates skipping the duplicated key check.
	DupKeyCheckSkip
)

// ApplyAddRecordOpt implements the AddRecordOption interface.
func (m DupKeyCheckMode) applyAddRecordOpt(opt *AddRecordOpt) {
	opt.dupKeyCheck = m
}

// ApplyUpdateRecordOpt implements the UpdateRecordOption interface.
func (m DupKeyCheckMode) applyUpdateRecordOpt(opt *UpdateRecordOpt) {
	opt.dupKeyCheck = m
}

// ApplyCreateIdxOpt implements the CreateIdxOption interface.
func (m DupKeyCheckMode) applyCreateIdxOpt(opt *CreateIdxOpt) {
	opt.dupKeyCheck = m
}

// PessimisticLazyDupKeyCheckMode only takes effect for pessimistic transaction
// when `DupKeyCheckMode` is set to `DupKeyCheckLazy`.
// It indicates how to check the duplicated key in store.
type PessimisticLazyDupKeyCheckMode uint8

const (
	// DupKeyCheckInAcquireLock indicates to check the duplicated key when acquiring the pessimistic lock.
	DupKeyCheckInAcquireLock PessimisticLazyDupKeyCheckMode = iota
	// DupKeyCheckInPrewrite indicates to check the duplicated key in the prewrite step when committing.
	// Please notice that if it is used, the duplicated key error may not be returned immediately after each statement,
	// because the duplicated key is not checked when acquiring the pessimistic lock.
	DupKeyCheckInPrewrite
)

// applyAddRecordOpt implements the AddRecordOption interface.
func (m PessimisticLazyDupKeyCheckMode) applyAddRecordOpt(opt *AddRecordOpt) {
	opt.pessimisticLazyDupKeyCheck = m
}

// applyUpdateRecordOpt implements the UpdateRecordOption interface.
func (m PessimisticLazyDupKeyCheckMode) applyUpdateRecordOpt(opt *UpdateRecordOpt) {
	opt.pessimisticLazyDupKeyCheck = m
}

// applyCreateIdxOpt implements the CreateIdxOption interface.
func (m PessimisticLazyDupKeyCheckMode) applyCreateIdxOpt(opt *CreateIdxOpt) {
	opt.pessimisticLazyDupKeyCheck = m
}

type columnAPI interface {
	// Cols returns the columns of the table which is used in select, including hidden columns.
	Cols() []*Column

	// VisibleCols returns the columns of the table which is used in select, excluding hidden columns.
	VisibleCols() []*Column

	// HiddenCols returns the hidden columns of the table.
	HiddenCols() []*Column

	// WritableCols returns columns of the table in writable states.
	// Writable states includes Public, WriteOnly, WriteOnlyReorganization.
	WritableCols() []*Column

	// DeletableCols returns columns of the table in deletable states.
	// Deletable states includes Public, WriteOnly, WriteOnlyReorganization, DeleteOnly, DeleteReorganization.
	DeletableCols() []*Column

	// FullHiddenColsAndVisibleCols returns hidden columns in all states and unhidden columns in public states.
	FullHiddenColsAndVisibleCols() []*Column
}

// MutateContext is used to when mutating a table.
type MutateContext = tblctx.MutateContext

// AllocatorContext is used to provide context for method `table.Allocators`.
type AllocatorContext = tblctx.AllocatorContext

// Table is used to retrieve and modify rows in table.
type Table interface {
	columnAPI

	// Indices returns the indices of the table.
	// The caller must be aware of that not all the returned indices are public.
	Indices() []Index
	DeletableIndices() []Index

	// WritableConstraint returns constraints of the table in writable states.
	WritableConstraint() []*Constraint

	// RecordPrefix returns the record key prefix.
	RecordPrefix() kv.Key
	// IndexPrefix returns the index key prefix.
	IndexPrefix() kv.Key

	// AddRecord inserts a row which should contain only public columns
	AddRecord(ctx MutateContext, txn kv.Transaction, r []types.Datum, opts ...AddRecordOption) (recordID kv.Handle, err error)

	// UpdateRecord updates a row which should contain only writable columns.
	UpdateRecord(ctx MutateContext, txn kv.Transaction, h kv.Handle, currData, newData []types.Datum, touched []bool, opts ...UpdateRecordOption) error

	// RemoveRecord removes a row in the table.
	RemoveRecord(ctx MutateContext, txn kv.Transaction, h kv.Handle, r []types.Datum, opts ...RemoveRecordOption) error

	// Allocators returns all allocators.
	Allocators(ctx AllocatorContext) autoid.Allocators

	// Meta returns TableInfo.
	Meta() *model.TableInfo

	// Type returns the type of table
	Type() Type

	// GetPartitionedTable returns nil if not partitioned
	GetPartitionedTable() PartitionedTable
}

func getIncrementAndOffset(vars *variable.SessionVars) (int, int) {
	increment := vars.AutoIncrementIncrement
	offset := vars.AutoIncrementOffset
	// When the value of auto_increment_offset is greater than that of auto_increment_increment,
	// the value of auto_increment_offset is ignored.
	// Ref https://dev.mysql.com/doc/refman/8.0/en/replication-options-source.html
	if offset > increment {
		offset = 1
	}
	return increment, offset
}

// AllocAutoIncrementValue allocates an auto_increment value for a new row.
func AllocAutoIncrementValue(ctx context.Context, t Table, sctx sessionctx.Context) (int64, error) {
	r, ctx := tracing.StartRegionEx(ctx, "table.AllocAutoIncrementValue")
	defer r.End()
	increment, offset := getIncrementAndOffset(sctx.GetSessionVars())
	alloc := t.Allocators(sctx.GetTableCtx()).Get(autoid.AutoIncrementType)
	_, maxv, err := alloc.Alloc(ctx, uint64(1), int64(increment), int64(offset))
	if err != nil {
		return 0, err
	}
	return maxv, err
}

// AllocBatchAutoIncrementValue allocates batch auto_increment value for rows, returning firstID, increment and err.
// The caller can derive the autoID by adding increment to firstID for N-1 times.
func AllocBatchAutoIncrementValue(ctx context.Context, t Table, sctx sessionctx.Context, N int) ( /* firstID */ int64 /* increment */, int64 /* err */, error) {
	increment1, offset := getIncrementAndOffset(sctx.GetSessionVars())
	alloc := t.Allocators(sctx.GetTableCtx()).Get(autoid.AutoIncrementType)
	minv, maxv, err := alloc.Alloc(ctx, uint64(N), int64(increment1), int64(offset))
	if err != nil {
		return minv, maxv, err
	}
	// SeekToFirstAutoIDUnSigned seeks to first autoID. Because AutoIncrement always allocate from 1,
	// signed and unsigned value can be unified as the unsigned handle.
	nr := int64(autoid.SeekToFirstAutoIDUnSigned(uint64(minv), uint64(increment1), uint64(offset)))
	return nr, int64(increment1), nil
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
	GetPartitionByRow(expression.EvalContext, []types.Datum) (PhysicalTable, error)
	GetPartitionIdxByRow(expression.EvalContext, []types.Datum) (int, error)
	GetAllPartitionIDs() []int64
	GetPartitionColumnIDs() []int64
	GetPartitionColumnNames() []pmodel.CIStr
	CheckForExchangePartition(ctx expression.EvalContext, pi *model.PartitionInfo, r []types.Datum, partID, ntID int64) error
}

// TableFromMeta builds a table.Table from *model.TableInfo.
// Currently, it is assigned to tables.TableFromMeta in tidb package's init function.
var TableFromMeta func(allocators autoid.Allocators, tblInfo *model.TableInfo) (Table, error)

// MockTableFromMeta only serves for test.
var MockTableFromMeta func(tableInfo *model.TableInfo) Table

// CachedTable is a Table, and it has a UpdateLockForRead() method
// UpdateLockForRead() according to the reasons for not meeting the read conditions, update the lock information,
// And at the same time reload data from the original table.
type CachedTable interface {
	Table

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

// CheckRowConstraint verify row check constraints.
func CheckRowConstraint(ctx exprctx.EvalContext, constraints []*Constraint, rowToCheck chunk.Row) error {
	for _, constraint := range constraints {
		ok, isNull, err := constraint.ConstraintExpr.EvalInt(ctx, rowToCheck)
		if err != nil {
			return err
		}
		if ok == 0 && !isNull {
			return ErrCheckConstraintViolated.FastGenByArgs(constraint.Name.O)
		}
	}
	return nil
}

// CheckRowConstraintWithDatum verify row check constraints.
// It is the same with `CheckRowConstraint` but receives a slice of `types.Datum` instead of `chunk.Row`.
func CheckRowConstraintWithDatum(ctx exprctx.EvalContext, constraints []*Constraint, row []types.Datum) error {
	if len(constraints) == 0 {
		return nil
	}
	return CheckRowConstraint(ctx, constraints, chunk.MutRowFromDatums(row).ToRow())
}
