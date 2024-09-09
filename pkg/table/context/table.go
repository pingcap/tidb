// Copyright 2024 PingCAP, Inc.
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

package context

import (
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/tableutil"
	"github.com/pingcap/tipb/go-binlog"
)

var _ AllocatorContext = MutateContext(nil)

// RowEncodingConfig is used to provide config for row encoding.
type RowEncodingConfig struct {
	// IsRowLevelChecksumEnabled indicates whether the row level checksum is enabled.
	IsRowLevelChecksumEnabled bool
	// RowEncoder is used to encode a row
	RowEncoder *rowcodec.Encoder
}

// BinlogSupport is used for binlog operations
type BinlogSupport interface {
	// GetBinlogMutation returns a `binlog.TableMutation` object for a table.
	GetBinlogMutation(tblID int64) *binlog.TableMutation
}

// StatisticsSupport is used for statistics update operations.
type StatisticsSupport interface {
	// UpdatePhysicalTableDelta updates the physical table delta.
	UpdatePhysicalTableDelta(physicalTableID int64, delta int64, count int64, cols variable.DeltaCols)
}

// CachedTableSupport is used for cached table operations
type CachedTableSupport interface {
	// AddCachedTableHandleToTxn adds a cached handle to the current transaction
	// to handle cached table when committing txn.
	// The handle argument should implement `table.CachedTable` interface, but here is `any` to avoid import cycle.
	AddCachedTableHandleToTxn(tableID int64, handle any)
}

// TemporaryTableHandler is used by `table.Table` to handle temporary table.
type TemporaryTableHandler struct {
	tblInTxn tableutil.TempTable
	data     variable.TemporaryTableData
}

// NewTemporaryTableHandler creates a new TemporaryTableHandler
func NewTemporaryTableHandler(tbl tableutil.TempTable, data variable.TemporaryTableData) TemporaryTableHandler {
	return TemporaryTableHandler{
		tblInTxn: tbl,
		data:     data,
	}
}

// Meta returns the meta
func (h *TemporaryTableHandler) Meta() *model.TableInfo {
	return h.tblInTxn.GetMeta()
}

// GetDirtySize returns the size of dirty data in txn of the temporary table
func (h *TemporaryTableHandler) GetDirtySize() int64 {
	return h.tblInTxn.GetSize()
}

// GetCommittedSize returns the committed data size of the temporary table
func (h *TemporaryTableHandler) GetCommittedSize() int64 {
	if h.data == nil {
		return 0
	}
	return h.data.GetTableSize(h.tblInTxn.GetMeta().ID)
}

// UpdateTxnDeltaSize updates the size of dirty data statistics in txn of the temporary table
func (h *TemporaryTableHandler) UpdateTxnDeltaSize(delta int) {
	h.tblInTxn.SetSize(h.tblInTxn.GetSize() + int64(delta))
}

// TemporaryTableSupport is used for temporary table operations
type TemporaryTableSupport interface {
	// GetTemporaryTableSizeLimit returns the size limit of a temporary table.
	GetTemporaryTableSizeLimit() int64
	// AddTemporaryTableToTxn adds a temporary table to txn to mark it is modified
	// and txn will handle it when committing.
	// It returns a `TemporaryTableHandler` object which provides some extra info for the temporary table.
	AddTemporaryTableToTxn(tblInfo *model.TableInfo) (TemporaryTableHandler, bool)
}

// ExchangePartitionDMLSupport is used for DML operations when the table exchanging a partition.
type ExchangePartitionDMLSupport interface {
	// GetInfoSchemaToCheckExchangeConstraint is used by DML to get the exchanged table to check
	// constraints when exchanging partition.
	GetInfoSchemaToCheckExchangeConstraint() infoschema.MetaOnlyInfoSchema
}

// MutateContext is used to when mutating a table.
type MutateContext interface {
	AllocatorContext
	// GetExprCtx returns the context to build or evaluate expressions
	GetExprCtx() exprctx.ExprContext
	// ConnectionID returns the id of the current connection.
	// If the current environment is not in a query from the client, the return value is 0.
	ConnectionID() uint64
	// InRestrictedSQL returns whether the current context is used in restricted SQL.
	InRestrictedSQL() bool
	// TxnAssertionLevel returns the assertion level of the current transaction.
	TxnAssertionLevel() variable.AssertionLevel
	// EnableMutationChecker returns whether to check data consistency for mutations.
	EnableMutationChecker() bool
	// GetRowEncodingConfig returns the RowEncodingConfig.
	GetRowEncodingConfig() RowEncodingConfig
	// GetMutateBuffers returns the MutateBuffers,
	// which is a buffer for table related structures that aims to reuse memory and
	// saves allocation.
	GetMutateBuffers() *MutateBuffers
	// GetRowIDShardGenerator returns the `RowIDShardGenerator` object to shard rows.
	GetRowIDShardGenerator() *variable.RowIDShardGenerator
	// GetReservedRowIDAlloc returns the `ReservedRowIDAlloc` object to allocate row id from reservation.
	GetReservedRowIDAlloc() (*stmtctx.ReservedRowIDAlloc, bool)
	// GetBinlogSupport returns a `BinlogSupport` if the context supports it.
	// If the context does not support binlog, the second return value will be false.
	GetBinlogSupport() (BinlogSupport, bool)
	// GetStatisticsSupport returns a `StatisticsSupport` if the context supports it.
	// If the context does not support statistics update, the second return value will be false.
	GetStatisticsSupport() (StatisticsSupport, bool)
	// GetCachedTableSupport returns a `CachedTableSupport` if the context supports it.
	// If the context does not support cached table, the second return value will be false.
	GetCachedTableSupport() (CachedTableSupport, bool)
	// GetTemporaryTableSupport returns a `TemporaryTableSupport` if the context supports it.
	// If the context does not support temporary table, the second return value will be false.
	GetTemporaryTableSupport() (TemporaryTableSupport, bool)
	// GetExchangePartitionDMLSupport returns a `ExchangePartitionDMLSupport` if the context supports it.
	// ExchangePartitionDMLSupport is used by DMLs when the table is exchanging a partition.
	GetExchangePartitionDMLSupport() (ExchangePartitionDMLSupport, bool)
}

// AllocatorContext is used to provide context for method `table.Allocators`.
type AllocatorContext interface {
	// AlternativeAllocators returns an alternative `autoid.Allocators` for the table.
	// If the second return value is nil, it means there are no alternative allocators in the context.
	// Currently, it provides alternative allocators for temporary tables to alloc IDs in session.
	AlternativeAllocators(tbl *model.TableInfo) (autoid.Allocators, bool)
}
