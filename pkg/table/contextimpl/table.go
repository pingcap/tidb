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

package contextimpl

import (
	"github.com/pingcap/failpoint"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table/context"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tipb/go-binlog"
)

var _ context.MutateContext = &TableContextImpl{}
var _ context.AllocatorContext = &TableContextImpl{}

// TableContextImpl is used to provide context for table operations.
type TableContextImpl struct {
	sessionctx.Context
	// mutateBuffers is a memory pool for table related memory allocation that aims to reuse memory
	// and saves allocation
	// The buffers are supposed to be used inside AddRecord/UpdateRecord/RemoveRecord.
	mutateBuffers *context.MutateBuffers
}

// NewTableContextImpl creates a new TableContextImpl.
func NewTableContextImpl(sctx sessionctx.Context) *TableContextImpl {
	return &TableContextImpl{
		Context:       sctx,
		mutateBuffers: context.NewMutateBuffers(sctx.GetSessionVars().GetWriteStmtBufs()),
	}
}

// AlternativeAllocators implements the AllocatorContext interface
func (ctx *TableContextImpl) AlternativeAllocators(tbl *model.TableInfo) (allocators autoid.Allocators, ok bool) {
	// Use an independent allocator for global temporary tables.
	if tbl.TempTableType == model.TempTableGlobal {
		if tempTbl := ctx.vars().GetTemporaryTable(tbl); tempTbl != nil {
			if alloc := tempTbl.GetAutoIDAllocator(); alloc != nil {
				return autoid.NewAllocators(false, alloc), true
			}
		}
		// If the session is not in a txn, for example, in "show create table", use the original allocator.
	}
	return
}

// GetExprCtx returns the ExprContext
func (ctx *TableContextImpl) GetExprCtx() exprctx.ExprContext {
	return ctx.Context.GetExprCtx()
}

// ConnectionID implements the MutateContext interface.
func (ctx *TableContextImpl) ConnectionID() uint64 {
	return ctx.vars().ConnectionID
}

// InRestrictedSQL returns whether the current context is used in restricted SQL.
func (ctx *TableContextImpl) InRestrictedSQL() bool {
	return ctx.vars().InRestrictedSQL
}

// TxnAssertionLevel implements the MutateContext interface.
func (ctx *TableContextImpl) TxnAssertionLevel() variable.AssertionLevel {
	return ctx.vars().AssertionLevel
}

// EnableMutationChecker implements the MutateContext interface.
func (ctx *TableContextImpl) EnableMutationChecker() bool {
	return ctx.vars().EnableMutationChecker
}

// GetRowEncodingConfig returns the RowEncodingConfig.
func (ctx *TableContextImpl) GetRowEncodingConfig() context.RowEncodingConfig {
	vars := ctx.vars()
	return context.RowEncodingConfig{
		IsRowLevelChecksumEnabled: vars.IsRowLevelChecksumEnabled(),
		RowEncoder:                &vars.RowEncoder,
	}
}

// GetMutateBuffers implements the MutateContext interface.
func (ctx *TableContextImpl) GetMutateBuffers() *context.MutateBuffers {
	return ctx.mutateBuffers
}

// GetRowIDShardGenerator implements the MutateContext interface.
func (ctx *TableContextImpl) GetRowIDShardGenerator() *variable.RowIDShardGenerator {
	return ctx.vars().GetRowIDShardGenerator()
}

// GetReservedRowIDAlloc implements the MutateContext interface.
func (ctx *TableContextImpl) GetReservedRowIDAlloc() (*stmtctx.ReservedRowIDAlloc, bool) {
	if sc := ctx.vars().StmtCtx; sc != nil {
		return &sc.ReservedRowIDAlloc, true
	}
	// `StmtCtx` should not be nil in the `variable.SessionVars`.
	// We just put an assertion that will panic only if in test here.
	// In production code, here returns (nil, false) to make code safe
	// because some old code checks `StmtCtx != nil` but we don't know why.
	intest.Assert(false, "SessionVars.StmtCtx should not be nil")
	return nil, false
}

// GetBinlogSupport implements the MutateContext interface.
func (ctx *TableContextImpl) GetBinlogSupport() (context.BinlogSupport, bool) {
	failpoint.Inject("forceWriteBinlog", func() {
		// Just to cover binlog related code in this package, since the `BinlogClient` is
		// still nil, mutations won't be written to pump on commit.
		failpoint.Return(ctx, true)
	})
	if ctx.vars().BinlogClient != nil {
		return ctx, true
	}
	return nil, false
}

// GetBinlogMutation implements the BinlogSupport interface.
func (ctx *TableContextImpl) GetBinlogMutation(tblID int64) *binlog.TableMutation {
	return ctx.Context.StmtGetMutation(tblID)
}

// GetStatisticsSupport implements the MutateContext interface.
func (ctx *TableContextImpl) GetStatisticsSupport() (context.StatisticsSupport, bool) {
	if ctx.vars().TxnCtx != nil {
		return ctx, true
	}
	return nil, false
}

// UpdatePhysicalTableDelta implements the StatisticsSupport interface.
func (ctx *TableContextImpl) UpdatePhysicalTableDelta(
	physicalTableID int64, delta int64, count int64, cols variable.DeltaCols,
) {
	if txnCtx := ctx.vars().TxnCtx; txnCtx != nil {
		txnCtx.UpdateDeltaForTable(physicalTableID, delta, count, cols)
	}
}

// GetCachedTableSupport implements the MutateContext interface.
func (ctx *TableContextImpl) GetCachedTableSupport() (context.CachedTableSupport, bool) {
	if ctx.vars().TxnCtx != nil {
		return ctx, true
	}
	return nil, false
}

// AddCachedTableHandleToTxn implements `CachedTableSupport` interface
func (ctx *TableContextImpl) AddCachedTableHandleToTxn(tableID int64, handle any) {
	txnCtx := ctx.vars().TxnCtx
	if txnCtx.CachedTables == nil {
		txnCtx.CachedTables = make(map[int64]any)
	}
	if _, ok := txnCtx.CachedTables[tableID]; !ok {
		txnCtx.CachedTables[tableID] = handle
	}
}

// GetTemporaryTableSupport implements the MutateContext interface.
func (ctx *TableContextImpl) GetTemporaryTableSupport() (context.TemporaryTableSupport, bool) {
	if ctx.vars().TxnCtx == nil {
		return nil, false
	}
	return ctx, true
}

// GetInfoSchemaToCheckExchangeConstraint implements the ExchangePartitionDMLSupport interface.
func (ctx *TableContextImpl) GetInfoSchemaToCheckExchangeConstraint() infoschema.MetaOnlyInfoSchema {
	return ctx.Context.GetDomainInfoSchema()
}

// GetExchangePartitionDMLSupport implements the MutateContext interface.
func (ctx *TableContextImpl) GetExchangePartitionDMLSupport() (context.ExchangePartitionDMLSupport, bool) {
	return ctx, true
}

// GetTemporaryTableSizeLimit implements TemporaryTableSupport interface.
func (ctx *TableContextImpl) GetTemporaryTableSizeLimit() int64 {
	return ctx.vars().TMPTableSize
}

// AddTemporaryTableToTxn implements the TemporaryTableSupport interface.
func (ctx *TableContextImpl) AddTemporaryTableToTxn(tblInfo *model.TableInfo) (context.TemporaryTableHandler, bool) {
	vars := ctx.vars()
	if tbl := vars.GetTemporaryTable(tblInfo); tbl != nil {
		tbl.SetModified(true)
		return context.NewTemporaryTableHandler(tbl, vars.TemporaryTableData), true
	}
	return context.TemporaryTableHandler{}, false
}

func (ctx *TableContextImpl) vars() *variable.SessionVars {
	return ctx.Context.GetSessionVars()
}
