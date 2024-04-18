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
	"github.com/pingcap/errors"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table/context"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/tableutil"
)

var _ context.MutateContext = &TableContextImpl{}
var _ context.AllocatorContext = &TableContextImpl{}

// TableContextImpl is used to provide context for table operations.
type TableContextImpl struct {
	sessionctx.Context
	exprCtx exprctx.ExprContext
}

// NewTableContextImpl creates a new TableContextImpl.
func NewTableContextImpl(sctx sessionctx.Context, exprCtx exprctx.ExprContext) *TableContextImpl {
	return &TableContextImpl{Context: sctx, exprCtx: exprCtx}
}

// TxnRecordTempTable record the temporary table to the current transaction.
// This method will be called when the temporary table is modified in the transaction.
func (ctx *TableContextImpl) TxnRecordTempTable(tbl *model.TableInfo) tableutil.TempTable {
	return ctx.vars().GetTemporaryTable(tbl)
}

func (ctx *TableContextImpl) CheckTempTableSize(tmpTable tableutil.TempTable, tblInfo *model.TableInfo) error {
	tmpTableSize := tmpTable.GetSize()
	if tempTableData := ctx.GetSessionVars().TemporaryTableData; tempTableData != nil {
		tmpTableSize += tempTableData.GetTableSize(tblInfo.ID)
	}

	if tmpTableSize > ctx.GetSessionVars().TMPTableSize {
		return errors.New("overflow size")
	}

	return nil
}

func (ctx *TableContextImpl) IncreaseTTLMetricsCount(cnt int) {
	if txnCtx := ctx.GetSessionVars().TxnCtx; txnCtx != nil {
		txnCtx.InsertTTLRowsCount += cnt
	}
}

func (ctx *TableContextImpl) SkipWriteUntouchedKeys() bool {
	sc := ctx.GetSessionVars().StmtCtx
	if ctx.InTxn() || sc.InHandleForeignKeyTrigger || sc.ForeignKeyTriggerCtx.HasFKCascades {
		return false
	}
	return true
}

func (ctx *TableContextImpl) TemporaryTableIDAllocator(tbl *model.TableInfo) (autoid.Allocator, bool) {
	if tmpTbl := ctx.vars().GetTemporaryTable(tbl); tmpTbl != nil {
		if alloc := tmpTbl.GetAutoIDAllocator(); alloc != nil {
			return alloc, true
		}
	}
	return nil, false
}

// GetExprCtx returns the ExprContext
func (ctx *TableContextImpl) GetExprCtx() exprctx.ExprContext {
	return ctx.exprCtx
}

func (ctx *TableContextImpl) ShouldWriteBinlog() bool {
	return ctx.vars().BinlogClient != nil && !ctx.vars().InRestrictedSQL
}

func (ctx *TableContextImpl) BatchCheck() bool {
	return ctx.vars().StmtCtx.BatchCheck
}

func (ctx *TableContextImpl) LazyCheckKeyNotExists() bool {
	return ctx.vars().LazyCheckKeyNotExists()
}

func (ctx *TableContextImpl) ConstraintCheckInPlacePessimistic() bool {
	return ctx.vars().ConstraintCheckInPlacePessimistic
}

func (ctx *TableContextImpl) ConstraintCheckInPlace() bool {
	return ctx.vars().ConstraintCheckInPlace
}

func (ctx *TableContextImpl) SetPresumeKeyNotExists(val bool) {
	ctx.vars().PresumeKeyNotExists = val
}

func (ctx *TableContextImpl) PresumeKeyNotExists() bool {
	return ctx.vars().PresumeKeyNotExists
}

func (ctx *TableContextImpl) IsPessimistic() bool {
	if txnCtx := ctx.GetSessionVars().TxnCtx; txnCtx != nil {
		return txnCtx.IsPessimistic
	}
	return false
}

func (ctx *TableContextImpl) InTxn() bool {
	return ctx.GetSessionVars().InTxn()
}

func (ctx *TableContextImpl) RowEncoder() *rowcodec.Encoder {
	return &ctx.GetSessionVars().RowEncoder
}

func (ctx *TableContextImpl) UpdateDeltaForTable(physicalTableID int64, delta int64, count int64, colSize map[int64]int64) {
	if txnCtx := ctx.GetSessionVars().TxnCtx; txnCtx != nil {
		ctx.GetSessionVars().TxnCtx.UpdateDeltaForTable(physicalTableID, delta, count, colSize)
	}
}

func (ctx *TableContextImpl) InRestrictedSQL() bool {
	return ctx.vars().StmtCtx.InRestrictedSQL
}

func (ctx *TableContextImpl) ConnectionID() uint64 {
	return ctx.vars().ConnectionID
}

func (ctx *TableContextImpl) TxnStartTS() uint64 {
	if txnCtx := ctx.GetSessionVars().TxnCtx; txnCtx != nil {
		return txnCtx.StartTS
	}
	return 0
}

func (ctx *TableContextImpl) IsRowLevelChecksumEnabled() bool {
	return ctx.vars().IsRowLevelChecksumEnabled()
}

func (ctx *TableContextImpl) EnableMutationChecker() bool {
	return ctx.vars().EnableMutationChecker
}

func (ctx *TableContextImpl) AssertionLevel() variable.AssertionLevel {
	return ctx.vars().AssertionLevel
}

func (ctx *TableContextImpl) GetWriteStmtBufs() *variable.WriteStmtBufs {
	return ctx.vars().GetWriteStmtBufs()
}

func (ctx *TableContextImpl) OnMutateCachedTable(tid int64, handle any) error {
	txnCtx := ctx.vars().TxnCtx
	if txnCtx == nil {
		return nil
	}

	if txnCtx.CachedTables == nil {
		txnCtx.CachedTables = make(map[int64]any)
	}
	if _, ok := txnCtx.CachedTables[tid]; !ok {
		txnCtx.CachedTables[tid] = handle
	}
	return nil
}

func (ctx *TableContextImpl) ReserveRowID(baseID int64, maxID int64) {
	sessVars := ctx.vars()
	sc := sessVars.StmtCtx
	sc.BaseRowID, sc.MaxRowID = baseID, maxID
}

func (ctx *TableContextImpl) ConsumeReservedRowID() (int64, bool) {
	sessVars := ctx.vars()
	sc := sessVars.StmtCtx
	baseID, maxID := sc.BaseRowID, sc.MaxRowID
	if baseID < maxID {
		sc.BaseRowID++
		return sc.BaseRowID, true
	}
	return 0, false
}

func (ctx *TableContextImpl) ShardNextIDs(rowCnt int) int64 {
	return ctx.vars().GetCurrentShard(rowCnt)
}

func (ctx *TableContextImpl) vars() *variable.SessionVars {
	return ctx.Context.GetSessionVars()
}
