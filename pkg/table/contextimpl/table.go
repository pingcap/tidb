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
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table/context"
	"github.com/pingcap/tidb/pkg/util/tableutil"
)

var _ context.MutateContext = &TableContextImpl{}
var _ context.AllocatorContext = &TableContextImpl{}

// TableContextImpl is used to provide context for table operations.
type TableContextImpl struct {
	sessionctx.Context
	exprCtx exprctx.ExprContext
	// mutateBuffers is a memory pool for table related memory allocation that aims to reuse memory
	// and saves allocation
	// The buffers are supposed to be used inside AddRecord/UpdateRecord/RemoveRecord.
	mutateBuffers *context.MutateBuffers

	hasExtraPosInfo bool
	// extraIndexKeyInfo records how we locate indexes from the given row data.
	// The original IndexInfo only records the column offsets in the full row.
	// We'll need the extraIndexKeyPosInfo when the given row is pruned and only needed columns are left.
	extraIndexKeyPosInfo map[int64][]int
}

// NewTableContextImpl creates a new TableContextImpl.
func NewTableContextImpl(sctx sessionctx.Context, exprCtx exprctx.ExprContext) *TableContextImpl {
	return &TableContextImpl{
		Context:       sctx,
		exprCtx:       exprCtx,
		mutateBuffers: context.NewMutateBuffers(sctx.GetSessionVars().GetWriteStmtBufs()),
	}
}

// TxnRecordTempTable record the temporary table to the current transaction.
// This method will be called when the temporary table is modified in the transaction.
func (ctx *TableContextImpl) TxnRecordTempTable(tbl *model.TableInfo) tableutil.TempTable {
	return ctx.vars().GetTemporaryTable(tbl)
}

// GetExprCtx returns the ExprContext
func (ctx *TableContextImpl) GetExprCtx() exprctx.ExprContext {
	return ctx.exprCtx
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

func (ctx *TableContextImpl) vars() *variable.SessionVars {
	return ctx.Context.GetSessionVars()
}

// SetExtraIndexKeyPosInfo sets the extra index key pos info.
func (ctx *TableContextImpl) SetExtraIndexKeyPosInfo(indexKeyPos map[int64][]int) {
	ctx.hasExtraPosInfo = true
	ctx.extraIndexKeyPosInfo = indexKeyPos
}

// GetExtraIndexKeyPosInfo gets the extra index key pos info.
// Check HasExtraInfo before calling this function.
func (ctx *TableContextImpl) GetExtraIndexKeyPosInfo(idxID int64) []int {
	return ctx.extraIndexKeyPosInfo[idxID]
}

// ResetExtraInfo resets the extra index key pos info.
// We need to reset it just after the info is used because we supported the foreign key
// and it's executed before the recordSet.Close() is called.
// If we reset it in the executor's Close() triggered by recordSet.Close().
// The execution of the foreign key will get wrong information.
func (ctx *TableContextImpl) ResetExtraInfo() {
	ctx.hasExtraPosInfo = false
	ctx.extraIndexKeyPosInfo = nil
}

// HasExtraInfo returns whether the context has extra index key pos info.
func (ctx *TableContextImpl) HasExtraInfo() bool {
	return ctx.hasExtraPosInfo
}
