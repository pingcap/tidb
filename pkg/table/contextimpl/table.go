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

// GetExprCtx returns the ExprContext
func (ctx *TableContextImpl) GetExprCtx() exprctx.ExprContext {
	return ctx.exprCtx
}

func (ctx *TableContextImpl) vars() *variable.SessionVars {
	return ctx.Context.GetSessionVars()
}
