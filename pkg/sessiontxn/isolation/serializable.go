// Copyright 2022 PingCAP, Inc.
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

package isolation

import (
	"context"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
)

// PessimisticSerializableTxnContextProvider provides txn context for isolation level oracle-like serializable
type PessimisticSerializableTxnContextProvider struct {
	baseTxnContextProvider
}

// NewPessimisticSerializableTxnContextProvider returns a new PessimisticSerializableTxnContextProvider
func NewPessimisticSerializableTxnContextProvider(sctx sessionctx.Context,
	causalConsistencyOnly bool) *PessimisticSerializableTxnContextProvider {
	provider := &PessimisticSerializableTxnContextProvider{
		baseTxnContextProvider: baseTxnContextProvider{
			sctx:                  sctx,
			causalConsistencyOnly: causalConsistencyOnly,
			onInitializeTxnCtx: func(txnCtx *variable.TransactionContext) {
				txnCtx.IsPessimistic = true
				txnCtx.Isolation = ast.Serializable
			},
			onTxnActiveFunc: func(txn kv.Transaction, _ sessiontxn.EnterNewTxnType) {
				txn.SetOption(kv.Pessimistic, true)
			},
		},
	}

	provider.getStmtForUpdateTSFunc = provider.getTxnStartTS
	provider.getStmtReadTSFunc = provider.getTxnStartTS
	return provider
}

// OnStmtErrorForNextAction is the hook that should be called when a new statement get an error
func (p *PessimisticSerializableTxnContextProvider) OnStmtErrorForNextAction(ctx context.Context, point sessiontxn.StmtErrorHandlePoint, err error) (sessiontxn.StmtErrorAction, error) {
	switch point {
	case sessiontxn.StmtErrAfterPessimisticLock:
		// In oracle-like serializable isolation, we do not retry encountering pessimistic lock error.
		return sessiontxn.ErrorAction(err)
	default:
		return sessiontxn.NoIdea()
	}
}
