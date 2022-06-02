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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
)

type PessimisticSerializableTxnContextProvider struct {
	baseTxnContextProvider
}

func NewPessimisticSerializableTxnContextProvider(sctx sessionctx.Context,
	causalConsistencyOnly bool) *PessimisticSerializableTxnContextProvider {
	provider := &PessimisticSerializableTxnContextProvider{
		baseTxnContextProvider{
			sctx:                  sctx,
			causalConsistencyOnly: causalConsistencyOnly,
			onInitializeTxnCtx: func(txnCtx *variable.TransactionContext) {
				txnCtx.IsPessimistic = true
				txnCtx.Isolation = ast.Serializable
			},
			onTxnActive: func(txn kv.Transaction) {
				txn.SetOption(kv.Pessimistic, true)
			},
		},
	}

	provider.getStmtForUpdateTSFunc = provider.getTxnStartTS
	provider.getStmtReadTSFunc = provider.getTxnStartTS
	return provider
}

// OnStmtRetry is the hook that should be called when a statement is retried internally.
func (p *PessimisticSerializableTxnContextProvider) OnStmtRetry(_ context.Context) error {
	return errors.New("Should never retry in serializable isolation")
}

// OnStmtStart is the hook that should be called when a new statement started
func (p *PessimisticSerializableTxnContextProvider) OnStmtStart(ctx context.Context) error {
	if err := p.baseTxnContextProvider.OnStmtStart(ctx); err != nil {
		return err
	}
	return nil
}

// Advise is used to give advice to provider
func (p *PessimisticSerializableTxnContextProvider) Advise(tp sessiontxn.AdviceType) error {
	switch tp {
	case sessiontxn.AdviceWarmUp:
		return p.warmUp()
	default:
		return p.baseTxnContextProvider.Advise(tp)
	}
}

// OnStmtErrorForNextAction is the hook that should be called when a new statement get an error
func (p *PessimisticSerializableTxnContextProvider) OnStmtErrorForNextAction(
	point sessiontxn.StmtErrorHandlePoint, err error) (sessiontxn.StmtErrorAction, error) {
	switch point {
	case sessiontxn.StmtErrAfterPessimisticLock:
		return p.handleAfterPessimisticLockError(err)
	default:
		return sessiontxn.NoIdea()
	}
}

func (p *PessimisticSerializableTxnContextProvider) handleAfterPessimisticLockError(
	lockErr error) (sessiontxn.StmtErrorAction, error) {
	return sessiontxn.ErrorAction(lockErr)
}
