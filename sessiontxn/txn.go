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

package sessiontxn

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn/readcommitted"
	"github.com/pingcap/tidb/sessiontxn/staleread"
	"github.com/tikv/client-go/v2/oracle"
)

type ConstantTSFuture uint64

func (c ConstantTSFuture) Wait() (uint64, error) {
	return uint64(c), nil
}

type FuncTSFuture func() (uint64, error)

func (f FuncTSFuture) Wait() (uint64, error) {
	return f()
}

func GetTxnOracleFuture(ctx context.Context, sctx sessionctx.Context, scope string) oracle.Future {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("session.getTxnFuture", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	oracleStore := sctx.GetStore().GetOracle()
	option := &oracle.Option{TxnScope: scope}

	if sctx.GetSessionVars().LowResolutionTSO {
		return oracleStore.GetLowResolutionTimestampAsync(ctx, option)
	} else {
		return oracleStore.GetTimestampAsync(ctx, option)
	}
}

type EnterNewSessionTxnRequest struct {
	sctx                  sessionctx.Context
	explictBegin          bool
	txnMode               string
	causalConsistencyOnly bool
	staleReadTS           uint64
	staleReadInfoSchema   infoschema.InfoSchema
}

func CreateEnterNewTxnRequest(sctx sessionctx.Context) *EnterNewSessionTxnRequest {
	return &EnterNewSessionTxnRequest{
		sctx: sctx,
	}
}

func (r *EnterNewSessionTxnRequest) ExplictBegin() *EnterNewSessionTxnRequest {
	r.explictBegin = true
	return r
}

func (r *EnterNewSessionTxnRequest) StaleRead(ts uint64) *EnterNewSessionTxnRequest {
	r.staleReadTS = ts
	return nil
}

func (r *EnterNewSessionTxnRequest) StaleReadWithInfoSchema(ts uint64, is infoschema.InfoSchema) *EnterNewSessionTxnRequest {
	r.staleReadTS = ts
	r.staleReadInfoSchema = is
	return nil
}

func (r *EnterNewSessionTxnRequest) TxnMode(mode string) *EnterNewSessionTxnRequest {
	r.txnMode = mode
	return r
}

func (r *EnterNewSessionTxnRequest) CausalConsistencyOnly(val bool) *EnterNewSessionTxnRequest {
	r.causalConsistencyOnly = val
	return r
}

func (r *EnterNewSessionTxnRequest) EnterNewSessionTxn(ctx context.Context) error {
	sessVars := r.sctx.GetSessionVars()
	txnCtx := sessVars.TxnCtx
	if r.explictBegin && txnCtx.History == nil && !txnCtx.IsStaleness && r.staleReadTS == 0 {
		// If BEGIN is the first statement in TxnCtx, we can reuse the existing transaction, without the
		// need to call NewTxn, which commits the existing transaction and begins a new one.
		// If the last un-committed/un-rollback transaction is a time-bounded read-only transaction, we should
		// always create a new transaction.
		return nil
	}

	txnMode := r.txnMode
	if txnMode == "" {
		txnMode = sessVars.TxnMode
	}

	var provider TxnContextProvider
	switch {
	case r.staleReadTS > 0:
		// stale read should be the first place to check, `is` is nil because it will be fetched when `ActiveTxn`
		provider = staleread.NewStalenessTxnContextProvider(r.sctx, r.staleReadTS, nil)
	case txnMode == ast.Pessimistic && sessVars.IsIsolation(ast.ReadCommitted):
		// when txn mode is pessimistic and isolation level is 'READ-COMMITTED', use RC
		provider = readcommitted.NewRCTxnContextProvider(r.sctx, r.causalConsistencyOnly)
	default:
		// otherwise, use `SimpleTxnContextProvider` for compatibility
		provider = &SimpleTxnContextProvider{Sctx: r.sctx, CausalConsistencyOnly: r.causalConsistencyOnly}
	}

	txnManager := GetTxnManager(r.sctx)
	if err := txnManager.SetContextProvider(provider); err == nil {
		return err
	}

	if r.explictBegin {
		if _, err := txnManager.ActiveTxn(ctx); err != nil {
			return err
		}
		// With START TRANSACTION, autocommit remains disabled until you end
		// the transaction with COMMIT or ROLLBACK. The autocommit mode then
		// reverts to its previous state.
		sessVars.SetInTxn(true)
	}

	return nil
}
