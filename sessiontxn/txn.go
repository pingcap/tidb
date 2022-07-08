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
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table/temptable"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// ConstantFuture implements oracle.Future
type ConstantFuture uint64

// Wait returns a constant ts
func (n ConstantFuture) Wait() (uint64, error) {
	return uint64(n), nil
}

// FuncFuture implements oracle.Future
type FuncFuture func() (uint64, error)

// Wait returns a ts got from the func
func (f FuncFuture) Wait() (uint64, error) {
	return f()
}

// NewOracleFuture creates new future according to the scope and the session context
func NewOracleFuture(ctx context.Context, sctx sessionctx.Context, scope string) oracle.Future {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("sessiontxn.NewOracleFuture", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	oracleStore := sctx.GetStore().GetOracle()
	option := &oracle.Option{TxnScope: scope}

	if sctx.GetSessionVars().LowResolutionTSO {
		return oracleStore.GetLowResolutionTimestampAsync(ctx, option)
	}
	return oracleStore.GetTimestampAsync(ctx, option)
}

// CanReuseTxnWhenExplicitBegin returns whether we should reuse the txn when starting a transaction explicitly
func CanReuseTxnWhenExplicitBegin(sctx sessionctx.Context) bool {
	sessVars := sctx.GetSessionVars()
	txnCtx := sessVars.TxnCtx
	// If BEGIN is the first statement in TxnCtx, we can reuse the existing transaction, without the
	// need to call NewTxn, which commits the existing transaction and begins a new one.
	// If the last un-committed/un-rollback transaction is a time-bounded read-only transaction, we should
	// always create a new transaction.
	// If the variable `tidb_snapshot` is set, we should always create a new transaction because the current txn may be
	// initialized with snapshot ts.
	return txnCtx.History == nil && !txnCtx.IsStaleness && sessVars.SnapshotTS == 0
}

// CommitBeforeEnterNewTxn is called before entering a new transaction. It checks whether the old
// txn is valid in which case we should commit it first.
func CommitBeforeEnterNewTxn(ctx context.Context, sctx sessionctx.Context) error {
	txn, err := sctx.Txn(false)
	if err != nil {
		return err
	}
	if txn.Valid() {
		txnStartTS := txn.StartTS()
		txnScope := sctx.GetSessionVars().TxnCtx.TxnScope
		err = sctx.CommitTxn(ctx)
		if err != nil {
			return err
		}
		logutil.Logger(ctx).Info("Try to create a new txn inside a transaction auto commit",
			zap.Int64("schemaVersion", sctx.GetInfoSchema().SchemaMetaVersion()),
			zap.Uint64("txnStartTS", txnStartTS),
			zap.String("txnScope", txnScope))
	}
	return nil
}

// GetSnapshotWithTS returns a snapshot with ts.
func GetSnapshotWithTS(s sessionctx.Context, ts uint64) kv.Snapshot {
	snap := s.GetStore().GetSnapshot(kv.Version{Ver: ts})
	snap.SetOption(kv.SnapInterceptor, temptable.SessionSnapshotInterceptor(s))
	if s.GetSessionVars().InRestrictedSQL {
		snap.SetOption(kv.RequestSourceInternal, true)
	}
	if tp := s.GetSessionVars().RequestSourceType; tp != "" {
		snap.SetOption(kv.RequestSourceType, tp)
	}
	return snap
}

// SetTxnAssertionLevel sets assertion level of a transactin. Note that assertion level should be set only once just
// after creating a new transaction.
func SetTxnAssertionLevel(txn kv.Transaction, assertionLevel variable.AssertionLevel) {
	switch assertionLevel {
	case variable.AssertionLevelOff:
		txn.SetOption(kv.AssertionLevel, kvrpcpb.AssertionLevel_Off)
	case variable.AssertionLevelFast:
		txn.SetOption(kv.AssertionLevel, kvrpcpb.AssertionLevel_Fast)
	case variable.AssertionLevelStrict:
		txn.SetOption(kv.AssertionLevel, kvrpcpb.AssertionLevel_Strict)
	}
}
