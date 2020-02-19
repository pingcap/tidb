// Copyright 2018 PingCAP, Inc.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package session

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/session/txnstate"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tipb/go-binlog"
)

func (s *session) getTxnFuture(ctx context.Context) *txnstate.TxnFuture {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("session.getTxnFuture", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	oracleStore := s.store.GetOracle()
	var tsFuture oracle.Future
	if s.sessionVars.LowResolutionTSO {
		tsFuture = oracleStore.GetLowResolutionTimestampAsync(ctx)
	} else {
		tsFuture = oracleStore.GetTimestampAsync(ctx)
	}
	return txnstate.NewTxnFuture(tsFuture, s.store)
}

// HasDirtyContent checks whether there's dirty update on the given table.
// Put this function here is to avoid cycle import.
func (s *session) HasDirtyContent(tid int64) bool {
	x := s.GetSessionVars().TxnCtx.DirtyDB
	if x == nil {
		return false
	}
	return !x.(*executor.DirtyDB).GetDirtyTable(tid).IsEmpty()
}

// StmtCommit implements the sessionctx.Context interface.
func (s *session) StmtCommit(memTracker *memory.Tracker) error {
	return s.txn.StmtCommit(s, memTracker)
}

// StmtRollback implements the sessionctx.Context interface.
func (s *session) StmtRollback() {
	s.txn.Cleanup()
}

// StmtGetMutation implements the sessionctx.Context interface.
func (s *session) StmtGetMutation(tableID int64) *binlog.TableMutation {
	return s.txn.StmtGetMutation(tableID)
}

func (s *session) StmtAddDirtyTableOP(op int, tid int64, handle int64) {
	s.txn.StmtAddDirtyTableOP(op, tid, handle)
}
