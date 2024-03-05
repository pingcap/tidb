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

package internal

import (
	"context"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

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
func GetSnapshotWithTS(s sessionctx.Context, ts uint64, interceptor kv.SnapshotInterceptor) kv.Snapshot {
	snap := s.GetStore().GetSnapshot(kv.Version{Ver: ts})
	if interceptor != nil {
		snap.SetOption(kv.SnapInterceptor, interceptor)
	}
	if s.GetSessionVars().InRestrictedSQL {
		snap.SetOption(kv.RequestSourceInternal, true)
	}
	if tp := s.GetSessionVars().RequestSourceType; tp != "" {
		snap.SetOption(kv.RequestSourceType, tp)
	}
	if tp := s.GetSessionVars().ExplicitRequestSourceType; tp != "" {
		snap.SetOption(kv.ExplicitRequestSourceType, tp)
	}
	if s.GetSessionVars().LoadBasedReplicaReadThreshold > 0 {
		snap.SetOption(kv.LoadBasedReplicaReadThreshold, s.GetSessionVars().LoadBasedReplicaReadThreshold)
	}
	return snap
}
