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
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table/temptable"
	"github.com/pingcap/tidb/util/logutil"
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

// GetSessionSnapshotInfoSchema returns the session's information schema with specified ts
func GetSessionSnapshotInfoSchema(sctx sessionctx.Context, snapshotTS uint64) (infoschema.InfoSchema, error) {
	is, err := domain.GetDomain(sctx).GetSnapshotInfoSchema(snapshotTS)
	if err != nil {
		return nil, err
	}
	return temptable.AttachLocalTemporaryTableInfoSchema(sctx, is), nil
}
