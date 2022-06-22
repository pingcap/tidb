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

package isolation_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func getOracleTS(t *testing.T, sctx sessionctx.Context) uint64 {
	ts, err := sctx.GetStore().GetOracle().GetTimestamp(context.TODO(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoError(t, err)
	return ts
}

type txnAssert[T sessiontxn.TxnContextProvider] struct {
	sctx                  sessionctx.Context
	isolation             string
	minStartTime          time.Time
	active                bool
	inTxn                 bool
	minStartTS            uint64
	startTS               uint64
	causalConsistencyOnly bool
	couldRetry            bool
}

func (a *txnAssert[T]) Check(t *testing.T) {
	provider := sessiontxn.GetTxnManager(a.sctx).GetContextProvider()
	sessVars := a.sctx.GetSessionVars()
	txnCtx := sessVars.TxnCtx

	if sessVars.SnapshotInfoschema == nil {
		require.Same(t, provider.GetTxnInfoSchema(), txnCtx.InfoSchema)
	} else {
		require.Equal(t, sessVars.SnapshotInfoschema.(infoschema.InfoSchema).SchemaMetaVersion(), provider.GetTxnInfoSchema().SchemaMetaVersion())
	}
	require.Equal(t, a.isolation, txnCtx.Isolation)
	require.Equal(t, a.isolation != "", txnCtx.IsPessimistic)
	require.Equal(t, sessVars.CheckAndGetTxnScope(), txnCtx.TxnScope)
	require.Equal(t, sessVars.ShardAllocateStep, int64(txnCtx.ShardStep))
	require.False(t, txnCtx.IsStaleness)
	require.GreaterOrEqual(t, txnCtx.CreateTime.Nanosecond(), a.minStartTime.Nanosecond())
	require.Equal(t, a.inTxn, sessVars.InTxn())
	require.Equal(t, a.inTxn, txnCtx.IsExplicit)
	require.Equal(t, a.couldRetry, txnCtx.CouldRetry)

	txn, err := a.sctx.Txn(false)
	require.NoError(t, err)
	require.Equal(t, a.active, txn.Valid())
	if !a.active {
		require.False(t, a.inTxn)
		require.Zero(t, a.startTS)
		require.Zero(t, txnCtx.StartTS)
	} else {
		require.True(t, a.minStartTS != 0 || a.startTS != 0)
		require.Greater(t, txnCtx.StartTS, a.minStartTS)
		if a.startTS != 0 {
			require.Equal(t, a.startTS, txnCtx.StartTS)
		}
		require.Equal(t, txnCtx.StartTS, txn.StartTS())
		require.Same(t, sessVars.KVVars, txn.GetVars())
		require.Equal(t, txnCtx.TxnScope, txn.GetOption(kv.TxnScope))
		require.Equal(t, a.causalConsistencyOnly, !txn.GetOption(kv.GuaranteeLinearizability).(bool))
		require.Equal(t, txnCtx.IsPessimistic, txn.IsPessimistic())
	}
	// The next line is testing the provider has the type T, if not, the cast will panic
	_ = provider.(T)
}

func activeSnapshotTxnAssert(sctx sessionctx.Context, ts uint64, isolation string) *txnAssert[sessiontxn.TxnContextProvider] {
	return &txnAssert[sessiontxn.TxnContextProvider]{
		sctx:         sctx,
		minStartTime: time.Now(),
		startTS:      ts,
		active:       true,
		inTxn:        false,
		isolation:    isolation,
	}
}

func (a *txnAssert[T]) CheckAndGetProvider(t *testing.T) T {
	a.Check(t)
	return sessiontxn.GetTxnManager(a.sctx).GetContextProvider().(T)
}
