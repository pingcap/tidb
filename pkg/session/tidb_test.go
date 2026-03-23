// Copyright 2015 PingCAP, Inc.
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

package session

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestDomapHandleNil(t *testing.T) {
	// this is required for enterprise plugins
	// ref: https://github.com/pingcap/tidb/issues/37319
	require.NotPanics(t, func() {
		_, _ = domap.Get(nil)
	})
}

func TestSysSessionPoolGoroutineLeak(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	se, err := createSession(store)
	require.NoError(t, err)

	count := 200
	stmts := make([]ast.StmtNode, count)
	for i := range count {
		stmt, err := se.ParseWithParams(context.Background(), "select * from mysql.user limit 1")
		require.NoError(t, err)
		stmts[i] = stmt
	}
	// Test an issue that sysSessionPool doesn't call session's Close, cause
	// asyncGetTSWorker goroutine leak.
	var wg util.WaitGroupWrapper
	for i := range count {
		s := stmts[i]
		wg.Run(func() {
			ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
			_, _, err := se.ExecRestrictedStmt(ctx, s)
			require.NoError(t, err)
		})
	}
	wg.Wait()
}

func TestRUV2SessionParserTotalDoesNotLeakAcrossStandaloneParse(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	se, err := createSession(store)
	require.NoError(t, err)

	_, err = se.ParseWithParams(context.Background(), "select 1")
	require.NoError(t, err)
	require.Equal(t, int64(1), se.sessionVars.RUV2PendingSessionParserTotal.Load())

	stmt, err := se.ParseWithParams(context.Background(), "set @a=1")
	require.NoError(t, err)
	require.Equal(t, int64(1), se.sessionVars.RUV2PendingSessionParserTotal.Load())

	_, err = se.ExecuteStmt(context.Background(), stmt)
	require.NoError(t, err)
	require.Zero(t, se.sessionVars.RUV2PendingSessionParserTotal.Load())
	require.NotNil(t, se.sessionVars.RUV2Metrics)
	require.Equal(t, int64(1), se.sessionVars.RUV2Metrics.SessionParserTotal())

	dctx := se.GetDistSQLCtx()
	require.Equal(t, se.sessionVars.RUV2Metrics, dctx.RUV2Metrics)
	require.NotNil(t, dctx.RUV2RPCInterceptor)
}

func TestRUV2MetricsIsolatedPerStatementInExplicitTxn(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	se, err := createSession(store)
	require.NoError(t, err)

	ctx := context.Background()

	// BEGIN
	stmtBegin, err := se.ParseWithParams(ctx, "begin")
	require.NoError(t, err)
	_, err = se.ExecuteStmt(ctx, stmtBegin)
	require.NoError(t, err)
	metricsBegin := se.sessionVars.RUV2Metrics
	require.NotNil(t, metricsBegin)

	// Statement 1 inside the transaction
	stmt1, err := se.ParseWithParams(ctx, "select 1")
	require.NoError(t, err)
	_, err = se.ExecuteStmt(ctx, stmt1)
	require.NoError(t, err)
	metrics1 := se.sessionVars.RUV2Metrics
	require.NotNil(t, metrics1)

	// Statement 2 inside the transaction
	stmt2, err := se.ParseWithParams(ctx, "select 2")
	require.NoError(t, err)
	_, err = se.ExecuteStmt(ctx, stmt2)
	require.NoError(t, err)
	metrics2 := se.sessionVars.RUV2Metrics

	// Each statement must get a fresh RUV2Metrics object so that the
	// interceptor bound during execution targets the current statement,
	// not a previous one.
	require.NotNil(t, metrics2)
	require.NotSame(t, metricsBegin, metrics1, "stmt1 should have different metrics from BEGIN")
	require.NotSame(t, metrics1, metrics2, "stmt2 should have different metrics from stmt1")
}

func TestSchemaCacheSizeVar(t *testing.T) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	size, isNull, err := m.GetSchemaCacheSize()
	require.NoError(t, err)
	require.Equal(t, size, uint64(0))
	require.Equal(t, isNull, true)
	require.NoError(t, txn.Rollback())

	dom, err := BootstrapSession(store)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	txn, err = store.Begin()
	require.NoError(t, err)
	m = meta.NewMutator(txn)
	size, isNull, err = m.GetSchemaCacheSize()
	require.NoError(t, err)
	require.Equal(t, size, uint64(vardef.DefTiDBSchemaCacheSize))
	require.Equal(t, isNull, false)
	require.NoError(t, txn.Rollback())
}
