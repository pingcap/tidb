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
	"encoding/base64"
	"runtime"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestDomapHandleNil(t *testing.T) {
	// this is required for enterprise plugins
	// ref: https://github.com/pingcap/tidb/issues/37319
	require.NotPanics(t, func() {
		_, _ = domap.Get(nil)
	})
}

func TestGlobalVariableInitDomainSkipsStatusEndpointClaim(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create a file containing a colon, which is not allowed on Windows")
	}
	if kerneltype.IsNextGen() {
		t.Skip("this classic-kernel startup path uses a store without a system keyspace")
	}
	integration.BeforeTestExternal(t)

	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	// Bootstrap without etcd first so the second startup exercises only the
	// global-variable initialization Domain and the final serving Domain.
	initialDomain, err := BootstrapSession(store)
	require.NoError(t, err)
	initialDomain.Close()

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	mockStore := &mockEtcdBackend{
		Storage: store,
		pdAddrs: []string{cluster.Members[0].GRPCURL()},
	}
	previousConfig := config.GetGlobalConfig()
	defer config.StoreGlobalConfig(previousConfig)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Store = config.StoreTypeTiKV
		conf.AdvertiseAddress = "127.0.0.1"
		conf.Status.ReportStatus = true
		conf.Status.StatusPort = 10080
	})

	ctx := context.Background()
	require.NoError(t, ddl.StartOwnerManager(ctx, mockStore))
	defer ddl.CloseOwnerManager(mockStore)

	endpoint := "127.0.0.1:10080"
	claimKey := "/tidb/server/status_addr/" + base64.RawURLEncoding.EncodeToString([]byte(endpoint))
	etcdClient := cluster.RandClient()
	claimLease, err := etcdClient.Grant(ctx, ddlutil.SessionTTL)
	require.NoError(t, err)
	defer func() {
		_, revokeErr := etcdClient.Revoke(ctx, claimLease.ID)
		require.NoError(t, revokeErr)
	}()
	_, err = etcdClient.Put(ctx, claimKey, "existing-server", clientv3.WithLease(claimLease.ID))
	require.NoError(t, err)

	core, recorded := observer.New(zap.WarnLevel)
	restoreLogger := log.ReplaceGlobals(
		zap.New(core),
		&log.ZapProperties{
			Core:  core,
			Level: zap.NewAtomicLevelAt(zap.WarnLevel),
		},
	)
	defer restoreLogger()

	dom, err := BootstrapSession(mockStore)
	require.NoError(t, err)
	defer func() {
		if dom != nil {
			dom.Close()
		}
	}()

	warnings := recorded.FilterMessage("advertised status endpoint already has an active claim").All()
	require.Len(t, warnings, 1)
	servingID := dom.DDL().GetID()
	warningFields := warnings[0].ContextMap()
	require.Equal(t, endpoint, warningFields["advertised-status-endpoint"])
	require.Equal(t, claimKey, warningFields["claim-key"])
	require.Equal(t, servingID, warningFields["local-server-info-id"])
	require.Equal(t, "existing-server", warningFields["existing-server-info-id"])
	require.Equal(t, util.FormatLeaseID(claimLease.ID), warningFields["existing-lease-id"])
	require.NotEmpty(t, warningFields["action"])

	// Start again without the external claim. The initialization Domain closes before
	// BootstrapSession returns, so only the serving Domain can leave this claim behind.
	dom.Close()
	dom = nil
	_, err = etcdClient.Delete(ctx, claimKey)
	require.NoError(t, err)
	dom, err = BootstrapSession(mockStore)
	require.NoError(t, err)
	servingID = dom.DDL().GetID()
	claimResp, err := etcdClient.Get(ctx, claimKey)
	require.NoError(t, err)
	require.Len(t, claimResp.Kvs, 1)
	require.Equal(t, servingID, string(claimResp.Kvs[0].Value))
	serverInfoResp, err := etcdClient.Get(ctx, "/tidb/server/info/"+servingID)
	require.NoError(t, err)
	require.Len(t, serverInfoResp.Kvs, 1)
	require.NotZero(t, claimResp.Kvs[0].Lease)
	require.Equal(t, serverInfoResp.Kvs[0].Lease, claimResp.Kvs[0].Lease)
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

	t.Run("standalone parse carries into next statement only once", func(t *testing.T) {
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
		require.Same(t, se.sessionVars.RUV2Metrics, dctx.RUV2Metrics)
	})

	t.Run("internal others bypass skips parser ru accounting", func(t *testing.T) {
		stmt, err := se.ParseWithParams(context.Background(), "set @b=1")
		require.NoError(t, err)
		require.Equal(t, int64(1), se.sessionVars.RUV2PendingSessionParserTotal.Load())

		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
		_, err = se.ExecuteStmt(ctx, stmt)
		require.NoError(t, err)
		require.Zero(t, se.sessionVars.RUV2PendingSessionParserTotal.Load())
		require.NotNil(t, se.sessionVars.RUV2Metrics)
		require.True(t, se.sessionVars.RUV2Metrics.Bypass())
		require.Zero(t, se.sessionVars.RUV2Metrics.SessionParserTotal())
	})

	t.Run("statement bypass decision follows internal analyze semantics", func(t *testing.T) {
		statsCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
		origIsNextGenForRUV2 := isNextGenForRUV2
		defer func() {
			isNextGenForRUV2 = origIsNextGenForRUV2
		}()

		MustExec(t, se, "use test")
		MustExec(t, se, "drop table if exists bypass_prepare")
		MustExec(t, se, "create table bypass_prepare (a int)")

		stmtID, _, _, err := se.PrepareStmt("analyze table bypass_prepare")
		require.NoError(t, err)
		prepStmt, err := se.GetSessionVars().GetPreparedStmtByID(stmtID)
		require.NoError(t, err)
		execAnalyzeStmt := &ast.ExecuteStmt{PrepStmt: prepStmt}

		isNextGenForRUV2 = func() bool { return true }
		require.True(t, shouldBypass(statsCtx, &ast.AnalyzeTableStmt{}, se.sessionVars))
		require.True(t, shouldBypass(statsCtx, execAnalyzeStmt, se.sessionVars))

		isNextGenForRUV2 = func() bool { return false }
		require.False(t, shouldBypass(statsCtx, &ast.AnalyzeTableStmt{}, se.sessionVars))
		require.False(t, shouldBypass(statsCtx, execAnalyzeStmt, se.sessionVars))
		require.False(t, shouldBypass(statsCtx, &ast.SelectStmt{}, se.sessionVars))
	})

	t.Run("current-session restricted sql restores outer ruv2 metrics", func(t *testing.T) {
		outerCtx := execdetails.ContextWithInitializedExecDetails(context.Background())
		outerMetrics := execdetails.RUV2MetricsFromContext(outerCtx)
		require.NotNil(t, outerMetrics)
		se.sessionVars.RUV2Metrics = outerMetrics

		internalCtx := kv.WithInternalSourceType(outerCtx, kv.InternalTxnOthers)
		_, _, err := se.ExecRestrictedSQL(internalCtx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, "select 1")
		require.NoError(t, err)

		require.Same(t, outerMetrics, se.sessionVars.RUV2Metrics)
		require.False(t, outerMetrics.Bypass())
	})
}

func TestCrossKSSessionDistSQLCtxDoesNotExposeTypedNilRUReporter(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	se, err := createSessionWithOpt(store, nil, nil, nil, nil)
	require.NoError(t, err)

	se.sessionVars.StmtCtx.ResourceGroupName = "default"

	dctx := se.GetDistSQLCtx()
	require.True(t, dctx.RUConsumptionReporter == nil)
}

func TestDistSQLCtxPagingSizeBytesRequiresHardCappedResourceGroup(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	oldRCEnabled := vardef.EnableResourceControl.Load()
	vardef.EnableResourceControl.Store(true)
	defer vardef.EnableResourceControl.Store(oldRCEnabled)

	se, err := createSession(store)
	require.NoError(t, err)
	MustExec(t, se, "create resource group rg_paging_capped ru_per_sec=1000")
	MustExec(t, se, "create resource group rg_paging_unlimited ru_per_sec=1000 burstable=unlimited")

	const pagingSizeBytes = 4 * 1024 * 1024
	se.sessionVars.PagingSizeBytes = pagingSizeBytes

	check := func(resourceGroupName string, rcEnabled bool, expected int) {
		vardef.EnableResourceControl.Store(rcEnabled)
		se.sessionVars.StmtCtx.ResetForRetry()
		se.sessionVars.StmtCtx.ResourceGroupName = resourceGroupName
		require.Equal(t, expected, se.GetDistSQLCtx().PagingSizeBytes)
	}

	check("default", true, 0)
	MustExec(t, se, "alter resource group `default` ru_per_sec=1000 burstable=off")
	check("default", true, pagingSizeBytes)
	check("rg_paging_capped", true, pagingSizeBytes)
	check("rg_paging_unlimited", true, 0)
	check("rg_paging_capped", false, 0)
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

	// Each statement must get a fresh RUV2Metrics object so that RUv2 accounting
	// stays isolated per statement, not reused from a previous one.
	require.NotNil(t, metrics2)
	require.NotSame(t, metricsBegin, metrics1, "stmt1 should have different metrics from BEGIN")
	require.NotSame(t, metrics1, metrics2, "stmt2 should have different metrics from stmt1")

	t.Run("optimistic autocommit retry count respects retry limit", func(t *testing.T) {
		MustExec(t, se, "use test")
		MustExec(t, se, "set @@session.tidb_txn_mode = 'optimistic'")
		MustExec(t, se, "drop table if exists max_retry_count")
		MustExec(t, se, "create table max_retry_count (id int primary key, v int)")
		MustExec(t, se, "insert into max_retry_count values (1, 1)")
		MustExec(t, se, "set @@session.tidb_retry_limit = 1")

		func() {
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/mockCommitError8942", `return(true)`))
			defer func() {
				require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/mockCommitError8942"))
			}()

			_, err = exec(se, "update max_retry_count set v = v + 1 where id = 1")
		}()

		require.Error(t, err)
		require.True(t, kv.ErrTxnRetryable.Equal(err), "error: %s", err)
		require.Equal(t, uint64(1), se.GetSessionVars().StmtCtx.ExecRetryCount)

		MustExec(t, se, "insert into max_retry_count values (2, 2)")
	})

	t.Run("optimistic explicit retry count ignores pre-exec failure", func(t *testing.T) {
		MustExec(t, se, "use test")
		MustExec(t, se, "set @@session.tidb_txn_mode = 'optimistic'")
		MustExec(t, se, "drop table if exists pre_exec_retry_count")
		MustExec(t, se, "create table pre_exec_retry_count (id int primary key, v int)")
		MustExec(t, se, "insert into pre_exec_retry_count values (1, 1)")
		MustExec(t, se, "set @@session.tidb_retry_limit = 1")

		func() {
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/injectOptimisticTxnRetryable", `return(true)`))
			defer func() {
				require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/injectOptimisticTxnRetryable"))
			}()
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/mockCommitError8942", `return(true)`))
			defer func() {
				require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/mockCommitError8942"))
			}()
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/txnRetryPreExecError", `return(true)`))
			defer func() {
				require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/txnRetryPreExecError"))
			}()

			MustExec(t, se, "begin")
			MustExec(t, se, "update pre_exec_retry_count set v = v + 1 where id = 1")
			_, err = exec(se, "commit")
		}()

		require.Error(t, err)
		require.ErrorContains(t, err, "mock txn retry pre-exec error")
		require.Equal(t, uint64(0), se.GetSessionVars().StmtCtx.ExecRetryCount)

		MustExec(t, se, "insert into pre_exec_retry_count values (2, 2)")
	})
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
