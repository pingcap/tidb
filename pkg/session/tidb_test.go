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

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/breakpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type recordingObserver struct {
	count int
}

func (o *recordingObserver) Observe(float64) {
	o.count++
}

func TestSharedLockLostRollsBackTransaction(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	testCases := []struct {
		name           string
		beginSQL       string
		pessimistic    bool
		sharedLockLost bool
	}{
		{
			name:           "pessimistic shared lock lost",
			beginSQL:       "begin pessimistic",
			pessimistic:    true,
			sharedLockLost: true,
		},
		{
			name:           "optimistic shared lock lost mode mismatch",
			beginSQL:       "begin optimistic",
			pessimistic:    false,
			sharedLockLost: true,
		},
		{
			name:           "pessimistic deadlock unchanged",
			beginSQL:       "begin pessimistic",
			pessimistic:    true,
			sharedLockLost: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			se, err := createSession(store)
			require.NoError(t, err)
			defer se.Close()

			MustExec(t, se, testCase.beginSQL)
			txnManager := sessiontxn.GetTxnManager(se)
			txn, err := txnManager.ActivateTxn()
			require.NoError(t, err)
			require.True(t, txn.Valid())
			require.Equal(t, testCase.pessimistic, txn.IsPessimistic())
			require.NotNil(t, txnManager.GetContextProvider())

			recorder := &recordingObserver{}
			var restoreObserver func()
			if testCase.pessimistic {
				originalObserver := session_metrics.TransactionDurationPessimisticAbortGeneral
				session_metrics.TransactionDurationPessimisticAbortGeneral = recorder
				restoreObserver = func() {
					session_metrics.TransactionDurationPessimisticAbortGeneral = originalObserver
				}
			} else {
				originalObserver := session_metrics.TransactionDurationOptimisticAbortGeneral
				session_metrics.TransactionDurationOptimisticAbortGeneral = recorder
				restoreObserver = func() {
					session_metrics.TransactionDurationOptimisticAbortGeneral = originalObserver
				}
			}
			defer restoreObserver()

			var stmtErr error
			if testCase.sharedLockLost {
				stmtErr = kv.ErrSharedLockLost.GenWithStackByArgs(txn.StartTS(), "6B6579")
			} else {
				stmtErr = exeerrors.ErrDeadlock
			}
			got := autoCommitAfterStmt(context.Background(), se, stmtErr, nil)
			require.Same(t, stmtErr, got)
			if testCase.sharedLockLost {
				require.True(t, kv.ErrSharedLockLost.Equal(got))
			} else {
				require.True(t, exeerrors.ErrDeadlock.Equal(got))
			}
			require.False(t, se.sessionVars.InTxn())
			require.False(t, se.txn.Valid())
			require.Nil(t, txnManager.GetContextProvider())
			require.Equal(t, 1, recorder.count)
		})
	}
}

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
	originalBudget := vardef.PagingSizeBytes.Load()
	t.Cleanup(func() { vardef.PagingSizeBytes.Store(originalBudget) })

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
	MustExec(t, se, "set global tidb_paging_size_bytes = 4194304")

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

func TestDistSQLCtxPagingSizeBytesGlobalUpdate(t *testing.T) {
	originalBudget := vardef.PagingSizeBytes.Load()
	t.Cleanup(func() { vardef.PagingSizeBytes.Store(originalBudget) })

	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	writer, err := createSession(store)
	require.NoError(t, err)
	defer writer.Close()
	oldBudget, err := writer.GetGlobalSysVar(vardef.TiDBPagingSizeBytes)
	require.NoError(t, err)
	oldRC, err := writer.GetGlobalSysVar(vardef.TiDBEnableResourceControl)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, writer.SetGlobalSysVar(context.Background(), vardef.TiDBPagingSizeBytes, oldBudget))
		require.NoError(t, writer.SetGlobalSysVar(context.Background(), vardef.TiDBEnableResourceControl, oldRC))
	}()
	MustExec(t, writer, "set global tidb_enable_resource_control = on")
	MustExec(t, writer, "set global tidb_paging_size_bytes = 0")
	MustExec(t, writer, "create resource group rg_paging_global ru_per_sec=1000 burstable=off")

	reader, err := createSession(store)
	require.NoError(t, err)
	defer reader.Close()
	MustExec(t, reader, "set resource group rg_paging_global")
	MustExec(t, reader, "begin")
	MustExec(t, reader, "select 1")
	previous := reader.GetDistSQLCtx()
	require.Zero(t, previous.PagingSizeBytes)

	for _, tc := range []struct {
		value string
		bytes int
	}{
		{"4194304", 4 * 1024 * 1024},
		{"1048576", 1024 * 1024},
		{"8388608", 8 * 1024 * 1024},
		{"default", 0},
		{"4194304", 4 * 1024 * 1024},
		{"0", 0},
	} {
		previousBudget := previous.PagingSizeBytes
		MustExec(t, writer, "set global tidb_paging_size_bytes = "+tc.value)
		// Updating another session must not change an initialized context.
		require.Same(t, previous, reader.GetDistSQLCtx())
		require.Equal(t, previousBudget, reader.GetDistSQLCtx().PagingSizeBytes)

		MustExec(t, reader, "select 1")
		current := reader.GetDistSQLCtx()
		require.NotSame(t, previous, current)
		require.Equal(t, tc.bytes, current.PagingSizeBytes)
		require.True(t, reader.sessionVars.InTxn())
		previous = current
	}
	MustExec(t, reader, "rollback")

	MustExec(t, writer, "set global tidb_paging_size_bytes = 4194304")
	newReader, err := createSession(store)
	require.NoError(t, err)
	defer newReader.Close()
	MustExec(t, newReader, "set resource group rg_paging_global")
	rs := MustExecToRecodeSet(t, newReader, "select @@global.tidb_paging_size_bytes, @@tidb_paging_size_bytes")
	rows, err := ResultSetToStringSlice(context.Background(), newReader, rs)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"4194304", "4194304"}}, rows)
	require.Equal(t, 4*1024*1024, newReader.GetDistSQLCtx().PagingSizeBytes)

	// Cache rebuilds must restore the persisted budget, as on startup or a peer update.
	vardef.PagingSizeBytes.Store(0)
	dom.NotifyUpdateSysVarCache(true)
	MustExec(t, reader, "select 1")
	require.Equal(t, 4*1024*1024, reader.GetDistSQLCtx().PagingSizeBytes)
}

func TestScalarSubqueryRegistryTxnReplay(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()
	se, err := createSession(store)
	require.NoError(t, err)
	defer se.Close()
	MustExec(t, se, "use test")
	MustExec(t, se, "set tidb_disable_txn_auto_retry = off")
	MustExec(t, se, "set tidb_retry_limit = 3")
	MustExec(t, se, "set tidb_enable_non_prepared_plan_cache_for_dml = off")
	MustExec(t, se, "create table scalar_registry_retry (id int primary key, v int)")
	MustExec(t, se, "insert into scalar_registry_retry values (1, 1), (2, 2)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/sessiontxn/isolation/injectOptimisticTxnRetryable", "return(true)")
	MustExec(t, se, "begin optimistic")
	MustExec(t, se, "update scalar_registry_retry set v = (select max(v) from scalar_registry_retry) where id = 1")
	require.Len(t, se.GetSessionVars().MapScalarSubQ, 1)
	MustExec(t, se, "update scalar_registry_retry set v = v + 1 where id = 2")

	// Observe each rebuilt UPDATE before execution, using the existing
	// session-scoped breakpoint rather than only inspecting COMMIT's context.
	var registrySizes, scalarTreeCounts []int
	var replayPlans []base.Plan
	se.SetValue(breakpoint.NotifyBreakPointFuncKey, func(_ string) {
		vars := se.GetSessionVars()
		if !vars.RetryInfo.Retrying {
			return
		}
		plan, ok := vars.StmtCtx.GetPlan().(base.Plan)
		require.True(t, ok)
		if _, ok := plan.(*physicalop.Update); !ok {
			return
		}
		replayPlans = append(replayPlans, plan)
		registrySizes = append(registrySizes, len(vars.MapScalarSubQ))
		scalarTreeCounts = append(scalarTreeCounts, len(plannercore.FlattenPhysicalPlan(plan, true).ScalarSubQueries))
	})
	defer se.ClearValue(breakpoint.NotifyBreakPointFuncKey)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/breakpoint/"+sessiontxn.BreakPointBeforeExecutorFirstRun, "return")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/session/mockCommitError8942", "1*return(true)->return(false)")
	MustExec(t, se, "commit")
	require.Equal(t, uint64(1), se.GetSessionVars().StmtCtx.ExecRetryCount)
	t.Logf("replay registry sizes=%v flattened scalar trees=%v", registrySizes, scalarTreeCounts)

	rs := MustExecToRecodeSet(t, se, "select id, v from scalar_registry_retry order by id")
	rows, err := ResultSetToStringSlice(context.Background(), se, rs)
	require.NoError(t, err)
	require.NoError(t, rs.Close())
	require.Equal(t, [][]string{{"1", "2"}, {"2", "3"}}, rows)
	require.Len(t, replayPlans, 2)
	update, ok := replayPlans[1].(*physicalop.Update)
	require.True(t, ok)
	require.IsType(t, &physicalop.PointGetPlan{}, update.SelectPlan)
	require.Equal(t, []int{1, 0}, scalarTreeCounts)
	require.Equal(t, []int{1, 0}, registrySizes)
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
