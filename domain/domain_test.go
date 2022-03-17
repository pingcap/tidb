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

package domain

import (
	"context"
	"crypto/tls"
	"math"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/mock"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestInfo(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}

	integration.BeforeTest(t)

	if !unixSocketAvailable() {
		t.Skip("ETCD use ip:port as unix socket address, skip when it is unavailable.")
	}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/infosync/FailPlacement", `return(true)`))

	s, err := mockstore.NewMockStore()
	require.NoError(t, err)

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)

	mockStore := &mockEtcdBackend{
		Storage: s,
		pdAddrs: []string{cluster.Members[0].GRPCURL()}}
	ddlLease := 80 * time.Millisecond
	dom := NewDomain(mockStore, ddlLease, 0, 0, 0, mockFactory, nil)
	defer func() {
		dom.Close()
		err := s.Close()
		require.NoError(t, err)
	}()

	client := cluster.RandClient()
	dom.etcdClient = client
	// Mock new DDL and init the schema syncer with etcd client.
	goCtx := context.Background()
	dom.ddl = ddl.NewDDL(
		goCtx,
		ddl.WithEtcdClient(dom.GetEtcdClient()),
		ddl.WithStore(s),
		ddl.WithInfoCache(dom.infoCache),
		ddl.WithLease(ddlLease),
	)
	ddl.DisableTiFlashPoll(dom.ddl)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/MockReplaceDDL", `return(true)`))
	require.NoError(t, dom.Init(ddlLease, sysMockFactory))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/MockReplaceDDL"))

	// Test for GetServerInfo and GetServerInfoByID.
	ddlID := dom.ddl.GetID()
	serverInfo, err := infosync.GetServerInfo()
	require.NoError(t, err)

	info, err := infosync.GetServerInfoByID(goCtx, ddlID)
	require.NoError(t, err)

	if serverInfo.ID != info.ID {
		t.Fatalf("server self info %v, info %v", serverInfo, info)
	}

	_, err = infosync.GetServerInfoByID(goCtx, "not_exist_id")
	require.Error(t, err)
	require.Equal(t, "[info-syncer] get /tidb/server/info/not_exist_id failed", err.Error())

	// Test for GetAllServerInfo.
	infos, err := infosync.GetAllServerInfo(goCtx)
	require.NoError(t, err)
	require.Lenf(t, infos, 1, "server one info %v, info %v", infos[ddlID], info)
	require.Equalf(t, info.ID, infos[ddlID].ID, "server one info %v, info %v", infos[ddlID], info)

	// Test the scene where syncer.Done() gets the information.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/util/ErrorMockSessionDone", `return(true)`))
	<-dom.ddl.SchemaSyncer().Done()
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/util/ErrorMockSessionDone"))
	time.Sleep(15 * time.Millisecond)
	syncerStarted := false
	for i := 0; i < 1000; i++ {
		if dom.SchemaValidator.IsStarted() {
			syncerStarted = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.True(t, syncerStarted)

	// Make sure loading schema is normal.
	cs := &ast.CharsetOpt{
		Chs: "utf8",
		Col: "utf8_bin",
	}
	ctx := mock.NewContext()
	require.NoError(t, dom.ddl.CreateSchema(ctx, model.NewCIStr("aaa"), cs, nil))
	require.NoError(t, dom.Reload())
	require.Equal(t, int64(1), dom.InfoSchema().SchemaMetaVersion())

	// Test for RemoveServerInfo.
	dom.info.RemoveServerInfo()
	infos, err = infosync.GetAllServerInfo(goCtx)
	require.NoError(t, err)
	require.Len(t, infos, 0)

	// Test for acquireServerID & refreshServerIDTTL
	err = dom.acquireServerID(goCtx)
	require.NoError(t, err)
	require.NotEqual(t, 0, dom.ServerID())

	err = dom.refreshServerIDTTL(goCtx)
	require.NoError(t, err)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/infosync/FailPlacement"))
}

func TestDomain(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)

	ddlLease := 80 * time.Millisecond
	dom := NewDomain(store, ddlLease, 0, 0, 0, mockFactory, nil)
	err = dom.Init(ddlLease, sysMockFactory)
	require.NoError(t, err)

	ctx := mock.NewContext()
	ctx.Store = dom.Store()
	dd := dom.DDL()
	ddl.DisableTiFlashPoll(dd)
	require.NotNil(t, dd)
	require.Equal(t, 80*time.Millisecond, dd.GetLease())

	snapTS := oracle.GoTimeToTS(time.Now())
	cs := &ast.CharsetOpt{
		Chs: "utf8",
		Col: "utf8_bin",
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = dd.CreateSchema(ctx, model.NewCIStr("aaa"), cs, nil)
	require.NoError(t, err)

	// Test for fetchSchemasWithTables when "tables" isn't nil.
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = dd.CreateTable(ctx, &ast.CreateTableStmt{Table: &ast.TableName{
		Schema: model.NewCIStr("aaa"),
		Name:   model.NewCIStr("tbl")}})
	require.NoError(t, err)

	is := dom.InfoSchema()
	require.NotNil(t, is)

	// for updating the self schema version
	goCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = dd.SchemaSyncer().OwnerCheckAllVersions(goCtx, is.SchemaMetaVersion())
	cancel()
	require.NoError(t, err)

	snapIs, err := dom.GetSnapshotInfoSchema(snapTS)
	require.NotNil(t, snapIs)
	require.NoError(t, err)

	// Make sure that the self schema version doesn't be changed.
	goCtx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = dd.SchemaSyncer().OwnerCheckAllVersions(goCtx, is.SchemaMetaVersion())
	cancel()
	require.NoError(t, err)

	// for GetSnapshotInfoSchema
	currSnapTS := oracle.GoTimeToTS(time.Now())
	currSnapIs, err := dom.GetSnapshotInfoSchema(currSnapTS)
	require.NoError(t, err)
	require.NotNil(t, currSnapTS)
	require.Equal(t, is.SchemaMetaVersion(), currSnapIs.SchemaMetaVersion())

	// for GetSnapshotMeta
	dbInfo, ok := currSnapIs.SchemaByName(model.NewCIStr("aaa"))
	require.True(t, ok)

	tbl, err := currSnapIs.TableByName(model.NewCIStr("aaa"), model.NewCIStr("tbl"))
	require.NoError(t, err)

	m, err := dom.GetSnapshotMeta(snapTS)
	require.NoError(t, err)

	tblInfo1, err := m.GetTable(dbInfo.ID, tbl.Meta().ID)
	require.True(t, meta.ErrDBNotExists.Equal(err))
	require.Nil(t, tblInfo1)

	m, err = dom.GetSnapshotMeta(currSnapTS)
	require.NoError(t, err)

	tblInfo2, err := m.GetTable(dbInfo.ID, tbl.Meta().ID)
	require.NoError(t, err)
	require.Equal(t, tblInfo2, tbl.Meta())

	// Test for tryLoadSchemaDiffs when "isTooOldSchema" is false.
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = dd.CreateSchema(ctx, model.NewCIStr("bbb"), cs, nil)
	require.NoError(t, err)

	err = dom.Reload()
	require.NoError(t, err)

	// for schemaValidator
	schemaVer := dom.SchemaValidator.(*schemaValidator).LatestSchemaVersion()
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)

	ts := ver.Ver
	_, res := dom.SchemaValidator.Check(ts, schemaVer, nil)
	require.Equal(t, ResultSucc, res)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/ErrorMockReloadFailed", `return(true)`))

	err = dom.Reload()
	require.Error(t, err)
	_, res = dom.SchemaValidator.Check(ts, schemaVer, nil)
	require.Equal(t, ResultSucc, res)
	time.Sleep(ddlLease)

	ver, err = store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	ts = ver.Ver
	_, res = dom.SchemaValidator.Check(ts, schemaVer, nil)
	require.Equal(t, ResultUnknown, res)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/ErrorMockReloadFailed"))
	err = dom.Reload()
	require.NoError(t, err)

	_, res = dom.SchemaValidator.Check(ts, schemaVer, nil)
	require.Equal(t, ResultSucc, res)

	// For slow query.
	dom.LogSlowQuery(&SlowQueryInfo{SQL: "aaa", Duration: time.Second, Internal: true})
	dom.LogSlowQuery(&SlowQueryInfo{SQL: "bbb", Duration: 3 * time.Second})
	dom.LogSlowQuery(&SlowQueryInfo{SQL: "ccc", Duration: 2 * time.Second})
	// Collecting slow queries is asynchronous, wait a while to ensure it's done.
	time.Sleep(5 * time.Millisecond)

	result := dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 2})
	require.Len(t, result, 2)
	require.Equal(t, "bbb", result[0].SQL)
	require.Equal(t, 3*time.Second, result[0].Duration)
	require.Equal(t, "ccc", result[1].SQL)
	require.Equal(t, 2*time.Second, result[1].Duration)

	result = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 2, Kind: ast.ShowSlowKindInternal})
	require.Len(t, result, 1)
	require.Equal(t, "aaa", result[0].SQL)
	require.Equal(t, time.Second, result[0].Duration)
	require.True(t, result[0].Internal)

	result = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 4, Kind: ast.ShowSlowKindAll})
	require.Len(t, result, 3)
	require.Equal(t, "bbb", result[0].SQL)
	require.Equal(t, 3*time.Second, result[0].Duration)
	require.Equal(t, "ccc", result[1].SQL)
	require.Equal(t, 2*time.Second, result[1].Duration)
	require.Equal(t, "aaa", result[2].SQL)
	require.Equal(t, time.Second, result[2].Duration)
	require.True(t, result[2].Internal)

	result = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowRecent, Count: 2})
	require.Len(t, result, 2)
	require.Equal(t, "ccc", result[0].SQL)
	require.Equal(t, 2*time.Second, result[0].Duration)
	require.Equal(t, "bbb", result[1].SQL)
	require.Equal(t, 3*time.Second, result[1].Duration)

	metrics.PanicCounter.Reset()
	// Since the stats lease is 0 now, so create a new ticker will panic.
	// Test that they can recover from panic correctly.
	dom.updateStatsWorker(ctx, nil)
	dom.autoAnalyzeWorker(nil)
	counter := metrics.PanicCounter.WithLabelValues(metrics.LabelDomain)
	pb := &dto.Metric{}
	err = counter.Write(pb)
	require.NoError(t, err)
	require.Equal(t, float64(2), pb.GetCounter().GetValue())

	scope := dom.GetScope("status")
	require.Equal(t, variable.DefaultStatusVarScopeFlag, scope)

	// For schema check, it tests for getting the result of "ResultUnknown".
	schemaChecker := NewSchemaChecker(dom, is.SchemaMetaVersion(), nil)
	originalRetryTime := SchemaOutOfDateRetryTimes.Load()
	originalRetryInterval := SchemaOutOfDateRetryInterval.Load()
	// Make sure it will retry one time and doesn't take a long time.
	SchemaOutOfDateRetryTimes.Store(1)
	SchemaOutOfDateRetryInterval.Store(time.Millisecond * 1)
	defer func() {
		SchemaOutOfDateRetryTimes.Store(originalRetryTime)
		SchemaOutOfDateRetryInterval.Store(originalRetryInterval)
	}()
	dom.SchemaValidator.Stop()
	_, err = schemaChecker.Check(uint64(123456))
	require.EqualError(t, err, ErrInfoSchemaExpired.Error())
	dom.SchemaValidator.Reset()

	// Test for reporting min start timestamp.
	infoSyncer := dom.InfoSyncer()
	sm := &mockSessionManager{
		PS: make([]*util.ProcessInfo, 0),
	}
	infoSyncer.SetSessionManager(sm)
	beforeTS := oracle.GoTimeToTS(time.Now())
	infoSyncer.ReportMinStartTS(dom.Store())
	afterTS := oracle.GoTimeToTS(time.Now())
	require.False(t, infoSyncer.GetMinStartTS() > beforeTS && infoSyncer.GetMinStartTS() < afterTS)

	now := time.Now()
	validTS := oracle.GoTimeToLowerLimitStartTS(now.Add(time.Minute), tikv.MaxTxnTimeUse)
	lowerLimit := oracle.GoTimeToLowerLimitStartTS(now, tikv.MaxTxnTimeUse)
	sm.PS = []*util.ProcessInfo{
		{CurTxnStartTS: 0},
		{CurTxnStartTS: math.MaxUint64},
		{CurTxnStartTS: lowerLimit},
		{CurTxnStartTS: validTS},
	}
	infoSyncer.SetSessionManager(sm)
	infoSyncer.ReportMinStartTS(dom.Store())
	require.Equal(t, validTS, infoSyncer.GetMinStartTS())

	err = store.Close()
	require.NoError(t, err)

	isClose := dom.isClose()
	require.False(t, isClose)

	dom.Close()
	isClose = dom.isClose()
	require.True(t, isClose)
}

// ETCD use ip:port as unix socket address, however this address is invalid on windows.
// We have to skip some of the test in such case.
// https://github.com/etcd-io/etcd/blob/f0faa5501d936cd8c9f561bb9d1baca70eb67ab1/pkg/types/urls.go#L42
func unixSocketAvailable() bool {
	c, err := net.Listen("unix", "127.0.0.1:0")
	if err == nil {
		_ = c.Close()
		return true
	}
	return false
}

func mockFactory() (pools.Resource, error) {
	return nil, errors.New("mock factory should not be called")
}

func sysMockFactory(*Domain) (pools.Resource, error) {
	return nil, nil
}

type mockEtcdBackend struct {
	kv.Storage
	pdAddrs []string
}

func (mebd *mockEtcdBackend) EtcdAddrs() ([]string, error) {
	return mebd.pdAddrs, nil
}

func (mebd *mockEtcdBackend) TLSConfig() *tls.Config { return nil }

func (mebd *mockEtcdBackend) StartGCWorker() error {
	panic("not implemented")
}

type mockSessionManager struct {
	PS []*util.ProcessInfo
}

func (msm *mockSessionManager) ShowTxnList() []*txninfo.TxnInfo {
	panic("unimplemented!")
}

func (msm *mockSessionManager) ShowProcessList() map[uint64]*util.ProcessInfo {
	ret := make(map[uint64]*util.ProcessInfo)
	for _, item := range msm.PS {
		ret[item.ID] = item
	}
	return ret
}

func (msm *mockSessionManager) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	for _, item := range msm.PS {
		if item.ID == id {
			return item, true
		}
	}
	return &util.ProcessInfo{}, false
}

func (msm *mockSessionManager) Kill(uint64, bool) {}

func (msm *mockSessionManager) KillAllConnections() {}

func (msm *mockSessionManager) UpdateTLSConfig(*tls.Config) {}

func (msm *mockSessionManager) ServerID() uint64 {
	return 1
}
