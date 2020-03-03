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
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	"context"
	"testing"
	"time"

	"github.com/ngaut/pools"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	dto "github.com/prometheus/client_model/go"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func mockFactory() (pools.Resource, error) {
	return nil, errors.New("mock factory should not be called")
}

func sysMockFactory(dom *Domain) (pools.Resource, error) {
	return nil, nil
}

<<<<<<< HEAD
=======
type mockEtcdBackend struct {
	kv.Storage
	pdAddrs []string
}

func (mebd *mockEtcdBackend) EtcdAddrs() []string {
	return mebd.pdAddrs
}
func (mebd *mockEtcdBackend) TLSConfig() *tls.Config { return nil }
func (mebd *mockEtcdBackend) StartGCWorker() error {
	panic("not implemented")
}

// ETCD use ip:port as unix socket address, however this address is invalid on windows.
// We have to skip some of the test in such case.
// https://github.com/etcd-io/etcd/blob/f0faa5501d936cd8c9f561bb9d1baca70eb67ab1/pkg/types/urls.go#L42
func unixSocketAvailable() bool {
	c, err := net.Listen("unix", "127.0.0.1:0")
	if err == nil {
		c.Close()
		return true
	}
	return false
}

func TestInfo(t *testing.T) {
	if !unixSocketAvailable() {
		return
	}
	defer testleak.AfterTestT(t)()
	ddlLease := 80 * time.Millisecond
	s, err := mockstore.NewMockTikvStore()
	if err != nil {
		t.Fatal(err)
	}
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)
	mockStore := &mockEtcdBackend{
		Storage: s,
		pdAddrs: []string{clus.Members[0].GRPCAddr()}}
	dom := NewDomain(mockStore, ddlLease, 0, mockFactory)
	defer func() {
		dom.Close()
		s.Close()
	}()

	cli := clus.RandClient()
	dom.etcdClient = cli
	// Mock new DDL and init the schema syncer with etcd client.
	goCtx := context.Background()
	dom.ddl = ddl.NewDDL(
		goCtx,
		ddl.WithEtcdClient(dom.GetEtcdClient()),
		ddl.WithStore(s),
		ddl.WithInfoHandle(dom.infoHandle),
		ddl.WithLease(ddlLease),
	)
	err = failpoint.Enable("github.com/pingcap/tidb/domain/MockReplaceDDL", `return(true)`)
	if err != nil {
		t.Fatal(err)
	}
	err = dom.Init(ddlLease, sysMockFactory)
	if err != nil {
		t.Fatal(err)
	}
	err = failpoint.Disable("github.com/pingcap/tidb/domain/MockReplaceDDL")
	if err != nil {
		t.Fatal(err)
	}

	// Test for GetServerInfo and GetServerInfoByID.
	ddlID := dom.ddl.GetID()
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		t.Fatal(err)
	}
	info, err := infosync.GetServerInfoByID(goCtx, ddlID)
	if err != nil {
		t.Fatal(err)
	}
	if serverInfo.ID != info.ID {
		t.Fatalf("server self info %v, info %v", serverInfo, info)
	}
	_, err = infosync.GetServerInfoByID(goCtx, "not_exist_id")
	if err == nil || (err != nil && err.Error() != "[info-syncer] get /tidb/server/info/not_exist_id failed") {
		t.Fatal(err)
	}

	// Test for GetAllServerInfo.
	infos, err := infosync.GetAllServerInfo(goCtx)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[ddlID].ID != info.ID {
		t.Fatalf("server one info %v, info %v", infos[ddlID], info)
	}

	// Test the scene where syncer.Done() gets the information.
	err = failpoint.Enable("github.com/pingcap/tidb/ddl/util/ErrorMockSessionDone", `return(true)`)
	if err != nil {
		t.Fatal(err)
	}
	<-dom.ddl.SchemaSyncer().Done()
	err = failpoint.Disable("github.com/pingcap/tidb/ddl/util/ErrorMockSessionDone")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)
	syncerStarted := false
	for i := 0; i < 200; i++ {
		if dom.SchemaValidator.IsStarted() {
			syncerStarted = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !syncerStarted {
		t.Fatal("start syncer failed")
	}
	// Make sure loading schema is normal.
	cs := &ast.CharsetOpt{
		Chs: "utf8",
		Col: "utf8_bin",
	}
	ctx := mock.NewContext()
	err = dom.ddl.CreateSchema(ctx, model.NewCIStr("aaa"), cs)
	if err != nil {
		t.Fatal(err)
	}
	err = dom.Reload()
	if err != nil {
		t.Fatal(err)
	}
	if dom.InfoSchema().SchemaMetaVersion() != 1 {
		t.Fatalf("update schema version failed, ver %d", dom.InfoSchema().SchemaMetaVersion())
	}

	// Test for RemoveServerInfo.
	dom.info.RemoveServerInfo()
	infos, err = infosync.GetAllServerInfo(goCtx)
	if err != nil || len(infos) != 0 {
		t.Fatalf("err %v, infos %v", err, infos)
	}
}

type mockSessionManager struct {
	PS []*util.ProcessInfo
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

func (msm *mockSessionManager) Kill(cid uint64, query bool) {}

func (msm *mockSessionManager) UpdateTLSConfig(cfg *tls.Config) {}

>>>>>>> 5c68d53... *: support reload tls used by mysql protocol in place (#14749)
func (*testSuite) TestT(c *C) {
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	ddlLease := 80 * time.Millisecond
	dom := NewDomain(store, ddlLease, 0, mockFactory)
	err = dom.Init(ddlLease, sysMockFactory)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
	}()
	store = dom.Store()
	ctx := mock.NewContext()
	ctx.Store = store
	snapTS := oracle.EncodeTSO(oracle.GetPhysical(time.Now()))
	dd := dom.DDL()
	c.Assert(dd, NotNil)
	c.Assert(dd.GetLease(), Equals, 80*time.Millisecond)
	cs := &ast.CharsetOpt{
		Chs: "utf8",
		Col: "utf8_bin",
	}
	err = dd.CreateSchema(ctx, model.NewCIStr("aaa"), cs)
	c.Assert(err, IsNil)
	is := dom.InfoSchema()
	c.Assert(is, NotNil)

	// for setting lease
	lease := 100 * time.Millisecond

	// for updating the self schema version
	goCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err = dd.SchemaSyncer().OwnerCheckAllVersions(goCtx, is.SchemaMetaVersion())
	cancel()
	c.Assert(err, IsNil)
	snapIs, err := dom.GetSnapshotInfoSchema(snapTS)
	c.Assert(snapIs, NotNil)
	c.Assert(err, IsNil)
	// Make sure that the self schema version doesn't be changed.
	goCtx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	err = dd.SchemaSyncer().OwnerCheckAllVersions(goCtx, is.SchemaMetaVersion())
	cancel()
	c.Assert(err, IsNil)

	// for GetSnapshotInfoSchema
	snapTS = oracle.EncodeTSO(oracle.GetPhysical(time.Now()))
	snapIs, err = dom.GetSnapshotInfoSchema(snapTS)
	c.Assert(err, IsNil)
	c.Assert(snapIs, NotNil)
	c.Assert(snapIs.SchemaMetaVersion(), Equals, is.SchemaMetaVersion())

	// for schemaValidator
	schemaVer := dom.SchemaValidator.(*schemaValidator).LatestSchemaVersion()
	ver, err := store.CurrentVersion()
	c.Assert(err, IsNil)
	ts := ver.Ver

	succ := dom.SchemaValidator.Check(ts, schemaVer, nil)
	c.Assert(succ, Equals, ResultSucc)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/domain/ErrorMockReloadFailed", `return(true)`), IsNil)
	err = dom.Reload()
	c.Assert(err, NotNil)
	succ = dom.SchemaValidator.Check(ts, schemaVer, nil)
	c.Assert(succ, Equals, ResultSucc)
	time.Sleep(lease)

	ver, err = store.CurrentVersion()
	c.Assert(err, IsNil)
	ts = ver.Ver
	succ = dom.SchemaValidator.Check(ts, schemaVer, nil)
	c.Assert(succ, Equals, ResultUnknown)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/domain/ErrorMockReloadFailed"), IsNil)
	err = dom.Reload()
	c.Assert(err, IsNil)
	succ = dom.SchemaValidator.Check(ts, schemaVer, nil)
	c.Assert(succ, Equals, ResultSucc)

	// For slow query.
	dom.LogSlowQuery(&SlowQueryInfo{SQL: "aaa", Duration: time.Second, Internal: true})
	dom.LogSlowQuery(&SlowQueryInfo{SQL: "bbb", Duration: 3 * time.Second})
	dom.LogSlowQuery(&SlowQueryInfo{SQL: "ccc", Duration: 2 * time.Second})
	// Collecting slow queries is asynchronous, wait a while to ensure it's done.
	time.Sleep(5 * time.Millisecond)

	res := dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 2})
	c.Assert(res, HasLen, 2)
	c.Assert(*res[0], Equals, SlowQueryInfo{SQL: "bbb", Duration: 3 * time.Second})
	c.Assert(*res[1], Equals, SlowQueryInfo{SQL: "ccc", Duration: 2 * time.Second})

	res = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 2, Kind: ast.ShowSlowKindInternal})
	c.Assert(res, HasLen, 1)
	c.Assert(*res[0], Equals, SlowQueryInfo{SQL: "aaa", Duration: time.Second, Internal: true})

	res = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 4, Kind: ast.ShowSlowKindAll})
	c.Assert(res, HasLen, 3)
	c.Assert(*res[0], Equals, SlowQueryInfo{SQL: "bbb", Duration: 3 * time.Second})
	c.Assert(*res[1], Equals, SlowQueryInfo{SQL: "ccc", Duration: 2 * time.Second})
	c.Assert(*res[2], Equals, SlowQueryInfo{SQL: "aaa", Duration: time.Second, Internal: true})

	res = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowRecent, Count: 2})
	c.Assert(res, HasLen, 2)
	c.Assert(*res[0], Equals, SlowQueryInfo{SQL: "ccc", Duration: 2 * time.Second})
	c.Assert(*res[1], Equals, SlowQueryInfo{SQL: "bbb", Duration: 3 * time.Second})

	metrics.PanicCounter.Reset()
	// Since the stats lease is 0 now, so create a new ticker will panic.
	// Test that they can recover from panic correctly.
	dom.updateStatsWorker(ctx, nil)
	dom.autoAnalyzeWorker(nil)
	counter := metrics.PanicCounter.WithLabelValues(metrics.LabelDomain)
	pb := &dto.Metric{}
	counter.Write(pb)
	c.Assert(pb.GetCounter().GetValue(), Equals, float64(2))

	err = store.Close()
	c.Assert(err, IsNil)
}

func (*testSuite) TestErrorCode(c *C) {
	c.Assert(int(ErrInfoSchemaExpired.ToSQLError().Code), Equals, mysql.ErrUnknown)
	c.Assert(int(ErrInfoSchemaChanged.ToSQLError().Code), Equals, mysql.ErrUnknown)
}
