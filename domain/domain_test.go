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
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/mock"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
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

func TestStatWorkRecoverFromPanic(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)

	ddlLease := 80 * time.Millisecond
	dom := NewDomain(store, ddlLease, 0, 0, 0, mockFactory, nil)

	metrics.PanicCounter.Reset()
	// Since the stats lease is 0 now, so create a new ticker will panic.
	// Test that they can recover from panic correctly.
	dom.updateStatsWorker(mock.NewContext(), nil)
	dom.autoAnalyzeWorker(nil)
	counter := metrics.PanicCounter.WithLabelValues(metrics.LabelDomain)
	pb := &dto.Metric{}
	err = counter.Write(pb)
	require.NoError(t, err)
	require.Equal(t, float64(2), pb.GetCounter().GetValue())

	scope := dom.GetScope("status")
	require.Equal(t, variable.DefaultStatusVarScopeFlag, scope)

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
