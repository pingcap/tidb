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
	"encoding/json"
	"fmt"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
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
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestInfo(t *testing.T) {
	t.Skip("TestInfo will hang currently, it should be fixed later")

	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}

	integration.BeforeTestExternal(t)

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
	dom := NewDomain(mockStore, ddlLease, 0, 0, 0, mockFactory)
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/NoDDLDispatchLoop", `return(true)`))
	require.NoError(t, dom.Init(ddlLease, sysMockFactory, nil))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/NoDDLDispatchLoop"))
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/syncer/ErrorMockSessionDone", `return(true)`))
	<-dom.ddl.SchemaSyncer().Done()
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/syncer/ErrorMockSessionDone"))
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

	stmt := &ast.CreateDatabaseStmt{
		Name: model.NewCIStr("aaa"),
		// Make sure loading schema is normal.
		Options: []*ast.DatabaseOption{
			{
				Tp:    ast.DatabaseOptionCharset,
				Value: "utf8",
			},
			{
				Tp:    ast.DatabaseOptionCollate,
				Value: "utf8_bin",
			},
		},
	}
	ctx := mock.NewContext()
	require.NoError(t, dom.ddl.CreateSchema(ctx, stmt))
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
	dom := NewDomain(store, ddlLease, 0, 0, 0, mockFactory)

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

func TestClosestReplicaReadChecker(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)

	ddlLease := 80 * time.Millisecond
	dom := NewDomain(store, ddlLease, 0, 0, 0, mockFactory)
	defer func() {
		dom.Close()
		require.Nil(t, store.Close())
	}()
	dom.sysVarCache.Lock()
	dom.sysVarCache.global = map[string]string{
		variable.TiDBReplicaRead: "closest-adaptive",
	}
	dom.sysVarCache.Unlock()

	makeFailpointRes := func(v interface{}) string {
		bytes, err := json.Marshal(v)
		require.NoError(t, err)
		return fmt.Sprintf("return(`%s`)", string(bytes))
	}

	mockedAllServerInfos := map[string]*infosync.ServerInfo{
		"s1": {
			ID: "s1",
			Labels: map[string]string{
				"zone": "zone1",
			},
		},
		"s2": {
			ID: "s2",
			Labels: map[string]string{
				"zone": "zone2",
			},
		},
	}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/infosync/mockGetAllServerInfo", makeFailpointRes(mockedAllServerInfos)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/infosync/mockGetServerInfo", makeFailpointRes(mockedAllServerInfos["s2"])))

	stores := []*metapb.Store{
		{
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "zone1",
				},
			},
		},
		{
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "zone2",
				},
			},
		},
		{
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "zone3",
				},
			},
		},
	}

	enabled := variable.IsAdaptiveReplicaReadEnabled()

	ctx := context.Background()
	pdClient := &mockInfoPdClient{}

	// check error
	pdClient.err = errors.New("mock error")
	err = dom.checkReplicaRead(ctx, pdClient)
	require.Error(t, err)
	require.Equal(t, enabled, variable.IsAdaptiveReplicaReadEnabled())

	// labels matches, should be enabled
	pdClient.err = nil
	pdClient.stores = stores[:2]
	variable.SetEnableAdaptiveReplicaRead(false)
	err = dom.checkReplicaRead(ctx, pdClient)
	require.Nil(t, err)
	require.True(t, variable.IsAdaptiveReplicaReadEnabled())

	// labels don't match, should disable the flag
	for _, i := range []int{0, 1, 3} {
		pdClient.stores = stores[:i]
		variable.SetEnableAdaptiveReplicaRead(true)
		err = dom.checkReplicaRead(ctx, pdClient)
		require.Nil(t, err)
		require.False(t, variable.IsAdaptiveReplicaReadEnabled())
	}

	// partial matches
	mockedAllServerInfos = map[string]*infosync.ServerInfo{
		"s1": {
			ID: "s1",
			Labels: map[string]string{
				"zone": "zone1",
			},
		},
		"s2": {
			ID: "s2",
			Labels: map[string]string{
				"zone": "zone2",
			},
		},
		"s22": {
			ID: "s22",
			Labels: map[string]string{
				"zone": "zone2",
			},
		},
		"s3": {
			ID: "s3",
			Labels: map[string]string{
				"zone": "zone3",
			},
		},
		"s4": {
			ID: "s4",
			Labels: map[string]string{
				"zone": "zone4",
			},
		},
	}
	pdClient.stores = stores
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/infosync/mockGetAllServerInfo", makeFailpointRes(mockedAllServerInfos)))
	cases := []struct {
		id      string
		matches bool
	}{
		{
			id:      "s1",
			matches: true,
		},
		{
			id:      "s2",
			matches: true,
		},
		{
			id:      "s22",
			matches: false,
		},
		{
			id:      "s3",
			matches: true,
		},
		{
			id:      "s4",
			matches: false,
		},
	}
	for _, c := range cases {
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/infosync/mockGetServerInfo", makeFailpointRes(mockedAllServerInfos[c.id])))
		variable.SetEnableAdaptiveReplicaRead(!c.matches)
		err = dom.checkReplicaRead(ctx, pdClient)
		require.Nil(t, err)
		require.Equal(t, c.matches, variable.IsAdaptiveReplicaReadEnabled())
	}

	variable.SetEnableAdaptiveReplicaRead(true)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/infosync/mockGetAllServerInfo"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/infosync/mockGetServerInfo"))
}

type mockInfoPdClient struct {
	pd.Client
	stores []*metapb.Store
	err    error
}

func (c *mockInfoPdClient) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return c.stores, c.err
}
