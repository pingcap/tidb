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

package executor_test

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/fn"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type infosSchemaClusterTableSuite struct {
	store      kv.Storage
	dom        *domain.Domain
	clean      func()
	rpcServer  *grpc.Server
	httpServer *httptest.Server
	mockAddr   string
	listenAddr string
	startTime  time.Time
}

func createInfosSchemaClusterTableSuite(t *testing.T) *infosSchemaClusterTableSuite {
	var clean func()

	s := new(infosSchemaClusterTableSuite)
	s.store, s.dom, clean = testkit.CreateMockStoreAndDomain(t)
	s.rpcServer, s.listenAddr = setUpRPCService(t, s.dom, "127.0.0.1:0")
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	s.clean = func() {
		s.rpcServer.Stop()
		s.httpServer.Close()
		clean()
	}

	return s
}

func setUpRPCService(t *testing.T, dom *domain.Domain, addr string) (*grpc.Server, string) {
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	// Fix issue 9836
	sm := &mockSessionManager{
		processInfoMap: make(map[uint64]*util.ProcessInfo, 1),
		serverID:       1,
	}
	sm.processInfoMap[1] = &util.ProcessInfo{
		ID:      1,
		User:    "root",
		Host:    "127.0.0.1",
		Command: mysql.ComQuery,
	}
	srv := server.NewRPCServer(config.GetGlobalConfig(), dom, sm)
	port := lis.Addr().(*net.TCPAddr).Port
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	go func() {
		require.NoError(t, srv.Serve(lis))
	}()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Status.StatusPort = uint(port)
	})
	return srv, addr
}

func (s *infosSchemaClusterTableSuite) setUpMockPDHTTPServer() (*httptest.Server, string) {
	// mock PD http server
	router := mux.NewRouter()
	srv := httptest.NewServer(router)
	// mock store stats stat
	mockAddr := strings.TrimPrefix(srv.URL, "http://")
	router.Handle(pdapi.Stores, fn.Wrap(func() (*helper.StoresStat, error) {
		return &helper.StoresStat{
			Count: 1,
			Stores: []helper.StoreStat{
				{
					Store: helper.StoreBaseStat{
						ID:             1,
						Address:        "127.0.0.1:20160",
						State:          0,
						StateName:      "Up",
						Version:        "4.0.0-alpha",
						StatusAddress:  mockAddr,
						GitHash:        "mock-tikv-githash",
						StartTimestamp: s.startTime.Unix(),
					},
				},
			},
		}, nil
	}))
	// mock PD API
	router.Handle(pdapi.Status, fn.Wrap(func() (interface{}, error) {
		return struct {
			Version        string `json:"version"`
			GitHash        string `json:"git_hash"`
			StartTimestamp int64  `json:"start_timestamp"`
		}{
			Version:        "4.0.0-alpha",
			GitHash:        "mock-pd-githash",
			StartTimestamp: s.startTime.Unix(),
		}, nil
	}))
	var mockConfig = func() (map[string]interface{}, error) {
		configuration := map[string]interface{}{
			"key1": "value1",
			"key2": map[string]string{
				"nest1": "n-value1",
				"nest2": "n-value2",
			},
			"key3": map[string]interface{}{
				"nest1": "n-value1",
				"nest2": "n-value2",
				"key4": map[string]string{
					"nest3": "n-value4",
					"nest4": "n-value5",
				},
			},
		}
		return configuration, nil
	}
	// PD config.
	router.Handle(pdapi.Config, fn.Wrap(mockConfig))
	// TiDB/TiKV config.
	router.Handle("/config", fn.Wrap(mockConfig))
	// PD region.
	router.Handle("/pd/api/v1/stats/region", fn.Wrap(func() (*helper.PDRegionStats, error) {
		return &helper.PDRegionStats{
			Count:            1,
			EmptyCount:       1,
			StorageSize:      1,
			StorageKeys:      1,
			StoreLeaderCount: map[uint64]int{1: 1},
			StorePeerCount:   map[uint64]int{1: 1},
		}, nil
	}))
	return srv, mockAddr
}

type mockSessionManager struct {
	processInfoMap map[uint64]*util.ProcessInfo
	serverID       uint64
}

func (sm *mockSessionManager) ShowTxnList() []*txninfo.TxnInfo {
	panic("unimplemented!")
}

func (sm *mockSessionManager) ShowProcessList() map[uint64]*util.ProcessInfo {
	return sm.processInfoMap
}

func (sm *mockSessionManager) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	rs, ok := sm.processInfoMap[id]
	return rs, ok
}

func (sm *mockSessionManager) StoreInternalSession(_ interface{}) {
}

func (sm *mockSessionManager) DeleteInternalSession(_ interface{}) {
}

func (sm *mockSessionManager) GetInternalSessionStartTSList() []uint64 {
	return nil
}

func (sm *mockSessionManager) Kill(_ uint64, _ bool) {}

func (sm *mockSessionManager) KillAllConnections() {}

func (sm *mockSessionManager) UpdateTLSConfig(_ *tls.Config) {}

func (sm *mockSessionManager) ServerID() uint64 {
	return sm.serverID
}

func (sm *mockSessionManager) SetServerID(serverID uint64) {
	sm.serverID = serverID
}

type mockStore struct {
	helper.Storage
	host string
}

func (s *mockStore) EtcdAddrs() ([]string, error) { return []string{s.host}, nil }
func (s *mockStore) TLSConfig() *tls.Config       { panic("not implemented") }
func (s *mockStore) StartGCWorker() error         { panic("not implemented") }
func (s *mockStore) Name() string                 { return "mockStore" }
func (s *mockStore) Describe() string             { return "" }

func TestTiDBClusterInfo(t *testing.T) {
	s := createInfosSchemaClusterTableSuite(t)
	defer s.clean()

	mockAddr := s.mockAddr
	store := &mockStore{
		s.store.(helper.Storage),
		mockAddr,
	}

	// information_schema.cluster_info
	tk := testkit.NewTestKit(t, store)
	tidbStatusAddr := fmt.Sprintf(":%d", config.GetGlobalConfig().Status.StatusPort)
	row := func(cols ...string) string { return strings.Join(cols, " ") }
	tk.MustQuery("select type, instance, status_address, version, git_hash from information_schema.cluster_info").Check(testkit.Rows(
		row("tidb", ":4000", tidbStatusAddr, "None", "None"),
		row("pd", mockAddr, mockAddr, "4.0.0-alpha", "mock-pd-githash"),
		row("tikv", "store1", "", "", ""),
	))
	startTime := s.startTime.Format(time.RFC3339)
	tk.MustQuery("select type, instance, start_time from information_schema.cluster_info where type != 'tidb'").Check(testkit.Rows(
		row("pd", mockAddr, startTime),
		row("tikv", "store1", ""),
	))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/infoschema/mockStoreTombstone", `return(true)`))
	tk.MustQuery("select type, instance, start_time from information_schema.cluster_info where type = 'tikv'").Check(testkit.Rows())
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/infoschema/mockStoreTombstone"))

	// information_schema.cluster_config
	instances := []string{
		"pd,127.0.0.1:11080," + mockAddr + ",mock-version,mock-githash,0",
		"tidb,127.0.0.1:11080," + mockAddr + ",mock-version,mock-githash,1001",
		"tikv,127.0.0.1:11080," + mockAddr + ",mock-version,mock-githash,0",
	}
	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/infoschema/mockClusterInfo", fpExpr))
	defer func() { require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/infoschema/mockClusterInfo")) }()
	tk.MustQuery("select type, instance, status_address, version, git_hash, server_id from information_schema.cluster_info").Check(testkit.Rows(
		row("pd", "127.0.0.1:11080", mockAddr, "mock-version", "mock-githash", "0"),
		row("tidb", "127.0.0.1:11080", mockAddr, "mock-version", "mock-githash", "1001"),
		row("tikv", "127.0.0.1:11080", mockAddr, "mock-version", "mock-githash", "0"),
	))
	tk.MustQuery("select * from information_schema.cluster_config").Check(testkit.Rows(
		"pd 127.0.0.1:11080 key1 value1",
		"pd 127.0.0.1:11080 key2.nest1 n-value1",
		"pd 127.0.0.1:11080 key2.nest2 n-value2",
		"pd 127.0.0.1:11080 key3.key4.nest3 n-value4",
		"pd 127.0.0.1:11080 key3.key4.nest4 n-value5",
		"pd 127.0.0.1:11080 key3.nest1 n-value1",
		"pd 127.0.0.1:11080 key3.nest2 n-value2",
		"tidb 127.0.0.1:11080 key1 value1",
		"tidb 127.0.0.1:11080 key2.nest1 n-value1",
		"tidb 127.0.0.1:11080 key2.nest2 n-value2",
		"tidb 127.0.0.1:11080 key3.key4.nest3 n-value4",
		"tidb 127.0.0.1:11080 key3.key4.nest4 n-value5",
		"tidb 127.0.0.1:11080 key3.nest1 n-value1",
		"tidb 127.0.0.1:11080 key3.nest2 n-value2",
		"tikv 127.0.0.1:11080 key1 value1",
		"tikv 127.0.0.1:11080 key2.nest1 n-value1",
		"tikv 127.0.0.1:11080 key2.nest2 n-value2",
		"tikv 127.0.0.1:11080 key3.key4.nest3 n-value4",
		"tikv 127.0.0.1:11080 key3.key4.nest4 n-value5",
		"tikv 127.0.0.1:11080 key3.nest1 n-value1",
		"tikv 127.0.0.1:11080 key3.nest2 n-value2",
	))
	tk.MustQuery("select TYPE, `KEY`, VALUE from information_schema.cluster_config where `key`='key3.key4.nest4' order by type").Check(testkit.Rows(
		"pd key3.key4.nest4 n-value5",
		"tidb key3.key4.nest4 n-value5",
		"tikv key3.key4.nest4 n-value5",
	))
}

func TestTableStorageStats(t *testing.T) {
	s := createInfosSchemaClusterTableSuite(t)
	defer s.clean()

	tk := testkit.NewTestKit(t, s.store)
	err := tk.QueryToErr("select * from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'test'")
	require.EqualError(t, err, "pd unavailable")
	mockAddr := s.mockAddr
	store := &mockStore{
		s.store.(helper.Storage),
		mockAddr,
	}

	// Test information_schema.TABLE_STORAGE_STATS.
	tk = testkit.NewTestKit(t, store)

	// Test not set the schema.
	err = tk.QueryToErr("select * from information_schema.TABLE_STORAGE_STATS")
	require.EqualError(t, err, "Please specify the 'table_schema'")

	// Test it would get null set when get the sys schema.
	tk.MustQuery("select TABLE_NAME from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'information_schema';").Check([][]interface{}{})
	tk.MustQuery("select TABLE_NAME from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA in ('information_schema', 'metrics_schema');").Check([][]interface{}{})
	tk.MustQuery("select TABLE_NAME from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'information_schema' and TABLE_NAME='schemata';").Check([][]interface{}{})

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustQuery("select TABLE_NAME, TABLE_SIZE from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'test' and TABLE_NAME='t';").Check(testkit.Rows("t 1"))

	tk.MustExec("create table t1 (a int, b int, index idx(a))")
	tk.MustQuery("select TABLE_NAME, sum(TABLE_SIZE) from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'test' group by TABLE_NAME;").Sort().Check(testkit.Rows(
		"t 1",
		"t1 1",
	))
	tk.MustQuery("select TABLE_SCHEMA, sum(TABLE_SIZE) from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'test' group by TABLE_SCHEMA;").Check(testkit.Rows(
		"test 2",
	))
	rows := tk.MustQuery("select TABLE_NAME from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'mysql';").Rows()
	result := 32
	require.Len(t, rows, result)

	// More tests about the privileges.
	tk.MustExec("create user 'testuser'@'localhost'")
	tk.MustExec("create user 'testuser2'@'localhost'")
	tk.MustExec("create user 'testuser3'@'localhost'")
	tk1 := testkit.NewTestKit(t, store)
	defer tk1.MustExec("drop user 'testuser'@'localhost'")
	defer tk1.MustExec("drop user 'testuser2'@'localhost'")
	defer tk1.MustExec("drop user 'testuser3'@'localhost'")

	tk.MustExec("grant all privileges on *.* to 'testuser2'@'localhost'")
	tk.MustExec("grant select on *.* to 'testuser3'@'localhost'")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "testuser",
		Hostname: "localhost",
	}, nil, nil))

	// User has no access to this schema, so the result set is empty.
	tk.MustQuery("select count(1) from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'mysql'").Check(testkit.Rows("0"))

	require.True(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "testuser2",
		Hostname: "localhost",
	}, nil, nil))

	tk.MustQuery("select count(1) from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'mysql'").Check(testkit.Rows(strconv.Itoa(result)))

	require.True(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "testuser3",
		Hostname: "localhost",
	}, nil, nil))

	tk.MustQuery("select count(1) from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'mysql'").Check(testkit.Rows(strconv.Itoa(result)))
}
