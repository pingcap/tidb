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
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client/http"
	"google.golang.org/grpc"
)

type infosSchemaClusterTableSuite struct {
	store      kv.Storage
	dom        *domain.Domain
	rpcServer  *grpc.Server
	httpServer *httptest.Server
	mockAddr   string
	listenAddr string
	startTime  time.Time
}

func createInfosSchemaClusterTableSuite(t *testing.T) *infosSchemaClusterTableSuite {
	s := new(infosSchemaClusterTableSuite)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	pdAddrs := []string{s.mockAddr}
	s.store, s.dom = testkit.CreateMockStoreAndDomain(
		t,
		mockstore.WithTiKVOptions(tikv.WithPDHTTPClient("infoschema-cluster-table-test", pdAddrs)),
		mockstore.WithPDAddr(pdAddrs),
	)
	s.rpcServer, s.listenAddr = setUpRPCService(t, s.dom, "127.0.0.1:0")
	s.startTime = time.Now()
	t.Cleanup(func() {
		if s.rpcServer != nil {
			s.rpcServer.Stop()
		}
		if s.httpServer != nil {
			s.httpServer.Close()
		}
	})
	return s
}

func setUpRPCService(t *testing.T, dom *domain.Domain, addr string) (*grpc.Server, string) {
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	// Fix issue 9836
	sm := &testkit.MockSessionManager{
		PS:    make([]*util.ProcessInfo, 1),
		SerID: 1,
	}
	sm.PS = append(sm.PS, &util.ProcessInfo{
		ID:      1,
		User:    "root",
		Host:    "127.0.0.1",
		Command: mysql.ComQuery,
	})
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
	router.Handle(pd.Stores, fn.Wrap(func() (*pd.StoresInfo, error) {
		return &pd.StoresInfo{
			Count: 1,
			Stores: []pd.StoreInfo{
				{
					Store: pd.MetaStore{
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
	// mock regions
	router.Handle(pd.Regions, fn.Wrap(func() (*pd.RegionsInfo, error) {
		return &pd.RegionsInfo{
			Count: 1,
			Regions: []pd.RegionInfo{
				{
					ID:       1,
					StartKey: "",
					EndKey:   "",
					Epoch: pd.RegionEpoch{
						ConfVer: 1,
						Version: 2,
					},
					WrittenBytes:    10000,
					ReadBytes:       20000,
					ApproximateSize: 300000,
					ApproximateKeys: 1000,
				},
			},
		}, nil
	}))
	// mock PD API
	router.Handle(pd.Status, fn.Wrap(func() (any, error) {
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
	var mockConfig = func() (map[string]any, error) {
		configuration := map[string]any{
			"key1": "value1",
			"key2": map[string]string{
				"nest1": "n-value1",
				"nest2": "n-value2",
			},
			"key3": map[string]any{
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
	router.Handle(pd.Config, fn.Wrap(mockConfig))
	// TiDB/TiKV config.
	router.Handle("/config", fn.Wrap(mockConfig))
	// PD region.
	router.Handle(pd.StatsRegion, fn.Wrap(func() (*pd.RegionStats, error) {
		return &pd.RegionStats{
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
	startTime := types.NewTime(types.FromGoTime(s.startTime), mysql.TypeDatetime, 0).String()
	tk.MustQuery("select type, instance, start_time from information_schema.cluster_info where type = 'pd'").Check(testkit.Rows(
		row("pd", mockAddr, startTime),
	))
	// The start_time is filled in `dataForTiDBClusterInfo` function not always same as `s.startTime`.
	tk.MustQuery("select type, instance from information_schema.cluster_info where type = 'tikv'").Check(testkit.Rows(
		row("tikv", "store1"),
	))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockStoreTombstone", `return(true)`))
	tk.MustQuery("select type, instance, start_time from information_schema.cluster_info where type = 'tikv'").Check(testkit.Rows())
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockStoreTombstone"))

	// information_schema.cluster_config
	instances := []string{
		"pd,127.0.0.1:11080," + mockAddr + ",mock-version,mock-githash,0",
		"tidb,127.0.0.1:11080," + mockAddr + ",mock-version,mock-githash,1001",
		"tikv,127.0.0.1:11080," + mockAddr + ",mock-version,mock-githash,0",
		"tiproxy,127.0.0.1:6000," + mockAddr + ",mock-version,mock-githash,0",
		"ticdc,127.0.0.1:8300," + mockAddr + ",mock-version,mock-githash,0",
	}
	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockClusterInfo", fpExpr))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockClusterInfo"))
	}()
	tk.MustQuery("select type, instance, status_address, version, git_hash, server_id from information_schema.cluster_info").Check(testkit.Rows(
		row("pd", "127.0.0.1:11080", mockAddr, "mock-version", "mock-githash", "0"),
		row("tidb", "127.0.0.1:11080", mockAddr, "mock-version", "mock-githash", "1001"),
		row("tikv", "127.0.0.1:11080", mockAddr, "mock-version", "mock-githash", "0"),
		row("tiproxy", "127.0.0.1:6000", mockAddr, "mock-version", "mock-githash", "0"),
		row("ticdc", "127.0.0.1:8300", mockAddr, "mock-version", "mock-githash", "0"),
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
		"ticdc 127.0.0.1:8300 key1 value1",
		"ticdc 127.0.0.1:8300 key2.nest1 n-value1",
		"ticdc 127.0.0.1:8300 key2.nest2 n-value2",
		"ticdc 127.0.0.1:8300 key3.key4.nest3 n-value4",
		"ticdc 127.0.0.1:8300 key3.key4.nest4 n-value5",
		"ticdc 127.0.0.1:8300 key3.nest1 n-value1",
		"ticdc 127.0.0.1:8300 key3.nest2 n-value2",
	))
	tk.MustQuery("select TYPE, `KEY`, VALUE from information_schema.cluster_config where `key`='key3.key4.nest4' order by type").Check(testkit.Rows(
		"pd key3.key4.nest4 n-value5",
		"ticdc key3.key4.nest4 n-value5",
		"tidb key3.key4.nest4 n-value5",
		"tikv key3.key4.nest4 n-value5",
	))
}

func TestTikvRegionStatus(t *testing.T) {
	s := createInfosSchemaClusterTableSuite(t)
	mockAddr := s.mockAddr
	store := &mockStore{
		s.store.(helper.Storage),
		mockAddr,
	}
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index=true")
	defer func() {
		tk.MustExec("set tidb_enable_global_index=default")
	}()
	tk.MustExec("drop table if exists test_t1")
	tk.MustExec(`CREATE TABLE test_t1 ( a int(11) DEFAULT NULL, b int(11) DEFAULT NULL, c int(11) DEFAULT NULL)`)
	tk.MustQuery("select REGION_ID, DB_NAME, TABLE_NAME, IS_INDEX, INDEX_ID, INDEX_NAME, IS_PARTITION, PARTITION_NAME from information_schema.TIKV_REGION_STATUS where DB_NAME = 'test' and TABLE_NAME = 'test_t1'").Check(testkit.Rows(
		"1 test test_t1 0 <nil> <nil> 0 <nil>",
	))

	tk.MustExec("alter table test_t1 add index p_a (a)")
	tk.MustQuery("select REGION_ID, DB_NAME, TABLE_NAME, IS_INDEX, INDEX_NAME, IS_PARTITION, PARTITION_NAME from information_schema.TIKV_REGION_STATUS where DB_NAME = 'test' and TABLE_NAME = 'test_t1' order by IS_INDEX").Check(testkit.Rows(
		"1 test test_t1 0 <nil> 0 <nil>",
		"1 test test_t1 1 p_a 0 <nil>",
	))

	tk.MustExec("alter table test_t1 add unique p_b (b);")
	tk.MustQuery("select REGION_ID, DB_NAME, TABLE_NAME, IS_INDEX, INDEX_NAME, IS_PARTITION, PARTITION_NAME from information_schema.TIKV_REGION_STATUS where DB_NAME = 'test' and TABLE_NAME = 'test_t1' order by IS_INDEX, INDEX_NAME").Check(testkit.Rows(
		"1 test test_t1 0 <nil> 0 <nil>",
		"1 test test_t1 1 p_a 0 <nil>",
		"1 test test_t1 1 p_b 0 <nil>",
	))

	tk.MustExec("drop table if exists test_t2")
	tk.MustExec(`CREATE TABLE test_t2 ( a int(11) DEFAULT NULL, b int(11) DEFAULT NULL, c int(11) DEFAULT NULL)
		PARTITION BY RANGE (c) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (MAXVALUE))`)
	tk.MustQuery("select REGION_ID, DB_NAME, TABLE_NAME, IS_INDEX, INDEX_ID, INDEX_NAME, IS_PARTITION, PARTITION_NAME from information_schema.TIKV_REGION_STATUS where DB_NAME = 'test' and TABLE_NAME = 'test_t2' order by PARTITION_NAME").Check(testkit.Rows(
		"1 test test_t2 0 <nil> <nil> 1 p0",
		"1 test test_t2 0 <nil> <nil> 1 p1",
	))

	tk.MustExec("alter table test_t2 add index p_a (a)")
	tk.MustQuery("select REGION_ID, DB_NAME, TABLE_NAME, IS_INDEX, INDEX_NAME, IS_PARTITION, PARTITION_NAME from information_schema.TIKV_REGION_STATUS where DB_NAME = 'test' and TABLE_NAME = 'test_t2' order by IS_INDEX, PARTITION_NAME").Check(testkit.Rows(
		"1 test test_t2 0 <nil> 1 p0",
		"1 test test_t2 0 <nil> 1 p1",
		"1 test test_t2 1 p_a 1 p0",
		"1 test test_t2 1 p_a 1 p1",
	))

	tk.MustExec("alter table test_t2 add unique p_b (b);")
	tk.MustQuery("select REGION_ID, DB_NAME, TABLE_NAME, IS_INDEX, INDEX_NAME, IS_PARTITION, PARTITION_NAME from information_schema.TIKV_REGION_STATUS where DB_NAME = 'test' and TABLE_NAME = 'test_t2' order by IS_INDEX, IS_PARTITION desc, PARTITION_NAME").Check(testkit.Rows(
		"1 test test_t2 0 <nil> 1 p0",
		"1 test test_t2 0 <nil> 1 p1",
		"1 test test_t2 1 p_a 1 p0",
		"1 test test_t2 1 p_a 1 p1",
		"1 test test_t2 1 p_b 0 <nil>",
	))
}

func TestTableStorageStats(t *testing.T) {
	s := createInfosSchemaClusterTableSuite(t)

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

	// Test table_schema is not specified.
	err = tk.QueryToErr("select * from information_schema.TABLE_STORAGE_STATS")
	require.EqualError(t, err, "Please add where clause to filter the column TABLE_SCHEMA. "+
		"For example, where TABLE_SCHEMA = 'xxx' or where TABLE_SCHEMA in ('xxx', 'yyy')")

	// Test it would get null set when get the sys schema.
	tk.MustQuery("select TABLE_NAME from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'information_schema';").Check([][]any{})
	tk.MustQuery("select TABLE_NAME from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA in ('information_schema', 'metrics_schema');").Check([][]any{})
	tk.MustQuery("select TABLE_NAME from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'information_schema' and TABLE_NAME='schemata';").Check([][]any{})

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
	result := 54
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
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "testuser",
		Hostname: "localhost",
	}, nil, nil, nil))

	// User has no access to this schema, so the result set is empty.
	tk.MustQuery("select count(1) from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'mysql'").Check(testkit.Rows("0"))

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "testuser2",
		Hostname: "localhost",
	}, nil, nil, nil))

	tk.MustQuery("select count(1) from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'mysql'").Check(testkit.Rows(strconv.Itoa(result)))

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "testuser3",
		Hostname: "localhost",
	}, nil, nil, nil))

	tk.MustQuery("select count(1) from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'mysql'").Check(testkit.Rows(strconv.Itoa(result)))
}

func TestIssue42619(t *testing.T) {
	s := createInfosSchemaClusterTableSuite(t)
	mockAddr := s.mockAddr
	store := &mockStore{
		s.store.(helper.Storage),
		mockAddr,
	}
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustQuery("SELECT TABLE_SCHEMA, TABLE_NAME, PEER_COUNT, REGION_COUNT, EMPTY_REGION_COUNT, TABLE_SIZE, TABLE_KEYS " +
		"FROM information_schema.TABLE_STORAGE_STATS " +
		"WHERE TABLE_SCHEMA = 'test' and TABLE_NAME='t'").Check(
		testkit.Rows("test t 1 1 1 1 1"))

	tk.MustExec(
		"CREATE TABLE tp (a int(11) DEFAULT NULL,b int(11) DEFAULT NULL,c int(11) DEFAULT NULL," +
			"KEY ia(a), KEY ib(b), KEY ic (c))" +
			"PARTITION BY RANGE (`a`)" +
			"(PARTITION `p0` VALUES LESS THAN (300)," +
			"PARTITION `p1` VALUES LESS THAN (600)," +
			"PARTITION `p2` VALUES LESS THAN (900)," +
			"PARTITION `p3` VALUES LESS THAN (MAXVALUE))")
	tk.MustQuery("SELECT TABLE_SCHEMA, TABLE_NAME, PEER_COUNT, REGION_COUNT, EMPTY_REGION_COUNT, TABLE_SIZE, TABLE_KEYS " +
		"FROM information_schema.TABLE_STORAGE_STATS " +
		"WHERE TABLE_SCHEMA = 'test'").Sort().Check(
		testkit.Rows(
			"test t 1 1 1 1 1",
			"test tp 1 1 1 1 1",
			"test tp 1 1 1 1 1",
			"test tp 1 1 1 1 1",
			"test tp 1 1 1 1 1",
			"test tp 1 1 1 1 1",
		))
}
