// Copyright 2021 PingCAP, Inc.
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

package infoschema_test

import (
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/fn"
	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/mockstore/mockstorage"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/resourcegrouptag"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type clusterTablesSuite struct {
	store      kv.Storage
	dom        *domain.Domain
	rpcserver  *grpc.Server
	httpServer *httptest.Server
	mockAddr   string
	listenAddr string
	startTime  time.Time
}

func TestForClusterServerInfo(t *testing.T) {
	// setup suite
	var clean func()
	s := new(clusterTablesSuite)
	s.store, s.dom, clean = testkit.CreateMockStoreAndDomain(t)
	defer clean()
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0")
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()

	tk := testkit.NewTestKit(t, s.store)
	instances := []string{
		strings.Join([]string{"tidb", s.listenAddr, s.listenAddr, "mock-version,mock-githash,1001"}, ","),
		strings.Join([]string{"pd", s.listenAddr, s.listenAddr, "mock-version,mock-githash,0"}, ","),
		strings.Join([]string{"tikv", s.listenAddr, s.listenAddr, "mock-version,mock-githash,0"}, ","),
	}

	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	fpName := "github.com/pingcap/tidb/infoschema/mockClusterInfo"
	require.NoError(t, failpoint.Enable(fpName, fpExpr))
	defer func() { require.NoError(t, failpoint.Disable(fpName)) }()

	cases := []struct {
		sql        string
		types      set.StringSet
		addrs      set.StringSet
		names      set.StringSet
		skipOnDist set.StringSet
	}{
		{
			sql:   "select * from information_schema.CLUSTER_LOAD;",
			types: set.NewStringSet("tidb", "tikv", "pd"),
			addrs: set.NewStringSet(s.listenAddr),
			names: set.NewStringSet("cpu", "memory", "net"),
		},
		{
			sql:   "select * from information_schema.CLUSTER_HARDWARE;",
			types: set.NewStringSet("tidb", "tikv", "pd"),
			addrs: set.NewStringSet(s.listenAddr),
			names: set.NewStringSet("cpu", "memory", "net", "disk"),
			// The sysutil package will filter out all disk don't have /dev prefix.
			// gopsutil cpu.Info will fail on mac M1
			skipOnDist: set.NewStringSet("windows", "darwin/arm64"),
		},
		{
			sql:   "select * from information_schema.CLUSTER_SYSTEMINFO;",
			types: set.NewStringSet("tidb", "tikv", "pd"),
			addrs: set.NewStringSet(s.listenAddr),
			names: set.NewStringSet("system"),
			// This test get empty result and fails on the windows platform.
			// Because the underlying implementation use `sysctl` command to get the result
			// and there is no such command on windows.
			// https://github.com/pingcap/sysutil/blob/2bfa6dc40bcd4c103bf684fba528ae4279c7ec9f/system_info.go#L50
			skipOnDist: set.NewStringSet("windows"),
		},
	}

	for _, cas := range cases {
		if cas.skipOnDist.Exist(runtime.GOOS+"/"+runtime.GOARCH) || cas.skipOnDist.Exist(runtime.GOOS) {
			continue
		}

		result := tk.MustQuery(cas.sql)
		rows := result.Rows()
		require.Greater(t, len(rows), 0)

		gotTypes := set.StringSet{}
		gotAddrs := set.StringSet{}
		gotNames := set.StringSet{}

		for _, row := range rows {
			gotTypes.Insert(row[0].(string))
			gotAddrs.Insert(row[1].(string))
			gotNames.Insert(row[2].(string))
		}

		require.Equalf(t, cas.types, gotTypes, "sql: %s", cas.sql)
		require.Equalf(t, cas.addrs, gotAddrs, "sql: %s", cas.sql)
		require.Equalf(t, cas.names, gotNames, "sql: %s", cas.sql)
	}
}

func TestTestDataLockWaits(t *testing.T) {
	// setup suite
	var clean func()
	s := new(clusterTablesSuite)
	s.store, s.dom, clean = testkit.CreateMockStoreAndDomain(t)
	defer clean()
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0")
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()

	_, digest1 := parser.NormalizeDigest("select * from test_data_lock_waits for update")
	_, digest2 := parser.NormalizeDigest("update test_data_lock_waits set f1=1 where id=2")
	s.store.(mockstorage.MockLockWaitSetter).SetMockLockWaits([]*deadlock.WaitForEntry{
		{Txn: 1, WaitForTxn: 2, Key: []byte("key1"), ResourceGroupTag: resourcegrouptag.EncodeResourceGroupTag(digest1, nil, tipb.ResourceGroupTagLabel_ResourceGroupTagLabelUnknown)},
		{Txn: 3, WaitForTxn: 4, Key: []byte("key2"), ResourceGroupTag: resourcegrouptag.EncodeResourceGroupTag(digest2, nil, tipb.ResourceGroupTagLabel_ResourceGroupTagLabelUnknown)},
		// Invalid digests
		{Txn: 5, WaitForTxn: 6, Key: []byte("key3"), ResourceGroupTag: resourcegrouptag.EncodeResourceGroupTag(nil, nil, tipb.ResourceGroupTagLabel_ResourceGroupTagLabelUnknown)},
		{Txn: 7, WaitForTxn: 8, Key: []byte("key4"), ResourceGroupTag: []byte("asdfghjkl")},
	})

	tk := s.newTestKitWithRoot(t)

	// Execute one of the query once, so it's stored into statements_summary.
	tk.MustExec("create table test_data_lock_waits (id int primary key, f1 int)")
	tk.MustExec("select * from test_data_lock_waits for update")

	tk.MustQuery("select * from information_schema.DATA_LOCK_WAITS").Check(testkit.Rows(
		"6B657931 <nil> 1 2 "+digest1.String()+" select * from `test_data_lock_waits` for update",
		"6B657932 <nil> 3 4 "+digest2.String()+" <nil>",
		"6B657933 <nil> 5 6 <nil> <nil>",
		"6B657934 <nil> 7 8 <nil> <nil>"))

}

func SubTestDataLockWaitsPrivilege(t *testing.T) {
	// setup suite
	var clean func()
	s := new(clusterTablesSuite)
	s.store, s.dom, clean = testkit.CreateMockStoreAndDomain(t)
	defer clean()
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0")
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()

	dropUserTk := s.newTestKitWithRoot(t)

	tk := s.newTestKitWithRoot(t)

	tk.MustExec("create user 'testuser'@'localhost'")
	defer dropUserTk.MustExec("drop user 'testuser'@'localhost'")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "testuser",
		Hostname: "localhost",
	}, nil, nil))
	err := tk.QueryToErr("select * from information_schema.DATA_LOCK_WAITS")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	tk = s.newTestKitWithRoot(t)
	tk.MustExec("create user 'testuser2'@'localhost'")
	defer dropUserTk.MustExec("drop user 'testuser2'@'localhost'")
	tk.MustExec("grant process on *.* to 'testuser2'@'localhost'")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "testuser2",
		Hostname: "localhost",
	}, nil, nil))
	_ = tk.MustQuery("select * from information_schema.DATA_LOCK_WAITS")

}

func TestSelectClusterTable(t *testing.T) {
	// setup suite
	var clean func()
	s := new(clusterTablesSuite)
	s.store, s.dom, clean = testkit.CreateMockStoreAndDomain(t)
	defer clean()
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0")
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)
	slowLogFileName := "tidb-slow.log"
	prepareSlowLogfile(t, slowLogFileName)
	defer func() { require.NoError(t, os.Remove(slowLogFileName)) }()

	tk.MustExec("use information_schema")
	tk.MustExec("set @@global.tidb_enable_stmt_summary=1")
	tk.MustExec("set time_zone = '+08:00';")
	tk.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("2"))
	tk.MustQuery("select time from `CLUSTER_SLOW_QUERY` where time='2019-02-12 19:33:56.571953'").Check(testkit.RowsWithSep("|", "2019-02-12 19:33:56.571953"))
	tk.MustQuery("select count(*) from `CLUSTER_PROCESSLIST`").Check(testkit.Rows("1"))
	tk.MustQuery("select * from `CLUSTER_PROCESSLIST`").Check(testkit.Rows(fmt.Sprintf(":10080 1 root 127.0.0.1 <nil> Query 9223372036 %s <nil>  0 0 ", "")))
	tk.MustQuery("select query_time, conn_id from `CLUSTER_SLOW_QUERY` order by time limit 1").Check(testkit.Rows("4.895492 6"))
	tk.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY` group by digest").Check(testkit.Rows("1", "1"))
	tk.MustQuery("select digest, count(*) from `CLUSTER_SLOW_QUERY` group by digest order by digest").Check(testkit.Rows("124acb3a0bec903176baca5f9da00b4e7512a41c93b417923f26502edeb324cc 1", "42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772 1"))
	tk.MustQuery(`select length(query) as l,time from information_schema.cluster_slow_query where time > "2019-02-12 19:33:56" order by abs(l) desc limit 10;`).Check(testkit.Rows("21 2019-02-12 19:33:56.571953"))
	tk.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY` where time > now() group by digest").Check(testkit.Rows())
	re := tk.MustQuery("select * from `CLUSTER_statements_summary`")
	require.NotNil(t, re)
	require.Greater(t, len(re.Rows()), 0)
	re = tk.MustQuery("select * from `CLUSTER_statements_summary` where table_names REGEXP '\\binformation_schema\\.'")
	require.NotNil(t, re)
	require.Equal(t, len(re.Rows()), 0)
	re = tk.MustQuery("select * from `CLUSTER_statements_summary` where table_names REGEXP 'information_schema\\.'")
	require.NotNil(t, re)
	require.Greater(t, len(re.Rows()), 0)
	// Test for TiDB issue 14915.
	re = tk.MustQuery("select sum(exec_count*avg_mem) from cluster_statements_summary_history group by schema_name,digest,digest_text;")
	require.NotNil(t, re)
	require.Greater(t, len(re.Rows()), 0)
	tk.MustQuery("select * from `CLUSTER_statements_summary_history`")
	require.NotNil(t, re)
	require.Greater(t, len(re.Rows()), 0)
	tk.MustExec("set @@global.tidb_enable_stmt_summary=0")
	re = tk.MustQuery("select * from `CLUSTER_statements_summary`")
	require.NotNil(t, re)
	require.Equal(t, 0, len(re.Rows()))
	tk.MustQuery("select * from `CLUSTER_statements_summary_history`")
	require.NotNil(t, re)
	require.Equal(t, 0, len(re.Rows()))
}

func SubTestSelectClusterTablePrivilege(t *testing.T) {
	// setup suite
	var clean func()
	s := new(clusterTablesSuite)
	s.store, s.dom, clean = testkit.CreateMockStoreAndDomain(t)
	defer clean()
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0")
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := testkit.NewTestKit(t, s.store)
	slowLogFileName := "tidb-slow.log"
	f, err := os.OpenFile(slowLogFileName, os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte(
		`# Time: 2019-02-12T19:33:57.571953+08:00
# User@Host: user2 [user2] @ 127.0.0.1 [127.0.0.1]
select * from t2;
# Time: 2019-02-12T19:33:56.571953+08:00
# User@Host: user1 [user1] @ 127.0.0.1 [127.0.0.1]
select * from t1;
# Time: 2019-02-12T19:33:58.571953+08:00
# User@Host: user2 [user2] @ 127.0.0.1 [127.0.0.1]
select * from t3;
# Time: 2019-02-12T19:33:59.571953+08:00
select * from t3;
`))
	require.NoError(t, f.Close())
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(slowLogFileName)) }()
	tk.MustExec("use information_schema")
	tk.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from `SLOW_QUERY`").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from `CLUSTER_PROCESSLIST`").Check(testkit.Rows("1"))
	tk.MustQuery("select * from `CLUSTER_PROCESSLIST`").Check(testkit.Rows(fmt.Sprintf(":10080 1 root 127.0.0.1 <nil> Query 9223372036 %s <nil>  0 0 ", "")))
	tk.MustExec("create user user1")
	tk.MustExec("create user user2")
	user1 := testkit.NewTestKit(t, s.store)
	user1.MustExec("use information_schema")
	require.True(t, user1.Session().Auth(&auth.UserIdentity{
		Username: "user1",
		Hostname: "127.0.0.1",
	}, nil, nil))
	user1.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("1"))
	user1.MustQuery("select count(*) from `SLOW_QUERY`").Check(testkit.Rows("1"))
	user1.MustQuery("select user,query from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("user1 select * from t1;"))

	user2 := testkit.NewTestKit(t, s.store)
	user2.MustExec("use information_schema")
	require.True(t, user2.Session().Auth(&auth.UserIdentity{
		Username: "user2",
		Hostname: "127.0.0.1",
	}, nil, nil))
	user2.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("2"))
	user2.MustQuery("select user,query from `CLUSTER_SLOW_QUERY` order by query").Check(testkit.Rows("user2 select * from t2;", "user2 select * from t3;"))
}

func TestStmtSummaryEvictedCountTable(t *testing.T) {
	// setup suite
	var clean func()
	s := new(clusterTablesSuite)
	s.store, s.dom, clean = testkit.CreateMockStoreAndDomain(t)
	defer clean()
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0")
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()

	tk := s.newTestKitWithRoot(t)
	// disable refreshing
	tk.MustExec("set global tidb_stmt_summary_refresh_interval=9999")
	// set information_schema.statements_summary's size to 2
	tk.MustExec("set global tidb_stmt_summary_max_stmt_count = 2")
	// no evict happened, no record in cluster evicted table.
	tk.MustQuery("select count(*) from information_schema.cluster_statements_summary_evicted;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_stmt_summary_max_stmt_count = 1")
	// cleanup side effects
	defer tk.MustExec("set global tidb_stmt_summary_max_stmt_count = 100")
	defer tk.MustExec("set global tidb_stmt_summary_refresh_interval = 1800")
	// clear information_schema.statements_summary
	tk.MustExec("set global tidb_enable_stmt_summary=0")
	// statements_summary is off, statements_summary_evicted is empty.
	tk.MustQuery("select count(*) from information_schema.cluster_statements_summary_evicted;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_enable_stmt_summary=1")

	// make a new session for test...
	tk = s.newTestKitWithRoot(t)
	// first sql
	tk.MustExec("show databases;")
	// second sql, evict former sql from stmt_summary
	tk.MustQuery("select evicted_count from information_schema.cluster_statements_summary_evicted;").
		Check(testkit.Rows("1"))
	// after executed the sql above
	tk.MustQuery("select evicted_count from information_schema.cluster_statements_summary_evicted;").
		Check(testkit.Rows("2"))
	// TODO: Add more tests.

	tk.MustExec("create user 'testuser'@'localhost'")
	tk.MustExec("create user 'testuser2'@'localhost'")
	tk.MustExec("grant process on *.* to 'testuser2'@'localhost'")
	tk1 := s.newTestKitWithRoot(t)
	defer tk1.MustExec("drop user 'testuser'@'localhost'")
	defer tk1.MustExec("drop user 'testuser2'@'localhost'")

	require.True(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "testuser",
		Hostname: "localhost",
	}, nil, nil))

	err := tk.QueryToErr("select * from information_schema.CLUSTER_STATEMENTS_SUMMARY_EVICTED")
	// This error is come from cop(TiDB) fetch from rpc server.
	require.EqualError(t, err, "other error: [planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	require.True(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "testuser2",
		Hostname: "localhost",
	}, nil, nil))
	require.NoError(t, tk.QueryToErr("select * from information_schema.CLUSTER_STATEMENTS_SUMMARY_EVICTED"))
}

func TestStmtSummaryHistoryTableWithUserTimezone(t *testing.T) {
	// setup suite
	var clean func()
	s := new(clusterTablesSuite)
	s.store, s.dom, clean = testkit.CreateMockStoreAndDomain(t)
	defer clean()
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0")
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()

	tk := s.newTestKitWithRoot(t)
	tk.MustExec("drop table if exists test_summary")
	tk.MustExec("create table test_summary(a int, b varchar(10), key k(a))")

	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))

	// Disable refreshing summary.
	tk.MustExec("set global tidb_stmt_summary_refresh_interval = 999999999")
	tk.MustQuery("select @@global.tidb_stmt_summary_refresh_interval").Check(testkit.Rows("999999999"))

	// Create a new session to test.
	tk = s.newTestKitWithRoot(t)
	tk.MustExec("use test;")
	tk.MustExec("set time_zone = '+08:00';")
	tk.MustExec("select sleep(0.1);")
	r := tk.MustQuery("select FIRST_SEEN, LAST_SEEN, SUMMARY_BEGIN_TIME, SUMMARY_END_TIME from INFORMATION_SCHEMA.STATEMENTS_SUMMARY_HISTORY order by LAST_SEEN limit 1;")
	date8First, err := time.Parse("2006-01-02 15:04:05", r.Rows()[0][0].(string))
	require.NoError(t, err)
	date8Last, err := time.Parse("2006-01-02 15:04:05", r.Rows()[0][1].(string))
	require.NoError(t, err)
	date8Begin, err := time.Parse("2006-01-02 15:04:05", r.Rows()[0][2].(string))
	require.NoError(t, err)
	date8End, err := time.Parse("2006-01-02 15:04:05", r.Rows()[0][3].(string))
	require.NoError(t, err)
	tk.MustExec("set time_zone = '+01:00';")
	r = tk.MustQuery("select FIRST_SEEN, LAST_SEEN, SUMMARY_BEGIN_TIME, SUMMARY_END_TIME from INFORMATION_SCHEMA.STATEMENTS_SUMMARY_HISTORY order by LAST_SEEN limit 1;")
	date1First, err := time.Parse("2006-01-02 15:04:05", r.Rows()[0][0].(string))
	require.NoError(t, err)
	date1Last, err := time.Parse("2006-01-02 15:04:05", r.Rows()[0][1].(string))
	require.NoError(t, err)
	date1Begin, err := time.Parse("2006-01-02 15:04:05", r.Rows()[0][2].(string))
	require.NoError(t, err)
	date1End, err := time.Parse("2006-01-02 15:04:05", r.Rows()[0][3].(string))
	require.NoError(t, err)

	require.Less(t, date1First.Unix(), date8First.Unix())
	require.Less(t, date1Last.Unix(), date8Last.Unix())
	require.Less(t, date1Begin.Unix(), date8Begin.Unix())
	require.Less(t, date1End.Unix(), date8End.Unix())
}

func TestStmtSummaryHistoryTable(t *testing.T) {
	// setup suite
	var clean func()
	s := new(clusterTablesSuite)
	s.store, s.dom, clean = testkit.CreateMockStoreAndDomain(t)
	defer clean()
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0")
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()

	tk := s.newTestKitWithRoot(t)
	tk.MustExec("drop table if exists test_summary")
	tk.MustExec("create table test_summary(a int, b varchar(10), key k(a))")

	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))

	// Disable refreshing summary.
	tk.MustExec("set global tidb_stmt_summary_refresh_interval = 999999999")
	tk.MustQuery("select @@global.tidb_stmt_summary_refresh_interval").Check(testkit.Rows("999999999"))

	// Create a new session to test.
	tk = s.newTestKitWithRoot(t)

	// Test INSERT
	tk.MustExec("insert into test_summary values(1, 'a')")
	tk.MustExec("insert into test_summary    values(2, 'b')")
	tk.MustExec("insert into TEST_SUMMARY VALUES(3, 'c')")
	tk.MustExec("/**/insert into test_summary values(4, 'd')")

	sql := "select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys," +
		"max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions," +
		"max_prewrite_regions, avg_affected_rows, query_sample_text " +
		"from information_schema.statements_summary_history " +
		"where digest_text like 'insert into `test_summary`%'"
	tk.MustQuery(sql).Check(testkit.Rows("Insert test test.test_summary <nil> 4 0 0 0 0 0 2 2 1 1 1 insert into test_summary values(1, 'a')"))

	tk.MustExec("set global tidb_stmt_summary_history_size = 0")
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan
		from information_schema.statements_summary_history`,
	).Check(testkit.Rows())

	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("drop table if exists `table`")
	tk.MustExec("set global tidb_stmt_summary_history_size = 1")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustExec("create table `table`(`insert` int)")
	tk.MustExec("select `insert` from `table`")

	sql = "select digest_text from information_schema.statements_summary_history;"
	tk.MustQuery(sql).Check(testkit.Rows(
		"select `insert` from `table`",
		"create table `table` ( `insert` int )",
		"set global `tidb_enable_stmt_summary` = ?",
	))
}

func TestIssue26379(t *testing.T) {
	var clean func()
	s := new(clusterTablesSuite)
	s.store, s.dom, clean = testkit.CreateMockStoreAndDomain(t)
	defer clean()
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0")
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)

	// Clear all statements.
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustExec("set @@global.tidb_stmt_summary_max_stmt_count=10")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), c int, d int, key k(a))")

	_, digest1 := parser.NormalizeDigest("select * from t where a = 3")
	_, digest2 := parser.NormalizeDigest("select * from t where b = 'b'")
	_, digest3 := parser.NormalizeDigest("select * from t where c = 6")
	_, digest4 := parser.NormalizeDigest("select * from t where d = 5")
	fillStatementCache := func() {
		tk.MustQuery("select * from t where a = 3")
		tk.MustQuery("select * from t where b = 'b'")
		tk.MustQuery("select * from t where c = 6")
		tk.MustQuery("select * from t where d = 5")
	}
	fillStatementCache()
	tk.MustQuery(fmt.Sprintf("select digest from information_schema.statements_summary where digest = '%s'", digest1.String())).Check(testkit.Rows(digest1.String()))
	tk.MustQuery(fmt.Sprintf("select digest from information_schema.cluster_statements_summary where digest = '%s'", digest1.String())).Check(testkit.Rows(digest1.String()))
	fillStatementCache()
	tk.MustQuery(fmt.Sprintf("select digest from information_schema.statements_summary where digest = '%s'", digest2.String())).Check(testkit.Rows(digest2.String()))
	tk.MustQuery(fmt.Sprintf("select digest from information_schema.cluster_statements_summary where digest = '%s'", digest2.String())).Check(testkit.Rows(digest2.String()))
	fillStatementCache()
	tk.MustQuery(fmt.Sprintf("select digest from information_schema.statements_summary where digest = '%s'", digest3.String())).Check(testkit.Rows(digest3.String()))
	tk.MustQuery(fmt.Sprintf("select digest from information_schema.cluster_statements_summary where digest = '%s'", digest3.String())).Check(testkit.Rows(digest3.String()))
	fillStatementCache()
	tk.MustQuery(fmt.Sprintf("select digest from information_schema.statements_summary where digest = '%s'", digest4.String())).Check(testkit.Rows(digest4.String()))
	tk.MustQuery(fmt.Sprintf("select digest from information_schema.cluster_statements_summary where digest = '%s'", digest4.String())).Check(testkit.Rows(digest4.String()))
	fillStatementCache()
	tk.MustQuery(fmt.Sprintf("select digest from information_schema.statements_summary where digest = '%s' or digest = '%s'", digest1.String(), digest2.String())).Sort().Check(testkit.Rows(digest1.String(), digest2.String()))
	tk.MustQuery(fmt.Sprintf("select digest from information_schema.cluster_statements_summary where digest = '%s' or digest = '%s'", digest1.String(), digest2.String())).Sort().Check(testkit.Rows(digest1.String(), digest2.String()))
	re := tk.MustQuery(fmt.Sprintf("select digest from information_schema.cluster_statements_summary where digest = '%s' and digest = '%s'", digest1.String(), digest2.String()))
	require.Equal(t, 0, len(re.Rows()))
	re = tk.MustQuery(fmt.Sprintf("select digest from information_schema.cluster_statements_summary where digest = '%s' and digest = '%s'", digest1.String(), digest2.String()))
	require.Equal(t, 0, len(re.Rows()))
	fillStatementCache()
	tk.MustQuery(fmt.Sprintf("select digest from information_schema.statements_summary where digest in ('%s', '%s', '%s', '%s')", digest1.String(), digest2.String(), digest3.String(), digest4.String())).Sort().Check(testkit.Rows(digest1.String(), digest4.String(), digest2.String(), digest3.String()))
	tk.MustQuery(fmt.Sprintf("select digest from information_schema.cluster_statements_summary where digest in ('%s', '%s', '%s', '%s')", digest1.String(), digest2.String(), digest3.String(), digest4.String())).Sort().Check(testkit.Rows(digest1.String(), digest4.String(), digest2.String(), digest3.String()))
	fillStatementCache()
	tk.MustQuery("select count(*) from information_schema.statements_summary where digest=''").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from information_schema.statements_summary where digest is null").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from information_schema.cluster_statements_summary where digest=''").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from information_schema.cluster_statements_summary where digest is null").Check(testkit.Rows("1"))
}

func TestStmtSummaryResultRows(t *testing.T) {
	// setup suite
	var clean func()
	s := new(clusterTablesSuite)
	s.store, s.dom, clean = testkit.CreateMockStoreAndDomain(t)
	defer clean()
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0")
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()

	tk := s.newTestKitWithRoot(t)
	tk.MustExec("set global tidb_stmt_summary_refresh_interval=999999999")
	tk.MustExec("set global tidb_stmt_summary_max_stmt_count = 3000")
	tk.MustExec("set global tidb_stmt_summary_history_size=24")
	tk.MustExec("set global tidb_stmt_summary_max_sql_length=4096")
	tk.MustExec("set global tidb_enable_stmt_summary=0")
	tk.MustExec("set global tidb_enable_stmt_summary=1")
	if !config.GetGlobalConfig().EnableCollectExecutionInfo {
		tk.MustExec("set @@tidb_enable_collect_execution_info=1")
		defer tk.MustExec("set @@tidb_enable_collect_execution_info=0")
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	for i := 1; i <= 30; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%v)", i))
	}

	tk.MustQuery("select * from test.t limit 10;")
	tk.MustQuery("select * from test.t limit 20;")
	tk.MustQuery("select * from test.t limit 30;")
	tk.MustQuery("select MIN_RESULT_ROWS,MAX_RESULT_ROWS,AVG_RESULT_ROWS from information_schema.statements_summary where query_sample_text like 'select%test.t limit%' and MAX_RESULT_ROWS > 10").
		Check(testkit.Rows("10 30 20"))
	tk.MustQuery("select MIN_RESULT_ROWS,MAX_RESULT_ROWS,AVG_RESULT_ROWS from information_schema.cluster_statements_summary where query_sample_text like 'select%test.t limit%' and MAX_RESULT_ROWS > 10").
		Check(testkit.Rows("10 30 20"))
}

func (s *clusterTablesSuite) setUpRPCService(t *testing.T, addr string) (*grpc.Server, string) {
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	// Fix issue 9836
	sm := &mockSessionManager{make(map[uint64]*util.ProcessInfo, 1), nil}
	sm.processInfoMap[1] = &util.ProcessInfo{
		ID:      1,
		User:    "root",
		Host:    "127.0.0.1",
		Command: mysql.ComQuery,
	}
	srv := server.NewRPCServer(config.GetGlobalConfig(), s.dom, sm)
	port := lis.Addr().(*net.TCPAddr).Port
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	go func() {
		err = srv.Serve(lis)
		require.NoError(t, err)
	}()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Status.StatusPort = uint(port)
	})
	return srv, addr
}

func (s *clusterTablesSuite) setUpMockPDHTTPServer() (*httptest.Server, string) {
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
	// pd config
	router.Handle(pdapi.Config, fn.Wrap(mockConfig))
	// TiDB/TiKV config
	router.Handle("/config", fn.Wrap(mockConfig))
	return srv, mockAddr
}

func (s *clusterTablesSuite) newTestKitWithRoot(t *testing.T) *testkit.TestKit {
	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	return tk
}
