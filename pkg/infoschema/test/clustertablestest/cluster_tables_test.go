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

package clustertablestest

import (
	"fmt"
	"math/rand"
	"net"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/fn"
	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/internal"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/mockstorage"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/resourcegrouptag"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	pd "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
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
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
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
	fpName := "github.com/pingcap/tidb/pkg/infoschema/mockClusterInfo"
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
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
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

func TestDataLockWaitsPrivilege(t *testing.T) {
	// setup suite
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()

	dropUserTk := s.newTestKitWithRoot(t)

	tk := s.newTestKitWithRoot(t)

	tk.MustExec("create user 'testuser'@'localhost'")
	defer dropUserTk.MustExec("drop user 'testuser'@'localhost'")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "testuser",
		Hostname: "localhost",
	}, nil, nil, nil))
	err := tk.QueryToErr("select * from information_schema.DATA_LOCK_WAITS")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	tk = s.newTestKitWithRoot(t)
	tk.MustExec("create user 'testuser2'@'localhost'")
	defer dropUserTk.MustExec("drop user 'testuser2'@'localhost'")
	tk.MustExec("grant process on *.* to 'testuser2'@'localhost'")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "testuser2",
		Hostname: "localhost",
	}, nil, nil, nil))
	_ = tk.MustQuery("select * from information_schema.DATA_LOCK_WAITS")
}

func TestSelectClusterTable(t *testing.T) {
	// setup suite
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", &testkit.MockSessionManager{
		PS: []*util.ProcessInfo{
			{
				ID:           1,
				User:         "root",
				Host:         "127.0.0.1",
				Command:      mysql.ComQuery,
				SessionAlias: "alias456",
			},
		},
	})
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)
	slowLogFileName := "tidb-slow0.log"
	internal.PrepareSlowLogfile(t, slowLogFileName)
	defer func() { require.NoError(t, os.Remove(slowLogFileName)) }()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.SlowQueryFile = slowLogFileName
	})

	tk.MustExec("use information_schema")
	tk.MustExec("set @@global.tidb_enable_stmt_summary=1")
	tk.MustExec("set time_zone = '+08:00';")
	tk.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("2"))
	tk.MustQuery("select time from `CLUSTER_SLOW_QUERY` where time='2019-02-12 19:33:56.571953'").Check(testkit.RowsWithSep("|", "2019-02-12 19:33:56.571953"))
	tk.MustQuery("select count(*) from `CLUSTER_PROCESSLIST`").Check(testkit.Rows("1"))
	// skip instance and host column because it now includes the TCP socket details (unstable)
	tk.MustQuery("select id, user, db, command, time, state, info, digest, mem, disk, txnstart, session_alias from `CLUSTER_PROCESSLIST`").Check(testkit.Rows(fmt.Sprintf("1 root <nil> Query 9223372036 %s <nil>  0 0  alias456", "")))
	tk.MustQuery("select query_time, conn_id, session_alias from `CLUSTER_SLOW_QUERY` order by time limit 1").Check(testkit.Rows("4.895492 6 "))
	tk.MustQuery("select query_time, conn_id, session_alias from `CLUSTER_SLOW_QUERY` order by time desc limit 1").Check(testkit.Rows("25.571605962 40507 alias123"))
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

	// Test for https://github.com/pingcap/tidb/issues/33974
	instanceAddr, err := infoschema.GetInstanceAddr(tk.Session())
	require.NoError(t, err)
	tk.MustQuery("select instance from `CLUSTER_SLOW_QUERY` where time='2019-02-12 19:33:56.571953'").Check(testkit.Rows(instanceAddr))
}

func TestSelectClusterTablePrivilege(t *testing.T) {
	// setup suite
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
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
	tk.MustQuery("select * from `CLUSTER_PROCESSLIST`").Check(testkit.Rows(fmt.Sprintf(":10080 1 root 127.0.0.1 <nil> Query 9223372036 %s <nil>  0 0   ", "")))
	tk.MustExec("create user user1")
	tk.MustExec("create user user2")
	user1 := testkit.NewTestKit(t, s.store)
	user1.MustExec("use information_schema")
	require.NoError(t, user1.Session().Auth(&auth.UserIdentity{
		Username: "user1",
		Hostname: "127.0.0.1",
	}, nil, nil, nil))
	user1.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("1"))
	user1.MustQuery("select count(*) from `SLOW_QUERY`").Check(testkit.Rows("1"))
	user1.MustQuery("select user,query from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("user1 select * from t1;"))

	user2 := testkit.NewTestKit(t, s.store)
	user2.MustExec("use information_schema")
	require.NoError(t, user2.Session().Auth(&auth.UserIdentity{
		Username: "user2",
		Hostname: "127.0.0.1",
	}, nil, nil, nil))
	user2.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("2"))
	user2.MustQuery("select user,query from `CLUSTER_SLOW_QUERY` order by query").Check(testkit.Rows("user2 select * from t2;", "user2 select * from t3;"))
}

func TestStmtSummaryEvictedCountTable(t *testing.T) {
	// setup suite
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
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

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "testuser",
		Hostname: "localhost",
	}, nil, nil, nil))

	err := tk.QueryToErr("select * from information_schema.CLUSTER_STATEMENTS_SUMMARY_EVICTED")
	// This error is come from cop(TiDB) fetch from rpc server.
	require.EqualError(t, err, "other error: [planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "testuser2",
		Hostname: "localhost",
	}, nil, nil, nil))
	require.NoError(t, tk.QueryToErr("select * from information_schema.CLUSTER_STATEMENTS_SUMMARY_EVICTED"))
}

func TestStmtSummaryIssue35340(t *testing.T) {
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)

	tk := s.newTestKitWithRoot(t)
	tk.MustExec("set global tidb_stmt_summary_refresh_interval=1800")
	tk.MustExec("set global tidb_stmt_summary_max_stmt_count = 3000")
	for i := 0; i < 100; i++ {
		user := "user" + strconv.Itoa(i)
		tk.MustExec(fmt.Sprintf("create user '%v'@'localhost'", user))
	}
	tk.MustExec("flush privileges")
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk := s.newTestKitWithRoot(t)
			for j := 0; j < 100; j++ {
				user := "user" + strconv.Itoa(j)
				require.NoError(t, tk.Session().Auth(&auth.UserIdentity{
					Username: user,
					Hostname: "localhost",
				}, nil, nil, nil))
				tk.MustQuery("select count(*) from information_schema.statements_summary;")
			}
		}()
	}
	wg.Wait()
}

func TestStmtSummaryHistoryTableWithUserTimezone(t *testing.T) {
	// setup suite
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
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
	date8First, err := time.Parse(time.DateTime, r.Rows()[0][0].(string))
	require.NoError(t, err)
	date8Last, err := time.Parse(time.DateTime, r.Rows()[0][1].(string))
	require.NoError(t, err)
	date8Begin, err := time.Parse(time.DateTime, r.Rows()[0][2].(string))
	require.NoError(t, err)
	date8End, err := time.Parse(time.DateTime, r.Rows()[0][3].(string))
	require.NoError(t, err)
	tk.MustExec("set time_zone = '+01:00';")
	r = tk.MustQuery("select FIRST_SEEN, LAST_SEEN, SUMMARY_BEGIN_TIME, SUMMARY_END_TIME from INFORMATION_SCHEMA.STATEMENTS_SUMMARY_HISTORY order by LAST_SEEN limit 1;")
	date1First, err := time.Parse(time.DateTime, r.Rows()[0][0].(string))
	require.NoError(t, err)
	date1Last, err := time.Parse(time.DateTime, r.Rows()[0][1].(string))
	require.NoError(t, err)
	date1Begin, err := time.Parse(time.DateTime, r.Rows()[0][2].(string))
	require.NoError(t, err)
	date1End, err := time.Parse(time.DateTime, r.Rows()[0][3].(string))
	require.NoError(t, err)

	require.Less(t, date1First.Unix(), date8First.Unix())
	require.Less(t, date1Last.Unix(), date8Last.Unix())
	require.Less(t, date1Begin.Unix(), date8Begin.Unix())
	require.Less(t, date1End.Unix(), date8End.Unix())
}

func TestStmtSummaryHistoryTable(t *testing.T) {
	// setup suite
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
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
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
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
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
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
	if !config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Load() {
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

func TestSlowQueryOOM(t *testing.T) {
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)

	f, err := os.CreateTemp("", "tidb-slow-*.log")
	require.NoError(t, err)
	_, err = f.WriteString(`
# Time: 2022-04-14T10:50:28.185954+08:00
# Txn_start_ts: 432512598850928660
# User@Host: root[root] @ 127.0.0.1 [127.0.0.1]
# Conn_ID: 465
# Query_time: 0.000955269
# Parse_time: 0
# Compile_time: 0.000486719
# Rewrite_time: 0.000142467
# Optimize_time: 0.000312527
# Wait_TS: 0.000004489
# Cop_time: 0.000169235 Request_count: 2
# DB: test
# Index_names: [t_normal_oltp:idx0]
# Is_internal: false
# Digest: dcb13f841a568ec94baec50c88d0679c533bbd65539ba8fee6deb2e39881acdd
# Stats: t_normal_oltp:432512598027796484
# Num_cop_tasks: 2
# Cop_proc_avg: 0 Cop_proc_p90: 0 Cop_proc_max: 0 Cop_proc_addr: store1
# Cop_wait_avg: 0 Cop_wait_p90: 0 Cop_wait_max: 0 Cop_wait_addr: store1
# Mem_max: 11372
# Prepared: true
# Plan_from_cache: false
# Plan_from_binding: false
# Has_more_results: false
# KV_total: 0
# PD_total: 0.000000671
# Backoff_total: 0
# Write_sql_response_total: 0.000000606
# Result_rows: 1
# Succ: true
# IsExplicitTxn: false
# Plan: tidb_decode_plan('lQeAMAk1XzEwCTAJMQlmdW5jczpzdW0oQ29sdW1uIzEyKS0+DQzwUjYJMQl0aW1lOjQyMS45wrVzLCBsb29wczoyCTEuNDUgS0IJTi9BCjEJM18yNwkwCTAJY2FzdChwbHVzKHRlc3QudF9ub3JtYWxfb2x0cC5hLCB0RhYAXGIpLCBkZWNpbWFsKDIwLDApIEJJTkFSWRmHDDEyCTERiQQxOC6HAGwsIENvbmN1cnJlbmN5Ok9GRgk3NjAgQnl0ZXMJAZoYMgkzMF8yNAWbGUQMMDkuNzZGAEhpbmRleF90YXNrOiB7dG90YWxfBfgUIDEwMS4yBSwsZmV0Y2hfaGFuZGxlARgIMC4xBRiAYnVpbGQ6IDQ2OW5zLCB3YWl0OiA1OTVuc30sIHRhYmxlTlcADDI1Ny4pUCBudW06IDEsIGMdyBwgNX0JOC45MTFgODMJNDdfMjIJMV8wCTAJdAFVADoyWgEALAnBwDppZHgwKGEpLCByYW5nZTpbNjY4Mzk4LDY2ODQwOF0sIGtlZXAgb3JkZXI6ZmFsc2U1Ewg5Ni4uWAEAMwGQAHARuRGjGG1heDogNzQF9kRwcm9jX2tleXM6IDAsIHJwY18RJgEMKTwENjMN5GRjb3ByX2NhY2hlX2hpdF9yYXRpbzogMC4wMCEkCGlrdglqAHsFNwA1LokAFDB9CU4vQQEEIQUMNV8yM24FAWbfAAA1LcVNvmrfAAAwot8ADDU5LjYFLbbfAAQzLlrhAA==')
# Plan_digest: e7b1a5789200cb6d91aaac8af3f5560af51870369bac2e247b84fe9b5e754cbe
select sum(a+b) from test.t_normal_oltp where a >= ? and a <= ? [arguments: (668398, 668408)];
# Time: 2022-04-14T10:50:28.185987+08:00
select * from t;
# Time: 2022-04-14T10:50:28.186028+08:00
select * from t1;
`)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	executor.ParseSlowLogBatchSize = 1
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		executor.ParseSlowLogBatchSize = 64
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, os.Remove(newCfg.Log.SlowQueryFile))
	}()
	// The server default is CANCEL, but the testsuite defaults to LOG
	tk.MustExec("set global tidb_mem_oom_action='CANCEL'")
	defer tk.MustExec("set global tidb_mem_oom_action='LOG'")
	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", f.Name()))
	// Align with the timezone in slow log files
	tk.MustExec("set @@time_zone='+08:00'")
	checkFn := func(quota int) {
		tk.MustExec("set tidb_mem_quota_query=" + strconv.Itoa(quota)) // session

		err = tk.QueryToErr("select * from `information_schema`.`slow_query` where time > '2022-04-14 00:00:00' and time < '2022-04-15 00:00:00'")
		require.Error(t, err, quota)
		require.True(t, exeerrors.ErrMemoryExceedForQuery.Equal(err))
	}
	memQuotas := []int{128, 512, 1024, 2048, 4096}
	for _, quota := range memQuotas {
		checkFn(quota)
	}
	for i := 0; i < 100; i++ {
		quota := rand.Int()%8192 + 1
		checkFn(quota)
	}

	newMemQuota := 1024 * 1024 * 1024
	tk.MustExec("set @@tidb_mem_quota_query=" + strconv.Itoa(newMemQuota))
	tk.MustQuery("select * from `information_schema`.`slow_query` where time > '2022-04-14 00:00:00' and time < '2022-04-15 00:00:00'")
	mem := tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed()
	require.Equal(t, mem, int64(0))
	tk.MustQuery("select * from `information_schema`.`cluster_slow_query` where time > '2022-04-14 00:00:00' and time < '2022-04-15 00:00:00'")
}

func (s *clusterTablesSuite) setUpRPCService(t *testing.T, addr string, sm util.SessionManager) (*grpc.Server, string) {
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	// Fix issue 9836
	if sm == nil {
		sm = &testkit.MockSessionManager{PS: make([]*util.ProcessInfo, 1)}
		sm.(*testkit.MockSessionManager).PS[0] = &util.ProcessInfo{
			ID:      1,
			User:    "root",
			Host:    "127.0.0.1",
			Command: mysql.ComQuery,
		}
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
	// pd config
	router.Handle(pd.Config, fn.Wrap(mockConfig))
	// TiDB/TiKV config
	router.Handle("/config", fn.Wrap(mockConfig))
	return srv, mockAddr
}

func (s *clusterTablesSuite) newTestKitWithRoot(t *testing.T) *testkit.TestKit {
	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	return tk
}

func TestMDLView(t *testing.T) {
	testCases := []struct {
		name        string
		createTable []string
		ddl         string
		queryInTxn  []string
		sqlDigest   string
	}{
		{"add column", []string{"create table t(a int)"}, "alter table test.t add column b int", []string{"select 1", "select * from t"}, "[\"begin\",\"select ?\",\"select * from `t`\"]"},
		{"change column in 1 step", []string{"create table t(a int)"}, "alter table test.t change column a b int", []string{"select 1", "select * from t"}, "[\"begin\",\"select ?\",\"select * from `t`\"]"},
		{"rename tables", []string{"create table t(a int)", "create table t1(a int)"}, "rename table test.t to test.t2, test.t1 to test.t3", []string{"select 1", "select * from t"}, "[\"begin\",\"select ?\",\"select * from `t`\"]"},
		{"err don't show rollbackdone ddl", []string{"create table t(a int)", "insert into t values (1);", "insert into t values (1);", "alter table t add unique idx(id);"}, "alter table test.t add column b int", []string{"select 1", "select * from t"}, "[\"begin\",\"select ?\",\"select * from `t`\"]"},
	}
	save := privileges.SkipWithGrant
	privileges.SkipWithGrant = true
	defer func() {
		privileges.SkipWithGrant = save
	}()
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			// setup suite
			s := new(clusterTablesSuite)
			s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
			s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
			s.startTime = time.Now()
			defer s.httpServer.Close()

			tk := s.newTestKitWithRoot(t)
			tkDDL := s.newTestKitWithRoot(t)
			tk3 := s.newTestKitWithRoot(t)
			tk.MustExec("use test")
			tk.MustExec("set global tidb_enable_metadata_lock=1")
			for _, cr := range c.createTable {
				if strings.Contains(c.name, "err") {
					_, _ = tk.Exec(cr)
				} else {
					tk.MustExec(cr)
				}
			}

			tk.MustExec("begin")
			for _, q := range c.queryInTxn {
				tk.MustQuery(q)
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				tkDDL.MustExec(c.ddl)
				wg.Done()
			}()

			time.Sleep(200 * time.Millisecond)

			s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", tk3.Session().GetSessionManager())
			defer s.rpcserver.Stop()

			tk3.MustQuery("select DB_NAME, QUERY, SQL_DIGESTS from mysql.tidb_mdl_view").Check(testkit.Rows(
				strings.Join([]string{
					"test",
					c.ddl,
					c.sqlDigest,
				}, " "),
			))

			tk.MustExec("commit")

			wg.Wait()
		})
	}
}

func TestMDLViewPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustQuery("select * from mysql.tidb_mdl_view;").Check(testkit.Rows())
	tk.MustExec("create user 'test'@'%' identified by '';")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test", Hostname: "%"}, nil, nil, nil))
	_, err := tk.Exec("select * from mysql.tidb_mdl_view;")
	require.ErrorContains(t, err, "view lack rights")

	// grant all privileges to test user.
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("grant all privileges on *.* to 'test'@'%';")
	tk.MustExec("flush privileges;")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test", Hostname: "%"}, nil, nil, nil))
	tk.MustQuery("select * from mysql.tidb_mdl_view;").Check(testkit.Rows())
}

func TestQuickBinding(t *testing.T) {
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))

	tk.MustExec("use test")
	tk.MustExec(`create table t1 (pk int, a int, b int, c int, primary key(pk), key k_a(a), key k_bc(b, c))`)
	tk.MustExec(`create table t2 (a int, b int, c int, key k_a(a), key k_bc(b, c))`) // no primary key

	type testCase struct {
		template                string
		expectedHint            string
		dmlAndSubqueryTemplates []string
	}
	subQueryTemp := []string{
		"select a from (?) tx where tx.c<100",
		"select * from (?) tx1, (?) tx2 where tx1.c<100 and tx2.c<100",
	}
	testCases := []testCase{
		// access path selection with use_index / ignore_index
		{`select /*+ use_index(t1, k_a) */ * from t1 where b=?`, "use_index(@`sel_1` `test`.`t1` `k_a`), no_order_index(@`sel_1` `test`.`t1` `k_a`)", subQueryTemp},
		{`select /*+ use_index(t1, k_bc) */ * from t1 where a=?`, "use_index(@`sel_1` `test`.`t1` `k_bc`), no_order_index(@`sel_1` `test`.`t1` `k_bc`)", subQueryTemp},
		{`select /*+ use_index(t1, primary) */ * from t1 where a=? and b=?`, "use_index(@`sel_1` `test`.`t1` ), no_order_index(@`sel_1` `test`.`t1` `primary`)", subQueryTemp},
		{`select /*+ ignore_index(t1, k_a, k_bc) */ * from t1 where a=? and b=?`, "use_index(@`sel_1` `test`.`t1` ), no_order_index(@`sel_1` `test`.`t1` `primary`), ignore_index(`t1` `k_a`, `k_bc`)", subQueryTemp},
		{`select /*+ use_index(t1) */ * from t1 where a=? and b=?`, "use_index(@`sel_1` `test`.`t1` ), no_order_index(@`sel_1` `test`.`t1` `primary`)", subQueryTemp},
		{`select /*+ use_index(t2) */ * from t2 where a=? and b=?`, "use_index(@`sel_1` `test`.`t2` )", subQueryTemp},

		// aggregation
		{`select /*+ hash_agg(), use_index(t1, primary), agg_to_cop() */ count(*) from t1 where a<?`, "hash_agg(@`sel_1`), use_index(@`sel_1` `test`.`t1` ), no_order_index(@`sel_1` `test`.`t1` `primary`), agg_to_cop(@`sel_1`)", nil},
		{`select /*+ hash_agg(), use_index(t1, primary), agg_to_cop() */ count(*), b from t1 where a<? group by b`, "hash_agg(@`sel_1`), use_index(@`sel_1` `test`.`t1` ), no_order_index(@`sel_1` `test`.`t1` `primary`), agg_to_cop(@`sel_1`)", nil},
		{`select /*+ stream_agg(), use_index(t1, primary), agg_to_cop() */ count(*) from t1 where a<?`, "stream_agg(@`sel_1`), use_index(@`sel_1` `test`.`t1` ), no_order_index(@`sel_1` `test`.`t1` `primary`), agg_to_cop(@`sel_1`)", nil},
		{`select /*+ stream_agg(), use_index(t1, primary), agg_to_cop() */ count(*), b from t1 where a<? group by b`, "stream_agg(@`sel_1`), use_index(@`sel_1` `test`.`t1` ), no_order_index(@`sel_1` `test`.`t1` `primary`)", nil},
		{`select a+b+? from (select /*+ stream_agg() */ count(*) as a from t1) tt1, (select /*+ hash_agg() */ count(*) as b from t1) tt2`, "stream_agg(@`sel_2`), use_index(@`sel_2` `test`.`t1` `k_a`), no_order_index(@`sel_2` `test`.`t1` `k_a`), agg_to_cop(@`sel_2`), hash_agg(@`sel_3`), use_index(@`sel_3` `test`.`t1` `k_a`), no_order_index(@`sel_3` `test`.`t1` `k_a`), agg_to_cop(@`sel_3`)", nil},

		// 2-way hash joins
		{`select /*+ hash_join(t1, t2), use_index(t1), use_index(t2) */ t1.* from t1, t2 where t1.a=t2.a and t1.a<?`, "hash_join(@`sel_1` `test`.`t1`), use_index(@`sel_1` `test`.`t1` ), no_order_index(@`sel_1` `test`.`t1` `primary`), use_index(@`sel_1` `test`.`t2` )", nil},
		// not support, fix them later on
		//{`select /*+ hash_join_build(t1), use_index(t1), use_index(t2) */ * from t1, t2 where t1.a=t2.a and t1.a<?`, "hash_join_build(@`sel_1` `test`.`t1`), use_index(@`sel_1` `test`.`t1` ), use_index(@`sel_1` `test`.`t2` )", nil},
		//{`select /*+ hash_join_build(t2), use_index(t1), use_index(t2) */ * from t1, t2 where t1.a=t2.a and t1.a<?`, "hash_join_build(@`sel_1` `test`.`t1`), use_index(@`sel_1` `test`.`t1` ), use_index(@`sel_1` `test`.`t2` )", nil},
		//{`select /*+ hash_join_probe(t1), use_index(t1), use_index(t2) */ * from t1, t2 where t1.a=t2.a and t1.a<?`, "hash_join_build(@`sel_1` `test`.`t1`), use_index(@`sel_1` `test`.`t1` ), use_index(@`sel_1` `test`.`t2` )", nil},
		//{`select /*+ hash_join_probe(t2), use_index(t1), use_index(t2) */ * from t1, t2 where t1.a=t2.a and t1.a<?`, "hash_join_build(@`sel_1` `test`.`t1`), use_index(@`sel_1` `test`.`t1` ), use_index(@`sel_1` `test`.`t2` )", nil},

		// 2-way index join
		{`select /*+ inl_join(t1) */ * from t1, t2 where t1.a=t2.a and t1.b<? and t2.b<?`, "inl_join(@`sel_1` `test`.`t1`), use_index(@`sel_1` `test`.`t1` `k_a`), no_order_index(@`sel_1` `test`.`t1` `k_a`), use_index(@`sel_1` `test`.`t2` )", nil},
		{`select /*+ inl_join(t2) */ * from t1, t2 where t1.a=t2.a and t1.b<? and t2.b<?`, "inl_join(@`sel_1` `test`.`t2`), use_index(@`sel_1` `test`.`t1` ), no_order_index(@`sel_1` `test`.`t1` `primary`), use_index(@`sel_1` `test`.`t2` `k_a`), no_order_index(@`sel_1` `test`.`t2` `k_a`)", nil},
		{`select /*+ inl_join(t1) */ * from t1, t2 where t1.b=t2.b and t1.c=t2.c and t1.a<? and t2.a<?`, "inl_join(@`sel_1` `test`.`t1`), use_index(@`sel_1` `test`.`t1` `k_bc`), no_order_index(@`sel_1` `test`.`t1` `k_bc`), use_index(@`sel_1` `test`.`t2` )", nil},
		{`select /*+ inl_join(t2) */ * from t1, t2 where t1.b=t2.b and t1.c=t2.c and t1.a<? and t2.a<?`, "inl_join(@`sel_1` `test`.`t2`), use_index(@`sel_1` `test`.`t1` ), no_order_index(@`sel_1` `test`.`t1` `primary`), use_index(@`sel_1` `test`.`t2` `k_bc`), no_order_index(@`sel_1` `test`.`t2` `k_bc`)", nil},
		{`select /*+ inl_join(t1) */ * from t1, t2 where t1.a=t2.a and t1.b<? and t2.b<? order by t1.a limit 5`, "inl_join(@`sel_1` `test`.`t1`), use_index(@`sel_1` `test`.`t1` `k_a`), no_order_index(@`sel_1` `test`.`t1` `k_a`), use_index(@`sel_1` `test`.`t2` )", nil},
		{`select /*+ inl_join(t2) */ * from t1, t2 where t1.a=t2.a and t1.b<? and t2.b<? order by t1.a limit 5`, "inl_join(@`sel_1` `test`.`t2`), use_index(@`sel_1` `test`.`t1` `k_a`), order_index(@`sel_1` `test`.`t1` `k_a`), use_index(@`sel_1` `test`.`t2` `k_a`), no_order_index(@`sel_1` `test`.`t2` `k_a`)", nil},

		// 2-way index hash join
		{`select /*+ inl_hash_join(t1) */ * from t1, t2 where t1.a=t2.a and t1.b<? and t2.b<?`, "inl_hash_join(@`sel_1` `test`.`t1`), use_index(@`sel_1` `test`.`t1` `k_a`), no_order_index(@`sel_1` `test`.`t1` `k_a`), use_index(@`sel_1` `test`.`t2` )", nil},
		{`select /*+ inl_hash_join(t2) */ * from t1, t2 where t1.a=t2.a and t1.b<? and t2.b<?`, "inl_hash_join(@`sel_1` `test`.`t2`), use_index(@`sel_1` `test`.`t1` ), no_order_index(@`sel_1` `test`.`t1` `primary`), use_index(@`sel_1` `test`.`t2` `k_a`), no_order_index(@`sel_1` `test`.`t2` `k_a`)", nil},
		{`select /*+ inl_hash_join(t1) */ * from t1, t2 where t1.b=t2.b and t1.c=t2.c and t1.a<? and t2.a<?`, "inl_hash_join(@`sel_1` `test`.`t1`), use_index(@`sel_1` `test`.`t1` `k_bc`), no_order_index(@`sel_1` `test`.`t1` `k_bc`), use_index(@`sel_1` `test`.`t2` )", nil},
		{`select /*+ inl_hash_join(t2) */ * from t1, t2 where t1.b=t2.b and t1.c=t2.c and t1.a<? and t2.a<?`, "inl_hash_join(@`sel_1` `test`.`t2`), use_index(@`sel_1` `test`.`t1` ), no_order_index(@`sel_1` `test`.`t1` `primary`), use_index(@`sel_1` `test`.`t2` `k_bc`), no_order_index(@`sel_1` `test`.`t2` `k_bc`)", nil},
		{`select /*+ inl_hash_join(t1) */ * from t1, t2 where t1.a=t2.a and t1.b<? and t2.b<? order by t1.a limit 5`, "inl_hash_join(@`sel_1` `test`.`t1`), use_index(@`sel_1` `test`.`t1` `k_a`), no_order_index(@`sel_1` `test`.`t1` `k_a`), use_index(@`sel_1` `test`.`t2` )", nil},
		{`select /*+ inl_hash_join(t2) */ * from t1, t2 where t1.a=t2.a and t1.b<? and t2.b<? order by t1.a limit 5`, "inl_hash_join(@`sel_1` `test`.`t2`), use_index(@`sel_1` `test`.`t1` `k_a`), order_index(@`sel_1` `test`.`t1` `k_a`), use_index(@`sel_1` `test`.`t2` `k_a`), no_order_index(@`sel_1` `test`.`t2` `k_a`)", nil},

		// 2-way merge joins
		{`select /*+ merge_join(t1, t2), use_index(t1), use_index(t2) */ t1.* from t1, t2 where t1.a=t2.a and t1.a<?`, "merge_join(@`sel_1` `test`.`t1`), use_index(@`sel_1` `test`.`t1` ), no_order_index(@`sel_1` `test`.`t1` `primary`), use_index(@`sel_1` `test`.`t2` )", nil},
		{`select /*+ merge_join(t1, t2), use_index(t1, k_a), use_index(t2, k_a) */ t1.* from t1, t2 where t1.a=t2.a and t1.a<?`, "merge_join(@`sel_1` `test`.`t1`), use_index(@`sel_1` `test`.`t1` `k_a`), order_index(@`sel_1` `test`.`t1` `k_a`), use_index(@`sel_1` `test`.`t2` `k_a`), order_index(@`sel_1` `test`.`t2` `k_a`)", nil},

		// limit_to_cop
		{`select /*+ limit_to_cop(), use_index(t1, k_a) */ * from t1 where a < ? limit 100`, "use_index(@`sel_1` `test`.`t1` `k_a`), no_order_index(@`sel_1` `test`.`t1` `k_a`), limit_to_cop(@`sel_1`)", nil},
		{`select /*+ limit_to_cop(), use_index(t1, k_a) */ * from t1 where b < ? limit 100`, "use_index(@`sel_1` `test`.`t1` `k_a`), no_order_index(@`sel_1` `test`.`t1` `k_a`), limit_to_cop(@`sel_1`)", nil},
		{`select /*+ limit_to_cop(), use_index(t1, k_a) */ * from t1 where a < ? order by a limit 100`, "use_index(@`sel_1` `test`.`t1` `k_a`), order_index(@`sel_1` `test`.`t1` `k_a`), limit_to_cop(@`sel_1`)", nil},
		{`select /*+ limit_to_cop(), use_index(t1, k_a) */ * from t1 where a < ? order by b limit 100`, "use_index(@`sel_1` `test`.`t1` `k_a`), no_order_index(@`sel_1` `test`.`t1` `k_a`), limit_to_cop(@`sel_1`)", nil},

		// index merge
		{`select /*+ use_index_merge(t1, primary, k_a, k_bc) */ * from t1 where pk<? and a<? and b<1`, "use_index_merge(@`sel_1` `t1` `k_a`, `k_bc`)", nil},
		{`select /*+ use_index_merge(t1, primary, k_a, k_bc) */ * from t1 where pk<? or a<? or b<1`, "use_index_merge(@`sel_1` `t1` `primary`, `k_a`, `k_bc`)", nil},
		{`select /*+ use_index_merge(t2, k_a, k_bc) */ * from t2 where a<? and b<1 and c<1`, "use_index_merge(@`sel_1` `t2` `k_a`, `k_bc`)", nil},
		{`select /*+ use_index_merge(t2, k_a, k_bc) */ * from t2 where a<? or b<1 and c<1`, "use_index_merge(@`sel_1` `t2` `k_a`, `k_bc`)", nil},
	}

	removeHint := func(sql string) string {
		for {
			b := strings.Index(sql, "/*+")
			e := strings.Index(sql, "*/")
			if b == -1 || e == -1 {
				return sql
			}
			sql = sql[:b] + sql[e+2:]
		}
	}
	randValue := func() string {
		switch rand.Intn(4) {
		case 0:
			return "0"
		case 1:
			return "-9999999999999"
		case 2:
			return "9999999999999"
		default:
			width := 100000
			return strconv.Itoa(width - rand.Intn(width*2))
		}
	}
	fillValues := func(sql string) string {
		for strings.Contains(sql, "?") {
			sql = strings.Replace(sql, "?", randValue(), 1)
		}
		return sql
	}
	genPrepSQL := func(sql string) (prepStmt, setStmt, execStmt string) {
		sql = removeHint(sql)
		prepStmt = fmt.Sprintf("prepare st from '%v'", sql)
		nParam := strings.Count(sql, "?")
		var x, y []string
		for i := 0; i < nParam; i++ {
			x = append(x, fmt.Sprintf("@a%d=%v", i, randValue()))
			y = append(y, fmt.Sprintf("@a%d", i))
		}
		setStmt = fmt.Sprintf("set %v", strings.Join(x, ", "))
		execStmt = fmt.Sprintf("execute st using %v", strings.Join(y, ", "))
		return
	}

	// test general queries and prepared / execute statements
	for _, tc := range testCases {
		stmtsummary.StmtSummaryByDigestMap.Clear()
		firstSQL := fillValues(tc.template)
		tk.MustExec(firstSQL)
		result := tk.MustQuery(`select plan_hint, digest, plan_digest from information_schema.statements_summary`).Rows()
		planHint, sqlDigest, planDigest := result[0][0].(string), result[0][1].(string), result[0][2].(string)
		require.Equal(t, tc.expectedHint, planHint)
		tk.MustExec(fmt.Sprintf(`create session binding from history using plan digest '%v'`, planDigest))

		// normal test
		sqlWithoutHint := removeHint(tc.template)
		for i := 0; i < 5; i++ {
			stmtsummary.StmtSummaryByDigestMap.Clear()
			testSQL := fillValues(sqlWithoutHint)
			tk.MustExec(testSQL)
			tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
			// has the same plan-digest
			tk.MustQuery(fmt.Sprintf(`select plan_digest from information_schema.statements_summary where digest='%v'`, sqlDigest)).Check(testkit.Rows(planDigest))
		}

		// test with prepared / execute protocol
		for i := 0; i < 5; i++ {
			stmtsummary.StmtSummaryByDigestMap.Clear()
			prepStmt, setStmt, execStmt := genPrepSQL(tc.template)
			tk.MustExec(prepStmt)
			tk.MustExec(setStmt)
			tk.MustExec(execStmt)
			tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
			if strings.Contains(tc.expectedHint, "use_index_merge") {
				continue // for safety, plan cache keeps some extra predicates upon the IndexMerge's TableScan, so the plan-digest may be different
			}

			// has the same plan-digest
			tk.MustQuery(fmt.Sprintf(`select plan_digest from information_schema.statements_summary where digest='%v'`, sqlDigest)).Check(testkit.Rows(planDigest))
		}

		tk.MustExec(fmt.Sprintf(`drop session binding for %s`, firstSQL))
	}

	// test with DML and sub-query
	for _, tc := range testCases {
		for _, temp := range tc.dmlAndSubqueryTemplates {
			temp = strings.Replace(temp, "?", tc.template, -1)
			stmtsummary.StmtSummaryByDigestMap.Clear()
			firstSQL := fillValues(temp)
			tk.MustExec(firstSQL)
			result := tk.MustQuery(`select plan_hint, digest, plan_digest from information_schema.statements_summary`).Rows()
			_, sqlDigest, planDigest := result[0][0].(string), result[0][1].(string), result[0][2].(string)
			tk.MustExec(fmt.Sprintf(`create session binding from history using plan digest '%v'`, planDigest))

			// normal test
			sqlWithoutHint := removeHint(temp)
			for i := 0; i < 5; i++ {
				stmtsummary.StmtSummaryByDigestMap.Clear()
				testSQL := fillValues(sqlWithoutHint)
				tk.MustExec(testSQL)
				tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
				// has the same plan-digest
				tk.MustQuery(fmt.Sprintf(`select plan_digest from information_schema.statements_summary where digest='%v'`, sqlDigest)).Check(testkit.Rows(planDigest))
			}

			tk.MustExec(fmt.Sprintf(`drop session binding for %s`, firstSQL))
		}
	}
}

// for testing, only returns Original_sql, Bind_sql, Default_db, Status, Source, Sql_digest
func showBinding(tk *testkit.TestKit, showStmt string) [][]any {
	rows := tk.MustQuery(showStmt).Sort().Rows()
	result := make([][]any, len(rows))
	for i, r := range rows {
		result[i] = append(result[i], r[:4]...)
		result[i] = append(result[i], r[8:10]...)
	}
	return result
}

func TestUniversalBindingFromHistory(t *testing.T) {
	t.Skip("skip it temporarily")
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))

	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, key(a), key(b), key(c))`)
	tk.MustExec(`select /*+ use_index(t, b) */ a from t where a=1`)
	tk.MustExec(`select /*+ use_index(t, c) */ b from t where b=1`)

	planDigest := tk.MustQuery(`select plan_digest from information_schema.statements_summary where query_sample_text='select /*+ use_index(t, b) */ a from t where a=1'`).Rows()
	tk.MustExec(fmt.Sprintf("create global universal binding from history using plan digest '%s'", planDigest[0][0].(string)))
	planDigest = tk.MustQuery(`select plan_digest from information_schema.statements_summary where query_sample_text='select /*+ use_index(t, c) */ b from t where b=1'`).Rows()
	tk.MustExec(fmt.Sprintf("create global universal binding from history using plan digest '%s'", planDigest[0][0].(string)))

	require.Equal(t, showBinding(tk, `show global bindings`), [][]any{
		{"select `a` from `t` where `a` = ?", "SELECT /*+ use_index(@`sel_1` `t` `b`) no_order_index(@`sel_1` `t` `b`)*/ `a` FROM `t` WHERE `a` = 1", "", "enabled", "history", "f8e294e078ed195998dee6717e71499d6a14b8e0f405952af8d0a5b24d0cae30"},
		{"select `b` from `t` where `b` = ?", "SELECT /*+ use_index(@`sel_1` `t` `c`) no_order_index(@`sel_1` `t` `c`)*/ `b` FROM `t` WHERE `b` = 1", "", "enabled", "history", "cfb4dd59c4c75ff1ee126236c6bd365f7d04f6120990d922e75aa47ae8bd94eb"},
	})

	tk.MustExec(`admin reload bindings`)
	tk.MustExec(`set @@tidb_opt_enable_fuzzy_binding=1`)
	tk.MustExec(`create database test2`)
	tk.MustExec(`use test2`)
	tk.MustExec(`create table t (a int, b int, c int, key(a), key(b), key(c))`)
	tk.MustExec(`select a from t where a=10`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
	tk.MustExec(`select b from t where b=10`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
	tk.MustExec(`select b from test.t where b=10`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
}

func TestCreateBindingFromHistory(t *testing.T) {
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t1(id int primary key, a int, b int, key(a))")
	tk.MustExec("create table t2(id int primary key, a int, b int, key(a))")

	var testCases = []struct {
		sqls []string
		hint string
	}{
		{
			sqls: []string{
				"select %s * from t1, t2 where t1.id = t2.id",
				"select %s * from test.t1, t2 where t1.id = t2.id",
				"select %s * from test.t1, test.t2 where t1.id = t2.id",
				"select %s * from t1, test.t2 where t1.id = t2.id",
			},
			hint: "/*+ merge_join(t1, t2) */",
		},
		{
			sqls: []string{
				"select %s * from t1 where a = 1",
				"select %s * from test.t1 where a = 1",
			},
			hint: "/*+ ignore_index(t, a) */",
		},
	}

	for _, testCase := range testCases {
		for _, bind := range testCase.sqls {
			stmtsummary.StmtSummaryByDigestMap.Clear()
			bindSQL := fmt.Sprintf(bind, testCase.hint)
			tk.MustExec(bindSQL)
			planDigest := tk.MustQuery(fmt.Sprintf("select plan_digest from information_schema.statements_summary where query_sample_text = '%s'", bindSQL)).Rows()
			tk.MustExec(fmt.Sprintf("create session binding from history using plan digest '%s'", planDigest[0][0]))
			showRes := tk.MustQuery("show bindings").Rows()
			require.Equal(t, len(showRes), 1)
			require.Equal(t, planDigest[0][0], showRes[0][10])
			for _, sql := range testCase.sqls {
				tk.MustExec(fmt.Sprintf(sql, ""))
				tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
			}
		}
		showRes := tk.MustQuery("show bindings").Rows()
		require.Equal(t, len(showRes), 1)
		tk.MustExec(fmt.Sprintf("drop binding for sql digest '%s'", showRes[0][9]))
	}

	// exception cases
	tk.MustGetErrMsg(fmt.Sprintf("create binding from history using plan digest '%s'", "1"), "can't find any plans for '1'")
	tk.MustGetErrMsg(fmt.Sprintf("create binding from history using plan digest '%s'", ""), "plan digest is empty")
	tk.MustExec("create binding for select * from t1, t2 where t1.id = t2.id using select /*+ merge_join(t1, t2) */ * from t1, t2 where t1.id = t2.id")
	showRes := tk.MustQuery("show bindings").Rows()
	require.Equal(t, showRes[0][10], "") // plan digest should be nil by create for
}

func TestCreateBindingForPrepareFromHistory(t *testing.T) {
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, a int, key(a))")

	tk.MustExec("prepare stmt from 'select /*+ ignore_index(t,a) */ * from t where a = ?'")
	tk.MustExec("set @a = 1")
	tk.MustExec("execute stmt using @a")
	planDigest := tk.MustQuery(fmt.Sprintf("select plan_digest from information_schema.statements_summary where query_sample_text = '%s'", "select /*+ ignore_index(t,a) */ * from t where a = ? [arguments: 1]")).Rows()
	showRes := tk.MustQuery("show bindings").Rows()
	require.Equal(t, len(showRes), 0)
	tk.MustExec(fmt.Sprintf("create binding from history using plan digest '%s'", planDigest[0][0]))
	showRes = tk.MustQuery("show bindings").Rows()
	require.Equal(t, len(showRes), 1)
	require.Equal(t, planDigest[0][0], showRes[0][10])
	tk.MustExec("execute stmt using @a")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
}

func TestErrorCasesCreateBindingFromHistory(t *testing.T) {
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1(id int)")
	tk.MustExec("create table t2(id int)")
	tk.MustExec("create table t3(id int)")

	sql := "select * from t1 where t1.id in (select id from t2)"
	tk.MustExec(sql)
	planDigest := tk.MustQuery(fmt.Sprintf("select plan_digest from information_schema.statements_summary where query_sample_text = '%s'", sql)).Rows()
	tk.MustExec(fmt.Sprintf("create binding from history using plan digest '%s'", planDigest[0][0]))
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 auto-generated hint for queries with sub queries might not be complete, the plan might change even after creating this binding"))

	sql = "select * from t1, t2, t3 where t1.id = t2.id and t2.id = t3.id"
	tk.MustExec(sql)
	planDigest = tk.MustQuery(fmt.Sprintf("select plan_digest from information_schema.statements_summary where query_sample_text = '%s'", sql)).Rows()
	tk.MustExec(fmt.Sprintf("create binding from history using plan digest '%s'", planDigest[0][0]))
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 auto-generated hint for queries with more than 3 table join might not be complete, the plan might change even after creating this binding"))
}

// withMockTiFlash sets the mockStore to have N TiFlash stores (naming as tiflash0, tiflash1, ...).
func withMockTiFlash(nodes int) mockstore.MockTiKVStoreOption {
	return mockstore.WithMultipleOptions(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockCluster := c.(*unistore.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			tiflashIdx := 0
			for tiflashIdx < nodes {
				store2 := c.AllocID()
				peer2 := c.AllocID()
				addr2 := fmt.Sprintf("tiflash%d", tiflashIdx)
				mockCluster.AddStore(store2, addr2, &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
				mockCluster.AddPeer(region1, store2, peer2)
				tiflashIdx++
			}
		}),
		mockstore.WithStoreType(mockstore.EmbedUnistore),
	)
}

func TestBindingFromHistoryWithTiFlashBindable(t *testing.T) {
	s := new(clusterTablesSuite)
	_, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.store = testkit.CreateMockStore(t, withMockTiFlash(1))
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("alter table test.t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")

	sql := "select * from t"
	tk.MustExec(sql)
	planDigest := tk.MustQuery(fmt.Sprintf("select plan_digest from information_schema.cluster_statements_summary where query_sample_text = '%s'", sql)).Rows()
	tk.MustExec(fmt.Sprintf("create binding from history using plan digest '%s'", planDigest[0][0]))
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 auto-generated hint for queries accessing TiFlash might not be complete, the plan might change even after creating this binding"))
}

func TestSetBindingStatusBySQLDigest(t *testing.T) {
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int, a int, key(a))")
	sql := "select /*+ ignore_index(t, a) */ * from t where t.a = 1"
	tk.MustExec(sql)
	planDigest := tk.MustQuery(fmt.Sprintf("select plan_digest from information_schema.cluster_statements_summary where query_sample_text = '%s'", sql)).Rows()
	tk.MustExec(fmt.Sprintf("create global binding from history using plan digest '%s'", planDigest[0][0]))
	sql = "select * from t where t.a = 1"
	tk.MustExec(sql)
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))

	sqlDigest := tk.MustQuery("show global bindings").Rows()
	tk.MustExec(fmt.Sprintf("set binding disabled for sql digest '%s'", sqlDigest[0][9]))
	tk.MustExec(sql)
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	tk.MustExec(fmt.Sprintf("set binding enabled for sql digest '%s'", sqlDigest[0][9]))
	tk.MustExec(sql)
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	tk.MustGetErrMsg("set binding enabled for sql digest ''", "sql digest is empty")
	tk.MustGetErrMsg("set binding disabled for sql digest ''", "sql digest is empty")
}

func TestCreateBindingForNotSupportedStmt(t *testing.T) {
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, a int, key(a))")

	sql := "admin show ddl jobs"
	tk.MustExec(sql)
	planDigest := tk.MustQuery(fmt.Sprintf("select plan_digest from information_schema.statements_summary where query_sample_text = '%s'", sql)).Rows()
	tk.MustGetErrMsg(fmt.Sprintf("create session binding from history using plan digest '%s'", planDigest[0][0]), fmt.Sprintf("can't find any plans for '%s'", planDigest[0][0]))

	sql = "show tables"
	tk.MustExec(sql)
	planDigest = tk.MustQuery(fmt.Sprintf("select plan_digest from information_schema.statements_summary where query_sample_text = '%s'", sql)).Rows()
	tk.MustGetErrMsg(fmt.Sprintf("create session binding from history using plan digest '%s'", planDigest[0][0]), fmt.Sprintf("can't find any plans for '%s'", planDigest[0][0]))

	sql = "explain select /*+ ignore_index(t, a) */ * from t where a = 1"
	tk.MustExec(sql)
	planDigest = tk.MustQuery(fmt.Sprintf("select plan_digest from information_schema.statements_summary where query_sample_text = '%s'", sql)).Rows()
	tk.MustGetErrMsg(fmt.Sprintf("create session binding from history using plan digest '%s'", planDigest[0][0]), fmt.Sprintf("can't find any plans for '%s'", planDigest[0][0]))
}

func TestCreateBindingRepeatedly(t *testing.T) {
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, a int, key(a))")

	sql := "select /*+ ignore_index(t, a) */ * from t where a = 1"
	tk.MustExec(sql)
	planDigest := tk.MustQuery(fmt.Sprintf("select plan_digest from information_schema.statements_summary where query_sample_text = '%s'", sql)).Rows()
	tk.MustExec(fmt.Sprintf("create session binding from history using plan digest '%s'", planDigest[0][0]))
	time.Sleep(time.Millisecond * 10)
	binding := tk.MustQuery("show bindings").Rows()
	loc, _ := time.LoadLocation("Asia/Shanghai")
	createTime, _ := time.ParseInLocation(time.DateTime, binding[0][4].(string), loc)
	updateTime, _ := time.ParseInLocation(time.DateTime, binding[0][5].(string), loc)

	// binding from history cover binding from history
	tk.MustExec(fmt.Sprintf("create session binding from history using plan digest '%s'", planDigest[0][0]))
	time.Sleep(time.Millisecond * 10)
	binding1 := tk.MustQuery("show bindings").Rows()
	createTime1, _ := time.ParseInLocation(time.DateTime, binding1[0][4].(string), loc)
	updateTime1, _ := time.ParseInLocation(time.DateTime, binding1[0][5].(string), loc)
	require.Greater(t, createTime1.UnixNano(), createTime.UnixNano())
	require.Greater(t, updateTime1.UnixNano(), updateTime.UnixNano())
	for i := range binding1[0] {
		if i != 4 && i != 5 {
			require.Equal(t, binding[0][i], binding1[0][i])
		}
	}
	// binding from sql cover binding from history
	tk.MustExec("create binding for select * from t where a = 1 using select /*+ ignore_index(t, a) */ * from t where a = 1")
	time.Sleep(time.Millisecond * 10)
	binding2 := tk.MustQuery("show bindings").Rows()
	createTime2, _ := time.ParseInLocation(time.DateTime, binding2[0][4].(string), loc)
	updateTime2, _ := time.ParseInLocation(time.DateTime, binding2[0][5].(string), loc)
	require.Greater(t, createTime2.UnixNano(), createTime1.UnixNano())
	require.Greater(t, updateTime2.UnixNano(), updateTime1.UnixNano())
	require.Equal(t, binding2[0][8], "manual")
	require.Equal(t, binding2[0][10], "")
	for i := range binding2[0] {
		if i != 1 && i != 4 && i != 5 && i != 8 && i != 10 {
			// bind_sql, create_time, update_time, source, plan_digest may be different
			require.Equal(t, binding1[0][i], binding2[0][i])
		}
	}
	// binding from history cover binding from sql
	tk.MustExec(fmt.Sprintf("create session binding from history using plan digest '%s'", planDigest[0][0]))
	time.Sleep(time.Millisecond * 10)
	binding3 := tk.MustQuery("show bindings").Rows()
	createTime3, _ := time.ParseInLocation(time.DateTime, binding3[0][4].(string), loc)
	updateTime3, _ := time.ParseInLocation(time.DateTime, binding3[0][5].(string), loc)
	require.Greater(t, createTime3.UnixNano(), createTime2.UnixNano())
	require.Greater(t, updateTime3.UnixNano(), updateTime2.UnixNano())
	require.Equal(t, binding3[0][8], "history")
	require.Equal(t, binding3[0][10], planDigest[0][0])
	for i := range binding3[0] {
		if i != 1 && i != 4 && i != 5 && i != 8 && i != 10 {
			// bind_sql, create_time, update_time, source, plan_digest may be different
			require.Equal(t, binding2[0][i], binding3[0][i])
		}
	}
}

func TestCreateBindingWithUsingKeyword(t *testing.T) {
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2")
	tk.MustExec("create table t(id int primary key, a int, key(a))")
	tk.MustExec("create table t1(id int primary key, a int, key(a))")
	tk.MustExec("create table t2(id int primary key, a int, key(a))")

	// `JOIN` keyword and not specifying the associated columns with the `USING` keyword.
	tk.MustGetErrMsg("CREATE GLOBAL BINDING for SELECT * FROM t t1 JOIN t t2 USING SELECT * FROM t t1 JOIN t t2;",
		"[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 67 near \"SELECT * FROM t t1 JOIN t t2;\" ")
	sql := "SELECT * FROM t t1 JOIN t t2;"
	tk.MustExec(sql)
	planDigest := tk.MustQuery(fmt.Sprintf("select plan_digest from information_schema.statements_summary where query_sample_text = '%s'", sql)).Rows()
	tk.MustExec(fmt.Sprintf("create session binding from history using plan digest '%s'", planDigest[0][0]))
	tk.MustExec(sql)
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))

	// `DELETE` statements that contain the `USING` keyword.
	tk.MustGetErrMsg("`CREATE GLOBAL BINDING for DELETE FROM t1 USING t1 JOIN t2 ON t1.a = t2.a USING DELETE FROM t1 USING t1 JOIN t2 ON t1.a = t2.a;",
		"[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 127 near \"`CREATE GLOBAL BINDING for DELETE FROM t1 USING t1 JOIN t2 ON t1.a = t2.a USING DELETE FROM t1 USING t1 JOIN t2 ON t1.a = t2.a;\" ")
	sql = "DELETE FROM t1 USING t1 JOIN t2 ON t1.a = t2.a;"
	tk.MustExec(sql)
	planDigest = tk.MustQuery(fmt.Sprintf("select plan_digest from information_schema.statements_summary where query_sample_text = '%s'", sql)).Rows()
	tk.MustExec(fmt.Sprintf("create session binding from history using plan digest '%s'", planDigest[0][0]))
	tk.MustExec(sql)
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
}

func TestNewCreatedBindingCanWorkWithPlanCache(t *testing.T) {
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2")
	tk.MustExec("create table t(id int primary key, a int, key(a))")

	tk.MustExec("prepare stmt from 'select * from t where a = ?'")
	tk.MustExec("set @a = 0")
	tk.MustExec("execute stmt using @a")
	tk.MustExec("execute stmt using @a")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	sql := "select /*+ ignore_index(t, a) */ * from t where a = 1"
	tk.MustExec(sql)
	planDigest := tk.MustQuery(fmt.Sprintf("select plan_digest from information_schema.statements_summary where query_sample_text = '%s'", sql)).Rows()
	tk.MustExec(fmt.Sprintf("create session binding from history using plan digest '%s'", planDigest[0][0]))
	tk.MustExec("execute stmt using @a")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
}

func TestCreateBindingForPrepareToken(t *testing.T) {
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b time, c varchar(5))")

	//some builtin functions listed in https://dev.mysql.com/doc/refman/8.0/en/function-resolution.html
	cases := []string{
		"select std(a) from t",
		"select cast(a as decimal(10, 2)) from t",
		"select bit_or(a) from t",
		"select min(a) from t",
		"select max(a) from t",
		"select substr(c, 1, 2) from t",
	}

	for _, sql := range cases {
		prep := fmt.Sprintf("prepare stmt from '%s'", sql)
		tk.MustExec(prep)
		tk.MustExec("execute stmt")
		planDigest := tk.MustQuery(fmt.Sprintf("select plan_digest from information_schema.statements_summary where query_sample_text = '%s'", sql)).Rows()
		tk.MustExec(fmt.Sprintf("create binding from history using plan digest '%s'", planDigest[0][0]))
	}
}

func testIndexUsageTable(t *testing.T, clusterTable bool) {
	var tk *testkit.TestKit
	var tableName string

	if clusterTable {
		s := new(clusterTablesSuite)
		s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
		s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
		s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
		s.startTime = time.Now()
		defer s.httpServer.Close()
		defer s.rpcserver.Stop()
		tk = s.newTestKitWithRoot(t)
		tableName = infoschema.ClusterTableTiDBIndexUsage
	} else {
		store := testkit.CreateMockStore(t)
		tk = testkit.NewTestKit(t, store)
		tableName = infoschema.TableTiDBIndexUsage
	}

	tk.MustExec("use test")
	tk.MustExec("create table t1(id1 int unique, id2 int unique)")
	tk.MustExec("create table t2(id1 int unique, id2 int unique)")

	for i := 0; i < 100; i++ {
		for j := 1; j <= 2; j++ {
			tk.MustExec(fmt.Sprintf("insert into t%d values (?, ?)", j), i, i)
		}
	}
	tk.MustExec("analyze table t1, t2")
	tk.RefreshSession()
	tk.MustExec("use test")
	// range scan 0-10 through t1 id1
	tk.MustQuery("select * from t1 use index(id1) where id1 >= 0 and id1 < 10")
	// range scan 10-30 through t1 id2
	tk.MustQuery("select * from t1 use index(id2) where id2 >= 10 and id2 < 30")
	// range scan 30-60 through t2 id1
	tk.MustQuery("select * from t2 use index(id1) where id1 >= 30 and id1 < 60")
	// range scan 60-100 through t2 id2
	tk.MustQuery("select * from t2 use index(id2) where id2 >= 60 and id2 < 100")
	tk.RefreshSession()

	require.Eventually(t, func() bool {
		result := tk.MustQuery(fmt.Sprintf(`select
			query_total,
			rows_access_total,
			percentage_access_0,
			percentage_access_0_1,
			percentage_access_1_10,
			percentage_access_10_20,
			percentage_access_20_50,
			percentage_access_50_100,
			percentage_access_100
		from information_schema.%s
		where table_schema='test' and
		      (table_name='t1' or table_name='t2') and
			(index_name='id1' or index_name='id2') and
			last_access_time is not null
		order by table_name, index_name;`, tableName))
		expectedResult := testkit.Rows(
			"1 10 0 0 0 1 0 0 0",
			"1 20 0 0 0 0 1 0 0",
			"1 30 0 0 0 0 1 0 0",
			"1 40 0 0 0 0 1 0 0")
		if !result.Equal(expectedResult) {
			logutil.BgLogger().Warn("result not equal", zap.Any("rows", result.Rows()))
			return false
		}
		return true
	}, time.Second*5, time.Millisecond*100)

	// use another less-privileged user to select
	tk.MustExec("create user test_user")
	tk.MustExec("grant all privileges on test.t1 to test_user")
	tk.RefreshSession()
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "test_user",
		Hostname: "127.0.0.1",
	}, nil, nil, nil))
	// `test_user` cannot see table `t2`.
	tk.MustQuery(fmt.Sprintf(`select
		query_total,
		rows_access_total,
		percentage_access_0,
		percentage_access_0_1,
		percentage_access_1_10,
		percentage_access_10_20,
		percentage_access_20_50,
		percentage_access_50_100,
		percentage_access_100
	from information_schema.%s
	where table_schema='test' and
		  (table_name='t1' or table_name='t2') and
		(index_name='id1' or index_name='id2') and
		last_access_time is not null
	order by table_name, index_name;`, tableName)).Check(testkit.Rows(
		"1 10 0 0 0 1 0 0 0",
		"1 20 0 0 0 0 1 0 0"))
}

func TestIndexUsageTable(t *testing.T) {
	testIndexUsageTable(t, false)
}

func TestClusterIndexUsageTable(t *testing.T) {
	testIndexUsageTable(t, true)
}

func TestUnusedIndexView(t *testing.T) {
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()
	tk := s.newTestKitWithRoot(t)

	tk.MustExec("use test")
	tk.MustExec("create table t(id1 int unique, id2 int unique)")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	tk.MustExec("analyze table t")
	tk.RefreshSession()
	tk.MustExec("use test")
	// range scan 0-10 through t1 id1
	tk.MustQuery("select * from t use index(id1) where id1 >= 0 and id1 < 10")
	tk.MustHavePlan("select * from t use index(id1) where id1 >= 0 and id1 < 10", "IndexLookUp")
	tk.RefreshSession()
	// the index `id2` is unused
	require.Eventually(t, func() bool {
		result := tk.MustQuery(`select * from sys.schema_unused_indexes where object_name = 't'`)
		logutil.BgLogger().Info("select schema_unused_indexes", zap.Any("row", result.Rows()))
		expectedResult := testkit.Rows("test t id2")
		return result.Equal(expectedResult)
	}, 5*time.Second, 100*time.Millisecond)
}

func TestMDLViewIDConflict(t *testing.T) {
	save := privileges.SkipWithGrant
	privileges.SkipWithGrant = true
	defer func() {
		privileges.SkipWithGrant = save
	}()

	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	tk := s.newTestKitWithRoot(t)

	tk.MustExec("use test")
	tk.MustExec("create table t(a int);")
	tbl, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tk.MustExec("insert into t values (1)")

	bigID := tbl.Meta().ID * 10
	bigTableName := ""
	// set a hard limitation on 10000 to avoid using too much resource
	for i := 0; i < 10000; i++ {
		bigTableName = fmt.Sprintf("t%d", i)
		tk.MustExec(fmt.Sprintf("create table %s(a int);", bigTableName))

		tbl, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr(bigTableName))
		require.NoError(t, err)

		require.LessOrEqual(t, tbl.Meta().ID, bigID)
		if tbl.Meta().ID == bigID {
			break
		}
	}
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec(fmt.Sprintf("insert into %s values (1)", bigTableName))

	// Now we have two table: t and `bigTableName`. The later one's ID is 10 times the former one.
	// Then create two session to run TXNs on these two tables
	txnTK1 := s.newTestKitWithRoot(t)
	txnTK2 := s.newTestKitWithRoot(t)
	txnTK1.MustExec("use test")
	txnTK1.MustExec("BEGIN")
	// this transaction will query `t` and one another table. Then the `related_table_ids` is `smallID|anotherID`
	txnTK1.MustQuery("SELECT * FROM t").Check(testkit.Rows("1"))
	txnTK1.MustQuery("SELECT * FROM t1").Check(testkit.Rows("1"))
	txnTK2.MustExec("use test")
	txnTK2.MustExec("BEGIN")
	txnTK2.MustQuery("SELECT * FROM " + bigTableName).Check(testkit.Rows("1"))

	testTK := s.newTestKitWithRoot(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", testTK.Session().GetSessionManager())
	defer s.rpcserver.Stop()
	testTK.MustQuery("select table_name from mysql.tidb_mdl_view").Check(testkit.Rows())

	// run a DDL on the table with smallID
	ddlTK1 := s.newTestKitWithRoot(t)
	ddlTK1.MustExec("use test")
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		ddlTK1.MustExec("ALTER TABLE t ADD COLUMN b INT;")
		wg.Done()
	}()
	ddlTK2 := s.newTestKitWithRoot(t)
	ddlTK2.MustExec("use test")
	wg.Add(1)
	go func() {
		ddlTK2.MustExec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN b INT;", bigTableName))
		wg.Done()
	}()

	require.Eventually(t, func() bool {
		rows := testTK.MustQuery("select table_ids from mysql.tidb_mdl_info").Rows()
		return len(rows) == 2
	}, time.Second*10, time.Second)

	// it only contains the table with smallID
	require.Eventually(t, func() bool {
		rows := testTK.MustQuery("select table_name, query, start_time from mysql.tidb_mdl_view order by table_name").Rows()
		return len(rows) == 2
	}, time.Second*10, time.Second)
	txnTK1.MustExec("COMMIT")
	txnTK2.MustExec("COMMIT")
	wg.Wait()
}
