// Copyright 2019 PingCAP, Inc.
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

package executor_test

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"math"
	"strconv"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/israce"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/testkit"
)

// mockSessionManager is a mocked session manager which is used for test.
type mockSessionManager1 struct {
	PS []*util.ProcessInfo
}

// ShowProcessList implements the SessionManager.ShowProcessList interface.
func (msm *mockSessionManager1) ShowProcessList() map[uint64]*util.ProcessInfo {
	ret := make(map[uint64]*util.ProcessInfo)
	for _, item := range msm.PS {
		ret[item.ID] = item
	}
	return ret
}

func (msm *mockSessionManager1) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	for _, item := range msm.PS {
		if item.ID == id {
			return item, true
		}
	}
	return &util.ProcessInfo{}, false
}

// Kill implements the SessionManager.Kill interface.
func (msm *mockSessionManager1) Kill(cid uint64, query bool) {

}

func (msm *mockSessionManager1) UpdateTLSConfig(cfg *tls.Config) {
}

func (s *testSuite9) TestExplainFor(c *C) {
	tkRoot := testkit.NewTestKitWithInit(c, s.store)
	tkUser := testkit.NewTestKitWithInit(c, s.store)
	tkRoot.MustExec("create table t1(c1 int, c2 int)")
	tkRoot.MustExec("create table t2(c1 int, c2 int)")
	tkRoot.MustExec("create user tu@'%'")
	tkRoot.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	tkUser.Se.Auth(&auth.UserIdentity{Username: "tu", Hostname: "localhost", CurrentUser: true, AuthUsername: "tu", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	tkRoot.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tkRoot.MustQuery("select * from t1;")
	tkRootProcess := tkRoot.Se.ShowProcess()
	ps := []*util.ProcessInfo{tkRootProcess}
	tkRoot.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	tkUser.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	tkRoot.MustQuery(fmt.Sprintf("explain for connection %d", tkRootProcess.ID)).Check(testkit.Rows(
		"TableReader_5 10000.00 root  data:TableFullScan_4",
		"└─TableFullScan_4 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
	))
	tkRoot.MustExec("set @@tidb_enable_collect_execution_info=1;")
	check := func() {
		tkRootProcess = tkRoot.Se.ShowProcess()
		ps = []*util.ProcessInfo{tkRootProcess}
		tkRoot.Se.SetSessionManager(&mockSessionManager1{PS: ps})
		tkUser.Se.SetSessionManager(&mockSessionManager1{PS: ps})
		rows := tkRoot.MustQuery(fmt.Sprintf("explain for connection %d", tkRootProcess.ID)).Rows()
		c.Assert(len(rows), Equals, 2)
		c.Assert(len(rows[0]), Equals, 9)
		buf := bytes.NewBuffer(nil)
		for i, row := range rows {
			if i > 0 {
				buf.WriteString("\n")
			}
			for j, v := range row {
				if j > 0 {
					buf.WriteString(" ")
				}
				buf.WriteString(fmt.Sprintf("%v", v))
			}
		}
		c.Assert(buf.String(), Matches, ""+
			"TableReader_5 10000.00 0 root  time:.*, loops:1, cop_task:.*num: 1, max:.*, proc_keys: 0, rpc_num: 1, rpc_time: .*data:TableFullScan_4 N/A N/A\n"+
			"└─TableFullScan_4 10000.00 0 cop.* table:t1 time:.*, loops:.*keep order:false, stats:pseudo N/A N/A")
	}
	tkRoot.MustQuery("select * from t1;")
	check()
	tkRoot.MustQuery("explain analyze select * from t1;")
	check()
	err := tkUser.ExecToErr(fmt.Sprintf("explain for connection %d", tkRootProcess.ID))
	c.Check(core.ErrAccessDenied.Equal(err), IsTrue)
	err = tkUser.ExecToErr("explain for connection 42")
	c.Check(core.ErrNoSuchThread.Equal(err), IsTrue)

	tkRootProcess.Plan = nil
	ps = []*util.ProcessInfo{tkRootProcess}
	tkRoot.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	tkRoot.MustExec(fmt.Sprintf("explain for connection %d", tkRootProcess.ID))
}

func (s *testSuite9) TestIssue11124(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("drop table if exists kankan1")
	tk.MustExec("drop table if exists kankan2")
	tk.MustExec("create table kankan1(id int, name text);")
	tk.MustExec("create table kankan2(id int, h1 text);")
	tk.MustExec("insert into kankan1 values(1, 'a'), (2, 'a');")
	tk.MustExec("insert into kankan2 values(2, 'z');")
	tk.MustQuery("select t1.id from kankan1 t1 left join kankan2 t2 on t1.id = t2.id where (case  when t1.name='b' then 'case2' when t1.name='a' then 'case1' else NULL end) = 'case1'")
	tkRootProcess := tk.Se.ShowProcess()
	ps := []*util.ProcessInfo{tkRootProcess}
	tk.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	tk2.Se.SetSessionManager(&mockSessionManager1{PS: ps})

	rs := tk.MustQuery("explain select t1.id from kankan1 t1 left join kankan2 t2 on t1.id = t2.id where (case  when t1.name='b' then 'case2' when t1.name='a' then 'case1' else NULL end) = 'case1'").Rows()
	rs2 := tk2.MustQuery(fmt.Sprintf("explain for connection %d", tkRootProcess.ID)).Rows()
	for i := range rs {
		c.Assert(rs[i], DeepEquals, rs2[i])
	}
}

func (s *testSuite) TestExplainMemTablePredicate(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustQuery("desc select * from METRICS_SCHEMA.tidb_query_duration where time >= '2019-12-23 16:10:13' and time <= '2019-12-23 16:30:13' ").Check(testkit.Rows(
		"MemTableScan_5 10000.00 root table:tidb_query_duration PromQL:histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance)), start_time:2019-12-23 16:10:13, end_time:2019-12-23 16:30:13, step:1m0s"))
	tk.MustQuery("desc select * from METRICS_SCHEMA.up where time >= '2019-12-23 16:10:13' and time <= '2019-12-23 16:30:13' ").Check(testkit.Rows(
		"MemTableScan_5 10000.00 root table:up PromQL:up{}, start_time:2019-12-23 16:10:13, end_time:2019-12-23 16:30:13, step:1m0s"))
	tk.MustQuery("desc select * from information_schema.cluster_log where time >= '2019-12-23 16:10:13' and time <= '2019-12-23 16:30:13'").Check(testkit.Rows(
		"MemTableScan_5 10000.00 root table:CLUSTER_LOG start_time:2019-12-23 16:10:13, end_time:2019-12-23 16:30:13"))
	tk.MustQuery("desc select * from information_schema.cluster_log where level in ('warn','error') and time >= '2019-12-23 16:10:13' and time <= '2019-12-23 16:30:13'").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:CLUSTER_LOG start_time:2019-12-23 16:10:13, end_time:2019-12-23 16:30:13, log_levels:["error","warn"]`))
	tk.MustQuery("desc select * from information_schema.cluster_log where type in ('high_cpu_1','high_memory_1') and time >= '2019-12-23 16:10:13' and time <= '2019-12-23 16:30:13'").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:CLUSTER_LOG start_time:2019-12-23 16:10:13, end_time:2019-12-23 16:30:13, node_types:["high_cpu_1","high_memory_1"]`))
	tk.MustQuery("desc select * from information_schema.slow_query").Check(testkit.Rows(
		"MemTableScan_4 10000.00 root table:SLOW_QUERY only search in the current 'tidb-slow.log' file"))
	tk.MustQuery("desc select * from information_schema.slow_query where time >= '2019-12-23 16:10:13' and time <= '2019-12-23 16:30:13'").Check(testkit.Rows(
		"MemTableScan_5 10000.00 root table:SLOW_QUERY start_time:2019-12-23 16:10:13.000000, end_time:2019-12-23 16:30:13.000000"))
	tk.MustExec("set @@time_zone = '+00:00';")
	tk.MustQuery("desc select * from information_schema.slow_query where time >= '2019-12-23 16:10:13' and time <= '2019-12-23 16:30:13'").Check(testkit.Rows(
		"MemTableScan_5 10000.00 root table:SLOW_QUERY start_time:2019-12-23 16:10:13.000000, end_time:2019-12-23 16:30:13.000000"))
}

type testPrepareSerialSuite struct {
	*baseTestSuite
}

func (s *testPrepareSerialSuite) TestExplainForConnPlanCache(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test")
	}
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	var err error
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.Se, err = session.CreateSession4TestWithOpt(s.store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)
	tk2 := testkit.NewTestKitWithInit(c, s.store)

	tk1.MustExec("use test")
	tk1.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk1.MustExec("drop table if exists t")
	tk1.MustExec("create table t(a int)")
	tk1.MustExec("prepare stmt from 'select * from t where a = ?'")
	tk1.MustExec("set @p0='1'")

	executeQuery := "execute stmt using @p0"
	explainQuery := "explain for connection " + strconv.FormatUint(tk1.Se.ShowProcess().ID, 10)
	explainResult := testkit.Rows(
		"TableReader_7 8000.00 root  data:Selection_6",
		"└─Selection_6 8000.00 cop[tikv]  eq(cast(test.t.a), 1)",
		"  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo",
	)

	// Now the ProcessInfo held by mockSessionManager1 will not be updated in real time.
	// So it needs to be reset every time before tk2 query.
	// TODO: replace mockSessionManager1 with another mockSessionManager.

	// single test
	tk1.MustExec(executeQuery)
	tk2.Se.SetSessionManager(&mockSessionManager1{
		PS: []*util.ProcessInfo{tk1.Se.ShowProcess()},
	})
	tk2.MustQuery(explainQuery).Check(explainResult)

	// multiple test, '1000' is both effective and efficient.
	repeats := 1000
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for i := 0; i < repeats; i++ {
			tk1.MustExec(executeQuery)
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < repeats; i++ {
			tk2.Se.SetSessionManager(&mockSessionManager1{
				PS: []*util.ProcessInfo{tk1.Se.ShowProcess()},
			})
			tk2.MustQuery(explainQuery).Check(explainResult)
		}
		wg.Done()
	}()

	wg.Wait()
}

func (s *testPrepareSerialSuite) TestSavedPlanPanicPlanCache(c *C) {
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	var err error
	tk := testkit.NewTestKit(c, s.store)
	tk.Se, err = session.CreateSession4TestWithOpt(s.store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int generated always as (a+b) stored)")
	tk.MustExec("insert into t(a,b) values(1,1)")
	tk.MustExec("begin")
	tk.MustExec("update t set b = 2 where a = 1")
	tk.MustExec("prepare stmt from 'select b from t where a > ?'")
	tk.MustExec("set @p = 0")
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows(
		"2",
	))
	tk.MustExec("set @p = 1")
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows())
	err = tk.ExecToErr("insert into t(a,b,c) values(3,3,3)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:3105]The value specified for generated column 'c' in table 't' is not allowed.")
}

func (s *testPrepareSerialSuite) TestExplainDotForExplainPlan(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	rows := tk.MustQuery("select connection_id()").Rows()
	c.Assert(len(rows), Equals, 1)
	connID := rows[0][0].(string)
	tk.MustQuery("explain select 1").Check(testkit.Rows(
		"Projection_3 1.00 root  1->Column#1",
		"└─TableDual_4 1.00 root  rows:1",
	))

	tkProcess := tk.Se.ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Se.SetSessionManager(&mockSessionManager1{PS: ps})

	tk.MustQuery(fmt.Sprintf("explain format=\"dot\" for connection %s", connID)).Check(nil)
}

func (s *testSuite) TestExplainTiFlashSystemTables(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tiflashInstance := "192.168.1.7:3930"
	database := "test"
	table := "t"
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TIFLASH_TABLES where TIFLASH_INSTANCE = '%s'", tiflashInstance)).Check(testkit.Rows(
		fmt.Sprintf("MemTableScan_5 10000.00 root table:TIFLASH_TABLES tiflash_instances:[\"%s\"]", tiflashInstance)))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TIFLASH_SEGMENTS where TIFLASH_INSTANCE = '%s'", tiflashInstance)).Check(testkit.Rows(
		fmt.Sprintf("MemTableScan_5 10000.00 root table:TIFLASH_SEGMENTS tiflash_instances:[\"%s\"]", tiflashInstance)))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TIFLASH_TABLES where TIDB_DATABASE = '%s'", database)).Check(testkit.Rows(
		fmt.Sprintf("MemTableScan_5 10000.00 root table:TIFLASH_TABLES tidb_databases:[\"%s\"]", database)))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TIFLASH_SEGMENTS where TIDB_DATABASE = '%s'", database)).Check(testkit.Rows(
		fmt.Sprintf("MemTableScan_5 10000.00 root table:TIFLASH_SEGMENTS tidb_databases:[\"%s\"]", database)))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TIFLASH_TABLES where TIDB_TABLE = '%s'", table)).Check(testkit.Rows(
		fmt.Sprintf("MemTableScan_5 10000.00 root table:TIFLASH_TABLES tidb_tables:[\"%s\"]", table)))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TIFLASH_SEGMENTS where TIDB_TABLE = '%s'", table)).Check(testkit.Rows(
		fmt.Sprintf("MemTableScan_5 10000.00 root table:TIFLASH_SEGMENTS tidb_tables:[\"%s\"]", table)))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TIFLASH_TABLES where TIFLASH_INSTANCE = '%s' and TIDB_DATABASE = '%s' and TIDB_TABLE = '%s'", tiflashInstance, database, table)).Check(testkit.Rows(
		fmt.Sprintf("MemTableScan_5 10000.00 root table:TIFLASH_TABLES tiflash_instances:[\"%s\"], tidb_databases:[\"%s\"], tidb_tables:[\"%s\"]", tiflashInstance, database, table)))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TIFLASH_SEGMENTS where TIFLASH_INSTANCE = '%s' and TIDB_DATABASE = '%s' and TIDB_TABLE = '%s'", tiflashInstance, database, table)).Check(testkit.Rows(
		fmt.Sprintf("MemTableScan_5 10000.00 root table:TIFLASH_SEGMENTS tiflash_instances:[\"%s\"], tidb_databases:[\"%s\"], tidb_tables:[\"%s\"]", tiflashInstance, database, table)))
}

func (s *testSuite) TestExplainTableStorage(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'information_schema'")).Check(testkit.Rows(
		fmt.Sprintf("MemTableScan_5 10000.00 root table:TABLE_STORAGE_STATS schema:[\"information_schema\"]")))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TABLE_STORAGE_STATS where TABLE_NAME = 'schemata'")).Check(testkit.Rows(
		fmt.Sprintf("MemTableScan_5 10000.00 root table:TABLE_STORAGE_STATS table:[\"schemata\"]")))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'information_schema' and TABLE_NAME = 'schemata'")).Check(testkit.Rows(
		fmt.Sprintf("MemTableScan_5 10000.00 root table:TABLE_STORAGE_STATS schema:[\"information_schema\"], table:[\"schemata\"]")))
}

func (s *testPrepareSerialSuite) TestPointGetUserVarPlanCache(c *C) {
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("CREATE TABLE t1 (a BIGINT, b VARCHAR(40), PRIMARY KEY (a, b))")
	tk.MustExec("INSERT INTO t1 VALUES (1,'3'),(2,'4')")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("CREATE TABLE t2 (a BIGINT, b VARCHAR(40), UNIQUE KEY idx_a (a))")
	tk.MustExec("INSERT INTO t2 VALUES (1,'1'),(2,'2')")
	tk.MustExec("prepare stmt from 'select * from t1, t2 where t1.a = t2.a and t2.a = ?'")
	tk.MustExec("set @a=1")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows(
		"1 3 1 1",
	))
	tkProcess := tk.Se.ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	// t2 should use PointGet.
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Check(testkit.Rows(
		"Projection_7 1.00 root  test.t1.a, test.t1.b, test.t2.a, test.t2.b",
		"└─MergeJoin_8 1.00 root  inner join, left key:test.t2.a, right key:test.t1.a",
		"  ├─IndexReader_27(Build) 10.00 root  index:IndexRangeScan_26",
		"  │ └─IndexRangeScan_26 10.00 cop[tikv] table:t1, index:PRIMARY(a, b) range:[1,1], keep order:true, stats:pseudo",
		"  └─Selection_25(Probe) 0.80 root  not(isnull(test.t2.a))",
		"    └─Point_Get_24 1.00 root table:t2, index:idx_a(a) ",
	))
	tk.MustExec("set @a=2")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows(
		"2 4 2 2",
	))
	tkProcess = tk.Se.ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	// t2 should use PointGet, range is changed to [2,2].
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Check(testkit.Rows(
		"Projection_7 1.00 root  test.t1.a, test.t1.b, test.t2.a, test.t2.b",
		"└─MergeJoin_8 1.00 root  inner join, left key:test.t2.a, right key:test.t1.a",
		"  ├─IndexReader_27(Build) 10.00 root  index:IndexRangeScan_26",
		"  │ └─IndexRangeScan_26 10.00 cop[tikv] table:t1, index:PRIMARY(a, b) range:[2,2], keep order:true, stats:pseudo",
		"  └─Selection_25(Probe) 1.00 root  not(isnull(test.t2.a))",
		"    └─Point_Get_24 1.00 root table:t2, index:idx_a(a) ",
	))
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows(
		"2 4 2 2",
	))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(
		"1",
	))
}
