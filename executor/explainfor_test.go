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

func (msm *mockSessionManager1) KillAllConnections() {
}

func (msm *mockSessionManager1) UpdateTLSConfig(cfg *tls.Config) {
}

func (msm *mockSessionManager1) ServerID() uint64 {
	return 1
}

func (s *testSuite) TestExplainFor(c *C) {
	tkRoot := testkit.NewTestKitWithInit(c, s.store)
	tkUser := testkit.NewTestKitWithInit(c, s.store)
	tkRoot.MustExec("create table t1(c1 int, c2 int)")
	tkRoot.MustExec("create table t2(c1 int, c2 int)")
	tkRoot.MustExec("create user tu@'%'")
	tkRoot.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	tkUser.Se.Auth(&auth.UserIdentity{Username: "tu", Hostname: "localhost", CurrentUser: true, AuthUsername: "tu", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	tkRoot.MustQuery("select * from t1;")
	tkRootProcess := tkRoot.Se.ShowProcess()
	ps := []*util.ProcessInfo{tkRootProcess}
	tkRoot.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	tkUser.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	tkRoot.MustQuery(fmt.Sprintf("explain for connection %d", tkRootProcess.ID)).Check(testkit.Rows(
		"TableReader_5 10000.00 root  data:TableFullScan_4",
		"└─TableFullScan_4 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
	))
	err := tkUser.ExecToErr(fmt.Sprintf("explain for connection %d", tkRootProcess.ID))
	c.Check(core.ErrAccessDenied.Equal(err), IsTrue)
	err = tkUser.ExecToErr("explain for connection 42")
	c.Check(core.ErrNoSuchThread.Equal(err), IsTrue)

	tkRootProcess.Plan = nil
	ps = []*util.ProcessInfo{tkRootProcess}
	tkRoot.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	tkRoot.MustExec(fmt.Sprintf("explain for connection %d", tkRootProcess.ID))
}

func (s *testSuite) TestIssue11124(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
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

func (s *testSuite) TestExplainClusterTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.cluster_config where type in ('tikv', 'tidb')")).Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:CLUSTER_CONFIG node_types:["tidb","tikv"]`))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.cluster_config where instance='192.168.1.7:2379'")).Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:CLUSTER_CONFIG instances:["192.168.1.7:2379"]`))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.cluster_config where type='tidb' and instance='192.168.1.7:2379'")).Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:CLUSTER_CONFIG node_types:["tidb"], instances:["192.168.1.7:2379"]`))
}

func (s *testSuite) TestInspectionResultTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustQuery("desc select * from information_schema.inspection_result where rule = 'ddl' and rule = 'config'").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:INSPECTION_RESULT skip_inspection:true`))
	tk.MustQuery("desc select * from information_schema.inspection_result where rule in ('ddl', 'config')").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:INSPECTION_RESULT rules:["config","ddl"], items:[]`))
	tk.MustQuery("desc select * from information_schema.inspection_result where item in ('ddl.lease', 'raftstore.threadpool')").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:INSPECTION_RESULT rules:[], items:["ddl.lease","raftstore.threadpool"]`))
	tk.MustQuery("desc select * from information_schema.inspection_result where item in ('ddl.lease', 'raftstore.threadpool') and rule in ('ddl', 'config')").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:INSPECTION_RESULT rules:["config","ddl"], items:["ddl.lease","raftstore.threadpool"]`))
}

func (s *testSuite) TestInspectionRuleTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.inspection_rules where type='inspection'")).Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:INSPECTION_RULES node_types:["inspection"]`))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.inspection_rules where type='inspection' or type='summary'")).Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:INSPECTION_RULES node_types:["inspection","summary"]`))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.inspection_rules where type='inspection' and type='summary'")).Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:INSPECTION_RULES skip_request: true`))
}

type testPrepareSerialSuite struct {
	*baseTestSuite
}

func (s *testPrepareSerialSuite) TestExplainForConnPlanCache(c *C) {
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

func (s *testPrepareSerialSuite) TestExplainDotForQuery(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk2 := testkit.NewTestKit(c, s.store)

	rows := tk.MustQuery("select connection_id()").Rows()
	c.Assert(len(rows), Equals, 1)
	connID := rows[0][0].(string)
	tk.MustQuery("select 1")
	tkProcess := tk.Se.ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Se.SetSessionManager(&mockSessionManager1{PS: ps})

	expected := tk2.MustQuery("explain format=\"dot\" select 1").Rows()
	got := tk.MustQuery(fmt.Sprintf("explain format=\"dot\" for connection %s", connID)).Rows()
	for i := range got {
		c.Assert(got[i], DeepEquals, expected[i])
	}
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

func (s *testSuite) TestInspectionSummaryTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustQuery("desc select * from information_schema.inspection_summary where rule='ddl'").Check(testkit.Rows(
		`Selection_5 8000.00 root  eq(Column#1, "ddl")`,
		`└─MemTableScan_6 10000.00 root table:INSPECTION_SUMMARY rules:["ddl"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where 'ddl'=rule or rule='config'").Check(testkit.Rows(
		`Selection_5 8000.00 root  or(eq("ddl", Column#1), eq(Column#1, "config"))`,
		`└─MemTableScan_6 10000.00 root table:INSPECTION_SUMMARY rules:["config","ddl"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where 'ddl'=rule or rule='config' or rule='slow_query'").Check(testkit.Rows(
		`Selection_5 8000.00 root  or(eq("ddl", Column#1), or(eq(Column#1, "config"), eq(Column#1, "slow_query")))`,
		`└─MemTableScan_6 10000.00 root table:INSPECTION_SUMMARY rules:["config","ddl","slow_query"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where (rule='config' or rule='slow_query') and (metrics_name='metric_name3' or metrics_name='metric_name1')").Check(testkit.Rows(
		`Selection_5 8000.00 root  or(eq(Column#1, "config"), eq(Column#1, "slow_query")), or(eq(Column#3, "metric_name3"), eq(Column#3, "metric_name1"))`,
		`└─MemTableScan_6 10000.00 root table:INSPECTION_SUMMARY rules:["config","slow_query"], metric_names:["metric_name1","metric_name3"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where rule in ('ddl', 'slow_query')").Check(testkit.Rows(
		`Selection_5 8000.00 root  in(Column#1, "ddl", "slow_query")`,
		`└─MemTableScan_6 10000.00 root table:INSPECTION_SUMMARY rules:["ddl","slow_query"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where rule in ('ddl', 'slow_query') and metrics_name='metric_name1'").Check(testkit.Rows(
		`Selection_5 8000.00 root  eq(Column#3, "metric_name1"), in(Column#1, "ddl", "slow_query")`,
		`└─MemTableScan_6 10000.00 root table:INSPECTION_SUMMARY rules:["ddl","slow_query"], metric_names:["metric_name1"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where rule in ('ddl', 'slow_query') and metrics_name in ('metric_name1', 'metric_name2')").Check(testkit.Rows(
		`Selection_5 8000.00 root  in(Column#1, "ddl", "slow_query"), in(Column#3, "metric_name1", "metric_name2")`,
		`└─MemTableScan_6 10000.00 root table:INSPECTION_SUMMARY rules:["ddl","slow_query"], metric_names:["metric_name1","metric_name2"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where rule='ddl' and metrics_name in ('metric_name1', 'metric_name2')").Check(testkit.Rows(
		`Selection_5 8000.00 root  eq(Column#1, "ddl"), in(Column#3, "metric_name1", "metric_name2")`,
		`└─MemTableScan_6 10000.00 root table:INSPECTION_SUMMARY rules:["ddl"], metric_names:["metric_name1","metric_name2"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where rule='ddl' and metrics_name='metric_NAME3'").Check(testkit.Rows(
		`Selection_5 8000.00 root  eq(Column#1, "ddl"), eq(Column#3, "metric_NAME3")`,
		`└─MemTableScan_6 10000.00 root table:INSPECTION_SUMMARY rules:["ddl"], metric_names:["metric_name3"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where rule in ('ddl', 'config') and rule in ('slow_query', 'config')").Check(testkit.Rows(
		`Selection_5 8000.00 root  in(Column#1, "ddl", "config"), in(Column#1, "slow_query", "config")`,
		`└─MemTableScan_6 10000.00 root table:INSPECTION_SUMMARY rules:["config"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where metrics_name in ('metric_name1', 'metric_name4') and metrics_name in ('metric_name5', 'metric_name4') and rule in ('ddl', 'config') and rule in ('slow_query', 'config') and quantile in (0.80, 0.90)").Check(testkit.Rows(
		`Selection_5 8000.00 root  in(Column#1, "ddl", "config"), in(Column#1, "slow_query", "config"), in(Column#3, "metric_name1", "metric_name4"), in(Column#3, "metric_name5", "metric_name4")`,
		`└─MemTableScan_6 10000.00 root table:INSPECTION_SUMMARY rules:["config"], metric_names:["metric_name4"], quantiles:[0.800000,0.900000]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where metrics_name in ('metric_name1', 'metric_name4') and metrics_name in ('metric_name5', 'metric_name4') and metrics_name in ('metric_name5', 'metric_name1') and metrics_name in ('metric_name1', 'metric_name3')").Check(testkit.Rows(
		`Selection_5 8000.00 root  in(Column#3, "metric_name1", "metric_name3"), in(Column#3, "metric_name1", "metric_name4"), in(Column#3, "metric_name5", "metric_name1"), in(Column#3, "metric_name5", "metric_name4")`,
		`└─MemTableScan_6 10000.00 root table:INSPECTION_SUMMARY skip_inspection: true`,
	))
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
