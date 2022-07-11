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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/stretchr/testify/require"
)

func TestExplainFor(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tkRoot := testkit.NewTestKit(t, store)
	tkUser := testkit.NewTestKit(t, store)

	tkRoot.MustExec("use test")
	tkRoot.MustExec("drop table if exists t1, t2;")
	tkRoot.MustExec("create table t1(c1 int, c2 int)")
	tkRoot.MustExec("create table t2(c1 int, c2 int)")
	tkRoot.MustExec("create user tu@'%'")
	tkRoot.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	tkUser.Session().Auth(&auth.UserIdentity{Username: "tu", Hostname: "localhost", CurrentUser: true, AuthUsername: "tu", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	tkRoot.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tkRoot.MustQuery("select * from t1;")
	tkRootProcess := tkRoot.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkRootProcess}
	tkRoot.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tkUser.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tkRoot.MustQuery(fmt.Sprintf("explain for connection %d", tkRootProcess.ID)).Check(testkit.Rows(
		"TableReader_5 10000.00 root  data:TableFullScan_4",
		"└─TableFullScan_4 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
	))
	tkRoot.MustExec("set @@tidb_enable_collect_execution_info=1;")
	check := func() {
		tkRootProcess = tkRoot.Session().ShowProcess()
		ps = []*util.ProcessInfo{tkRootProcess}
		tkRoot.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
		tkUser.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
		rows := tkRoot.MustQuery(fmt.Sprintf("explain for connection %d", tkRootProcess.ID)).Rows()
		require.Len(t, rows, 2)
		require.Len(t, rows[0], 9)
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
		require.Regexp(t, "TableReader_5 10000.00 0 root  time:.*, loops:1, cop_task: {num:.*, max:.*, proc_keys:.* rpc_num: 1, rpc_time:.*} data:TableFullScan_4 N/A N/A\n"+
			"└─TableFullScan_4 10000.00 0 cop.* table:t1 tikv_task:{time:.*, loops:0} keep order:false, stats:pseudo N/A N/A",
			buf.String())
	}
	tkRoot.MustQuery("select * from t1;")
	check()
	tkRoot.MustQuery("explain analyze select * from t1;")
	check()
	err := tkUser.ExecToErr(fmt.Sprintf("explain for connection %d", tkRootProcess.ID))
	require.True(t, core.ErrAccessDenied.Equal(err))
	err = tkUser.ExecToErr("explain for connection 42")
	require.True(t, core.ErrNoSuchThread.Equal(err))

	tkRootProcess.Plan = nil
	ps = []*util.ProcessInfo{tkRootProcess}
	tkRoot.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tkRoot.MustExec(fmt.Sprintf("explain for connection %d", tkRootProcess.ID))
}

func TestExplainForVerbose(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(id int);")
	tk.MustQuery("select * from t1;")
	tkRootProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkRootProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk2.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	rs := tk.MustQuery("explain format = 'verbose' select * from t1").Rows()
	rs2 := tk2.MustQuery(fmt.Sprintf("explain format = 'verbose' for connection %d", tkRootProcess.ID)).Rows()
	require.Len(t, rs, len(rs2))
	for i := range rs {
		require.Equal(t, rs2[i], rs[i])
	}

	tk.MustExec("set @@tidb_enable_collect_execution_info=1;")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(id int);")
	tk.MustQuery("select * from t2;")
	tkRootProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkRootProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk2.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rs = tk.MustQuery("explain format = 'verbose' select * from t2").Rows()
	rs2 = tk2.MustQuery(fmt.Sprintf("explain format = 'verbose' for connection %d", tkRootProcess.ID)).Rows()
	require.Len(t, rs, len(rs2))
	for i := range rs {
		// "id", "estRows", "estCost", "task", "access object", "operator info"
		require.Len(t, rs[i], 6)
		// "id", "estRows", "estCost", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"
		require.Len(t, rs2[i], 10)
		for j := 0; j < 3; j++ {
			require.Equal(t, rs2[i][j], rs[i][j])
		}
	}
	tk.MustQuery("explain format = 'verbose' select * from t1").Rows()
	tk.MustQuery("explain format = 'VERBOSE' select * from t1").Rows()
	tk.MustQuery("explain analyze format = 'verbose' select * from t1").Rows()
	tk.MustQuery("explain analyze format = 'VERBOSE' select * from t1").Rows()
}

func TestIssue11124(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("drop table if exists kankan1")
	tk.MustExec("drop table if exists kankan2")
	tk.MustExec("create table kankan1(id int, name text);")
	tk.MustExec("create table kankan2(id int, h1 text);")
	tk.MustExec("insert into kankan1 values(1, 'a'), (2, 'a');")
	tk.MustExec("insert into kankan2 values(2, 'z');")
	tk.MustQuery("select t1.id from kankan1 t1 left join kankan2 t2 on t1.id = t2.id where (case  when t1.name='b' then 'case2' when t1.name='a' then 'case1' else NULL end) = 'case1'")
	tkRootProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkRootProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk2.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	rs := tk.MustQuery("explain select t1.id from kankan1 t1 left join kankan2 t2 on t1.id = t2.id where (case  when t1.name='b' then 'case2' when t1.name='a' then 'case1' else NULL end) = 'case1'").Rows()
	rs2 := tk2.MustQuery(fmt.Sprintf("explain for connection %d", tkRootProcess.ID)).Rows()
	for i := range rs {
		require.Equal(t, rs2[i], rs[i])
	}
}

func TestExplainMemTablePredicate(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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

func TestExplainClusterTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("desc select * from information_schema.cluster_config where type in ('tikv', 'tidb')").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:CLUSTER_CONFIG node_types:["tidb","tikv"]`))
	tk.MustQuery("desc select * from information_schema.cluster_config where instance='192.168.1.7:2379'").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:CLUSTER_CONFIG instances:["192.168.1.7:2379"]`))
	tk.MustQuery("desc select * from information_schema.cluster_config where type='tidb' and instance='192.168.1.7:2379'").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:CLUSTER_CONFIG node_types:["tidb"], instances:["192.168.1.7:2379"]`))
}

func TestInspectionResultTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("desc select * from information_schema.inspection_result where rule = 'ddl' and rule = 'config'").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:INSPECTION_RESULT skip_inspection:true`))
	tk.MustQuery("desc select * from information_schema.inspection_result where rule in ('ddl', 'config')").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:INSPECTION_RESULT rules:["config","ddl"], items:[]`))
	tk.MustQuery("desc select * from information_schema.inspection_result where item in ('ddl.lease', 'raftstore.threadpool')").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:INSPECTION_RESULT rules:[], items:["ddl.lease","raftstore.threadpool"]`))
	tk.MustQuery("desc select * from information_schema.inspection_result where item in ('ddl.lease', 'raftstore.threadpool') and rule in ('ddl', 'config')").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:INSPECTION_RESULT rules:["config","ddl"], items:["ddl.lease","raftstore.threadpool"]`))
}

func TestInspectionRuleTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("desc select * from information_schema.inspection_rules where type='inspection'").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:INSPECTION_RULES node_types:["inspection"]`))
	tk.MustQuery("desc select * from information_schema.inspection_rules where type='inspection' or type='summary'").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:INSPECTION_RULES node_types:["inspection","summary"]`))
	tk.MustQuery("desc select * from information_schema.inspection_rules where type='inspection' and type='summary'").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:INSPECTION_RULES skip_request: true`))
}

func TestExplainForConnPlanCache(t *testing.T) {
	t.Skip("unstable")

	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk1.SetSession(se)

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	tk1.MustExec("use test")
	tk1.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk1.MustExec("drop table if exists t")
	tk1.MustExec("create table t(a int)")
	tk1.MustExec("prepare stmt from 'select * from t where a = ?'")
	tk1.MustExec("set @p0='1'")

	executeQuery := "execute stmt using @p0"
	explainQuery := "explain for connection " + strconv.FormatUint(tk1.Session().ShowProcess().ID, 10)

	explainResult := testkit.Rows(
		"TableReader_7 10.00 root  data:Selection_6",
		"└─Selection_6 10.00 cop[tikv]  eq(test.t.a, 1)",
		"  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo",
	)

	// Now the ProcessInfo held by mockSessionManager1 will not be updated in real time.
	// So it needs to be reset every time before tk2 query.
	// TODO: replace mockSessionManager1 with another mockSessionManager.

	// single test
	tk1.MustExec(executeQuery)
	tk2.Session().SetSessionManager(&testkit.MockSessionManager{
		PS: []*util.ProcessInfo{tk1.Session().ShowProcess()},
	})
	tk2.MustQuery(explainQuery).Check(explainResult)
	tk1.MustExec(executeQuery)
	// The plan can not be cached because the string type parameter will be convert to int type for calculation.
	tk1.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	// multiple test, '1000' is both effective and efficient.
	repeats := 1000
	var wg util.WaitGroupWrapper

	wg.Run(func() {
		for i := 0; i < repeats; i++ {
			tk1.MustExec(executeQuery)
		}
	})

	wg.Run(func() {
		for i := 0; i < repeats; i++ {
			tk2.Session().SetSessionManager(&testkit.MockSessionManager{
				PS: []*util.ProcessInfo{tk1.Session().ShowProcess()},
			})
			tk2.MustQuery(explainQuery).Check(explainResult)
		}
	})

	wg.Wait()
}

func TestSavedPlanPanicPlanCache(t *testing.T) {
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

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
	require.EqualError(t, err, "[planner:3105]The value specified for generated column 'c' in table 't' is not allowed.")
}

func TestExplainDotForExplainPlan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	rows := tk.MustQuery("select connection_id()").Rows()
	require.Len(t, rows, 1)
	connID := rows[0][0].(string)
	tk.MustQuery("explain format = 'brief' select 1").Check(testkit.Rows(
		"Projection 1.00 root  1->Column#1",
		"└─TableDual 1.00 root  rows:1",
	))

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	tk.MustQuery(fmt.Sprintf("explain format=\"dot\" for connection %s", connID)).Check(nil)
}

func TestExplainDotForQuery(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	rows := tk.MustQuery("select connection_id()").Rows()
	require.Len(t, rows, 1)
	connID := rows[0][0].(string)
	tk.MustQuery("select 1")
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	expected := tk2.MustQuery("explain format=\"dot\" select 1").Rows()
	got := tk.MustQuery(fmt.Sprintf("explain format=\"dot\" for connection %s", connID)).Rows()
	for i := range got {
		require.Equal(t, expected[i], got[i])
	}
}

func TestExplainTableStorage(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("desc select * from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'information_schema'").Check(testkit.Rows(
		"MemTableScan_5 10000.00 root table:TABLE_STORAGE_STATS schema:[\"information_schema\"]"))
	tk.MustQuery("desc select * from information_schema.TABLE_STORAGE_STATS where TABLE_NAME = 'schemata'").Check(testkit.Rows(
		"MemTableScan_5 10000.00 root table:TABLE_STORAGE_STATS table:[\"schemata\"]"))
	tk.MustQuery("desc select * from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'information_schema' and TABLE_NAME = 'schemata'").Check(testkit.Rows(
		"MemTableScan_5 10000.00 root table:TABLE_STORAGE_STATS schema:[\"information_schema\"], table:[\"schemata\"]"))
}

func TestInspectionSummaryTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

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

func TestExplainTiFlashSystemTables(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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

func TestPointGetUserVarPlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tmp := testkit.NewTestKit(t, store)
	defer tmp.MustExec("set global tidb_enable_prepared_plan_cache=" + variable.BoolToOnOff(variable.EnablePreparedPlanCache.Load()))
	tmp.MustExec("set global tidb_enable_prepared_plan_cache=ON")
	tk := testkit.NewTestKit(t, store)
	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
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
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Check(testkit.Rows( // can use idx_a
		`Projection_9 1.00 root  test.t1.a, test.t1.b, test.t2.a, test.t2.b`,
		`└─IndexJoin_17 1.00 root  inner join, inner:TableReader_13, outer key:test.t2.a, inner key:test.t1.a, equal cond:eq(test.t2.a, test.t1.a)`,
		`  ├─Selection_44(Build) 0.80 root  not(isnull(test.t2.a))`,
		`  │ └─Point_Get_43 1.00 root table:t2, index:idx_a(a) `,
		`  └─TableReader_13(Probe) 0.00 root  data:Selection_12`,
		`    └─Selection_12 0.00 cop[tikv]  eq(test.t1.a, 1)`,
		`      └─TableRangeScan_11 1.00 cop[tikv] table:t1 range: decided by [test.t2.a], keep order:false, stats:pseudo`))

	tk.MustExec("set @a=2")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows(
		"2 4 2 2",
	))
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Check(testkit.Rows( // can use idx_a
		`Projection_9 1.00 root  test.t1.a, test.t1.b, test.t2.a, test.t2.b`,
		`└─IndexJoin_17 1.00 root  inner join, inner:TableReader_13, outer key:test.t2.a, inner key:test.t1.a, equal cond:eq(test.t2.a, test.t1.a)`,
		`  ├─Selection_44(Build) 0.80 root  not(isnull(test.t2.a))`,
		`  │ └─Point_Get_43 1.00 root table:t2, index:idx_a(a) `,
		`  └─TableReader_13(Probe) 0.00 root  data:Selection_12`,
		`    └─Selection_12 0.00 cop[tikv]  eq(test.t1.a, 2)`,
		`      └─TableRangeScan_11 1.00 cop[tikv] table:t1 range: decided by [test.t2.a], keep order:false, stats:pseudo`))
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows(
		"2 4 2 2",
	))
}

func TestExpressionIndexPreparePlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, key ((a+b)));")
	tk.MustExec("prepare stmt from 'select * from t where a+b = ?'")
	tk.MustExec("set @a = 123")
	tk.MustExec("execute stmt using @a")

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res := tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 4)
	require.Regexp(t, ".*expression_index.*", res.Rows()[2][3])
	require.Regexp(t, ".*[123,123].*", res.Rows()[2][4])

	tk.MustExec("set @a = 1234")
	tk.MustExec("execute stmt using @a")
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 4)
	require.Regexp(t, ".*expression_index.*", res.Rows()[2][3])
	require.Regexp(t, ".*[1234,1234].*", res.Rows()[2][4])
}

func TestIssue28259(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	// test for indexRange
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("drop table if exists UK_GCOL_VIRTUAL_18588;")
	tk.MustExec("CREATE TABLE `UK_GCOL_VIRTUAL_18588` (`COL1` bigint(20), UNIQUE KEY `UK_COL1` (`COL1`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into UK_GCOL_VIRTUAL_18588 values('8502658334322817163');")
	tk.MustExec(`prepare stmt from 'select col1 from UK_GCOL_VIRTUAL_18588 where col1 between ? and ? or col1 < ?';`)
	tk.MustExec("set @a=5516958330762833919, @b=8551969118506051323, @c=2887622822023883594;")
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows("8502658334322817163"))

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res := tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 3)
	require.Regexp(t, ".*Selection.*", res.Rows()[0][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[2][0])

	tk.MustExec("set @a=-1696020282760139948, @b=-2619168038882941276, @c=-4004648990067362699;")
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 4)
	require.Regexp(t, ".*Selection.*", res.Rows()[0][0])
	require.Regexp(t, ".*IndexFullScan.*", res.Rows()[3][0])

	res = tk.MustQuery("explain format = 'brief' select col1 from UK_GCOL_VIRTUAL_18588 use index(UK_COL1) " +
		"where col1 between -1696020282760139948 and -2619168038882941276 or col1 < -4004648990067362699;")
	require.Len(t, res.Rows(), 3)
	require.Regexp(t, ".*Selection.*", res.Rows()[1][0])
	require.Regexp(t, ".*IndexFullScan.*", res.Rows()[2][0])
	res = tk.MustQuery("explain format = 'brief' select col1 from UK_GCOL_VIRTUAL_18588 use index(UK_COL1) " +
		"where col1 between 5516958330762833919 and 8551969118506051323 or col1 < 2887622822023883594;")
	require.Len(t, res.Rows(), 2)
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[1][0])

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a int, b int, index idx(a, b));")
	tk.MustExec("insert into t values(1, 0);")
	tk.MustExec(`prepare stmt from 'select a from t where (a between ? and ? or a < ?) and b < 1;'`)
	tk.MustExec("set @a=0, @b=2, @c=2;")
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows("1"))
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 5)
	require.Regexp(t, ".*Selection.*", res.Rows()[1][0])
	require.Equal(t, "lt(test.t.b, 1), or(and(ge(test.t.a, 0), le(test.t.a, 2)), lt(test.t.a, 2))", res.Rows()[1][4])
	require.Regexp(t, ".*IndexReader.*", res.Rows()[2][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[4][0])

	tk.MustExec("set @a=2, @b=1, @c=1;")
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 5)
	require.Regexp(t, ".*Selection.*", res.Rows()[1][0])
	require.Equal(t, "lt(test.t.b, 1), or(and(ge(test.t.a, 2), le(test.t.a, 1)), lt(test.t.a, 1))", res.Rows()[1][4])
	require.Regexp(t, ".*IndexReader.*", res.Rows()[2][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[4][0])

	res = tk.MustQuery("explain format = 'brief' select a from t use index(idx) " +
		"where (a between 0 and 2 or a < 2) and b < 1;")
	require.Len(t, res.Rows(), 4)
	require.Regexp(t, ".*IndexReader.*", res.Rows()[1][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[2][0])
	require.Equal(t, "lt(test.t.b, 1)", res.Rows()[2][4])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[3][0])
	res = tk.MustQuery("explain format = 'brief' select a from t use index(idx) " +
		"where (a between 2 and 1 or a < 1) and b < 1;")
	require.Len(t, res.Rows(), 4)
	require.Regexp(t, ".*IndexReader.*", res.Rows()[1][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[2][0])
	require.Equal(t, "lt(test.t.b, 1)", res.Rows()[2][4])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[3][0])

	// test for indexLookUp
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a int, b int, index idx(a));")
	tk.MustExec("insert into t values(1, 0);")
	tk.MustExec(`prepare stmt from 'select /*+ USE_INDEX(t, idx) */ a from t where (a between ? and ? or a < ?) and b < 1;'`)
	tk.MustExec("set @a=0, @b=2, @c=2;")
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows("1"))
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 6)
	require.Regexp(t, ".*Selection.*", res.Rows()[1][0])
	require.Regexp(t, ".*IndexLookUp.*", res.Rows()[2][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[3][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[4][0])
	require.Regexp(t, ".*TableRowIDScan.*", res.Rows()[5][0])

	tk.MustExec("set @a=2, @b=1, @c=1;")
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 6)
	require.Regexp(t, ".*Selection.*", res.Rows()[1][0])
	require.Regexp(t, ".*IndexLookUp.*", res.Rows()[2][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[3][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[4][0])
	require.Regexp(t, ".*TableRowIDScan.*", res.Rows()[5][0])

	res = tk.MustQuery("explain format = 'brief' select /*+ USE_INDEX(t, idx) */ a from t use index(idx) " +
		"where (a between 0 and 2 or a < 2) and b < 1;")
	require.Len(t, res.Rows(), 5)
	require.Regexp(t, ".*IndexLookUp.*", res.Rows()[1][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[2][0])
	res = tk.MustQuery("explain format = 'brief' select /*+ USE_INDEX(t, idx) */ a from t use index(idx) " +
		"where (a between 2 and 1 or a < 1) and b < 1;")
	require.Len(t, res.Rows(), 5)
	require.Regexp(t, ".*IndexLookUp.*", res.Rows()[1][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[2][0])

	// test for tableReader
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a int PRIMARY KEY CLUSTERED, b int);")
	tk.MustExec("insert into t values(1, 0);")
	tk.MustExec(`prepare stmt from 'select a from t where (a between ? and ? or a < ?) and b < 1;'`)
	tk.MustExec("set @a=0, @b=2, @c=2;")
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows("1"))
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 5)
	require.Regexp(t, ".*Selection.*", res.Rows()[1][0])
	require.Equal(t, "lt(test.t.b, 1), or(and(ge(test.t.a, 0), le(test.t.a, 2)), lt(test.t.a, 2))", res.Rows()[1][4])
	require.Regexp(t, ".*TableReader.*", res.Rows()[2][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[3][0])
	require.Regexp(t, ".*TableRangeScan.*", res.Rows()[4][0])

	tk.MustExec("set @a=2, @b=1, @c=1;")
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 5)
	require.Regexp(t, ".*Selection.*", res.Rows()[1][0])
	require.Equal(t, "lt(test.t.b, 1), or(and(ge(test.t.a, 2), le(test.t.a, 1)), lt(test.t.a, 1))", res.Rows()[1][4])
	require.Regexp(t, ".*TableReader.*", res.Rows()[2][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[3][0])
	require.Regexp(t, ".*TableRangeScan.*", res.Rows()[4][0])

	res = tk.MustQuery("explain format = 'brief' select a from t " +
		"where (a between 0 and 2 or a < 2) and b < 1;")
	require.Len(t, res.Rows(), 4)
	require.Regexp(t, ".*TableReader.*", res.Rows()[1][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[2][0])
	require.Equal(t, "lt(test.t.b, 1)", res.Rows()[2][4])
	require.Regexp(t, ".*TableRangeScan.*", res.Rows()[3][0])
	res = tk.MustQuery("explain format = 'brief' select a from t " +
		"where (a between 2 and 1 or a < 1) and b < 1;")
	require.Len(t, res.Rows(), 4)
	require.Regexp(t, ".*TableReader.*", res.Rows()[1][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[2][0])
	require.Equal(t, "lt(test.t.b, 1)", res.Rows()[2][4])
	require.Regexp(t, ".*TableRangeScan.*", res.Rows()[3][0])

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (a int primary key, b int, c int, d int);")
	tk.MustExec(`prepare stmt from 'select * from t where ((a > ? and a < 5 and b > 2) or (a > ? and a < 10 and c > 3)) and d = 5;';`)
	tk.MustExec("set @a=1, @b=8;")
	tk.MustQuery("execute stmt using @a,@b;").Check(testkit.Rows())
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 4)
	require.Regexp(t, ".*Selection.*", res.Rows()[0][0])
	require.Regexp(t, ".*TableReader.*", res.Rows()[1][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[2][0])
	require.Regexp(t, ".*TableRangeScan.*", res.Rows()[3][0])
}

func TestIssue28696(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(a int primary key, b varchar(255), c int);")
	tk.MustExec("create unique index b on t1(b(3));")
	tk.MustExec("insert into t1 values(1,'abcdfsafd',1),(2,'addfdsafd',2),(3,'ddcdsaf',3),(4,'bbcsa',4);")
	tk.MustExec(`prepare stmt from "select a from t1 where b = ?";`)
	tk.MustExec("set @a='bbcsa';")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("4"))

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res := tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 6)
	require.Regexp(t, ".*Selection.*", res.Rows()[1][0])
	require.Regexp(t, ".*IndexLookUp.*", res.Rows()[2][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[3][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[4][0])
	require.Regexp(t, ".*TableRowIDScan.*", res.Rows()[5][0])

	res = tk.MustQuery("explain format = 'brief' select a from t1 where b = 'bbcsa';")
	require.Len(t, res.Rows(), 5)
	require.Regexp(t, ".*IndexLookUp.*", res.Rows()[1][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[2][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[3][0])
	require.Regexp(t, ".*TableRowIDScan.*", res.Rows()[4][0])
}

func TestIndexMerge4PlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("drop table if exists IDT_MULTI15858STROBJSTROBJ;")
	tk.MustExec("CREATE TABLE `IDT_MULTI15858STROBJSTROBJ` (" +
		"`COL1` enum('aa','bb','cc','dd','ee','ff','gg','hh','ii','mm') DEFAULT NULL," +
		"`COL2` int(41) DEFAULT NULL," +
		"`COL3` datetime DEFAULT NULL," +
		"KEY `U_M_COL4` (`COL1`,`COL2`)," +
		"KEY `U_M_COL5` (`COL3`,`COL2`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into IDT_MULTI15858STROBJSTROBJ values('aa', 1333053589,'1037-12-26 01:38:52');")

	tk.MustExec("set tidb_enable_index_merge=on;")
	tk.MustExec("prepare stmt from 'select * from IDT_MULTI15858STROBJSTROBJ where col2 <> ? and col1 not in (?, ?, ?) or col3 = ? order by 2;';")
	tk.MustExec("set @a=2134549621, @b='aa', @c='aa', @d='aa', @e='9941-07-07 01:08:48';")
	tk.MustQuery("execute stmt using @a,@b,@c,@d,@e;").Check(testkit.Rows())

	tk.MustExec("set @a=-2144294194, @b='mm', @c='mm', @d='mm', @e='0198-09-29 20:19:49';")
	tk.MustQuery("execute stmt using @a,@b,@c,@d,@e;").Check(testkit.Rows("aa 1333053589 1037-12-26 01:38:52"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @a,@b,@c,@d,@e;").Check(testkit.Rows("aa 1333053589 1037-12-26 01:38:52"))

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res := tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 7)
	require.Regexp(t, ".*Selection.*", res.Rows()[1][0])
	require.Regexp(t, ".*IndexMerge.*", res.Rows()[2][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[4][0])
	require.Equal(t, "range:(NULL,\"mm\"), (\"mm\",+inf], keep order:false, stats:pseudo", res.Rows()[4][4])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[5][0])
	require.Equal(t, "range:[0198-09-29 20:19:49,0198-09-29 20:19:49], keep order:false, stats:pseudo", res.Rows()[5][4])

	// test for cluster index in indexMerge
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@tidb_enable_clustered_index = 1;")
	tk.MustExec("create table t(a int, b int, c int, primary key(a), index idx_b(b));")
	tk.MustExec("prepare stmt from 'select * from t where ((a > ? and a < ?) or b > 1) and c > 1;';")
	tk.MustExec("set @a = 0, @b = 3;")
	tk.MustQuery("execute stmt using @a, @b;").Check(testkit.Rows())

	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 6)
	require.Regexp(t, ".*Selection.*", res.Rows()[0][0])
	require.Regexp(t, ".*IndexMerge.*", res.Rows()[1][0])
	require.Regexp(t, ".*TableRangeScan.*", res.Rows()[2][0])
	require.Equal(t, "range:(0,3), keep order:false, stats:pseudo", res.Rows()[2][4])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[3][0])
	require.Equal(t, "range:(1,+inf], keep order:false, stats:pseudo", res.Rows()[3][4])

	// test for prefix index
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(a int primary key, b varchar(255), c int, index idx_c(c));")
	tk.MustExec("create unique index idx_b on t1(b(3));")
	tk.MustExec("insert into t1 values(1,'abcdfsafd',1),(2,'addfdsafd',2),(3,'ddcdsaf',3),(4,'bbcsa',4);")
	tk.MustExec("prepare stmt from 'select /*+ USE_INDEX_MERGE(t1, primary, idx_b, idx_c) */ * from t1 where b = ? or a > 10 or c > 10;';")
	tk.MustExec("set @a='bbcsa', @b='ddcdsaf';")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("4 bbcsa 4"))

	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Regexp(t, ".*IndexMerge.*", res.Rows()[1][0])

	tk.MustQuery("execute stmt using @b;").Check(testkit.Rows("3 ddcdsaf 3"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @b;").Check(testkit.Rows("3 ddcdsaf 3"))
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Regexp(t, ".*IndexMerge.*", res.Rows()[1][0])

	// rewrite the origin indexMerge test
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, primary key(a), key(b))")
	tk.MustExec("prepare stmt from 'select /*+ inl_join(t2) */ * from t t1 join t t2 on t1.a = t2.a and t1.c = t2.c where t2.a = 1 or t2.b = 1;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows())

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int primary key, b int, c int, key(b), key(c));")
	tk.MustExec("INSERT INTO t1 VALUES (10, 10, 10), (11, 11, 11)")
	tk.MustExec("prepare stmt from 'select /*+ use_index_merge(t1) */ * from t1 where c=? or (b=? and a=?);';")
	tk.MustExec("set @a = 10, @b = 11;")
	tk.MustQuery("execute stmt using @a, @a, @a").Check(testkit.Rows("10 10 10"))
	tk.MustQuery("execute stmt using @b, @b, @b").Check(testkit.Rows("11 11 11"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustExec("prepare stmt from 'select /*+ use_index_merge(t1) */ * from t1 where c=? or (b=? and (a=? or a=?));';")
	tk.MustQuery("execute stmt using @a, @a, @a, @a").Check(testkit.Rows("10 10 10"))
	tk.MustQuery("execute stmt using @b, @b, @b, @b").Check(testkit.Rows("11 11 11"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustExec("prepare stmt from 'select /*+ use_index_merge(t1) */ * from t1 where c=? or (b=? and (a=? and c=?));';")
	tk.MustQuery("execute stmt using @a, @a, @a, @a").Check(testkit.Rows("10 10 10"))
	tk.MustQuery("execute stmt using @b, @b, @b, @b").Check(testkit.Rows("11 11 11"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustExec("prepare stmt from 'select /*+ use_index_merge(t1) */ * from t1 where c=? or (b=? and (a >= ? and a <= ?));';")
	tk.MustQuery("execute stmt using @a, @a, @b, @a").Check(testkit.Rows("10 10 10"))
	tk.MustQuery("execute stmt using @b, @b, @b, @b").Check(testkit.Rows("11 11 11"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt from 'select /*+ use_index_merge(t1) */ * from t1 where c=10 or (a >=? and a <= ?);';")
	tk.MustExec("set @a=9, @b=10, @c=11;")
	tk.MustQuery("execute stmt using @a, @a;").Check(testkit.Rows("10 10 10"))
	tk.MustQuery("execute stmt using @a, @c;").Check(testkit.Rows("10 10 10", "11 11 11"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @c, @a;").Check(testkit.Rows("10 10 10"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt from 'select /*+ use_index_merge(t1) */ * from t1 where c=10 or (a >=? and a <= ?);';")
	tk.MustExec("set @a=9, @b=10, @c=11;")
	tk.MustQuery("execute stmt using @a, @c;").Check(testkit.Rows("10 10 10", "11 11 11"))
	tk.MustQuery("execute stmt using @a, @a;").Check(testkit.Rows("10 10 10"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @c, @a;").Check(testkit.Rows("10 10 10"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt from 'select /*+ use_index_merge(t1) */ * from t1 where c=10 or (a >=? and a <= ?);';")
	tk.MustExec("set @a=9, @b=10, @c=11;")
	tk.MustQuery("execute stmt using @c, @a;").Check(testkit.Rows("10 10 10"))
	tk.MustQuery("execute stmt using @a, @c;").Check(testkit.Rows("10 10 10", "11 11 11"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @a, @a;").Check(testkit.Rows("10 10 10"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT AS (1), c1 INT PRIMARY KEY)")
	tk.MustExec("INSERT INTO t0(c1) VALUES (0)")
	tk.MustExec("CREATE INDEX i0 ON t0(c0)")
	tk.MustExec("prepare stmt from 'SELECT /*+ USE_INDEX_MERGE(t0, i0, PRIMARY)*/ t0.c0 FROM t0 WHERE t0.c1 OR t0.c0;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt;").Check(testkit.Rows("1"))
	// The plan contains the generated column, so it can not be cached.
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(id int primary key, a int, b int, c int, d int)")
	tk.MustExec("create index t1a on t1(a)")
	tk.MustExec("create index t1b on t1(b)")
	tk.MustExec("create table t2(id int primary key, a int)")
	tk.MustExec("create index t2a on t2(a)")
	tk.MustExec("insert into t1 values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5)")
	tk.MustExec("insert into t2 values(1,1),(5,5)")
	tk.MustExec("prepare stmt from 'select /*+ use_index_merge(t1, t1a, t1b) */ sum(t1.a) from t1 join t2 on t1.id = t2.id where t1.a < ? or t1.b > ?';")
	tk.MustExec("set @a=2, @b=4, @c=5;")
	tk.MustQuery("execute stmt using @a, @b").Check(testkit.Rows("6"))
	tk.MustQuery("execute stmt using @a, @c").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
}

func TestSetOperations4PlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("CREATE TABLE `t1` (a int);")
	tk.MustExec("CREATE TABLE `t2` (a int);")
	tk.MustExec("insert into t1 values(1), (2);")
	tk.MustExec("insert into t2 values(1), (3);")
	// test for UNION
	tk.MustExec("prepare stmt from 'select * from t1 where a > ? union select * from t2 where a > ?;';")
	tk.MustExec("set @a=0, @b=1;")
	tk.MustQuery("execute stmt using @a, @b;").Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("execute stmt using @b, @a;").Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @b, @b;").Sort().Check(testkit.Rows("2", "3"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @a, @a;").Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt from 'select * from t1 where a > ? union all select * from t2 where a > ?;';")
	tk.MustExec("set @a=0, @b=1;")
	tk.MustQuery("execute stmt using @a, @b;").Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("execute stmt using @b, @a;").Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @b, @b;").Sort().Check(testkit.Rows("2", "3"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @a, @a;").Sort().Check(testkit.Rows("1", "1", "2", "3"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	// test for EXCEPT
	tk.MustExec("prepare stmt from 'select * from t1 where a > ? except select * from t2 where a > ?;';")
	tk.MustExec("set @a=0, @b=1;")
	tk.MustQuery("execute stmt using @a, @a;").Sort().Check(testkit.Rows("2"))
	tk.MustQuery("execute stmt using @b, @a;").Sort().Check(testkit.Rows("2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @b, @b;").Sort().Check(testkit.Rows("2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @a, @b;").Sort().Check(testkit.Rows("1", "2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	// test for INTERSECT
	tk.MustExec("prepare stmt from 'select * from t1 where a > ? union select * from t2 where a > ?;';")
	tk.MustExec("set @a=0, @b=1;")
	tk.MustQuery("execute stmt using @a, @a;").Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("execute stmt using @b, @a;").Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @b, @b;").Sort().Check(testkit.Rows("2", "3"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @a, @b;").Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	// test for UNION + INTERSECT
	tk.MustExec("prepare stmt from 'select * from t1 union all select * from t1 intersect select * from t2;'")
	tk.MustQuery("execute stmt;").Sort().Check(testkit.Rows("1", "1", "2"))

	tk.MustExec("prepare stmt from '(select * from t1 union all select * from t1) intersect select * from t2;'")
	tk.MustQuery("execute stmt;").Sort().Check(testkit.Rows("1"))

	// test for order by and limit
	tk.MustExec("prepare stmt from '(select * from t1 union all select * from t1 intersect select * from t2) order by a limit 2;'")
	tk.MustQuery("execute stmt;").Sort().Check(testkit.Rows("1", "1"))
}

func TestSPM4PlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, index idx_a(a));")
	tk.MustExec("delete from mysql.bind_info where default_db='test';")
	tk.MustExec("admin reload bindings;")

	res := tk.MustQuery("explain format = 'brief' select * from t;")
	require.Regexp(t, ".*TableReader.*", res.Rows()[0][0])
	require.Regexp(t, ".*TableFullScan.*", res.Rows()[1][0])

	tk.MustExec("prepare stmt from 'select * from t;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows())

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Regexp(t, ".*TableReader.*", res.Rows()[0][0])
	require.Regexp(t, ".*TableFullScan.*", res.Rows()[1][0])

	tk.MustExec("create global binding for select * from t using select * from t use index(idx_a);")

	res = tk.MustQuery("explain format = 'brief' select * from t;")
	require.Regexp(t, ".*IndexReader.*", res.Rows()[0][0])
	require.Regexp(t, ".*IndexFullScan.*", res.Rows()[1][0])
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))

	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	// The bindSQL has changed, the previous cache is invalid.
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	// We can use the new binding.
	require.Regexp(t, ".*IndexReader.*", res.Rows()[0][0])
	require.Regexp(t, ".*IndexFullScan.*", res.Rows()[1][0])
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))

	tk.MustExec("delete from mysql.bind_info where default_db='test';")
	tk.MustExec("admin reload bindings;")
}

func TestHint4PlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, index idx_a(a));")

	tk.MustExec("prepare stmt from 'select * from t;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt from 'select /*+ IGNORE_PLAN_CACHE() */ * from t;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
}

func TestSelectView4PlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("drop table if exists view_t;")
	tk.MustExec("create table view_t (a int,b int)")
	tk.MustExec("insert into view_t values(1,2)")
	tk.MustExec("create definer='root'@'localhost' view view1 as select * from view_t")
	tk.MustExec("create definer='root'@'localhost' view view2(c,d) as select * from view_t")
	tk.MustExec("create definer='root'@'localhost' view view3(c,d) as select a,b from view_t")
	tk.MustExec("create definer='root'@'localhost' view view4 as select * from (select * from (select * from view_t) tb1) tb;")
	tk.MustExec("prepare stmt1 from 'select * from view1;'")
	tk.MustQuery("execute stmt1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("execute stmt1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt2 from 'select * from view2;'")
	tk.MustQuery("execute stmt2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("execute stmt2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt3 from 'select * from view3;'")
	tk.MustQuery("execute stmt3;").Check(testkit.Rows("1 2"))
	tk.MustQuery("execute stmt3;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt4 from 'select * from view4;'")
	tk.MustQuery("execute stmt4;").Check(testkit.Rows("1 2"))
	tk.MustQuery("execute stmt4;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("drop table view_t;")
	tk.MustExec("create table view_t(c int,d int)")
	err = tk.ExecToErr("execute stmt1;")
	require.Equal(t, "[planner:1356]View 'test.view1' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them", err.Error())
	err = tk.ExecToErr("execute stmt2")
	require.Equal(t, "[planner:1356]View 'test.view2' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them", err.Error())
	err = tk.ExecToErr("execute stmt3")
	require.Equal(t, core.ErrViewInvalid.GenWithStackByArgs("test", "view3").Error(), err.Error())
	tk.MustExec("drop table view_t;")
	tk.MustExec("create table view_t(a int,b int,c int)")
	tk.MustExec("insert into view_t values(1,2,3)")

	tk.MustQuery("execute stmt1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustQuery("execute stmt2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustQuery("execute stmt3;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt3;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustQuery("execute stmt4;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt4;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("alter table view_t drop column a")
	tk.MustExec("alter table view_t add column a int after b")
	tk.MustExec("update view_t set a=1;")

	tk.MustQuery("execute stmt1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustQuery("execute stmt2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustQuery("execute stmt3;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt3;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustQuery("execute stmt4;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt4;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("drop table view_t;")
	tk.MustExec("drop view view1,view2,view3,view4;")

	tk.MustExec("set @@tidb_enable_window_function = 1")
	defer func() {
		tk.MustExec("set @@tidb_enable_window_function = 0")
	}()
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values (1,1),(1,2),(2,1),(2,2)")
	tk.MustExec("create definer='root'@'localhost' view v as select a, first_value(a) over(rows between 1 preceding and 1 following), last_value(a) over(rows between 1 preceding and 1 following) from t")
	tk.MustExec("prepare stmt from 'select * from v;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows("1 1 1", "1 1 2", "2 1 2", "2 2 2"))
	tk.MustQuery("execute stmt;").Check(testkit.Rows("1 1 1", "1 1 2", "2 1 2", "2 2 2"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("drop view v;")
}

func TestInvisibleIndex4PlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t(c1 INT, index idx_c(c1));")

	tk.MustExec("prepare stmt from 'select * from t use index(idx_c) where c1 > 1;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("ALTER TABLE t ALTER INDEX idx_c INVISIBLE;")
	err = tk.ExecToErr("select * from t use index(idx_c) where c1 > 1;")
	require.Equal(t, "[planner:1176]Key 'idx_c' doesn't exist in table 't'", err.Error())

	err = tk.ExecToErr("execute stmt;")
	require.Equal(t, "[planner:1176]Key 'idx_c' doesn't exist in table 't'", err.Error())
}

func TestCTE4PlanCache(t *testing.T) {
	// CTE can not be cached, because part of it will be treated as a subquery.
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("prepare stmt from 'with recursive cte1 as (" +
		"select ? c1 " +
		"union all " +
		"select c1 + 1 c1 from cte1 where c1 < ?) " +
		"select * from cte1;';")
	tk.MustExec("set @a=5, @b=4, @c=2, @d=1;")
	tk.MustQuery("execute stmt using @d, @a").Check(testkit.Rows("1", "2", "3", "4", "5"))
	tk.MustQuery("execute stmt using @d, @b").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @c, @b").Check(testkit.Rows("2", "3", "4"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	// Two seed parts.
	tk.MustExec("prepare stmt from 'with recursive cte1 as (" +
		"select 1 c1 " +
		"union all " +
		"select 2 c1 " +
		"union all " +
		"select c1 + 1 c1 from cte1 where c1 < ?) " +
		"select * from cte1 order by c1;';")
	tk.MustExec("set @a=10, @b=2;")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("1", "2", "2", "3", "3", "4", "4", "5", "5", "6", "6", "7", "7", "8", "8", "9", "9", "10", "10"))
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("1", "2", "2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	// Two recursive parts.
	tk.MustExec("prepare stmt from 'with recursive cte1 as (" +
		"select 1 c1 " +
		"union all " +
		"select 2 c1 " +
		"union all " +
		"select c1 + 1 c1 from cte1 where c1 < ? " +
		"union all " +
		"select c1 + ? c1 from cte1 where c1 < ?) " +
		"select * from cte1 order by c1;';")
	tk.MustExec("set @a=1, @b=2, @c=3, @d=4, @e=5;")
	tk.MustQuery("execute stmt using @c, @b, @e;").Check(testkit.Rows("1", "2", "2", "3", "3", "3", "4", "4", "5", "5", "5", "6", "6"))
	tk.MustQuery("execute stmt using @b, @a, @d;").Check(testkit.Rows("1", "2", "2", "2", "3", "3", "3", "4", "4", "4"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(a int);")
	tk.MustExec("insert into t1 values(1);")
	tk.MustExec("insert into t1 values(2);")
	tk.MustExec("prepare stmt from 'SELECT * FROM t1 dt WHERE EXISTS(WITH RECURSIVE qn AS (SELECT a*? AS b UNION ALL SELECT b+? FROM qn WHERE b=?) SELECT * FROM qn WHERE b=a);';")
	tk.MustExec("set @a=1, @b=2, @c=3, @d=4, @e=5, @f=0;")

	tk.MustQuery("execute stmt using @f, @a, @f").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @a, @b, @a").Sort().Check(testkit.Rows("1", "2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	tk.MustExec("prepare stmt from 'with recursive c(p) as (select ?), cte(a, b) as (select 1, 1 union select a+?, 1 from cte, c where a < ?)  select * from cte order by 1, 2;';")
	tk.MustQuery("execute stmt using @a, @a, @e;").Check(testkit.Rows("1 1", "2 1", "3 1", "4 1", "5 1"))
	tk.MustQuery("execute stmt using @b, @b, @c;").Check(testkit.Rows("1 1", "3 1"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
}

func TestValidity4PlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")

	tk.MustExec("prepare stmt from 'select * from t;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("drop database if exists plan_cache;")
	tk.MustExec("create database plan_cache;")
	tk.MustExec("use plan_cache;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt from 'select * from t;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt;").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("use test") // still read plan_cache.t and can hit the cache
	tk.MustQuery("execute stmt;").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
}

func TestListPartition4PlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("set @@session.tidb_enable_list_partition=1;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int) PARTITION BY LIST (a) ( PARTITION p0 VALUES IN (1, 2, 3), PARTITION p1 VALUES IN (4, 5, 6));")

	tk.MustExec("set @@tidb_partition_prune_mode='static';")
	tk.MustExec("prepare stmt from 'select * from t;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	// The list partition plan can not be cached.
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
}

func TestMoreSessions4PlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int);")
	tk.MustExec("prepare stmt from 'select * from t;';")

	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk2.SetSession(se)

	tk2.MustExec("use test;")
	require.EqualError(t, tk2.ExecToErr("execute stmt;"), "[planner:8111]Prepared statement not found")
	tk2.MustExec("prepare stmt from 'select * from t;';")
	tk2.MustQuery("execute stmt").Check(testkit.Rows())
	tk2.MustQuery("execute stmt").Check(testkit.Rows())
	tk2.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestIssue28792(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t12(a INT, b INT)")
	tk.MustExec("CREATE TABLE t97(a INT, b INT UNIQUE NOT NULL);")
	r1 := tk.MustQuery("EXPLAIN SELECT t12.a, t12.b FROM t12 LEFT JOIN t97 on t12.b = t97.b;").Rows()
	r2 := tk.MustQuery("EXPLAIN SELECT t12.a, t12.b FROM t12 LEFT JOIN t97 use index () on t12.b = t97.b;").Rows()
	require.Equal(t, r2, r1)
}
