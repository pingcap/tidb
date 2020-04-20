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

func (msm *mockSessionManager1) UpdateTLSConfig(cfg *tls.Config) {
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

func (s *testSuite) TestExplainMetricTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustQuery(fmt.Sprintf("desc select * from METRICS_SCHEMA.tidb_query_duration where time >= '2019-12-23 16:10:13' and time <= '2019-12-23 16:30:13' ")).Check(testkit.Rows(
		"MemTableScan_5 10000.00 root table:tidb_query_duration PromQL:histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance)), start_time:2019-12-23 16:10:13, end_time:2019-12-23 16:30:13, step:1m0s"))
	tk.MustQuery(fmt.Sprintf("desc select * from METRICS_SCHEMA.up where time >= '2019-12-23 16:10:13' and time <= '2019-12-23 16:30:13' ")).Check(testkit.Rows(
		"MemTableScan_5 10000.00 root table:up PromQL:up{}, start_time:2019-12-23 16:10:13, end_time:2019-12-23 16:30:13, step:1m0s"))
	tk.MustQuery("desc select * from information_schema.cluster_log where time >= '2019-12-23 16:10:13' and time <= '2019-12-23 16:30:13'").Check(testkit.Rows(
		"MemTableScan_5 10000.00 root table:CLUSTER_LOG start_time:2019-12-23 16:10:13, end_time:2019-12-23 16:30:13"))
	tk.MustQuery("desc select * from information_schema.cluster_log where level in ('warn','error') and time >= '2019-12-23 16:10:13' and time <= '2019-12-23 16:30:13'").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:CLUSTER_LOG start_time:2019-12-23 16:10:13, end_time:2019-12-23 16:30:13, log_levels:["error","warn"]`))
	tk.MustQuery("desc select * from information_schema.cluster_log where type in ('high_cpu_1','high_memory_1') and time >= '2019-12-23 16:10:13' and time <= '2019-12-23 16:30:13'").Check(testkit.Rows(
		`MemTableScan_5 10000.00 root table:CLUSTER_LOG start_time:2019-12-23 16:10:13, end_time:2019-12-23 16:30:13, node_types:["high_cpu_1","high_memory_1"]`))
}

<<<<<<< HEAD
func (s *testSuite) TestExplainForConnPlanCache(c *C) {
=======
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
>>>>>>> 2a69052... test: fix unit test and make test running serially. (#16525)
	tk := testkit.NewTestKit(c, s.store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	var err error
	tk.Se, err = session.CreateSession4TestWithOpt(s.store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	rows := tk.MustQuery("select connection_id()").Rows()
	c.Assert(len(rows), Equals, 1)
	connID := rows[0][0].(string)
	tk.MustExec("prepare stmt from 'select * from t where a = ?'")
	tk.MustExec("set @p0='1'")
	tk.MustExec("execute stmt using @p0")
	tkProcess := tk.Se.ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %s", connID)).Check(testkit.Rows(
		"TableReader_7 8000.00 root  data:Selection_6",
		"└─Selection_6 8000.00 cop[tikv]  eq(cast(test.t.a), 1)",
		"  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
}
