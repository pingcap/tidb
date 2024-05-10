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
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/stretchr/testify/require"
)

func TestExplainFor(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tkRoot := testkit.NewTestKit(t, store)
	tkUser := testkit.NewTestKit(t, store)

	tkRoot.MustExec("use test")
	tkRoot.MustExec("drop table if exists t1, t2;")
	tkRoot.MustExec("create table t1(c1 int, c2 int)")
	tkRoot.MustExec("create table t2(c1 int, c2 int)")
	tkRoot.MustExec("create user tu@'%'")
	tkRoot.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)
	tkUser.Session().Auth(&auth.UserIdentity{Username: "tu", Hostname: "localhost", CurrentUser: true, AuthUsername: "tu", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)

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
		require.Regexp(t, "TableReader_5 10000.00 0 root  time:.*, loops:1,( RU:.*,)? cop_task: {num:.*, max:.*, proc_keys:.*num_rpc:1, total_time:.*} data:TableFullScan_4 N/A N/A\n"+
			"└─TableFullScan_4 10000.00 0 cop.* table:t1 tikv_task:{time:.*, loops:0} keep order:false, stats:pseudo N/A N/A",
			buf.String())
	}
	tkRoot.MustQuery("select * from t1;")
	check()
	tkRoot.MustQuery("explain analyze select * from t1;")
	check()
	err := tkUser.ExecToErr(fmt.Sprintf("explain for connection %d", tkRootProcess.ID))
	require.True(t, plannererrors.ErrAccessDenied.Equal(err))
	err = tkUser.ExecToErr("explain for connection 42")
	require.True(t, plannererrors.ErrNoSuchThread.Equal(err))

	tkRootProcess.Plan = nil
	ps = []*util.ProcessInfo{tkRootProcess}
	tkRoot.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tkRoot.MustExec(fmt.Sprintf("explain for connection %d", tkRootProcess.ID))
}

func TestExplainForVerbose(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
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

func TestExplainForConnPlanCache(t *testing.T) {
	t.Skip("unstable")

	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec(`set tidb_enable_prepared_plan_cache=1`)

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

func TestExplainDotForExplainPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
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

func TestPointGetUserVarPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tmp := testkit.NewTestKit(t, store)
	tmp.MustExec("set tidb_enable_prepared_plan_cache=ON")
	tk := testkit.NewTestKit(t, store)
	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)

	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
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
		`Projection_9 10.00 root  test.t1.a, test.t1.b, test.t2.a, test.t2.b`,
		`└─HashJoin_11 10.00 root  CARTESIAN inner join`,
		`  ├─Point_Get_12(Build) 1.00 root table:t2, index:idx_a(a) `, // use idx_a
		`  └─TableReader_14(Probe) 10.00 root  data:TableRangeScan_13`,
		`    └─TableRangeScan_13 10.00 cop[tikv] table:t1 range:[1,1], keep order:false, stats:pseudo`))

	tk.MustExec("set @a=2")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows(
		"2 4 2 2",
	))
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Check(testkit.Rows( // can use idx_a
		`Projection_9 10.00 root  test.t1.a, test.t1.b, test.t2.a, test.t2.b`,
		`└─HashJoin_11 10.00 root  CARTESIAN inner join`,
		`  ├─Point_Get_12(Build) 1.00 root table:t2, index:idx_a(a) `,
		`  └─TableReader_14(Probe) 10.00 root  data:TableRangeScan_13`,
		`    └─TableRangeScan_13 10.00 cop[tikv] table:t1 range:[2,2], keep order:false, stats:pseudo`))
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows(
		"2 4 2 2",
	))
}

func TestExpressionIndexPreparePlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

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
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

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
	require.Len(t, res.Rows(), 2)
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[1][0])

	tk.MustExec("set @a=-1696020282760139948, @b=-2619168038882941276, @c=-4004648990067362699;")
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 2)
	require.Regexp(t, ".*IndexReader.*", res.Rows()[0][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[1][0])

	res = tk.MustQuery("explain format = 'brief' select col1 from UK_GCOL_VIRTUAL_18588 use index(UK_COL1) " +
		"where col1 between -1696020282760139948 and -2619168038882941276 or col1 < -4004648990067362699;")
	require.Len(t, res.Rows(), 2)
	require.Regexp(t, ".*IndexReader.*", res.Rows()[0][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[1][0])
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
	require.Len(t, res.Rows(), 4)
	require.Regexp(t, ".*IndexReader.*", res.Rows()[1][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[3][0])

	tk.MustExec("set @a=2, @b=1, @c=1;")
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 4)
	require.Regexp(t, ".*Selection.*", res.Rows()[2][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[3][0])

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
	require.Len(t, res.Rows(), 5)
	require.Regexp(t, ".*IndexLookUp.*", res.Rows()[1][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[2][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[3][0])
	require.Regexp(t, ".*TableRowIDScan.*", res.Rows()[4][0])

	tk.MustExec("set @a=2, @b=1, @c=1;")
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 5)
	require.Regexp(t, ".*IndexLookUp.*", res.Rows()[1][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[2][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[3][0])
	require.Regexp(t, ".*TableRowIDScan.*", res.Rows()[4][0])

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
	require.Len(t, res.Rows(), 4)
	require.Regexp(t, ".*TableReader.*", res.Rows()[1][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[2][0])
	require.Regexp(t, ".*TableRangeScan.*", res.Rows()[3][0])

	tk.MustExec("set @a=2, @b=1, @c=1;")
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Len(t, res.Rows(), 4)
	require.Regexp(t, ".*TableReader.*", res.Rows()[1][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[2][0])
	require.Regexp(t, ".*TableRangeScan.*", res.Rows()[3][0])

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
	require.Len(t, res.Rows(), 3)
	require.Regexp(t, ".*TableReader.*", res.Rows()[0][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[1][0])
	require.Regexp(t, ".*TableRangeScan.*", res.Rows()[2][0])
}

func TestIssue28696(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

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
	require.Len(t, res.Rows(), 5)
	require.Regexp(t, ".*IndexLookUp.*", res.Rows()[1][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[2][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[3][0])
	require.Regexp(t, ".*TableRowIDScan.*", res.Rows()[4][0])

	res = tk.MustQuery("explain format = 'brief' select a from t1 where b = 'bbcsa';")
	require.Len(t, res.Rows(), 5)
	require.Regexp(t, ".*IndexLookUp.*", res.Rows()[1][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[2][0])
	require.Regexp(t, ".*Selection.*", res.Rows()[3][0])
	require.Regexp(t, ".*TableRowIDScan.*", res.Rows()[4][0])
}

func TestIndexMerge4PlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

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
	require.Regexp(t, ".*IndexMerge.*", res.Rows()[1][0])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[3][0])
	require.Equal(t, "range:(NULL,\"mm\"), (\"mm\",+inf], keep order:false, stats:pseudo", res.Rows()[3][4])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[4][0])
	require.Equal(t, "range:[0198-09-29 20:19:49,0198-09-29 20:19:49], keep order:false, stats:pseudo", res.Rows()[4][4])

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
	require.Len(t, res.Rows(), 5)
	require.Regexp(t, ".*IndexMerge.*", res.Rows()[0][0])
	require.Regexp(t, ".*TableRangeScan.*", res.Rows()[1][0])
	require.Equal(t, "range:(0,3), keep order:false, stats:pseudo", res.Rows()[1][4])
	require.Regexp(t, ".*IndexRangeScan.*", res.Rows()[2][0])
	require.Equal(t, "range:(1,+inf], keep order:false, stats:pseudo", res.Rows()[2][4])

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
	require.Regexp(t, ".*IndexMerge.*", res.Rows()[0][0])

	tk.MustQuery("execute stmt using @b;").Check(testkit.Rows("3 ddcdsaf 3"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0")) // unsafe range
	tk.MustQuery("execute stmt using @b;").Check(testkit.Rows("3 ddcdsaf 3"))
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Regexp(t, ".*IndexMerge.*", res.Rows()[0][0])

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
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	tk.MustExec("prepare stmt from 'select /*+ use_index_merge(t1) */ * from t1 where c=10 or (a >=? and a <= ?);';")
	tk.MustExec("set @a=9, @b=10, @c=11;")
	tk.MustQuery("execute stmt using @a, @a;").Check(testkit.Rows("10 10 10"))
	tk.MustQuery("execute stmt using @a, @c;").Check(testkit.Rows("10 10 10", "11 11 11"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0")) // a>=9 and a<=9 --> a=9
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
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @a, @a;").Check(testkit.Rows("10 10 10"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT AS (1), c1 INT PRIMARY KEY)")
	tk.MustExec("INSERT INTO t0(c1) VALUES (0)")
	tk.MustExec("CREATE INDEX i0 ON t0(c0)")
	tk.MustExec("prepare stmt from 'SELECT /*+ USE_INDEX_MERGE(t0, i0, PRIMARY)*/ t0.c0 FROM t0 WHERE t0.c1 OR t0.c0;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt;").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

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

func TestSPM4PlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, index idx_a(a));")
	tk.MustExec("delete from mysql.bind_info where default_db='test';")
	tk.MustExec("admin reload bindings;")

	res := tk.MustQuery("explain format = 'brief' select * from t;")
	require.Regexp(t, ".*IndexReader.*", res.Rows()[0][0])
	require.Regexp(t, ".*IndexFullScan.*", res.Rows()[1][0])

	tk.MustExec("prepare stmt from 'select * from t;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows())

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Regexp(t, ".*IndexReader.*", res.Rows()[0][0])
	require.Regexp(t, ".*IndexFullScan.*", res.Rows()[1][0])

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

func TestExplainForJSON(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")
	tk1.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk1.MustExec("drop table if exists t1")
	tk1.MustExec("create table t1(id int);")
	tk1.MustQuery("select * from t1;")
	tk1RootProcess := tk1.Session().ShowProcess()
	ps := []*util.ProcessInfo{tk1RootProcess}
	tk1.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk2.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	resRow := tk2.MustQuery(fmt.Sprintf("explain format = 'row' for connection %d", tk1RootProcess.ID)).Rows()
	resJSON := tk2.MustQuery(fmt.Sprintf("explain format = 'tidb_json' for connection %d", tk1RootProcess.ID)).Rows()

	j := new([]*core.ExplainInfoForEncode)
	require.NoError(t, json.Unmarshal([]byte(resJSON[0][0].(string)), j))
	flatJSONRows := make([]*core.ExplainInfoForEncode, 0)
	for _, row := range *j {
		flatJSONRows = append(flatJSONRows, flatJSONPlan(row)...)
	}
	require.Equal(t, len(flatJSONRows), len(resRow))

	for i, row := range resRow {
		require.Contains(t, row[0], flatJSONRows[i].ID)
		require.Equal(t, flatJSONRows[i].EstRows, row[1])
		require.Equal(t, flatJSONRows[i].TaskType, row[2])
		require.Equal(t, flatJSONRows[i].AccessObject, row[3])
		require.Equal(t, flatJSONRows[i].OperatorInfo, row[4])
	}

	tk1.MustExec("set @@tidb_enable_collect_execution_info=1;")
	tk1.MustExec("drop table if exists t2")
	tk1.MustExec("create table t2(id int);")
	tk1.MustQuery("select * from t2;")
	tk1RootProcess = tk1.Session().ShowProcess()
	ps = []*util.ProcessInfo{tk1RootProcess}
	tk1.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk2.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	resRow = tk2.MustQuery(fmt.Sprintf("explain format = 'row' for connection %d", tk1RootProcess.ID)).Rows()
	resJSON = tk2.MustQuery(fmt.Sprintf("explain format = 'tidb_json' for connection %d", tk1RootProcess.ID)).Rows()

	j = new([]*core.ExplainInfoForEncode)
	require.NoError(t, json.Unmarshal([]byte(resJSON[0][0].(string)), j))
	flatJSONRows = []*core.ExplainInfoForEncode{}
	for _, row := range *j {
		flatJSONRows = append(flatJSONRows, flatJSONPlan(row)...)
	}
	require.Equal(t, len(flatJSONRows), len(resRow))

	for i, row := range resRow {
		require.Contains(t, row[0], flatJSONRows[i].ID)
		require.Equal(t, flatJSONRows[i].EstRows, row[1])
		require.Equal(t, flatJSONRows[i].ActRows, row[2])
		require.Equal(t, flatJSONRows[i].TaskType, row[3])
		require.Equal(t, flatJSONRows[i].AccessObject, row[4])
		require.Equal(t, flatJSONRows[i].OperatorInfo, row[6])
		// executeInfo, memory, disk maybe vary in multi execution
		require.NotEqual(t, flatJSONRows[i].ExecuteInfo, "")
		require.NotEqual(t, flatJSONRows[i].MemoryInfo, "")
		require.NotEqual(t, flatJSONRows[i].DiskInfo, "")
	}
	// test syntax
	tk2.MustExec(fmt.Sprintf("explain format = 'tidb_json' for connection %d", tk1RootProcess.ID))
	tk2.MustExec(fmt.Sprintf("explain format = tidb_json for connection %d", tk1RootProcess.ID))
	tk2.MustExec(fmt.Sprintf("explain format = 'TIDB_JSON' for connection %d", tk1RootProcess.ID))
	tk2.MustExec(fmt.Sprintf("explain format = TIDB_JSON for connection %d", tk1RootProcess.ID))
}
