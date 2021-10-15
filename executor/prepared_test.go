// Copyright 2016 PingCAP, Inc.
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
	"strings"
	"sync/atomic"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	txninfo "github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/israce"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite1) TestPreparedNameResolver(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, KEY id (id))")
	tk.MustExec("prepare stmt from 'select * from t limit ? offset ?'")
	_, err := tk.Exec("prepare stmt from 'select b from t'")
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'b' in 'field list'")

	_, err = tk.Exec("prepare stmt from '(select * FROM t) union all (select * FROM t) order by a limit ?'")
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'a' in 'order clause'")
}

// a 'create table' DDL statement should be accepted if it has no parameters.
func (s *testSuite1) TestPreparedDDL(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("prepare stmt from 'create table t (id int, KEY id (id))'")
}

// TestUnsupportedStmtForPrepare is related to https://github.com/pingcap/tidb/issues/17412
func (s *testSuite1) TestUnsupportedStmtForPrepare(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`prepare stmt0 from "create table t0(a int primary key)"`)
	tk.MustGetErrCode(`prepare stmt1 from "execute stmt0"`, mysql.ErrUnsupportedPs)
	tk.MustGetErrCode(`prepare stmt2 from "deallocate prepare stmt0"`, mysql.ErrUnsupportedPs)
	tk.MustGetErrCode(`prepare stmt4 from "prepare stmt3 from 'create table t1(a int, b int)'"`, mysql.ErrUnsupportedPs)
}

func (s *testSuite1) TestIgnorePlanCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (id int primary key, num int)")
	tk.MustExec("insert into t values (1, 1)")
	tk.MustExec("insert into t values (2, 2)")
	tk.MustExec("insert into t values (3, 3)")
	tk.MustExec("prepare stmt from 'select /*+ IGNORE_PLAN_CACHE() */ * from t where id=?'")
	tk.MustExec("set @ignore_plan_doma = 1")
	tk.MustExec("execute stmt using @ignore_plan_doma")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.UseCache, IsFalse)
}

func (s *testSerialSuite) TestPrepareStmtAfterIsolationReadChange(c *C) {
	if israce.RaceEnabled {
		c.Skip("race test for this case takes too long time")
	}
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(false) // requires plan cache disabled

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	// create virtual tiflash replica.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines='tikv'")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("prepare stmt from \"select * from t\"")
	tk.MustQuery("execute stmt")
	tkProcess := tk.Se.ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	c.Assert(rows[len(rows)-1][2], Equals, "cop[tikv]")

	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	tk.MustExec("execute stmt")
	tkProcess = tk.Se.ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	c.Assert(rows[len(rows)-1][2], Equals, "cop[tiflash]")

	c.Assert(len(tk.Se.GetSessionVars().PreparedStmts), Equals, 1)
	c.Assert(tk.Se.GetSessionVars().PreparedStmts[1].(*plannercore.CachedPrepareStmt).NormalizedSQL, Equals, "select * from `t`")
	c.Assert(tk.Se.GetSessionVars().PreparedStmts[1].(*plannercore.CachedPrepareStmt).NormalizedPlan, Equals, "")
}

type mockSessionManager2 struct {
	se     session.Session
	killed int32
}

func (sm *mockSessionManager2) ShowTxnList() []*txninfo.TxnInfo {
	panic("unimplemented!")
}

func (sm *mockSessionManager2) ShowProcessList() map[uint64]*util.ProcessInfo {
	pl := make(map[uint64]*util.ProcessInfo)
	if pi, ok := sm.GetProcessInfo(0); ok {
		pl[pi.ID] = pi
	}
	return pl
}

func (sm *mockSessionManager2) GetProcessInfo(id uint64) (pi *util.ProcessInfo, notNil bool) {
	pi = sm.se.ShowProcess()
	if pi != nil {
		notNil = true
	}
	return
}
func (sm *mockSessionManager2) Kill(connectionID uint64, query bool) {
	atomic.StoreInt32(&sm.killed, 1)
	atomic.StoreUint32(&sm.se.GetSessionVars().Killed, 1)
}
func (sm *mockSessionManager2) KillAllConnections()             {}
func (sm *mockSessionManager2) UpdateTLSConfig(cfg *tls.Config) {}
func (sm *mockSessionManager2) ServerID() uint64 {
	return 1
}

var _ = SerialSuites(&testSuite12{&baseTestSuite{}})

type testSuite12 struct {
	*baseTestSuite
}

func (s *testSuite12) TestPreparedStmtWithHint(c *C) {
	// see https://github.com/pingcap/tidb/issues/18535
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		store.Close()
		dom.Close()
	}()

	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	tk.Se = se

	sm := &mockSessionManager2{
		se: se,
	}
	se.SetSessionManager(sm)
	go dom.ExpensiveQueryHandle().SetSessionManager(sm).Run()
	tk.MustExec("prepare stmt from \"select /*+ max_execution_time(100) */ sleep(10)\"")
	tk.MustQuery("execute stmt").Check(testkit.Rows("1"))
	c.Check(atomic.LoadInt32(&sm.killed), Equals, int32(1))
}

func (s *testSerialSuite) TestPlanCacheClusterIndex(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("create table t1(a varchar(20), b varchar(20), c varchar(20), primary key(a, b))")
	tk.MustExec("insert into t1 values('1','1','111'),('2','2','222'),('3','3','333')")

	// For table scan
	tk.MustExec(`prepare stmt1 from "select * from t1 where t1.a = ? and t1.b > ?"`)
	tk.MustExec("set @v1 = '1'")
	tk.MustExec("set @v2 = '0'")
	tk.MustQuery("execute stmt1 using @v1,@v2").Check(testkit.Rows("1 1 111"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("set @v1 = '2'")
	tk.MustExec("set @v2 = '1'")
	tk.MustQuery("execute stmt1 using @v1,@v2").Check(testkit.Rows("2 2 222"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @v1 = '3'")
	tk.MustExec("set @v2 = '2'")
	tk.MustQuery("execute stmt1 using @v1,@v2").Check(testkit.Rows("3 3 333"))
	tkProcess := tk.Se.ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	c.Assert(strings.Index(rows[len(rows)-1][4].(string), `range:("3" "2","3" +inf]`), Equals, 0)

	// For point get
	tk.MustExec(`prepare stmt2 from "select * from t1 where t1.a = ? and t1.b = ?"`)
	tk.MustExec("set @v1 = '1'")
	tk.MustExec("set @v2 = '1'")
	tk.MustQuery("execute stmt2 using @v1,@v2").Check(testkit.Rows("1 1 111"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("set @v1 = '2'")
	tk.MustExec("set @v2 = '2'")
	tk.MustQuery("execute stmt2 using @v1,@v2").Check(testkit.Rows("2 2 222"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @v1 = '3'")
	tk.MustExec("set @v2 = '3'")
	tk.MustQuery("execute stmt2 using @v1,@v2").Check(testkit.Rows("3 3 333"))
	tkProcess = tk.Se.ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	c.Assert(strings.Index(rows[len(rows)-1][0].(string), `Point_Get`), Equals, 0)

	// For CBO point get and batch point get
	// case 1:
	tk.MustExec(`drop table if exists ta, tb`)
	tk.MustExec(`create table ta (a varchar(8) primary key, b int)`)
	tk.MustExec(`insert ta values ('a', 1), ('b', 2)`)
	tk.MustExec(`create table tb (a varchar(8) primary key, b int)`)
	tk.MustExec(`insert tb values ('a', 1), ('b', 2)`)
	tk.MustExec(`prepare stmt1 from "select * from ta, tb where ta.a = tb.a and ta.a = ?"`)
	tk.MustExec(`set @v1 = 'a', @v2 = 'b'`)
	tk.MustQuery(`execute stmt1 using @v1`).Check(testkit.Rows("a 1 a 1"))
	tk.MustQuery(`execute stmt1 using @v2`).Check(testkit.Rows("b 2 b 2"))

	// case 2:
	tk.MustExec(`drop table if exists ta, tb`)
	tk.MustExec(`create table ta (a varchar(10) primary key, b int not null)`)
	tk.MustExec(`insert ta values ('a', 1), ('b', 2)`)
	tk.MustExec(`create table tb (b int primary key, c int)`)
	tk.MustExec(`insert tb values (1, 1), (2, 2)`)
	tk.MustExec(`prepare stmt1 from "select * from ta, tb where ta.b = tb.b and ta.a = ?"`)
	tk.MustExec(`set @v1 = 'a', @v2 = 'b'`)
	tk.MustQuery(`execute stmt1 using @v1`).Check(testkit.Rows("a 1 1 1"))
	tk.MustQuery(`execute stmt1 using @v2`).Check(testkit.Rows("b 2 2 2"))
	tk.MustQuery(`execute stmt1 using @v2`).Check(testkit.Rows("b 2 2 2"))
	tkProcess = tk.Se.ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	c.Assert(strings.Contains(rows[3][0].(string), `TableRangeScan`), IsTrue)

	// case 3:
	tk.MustExec(`drop table if exists ta, tb`)
	tk.MustExec(`create table ta (a varchar(10), b varchar(10), c int, primary key (a, b))`)
	tk.MustExec(`insert ta values ('a', 'a', 1), ('b', 'b', 2), ('c', 'c', 3)`)
	tk.MustExec(`create table tb (b int primary key, c int)`)
	tk.MustExec(`insert tb values (1, 1), (2, 2), (3,3)`)
	tk.MustExec(`prepare stmt1 from "select * from ta, tb where ta.c = tb.b and ta.a = ? and ta.b = ?"`)
	tk.MustExec(`set @v1 = 'a', @v2 = 'b', @v3 = 'c'`)
	tk.MustQuery(`execute stmt1 using @v1, @v1`).Check(testkit.Rows("a a 1 1 1"))
	tk.MustQuery(`execute stmt1 using @v2, @v2`).Check(testkit.Rows("b b 2 2 2"))
	tk.MustExec(`prepare stmt2 from "select * from ta, tb where ta.c = tb.b and (ta.a, ta.b) in ((?, ?), (?, ?))"`)
	tk.MustQuery(`execute stmt2 using @v1, @v1, @v2, @v2`).Check(testkit.Rows("a a 1 1 1", "b b 2 2 2"))
	tk.MustQuery(`execute stmt2 using @v2, @v2, @v3, @v3`).Check(testkit.Rows("b b 2 2 2", "c c 3 3 3"))

	// For issue 19002
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1(a int, b int, c int, primary key(a, b))`)
	tk.MustExec(`insert into t1 values(1,1,111),(2,2,222),(3,3,333)`)
	// Point Get:
	tk.MustExec(`prepare stmt1 from "select * from t1 where t1.a = ? and t1.b = ?"`)
	tk.MustExec(`set @v1=1, @v2=1`)
	tk.MustQuery(`execute stmt1 using @v1,@v2`).Check(testkit.Rows("1 1 111"))
	tk.MustExec(`set @v1=2, @v2=2`)
	tk.MustQuery(`execute stmt1 using @v1,@v2`).Check(testkit.Rows("2 2 222"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	// Batch Point Get:
	tk.MustExec(`prepare stmt2 from "select * from t1 where (t1.a,t1.b) in ((?,?),(?,?))"`)
	tk.MustExec(`set @v1=1, @v2=1, @v3=2, @v4=2`)
	tk.MustQuery(`execute stmt2 using @v1,@v2,@v3,@v4`).Check(testkit.Rows("1 1 111", "2 2 222"))
	tk.MustExec(`set @v1=2, @v2=2, @v3=3, @v4=3`)
	tk.MustQuery(`execute stmt2 using @v1,@v2,@v3,@v4`).Check(testkit.Rows("2 2 222", "3 3 333"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func (s *testPrepareSuite) TestPlanCacheWithDifferentVariableTypes(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("create table t1(a varchar(20), b int, c float, key(b, a))")
	tk.MustExec("insert into t1 values('1',1,1.1),('2',2,222),('3',3,333)")
	tk.MustExec("create table t2(a varchar(20), b int, c float, key(b, a))")
	tk.MustExec("insert into t2 values('3',3,3.3),('2',2,222),('3',3,333)")

	var input []struct {
		PrepareStmt string
		Executes    []struct {
			Vars []struct {
				Name  string
				Value string
			}
			ExecuteSQL string
		}
	}
	var output []struct {
		PrepareStmt string
		Executes    []struct {
			SQL  string
			Vars []struct {
				Name  string
				Value string
			}
			Plan             []string
			LastPlanUseCache string
			Result           []string
		}
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		tk.MustExec(tt.PrepareStmt)
		s.testData.OnRecord(func() {
			output[i].PrepareStmt = tt.PrepareStmt
			output[i].Executes = make([]struct {
				SQL  string
				Vars []struct {
					Name  string
					Value string
				}
				Plan             []string
				LastPlanUseCache string
				Result           []string
			}, len(tt.Executes))
		})
		c.Assert(output[i].PrepareStmt, Equals, tt.PrepareStmt)
		for j, exec := range tt.Executes {
			for _, v := range exec.Vars {
				tk.MustExec(fmt.Sprintf(`set @%s = %s`, v.Name, v.Value))
			}
			res := tk.MustQuery(exec.ExecuteSQL)
			lastPlanUseCache := tk.MustQuery("select @@last_plan_from_cache").Rows()[0][0]
			tk.MustQuery(exec.ExecuteSQL)
			tkProcess := tk.Se.ShowProcess()
			ps := []*util.ProcessInfo{tkProcess}
			tk.Se.SetSessionManager(&mockSessionManager1{PS: ps})
			plan := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
			s.testData.OnRecord(func() {
				output[i].Executes[j].SQL = exec.ExecuteSQL
				output[i].Executes[j].Plan = s.testData.ConvertRowsToStrings(plan.Rows())
				output[i].Executes[j].Vars = exec.Vars
				output[i].Executes[j].LastPlanUseCache = lastPlanUseCache.(string)
				output[i].Executes[j].Result = s.testData.ConvertRowsToStrings(res.Rows())
			})
			c.Assert(output[i].Executes[j].SQL, Equals, exec.ExecuteSQL)
			plan.Check(testkit.Rows(output[i].Executes[j].Plan...))
			c.Assert(output[i].Executes[j].Vars, DeepEquals, exec.Vars)
			c.Assert(output[i].Executes[j].LastPlanUseCache, Equals, lastPlanUseCache.(string))
			res.Check(testkit.Rows(output[i].Executes[j].Result...))
		}
	}
}

func (s *testPrepareSuite) TestPlanCacheXXX(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)

	type ExecCase struct {
		Parameters []string
		UseCache   bool
	}
	type PrepCase struct {
		PrepStmt  string
		ExecCases []ExecCase
	}

	cases := []PrepCase{
		{"use test", nil},

		// cases for Limit
		{"create table t (a int)", nil},
		{"insert into t values (1), (1), (2), (2), (3), (4), (5), (6), (7), (8), (9), (0), (0)", nil},
		{"select * from t limit ?", []ExecCase{
			{[]string{"20"}, false},
			{[]string{"30"}, false},
		}},
		{"select * from t limit 40, ?", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"2"}, false},
		}},
		{"select * from t limit ?, 10", []ExecCase{
			{[]string{"20"}, false},
			{[]string{"30"}, false},
		}},
		{"select * from t limit ?, ?", []ExecCase{
			{[]string{"20", "20"}, false},
			{[]string{"20", "40"}, false},
		}},
		{"select * from t where a<? limit 20", []ExecCase{
			{[]string{"2"}, false},
			{[]string{"5"}, true},
			{[]string{"9"}, true},
		}},
		{"drop table t", nil},

		// cases for order
		{"create table t (a int, b int)", nil},
		{"insert into t values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)", nil},
		{"select * from t order by ?", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"2"}, false},
		}},
		{"select * from t order by b+?", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"2"}, true},
			{[]string{"3"}, true},
		}},
		{"select * from t order by mod(a, ?)", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"2"}, true},
			{[]string{"3"}, true},
		}},
		{"select * from t where b>? order by mod(a, 3)", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"2"}, true},
			{[]string{"3"}, true},
		}},

		// cases for topN
		{"select * from t order by b limit ?", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"2"}, false},
		}},
		{"select * from t order by b limit 10, ?", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"2"}, false},
		}},
		{"select * from t order by ? limit 10", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"2"}, false},
		}},
		{"select * from t order by ? limit ?", []ExecCase{
			{[]string{"1", "10"}, false},
			{[]string{"2", "20"}, false},
		}},
	}

	for _, prepCase := range cases {
		isQuery := strings.Contains(prepCase.PrepStmt, "select")
		if !isQuery {
			tk.MustExec(prepCase.PrepStmt)
			continue
		}

		tk.MustExec(fmt.Sprintf(`prepare stmt from '%v'`, prepCase.PrepStmt))
		for _, execCase := range prepCase.ExecCases {
			// set all parameters
			usingStmt := ""
			if len(execCase.Parameters) > 0 {
				setStmt := "set "
				usingStmt = "using "
				for i, parameter := range execCase.Parameters {
					if i > 0 {
						setStmt += ", "
						usingStmt += ", "
					}
					setStmt += fmt.Sprintf("@x%v=%v", i, parameter)
					usingStmt += fmt.Sprintf("@x%v", i)
				}
				tk.MustExec(setStmt)
			}

			// execute this statement and check whether it uses a cached plan
			results := tk.MustQuery("execute stmt " + usingStmt).Sort().Rows()
			useCache := "0"
			if execCase.UseCache {
				useCache = "1"
			}
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(useCache))

			// check whether the result is correct
			tmp := strings.Split(prepCase.PrepStmt, "?")
			c.Assert(len(tmp), Equals, len(execCase.Parameters)+1)
			query := ""
			for i := range tmp {
				query += tmp[i]
				if i < len(execCase.Parameters) {
					query += execCase.Parameters[i]
				}
			}
			tk.MustQuery(query).Sort().Check(results)
		}
	}
}

func (s *testSerialSuite) TestIssue28782(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("prepare stmt from 'SELECT IF(?, 1, 0);';")
	tk.MustExec("set @a=1, @b=null, @c=0")

	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @b;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @c;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
}

func (s *testSerialSuite) TestIssue28087And28162(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)

	// issue 28087
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists IDT_26207`)
	tk.MustExec(`CREATE TABLE IDT_26207 (col1 bit(1))`)
	tk.MustExec(`insert into  IDT_26207 values(0x0), (0x1)`)
	tk.MustExec(`prepare stmt from 'select t1.col1 from IDT_26207 as t1 left join IDT_26207 as t2 on t1.col1 = t2.col1 where t1.col1 in (?, ?, ?)'`)
	tk.MustExec(`set @a=0x01, @b=0x01, @c=0x01`)
	tk.MustQuery(`execute stmt using @a,@b,@c`).Check(testkit.Rows("\x01"))
	tk.MustExec(`set @a=0x00, @b=0x00, @c=0x01`)
	tk.MustQuery(`execute stmt using @a,@b,@c`).Check(testkit.Rows("\x00", "\x01"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	// issue 28162
	tk.MustExec(`drop table if exists IDT_MC21780`)
	tk.MustExec(`CREATE TABLE IDT_MC21780 (
		COL1 timestamp NULL DEFAULT NULL,
		COL2 timestamp NULL DEFAULT NULL,
		COL3 timestamp NULL DEFAULT NULL,
		KEY U_M_COL (COL1,COL2)
	)`)
	tk.MustExec(`insert into IDT_MC21780 values("1970-12-18 10:53:28", "1970-12-18 10:53:28", "1970-12-18 10:53:28")`)
	tk.MustExec(`prepare stmt from 'select/*+ hash_join(t1) */ * from IDT_MC21780 t1 join IDT_MC21780 t2 on t1.col1 = t2.col1 where t1. col1 < ? and t2. col1 in (?, ?, ?);'`)
	tk.MustExec(`set @a="2038-01-19 03:14:07", @b="2038-01-19 03:14:07", @c="2038-01-19 03:14:07", @d="2038-01-19 03:14:07"`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d`).Check(testkit.Rows())
	tk.MustExec(`set @a="1976-09-09 20:21:11", @b="2021-07-14 09:28:16", @c="1982-01-09 03:36:39", @d="1970-12-18 10:53:28"`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d`).Check(testkit.Rows("1970-12-18 10:53:28 1970-12-18 10:53:28 1970-12-18 10:53:28 1970-12-18 10:53:28 1970-12-18 10:53:28 1970-12-18 10:53:28"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func (s *testSerialSuite) TestIssue28064(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t28064")
	tk.MustExec("CREATE TABLE `t28064` (" +
		"`a` decimal(10,0) DEFAULT NULL," +
		"`b` decimal(10,0) DEFAULT NULL," +
		"`c` decimal(10,0) DEFAULT NULL," +
		"`d` decimal(10,0) DEFAULT NULL," +
		"KEY `iabc` (`a`,`b`,`c`));")
	tk.MustExec("set @a='123', @b='234', @c='345';")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("prepare stmt1 from 'select * from t28064 use index (iabc) where a = ? and b = ? and c = ?';")

	tk.MustExec("execute stmt1 using @a, @b, @c;")
	tkProcess := tk.Se.ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
	rows.Check(testkit.Rows("IndexLookUp_8 0.00 root  ",
		"├─Selection_7(Build) 0.00 cop[tikv]  eq(test.t28064.a, 123), eq(test.t28064.b, 234), eq(test.t28064.c, 345)",
		"│ └─IndexRangeScan_5 0.00 cop[tikv] table:t28064, index:iabc(a, b, c) range:[123 234 345,123 234 345], keep order:false, stats:pseudo",
		"└─TableRowIDScan_6(Probe) 0.00 cop[tikv] table:t28064 keep order:false, stats:pseudo"))

	tk.MustExec("execute stmt1 using @a, @b, @c;")
	rows = tk.MustQuery("select @@last_plan_from_cache")
	rows.Check(testkit.Rows("1"))

	tk.MustExec("execute stmt1 using @a, @b, @c;")
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
	rows.Check(testkit.Rows("IndexLookUp_8 0.00 root  ",
		"├─Selection_7(Build) 0.00 cop[tikv]  eq(test.t28064.a, 123), eq(test.t28064.b, 234), eq(test.t28064.c, 345)",
		"│ └─IndexRangeScan_5 0.00 cop[tikv] table:t28064, index:iabc(a, b, c) range:[123 234 345,123 234 345], keep order:false, stats:pseudo",
		"└─TableRowIDScan_6(Probe) 0.00 cop[tikv] table:t28064 keep order:false, stats:pseudo"))
}
