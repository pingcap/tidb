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
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func TestPreparedNameResolver(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, KEY id (id))")
	tk.MustExec("prepare stmt from 'select * from t limit ? offset ?'")
	_, err := tk.Exec("prepare stmt from 'select b from t'")
	require.EqualError(t, err, "[planner:1054]Unknown column 'b' in 'field list'")

	_, err = tk.Exec("prepare stmt from '(select * FROM t) union all (select * FROM t) order by a limit ?'")
	require.EqualError(t, err, "[planner:1054]Unknown column 'a' in 'order clause'")
}

// a 'create table' DDL statement should be accepted if it has no parameters.
func TestPreparedDDL(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("prepare stmt from 'create table t (id int, KEY id (id))'")
}

// TestUnsupportedStmtForPrepare is related to https://github.com/pingcap/tidb/issues/17412
func TestUnsupportedStmtForPrepare(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`prepare stmt0 from "create table t0(a int primary key)"`)
	tk.MustGetErrCode(`prepare stmt1 from "execute stmt0"`, mysql.ErrUnsupportedPs)
	tk.MustGetErrCode(`prepare stmt2 from "deallocate prepare stmt0"`, mysql.ErrUnsupportedPs)
	tk.MustGetErrCode(`prepare stmt4 from "prepare stmt3 from 'create table t1(a int, b int)'"`, mysql.ErrUnsupportedPs)
}

func TestIgnorePlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (id int primary key, num int)")
	tk.MustExec("insert into t values (1, 1)")
	tk.MustExec("insert into t values (2, 2)")
	tk.MustExec("insert into t values (3, 3)")
	tk.MustExec("prepare stmt from 'select /*+ IGNORE_PLAN_CACHE() */ * from t where id=?'")
	tk.MustExec("set @ignore_plan_doma = 1")
	tk.MustExec("execute stmt using @ignore_plan_doma")
	require.False(t, tk.Session().GetSessionVars().StmtCtx.UseCache)
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

func (sm *mockSessionManager2) GetProcessInfo(_ uint64) (pi *util.ProcessInfo, notNil bool) {
	pi = sm.se.ShowProcess()
	if pi != nil {
		notNil = true
	}
	return
}

func (sm *mockSessionManager2) Kill(_ uint64, _ bool) {
	atomic.StoreInt32(&sm.killed, 1)
	atomic.StoreUint32(&sm.se.GetSessionVars().Killed, 1)
}
func (sm *mockSessionManager2) KillAllConnections()           {}
func (sm *mockSessionManager2) UpdateTLSConfig(_ *tls.Config) {}
func (sm *mockSessionManager2) ServerID() uint64              { return 1 }

func (sm *mockSessionManager2) StoreInternalSession(se interface{}) {}

func (sm *mockSessionManager2) DeleteInternalSession(se interface{}) {}

func (sm *mockSessionManager2) GetInternalSessionStartTSList() []uint64 {
	return nil
}

func TestPreparedStmtWithHint(t *testing.T) {
	// see https://github.com/pingcap/tidb/issues/18535
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.SetSession(se)

	sm := &mockSessionManager2{
		se: se,
	}
	se.SetSessionManager(sm)
	go dom.ExpensiveQueryHandle().SetSessionManager(sm).Run()
	tk.MustExec("prepare stmt from \"select /*+ max_execution_time(100) */ sleep(10)\"")
	tk.MustQuery("execute stmt").Check(testkit.Rows("1"))
	require.Equal(t, int32(1), atomic.LoadInt32(&sm.killed))
}

func TestPreparedNullParam(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()

	flags := []bool{false, true}
	for _, flag := range flags {
		plannercore.SetPreparedPlanCache(flag)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_enable_collect_execution_info=0")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (id int not null, KEY id (id))")
		tk.MustExec("insert into t values (1), (2), (3)")

		tk.MustExec("prepare stmt from 'select * from t where id = ?'")
		tk.MustExec("set @a= null")
		tk.MustQuery("execute stmt using @a").Check(testkit.Rows())

		tkProcess := tk.Session().ShowProcess()
		ps := []*util.ProcessInfo{tkProcess}
		tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
		tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Check(testkit.Rows(
			"TableDual_5 0.00 root  rows:0"))
	}
}

func TestIssue29850(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`set tidb_enable_clustered_index=on`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0")
	tk.MustExec(`use test`)
	tk.MustExec(`CREATE TABLE customer (
	  c_id int(11) NOT NULL,
	  c_d_id int(11) NOT NULL,
	  c_first varchar(16) DEFAULT NULL,
	  c_w_id int(11) NOT NULL,
	  c_last varchar(16) DEFAULT NULL,
	  c_credit char(2) DEFAULT NULL,
	  c_discount decimal(4,4) DEFAULT NULL,
	  PRIMARY KEY (c_w_id,c_d_id,c_id),
	  KEY idx_customer (c_w_id,c_d_id,c_last,c_first))`)
	tk.MustExec(`CREATE TABLE warehouse (
	  w_id int(11) NOT NULL,
	  w_tax decimal(4,4) DEFAULT NULL,
	  PRIMARY KEY (w_id))`)
	tk.MustExec(`prepare stmt from 'SELECT c_discount, c_last, c_credit, w_tax
		FROM customer, warehouse
		WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?'`)
	tk.MustExec(`set @w_id=1262`)
	tk.MustExec(`set @c_d_id=7`)
	tk.MustExec(`set @c_id=1549`)
	tk.MustQuery(`execute stmt using @w_id, @c_d_id, @c_id`).Check(testkit.Rows())
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Check(testkit.Rows( // can use PointGet
		`Projection_7 0.00 root  test.customer.c_discount, test.customer.c_last, test.customer.c_credit, test.warehouse.w_tax`,
		`└─MergeJoin_8 0.00 root  inner join, left key:test.customer.c_w_id, right key:test.warehouse.w_id`,
		`  ├─Point_Get_34(Build) 1.00 root table:warehouse handle:1262`,
		`  └─Point_Get_33(Probe) 1.00 root table:customer, clustered index:PRIMARY(c_w_id, c_d_id, c_id) `))
	tk.MustQuery(`execute stmt using @w_id, @c_d_id, @c_id`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1")) // can use the cached plan

	tk.MustExec(`create table t (a int primary key)`)
	tk.MustExec(`insert into t values (1), (2)`)
	tk.MustExec(`prepare stmt from 'select * from t where a>=? and a<=?'`)
	tk.MustExec(`set @a1=1, @a2=2`)
	tk.MustQuery(`execute stmt using @a1, @a1`).Check(testkit.Rows("1"))
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Check(testkit.Rows( // cannot use PointGet since it contains a range condition
		`Selection_7 1.00 root  ge(test.t.a, 1), le(test.t.a, 1)`,
		`└─TableReader_6 1.00 root  data:TableRangeScan_5`,
		`  └─TableRangeScan_5 1.00 cop[tikv] table:t range:[1,1], keep order:false, stats:pseudo`))
	tk.MustQuery(`execute stmt using @a1, @a2`).Check(testkit.Rows("1", "2"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`prepare stmt from 'select * from t where a=? or a=?'`)
	tk.MustQuery(`execute stmt using @a1, @a1`).Check(testkit.Rows("1"))
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Check(testkit.Rows( // cannot use PointGet since it contains a or condition
		`Selection_7 1.00 root  or(eq(test.t.a, 1), eq(test.t.a, 1))`,
		`└─TableReader_6 1.00 root  data:TableRangeScan_5`,
		`  └─TableRangeScan_5 1.00 cop[tikv] table:t range:[1,1], keep order:false, stats:pseudo`))
	tk.MustQuery(`execute stmt using @a1, @a2`).Check(testkit.Rows("1", "2"))
}

func TestIssue28064(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk := testkit.NewTestKit(t, store)
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
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
	rows.Check(testkit.Rows("Selection_8 0.00 root  eq(test.t28064.a, 123), eq(test.t28064.b, 234), eq(test.t28064.c, 345)",
		"└─IndexLookUp_7 0.00 root  ",
		"  ├─IndexRangeScan_5(Build) 0.00 cop[tikv] table:t28064, index:iabc(a, b, c) range:[123 234 345,123 234 345], keep order:false, stats:pseudo",
		"  └─TableRowIDScan_6(Probe) 0.00 cop[tikv] table:t28064 keep order:false, stats:pseudo"))

	tk.MustExec("execute stmt1 using @a, @b, @c;")
	rows = tk.MustQuery("select @@last_plan_from_cache")
	rows.Check(testkit.Rows("1"))

	tk.MustExec("execute stmt1 using @a, @b, @c;")
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
	rows.Check(testkit.Rows("Selection_8 0.00 root  eq(test.t28064.a, 123), eq(test.t28064.b, 234), eq(test.t28064.c, 345)",
		"└─IndexLookUp_7 0.00 root  ",
		"  ├─IndexRangeScan_5(Build) 0.00 cop[tikv] table:t28064, index:iabc(a, b, c) range:[123 234 345,123 234 345], keep order:false, stats:pseudo",
		"  └─TableRowIDScan_6(Probe) 0.00 cop[tikv] table:t28064 keep order:false, stats:pseudo"))
}

func TestPreparePlanCache4Blacklist(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")

	// test the blacklist of optimization rules
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("prepare stmt from 'select min(a) from t;';")
	tk.MustExec("execute stmt;")
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
	require.Contains(t, res.Rows()[1][0], "TopN")

	res = tk.MustQuery("explain format = 'brief' select min(a) from t")
	require.Contains(t, res.Rows()[1][0], "TopN")

	tk.MustExec("INSERT INTO mysql.opt_rule_blacklist VALUES('max_min_eliminate');")
	tk.MustExec("ADMIN reload opt_rule_blacklist;")

	tk.MustExec("execute stmt;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustExec("execute stmt;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
	// Plans that have been cached will not be affected by the blacklist.
	require.Contains(t, res.Rows()[1][0], "TopN")

	res = tk.MustQuery("explain format = 'brief' select min(a) from t")
	require.Contains(t, res.Rows()[0][0], "StreamAgg")

	// test the blacklist of Expression Pushdown
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("prepare stmt from 'SELECT * FROM t WHERE a < 2 and a > 2;';")
	tk.MustExec("execute stmt;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
	require.Equal(t, 3, len(res.Rows()))
	require.Contains(t, res.Rows()[1][0], "Selection")
	require.Equal(t, "gt(test.t.a, 2), lt(test.t.a, 2)", res.Rows()[1][4])

	res = tk.MustQuery("explain format = 'brief' SELECT * FROM t WHERE a < 2 and a > 2;")
	require.Equal(t, 3, len(res.Rows()))
	require.Equal(t, "gt(test.t.a, 2), lt(test.t.a, 2)", res.Rows()[1][4])

	tk.MustExec("INSERT INTO mysql.expr_pushdown_blacklist VALUES('<','tikv','');")
	tk.MustExec("ADMIN reload expr_pushdown_blacklist;")

	tk.MustExec("execute stmt;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustExec("execute stmt;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
	// The expressions can still be pushed down to tikv.
	require.Equal(t, 3, len(res.Rows()))
	require.Contains(t, res.Rows()[1][0], "Selection")
	require.Equal(t, "gt(test.t.a, 2), lt(test.t.a, 2)", res.Rows()[1][4])

	res = tk.MustQuery("explain format = 'brief' SELECT * FROM t WHERE a < 2 and a > 2;")
	require.Equal(t, 4, len(res.Rows()))
	require.Contains(t, res.Rows()[0][0], "Selection")
	require.Equal(t, "lt(test.t.a, 2)", res.Rows()[0][4])
	require.Contains(t, res.Rows()[2][0], "Selection")
	require.Equal(t, "gt(test.t.a, 2)", res.Rows()[2][4])

	tk.MustExec("DELETE FROM mysql.expr_pushdown_blacklist;")
	tk.MustExec("ADMIN reload expr_pushdown_blacklist;")
}

func TestPlanCacheClusterIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
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
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Equal(t, 0, strings.Index(rows[len(rows)-1][4].(string), `range:("3" "2","3" +inf]`))
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
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Equal(t, 0, strings.Index(rows[len(rows)-1][0].(string), `Point_Get`))
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
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.True(t, strings.Contains(rows[3][0].(string), `TableRangeScan`))

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
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
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

func TestPlanCacheWithDifferentVariableTypes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk := testkit.NewTestKit(t, store)

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
	prepareMergeSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		tk.MustExec(tt.PrepareStmt)
		testdata.OnRecord(func() {
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
		require.Equal(t, tt.PrepareStmt, output[i].PrepareStmt)
		for j, exec := range tt.Executes {
			for _, v := range exec.Vars {
				tk.MustExec(fmt.Sprintf(`set @%s = %s`, v.Name, v.Value))
			}
			res := tk.MustQuery(exec.ExecuteSQL)
			lastPlanUseCache := tk.MustQuery("select @@last_plan_from_cache").Rows()[0][0]
			tk.MustQuery(exec.ExecuteSQL)
			tkProcess := tk.Session().ShowProcess()
			ps := []*util.ProcessInfo{tkProcess}
			tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
			plan := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
			testdata.OnRecord(func() {
				output[i].Executes[j].SQL = exec.ExecuteSQL
				output[i].Executes[j].Plan = testdata.ConvertRowsToStrings(plan.Rows())
				output[i].Executes[j].Vars = exec.Vars
				output[i].Executes[j].LastPlanUseCache = lastPlanUseCache.(string)
				output[i].Executes[j].Result = testdata.ConvertRowsToStrings(res.Rows())
			})

			require.Equal(t, exec.ExecuteSQL, output[i].Executes[j].SQL)
			plan.Check(testkit.Rows(output[i].Executes[j].Plan...))
			require.Equal(t, exec.Vars, output[i].Executes[j].Vars)
			require.Equal(t, lastPlanUseCache.(string), output[i].Executes[j].LastPlanUseCache)
			res.Check(testkit.Rows(output[i].Executes[j].Result...))
		}
	}
}

func TestPlanCacheOperators(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk := testkit.NewTestKit(t, store)
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

		// cases for TableReader on PK
		{"create table t (a int, b int, primary key(a))", nil},
		{"insert into t values (1,1), (2,2), (3,3), (4,4), (5,5), (6,null)", nil},
		{"select a from t where a=?", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"2"}, true},
			{[]string{"3"}, true},
		}},
		{"select a from t where a in (?,?,?)", []ExecCase{
			{[]string{"1", "1", "1"}, false},
			{[]string{"2", "3", "4"}, true},
			{[]string{"3", "5", "7"}, true},
		}},
		{"select a from t where a>? and a<?", []ExecCase{
			{[]string{"5", "1"}, false},
			{[]string{"1", "4"}, true},
			{[]string{"3", "9"}, true},
		}},
		{"drop table t", nil},

		// cases for IndexReader on UK
		{"create table t (a int, b int, unique key(a))", nil},
		{"insert into t values (1,1), (2,2), (3,3), (4,4), (5,5), (6,null)", nil},
		{"select a from t where a=?", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"2"}, true},
			{[]string{"3"}, true},
		}},
		{"select a from t where a in (?,?,?)", []ExecCase{
			{[]string{"1", "1", "1"}, false},
			{[]string{"2", "3", "4"}, true},
			{[]string{"3", "5", "7"}, true},
		}},
		{"select a from t where a>? and a<?", []ExecCase{
			{[]string{"5", "1"}, false},
			{[]string{"1", "4"}, true},
			{[]string{"3", "9"}, true},
		}},
		{"drop table t", nil},

		// cases for IndexReader on Index
		{"create table t (a int, b int, key(a))", nil},
		{"insert into t values (1,1), (2,2), (3,3), (4,4), (5,5), (6,null)", nil},
		{"select a from t where a=?", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"2"}, true},
			{[]string{"3"}, true},
		}},
		{"select a from t where a in (?,?,?)", []ExecCase{
			{[]string{"1", "1", "1"}, false},
			{[]string{"2", "3", "4"}, true},
			{[]string{"3", "5", "7"}, true},
		}},
		{"select a from t where a>? and a<?", []ExecCase{
			{[]string{"5", "1"}, false},
			{[]string{"1", "4"}, true},
			{[]string{"3", "9"}, true},
		}},
		{"drop table t", nil},

		// cases for IndexLookUp on UK
		{"create table t (a int, b int, unique key(a))", nil},
		{"insert into t values (1,1), (2,2), (3,3), (4,4), (5,5), (6,null)", nil},
		{"select * from t where a=?", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"2"}, true},
			{[]string{"3"}, true},
		}},
		{"select * from t where a in (?,?,?)", []ExecCase{
			{[]string{"1", "1", "1"}, false},
			{[]string{"2", "3", "4"}, true},
			{[]string{"3", "5", "7"}, true},
		}},
		{"select * from t where a>? and a<?", []ExecCase{
			{[]string{"5", "1"}, false},
			{[]string{"1", "4"}, true},
			{[]string{"3", "9"}, true},
		}},
		{"drop table t", nil},

		// cases for IndexLookUp on Index
		{"create table t (a int, b int, key(a))", nil},
		{"insert into t values (1,1), (2,2), (3,3), (4,4), (5,5), (6,null)", nil},
		{"select * from t where a=?", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"2"}, true},
			{[]string{"3"}, true},
		}},
		{"select * from t where a in (?,?,?)", []ExecCase{
			{[]string{"1", "1", "1"}, false},
			{[]string{"2", "3", "4"}, true},
			{[]string{"3", "5", "7"}, true},
		}},
		{"select * from t where a>? and a<?", []ExecCase{
			{[]string{"5", "1"}, false},
			{[]string{"1", "4"}, true},
			{[]string{"3", "9"}, true},
		}},
		{"drop table t", nil},

		// cases for HashJoin
		{"create table t (a int, b int, key(a))", nil},
		{"insert into t values (1,1), (2,2), (3,3), (4,4), (5,5), (6,null)", nil},
		{"select /*+ HASH_JOIN(t1, t2) */ * from t t1, t t2 where t1.a=t2.a and t1.b>?", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"3"}, true},
			{[]string{"5"}, true},
		}},
		{"select /*+ HASH_JOIN(t1, t2) */ * from t t1, t t2 where t1.a=t2.a and t2.b>?", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"3"}, true},
			{[]string{"5"}, true},
		}},
		{"select /*+ HASH_JOIN(t1, t2) */ * from t t1, t t2 where t1.a=t2.a and t1.b>? and t2.b<?", []ExecCase{
			{[]string{"1", "10"}, false},
			{[]string{"3", "5"}, true},
			{[]string{"5", "3"}, true},
		}},

		// cases for MergeJoin
		{"select /*+ MERGE_JOIN(t1, t2) */ * from t t1, t t2 where t1.a=t2.a and t1.b>?", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"3"}, true},
			{[]string{"5"}, true},
		}},
		{"select /*+ MERGE_JOIN(t1, t2) */ * from t t1, t t2 where t1.a=t2.a and t2.b>?", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"3"}, true},
			{[]string{"5"}, true},
		}},
		{"select /*+ MERGE_JOIN(t1, t2) */ * from t t1, t t2 where t1.a=t2.a and t1.b>? and t2.b<?", []ExecCase{
			{[]string{"1", "10"}, false},
			{[]string{"3", "5"}, true},
			{[]string{"5", "3"}, true},
		}},

		// cases for IndexJoin
		{"select /*+ INL_JOIN(t1, t2) */ * from t t1, t t2 where t1.a=t2.a and t1.b>?", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"3"}, true},
			{[]string{"5"}, true},
		}},
		{"select /*+ INL_JOIN(t1, t2) */ * from t t1, t t2 where t1.a=t2.a and t2.b>?", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"3"}, true},
			{[]string{"5"}, true},
		}},
		{"select /*+ INL_JOIN(t1, t2) */ * from t t1, t t2 where t1.a=t2.a and t1.b>? and t2.b<?", []ExecCase{
			{[]string{"1", "10"}, false},
			{[]string{"3", "5"}, true},
			{[]string{"5", "3"}, true},
		}},

		// cases for NestedLoopJoin (Apply)
		{"select * from t t1 where t1.b>? and t1.a > (select min(t2.a) from t t2 where t2.b < t1.b)", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"3"}, false}, // plans with sub-queries cannot be cached, but the result must be correct
			{[]string{"5"}, false},
		}},
		{"select * from t t1 where t1.a > (select min(t2.a) from t t2 where t2.b < t1.b+?)", []ExecCase{
			{[]string{"1"}, false},
			{[]string{"3"}, false},
			{[]string{"5"}, false},
		}},
		{"select * from t t1 where t1.b>? and t1.a > (select min(t2.a) from t t2 where t2.b < t1.b+?)", []ExecCase{
			{[]string{"1", "1"}, false},
			{[]string{"3", "2"}, false},
			{[]string{"5", "3"}, false},
		}},
		{"drop table t", nil},

		// cases for Window
		{"create table t (name varchar(50), y int, sale decimal(14,2))", nil},
		{"insert into t values ('Bob',2016,2.4), ('Bob',2017,3.2), ('Bob',2018,2.1), ('Alice',2016,1.4), ('Alice',2017,2), ('Alice',2018,3.3), ('John',2016,4), ('John',2017,2.1), ('John',2018,5)", nil},
		{"select *, sum(sale) over (partition by y order by sale) total from t where sale>? order by y", []ExecCase{
			{[]string{"0.1"}, false},
			{[]string{"0.5"}, true},
			{[]string{"1.5"}, true},
			{[]string{"3.5"}, true},
		}},
		{"select *, sum(sale) over (partition by y order by sale+? rows 2 preceding) total from t order by y", []ExecCase{
			{[]string{"0.1"}, false},
			{[]string{"0.5"}, true},
			{[]string{"1.5"}, true},
			{[]string{"3.5"}, true},
		}},
		{"select *, rank() over (partition by y order by sale+? rows 2 preceding) total from t order by y", []ExecCase{
			{[]string{"0.1"}, false},
			{[]string{"0.5"}, true},
			{[]string{"1.5"}, true},
			{[]string{"3.5"}, true},
		}},
		{"select *, first_value(sale) over (partition by y order by sale+? rows 2 preceding) total from t order by y", []ExecCase{
			{[]string{"0.1"}, false},
			{[]string{"0.5"}, true},
			{[]string{"1.5"}, true},
			{[]string{"3.5"}, true},
		}},
		{"select *, first_value(sale) over (partition by y order by sale rows ? preceding) total from t order by y", []ExecCase{
			{[]string{"1"}, false}, // window plans with parameters in frame cannot be cached
			{[]string{"2"}, false},
			{[]string{"3"}, false},
			{[]string{"4"}, false},
		}},
		{"drop table t", nil},

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

			// check whether the result is correct
			tmp := strings.Split(prepCase.PrepStmt, "?")
			require.Equal(t, len(execCase.Parameters)+1, len(tmp))
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

func TestIssue28782(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("prepare stmt from 'SELECT IF(?, 1, 0);';")
	tk.MustExec("set @a=1, @b=null, @c=0")

	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @b;").Check(testkit.Rows("0"))
	// TODO(Reminiscent): Support cache more tableDual plan.
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @c;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
}

func TestIssue29101(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec(`CREATE TABLE customer (
	  c_id int(11) NOT NULL,
	  c_d_id int(11) NOT NULL,
	  c_w_id int(11) NOT NULL,
	  c_first varchar(16) DEFAULT NULL,
	  c_last varchar(16) DEFAULT NULL,
	  c_credit char(2) DEFAULT NULL,
	  c_discount decimal(4,4) DEFAULT NULL,
	  PRIMARY KEY (c_w_id,c_d_id,c_id),
	  KEY idx_customer (c_w_id,c_d_id,c_last,c_first)
	)`)
	tk.MustExec(`CREATE TABLE warehouse (
	  w_id int(11) NOT NULL,
	  w_tax decimal(4,4) DEFAULT NULL,
	  PRIMARY KEY (w_id)
	)`)
	tk.MustExec(`prepare s1 from 'SELECT /*+ TIDB_INLJ(customer,warehouse) */ c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?'`)
	tk.MustExec(`set @a=936,@b=7,@c=158`)
	tk.MustQuery(`execute s1 using @a,@b,@c`).Check(testkit.Rows())
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Check(testkit.Rows( // can use IndexJoin
		`Projection_6 1.00 root  test.customer.c_discount, test.customer.c_last, test.customer.c_credit, test.warehouse.w_tax`,
		`└─IndexJoin_14 1.00 root  inner join, inner:TableReader_10, outer key:test.customer.c_w_id, inner key:test.warehouse.w_id, equal cond:eq(test.customer.c_w_id, test.warehouse.w_id)`,
		`  ├─Point_Get_33(Build) 1.00 root table:customer, index:PRIMARY(c_w_id, c_d_id, c_id) `,
		`  └─TableReader_10(Probe) 0.00 root  data:Selection_9`,
		`    └─Selection_9 0.00 cop[tikv]  eq(test.warehouse.w_id, 936)`,
		`      └─TableRangeScan_8 1.00 cop[tikv] table:warehouse range: decided by [test.customer.c_w_id], keep order:false, stats:pseudo`))
	tk.MustQuery(`execute s1 using @a,@b,@c`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1")) // can use the plan-cache

	tk.MustExec(`CREATE TABLE order_line (
	  ol_o_id int(11) NOT NULL,
	  ol_d_id int(11) NOT NULL,
	  ol_w_id int(11) NOT NULL,
	  ol_number int(11) NOT NULL,
	  ol_i_id int(11) NOT NULL,
	  PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number))`)
	tk.MustExec(`CREATE TABLE stock (
	  s_i_id int(11) NOT NULL,
	  s_w_id int(11) NOT NULL,
	  s_quantity int(11) DEFAULT NULL,
	  PRIMARY KEY (s_w_id,s_i_id))`)
	tk.MustExec(`prepare s1 from 'SELECT /*+ TIDB_INLJ(order_line,stock) */ COUNT(DISTINCT (s_i_id)) stock_count FROM order_line, stock  WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id < ? AND ol_o_id >= ? - 20 AND s_w_id = ? AND s_i_id = ol_i_id AND s_quantity < ?'`)
	tk.MustExec(`set @a=391,@b=1,@c=3058,@d=18`)
	tk.MustExec(`execute s1 using @a,@b,@c,@c,@a,@d`)
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Check(testkit.Rows( // can use index-join
		`StreamAgg_9 1.00 root  funcs:count(distinct test.stock.s_i_id)->Column#11`,
		`└─IndexJoin_14 0.03 root  inner join, inner:IndexLookUp_13, outer key:test.order_line.ol_i_id, inner key:test.stock.s_i_id, equal cond:eq(test.order_line.ol_i_id, test.stock.s_i_id)`,
		`  ├─Selection_30(Build) 0.03 root  eq(test.order_line.ol_d_id, 1), eq(test.order_line.ol_w_id, 391), ge(test.order_line.ol_o_id, 3038), lt(test.order_line.ol_o_id, 3058)`,
		`  │ └─IndexLookUp_29 0.03 root  `,
		`  │   ├─IndexRangeScan_27(Build) 0.03 cop[tikv] table:order_line, index:PRIMARY(ol_w_id, ol_d_id, ol_o_id, ol_number) range:[391 1 3038,391 1 3058), keep order:false, stats:pseudo`,
		`  │   └─TableRowIDScan_28(Probe) 0.03 cop[tikv] table:order_line keep order:false, stats:pseudo`,
		`  └─IndexLookUp_13(Probe) 1.00 root  `,
		`    ├─IndexRangeScan_10(Build) 1.00 cop[tikv] table:stock, index:PRIMARY(s_w_id, s_i_id) range: decided by [eq(test.stock.s_i_id, test.order_line.ol_i_id) eq(test.stock.s_w_id, 391)], keep order:false, stats:pseudo`,
		`    └─Selection_12(Probe) 1.00 cop[tikv]  lt(test.stock.s_quantity, 18)`,
		`      └─TableRowIDScan_11 1.00 cop[tikv] table:stock keep order:false, stats:pseudo`))
	tk.MustExec(`execute s1 using @a,@b,@c,@c,@a,@d`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1")) // can use the plan-cache
}

func TestIssue28087And28162(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk := testkit.NewTestKit(t, store)
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
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

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

func TestParameterPushDown(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t (a int, b int, c int, key(a))`)
	tk.MustExec(`insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 6, 6)`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec(`set @x1=1,@x5=5,@x10=10,@x20=20`)

	var input []struct {
		SQL string
	}
	var output []struct {
		Result    []string
		Plan      []string
		FromCache string
	}
	prepareMergeSuiteData.GetTestCases(t, &input, &output)

	for i, tt := range input {
		if strings.HasPrefix(tt.SQL, "execute") {
			res := tk.MustQuery(tt.SQL).Sort()
			fromCache := tk.MustQuery("select @@last_plan_from_cache")
			tk.MustQuery(tt.SQL)
			tkProcess := tk.Session().ShowProcess()
			ps := []*util.ProcessInfo{tkProcess}
			tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
			plan := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))

			testdata.OnRecord(func() {
				output[i].Result = testdata.ConvertRowsToStrings(res.Rows())
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
				output[i].FromCache = fromCache.Rows()[0][0].(string)
			})

			res.Check(testkit.Rows(output[i].Result...))
			plan.Check(testkit.Rows(output[i].Plan...))
			require.Equal(t, fromCache.Rows()[0][0].(string), output[i].FromCache)
		} else {
			tk.MustExec(tt.SQL)
			testdata.OnRecord(func() {
				output[i].Result = nil
			})
		}
	}
}

func TestPreparePlanCache4Function(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")

	// Testing for non-deterministic functions
	tk.MustExec("prepare stmt from 'select rand()';")
	res := tk.MustQuery("execute stmt;")
	require.Equal(t, 1, len(res.Rows()))

	res1 := tk.MustQuery("execute stmt;")
	require.Equal(t, 1, len(res1.Rows()))
	require.NotEqual(t, res.Rows()[0][0], res1.Rows()[0][0])
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// Testing for control functions
	tk.MustExec("prepare stmt from 'SELECT IFNULL(?,0);';")
	tk.MustExec("set @a = 1, @b = null;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @b;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("prepare stmt from 'select a, case when a = ? then 0 when a <=> ? then 1 else 2 end b from t order by a;';")
	tk.MustExec("insert into t values(0), (1), (2), (null);")
	tk.MustExec("set @a = 0, @b = 1, @c = 2, @d = null;")
	tk.MustQuery("execute stmt using @a, @b;").Check(testkit.Rows("<nil> 2", "0 0", "1 1", "2 2"))
	tk.MustQuery("execute stmt using @c, @d;").Check(testkit.Rows("<nil> 1", "0 2", "1 2", "2 0"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
}

func TestPreparePlanCache4DifferentSystemVars(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")

	// Testing for 'sql_select_limit'
	tk.MustExec("set @@sql_select_limit = 1")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(0), (1), (null);")
	tk.MustExec("prepare stmt from 'select a from t order by a;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows("<nil>"))

	tk.MustExec("set @@sql_select_limit = 2")
	tk.MustQuery("execute stmt;").Check(testkit.Rows("<nil>", "0"))
	// The 'sql_select_limit' will be stored in the cache key. So if the `sql_select_limit`
	// have been changed, the plan cache can not be reused.
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	tk.MustExec("set @@sql_select_limit = 18446744073709551615")
	tk.MustQuery("execute stmt;").Check(testkit.Rows("<nil>", "0", "1"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	// test for 'tidb_enable_index_merge'
	tk.MustExec("set @@tidb_enable_index_merge = 1;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int, index idx_a(a), index idx_b(b));")
	tk.MustExec("prepare stmt from 'select * from t use index(idx_a, idx_b) where a > 1 or b > 1;';")
	tk.MustExec("execute stmt;")
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res := tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Equal(t, 4, len(res.Rows()))
	require.Contains(t, res.Rows()[0][0], "IndexMerge")

	tk.MustExec("set @@tidb_enable_index_merge = 0;")
	tk.MustExec("execute stmt;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Equal(t, 4, len(res.Rows()))
	require.Contains(t, res.Rows()[0][0], "IndexMerge")
	tk.MustExec("execute stmt;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	// test for 'tidb_enable_parallel_apply'
	tk.MustExec("set @@tidb_enable_collect_execution_info=1;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (null, null)")

	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustExec("prepare stmt from 'select t1.b from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a);';")
	tk.MustQuery("execute stmt;").Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Contains(t, res.Rows()[1][0], "Apply")
	require.Contains(t, res.Rows()[1][5], "Concurrency")

	tk.MustExec("set tidb_enable_parallel_apply=false")
	tk.MustQuery("execute stmt;").Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Contains(t, res.Rows()[1][0], "Apply")
	executionInfo := fmt.Sprintf("%v", res.Rows()[1][4])
	// Do not use the parallel apply.
	require.False(t, strings.Contains(executionInfo, "Concurrency"))
	tk.MustExec("execute stmt;")
	// The subquery plan can not be cached.
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	// test for apply cache
	tk.MustExec("set @@tidb_enable_collect_execution_info=1;")
	tk.MustExec("set tidb_mem_quota_apply_cache=33554432")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (null, null)")

	tk.MustExec("prepare stmt from 'select t1.b from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a);';")
	tk.MustQuery("execute stmt;").Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Contains(t, res.Rows()[1][0], "Apply")
	require.Contains(t, res.Rows()[1][5], "cache:ON")

	tk.MustExec("set tidb_mem_quota_apply_cache=0")
	tk.MustQuery("execute stmt;").Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Contains(t, res.Rows()[1][0], "Apply")
	executionInfo = fmt.Sprintf("%v", res.Rows()[1][5])
	// Do not use the apply cache.
	require.True(t, strings.Contains(executionInfo, "cache:OFF"))
	tk.MustExec("execute stmt;")
	// The subquery plan can not be cached.
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
}

func TestTemporaryTable4PlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("drop table if exists tmp2")
	tk.MustExec("create temporary table tmp2 (a int, b int, key(a), key(b));")
	tk.MustExec("prepare stmt from 'select * from tmp2;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	tk.MustExec("drop table if exists tmp_t;")
	tk.MustExec("create global temporary table tmp_t (id int primary key, a int, b int, index(a)) on commit delete rows")
	tk.MustExec("prepare stmt from 'select * from tmp_t;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("execute stmt;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

}

func TestPrepareStmtAfterIsolationReadChange(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(false) // requires plan cache disabled
	tk := testkit.NewTestKit(t, store)
	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	// create virtual tiflash replica.
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
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
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Equal(t, "cop[tikv]", rows[len(rows)-1][2])

	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	tk.MustExec("execute stmt")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Equal(t, rows[len(rows)-1][2], "cop[tiflash]")

	require.Equal(t, 1, len(tk.Session().GetSessionVars().PreparedStmts))
	require.Equal(t, "select * from `t`", tk.Session().GetSessionVars().PreparedStmts[1].(*plannercore.CachedPrepareStmt).NormalizedSQL)
	require.Equal(t, "", tk.Session().GetSessionVars().PreparedStmts[1].(*plannercore.CachedPrepareStmt).NormalizedPlan)
}

func TestPreparePC4Binding(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true) // requires plan cache enable
	tk := testkit.NewTestKit(t, store)
	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")

	tk.MustExec("prepare stmt from \"select * from t\"")
	require.Equal(t, 1, len(tk.Session().GetSessionVars().PreparedStmts))
	require.Equal(t, "select * from `test` . `t`", tk.Session().GetSessionVars().PreparedStmts[1].(*plannercore.CachedPrepareStmt).NormalizedSQL4PC)

	tk.MustQuery("execute stmt")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("create binding for select * from t using select * from t")
	res := tk.MustQuery("show session bindings")
	require.Equal(t, 1, len(res.Rows()))

	tk.MustQuery("execute stmt")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
}

func TestIssue31141(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true) // requires plan cache enable
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set @@tidb_txn_mode = 'pessimistic'")

	// No panic here.
	tk.MustExec("prepare stmt1 from 'do 1'")

	tk.MustExec("set @@tidb_txn_mode = 'optimistic'")
	tk.MustExec("prepare stmt1 from 'do 1'")
}
