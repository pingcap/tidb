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
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	txninfo "github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/testkit"
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

func TestPreparedStmtWithHint(t *testing.T) {
	// see https://github.com/pingcap/tidb/issues/18535
	store, dom, err := newStoreWithBootstrap()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
		dom.Close()
	}()

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

func TestIssue29850(t *testing.T) {
	store, dom, err := newStoreWithBootstrap()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
		dom.Close()
	}()

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
	tk.Session().SetSessionManager(&mockSessionManager1{PS: ps})
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
	tk.Session().SetSessionManager(&mockSessionManager1{PS: ps})
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
	tk.Session().SetSessionManager(&mockSessionManager1{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Check(testkit.Rows( // cannot use PointGet since it contains a or condition
		`Selection_7 1.00 root  or(eq(test.t.a, 1), eq(test.t.a, 1))`,
		`└─TableReader_6 1.00 root  data:TableRangeScan_5`,
		`  └─TableRangeScan_5 1.00 cop[tikv] table:t range:[1,1], keep order:false, stats:pseudo`))
	tk.MustQuery(`execute stmt using @a1, @a2`).Check(testkit.Rows("1", "2"))
}
