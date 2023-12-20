// Copyright 2023 PingCAP, Inc.
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

package bindinfo_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// for testing, only returns Original_sql, Bind_sql, Default_db, Status, Source, Type, Sql_digest
func showBinding(tk *testkit.TestKit, showStmt string) [][]interface{} {
	rows := tk.MustQuery(showStmt).Sort().Rows()
	result := make([][]interface{}, len(rows))
	for i, r := range rows {
		result[i] = append(result[i], r[:4]...)
		result[i] = append(result[i], r[8:11]...)
	}
	return result
}

func removeAllBindings(tk *testkit.TestKit, global bool) {
	scope := "session"
	if global {
		scope = "global"
	}
	res := showBinding(tk, fmt.Sprintf("show %v bindings", scope))
	for _, r := range res {
		if r[4] == "builtin" {
			continue
		}
		tk.MustExec(fmt.Sprintf("drop %v binding for sql digest '%v'", scope, r[6]))
	}
	tk.MustQuery(fmt.Sprintf("show %v bindings", scope)).Check(testkit.Rows()) // empty
}

func TestUniversalBindingBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)

	tk1.MustExec(`use test`)
	tk1.MustExec(`create table t (a int, b int, c int, d int, e int, key(a), key(b), key(c), key(d), key(e))`)
	tk1.MustExec(`create database test1`)
	tk1.MustExec(`use test1`)
	tk1.MustExec(`create table t (a int, b int, c int, d int, e int, key(a), key(b), key(c), key(d), key(e))`)
	tk1.MustExec(`create database test2`)
	tk1.MustExec(`use test2`)
	tk1.MustExec(`create table t (a int, b int, c int, d int, e int, key(a), key(b), key(c), key(d), key(e))`)

	for _, hasForStmt := range []bool{true, false} {
		for _, scope := range []string{"", "session", "global"} {
			tk := testkit.NewTestKit(t, store)
			for _, idx := range []string{"a", "b", "c", "d", "e"} {
				tk.MustExec("use test")
				forStmt := "for select * from t"
				if !hasForStmt {
					forStmt = ""
				}
				tk.MustExec(fmt.Sprintf(`create %v universal binding %v using select /*+ use_index(t, %v) */ * from t`, scope, forStmt, idx))
				for _, useDB := range []string{"test", "test1", "test2"} {
					tk.MustExec("use " + useDB)
					for _, testDB := range []string{"", "test.", "test1.", "test2."} {
						tk.MustExec(`set @@tidb_opt_enable_universal_binding=1`) // enabled
						require.True(t, tk.MustUseIndex(fmt.Sprintf("select * from %vt", testDB), idx))
						tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
						require.True(t, tk.MustUseIndex(fmt.Sprintf("select * from %vt", testDB), idx))
						tk.MustQuery(`show warnings`).Check(testkit.Rows())      // no warning
						tk.MustExec(`set @@tidb_opt_enable_universal_binding=0`) // disabled
						tk.MustQuery(fmt.Sprintf("select * from %vt", testDB))
						tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))
					}
				}
			}
		}
	}
}

func TestUniversalDuplicatedBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int, e int, key(a), key(b), key(c), key(d), key(e))`)

	tk.MustExec(`create global universal binding using select * from t`)
	require.Equal(t, showBinding(tk, "show global bindings"),
		[][]interface{}{{"select * from `t`", "SELECT * FROM `t`", "", "enabled", "manual", "u", "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7"}})

	// if duplicated, the old one will be replaced
	tk.MustExec(`create global universal binding using select /*+ use_index(t, a) */ * from t`)
	require.Equal(t, showBinding(tk, "show global bindings"),
		[][]interface{}{{"select * from `t`", "SELECT /*+ use_index(`t` `a`)*/ * FROM `t`", "", "enabled", "manual", "u", "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7"}})

	// if duplicated, the old one will be replaced
	tk.MustExec(`create global universal binding using select /*+ use_index(t, b) */ * from t`)
	require.Equal(t, showBinding(tk, "show global bindings"),
		[][]interface{}{{"select * from `t`", "SELECT /*+ use_index(`t` `b`)*/ * FROM `t`", "", "enabled", "manual", "u", "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7"}})

	// normal bindings don't conflict with universal bindings
	tk.MustExec(`create global binding using select /*+ use_index(t, b) */ * from t`)
	require.Equal(t, showBinding(tk, "show global bindings"),
		[][]interface{}{{"select * from `t`", "SELECT /*+ use_index(`t` `b`)*/ * FROM `t`", "", "enabled", "manual", "u", "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7"},
			{"select * from `test` . `t`", "SELECT /*+ use_index(`t` `b`)*/ * FROM `test`.`t`", "test", "enabled", "manual", "", "8b193b00413fdb910d39073e0d494c96ebf24d1e30b131ecdd553883d0e29b42"}})

	// session bindings don't conflict with global bindings
	tk.MustExec(`create session universal binding using select /*+ use_index(t, c) */ * from t`)
	require.Equal(t, showBinding(tk, "show global bindings"),
		[][]interface{}{{"select * from `t`", "SELECT /*+ use_index(`t` `b`)*/ * FROM `t`", "", "enabled", "manual", "u", "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7"},
			{"select * from `test` . `t`", "SELECT /*+ use_index(`t` `b`)*/ * FROM `test`.`t`", "test", "enabled", "manual", "", "8b193b00413fdb910d39073e0d494c96ebf24d1e30b131ecdd553883d0e29b42"}})
}

func TestUniversalBindingPriority(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`set @@tidb_opt_enable_universal_binding=1`)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int, e int, key(a), key(b), key(c), key(d), key(e))`)

	tk.MustExec(`create global universal binding using select /*+ use_index(t, a) */ * from t`)
	tk.MustUseIndex(`select * from t`, "a")
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))

	// global normal > global universal
	tk.MustExec(`create global binding using select /*+ use_index(t, b) */ * from t`)
	tk.MustUseIndex(`select * from t`, "b")
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))

	// session universal > global normal
	tk.MustExec(`create session universal binding using select /*+ use_index(t, c) */ * from t`)
	tk.MustUseIndex(`select * from t`, "c")
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))

	// global normal takes effect again if disable universal bindings
	tk.MustExec(`set @@tidb_opt_enable_universal_binding=0`)
	tk.MustExec(`create global binding using select /*+ use_index(t, b) */ * from t`)
	tk.MustUseIndex(`select * from t`, "b")
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
	tk.MustExec(`set @@tidb_opt_enable_universal_binding=1`)

	// session normal > session universal
	tk.MustExec(`create session binding using select /*+ use_index(t, d) */ * from t`)
	tk.MustUseIndex(`select * from t`, "d")
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
}

func TestCreateUpdateUniversalBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int)`)

	// drop/show/update binding for sql digest can work for global universal bindings
	tk.MustExec(`create global universal binding using select * from t`)
	require.Equal(t, showBinding(tk, "show global bindings"),
		[][]interface{}{{"select * from `t`", "SELECT * FROM `t`", "", "enabled", "manual", "u", "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7"}})
	require.Equal(t, showBinding(tk, "show global bindings"), [][]interface{}{
		{"select * from `t`", "SELECT * FROM `t`", "", "enabled", "manual", "u", "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7"}})
	tk.MustExec(`set binding disabled for sql digest 'e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7'`)
	require.Equal(t, showBinding(tk, "show global bindings"), [][]interface{}{
		{"select * from `t`", "SELECT * FROM `t`", "", "disabled", "manual", "u", "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7"}})
	tk.MustExec(`set binding enabled for sql digest 'e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7'`)
	require.Equal(t, showBinding(tk, "show global bindings"), [][]interface{}{
		{"select * from `t`", "SELECT * FROM `t`", "", "enabled", "manual", "u", "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7"}})
	tk.MustExec(`drop global binding for sql digest 'e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7'`)
	require.Equal(t, showBinding(tk, "show global bindings"), [][]interface{}{})

	// drop/show/update binding for sql digest can work for session universal bindings
	tk.MustExec(`create session universal binding using select * from t`)
	require.Equal(t, showBinding(tk, "show session bindings"),
		[][]interface{}{{"select * from `t`", "SELECT * FROM `t`", "", "enabled", "manual", "u", "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7"}})
	require.Equal(t, showBinding(tk, "show session bindings"), [][]interface{}{
		{"select * from `t`", "SELECT * FROM `t`", "", "enabled", "manual", "u", "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7"}})
	tk.MustExec(`drop session binding for sql digest 'e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7'`)
	require.Equal(t, showBinding(tk, "show session bindings"), [][]interface{}{})
}

func TestUniversalBindingSwitch(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)

	tk1.MustExec(`use test`)
	tk1.MustExec(`create table t (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
	tk1.MustExec(`create database test1`)

	// switch can work for both global and session universal bindings
	// test for session bindings
	tk1.MustExec(`create session universal binding using select /*+ use_index(t, b) */ * from t`)
	tk1.MustExec(`use test1`)
	tk1.MustQuery(`select * from test.t`).Check(testkit.Rows())
	tk1.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))
	tk1.MustExec(`set @@tidb_opt_enable_universal_binding=1`)
	tk1.MustUseIndex(`select * from test.t`, "b")
	tk1.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
	tk1.MustExec(`set @@tidb_opt_enable_universal_binding=0`)
	tk1.MustQuery(`select * from test.t`).Check(testkit.Rows())
	tk1.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))

	// test for global bindings
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec(`use test1`)
	tk2.MustExec(`create global universal binding using select /*+ use_index(t, b) */ * from t`)
	tk2.MustQuery(`select * from test.t`).Check(testkit.Rows())
	tk2.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))
	tk2.MustExec(`set @@tidb_opt_enable_universal_binding=1`)
	tk2.MustUseIndex(`select * from test.t`, "b")
	tk2.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
	tk2.MustExec(`set @@tidb_opt_enable_universal_binding=0`)
	tk2.MustQuery(`select * from test.t`).Check(testkit.Rows())
	tk2.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))

	// the default value is off
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustQuery(`select @@tidb_opt_enable_universal_binding`).Check(testkit.Rows("0"))
	tk3.MustQuery(`show session variables like 'tidb_opt_enable_universal_binding'`).Check(testkit.Rows("tidb_opt_enable_universal_binding OFF"))
	tk3.MustQuery(`show global variables like 'tidb_opt_enable_universal_binding'`).Check(testkit.Rows("tidb_opt_enable_universal_binding OFF"))
}

func TestUniversalBindingSetVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, key(a), key(b))`)
	tk.MustExec(`create universal binding using select /*+ use_index(t, a) */ * from t`)

	tk.MustExec(`set @@tidb_opt_enable_universal_binding=0`)
	tk.MustExec(`select * from t`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))
	tk.MustExec(`select /*+ set_var(tidb_opt_enable_universal_binding=1) */ * from t`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
	tk.MustExec(`select /*+ set_var(tidb_opt_enable_universal_binding=0) */ * from t`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))

	tk.MustExec(`set @@tidb_opt_enable_universal_binding=1`)
	tk.MustExec(`select * from t`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
	tk.MustExec(`select /*+ set_var(tidb_opt_enable_universal_binding=0) */ * from t`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))
	tk.MustExec(`select /*+ set_var(tidb_opt_enable_universal_binding=1) */ * from t`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
}

func TestUniversalBindingDBInHints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
	tk.MustExec(`create table t2 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
	tk.MustExec(`create table t3 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)

	tk.MustExec(`create universal binding using select /*+ use_index(test.t, a) */ * from t`)
	tk.MustExec(`create universal binding using select /*+ leading(t1, test.t2) */ * from t1, t2 where t1.a=t2.a`)
	tk.MustExec(`create universal binding using select /*+ leading(test.t1, test.t2, test.t3) */ * from t1, t2 where t1.a=t2.a and t2.b=t3.b`)

	// use_index(test.t, a) --> use_index(t, a)
	// leading(t1, test.t2) --> leading(t1, t2)
	// leading(test.t1, test.t2, test.t3) --> leading(t1, t2, t3)
	rs := showBinding(tk, "show bindings")
	require.Equal(t, len(rs), 3)
	require.Equal(t, rs[0][1], "SELECT /*+ leading(`t1`, `t2`)*/ * FROM (`t1`) JOIN `t2` WHERE `t1`.`a` = `t2`.`a`")
	require.Equal(t, rs[1][1], "SELECT /*+ leading(`t1`, `t2`, `t3`)*/ * FROM (`t1`) JOIN `t2` WHERE `t1`.`a` = `t2`.`a` AND `t2`.`b` = `t3`.`b`")
	require.Equal(t, rs[2][1], "SELECT /*+ use_index(`t` `a`)*/ * FROM `t`")
}

func TestUniversalBindingGC(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)

	tk.MustExec(`create global universal binding using select /*+ use_index(t, b) */ * from t`)
	require.Equal(t, showBinding(tk, "show global bindings"),
		[][]interface{}{{"select * from `t`", "SELECT /*+ use_index(`t` `b`)*/ * FROM `t`", "", "enabled", "manual", "u", "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7"}})
	tk.MustExec(`drop global binding for sql digest 'e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7'`)
	require.Equal(t, showBinding(tk, "show global bindings"), [][]interface{}{}) // empty
	tk.MustQuery(`select bind_sql, status, type from mysql.bind_info where source != 'builtin'`).Check(
		testkit.Rows("SELECT /*+ use_index(`t` `b`)*/ * FROM `t` deleted u")) // status=deleted

	updateTime := time.Now().Add(-(15 * bindinfo.Lease))
	updateTimeStr := types.NewTime(types.FromGoTime(updateTime), mysql.TypeTimestamp, 3).String()
	tk.MustExec(fmt.Sprintf("update mysql.bind_info set update_time = '%v' where source != 'builtin'", updateTimeStr))
	bindHandle := bindinfo.NewGlobalBindingHandle(&mockSessionPool{tk.Session()})
	require.NoError(t, bindHandle.GCGlobalBinding())
	tk.MustQuery(`select bind_sql, status, type from mysql.bind_info where source != 'builtin'`).Check(testkit.Rows()) // empty after GC
}

func TestUniversalBindingHints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	for _, db := range []string{"db1", "db2", "db3"} {
		tk.MustExec(`create database ` + db)
		tk.MustExec(`use ` + db)
		tk.MustExec(`create table t1 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
		tk.MustExec(`create table t2 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
		tk.MustExec(`create table t3 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
	}
	tk.MustExec(`set @@tidb_opt_enable_universal_binding=1`)

	for _, c := range []struct {
		binding   string
		qTemplate string
	}{
		// use index
		{`create universal binding using select /*+ use_index(t1, c) */ * from t1 where a=1`,
			`select * from %st1 where a=1000`},
		{`create universal binding using select /*+ use_index(t1, c) */ * from t1 where d<1`,
			`select * from %st1 where d<10000`},
		{`create universal binding using select /*+ use_index(t1, c) */ * from t1, t2 where t1.d<1`,
			`select * from %st1, t2 where t1.d<100`},
		{`create universal binding using select /*+ use_index(t1, c) */ * from t1, t2 where t1.d<1`,
			`select * from t1, %st2 where t1.d<100`},
		{`create universal binding using select /*+ use_index(t1, c), use_index(t2, a) */ * from t1, t2 where t1.d<1`,
			`select * from %st1, t2 where t1.d<100`},
		{`create universal binding using select /*+ use_index(t1, c), use_index(t2, a) */ * from t1, t2 where t1.d<1`,
			`select * from t1, %st2 where t1.d<100`},
		{`create universal binding using select /*+ use_index(t1, c), use_index(t2, a) */ * from t1, t2, t3 where t1.d<1`,
			`select * from %st1, t2, t3 where t1.d<100`},
		{`create universal binding using select /*+ use_index(t1, c), use_index(t2, a) */ * from t1, t2, t3 where t1.d<1`,
			`select * from t1, t2, %st3 where t1.d<100`},

		// ignore index
		{`create universal binding using select /*+ ignore_index(t1, b) */ * from t1 where b=1`,
			`select * from %st1 where b=1000`},
		{`create universal binding using select /*+ ignore_index(t1, b) */ * from t1 where b>1`,
			`select * from %st1 where b>1000`},
		{`create universal binding using select /*+ ignore_index(t1, b) */ * from t1 where b in (1,2)`,
			`select * from %st1 where b in (1)`},
		{`create universal binding using select /*+ ignore_index(t1, b) */ * from t1 where b in (1,2)`,
			`select * from %st1 where b in (1,2,3,4,5)`},

		// order index hint
		{`create universal binding using select /*+ order_index(t1, a) */ a from t1 where a<10 order by a limit 10`,
			`select a from %st1 where a<10000 order by a limit 10`},
		{`create universal binding using select /*+ order_index(t1, b) */ b from t1 where b>10 order by b limit 1111`,
			`select b from %st1 where b>2 order by b limit 10`},

		// no order index hint
		{`create universal binding using select /*+ no_order_index(t1, c) */ c from t1 where c<10 order by c limit 10`,
			`select c from %st1 where c<10000 order by c limit 10`},
		{`create universal binding using select /*+ no_order_index(t1, d) */ d from t1 where d>10 order by d limit 1111`,
			`select d from %st1 where d>2 order by d limit 10`},

		// agg hint
		{`create universal binding using select /*+ hash_agg() */ count(*) from t1 group by a`,
			`select count(*) from %st1 group by a`},
		{`create universal binding using select /*+ stream_agg() */ count(*) from t1 group by b`,
			`select count(*) from %st1 group by b`},

		// to_cop hint
		{`create universal binding using select /*+ agg_to_cop() */ sum(a) from t1`,
			`select sum(a) from %st1`},
		{`create universal binding using select /*+ limit_to_cop() */ a from t1 limit 10`,
			`select a from %st1 limit 101`},

		// index merge hint
		{`create universal binding using select /*+ use_index_merge(t1, c, d) */ * from t1 where c=1 or d=1`,
			`select * from %st1 where c=1000 or d=1000`},
		{`create universal binding using select /*+ no_index_merge() */ * from t1 where a=1 or b=1`,
			`select * from %st1 where a=1000 or b=1000`},

		// join type hint
		{`create universal binding using select /*+ hash_join(t1) */ * from t1, t2 where t1.a=t2.a`,
			`select * from %st1, t2 where t1.a=t2.a`},
		{`create universal binding using select /*+ hash_join(t2) */ * from t1, t2 where t1.a=t2.a`,
			`select * from t1, %st2 where t1.a=t2.a`},
		{`create universal binding using select /*+ hash_join(t2) */ * from t1, t2, t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create universal binding using select /*+ hash_join_build(t1) */ * from t1, t2 where t1.a=t2.a`,
			`select * from t1, %st2 where t1.a=t2.a`},
		{`create universal binding using select /*+ hash_join_probe(t1) */ * from t1, t2 where t1.a=t2.a`,
			`select * from t1, %st2 where t1.a=t2.a`},
		{`create universal binding using select /*+ merge_join(t1) */ * from t1, t2 where t1.a=t2.a`,
			`select * from %st1, t2 where t1.a=t2.a`},
		{`create universal binding using select /*+ merge_join(t2) */ * from t1, t2 where t1.a=t2.a`,
			`select * from t1, %st2 where t1.a=t2.a`},
		{`create universal binding using select /*+ merge_join(t2) */ * from t1, t2, t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create universal binding using select /*+ inl_join(t1) */ * from t1, t2 where t1.a=t2.a`,
			`select * from %st1, t2 where t1.a=t2.a`},
		{`create universal binding using select /*+ inl_join(t2) */ * from t1, t2 where t1.a=t2.a`,
			`select * from t1, %st2 where t1.a=t2.a`},
		{`create universal binding using select /*+ inl_join(t2) */ * from t1, t2, t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},

		// no join type hint
		{`create universal binding using select /*+ no_hash_join(t1) */ * from t1, t2 where t1.b=t2.b`,
			`select * from %st1, t2 where t1.b=t2.b`},
		{`create universal binding using select /*+ no_hash_join(t2) */ * from t1, t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create universal binding using select /*+ no_hash_join(t2) */ * from t1, t2, t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create universal binding using select /*+ no_merge_join(t1) */ * from t1, t2 where t1.b=t2.b`,
			`select * from %st1, t2 where t1.b=t2.b`},
		{`create universal binding using select /*+ no_merge_join(t2) */ * from t1, t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create universal binding using select /*+ no_merge_join(t2) */ * from t1, t2, t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create universal binding using select /*+ no_index_join(t1) */ * from t1, t2 where t1.b=t2.b`,
			`select * from %st1, t2 where t1.b=t2.b`},
		{`create universal binding using select /*+ no_index_join(t2) */ * from t1, t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create universal binding using select /*+ no_index_join(t2) */ * from t1, t2, t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},

		// join order hint
		{`create universal binding using select /*+ leading(t2) */ * from t1, t2 where t1.b=t2.b`,
			`select * from %st1, t2 where t1.b=t2.b`},
		{`create universal binding using select /*+ leading(t2) */ * from t1, t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create universal binding using select /*+ leading(t2, t1) */ * from t1, t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create universal binding using select /*+ leading(t1, t2) */ * from t1, t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create universal binding using select /*+ leading(t1) */ * from t1, t2, t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create universal binding using select /*+ leading(t2) */ * from t1, t2, t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create universal binding using select /*+ leading(t2,t3) */ * from t1, t2, t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create universal binding using select /*+ leading(t2,t3,t1) */ * from t1, t2, t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
	} {
		removeAllBindings(tk, false)
		tk.MustExec(c.binding)
		for _, currentDB := range []string{"db1", "db2", "db3"} {
			tk.MustExec(`use ` + currentDB)
			for _, db := range []string{"db1.", "db2.", "db3.", ""} {
				query := fmt.Sprintf(c.qTemplate, db)
				tk.MustExec(query)
				tk.MustQuery(`show warnings`).Check(testkit.Rows()) // no warning
				tk.MustExec(query)
				tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
			}
		}
	}
}
