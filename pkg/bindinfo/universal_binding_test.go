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

	"github.com/pingcap/tidb/pkg/testkit"
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
						tk.MustUseIndex(fmt.Sprintf("select * from %vt", testDB), idx)
						tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
						tk.MustExec(`set @@tidb_opt_enable_universal_binding=0`) // disabled
						tk.MustUseIndex(fmt.Sprintf("select * from %vt", testDB), idx)
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
