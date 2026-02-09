// Copyright 2025 PingCAP, Inc.
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

package core_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestSetVarTimestampHintsWorks(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	// test that the set value is preserved
	tk.MustExec(`set timestamp=1745862208.446495;`)
	require.Equal(t, "1745862208.446495", tk.MustQuery(`select @@timestamp;`).Rows()[0][0].(string))
	require.Equal(t, "1", tk.MustQuery(`select /*+ set_var(timestamp=1) */ @@timestamp;`).Rows()[0][0].(string))
	require.Equal(t, "1745862208.446495", tk.MustQuery(`select @@timestamp;`).Rows()[0][0].(string))
}

func TestSetVarTimestampHintsWorksWithBindings(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create session binding for select @@timestamp + 41 using select /*+ set_var(timestamp=1) */ @@timestamp + 41;`)

	// test that bindings with hints correctly restore the previous non-default timestamp value
	tk.MustExec(`set timestamp=1745862208.446495;`)
	require.Equal(t, "1745862208.446495", tk.MustQuery(`select @@timestamp;`).Rows()[0][0].(string))
	require.Equal(t, "42", tk.MustQuery(`select @@timestamp + 41;`).Rows()[0][0].(string))
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	require.Equal(t, "1745862208.446495", tk.MustQuery(`select @@timestamp;`).Rows()[0][0].(string))
}

func TestSetVarInQueriesAndBindingsWorkTogether(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	tk.MustExec(`set @@max_execution_time=2000;`)
	tk.MustExec(`create table foo (a int);`)
	tk.MustExec(`create session binding for select * from foo where a = 1 using select /*+ set_var(max_execution_time=1234) */ * from foo where a = 1;`)
	tk.MustExec(`select /*+ set_var(max_execution_time=2222) */ * from foo where a = 1;`)
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	tk.MustQuery("select @@max_execution_time;").Check(testkit.Rows("2000"))
}

func TestSetVarHintsWithExplain(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	tk.MustExec(`set @@max_execution_time=2000;`)
	tk.MustExec(`explain select /*+ set_var(max_execution_time=100) */ @@max_execution_time;`)
	tk.MustQuery("select @@max_execution_time;").Check(testkit.Rows("2000"))

	tk.MustExec(`create table t(a int);`)
	tk.MustExec(`create global binding for select * from t where a = 1 and sleep(0.1) using select /*+ SET_VAR(max_execution_time=500) */ * from t where a = 1 and sleep(0.1);`)
	tk.MustExec(`select * from t where a = 1 and sleep(0.1);`)
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	tk.MustQuery("select @@max_execution_time;").Check(testkit.Rows("2000"))

	tk.MustExec(`explain select * from t where a = 1 and sleep(0.1);`)
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	tk.MustQuery("select @@max_execution_time;").Check(testkit.Rows("2000"))
}
