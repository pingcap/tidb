// Copyright 2024 PingCAP, Inc.
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

package parallelapply

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestParallelApplyWarnning(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, c int);")
	tk.MustExec("create table t2 (a int, b int, c int, key(a));")
	tk.MustExec("create table t3(a int, b int, c int, key(a));")
	tk.MustExec("set tidb_enable_parallel_apply=on;")
	tk.MustQuery("select (select 1 from t2, t3 where t2.a=t3.a and t2.b > t1.b) from t1;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Some apply operators can not be executed in parallel: *core.PhysicalIndexHashJoin doesn't support cloning"))
}
