// Copyright 2026 PingCAP, Inc.
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

package windows_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestWindowExecutorsBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values (1,1),(1,2),(2,1),(2,2)")
	tk.MustExec("set @@tidb_enable_window_function = 1")
	defer tk.MustExec("set @@tidb_enable_window_function = 0")

	for _, pipelined := range []int{0, 1} {
		tk.MustExec(fmt.Sprintf("set @@tidb_enable_pipelined_window_function = %d", pipelined))
		// The window's ORDER BY only defines numbering inside each partition. Add an
		// outer ORDER BY so the result set order is deterministic across executors.
		tk.MustQuery("select a, row_number() over(partition by a order by b) as rn from t order by a, rn").
			Check(testkit.Rows("1 1", "1 2", "2 1", "2 2"))
		tk.MustQuery("select a, sum(b) over(order by a, b rows between 1 preceding and current row) as s from t order by a, b").
			Check(testkit.Rows("1 1", "1 3", "2 3", "2 3"))
	}
}

func TestWindowReturnColumnNullableAttribute(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_window_function = 1")
	defer tk.MustExec("set @@tidb_enable_window_function = 0")
	tk.MustExec("drop table if exists agg")
	tk.MustExec("create table agg(p int not null, o int not null, v int not null)")
	tk.MustExec("insert into agg values (0,0,1), (1,1,2), (1,2,3), (1,3,4)")

	checkNullable := func(funcName string, isNullable bool) {
		rs, err := tk.Exec(fmt.Sprintf("select %s over (partition by p order by o rows between 1 preceding and 1 following) as a from agg", funcName))
		tk.RequireNoError(err)
		retField := rs.Fields()[0]
		if isNullable {
			tk.RequireNotEqual(mysql.NotNullFlag, retField.Column.FieldType.GetFlag()&mysql.NotNullFlag)
		} else {
			tk.RequireEqual(mysql.NotNullFlag, retField.Column.FieldType.GetFlag()&mysql.NotNullFlag)
		}
		tk.RequireNoError(rs.Close())
	}

	checkNullable("sum(v)", true)
	checkNullable("count(v)", false)
	checkNullable("row_number()", false)
	checkNullable("rank()", false)
	checkNullable("dense_rank()", false)
}
