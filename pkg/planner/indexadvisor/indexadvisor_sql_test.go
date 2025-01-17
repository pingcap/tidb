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

package indexadvisor_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func checkResult(t *testing.T, tk *testkit.TestKit, stmts, indexes string) {
	sql := fmt.Sprintf(`recommend index run for '%v'`, stmts)
	results := tk.MustQuery(sql).Rows()
	resultIndexes := make([]string, 0, len(results))
	for _, r := range results {
		resultIndexes = append(resultIndexes, fmt.Sprintf("%v.%v.%v", r[0], r[1], r[3]))
	}
	sort.Strings(resultIndexes)
	require.Equal(t, indexes, strings.Join(resultIndexes, "|"))
}

func TestIndexAdvisorForSQL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int)`)

	checkResult(t, tk, `select a from t where a=1`, "test.t.a")
	checkResult(t, tk, `select a from t where a=1 and b=1`, "test.t.a,b")
	checkResult(t, tk, `select a from t where a=1 and c=1`, "test.t.a,c")
}

func TestIndexAdvisorForMultipleTables(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, c int)`)
	tk.MustExec(`create table t2 (a int, b int, c int)`)
	checkResult(t, tk, `select * from t1 where a=1;select * from t2 where b=1`,
		"test.t1.a|test.t2.b")
	checkResult(t, tk, `select * from t1, t2 where t1.a=t2.a and t1.b=1`,
		"test.t1.b|test.t2.a")

	sqls := make([]string, 0, 20)
	for i := 1; i <= 20; i++ {
		sqls = append(sqls, fmt.Sprintf("select * from t%v where a=1", i))
		if i >= 3 {
			tk.MustExec(fmt.Sprintf("create table t%v (a int, b int, c int)", i))
		}
	}
	checkResult(t, tk, strings.Join(sqls, ";"), "test.t1.a|test.t10.a|test.t11.a|test.t12.a|test.t13.a")
}

func TestIndexAdvisorForVariousTypes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b varchar(32), c float, d datetime, e decimal(20,2))`)
	checkResult(t, tk, `select * from t where a=1`, "test.t.a")
	checkResult(t, tk, `select * from t where b="1"`, "test.t.b")
	checkResult(t, tk, `select * from t where c=1.0`, "test.t.c")
	checkResult(t, tk, `select * from t where d=now()`, `test.t.d`)
	checkResult(t, tk, `select * from t where e=1.0`, "test.t.e")

	checkResult(t, tk, `select * from t where a=1 and b="1"`, "test.t.a,b")
	checkResult(t, tk, `select * from t where a=1 and c=1.0`, "test.t.a,c")
	checkResult(t, tk, `select * from t where b="1" and c=1.0`, "test.t.c,b")
	checkResult(t, tk, `select * from t where d=now() and b="1"`, "test.t.d,b")
}
