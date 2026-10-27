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

package core_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestNullsOrderResults(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, a int, b int not null, key ia(a), key iab(a, b))")
	tk.MustExec("insert into t values (1, 2, 10), (2, null, 20), (3, 1, 30), (4, null, 40), (5, 3, 50)")

	tk.MustQuery("select id from t order by a").Check(testkit.Rows("2", "4", "3", "1", "5"))
	tk.MustQuery("select id from t order by a nulls first, id").Check(testkit.Rows("2", "4", "3", "1", "5"))
	tk.MustQuery("select id from t order by a nulls last, id").Check(testkit.Rows("3", "1", "5", "2", "4"))
	tk.MustQuery("select id from t order by a asc nulls last, id").Check(testkit.Rows("3", "1", "5", "2", "4"))
	tk.MustQuery("select id from t order by a desc nulls first, id").Check(testkit.Rows("2", "4", "5", "1", "3"))
	tk.MustQuery("select id from t order by a desc nulls last, id").Check(testkit.Rows("5", "1", "3", "2", "4"))
	tk.MustQuery("select id from t order by a desc, id").Check(testkit.Rows("5", "1", "3", "2", "4"))

	// TopN, including when pushed down to the coprocessor.
	tk.MustQuery("select id from t order by a nulls last, id limit 4").Check(testkit.Rows("3", "1", "5", "2"))
	tk.MustQuery("select id from t order by a desc nulls first, id limit 3").Check(testkit.Rows("2", "4", "5"))
	tk.MustQuery("select id from t order by a desc nulls first, id limit 2, 2").Check(testkit.Rows("5", "1"))

	// Expressions, positions and aliases.
	tk.MustQuery("select id, a + 1 as x from t order by x nulls last, id").Check(testkit.Rows("3 2", "1 3", "5 4", "2 <nil>", "4 <nil>"))
	tk.MustQuery("select id, a from t order by 2 nulls last, 1").Check(testkit.Rows("3 1", "1 2", "5 3", "2 <nil>", "4 <nil>"))

	// The clause is ignored silently for expressions that cannot be NULL.
	tk.MustQuery("select id from t order by b desc nulls first").Check(testkit.Rows("5", "4", "3", "2", "1"))
	tk.MustQuery("explain format = 'brief' select id from t order by b nulls last").CheckNotContain("isnull")
	tk.MustQuery("explain format = 'brief' select id from t order by id nulls last").CheckNotContain("isnull")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	// ... but not for NOT NULL columns on the inner side of an outer join.
	tk.MustQuery("select t1.id, t2.b from t t1 left join t t2 on t1.id = t2.id + 3 order by t2.b nulls last, t1.id").
		Check(testkit.Rows("4 10", "5 20", "1 <nil>", "2 <nil>", "3 <nil>"))

	// Set operations, DELETE and UPDATE.
	tk.MustQuery("(select a from t where id < 3) union all (select a from t where id >= 3) order by a nulls last").
		Check(testkit.Rows("1", "2", "3", "<nil>", "<nil>"))
	tk.MustQuery("select group_concat(id order by a nulls last, id) from t").Check(testkit.Rows("3,1,5,2,4"))
	tk.MustQuery("select group_concat(id order by a desc nulls first, id) from t").Check(testkit.Rows("2,4,5,1,3"))
	tk.MustExec("update t set b = b + 1 order by a nulls last limit 3")
	tk.MustQuery("select id from t where b % 10 = 1 order by id").Check(testkit.Rows("1", "3", "5"))
	tk.MustExec("delete from t order by a desc nulls first limit 2")
	tk.MustQuery("select id from t order by id").Check(testkit.Rows("1", "3", "5"))

	// Window functions.
	tk.MustExec("insert into t values (2, null, 20), (4, null, 40)")
	tk.MustQuery("select id, row_number() over (order by a nulls last, id) from t order by id").
		Check(testkit.Rows("1 2", "2 4", "3 1", "4 5", "5 3"))
	tk.MustQuery("select id, rank() over (order by a desc nulls first) from t order by id").
		Check(testkit.Rows("1 4", "2 1", "3 5", "4 1", "5 3"))
	tk.MustQuery("select id, sum(id) over (order by a nulls last range between unbounded preceding and current row) from t order by id").
		Check(testkit.Rows("1 4", "2 15", "3 3", "4 15", "5 9"))
	tk.MustGetErrMsg("select sum(id) over (order by a nulls last range between 1 preceding and current row) from t",
		"[planner:1235]This version of TiDB doesn't yet support 'NULLS FIRST/LAST in the ORDER BY of a RANGE frame with an offset'")
	tk.MustQuery("select sum(id) over (order by b nulls last range between 1 preceding and current row) from t order by id").
		Check(testkit.Rows("1", "2", "3", "4", "5"))

	// NULLS FIRST/LAST only applies to ORDER BY.
	tk.MustGetErrMsg("select a from t group by a nulls last",
		"[planner:1235]This version of TiDB doesn't yet support 'NULLS FIRST/LAST in GROUP BY'")
	tk.MustGetErrMsg("select row_number() over (partition by a nulls last) from t",
		"[planner:1235]This version of TiDB doesn't yet support 'NULLS FIRST/LAST in PARTITION BY'")
}
