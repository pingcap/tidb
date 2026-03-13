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

package rule

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
)

func TestOuterToSemiJoin(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists A, B")
		// A.id is not a primary key to allow NULL values for the test.
		tk.MustExec("CREATE TABLE A (id INT, val INT, nullable_val INT)")
		tk.MustExec("CREATE TABLE B (id INT PRIMARY KEY, a_id INT, val INT, non_null_col INT NOT NULL, nullable_col INT)")

		// Insert data into A
		// A.val=10, 20, NULL, 40
		tk.MustExec("INSERT INTO A VALUES (1, 10, 100), (2, 20, NULL), (3, NULL, 300), (4, 40, 400), (NULL, 50, 500)")

		// Insert data into B
		// B.val=10 matches A.val=10.
		// B.val=NULL matches A.val=NULL via <=>.
		// B.val=500 and 600 have no match in A.val.
		tk.MustExec("INSERT INTO B VALUES (101, 1, 10, 1, 1), (102, 2, NULL, 2, NULL), (103, 5, 500, 5, 5), (104, NULL, 600, 6, 6)")

		tk.MustExec("CREATE TABLE t1 (i INT NOT NULL)")
		tk.MustExec("INSERT INTO t1 VALUES (0), (2), (3), (4)")
		tk.MustExec("CREATE TABLE t2 (i INT NOT NULL)")
		tk.MustExec("INSERT INTO t2 VALUES (0), (1), (3), (4)")
		tk.MustExec("CREATE TABLE t3 (i INT NOT NULL)")
		tk.MustExec("INSERT INTO t3 VALUES (0), (1), (2), (4)")
		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
		suite := GetOuterToSemiJoinSuiteData()
		suite.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("EXPLAIN FORMAT='plan_tree' " + sql).Rows())
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			})
			tk.MustQuery("EXPLAIN FORMAT='plan_tree' " + sql).Check(testkit.Rows(output[i].Plan...))
			tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
		}
	})
}

func TestSemiJoinInnerDedup(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Outer table: small, no duplicates on join key.
	tk.MustExec("create table outer_t (id int not null, val int)")
	tk.MustExec("insert into outer_t values (1, 10), (2, 20), (3, 30)")

	// Inner table: many duplicates on join key (oid) — dupRatio must be >= 2.0
	// to pass the SemiJoinInnerDedup threshold.
	tk.MustExec("create table inner_t (id int not null, oid int not null, v int)")
	for i := 0; i < 50; i++ {
		tk.MustExec("insert into inner_t values (?, ?, ?)", i, i%5, i*10)
	}
	tk.MustExec("analyze table outer_t")
	tk.MustExec("analyze table inner_t")

	// Ensure the sysvar is ON so dedup fires (default for new clusters).
	tk.MustExec("set @@tidb_enable_inl_join_inner_multi_pattern = on")

	// Semi-join via EXISTS — should show a HashAgg (dedup) on the inner side.
	tk.MustHavePlan(
		"select * from outer_t where exists (select 1 from inner_t where outer_t.id = inner_t.oid)",
		"HashAgg",
	)

	// Anti-semi-join via NOT EXISTS — should also show dedup.
	tk.MustHavePlan(
		"select * from outer_t where not exists (select 1 from inner_t where outer_t.id = inner_t.oid)",
		"HashAgg",
	)

	// Semi-join via IN — should also show dedup.
	tk.MustHavePlan(
		"select * from outer_t where id in (select oid from inner_t)",
		"HashAgg",
	)

	// Verify correctness of results.
	tk.MustQuery(
		"select id from outer_t where exists (select 1 from inner_t where outer_t.id = inner_t.oid) order by id",
	).Check(testkit.Rows("1", "2", "3"))

	tk.MustQuery(
		"select id from outer_t where not exists (select 1 from inner_t where outer_t.id = inner_t.oid) order by id",
	).Check(testkit.Rows())

	tk.MustQuery(
		"select id from outer_t where id in (select oid from inner_t) order by id",
	).Check(testkit.Rows("1", "2", "3"))

	// Dedup fires regardless of tidb_enable_inl_join_inner_multi_pattern
	// because IndexJoin is enumerated before dedup in exhaustPhysicalPlans.
	tk.MustExec("set @@tidb_enable_inl_join_inner_multi_pattern = off")
	tk.MustHavePlan(
		"select * from outer_t where exists (select 1 from inner_t where outer_t.id = inner_t.oid)",
		"HashAgg",
	)

	// Correctness is preserved with dedup and sysvar OFF.
	tk.MustQuery(
		"select id from outer_t where exists (select 1 from inner_t where outer_t.id = inner_t.oid) order by id",
	).Check(testkit.Rows("1", "2", "3"))
}

func TestSemiJoinRewrite(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")

		tk.MustExec(`create table t1 (id varchar(64) not null,  key(id))`)
		tk.MustExec(`create table t2 (id bigint(20), k int)`)
		tk.MustExec(`insert into t1 values ("1"), ("2"), ("3")`)
		tk.MustExec(`insert into t2 values (1, 1), (2, 0)`)

		// issue:58829
		// the semi_join_rewrite hint can convert the semi-join to inner-join and finally allow the optimizer to choose the IndexJoin
		tk.MustHavePlan(`delete from t1 where t1.id in (select /*+ semi_join_rewrite() */ /* issue:58829 */ cast(id as char) from t2 where k=1)`, "IndexHashJoin")
		tk.MustExec(`delete from t1 where t1.id in (select /*+ semi_join_rewrite() */ cast(id as char) from t2 where k=1)`)
		tk.MustQuery(`select id from t1 order by id`).Check(testkit.Rows("2", "3"))
	})
}
