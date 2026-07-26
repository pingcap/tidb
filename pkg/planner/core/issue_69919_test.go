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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestIssue69919 verifies that `expr IN (subquery) OR FALSE` produces the same
// plan and results as bare `expr IN (subquery)`. Without the fix, the OR FALSE
// wrapper forces asScalar=true which blocks the InnerJoin+Distinct rewrite,
// causing the build phase to pick SemiApply (LeftOuterSemiJoin) instead.
// The PredicateSimplification rule runs too late to undo this decision.
func TestIssue69919(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t2")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("insert into t values (1), (2), (3), (4), (5)")
	tk.MustExec("insert into t2 values (1), (2), (3)")

	// Disable correlated subquery mode to test the base rewrite path deterministically.
	tk.MustExec("set session tidb_opt_enable_correlated_subquery = off")

	type testCase struct {
		name     string
		query    string
		refQuery string
	}
	cases := []testCase{
		{
			"IN OR FALSE",
			"select * from t where (a in (select a from t2)) or false",
			"select * from t where a in (select a from t2)",
		},
		{
			"FALSE OR IN",
			"select * from t where false or (a in (select a from t2))",
			"select * from t where a in (select a from t2)",
		},
		{
			"parenthesized IN OR FALSE",
			"select * from t where ((a in (select a from t2)) or false)",
			"select * from t where a in (select a from t2)",
		},
		{
			"IN OR FALSE AND extra condition",
			"select * from t where ((a in (select a from t2)) or false) and a > 0",
			"select * from t where (a in (select a from t2)) and a > 0",
		},
		{
			"IN OR FALSE in HAVING",
			"select a from t group by a having (a in (select a from t2)) or false",
			"select a from t group by a having a in (select a from t2)",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Compare plan operator name (col 0) and operator info (col 4, e.g. join mode).
			refRows := tk.MustQuery("explain format='brief' " + c.refQuery).Rows()
			planRows := tk.MustQuery("explain format='brief' " + c.query).Rows()

			require.Equal(t, len(refRows), len(planRows),
				"plan line count mismatch for %s", c.query)
			for i := range refRows {
				require.Equal(t, fmt.Sprintf("%v", refRows[i][0]), fmt.Sprintf("%v", planRows[i][0]),
					"plan operator mismatch at line %d for %s", i, c.query)
				require.Equal(t, fmt.Sprintf("%v", refRows[i][4]), fmt.Sprintf("%v", planRows[i][4]),
					"plan operator info mismatch at line %d for %s", i, c.query)
			}

			// Compare results.
			refRes := tk.MustQuery(c.refQuery).Sort().Rows()
			res := tk.MustQuery(c.query).Sort().Rows()
			require.Equal(t, refRes, res)
		})
	}
}

// TestIssue69919_NullIsNotFalse verifies that NULL is never treated as FALSE.
func TestIssue69919_NullIsNotFalse(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (1), (2), (3)")

	// (a=1 OR NULL) OR FALSE = a=1 OR NULL → 1 OR NULL = 1 → row returned.
	rows := tk.MustQuery("select * from t where ((a = 1 or null) or false)").Sort().Rows()
	require.Equal(t, 1, len(rows))
	require.Equal(t, "1", fmt.Sprintf("%v", rows[0][0]))

	// (a=2 AND NULL) = NULL (falsy), OR FALSE = NULL OR FALSE = NULL (still falsy) → no row.
	rows = tk.MustQuery("select * from t where ((a = 2 and null) or false)").Sort().Rows()
	require.Equal(t, 0, len(rows))

	// Verify projected NULL (not FALSE) — SELECT exposes NULL semantics that WHERE hides.
	// (a = 1) OR NULL → 1 OR NULL = 1; (a = 999) OR NULL → NULL
	rows = tk.MustQuery("select a, (a = 1) or null from t order by a").Rows()
	require.Equal(t, 3, len(rows))
	require.Equal(t, "1", fmt.Sprintf("%v", rows[0][0]))
	require.Equal(t, "1", fmt.Sprintf("%v", rows[0][1]))
	require.Equal(t, "2", fmt.Sprintf("%v", rows[1][0]))
	require.Equal(t, "<nil>", fmt.Sprintf("%v", rows[1][1]))
	require.Equal(t, "3", fmt.Sprintf("%v", rows[2][0]))
	require.Equal(t, "<nil>", fmt.Sprintf("%v", rows[2][1]))
}

// TestIssue69919_SelectList verifies SELECT-list subqueries with OR FALSE retain
// correct scalar behavior (the fix only applies in filter contexts).
func TestIssue69919_SelectList(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t2")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("insert into t values (1), (2), (3)")
	tk.MustExec("insert into t2 values (2), (3)")

	ref := tk.MustQuery("select a, a in (select a from t2) from t order by a").Rows()
	got := tk.MustQuery("select a, (a in (select a from t2)) or false from t order by a").Rows()
	require.Equal(t, ref, got)
}

// TestIssue69919_NotIn verifies NOT IN with OR FALSE is unchanged.
func TestIssue69919_NotIn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t2")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("insert into t values (1), (2), (3)")
	tk.MustExec("insert into t2 values (2)")

	ref := tk.MustQuery("select * from t where a not in (select a from t2)").Sort().Rows()
	got := tk.MustQuery("select * from t where (a not in (select a from t2)) or false").Sort().Rows()
	require.Equal(t, ref, got)
}

