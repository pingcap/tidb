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

package issuetest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

// TestNonPreparedPlanCacheZeroArgDateFunc guards against an out-of-range access
// on Args[0] when parameterizing a zero-arg date_format/str_to_date/time_format/
// from_unixtime call for the non-prepared plan cache.
func TestNonPreparedPlanCacheZeroArgDateFunc(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")
	tk.MustExec("create table t (a datetime, b int)")
	// The parameterizer visits these funcs before arity is validated; a zero-arg
	// call in the WHERE clause must not panic. The statement still errors on the
	// invalid call, but must not crash the server.
	tk.MustExecToErr("select * from t where a = date_format()")
	tk.MustExecToErr("select * from t where a = str_to_date()")
	tk.MustExecToErr("select * from t where b = time_format()")
	tk.MustExecToErr("select * from t where b = from_unixtime()")
}

// TestSkewDistinctAggConstantArg guards against an unchecked *Column assertion
// when the skew-distinct-agg rewrite decomposes a distinct aggregate whose
// argument is a constant (e.g. count(distinct 1)).
func TestSkewDistinctAggConstantArg(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_opt_skew_distinct_agg=1")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (1, 10), (2, 10), (3, 20)")
	tk.MustQuery("select b, count(distinct 1) from t group by b order by b").
		Check(testkit.Rows("10 1", "20 1"))
	tk.MustQuery("select b, sum(distinct 2) from t group by b order by b").
		Check(testkit.Rows("10 2", "20 2"))
}

// TestPushDownSequenceWithTableDual guards against indexing Children()[0] on a
// childless operator (a LogicalTableDual from a constant-false predicate) while
// pushing a sequence down under shared-CTE execution.
func TestPushDownSequenceWithTableDual(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_opt_enable_mpp_shared_cte_execution=1")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (1), (2)")
	tk.MustQuery("with cte as (select a from t) " +
		"select * from cte c1 join cte c2 on c1.a = c2.a where 1 = 0").
		Check(testkit.Rows())
}
