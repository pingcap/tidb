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

package rule

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

// TestCorrelateNullSemantics verifies that CorrelateSolver does not break
// 3-valued NULL semantics for scalar IN (LeftOuterSemiJoin).
func TestCorrelateNullSemantics(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = ON")

	// Case 1: non-null outer, null inner → must return NULL (not 0).
	tk.MustExec("drop table if exists tn, sn")
	tk.MustExec("create table tn(a int)")
	tk.MustExec("create table sn(a int, key(a))")
	tk.MustExec("insert into tn values (1)")
	tk.MustExec("insert into sn values (null)")
	tk.MustQuery("select tn.a in (select sn.a from sn) as r from tn").Check(testkit.Rows("<nil>"))

	// Case 2: null outer, non-null inner → must return NULL (not 0).
	tk.MustExec("truncate table tn")
	tk.MustExec("truncate table sn")
	tk.MustExec("insert into tn values (null)")
	tk.MustExec("insert into sn values (1)")
	tk.MustQuery("select tn.a in (select sn.a from sn) as r from tn").Check(testkit.Rows("<nil>"))

	// Case 3: both columns NOT NULL → correlate is safe; verify correct results.
	tk.MustExec("drop table if exists tnn, snn")
	tk.MustExec("create table tnn(a int not null)")
	tk.MustExec("create table snn(a int not null, key(a))")
	tk.MustExec("insert into tnn values (1), (2), (3)")
	tk.MustExec("insert into snn values (1), (2)")
	tk.MustQuery("select tnn.a in (select snn.a from snn) as r from tnn order by tnn.a").Check(testkit.Rows("1", "1", "0"))
}

// TestCorrelateAlternativeChoosesApply verifies that the correlate alternative
// round produces an Apply plan that wins the cost comparison for a non-correlated
// IN subquery when an outer WHERE predicate reduces the estimated row count.
// Without alternative plans, the InnerJoin+Agg rewrite produces IndexJoin+StreamAgg.
// With alternative plans, the correlate round produces Apply+Limit which is cheaper
// (avoids the StreamAgg overhead and uses Limit 1 for early exit on the inner side).
func TestCorrelateAlternativeChoosesApply(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int not null, b int, key(a))")
	tk.MustExec("create table t2 (a int not null, b int, key(a))")
	tk.MustExec("insert into t1 values (1,1),(2,2),(3,3)")
	tk.MustExec("insert into t2 values (1,10),(2,20)")

	sql := "select * from t1 where b = 1 and a in (select a from t2)"

	// Without alternative plans: standard InnerJoin+Agg path produces IndexJoin.
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = OFF")
	rows := tk.MustQuery("explain format = 'brief' " + sql).Rows()
	require.True(t, explainContains(rows, "IndexJoin"),
		"without alternative plans, expected IndexJoin in plan:\n%s", joinExplainRows(rows))

	// With alternative plans: correlate round produces Apply (cheaper than IndexJoin+StreamAgg).
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = ON")
	rows = tk.MustQuery("explain format = 'brief' " + sql).Rows()
	require.True(t, explainContains(rows, "Apply"),
		"with alternative plans, expected Apply in plan:\n%s", joinExplainRows(rows))

	// Verify correct results in both modes.
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = OFF")
	tk.MustQuery(sql).Check(testkit.Rows("1 1"))
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = ON")
	tk.MustQuery(sql).Check(testkit.Rows("1 1"))
}

func TestCorrelate(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1, t2, t3")
		tk.MustExec("create table t1 (a int, b int, key(a))")
		tk.MustExec("create table t2 (a int, b int, key(a))")
		tk.MustExec("create table t3 (a int, b int, key(a))")
		tk.MustExec("insert into t1 values (1,1),(2,2),(3,3)")
		tk.MustExec("insert into t2 values (1,10),(2,20)")
		tk.MustExec("insert into t3 values (10,1),(20,2)")

		// Enable the correlate rule.
		tk.MustExec("set tidb_opt_enable_alternative_logical_plans = ON")

		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
		suite := GetCorrelateSuiteData()
		suite.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + sql).Rows())
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			})
			tk.MustQuery("explain format = 'brief' " + sql).Check(testkit.Rows(output[i].Plan...))
			tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
		}
	})
}

// explainContains scans all explain rows for a substring in the operator column.
func explainContains(rows [][]any, substr string) bool {
	for _, row := range rows {
		if strings.Contains(row[0].(string), substr) {
			return true
		}
	}
	return false
}

// joinExplainRows formats explain rows into a single string for debug output.
func joinExplainRows(rows [][]any) string {
	var sb strings.Builder
	for _, row := range rows {
		sb.WriteString(row[0].(string))
		sb.WriteByte('\n')
	}
	return sb.String()
}

// TestCorrelateWithCostFactors verifies that when hash/merge join cost factors
// are increased, the correlate alternative round wins and produces Apply-based
// plans with correlated index access for cases that normally choose HashJoin.
func TestCorrelateWithCostFactors(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1, t2, t3")
		tk.MustExec("create table t1 (a int, b int, key(a))")
		tk.MustExec("create table t2 (a int, b int, key(a))")
		tk.MustExec("create table t3 (a int, b int, key(a))")
		tk.MustExec("insert into t1 values (1,1),(2,2),(3,3)")
		tk.MustExec("insert into t2 values (1,10),(2,20)")
		tk.MustExec("insert into t3 values (10,1),(20,2)")

		// Enable the correlate rule and penalize hash/merge joins so the
		// correlate alternative (Apply with index lookup) wins the cost comparison.
		tk.MustExec("set tidb_opt_enable_alternative_logical_plans = ON")
		tk.MustExec("set tidb_opt_hash_join_cost_factor = 1000")
		tk.MustExec("set tidb_opt_merge_join_cost_factor = 1000")

		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
		suite := GetCorrelateSuiteData()
		suite.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + sql).Rows())
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			})
			tk.MustQuery("explain format = 'brief' " + sql).Check(testkit.Rows(output[i].Plan...))
			tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
		}
	})
}
