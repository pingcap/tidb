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
	"fmt"
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

func TestCorrelatedInApplyEliminatesDistinct(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists international_contractors, members, member_employments")
	tk.MustExec("create table international_contractors (member_id int not null, company_id bigint not null, key(company_id), key(member_id))")
	tk.MustExec("create table members (id int not null, primary key(id))")
	tk.MustExec("create table member_employments (member_id int not null, worker_type varchar(20), country_code varchar(8), start_date date, end_date date, key(member_id, worker_type, start_date, end_date))")
	tk.MustExec("insert into international_contractors values (1, 1), (2, 1), (3, 1)")
	tk.MustExec("insert into members values (1), (2), (3)")
	tk.MustExec("insert into member_employments values " +
		"(1, 'Contractor', 'CA', '2025-01-01', null)," +
		"(1, 'Contractor', 'CA', '2025-01-02', null)," +
		"(2, 'Contractor', 'GB', '2025-01-01', '2026-02-01')," +
		"(3, 'Employee', 'US', '2025-01-01', null)")

	sql := `select count(1)
from international_contractors ic
join members m on m.id = ic.member_id
where m.id in (
	select distinct inner_members.id
	from members inner_members
	join member_employments me on me.member_id = inner_members.id
	where me.start_date <= '2026-01-05'
	  and (me.end_date is null or me.end_date >= '2026-01-05')
	  and me.worker_type = 'Contractor'
	  and me.country_code != 'US'
	  and me.member_id = m.id
)
and ic.company_id = 1`

	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = ON")
	tk.MustExec(`delete from mysql.opt_rule_blacklist where name = "aggregation_eliminate"`)
	tk.MustExec("admin reload opt_rule_blacklist")
	defer func() {
		tk.MustExec(`delete from mysql.opt_rule_blacklist where name = "aggregation_eliminate"`)
		tk.MustExec("admin reload opt_rule_blacklist")
	}()
	tk.MustExec(`insert into mysql.opt_rule_blacklist values ("aggregation_eliminate")`)
	tk.MustExec("admin reload opt_rule_blacklist")
	rows := tk.MustQuery("explain format = 'brief' " + sql).Rows()
	require.True(t, explainContains(rows, "Apply"),
		"with aggregation_eliminate disabled, expected Apply in plan:\n%s", joinExplainRows(rows))
	require.True(t, explainHasDistinctLikeAgg(rows),
		"with aggregation_eliminate disabled, expected inner distinct agg in Apply plan:\n%s", joinExplainRows(rows))

	tk.MustExec(`delete from mysql.opt_rule_blacklist where name = "aggregation_eliminate"`)
	tk.MustExec("admin reload opt_rule_blacklist")
	rows = tk.MustQuery("explain format = 'brief' " + sql).Rows()
	require.True(t, explainContains(rows, "Apply"),
		"with aggregation_eliminate enabled, expected Apply in plan:\n%s", joinExplainRows(rows))
	require.False(t, explainHasDistinctLikeAgg(rows),
		"with aggregation_eliminate enabled, expected correlated IN apply to eliminate inner distinct agg:\n%s", joinExplainRows(rows))
	tk.MustQuery(sql).Check(testkit.Rows("2"))
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

func explainHasDistinctLikeAgg(rows [][]any) bool {
	for _, row := range rows {
		if len(row) < 5 {
			continue
		}
		info := fmt.Sprintf("%v", row[4])
		if strings.Contains(info, "group by:") && strings.Contains(info, "firstrow(") {
			return true
		}
	}
	return false
}

// TestCorrelateParallelApply verifies that when the correlate alternative round
// produces an Apply plan and tidb_enable_parallel_apply is ON, the Apply is
// executed with parallel concurrency. This tests the interaction between the
// correlate optimization (converting decorrelated semi-join back to Apply) and
// the parallel apply executor.
func TestCorrelateParallelApply(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int not null, b int, key(a))")
	tk.MustExec("create table t2 (a int not null, b int, key(a))")
	tk.MustExec("insert into t1 values (1,1),(2,2),(3,3),(4,4),(5,5)")
	tk.MustExec("insert into t2 values (1,10),(2,20),(3,30)")

	sql := "select * from t1 where b = 1 and a in (select a from t2)"

	// Enable correlate alternative + parallel apply.
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = ON")
	tk.MustExec("set tidb_enable_parallel_apply = ON")
	tk.MustExec("set tidb_executor_concurrency = 5")

	// Verify the plan contains Apply (correlate alternative won).
	rows := tk.MustQuery("explain format = 'brief' " + sql).Rows()
	require.True(t, explainContains(rows, "Apply"),
		"with correlate alternative + parallel apply, expected Apply in plan:\n%s", joinExplainRows(rows))

	// Verify EXPLAIN ANALYZE reports Concurrency > 1 for the Apply.
	analyzeRows := tk.MustQuery("explain analyze " + sql).Rows()
	foundConcurrency := false
	for _, row := range analyzeRows {
		line := fmt.Sprintf("%v", row)
		if strings.Contains(line, "Apply") && strings.Contains(line, "Concurrency:") {
			idx := strings.Index(line, "Concurrency:")
			if idx >= 0 {
				rest := line[idx+len("Concurrency:"):]
				var n int
				if _, err := fmt.Sscanf(rest, "%d", &n); err == nil && n > 1 {
					foundConcurrency = true
				}
			}
			break
		}
	}
	require.True(t, foundConcurrency,
		"EXPLAIN ANALYZE must report Concurrency > 1 for Apply when parallel_apply is on")

	// Verify correctness: parallel + correlate must match serial + no correlate.
	tk.MustExec("set tidb_enable_parallel_apply = OFF")
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = OFF")
	serialRows := tk.MustQuery(sql).Rows()

	tk.MustExec("set tidb_enable_parallel_apply = ON")
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = ON")
	parallelRows := tk.MustQuery(sql).Rows()

	require.Equal(t, serialRows, parallelRows,
		"correlate alternative + parallel apply must produce the same result as standard path")
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
