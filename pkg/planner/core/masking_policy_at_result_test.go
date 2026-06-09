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
	"github.com/stretchr/testify/require"
)

// TestMaskingPolicyAtResult tests that masking follows AT RESULT semantics:
// HAVING, ORDER BY, and set operators use original values, not masked values.
func TestMaskingPolicyAtResult(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Test HAVING uses original values
	t.Run("HAVING uses original values", func(t *testing.T) {
		tk.MustExec("drop table if exists t_having")
		tk.MustExec("create table t_having(id int, val varchar(100))")
		tk.MustExec("insert into t_having values (1, 'apple'), (2, 'banana'), (3, 'cherry')")
		tk.MustExec("create masking policy p_having on t_having(val) as concat('[', val, ']') enable")

		// HAVING should compare original values, not masked values
		result := tk.MustQuery("select val, count(*) from t_having group by val having val > 'apple'")
		result.Check(testkit.Rows("[banana] 1", "[cherry] 1"))

		// This should return no rows because no value > 'zzzz'
		rows := tk.MustQuery("select val from t_having group by val having val > 'zzzz'").Rows()
		require.Len(t, rows, 0)
	})

	// Test ORDER BY uses original values
	t.Run("ORDER BY uses original values", func(t *testing.T) {
		tk.MustExec("drop table if exists t_orderby")
		tk.MustExec("create table t_orderby(id int, val varchar(100))")
		tk.MustExec("insert into t_orderby values (1, 'apple'), (2, 'banana'), (3, 'cherry')")
		tk.MustExec("create masking policy p_orderby on t_orderby(val) as concat('[', val, ']') enable")

		// ORDER BY should sort by original values
		result := tk.MustQuery("select id, val from t_orderby order by val")
		result.Check(testkit.Rows("1 [apple]", "2 [banana]", "3 [cherry]"))

		// Reverse order
		result = tk.MustQuery("select id, val from t_orderby order by val desc")
		result.Check(testkit.Rows("3 [cherry]", "2 [banana]", "1 [apple]"))
	})

	// Test UNION DISTINCT uses original values for deduplication
	t.Run("UNION DISTINCT uses original values", func(t *testing.T) {
		tk.MustExec("drop table if exists t_union")
		tk.MustExec("create table t_union(id int, val varchar(100))")
		tk.MustExec("insert into t_union values (1, 'value1'), (2, 'value2')")
		tk.MustExec("create masking policy p_union on t_union(val) as concat('[', val, ']') enable")

		// Two different original values should NOT be deduplicated
		// even though they mask to the same pattern
		result := tk.MustQuery("select val from t_union where id = 1 union distinct select val from t_union where id = 2")
		rows := result.Rows()
		require.Len(t, rows, 2)
	})

	// Test aggregate with HAVING
	t.Run("Aggregate with HAVING uses original values", func(t *testing.T) {
		tk.MustExec("drop table if exists t_agg")
		tk.MustExec("create table t_agg(category varchar(50), amount int)")
		tk.MustExec("insert into t_agg values ('A', 100), ('B', 200), ('C', 300)")
		tk.MustExec("create masking policy p_agg on t_agg(category) as concat('_', category) enable")

		// HAVING should compare original category values
		result := tk.MustQuery("select category, sum(amount) from t_agg group by category having category > 'A'")
		rows := result.Rows()
		require.Len(t, rows, 2) // B and C

		// All categories should be masked in output
		require.Contains(t, []string{"_B", "_C"}, rows[0][0])
		require.Contains(t, []string{"_B", "_C"}, rows[1][0])
		require.NotEqual(t, rows[0][0], rows[1][0])
	})
}
