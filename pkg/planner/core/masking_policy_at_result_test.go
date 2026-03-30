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
		tk.MustExec("create masking policy p_having on t_having(val) as mask_full(val, '*') enable")

		// HAVING should compare original values, not masked values
		// 'banana' > 'apple' is true, so we should get banana and cherry
		result := tk.MustQuery("select val, count(*) from t_having group by val having val > 'apple'")
		// mask_full replaces each character with *, so 'banana' (6 chars) -> '******', 'cherry' (6 chars) -> '******'
		result.Check(testkit.Rows("****** 1", "****** 1")) // banana, cherry

		// This should return no rows because no value > 'zzzz'
		rows := tk.MustQuery("select val from t_having group by val having val > 'zzzz'").Rows()
		require.Len(t, rows, 0)
	})

	// Test ORDER BY uses original values
	t.Run("ORDER BY uses original values", func(t *testing.T) {
		tk.MustExec("drop table if exists t_orderby")
		tk.MustExec("create table t_orderby(id int, val varchar(100))")
		tk.MustExec("insert into t_orderby values (1, 'apple'), (2, 'banana'), (3, 'cherry')")
		tk.MustExec("create masking policy p_orderby on t_orderby(val) as mask_full(val, '*') enable")

		// ORDER BY should sort by original values
		result := tk.MustQuery("select id, val from t_orderby order by val")
		// apple (5) -> *****, banana (6) -> ******, cherry (6) -> ******
		result.Check(testkit.Rows("1 *****", "2 ******", "3 ******")) // apple, banana, cherry order

		// Reverse order
		result = tk.MustQuery("select id, val from t_orderby order by val desc")
		result.Check(testkit.Rows("3 ******", "2 ******", "1 *****")) // cherry, banana, apple order
	})

	// Test auxiliary column consistency in ORDER BY
	t.Run("Auxiliary column consistency in ORDER BY", func(t *testing.T) {
		tk.MustExec("drop table if exists t_aux")
		tk.MustExec("create table t_aux(id int, val varchar(100))")
		tk.MustExec("insert into t_aux values (1, 'apple'), (2, 'banana'), (3, 'cherry')")
		tk.MustExec("create masking policy p_aux on t_aux(val) as mask_full(val, '*') enable")

		// Both queries should use the same order (by original val)
		result1 := tk.MustQuery("select id from t_aux order by val").Rows()
		result2 := tk.MustQuery("select val from t_aux order by val").Rows()

		// They should have the same number of rows
		require.Len(t, result1, 3)
		require.Len(t, result2, 3)

		// The IDs should be in the same order as the values would be
		// Note: Rows() returns []interface{}, and numbers are returned as strings
		require.Equal(t, "1", result1[0][0])
		require.Equal(t, "2", result1[1][0])
		require.Equal(t, "3", result1[2][0])
	})

	// Test UNION DISTINCT uses original values for deduplication
	t.Run("UNION DISTINCT uses original values", func(t *testing.T) {
		tk.MustExec("drop table if exists t_union")
		tk.MustExec("create table t_union(id int, val varchar(100))")
		tk.MustExec("insert into t_union values (1, 'value1'), (2, 'value2')")
		tk.MustExec("create masking policy p_union on t_union(val) as mask_full(val, '*') enable")

		// Two different original values should NOT be deduplicated
		// even though they mask to the same value
		result := tk.MustQuery("select val from t_union where id = 1 union distinct select val from t_union where id = 2")
		// Should return 2 rows because 'value1' != 'value2'
		rows := result.Rows()
		require.Len(t, rows, 2)
	})

	// Test aggregate with HAVING
	t.Run("Aggregate with HAVING uses original values", func(t *testing.T) {
		tk.MustExec("drop table if exists t_agg")
		tk.MustExec("create table t_agg(category varchar(50), amount int)")
		tk.MustExec("insert into t_agg values ('A', 100), ('B', 200), ('C', 300)")
		tk.MustExec("create masking policy p_agg on t_agg(category) as mask_full(category, '*') enable")

		// HAVING should compare original category values
		result := tk.MustQuery("select category, sum(amount) from t_agg group by category having category > 'A'")
		rows := result.Rows()
		require.Len(t, rows, 2) // B and C

		// All categories should be masked in output (A, B, C are 1 char each)
		for _, row := range rows {
			require.Equal(t, "*", row[0])
		}
	})
}

// TestMaskingPolicyAtResultWithPartialMasking tests AT RESULT semantics with partial masking
func TestMaskingPolicyAtResultWithPartialMasking(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t_partial")
	tk.MustExec("create table t_partial(id int, email varchar(100))")
	tk.MustExec("insert into t_partial values (1, 'alice@example.com'), (2, 'bob@example.com'), (3, 'charlie@example.com')")
	tk.MustExec("create masking policy p_partial on t_partial(email) as mask_partial(email, '*', 1, 100) enable")

	// ORDER BY should sort by original email, not masked
	result := tk.MustQuery("select id, email from t_partial order by email")
	// alice@example.com < bob@example.com < charlie@example.com
	result.Check(testkit.Rows("1 a****************", "2 b**************", "3 c******************"))
}

// TestMaskingPolicyAtResultWithConcat tests AT RESULT semantics with concat expressions
func TestMaskingPolicyAtResultWithConcat(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t_concat")
	tk.MustExec("create table t_concat(id int, val varchar(100))")
	tk.MustExec("insert into t_concat values (1, 'a'), (2, 'b'), (3, 'c')")
	tk.MustExec("create masking policy p_concat on t_concat(val) as concat('***', val) enable")

	// The masked output should be '***' + original value
	result := tk.MustQuery("select val from t_concat order by val")
	result.Check(testkit.Rows("***a", "***b", "***c"))

	// But ORDER BY should use original values for ordering
	result = tk.MustQuery("select id from t_concat order by val desc")
	result.Check(testkit.Rows("3", "2", "1"))
}
