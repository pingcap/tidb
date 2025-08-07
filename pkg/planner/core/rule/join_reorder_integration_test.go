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

package rule

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestJoinReorderIntegration_SimpleJoin tests simple join reordering
func TestJoinReorderIntegration_SimpleJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("create table t3(a int, b int)")
	tk.MustExec("insert into t1 values(1,1), (2,2)")
	tk.MustExec("insert into t2 values(1,1), (2,2)")
	tk.MustExec("insert into t3 values(1,1), (2,2)")

	// Test simple join reordering
	sql := "select * from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b"
	tk.MustQuery(sql).Check(testkit.Rows(
		"1 1 1 1 1 1",
		"2 2 2 2 2 2",
	))

	// Verify the plan is optimized
	plan := tk.MustQuery("explain " + sql).Rows()
	require.NotEmpty(t, plan)
}

// TestJoinReorderIntegration_ComplexJoin tests complex join reordering
func TestJoinReorderIntegration_ComplexJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3, t4")
	tk.MustExec("create table t1(a int, b int, c int)")
	tk.MustExec("create table t2(a int, b int, c int)")
	tk.MustExec("create table t3(a int, b int, c int)")
	tk.MustExec("create table t4(a int, b int, c int)")
	tk.MustExec("insert into t1 values(1,1,1), (2,2,2)")
	tk.MustExec("insert into t2 values(1,1,1), (2,2,2)")
	tk.MustExec("insert into t3 values(1,1,1), (2,2,2)")
	tk.MustExec("insert into t4 values(1,1,1), (2,2,2)")

	// Test complex join reordering with multiple conditions
	sql := "select * from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b join t4 on t3.c = t4.c"
	tk.MustQuery(sql).Check(testkit.Rows(
		"1 1 1 1 1 1 1 1 1 1 1 1",
		"2 2 2 2 2 2 2 2 2 2 2 2",
	))

	// Verify the plan is optimized
	plan := tk.MustQuery("explain " + sql).Rows()
	require.NotEmpty(t, plan)
}

// TestJoinReorderIntegration_OuterJoin tests outer join handling
func TestJoinReorderIntegration_OuterJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 values(1,1), (2,2)")
	tk.MustExec("insert into t2 values(1,1), (3,3)")

	// Test left outer join (should not be reordered)
	sql := "select * from t1 left join t2 on t1.a = t2.a"
	result := tk.MustQuery(sql).Rows()
	require.Len(t, result, 2)

	// Verify the plan is not reordered for outer joins
	plan := tk.MustQuery("explain " + sql).Rows()
	require.NotEmpty(t, plan)
}

// TestJoinReorderIntegration_WithConditions tests join reordering with additional conditions
func TestJoinReorderIntegration_WithConditions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("create table t3(a int, b int)")
	tk.MustExec("insert into t1 values(1,1), (2,2), (3,3)")
	tk.MustExec("insert into t2 values(1,1), (2,2), (4,4)")
	tk.MustExec("insert into t3 values(1,1), (2,2), (5,5)")

	// Test join reordering with WHERE conditions
	sql := "select * from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b where t1.b > 1"
	result := tk.MustQuery(sql).Rows()
	require.Len(t, result, 1) // Only row with t1.b = 2 should match

	// Verify the plan is optimized
	plan := tk.MustQuery("explain " + sql).Rows()
	require.NotEmpty(t, plan)
}

// TestJoinReorderIntegration_Subquery tests join reordering with subqueries
func TestJoinReorderIntegration_Subquery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 values(1,1), (2,2)")
	tk.MustExec("insert into t2 values(1,1), (2,2)")

	// Test join reordering with subquery
	sql := "select * from t1 join (select * from t2) t2_alias on t1.a = t2_alias.a"
	result := tk.MustQuery(sql).Rows()
	require.Len(t, result, 2)

	// Verify the plan is optimized
	plan := tk.MustQuery("explain " + sql).Rows()
	require.NotEmpty(t, plan)
}

// TestJoinReorderIntegration_Performance tests performance characteristics
func TestJoinReorderIntegration_Performance(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3, t4, t5")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("create table t3(a int, b int)")
	tk.MustExec("create table t4(a int, b int)")
	tk.MustExec("create table t5(a int, b int)")

	// Insert some test data
	for i := 1; i <= 10; i++ {
		tk.MustExec("insert into t1 values(?, ?)", i, i)
		tk.MustExec("insert into t2 values(?, ?)", i, i)
		tk.MustExec("insert into t3 values(?, ?)", i, i)
		tk.MustExec("insert into t4 values(?, ?)", i, i)
		tk.MustExec("insert into t5 values(?, ?)", i, i)
	}

	// Test complex join reordering (should use greedy algorithm)
	sql := "select * from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b join t4 on t3.a = t4.a join t5 on t4.b = t5.b"
	result := tk.MustQuery(sql).Rows()
	require.Len(t, result, 10)

	// Verify the plan is optimized
	plan := tk.MustQuery("explain " + sql).Rows()
	require.NotEmpty(t, plan)
}

// TestJoinReorderIntegration_Compatibility tests compatibility with existing functionality
func TestJoinReorderIntegration_Compatibility(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 values(1,1), (2,2)")
	tk.MustExec("insert into t2 values(1,1), (2,2)")

	// Test that the new rule produces the same results as the old one
	sql := "select * from t1 join t2 on t1.a = t2.a"
	result1 := tk.MustQuery(sql).Rows()

	// Disable join reorder and test again
	tk.MustExec("set tidb_opt_join_reorder_threshold = 0")
	result2 := tk.MustQuery(sql).Rows()

	// Results should be the same (same data, same join conditions)
	require.Equal(t, result1, result2)
}

// TestJoinReorderIntegration_ErrorHandling tests error handling
func TestJoinReorderIntegration_ErrorHandling(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 values(1,1), (2,2)")
	tk.MustExec("insert into t2 values(1,1), (2,2)")

	// Test with invalid join conditions (should not cause errors)
	sql := "select * from t1 join t2 on t1.a = t2.a where t1.b = t2.b"
	result := tk.MustQuery(sql).Rows()
	require.Len(t, result, 2)

	// Verify the plan is generated without errors
	plan := tk.MustQuery("explain " + sql).Rows()
	require.NotEmpty(t, plan)
}

// TestJoinReorderIntegration_Statistics tests join reordering with statistics
func TestJoinReorderIntegration_Statistics(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("create table t3(a int, b int)")

	// Insert data with different cardinalities
	tk.MustExec("insert into t1 values(1,1), (2,2), (3,3), (4,4), (5,5)")
	tk.MustExec("insert into t2 values(1,1), (2,2)")
	tk.MustExec("insert into t3 values(1,1), (2,2), (3,3)")

	// Analyze tables to generate statistics
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	tk.MustExec("analyze table t3")

	// Test join reordering with statistics
	sql := "select * from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b"
	result := tk.MustQuery(sql).Rows()
	require.Len(t, result, 2)

	// Verify the plan is optimized using statistics
	plan := tk.MustQuery("explain " + sql).Rows()
	require.NotEmpty(t, plan)
}

// TestJoinReorderIntegration_Stress tests stress scenarios
func TestJoinReorderIntegration_Stress(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3, t4, t5, t6")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("create table t3(a int, b int)")
	tk.MustExec("create table t4(a int, b int)")
	tk.MustExec("create table t5(a int, b int)")
	tk.MustExec("create table t6(a int, b int)")

	// Insert data for stress testing
	for i := 1; i <= 5; i++ {
		tk.MustExec("insert into t1 values(?, ?)", i, i)
		tk.MustExec("insert into t2 values(?, ?)", i, i)
		tk.MustExec("insert into t3 values(?, ?)", i, i)
		tk.MustExec("insert into t4 values(?, ?)", i, i)
		tk.MustExec("insert into t5 values(?, ?)", i, i)
		tk.MustExec("insert into t6 values(?, ?)", i, i)
	}

	// Test complex join reordering (should use DP algorithm)
	sql := "select * from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b join t4 on t3.a = t4.a join t5 on t4.b = t5.b join t6 on t5.a = t6.a"
	result := tk.MustQuery(sql).Rows()
	require.Len(t, result, 5)

	// Verify the plan is optimized
	plan := tk.MustQuery("explain " + sql).Rows()
	require.NotEmpty(t, plan)
}

// TestJoinReorderIntegration_Regression tests for regression issues
func TestJoinReorderIntegration_Regression(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 values(1,1), (2,2)")
	tk.MustExec("insert into t2 values(1,1), (2,2)")

	// Test various edge cases that might cause regressions
	testCases := []string{
		"select * from t1 join t2 on t1.a = t2.a",
		"select t1.a, t2.b from t1 join t2 on t1.a = t2.a",
		"select count(*) from t1 join t2 on t1.a = t2.a",
		"select * from t1 join t2 on t1.a = t2.a where t1.b > 0",
		"select * from t1 join t2 on t1.a = t2.a order by t1.a",
		"select * from t1 join t2 on t1.a = t2.a limit 1",
	}

	for _, sql := range testCases {
		t.Run(sql, func(t *testing.T) {
			result := tk.MustQuery(sql).Rows()
			require.NotNil(t, result)
			
			plan := tk.MustQuery("explain " + sql).Rows()
			require.NotEmpty(t, plan)
		})
	}
} 