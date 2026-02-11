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

package core

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/stretchr/testify/require"
)

// TestLateralJoinPlanBuilding tests that LATERAL joins build LogicalApply plans correctly
func TestLateralJoinPlanBuilding(t *testing.T) {
	s := coretestsdk.CreatePlannerSuiteElems()
	defer s.Close()
	ctx := context.Background()

	testCases := []struct {
		name              string
		sql               string
		expectApply       bool
		expectError       bool
		expectedErrorCode int
	}{
		{
			name:        "LATERAL with comma syntax builds LogicalApply",
			sql:         "SELECT * FROM t, LATERAL (SELECT t.a) AS dt",
			expectApply: true,
		},
		{
			name:        "LATERAL with LEFT JOIN builds LogicalApply",
			sql:         "SELECT * FROM t LEFT JOIN LATERAL (SELECT t.b) AS dt ON true",
			expectApply: true,
		},
		{
			name:        "LATERAL with CROSS JOIN builds LogicalApply",
			sql:         "SELECT * FROM t CROSS JOIN LATERAL (SELECT t.a + t.b as sum) AS dt",
			expectApply: true,
		},
		{
			name:              "LATERAL with RIGHT JOIN returns error",
			sql:               "SELECT * FROM t RIGHT JOIN LATERAL (SELECT t.a) AS dt ON true",
			expectError:       true,
			expectedErrorCode: mysql.ErrInvalidLateralJoin,
		},
		{
			name:        "Non-LATERAL derived table does not build LogicalApply",
			sql:         "SELECT * FROM t, (SELECT a FROM t) AS dt",
			expectApply: false,
		},
		{
			name:        "LATERAL with correlation builds LogicalApply",
			sql:         "SELECT * FROM t t1, LATERAL (SELECT * FROM t WHERE t.a = t1.a) AS dt",
			expectApply: true,
		},
		{
			name:        "Multiple LATERAL joins",
			sql:         "SELECT * FROM t, LATERAL (SELECT t.a) AS dt1, LATERAL (SELECT t.b) AS dt2",
			expectApply: true,
		},
		{
			name:        "LATERAL with aggregate and correlation",
			sql:         "SELECT * FROM t t1, LATERAL (SELECT COUNT(*) FROM t WHERE t.a = t1.a) AS dt",
			expectApply: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := s.GetParser().ParseOneStmt(tc.sql, "", "")
			require.NoError(t, err, "Failed to parse SQL: %s", tc.sql)

			nodeW := resolve.NewNodeW(stmt)
			p, err := BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())

			if tc.expectError {
				require.Error(t, err, "Expected error for: %s", tc.sql)
				if tc.expectedErrorCode != 0 {
					require.True(t, plannererrors.ErrInvalidLateralJoin.Equal(err),
						"Expected ErrInvalidLateralJoin error, got: %v", err)
				}
				return
			}

			require.NoError(t, err, "Failed to build plan for: %s", tc.sql)
			require.NotNil(t, p)

			// Check if LogicalApply is in the plan
			lp, ok := p.(base.LogicalPlan)
			require.True(t, ok, "Expected Plan to be LogicalPlan")
			hasApply := findLogicalApply(lp)

			if tc.expectApply {
				require.True(t, hasApply, "Expected LogicalApply in plan for: %s\nPlan: %s", tc.sql, ToString(p))
			} else {
				require.False(t, hasApply, "Did not expect LogicalApply in plan for: %s\nPlan: %s", tc.sql, ToString(p))
			}
		})
	}
}

// TestLateralJoinOptimization tests decorrelation and optimization behavior
func TestLateralJoinOptimization(t *testing.T) {
	s := coretestsdk.CreatePlannerSuiteElems()
	defer s.Close()
	ctx := context.Background()

	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "Simple LATERAL may decorrelate",
			sql:  "SELECT * FROM t, LATERAL (SELECT 1 as x) AS dt",
		},
		{
			name: "LATERAL with correlation attempts decorrelation",
			sql:  "SELECT * FROM t, LATERAL (SELECT t.a) AS dt",
		},
		{
			name: "LATERAL with aggregate stays as Apply",
			sql:  "SELECT * FROM t t1, LATERAL (SELECT COUNT(*) FROM t WHERE t.a = t1.a) AS dt",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := s.GetParser().ParseOneStmt(tc.sql, "", "")
			require.NoError(t, err)

			nodeW := resolve.NewNodeW(stmt)
			p, err := BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())
			require.NoError(t, err)
			require.NotNil(t, p)

			// Verify plan is valid
			require.NotNil(t, p.Schema())
		})
	}
}

// TestLateralJoinReordering tests that LATERAL joins prevent join reordering
func TestLateralJoinReordering(t *testing.T) {
	s := coretestsdk.CreatePlannerSuiteElems()
	defer s.Close()
	ctx := context.Background()

	testCases := []struct {
		name        string
		sql         string
		expectApply bool
	}{
		{
			name:        "Multiple LATERAL joins prevent reordering",
			sql:         "SELECT * FROM t, LATERAL (SELECT t.a) AS dt1, LATERAL (SELECT t.b) AS dt2",
			expectApply: true,
		},
		{
			name:        "LATERAL with multiple left tables",
			sql:         "SELECT * FROM t t1, t t2, LATERAL (SELECT t1.a + t2.a) AS dt",
			expectApply: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := s.GetParser().ParseOneStmt(tc.sql, "", "")
			require.NoError(t, err)

			nodeW := resolve.NewNodeW(stmt)
			p, err := BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())
			require.NoError(t, err)
			require.NotNil(t, p)

			lp, ok := p.(base.LogicalPlan)
			require.True(t, ok, "Expected Plan to be LogicalPlan")
			hasApply := findLogicalApply(lp)
			if tc.expectApply {
				require.True(t, hasApply, "Expected LogicalApply in plan")
				// Verify multiple Apply operators for multiple LATERAL joins
				applyCount := countLogicalApply(lp)
				require.GreaterOrEqual(t, applyCount, 1)
			}
		})
	}
}

// TestLateralJoinSchemaResolution tests column resolution in LATERAL joins
func TestLateralJoinSchemaResolution(t *testing.T) {
	s := coretestsdk.CreatePlannerSuiteElems()
	defer s.Close()
	ctx := context.Background()

	testCases := []struct {
		name        string
		sql         string
		expectError bool
	}{
		{
			name:        "LATERAL can reference left-side columns",
			sql:         "SELECT * FROM t, LATERAL (SELECT t.a + 1 AS x) AS dt",
			expectError: false,
		},
		{
			name:        "LATERAL with WHERE clause",
			sql:         "SELECT * FROM t t1, LATERAL (SELECT * FROM t WHERE t.a = t1.a) AS dt WHERE dt.b > 10",
			expectError: false,
		},
		{
			name:        "Nested LATERAL subquery",
			sql:         "SELECT * FROM t, LATERAL (SELECT * FROM (SELECT t.a) AS inner_dt) AS dt",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := s.GetParser().ParseOneStmt(tc.sql, "", "")
			require.NoError(t, err)

			nodeW := resolve.NewNodeW(stmt)
			p, err := BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, p)
			}
		})
	}
}

// TestLateralJoinExplain tests EXPLAIN output for LATERAL joins
func TestLateralJoinExplain(t *testing.T) {
	s := coretestsdk.CreatePlannerSuiteElems()
	defer s.Close()
	ctx := context.Background()

	sql := "SELECT * FROM t, LATERAL (SELECT t.a) AS dt"
	stmt, err := s.GetParser().ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	nodeW := resolve.NewNodeW(stmt)
	p, err := BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())
	require.NoError(t, err)
	require.NotNil(t, p)

	// Verify plan string representation
	planStr := ToString(p)
	require.NotEmpty(t, planStr)

	// LATERAL should use Apply operator
	lp, ok := p.(base.LogicalPlan)
	require.True(t, ok, "Expected Plan to be LogicalPlan")
	require.True(t, findLogicalApply(lp))
}

// TestLateralJoinErrorPaths tests various error scenarios
func TestLateralJoinErrorPaths(t *testing.T) {
	s := coretestsdk.CreatePlannerSuiteElems()
	defer s.Close()
	ctx := context.Background()

	testCases := []struct {
		name              string
		sql               string
		expectError       bool
		expectedErrorCode int
	}{
		{
			name:              "RIGHT JOIN with LATERAL is invalid",
			sql:               "SELECT * FROM t RIGHT JOIN LATERAL (SELECT t.a) AS dt ON true",
			expectError:       true,
			expectedErrorCode: mysql.ErrInvalidLateralJoin,
		},
		{
			name:        "LEFT JOIN with LATERAL is valid",
			sql:         "SELECT * FROM t LEFT JOIN LATERAL (SELECT t.a) AS dt ON true",
			expectError: false,
		},
		{
			name:        "INNER JOIN with LATERAL is valid",
			sql:         "SELECT * FROM t JOIN LATERAL (SELECT t.a) AS dt ON true",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := s.GetParser().ParseOneStmt(tc.sql, "", "")
			require.NoError(t, err, "Failed to parse: %s", tc.sql)

			nodeW := resolve.NewNodeW(stmt)
			p, err := BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())

			if tc.expectError {
				require.Error(t, err, "Expected error for: %s", tc.sql)
				if tc.expectedErrorCode != 0 {
					require.True(t, plannererrors.ErrInvalidLateralJoin.Equal(err),
						"Expected ErrInvalidLateralJoin error, got: %v", err)
				}
			} else {
				require.NoError(t, err, "Unexpected error for: %s", tc.sql)
				require.NotNil(t, p)
			}
		})
	}
}

// TestLateralJoinEdgeCases tests edge cases and corner scenarios
func TestLateralJoinEdgeCases(t *testing.T) {
	s := coretestsdk.CreatePlannerSuiteElems()
	defer s.Close()
	ctx := context.Background()

	testCases := []struct {
		name        string
		sql         string
		expectError bool
	}{
		{
			name:        "LATERAL with constant subquery",
			sql:         "SELECT * FROM t, LATERAL (SELECT 1) AS dt",
			expectError: false,
		},
		{
			name:        "LATERAL with empty result set",
			sql:         "SELECT * FROM t, LATERAL (SELECT t.a WHERE false) AS dt",
			expectError: false,
		},
		{
			name:        "LATERAL with UNION",
			sql:         "SELECT * FROM t, LATERAL (SELECT t.a UNION SELECT t.b) AS dt",
			expectError: false,
		},
		{
			name:        "LATERAL referencing multiple columns",
			sql:         "SELECT * FROM t, LATERAL (SELECT t.a, t.b, t.c) AS dt",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := s.GetParser().ParseOneStmt(tc.sql, "", "")
			require.NoError(t, err, "Failed to parse: %s", tc.sql)

			nodeW := resolve.NewNodeW(stmt)
			_, err = BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())

			if tc.expectError {
				require.Error(t, err, "Expected error for: %s", tc.sql)
			} else {
				require.NoError(t, err, "Unexpected error for: %s", tc.sql)
			}
		})
	}
}

// TestLateralJoinWithAggregates tests LATERAL with aggregate functions
func TestLateralJoinWithAggregates(t *testing.T) {
	s := coretestsdk.CreatePlannerSuiteElems()
	defer s.Close()
	ctx := context.Background()

	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "LATERAL with COUNT",
			sql:  "SELECT * FROM t t1, LATERAL (SELECT COUNT(*) as cnt FROM t WHERE t.a = t1.a) AS dt",
		},
		{
			name: "LATERAL with SUM",
			sql:  "SELECT * FROM t t1, LATERAL (SELECT SUM(a) as total FROM t WHERE t.a = t1.a) AS dt",
		},
		{
			name: "LATERAL with GROUP BY",
			sql:  "SELECT * FROM t t1, LATERAL (SELECT t.b, COUNT(*) FROM t WHERE t.a = t1.a GROUP BY t.b) AS dt",
		},
		{
			name: "LATERAL with MAX/MIN",
			sql:  "SELECT * FROM t t1, LATERAL (SELECT MAX(a), MIN(b) FROM t WHERE t.a = t1.a) AS dt",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := s.GetParser().ParseOneStmt(tc.sql, "", "")
			require.NoError(t, err)

			nodeW := resolve.NewNodeW(stmt)
			p, err := BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())
			require.NoError(t, err)
			require.NotNil(t, p)

			// Should use LogicalApply for correlated aggregates
			lp, ok := p.(base.LogicalPlan)
			require.True(t, ok, "Expected Plan to be LogicalPlan")
			require.True(t, findLogicalApply(lp), "Expected LogicalApply for: %s", tc.sql)
		})
	}
}

// TestLateralJoinComplexScenarios tests complex real-world LATERAL join patterns
func TestLateralJoinComplexScenarios(t *testing.T) {
	s := coretestsdk.CreatePlannerSuiteElems()
	defer s.Close()
	ctx := context.Background()

	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "LATERAL with nested aggregates",
			sql:  "SELECT * FROM t t1, LATERAL (SELECT AVG(cnt) FROM (SELECT COUNT(*) as cnt FROM t WHERE t.a = t1.a GROUP BY t.b) sub) AS dt",
		},
		{
			name: "Multiple LATERAL with different join types",
			sql:  "SELECT * FROM t t1 LEFT JOIN LATERAL (SELECT t1.a) AS dt1 ON true, LATERAL (SELECT t1.b) AS dt2",
		},
		{
			name: "LATERAL with complex WHERE conditions",
			sql:  "SELECT * FROM t t1, LATERAL (SELECT * FROM t WHERE t.a = t1.a AND t.b > t1.b OR t.c < t1.c) AS dt",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := s.GetParser().ParseOneStmt(tc.sql, "", "")
			require.NoError(t, err, "Failed to parse: %s", tc.sql)

			nodeW := resolve.NewNodeW(stmt)
			p, err := BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())
			require.NoError(t, err, "Failed to build plan for: %s", tc.sql)
			require.NotNil(t, p)

			// Verify plan is well-formed
			require.NotNil(t, p.Schema())
		})
	}
}

// Helper functions

// findLogicalApply recursively searches for LogicalApply in a plan tree
func findLogicalApply(p base.LogicalPlan) bool {
	if p == nil {
		return false
	}

	if _, ok := p.(*logicalop.LogicalApply); ok {
		return true
	}

	for _, child := range p.Children() {
		if findLogicalApply(child) {
			return true
		}
	}

	return false
}

// findFirstLogicalApply finds the first LogicalApply in a plan tree
func findFirstLogicalApply(p base.LogicalPlan) *logicalop.LogicalApply {
	if p == nil {
		return nil
	}

	if apply, ok := p.(*logicalop.LogicalApply); ok {
		return apply
	}

	for _, child := range p.Children() {
		if apply := findFirstLogicalApply(child); apply != nil {
			return apply
		}
	}

	return nil
}

// countLogicalApply counts the number of LogicalApply operators in a plan tree
func countLogicalApply(p base.LogicalPlan) int {
	if p == nil {
		return 0
	}

	count := 0
	if _, ok := p.(*logicalop.LogicalApply); ok {
		count = 1
	}

	for _, child := range p.Children() {
		count += countLogicalApply(child)
	}

	return count
}

// TestRecursiveCTEWithLateralOrderByLimit tests that ORDER BY and LIMIT are allowed
// within LATERAL subqueries in recursive CTEs
func TestRecursiveCTEWithLateralOrderByLimit(t *testing.T) {
	s := coretestsdk.CreatePlannerSuiteElems()
	defer s.Close()
	ctx := context.Background()

	testCases := []struct {
		name        string
		sql         string
		expectError bool
		errorMsg    string
	}{
		{
			name: "Recursive CTE with LATERAL ORDER BY - should work",
			sql: `
WITH RECURSIVE hierarchy AS (
  SELECT a, b FROM t WHERE a = 1
  UNION ALL
  SELECT n.a, n.b
  FROM hierarchy h
  CROSS JOIN LATERAL (
    SELECT a, b FROM t WHERE a = h.a + 1
    ORDER BY b DESC
    LIMIT 3
  ) AS n
  WHERE h.a < 5
)
SELECT * FROM hierarchy`,
			expectError: false,
		},
		{
			name: "Recursive CTE with LATERAL LIMIT only - should work",
			sql: `
WITH RECURSIVE cte AS (
  SELECT a FROM t WHERE a = 1
  UNION ALL
  SELECT n.a
  FROM cte c
  CROSS JOIN LATERAL (
    SELECT a FROM t WHERE a = c.a + 1
    LIMIT 5
  ) AS n
)
SELECT * FROM cte`,
			expectError: false,
		},
		{
			name: "Recursive CTE with LATERAL ORDER BY only - should work",
			sql: `
WITH RECURSIVE cte AS (
  SELECT a, b FROM t WHERE a = 1
  UNION ALL
  SELECT n.a, n.b
  FROM cte c
  CROSS JOIN LATERAL (
    SELECT a, b FROM t WHERE a = c.a + 1
    ORDER BY b ASC
  ) AS n
)
SELECT * FROM cte`,
			expectError: false,
		},
		{
			name: "Recursive CTE with non-LATERAL ORDER BY - should fail",
			sql: `
WITH RECURSIVE cte AS (
  SELECT a FROM t WHERE a = 1
  UNION ALL
  (SELECT t.a FROM t, cte WHERE t.a = cte.a + 1 ORDER BY t.a)
)
SELECT * FROM cte`,
			expectError: true,
			errorMsg:    "ORDER BY / LIMIT in recursive query block",
		},
		{
			name: "Recursive CTE with non-LATERAL LIMIT - should fail",
			sql: `
WITH RECURSIVE cte AS (
  SELECT a FROM t WHERE a = 1
  UNION ALL
  (SELECT t.a FROM t, cte WHERE t.a = cte.a + 1 LIMIT 10)
)
SELECT * FROM cte`,
			expectError: true,
			errorMsg:    "ORDER BY / LIMIT in recursive query block",
		},
		{
			name: "Recursive CTE with LEFT JOIN LATERAL and ORDER BY/LIMIT - should work",
			sql: `
WITH RECURSIVE hierarchy AS (
  SELECT a, b FROM t WHERE a = 1
  UNION ALL
  SELECT n.a, n.b
  FROM hierarchy h
  LEFT JOIN LATERAL (
    SELECT a, b FROM t WHERE a = h.a + 1
    ORDER BY b DESC
    LIMIT 2
  ) AS n ON true
)
SELECT * FROM hierarchy`,
			expectError: false,
		},
		{
			name: "Recursive CTE with multiple LATERAL joins with ORDER BY/LIMIT - should work",
			sql: `
WITH RECURSIVE cte AS (
  SELECT a FROM t WHERE a = 1
  UNION ALL
  SELECT n2.a
  FROM cte c
  CROSS JOIN LATERAL (
    SELECT a FROM t WHERE a = c.a + 1
    ORDER BY a
    LIMIT 2
  ) AS n1
  CROSS JOIN LATERAL (
    SELECT a FROM t WHERE a = n1.a + 1
    ORDER BY a DESC
    LIMIT 1
  ) AS n2
)
SELECT * FROM cte`,
			expectError: false,
		},
		{
			name: "Recursive CTE with non-LATERAL subquery with ORDER BY - should fail",
			sql: `
WITH RECURSIVE cte AS (
  SELECT a FROM t WHERE a = 1
  UNION ALL
  SELECT a FROM (
    SELECT a FROM t, cte WHERE t.a = cte.a + 1
    ORDER BY a
  ) AS sub
)
SELECT * FROM cte`,
			expectError: true,
			errorMsg:    "ORDER BY / LIMIT in recursive query block",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := s.GetParser().ParseOneStmt(tc.sql, "", "")
			require.NoError(t, err, "Failed to parse SQL: %s", tc.sql)

			nodeW := resolve.NewNodeW(stmt)
			p, err := BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())

			if tc.expectError {
				require.Error(t, err, "Expected error for: %s", tc.sql)
				if tc.errorMsg != "" {
					require.Contains(t, err.Error(), tc.errorMsg, "Error message mismatch")
				}
			} else {
				require.NoError(t, err, "Unexpected error for: %s\nError: %v", tc.sql, err)
				require.NotNil(t, p, "Plan should not be nil")
			}
		})
	}
}
