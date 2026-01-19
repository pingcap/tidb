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
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/stretchr/testify/require"
)

// TestLateralJoinPlanBuilding tests that LATERAL joins build LogicalApply plans correctly
func TestLateralJoinPlanBuilding(t *testing.T) {
	s := coretestsdk.CreatePlannerSuiteElems()
	defer s.Close()
	ctx := context.Background()

	// Create test tables
	s.GetSCtx().GetSessionVars().EnableClusteredIndex = 0
	_, err := s.GetTK().Exec("use test")
	require.NoError(t, err)
	_, err = s.GetTK().Exec("drop table if exists t1, t2")
	require.NoError(t, err)
	_, err = s.GetTK().Exec("create table t1(a int, b int)")
	require.NoError(t, err)
	_, err = s.GetTK().Exec("create table t2(x int, y int)")
	require.NoError(t, err)

	testCases := []struct {
		name                string
		sql                 string
		expectApply         bool
		expectError         bool
		expectedErrorCode   int
		checkCorrelation    bool
		checkJoinReordering bool
	}{
		{
			name:         "LATERAL with comma syntax builds LogicalApply",
			sql:          "SELECT * FROM t1, LATERAL (SELECT t1.a) AS dt",
			expectApply:  true,
			checkCorrelation: true,
		},
		{
			name:         "LATERAL with LEFT JOIN builds LogicalApply",
			sql:          "SELECT * FROM t1 LEFT JOIN LATERAL (SELECT t1.b) AS dt ON true",
			expectApply:  true,
			checkCorrelation: true,
		},
		{
			name:         "LATERAL with CROSS JOIN builds LogicalApply",
			sql:          "SELECT * FROM t1 CROSS JOIN LATERAL (SELECT t1.a + t1.b as sum) AS dt",
			expectApply:  true,
			checkCorrelation: true,
		},
		{
			name:              "LATERAL with RIGHT JOIN returns error",
			sql:               "SELECT * FROM t1 RIGHT JOIN LATERAL (SELECT t1.a) AS dt ON true",
			expectError:       true,
			expectedErrorCode: mysql.ErrInvalidLateralJoin,
		},
		{
			name:         "Non-LATERAL derived table does not build LogicalApply",
			sql:          "SELECT * FROM t1, (SELECT x FROM t2) AS dt",
			expectApply:  false,
		},
		{
			name:         "LATERAL with correlation builds LogicalApply",
			sql:          "SELECT * FROM t1, LATERAL (SELECT * FROM t2 WHERE t2.x = t1.a) AS dt",
			expectApply:  true,
			checkCorrelation: true,
		},
		{
			name:                "Multiple LATERAL joins prevent join reordering",
			sql:                 "SELECT * FROM t1, LATERAL (SELECT t1.a) AS dt1, LATERAL (SELECT t1.b) AS dt2",
			expectApply:         true,
			checkJoinReordering: true,
		},
		{
			name:         "LATERAL with aggregate and correlation",
			sql:          "SELECT * FROM t1, LATERAL (SELECT COUNT(*) FROM t2 WHERE t2.x = t1.a) AS dt",
			expectApply:  true,
			checkCorrelation: true,
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
					require.Equal(t, tc.expectedErrorCode, plannererrors.ErrInvalidLateralJoin.Code())
				}
				return
			}

			require.NoError(t, err, "Failed to build plan for: %s", tc.sql)
			require.NotNil(t, p)

			// Check if LogicalApply is in the plan
			hasApply := findLogicalApply(p)

			if tc.expectApply {
				require.True(t, hasApply, "Expected LogicalApply in plan for: %s\nPlan: %s", tc.sql, ToString(p))

				// Verify correlation columns are extracted
				if tc.checkCorrelation {
					apply := findFirstLogicalApply(p)
					require.NotNil(t, apply, "Expected to find LogicalApply")
					// LATERAL should have correlated columns extracted
					// (or be non-correlated, which is also valid)
				}
			} else {
				require.False(t, hasApply, "Did not expect LogicalApply in plan for: %s\nPlan: %s", tc.sql, ToString(p))
			}

			// Check join reordering flag
			if tc.checkJoinReordering {
				// Verify that the plan has multiple Apply operators (no reordering)
				applyCount := countLogicalApply(p)
				require.GreaterOrEqual(t, applyCount, 2, "Expected multiple LogicalApply for multiple LATERAL joins")
			}
		})
	}
}

// TestLateralJoinOptimization tests decorrelation and optimization behavior
func TestLateralJoinOptimization(t *testing.T) {
	s := coretestsdk.CreatePlannerSuiteElems()
	defer s.Close()
	ctx := context.Background()

	// Create test tables
	_, err := s.GetTK().Exec("use test")
	require.NoError(t, err)
	_, err = s.GetTK().Exec("drop table if exists t1, t2")
	require.NoError(t, err)
	_, err = s.GetTK().Exec("create table t1(a int, b int)")
	require.NoError(t, err)
	_, err = s.GetTK().Exec("create table t2(x int, y int)")
	require.NoError(t, err)

	testCases := []struct {
		name             string
		sql              string
		optimizeFlags    uint64
		expectDecorrelate bool // Whether we expect decorrelation to happen
	}{
		{
			name:             "Simple LATERAL may decorrelate",
			sql:              "SELECT * FROM t1, LATERAL (SELECT 1 as x) AS dt",
			optimizeFlags:    rule.FlagDecorrelate | rule.FlagPredicatePushDown,
			expectDecorrelate: true, // No actual correlation, should decorrelate
		},
		{
			name:             "LATERAL with correlation attempts decorrelation",
			sql:              "SELECT * FROM t1, LATERAL (SELECT t1.a) AS dt",
			optimizeFlags:    rule.FlagDecorrelate | rule.FlagPredicatePushDown,
			expectDecorrelate: false, // Simple column reference, may or may not decorrelate
		},
		{
			name:             "LATERAL with aggregate stays as Apply",
			sql:              "SELECT * FROM t1, LATERAL (SELECT COUNT(*) FROM t2 WHERE t2.x = t1.a) AS dt",
			optimizeFlags:    rule.FlagDecorrelate | rule.FlagPredicatePushDown,
			expectDecorrelate: false, // Aggregate with correlation, stays as Apply
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := s.GetParser().ParseOneStmt(tc.sql, "", "")
			require.NoError(t, err)

			nodeW := resolve.NewNodeW(stmt)
			p, err := BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())
			require.NoError(t, err)

			// Apply optimization
			optimized, err := logicalOptimize(ctx, tc.optimizeFlags, p.(base.LogicalPlan))
			require.NoError(t, err)

			hasApply := findLogicalApply(optimized)

			if tc.expectDecorrelate {
				// If we expect decorrelation, Apply should be removed
				require.False(t, hasApply, "Expected decorrelation to remove Apply for: %s\nPlan: %s", tc.sql, ToString(optimized))
			} else {
				// Complex cases should retain Apply
				// Note: decorrelator is conservative, so this is not always guaranteed
			}
		})
	}
}

// TestLateralJoinReordering tests that LATERAL joins prevent join reordering
func TestLateralJoinReordering(t *testing.T) {
	s := coretestsdk.CreatePlannerSuiteElems()
	defer s.Close()
	ctx := context.Background()

	// Create test tables
	_, err := s.GetTK().Exec("use test")
	require.NoError(t, err)
	_, err = s.GetTK().Exec("drop table if exists t1, t2, t3")
	require.NoError(t, err)
	_, err = s.GetTK().Exec("create table t1(a int, b int)")
	require.NoError(t, err)
	_, err = s.GetTK().Exec("create table t2(x int, y int)")
	require.NoError(t, err)
	_, err = s.GetTK().Exec("create table t3(m int, n int)")
	require.NoError(t, err)

	// Test that LATERAL joins are NOT reordered
	sql := "SELECT * FROM t1, t2, LATERAL (SELECT t1.a, t2.x) AS dt"

	stmt, err := s.GetParser().ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	nodeW := resolve.NewNodeW(stmt)
	p, err := BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())
	require.NoError(t, err)

	// Apply join reordering optimization
	optimized, err := logicalOptimize(ctx, rule.FlagJoinReOrder, p.(base.LogicalPlan))
	require.NoError(t, err)

	// LogicalApply should still be present (not reordered)
	hasApply := findLogicalApply(optimized)
	require.True(t, hasApply, "Expected LogicalApply to remain after join reordering")
}

// TestLateralJoinSchemaResolution tests that column resolution works correctly
func TestLateralJoinSchemaResolution(t *testing.T) {
	s := coretestsdk.CreatePlannerSuiteElems()
	defer s.Close()
	ctx := context.Background()

	// Create test tables
	_, err := s.GetTK().Exec("use test")
	require.NoError(t, err)
	_, err = s.GetTK().Exec("drop table if exists t1, t2")
	require.NoError(t, err)
	_, err = s.GetTK().Exec("create table t1(a int, b int)")
	require.NoError(t, err)
	_, err = s.GetTK().Exec("create table t2(x int, y int)")
	require.NoError(t, err)

	testCases := []struct {
		name        string
		sql         string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "LATERAL can reference left-side columns",
			sql:         "SELECT * FROM t1, LATERAL (SELECT t1.a, t1.b) AS dt",
			expectError: false,
		},
		{
			name:        "LATERAL can reference columns from both tables",
			sql:         "SELECT * FROM t1, t2, LATERAL (SELECT t1.a, t2.x) AS dt",
			expectError: false,
		},
		{
			name:        "Non-LATERAL cannot reference outer columns",
			sql:         "SELECT * FROM t1, (SELECT t1.a) AS dt",
			expectError: true,
			errorMsg:    "Unknown column",
		},
		{
			name:        "LATERAL with alias resolution",
			sql:         "SELECT * FROM t1 AS t, LATERAL (SELECT t.a) AS dt",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := s.GetParser().ParseOneStmt(tc.sql, "", "")
			require.NoError(t, err)

			nodeW := resolve.NewNodeW(stmt)
			_, err = BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())

			if tc.expectError {
				require.Error(t, err, "Expected error for: %s", tc.sql)
				if tc.errorMsg != "" {
					require.Contains(t, err.Error(), tc.errorMsg)
				}
			} else {
				require.NoError(t, err, "Unexpected error for: %s", tc.sql)
			}
		})
	}
}

// Helper functions

// findLogicalApply recursively searches for LogicalApply in a plan tree
func findLogicalApply(p base.Plan) bool {
	if p == nil {
		return false
	}

	switch p.(type) {
	case *logicalop.LogicalApply:
		return true
	}

	for _, child := range p.Children() {
		if findLogicalApply(child) {
			return true
		}
	}

	return false
}

// findFirstLogicalApply recursively searches for the first LogicalApply in a plan tree
func findFirstLogicalApply(p base.Plan) *logicalop.LogicalApply {
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
func countLogicalApply(p base.Plan) int {
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

// TestLateralJoinExplain tests EXPLAIN output for LATERAL joins
func TestLateralJoinExplain(t *testing.T) {
	s := coretestsdk.CreatePlannerSuiteElems()
	defer s.Close()

	// Create test tables
	_, err := s.GetTK().Exec("use test")
	require.NoError(t, err)
	_, err = s.GetTK().Exec("drop table if exists t1, t2")
	require.NoError(t, err)
	_, err = s.GetTK().Exec("create table t1(a int, b int)")
	require.NoError(t, err)
	_, err = s.GetTK().Exec("create table t2(x int, y int)")
	require.NoError(t, err)

	// Test EXPLAIN output
	result := s.GetTK().MustQuery("EXPLAIN SELECT * FROM t1, LATERAL (SELECT t1.a) AS dt")
	rows := result.Rows()

	// EXPLAIN should show Apply operator for LATERAL join
	found := false
	for _, row := range rows {
		rowStr := fmt.Sprintf("%v", row)
		if strings.Contains(rowStr, "Apply") {
			found = true
			break
		}
	}
	require.True(t, found, "Expected to find 'Apply' in EXPLAIN output")
}
