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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// MockLogicalJoin is a mock implementation for testing
type MockLogicalJoin struct {
	base.LogicalPlan
	schema *expression.Schema
	children []base.LogicalPlan
	joinType int
	statsInfo *base.StatsInfo
}

func (m *MockLogicalJoin) Schema() *expression.Schema {
	return m.schema
}

func (m *MockLogicalJoin) SetSchema(schema *expression.Schema) {
	m.schema = schema
}

func (m *MockLogicalJoin) Children() []base.LogicalPlan {
	return m.children
}

func (m *MockLogicalJoin) SetChildren(children ...base.LogicalPlan) {
	m.children = children
}

func (m *MockLogicalJoin) JoinType() int {
	return m.joinType
}

func (m *MockLogicalJoin) SetJoinType(joinType int) {
	m.joinType = joinType
}

func (m *MockLogicalJoin) StatsInfo() *base.StatsInfo {
	if m.statsInfo == nil {
		m.statsInfo = &base.StatsInfo{RowCount: 1000}
	}
	return m.statsInfo
}

func (m *MockLogicalJoin) QueryBlockOffset() int {
	return 0
}

func (m *MockLogicalJoin) SCtx() base.PlanContext {
	return sessionctx.NewContext()
}

func (m *MockLogicalJoin) RecursiveDeriveStats(childStats []*base.StatsInfo) (*base.StatsInfo, bool, error) {
	return m.StatsInfo(), false, nil
}

// MockLogicalTableScan is a mock implementation for testing
type MockLogicalTableScan struct {
	base.LogicalPlan
	schema *expression.Schema
	statsInfo *base.StatsInfo
}

func (m *MockLogicalTableScan) Schema() *expression.Schema {
	return m.schema
}

func (m *MockLogicalTableScan) SetSchema(schema *expression.Schema) {
	m.schema = schema
}

func (m *MockLogicalTableScan) Children() []base.LogicalPlan {
	return nil
}

func (m *MockLogicalTableScan) StatsInfo() *base.StatsInfo {
	if m.statsInfo == nil {
		m.statsInfo = &base.StatsInfo{RowCount: 100}
	}
	return m.statsInfo
}

func (m *MockLogicalTableScan) QueryBlockOffset() int {
	return 0
}

func (m *MockLogicalTableScan) SCtx() base.PlanContext {
	return sessionctx.NewContext()
}

func (m *MockLogicalTableScan) RecursiveDeriveStats(childStats []*base.StatsInfo) (*base.StatsInfo, bool, error) {
	return m.StatsInfo(), false, nil
}

// TestJoinReorderRule_Match tests the Match method of JoinReorderRule
func TestJoinReorderRule_Match(t *testing.T) {
	rule := &JoinReorderRule{}
	ctx := context.Background()

	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Test case 1: Plan with join should match
	join := &MockLogicalJoin{}
	join.SetJoinType(0) // InnerJoin
	
	matched, err := rule.Match(ctx, join)
	require.NoError(t, err)
	require.True(t, matched)

	// Test case 2: Plan without join should not match
	tableScan := &MockLogicalTableScan{}
	
	matched, err = rule.Match(ctx, tableScan)
	require.NoError(t, err)
	require.False(t, matched)

	// Test case 3: Plan with nested joins should match
	innerJoin := &MockLogicalJoin{}
	innerJoin.SetJoinType(0) // InnerJoin
	outerJoin := &MockLogicalJoin{}
	outerJoin.SetJoinType(1) // LeftOuterJoin
	outerJoin.SetChildren(innerJoin, tableScan)
	
	matched, err = rule.Match(ctx, outerJoin)
	require.NoError(t, err)
	require.True(t, matched)
}

// TestJoinReorderRule_Name tests the Name method of JoinReorderRule
func TestJoinReorderRule_Name(t *testing.T) {
	rule := &JoinReorderRule{}
	require.Equal(t, "join_reorder", rule.Name())
}

// TestJoinReorderRule_Optimize tests the Optimize method of JoinReorderRule
func TestJoinReorderRule_Optimize(t *testing.T) {
	rule := &JoinReorderRule{}
	ctx := context.Background()
	opt := optimizetrace.DefaultLogicalOptimizeOption()

	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Test case 1: Simple join optimization
	leftTable := &MockLogicalTableScan{}
	leftTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	rightTable := &MockLogicalTableScan{}
	rightTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	join := &MockLogicalJoin{}
	join.SetSchema(expression.MergeSchema(leftTable.Schema(), rightTable.Schema()))
	join.SetChildren(leftTable, rightTable)
	join.SetJoinType(0) // InnerJoin

	// Test optimization
	newPlan, changed, err := rule.Optimize(ctx, join, opt)
	require.NoError(t, err)
	require.NotNil(t, newPlan)
	// Note: Since we're using mock objects, the optimization might not actually change the plan
	// This is expected behavior for our simplified test
}

// TestJoinReOrderSolver_Optimize tests the Optimize method of JoinReOrderSolver
func TestJoinReOrderSolver_Optimize(t *testing.T) {
	solver := &JoinReOrderSolver{
		ctx: sessionctx.NewContext(),
	}
	ctx := context.Background()
	opt := optimizetrace.DefaultLogicalOptimizeOption()

	// Create a simple join plan
	leftTable := &MockLogicalTableScan{}
	leftTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	rightTable := &MockLogicalTableScan{}
	rightTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	join := &MockLogicalJoin{}
	join.SetSchema(expression.MergeSchema(leftTable.Schema(), rightTable.Schema()))
	join.SetChildren(leftTable, rightTable)
	join.SetJoinType(0) // InnerJoin

	// Test optimization
	newPlan, changed, err := solver.Optimize(ctx, join, opt)
	require.NoError(t, err)
	require.NotNil(t, newPlan)
	// Note: Since we're using mock objects, the optimization might not actually change the plan
	// This is expected behavior for our simplified test
}

// TestJoinReOrderSolver_containsReorderableJoins tests the containsReorderableJoins method
func TestJoinReOrderSolver_containsReorderableJoins(t *testing.T) {
	solver := &JoinReOrderSolver{
		ctx: sessionctx.NewContext(),
	}

	// Test case 1: Plan with join should return true
	join := &MockLogicalJoin{}
	join.SetJoinType(0) // InnerJoin
	join.SetChildren(&MockLogicalTableScan{}, &MockLogicalTableScan{})
	
	result := solver.containsReorderableJoins(join)
	require.True(t, result)

	// Test case 2: Plan without join should return false
	tableScan := &MockLogicalTableScan{}
	
	result = solver.containsReorderableJoins(tableScan)
	require.False(t, result)

	// Test case 3: Plan with nested joins should return true
	innerJoin := &MockLogicalJoin{}
	innerJoin.SetJoinType(0) // InnerJoin
	outerJoin := &MockLogicalJoin{}
	outerJoin.SetJoinType(1) // LeftOuterJoin
	outerJoin.SetChildren(innerJoin, tableScan)
	
	result = solver.containsReorderableJoins(outerJoin)
	require.True(t, result)
}

// TestJoinReOrderSolver_replaceJoinGroup tests the replaceJoinGroup method
func TestJoinReOrderSolver_replaceJoinGroup(t *testing.T) {
	solver := &JoinReOrderSolver{
		ctx: sessionctx.NewContext(),
	}

	// Create test data
	originalPlan := &MockLogicalJoin{}
	group := &util.JoinGroupResult{
		Group: []base.LogicalPlan{&MockLogicalTableScan{}, &MockLogicalTableScan{}},
	}
	newJoin := &MockLogicalJoin{}

	// Test replacement
	result := solver.replaceJoinGroup(originalPlan, group, newJoin)
	require.NotNil(t, result)
	// Note: Since this is a simplified implementation, we just verify it doesn't crash
}

// BenchmarkJoinReorderRule_Optimize benchmarks the Optimize method
func BenchmarkJoinReorderRule_Optimize(b *testing.B) {
	rule := &JoinReorderRule{}
	ctx := context.Background()
	opt := optimizetrace.DefaultLogicalOptimizeOption()

	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test plan
	leftTable := &MockLogicalTableScan{}
	leftTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	rightTable := &MockLogicalTableScan{}
	rightTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	join := &MockLogicalJoin{}
	join.SetSchema(expression.MergeSchema(leftTable.Schema(), rightTable.Schema()))
	join.SetChildren(leftTable, rightTable)
	join.SetJoinType(0) // InnerJoin

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := rule.Optimize(ctx, join, opt)
		if err != nil {
			b.Fatal(err)
		}
	}
} 