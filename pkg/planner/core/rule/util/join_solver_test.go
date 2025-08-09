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

package util

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// MockLogicalPlan is a mock implementation for testing
type MockLogicalPlan struct {
	base.LogicalPlan
	schema *expression.Schema
	children []base.LogicalPlan
	statsInfo *base.StatsInfo
}

func (m *MockLogicalPlan) Schema() *expression.Schema {
	return m.schema
}

func (m *MockLogicalPlan) SetSchema(schema *expression.Schema) {
	m.schema = schema
}

func (m *MockLogicalPlan) Children() []base.LogicalPlan {
	return m.children
}

func (m *MockLogicalPlan) SetChildren(children ...base.LogicalPlan) {
	m.children = children
}

func (m *MockLogicalPlan) StatsInfo() *base.StatsInfo {
	if m.statsInfo == nil {
		m.statsInfo = &base.StatsInfo{RowCount: 1000}
	}
	return m.statsInfo
}

func (m *MockLogicalPlan) QueryBlockOffset() int {
	return 0
}

func (m *MockLogicalPlan) SCtx() base.PlanContext {
	return sessionctx.NewContext()
}

func (m *MockLogicalPlan) RecursiveDeriveStats(childStats []*base.StatsInfo) (*base.StatsInfo, bool, error) {
	return m.StatsInfo(), false, nil
}

// TestBaseSingleGroupJoinOrderSolver tests the base solver functionality
func TestBaseSingleGroupJoinOrderSolver(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test tables
	leftTable := &MockLogicalPlan{}
	leftTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	rightTable := &MockLogicalPlan{}
	rightTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	// Create join group
	group := &JoinGroupResult{
		Group: []base.LogicalPlan{leftTable, rightTable},
		EqEdges: []*expression.ScalarFunction{
			expression.NewFunctionInternal(sctx.GetExprCtx(), ast.EQ, types.NewFieldType(ast.TypeLonglong), 
				leftTable.Schema().Columns[0], rightTable.Schema().Columns[0]).(*expression.ScalarFunction),
		},
		OtherConds: []expression.Expression{},
		JoinTypes: []*JoinTypeWithExtMsg{
			{JoinType: 0}, // 0 = InnerJoin
		},
	}

	// Create base solver
	solver := NewBaseSingleGroupJoinOrderSolver(sctx, group)
	require.NotNil(t, solver)
	require.Equal(t, sctx, solver.ctx)
	require.Equal(t, group, solver.group)
}

// TestBaseSingleGroupJoinOrderSolver_CheckConnection tests the CheckConnection method
func TestBaseSingleGroupJoinOrderSolver_CheckConnection(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test tables with columns
	leftCol := &expression.Column{RetType: types.NewFieldType(ast.TypeLonglong)}
	rightCol := &expression.Column{RetType: types.NewFieldType(ast.TypeLonglong)}

	leftTable := &MockLogicalPlan{}
	leftTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{leftCol},
	})

	rightTable := &MockLogicalPlan{}
	rightTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{rightCol},
	})

	// Create join group with equal condition
	eqCond := expression.NewFunctionInternal(sctx.GetExprCtx(), ast.EQ, types.NewFieldType(ast.TypeLonglong), leftCol, rightCol).(*expression.ScalarFunction)
	group := &JoinGroupResult{
		Group: []base.LogicalPlan{leftTable, rightTable},
		EqEdges: []*expression.ScalarFunction{eqCond},
		OtherConds: []expression.Expression{},
		JoinTypes: []*JoinTypeWithExtMsg{
			{JoinType: 0}, // 0 = InnerJoin
		},
	}

	// Create base solver
	solver := NewBaseSingleGroupJoinOrderSolver(sctx, group)

	// Test CheckConnection
	leftNode, rightNode, usedEdges, joinType := solver.CheckConnection(leftTable, rightTable)
	require.Equal(t, leftTable, leftNode)
	require.Equal(t, rightTable, rightNode)
	require.Len(t, usedEdges, 1)
	require.Equal(t, 0, joinType.JoinType) // 0 = InnerJoin
}

// TestBaseSingleGroupJoinOrderSolver_MakeJoin tests the MakeJoin method
func TestBaseSingleGroupJoinOrderSolver_MakeJoin(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test tables
	leftTable := &MockLogicalPlan{}
	leftTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	rightTable := &MockLogicalPlan{}
	rightTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	// Create join group
	group := &JoinGroupResult{
		Group: []base.LogicalPlan{leftTable, rightTable},
		EqEdges: []*expression.ScalarFunction{},
		OtherConds: []expression.Expression{},
		JoinTypes: []*JoinTypeWithExtMsg{
			{JoinType: 0}, // 0 = InnerJoin
		},
	}

	// Create base solver
	solver := NewBaseSingleGroupJoinOrderSolver(sctx, group)

	// Test MakeJoin
	joinType := &JoinTypeWithExtMsg{JoinType: 0} // 0 = InnerJoin
	join, remainConds := solver.MakeJoin(leftTable, rightTable, nil, joinType)
	require.NotNil(t, join)
	require.Len(t, remainConds, 0)
}

// TestBaseSingleGroupJoinOrderSolver_BaseNodeCumCost tests the BaseNodeCumCost method
func TestBaseSingleGroupJoinOrderSolver_BaseNodeCumCost(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test table
	table := &MockLogicalPlan{}
	table.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	// Create join group
	group := &JoinGroupResult{
		Group: []base.LogicalPlan{table},
		EqEdges: []*expression.ScalarFunction{},
		OtherConds: []expression.Expression{},
		JoinTypes: []*JoinTypeWithExtMsg{},
	}

	// Create base solver
	solver := NewBaseSingleGroupJoinOrderSolver(sctx, group)

	// Test BaseNodeCumCost
	cost := solver.BaseNodeCumCost(table)
	require.Greater(t, cost, float64(0))
}

// TestDPJoinOrderSolver tests the DP solver
func TestDPJoinOrderSolver(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test tables
	table1 := &MockLogicalPlan{}
	table1.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	table2 := &MockLogicalPlan{}
	table2.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	// Create join group
	group := &JoinGroupResult{
		Group: []base.LogicalPlan{table1, table2},
		EqEdges: []*expression.ScalarFunction{},
		OtherConds: []expression.Expression{},
		JoinTypes: []*JoinTypeWithExtMsg{},
	}

	// Create DP solver
	solver := NewDPJoinOrderSolver(sctx, group)
	require.NotNil(t, solver)

	// Test Solve
	result, err := solver.Solve()
	require.NoError(t, err)
	require.NotNil(t, result)
}

// TestGreedyJoinOrderSolver tests the greedy solver
func TestGreedyJoinOrderSolver(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test tables
	table1 := &MockLogicalPlan{}
	table1.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	table2 := &MockLogicalPlan{}
	table2.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	// Create join group
	group := &JoinGroupResult{
		Group: []base.LogicalPlan{table1, table2},
		EqEdges: []*expression.ScalarFunction{},
		OtherConds: []expression.Expression{},
		JoinTypes: []*JoinTypeWithExtMsg{},
	}

	// Create greedy solver
	solver := NewGreedyJoinOrderSolver(sctx, group)
	require.NotNil(t, solver)

	// Test Solve
	result, err := solver.Solve()
	require.NoError(t, err)
	require.NotNil(t, result)
}

// TestJoinOrderSolver_Interface tests the interface implementation
func TestJoinOrderSolver_Interface(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test tables
	table1 := &MockLogicalPlan{}
	table1.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	table2 := &MockLogicalPlan{}
	table2.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	// Create join group
	group := &JoinGroupResult{
		Group: []base.LogicalPlan{table1, table2},
		EqEdges: []*expression.ScalarFunction{},
		OtherConds: []expression.Expression{},
		JoinTypes: []*JoinTypeWithExtMsg{},
	}

	// Test DP solver interface
	dpSolver := NewDPJoinOrderSolver(sctx, group)
	var solver JoinOrderSolver = dpSolver
	result, err := solver.Solve()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Test greedy solver interface
	greedySolver := NewGreedyJoinOrderSolver(sctx, group)
	solver = greedySolver
	result, err = solver.Solve()
	require.NoError(t, err)
	require.NotNil(t, result)
}

// BenchmarkDPJoinOrderSolver_Solve benchmarks the DP solver
func BenchmarkDPJoinOrderSolver_Solve(b *testing.B) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test tables
	tables := make([]base.LogicalPlan, 4)
	for i := range tables {
		table := &MockLogicalPlan{}
		table.SetSchema(&expression.Schema{
			Columns: []*expression.Column{
				{RetType: types.NewFieldType(ast.TypeLonglong)},
			},
		})
		tables[i] = table
	}

	// Create join group
	group := &JoinGroupResult{
		Group: tables,
		EqEdges: []*expression.ScalarFunction{},
		OtherConds: []expression.Expression{},
		JoinTypes: []*JoinTypeWithExtMsg{},
	}

	// Create DP solver
	solver := NewDPJoinOrderSolver(sctx, group)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := solver.Solve()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGreedyJoinOrderSolver_Solve benchmarks the greedy solver
func BenchmarkGreedyJoinOrderSolver_Solve(b *testing.B) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test tables
	tables := make([]base.LogicalPlan, 8)
	for i := range tables {
		table := &MockLogicalPlan{}
		table.SetSchema(&expression.Schema{
			Columns: []*expression.Column{
				{RetType: types.NewFieldType(ast.TypeLonglong)},
			},
		})
		tables[i] = table
	}

	// Create join group
	group := &JoinGroupResult{
		Group: tables,
		EqEdges: []*expression.ScalarFunction{},
		OtherConds: []expression.Expression{},
		JoinTypes: []*JoinTypeWithExtMsg{},
	}

	// Create greedy solver
	solver := NewGreedyJoinOrderSolver(sctx, group)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := solver.Solve()
		if err != nil {
			b.Fatal(err)
		}
	}
} 