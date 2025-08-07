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
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestBaseSingleGroupJoinOrderSolver tests the base solver functionality
func TestBaseSingleGroupJoinOrderSolver(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test tables
	leftTable := &logicalop.LogicalTableScan{}
	leftTable.Init(sctx, 0)
	leftTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	rightTable := &logicalop.LogicalTableScan{}
	rightTable.Init(sctx, 0)
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
			{JoinType: logicalop.InnerJoin},
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

	leftTable := &logicalop.LogicalTableScan{}
	leftTable.Init(sctx, 0)
	leftTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{leftCol},
	})

	rightTable := &logicalop.LogicalTableScan{}
	rightTable.Init(sctx, 0)
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
			{JoinType: logicalop.InnerJoin},
		},
	}

	solver := NewBaseSingleGroupJoinOrderSolver(sctx, group)

	// Test CheckConnection
	leftNode, rightNode, usedEdges, joinType := solver.CheckConnection(leftTable, rightTable)
	require.Equal(t, leftTable, leftNode)
	require.Equal(t, rightTable, rightNode)
	require.Len(t, usedEdges, 1)
	require.Equal(t, logicalop.InnerJoin, joinType.JoinType)
}

// TestBaseSingleGroupJoinOrderSolver_MakeJoin tests the MakeJoin method
func TestBaseSingleGroupJoinOrderSolver_MakeJoin(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test tables
	leftTable := &logicalop.LogicalTableScan{}
	leftTable.Init(sctx, 0)
	leftTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	rightTable := &logicalop.LogicalTableScan{}
	rightTable.Init(sctx, 0)
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
			{JoinType: logicalop.InnerJoin},
		},
	}

	solver := NewBaseSingleGroupJoinOrderSolver(sctx, group)

	// Test MakeJoin
	eqEdges := []*expression.ScalarFunction{}
	joinType := &JoinTypeWithExtMsg{JoinType: logicalop.InnerJoin}
	
	result, remainingConds := solver.MakeJoin(leftTable, rightTable, eqEdges, joinType)
	require.NotNil(t, result)
	require.Len(t, remainingConds, 0)
}

// TestBaseSingleGroupJoinOrderSolver_BaseNodeCumCost tests the BaseNodeCumCost method
func TestBaseSingleGroupJoinOrderSolver_BaseNodeCumCost(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test table
	table := &logicalop.LogicalTableScan{}
	table.Init(sctx, 0)
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

	solver := NewBaseSingleGroupJoinOrderSolver(sctx, group)

	// Test BaseNodeCumCost
	cost := solver.BaseNodeCumCost(table)
	require.GreaterOrEqual(t, cost, float64(0))
}

// TestDPJoinOrderSolver tests the DP solver
func TestDPJoinOrderSolver(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test tables
	leftTable := &logicalop.LogicalTableScan{}
	leftTable.Init(sctx, 0)
	leftTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	rightTable := &logicalop.LogicalTableScan{}
	rightTable.Init(sctx, 0)
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
			{JoinType: logicalop.InnerJoin},
		},
	}

	// Create DP solver
	solver := NewDPJoinOrderSolver(sctx, group)
	require.NotNil(t, solver)

	// Test Solve
	result, err := solver.Solve()
	require.NoError(t, err)
	require.NotNil(t, result)
}

// TestGreedyJoinOrderSolver tests the Greedy solver
func TestGreedyJoinOrderSolver(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test tables
	leftTable := &logicalop.LogicalTableScan{}
	leftTable.Init(sctx, 0)
	leftTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	rightTable := &logicalop.LogicalTableScan{}
	rightTable.Init(sctx, 0)
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
			{JoinType: logicalop.InnerJoin},
		},
	}

	// Create Greedy solver
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
	leftTable := &logicalop.LogicalTableScan{}
	leftTable.Init(sctx, 0)
	leftTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	rightTable := &logicalop.LogicalTableScan{}
	rightTable.Init(sctx, 0)
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
			{JoinType: logicalop.InnerJoin},
		},
	}

	// Test DP solver interface
	var dpSolver JoinOrderSolver = NewDPJoinOrderSolver(sctx, group)
	result, err := dpSolver.Solve()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Test Greedy solver interface
	var greedySolver JoinOrderSolver = NewGreedyJoinOrderSolver(sctx, group)
	result, err = greedySolver.Solve()
	require.NoError(t, err)
	require.NotNil(t, result)
}

// Benchmark tests for performance comparison
func BenchmarkDPJoinOrderSolver_Solve(b *testing.B) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test tables
	leftTable := &logicalop.LogicalTableScan{}
	leftTable.Init(sctx, 0)
	leftTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	rightTable := &logicalop.LogicalTableScan{}
	rightTable.Init(sctx, 0)
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
			{JoinType: logicalop.InnerJoin},
		},
	}

	solver := NewDPJoinOrderSolver(sctx, group)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := solver.Solve()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGreedyJoinOrderSolver_Solve(b *testing.B) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create test tables
	leftTable := &logicalop.LogicalTableScan{}
	leftTable.Init(sctx, 0)
	leftTable.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	rightTable := &logicalop.LogicalTableScan{}
	rightTable.Init(sctx, 0)
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
			{JoinType: logicalop.InnerJoin},
		},
	}

	solver := NewGreedyJoinOrderSolver(sctx, group)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := solver.Solve()
		if err != nil {
			b.Fatal(err)
		}
	}
} 