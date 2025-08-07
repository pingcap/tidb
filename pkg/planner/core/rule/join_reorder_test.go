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
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestJoinReorderRule_Match tests the Match method of JoinReorderRule
func TestJoinReorderRule_Match(t *testing.T) {
	rule := &JoinReorderRule{}
	ctx := context.Background()

	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Test case 1: Plan with join should match
	join := &logicalop.LogicalJoin{
		JoinType: logicalop.InnerJoin,
	}
	join.Init(sctx, 0)
	
	matched, err := rule.Match(ctx, join)
	require.NoError(t, err)
	require.True(t, matched)

	// Test case 2: Plan without join should not match
	tableScan := &logicalop.LogicalTableScan{}
	tableScan.Init(sctx, 0)
	
	matched, err = rule.Match(ctx, tableScan)
	require.NoError(t, err)
	require.False(t, matched)

	// Test case 3: Plan with nested joins should match
	innerJoin := &logicalop.LogicalJoin{
		JoinType: logicalop.InnerJoin,
	}
	innerJoin.Init(sctx, 0)
	outerJoin := &logicalop.LogicalJoin{
		JoinType: logicalop.LeftOuterJoin,
	}
	outerJoin.Init(sctx, 0)
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

	join := &logicalop.LogicalJoin{
		JoinType: logicalop.InnerJoin,
	}
	join.Init(sctx, 0)
	join.SetChildren(leftTable, rightTable)

	result, changed, err := rule.Optimize(ctx, join, opt)
	require.NoError(t, err)
	require.NotNil(t, result)
	// The plan should not change for simple cases without optimization
	require.False(t, changed)
}

// TestJoinReOrderSolver_Optimize tests the JoinReOrderSolver
func TestJoinReOrderSolver_Optimize(t *testing.T) {
	ctx := context.Background()
	opt := optimizetrace.DefaultLogicalOptimizeOption()

	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	solver := &JoinReOrderSolver{
		ctx: sctx,
	}

	// Test case 1: Simple join without optimization
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

	join := &logicalop.LogicalJoin{
		JoinType: logicalop.InnerJoin,
	}
	join.Init(sctx, 0)
	join.SetChildren(leftTable, rightTable)

	result, changed, err := solver.Optimize(ctx, join, opt)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.False(t, changed)
}

// TestJoinReOrderSolver_containsReorderableJoins tests the containsReorderableJoins method
func TestJoinReOrderSolver_containsReorderableJoins(t *testing.T) {
	rule := &JoinReorderRule{}

	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Test case 1: Plan with join
	join := &logicalop.LogicalJoin{
		JoinType: logicalop.InnerJoin,
	}
	join.Init(sctx, 0)
	
	hasJoins := rule.containsReorderableJoins(join)
	require.True(t, hasJoins)

	// Test case 2: Plan without join
	tableScan := &logicalop.LogicalTableScan{}
	tableScan.Init(sctx, 0)
	
	hasJoins = rule.containsReorderableJoins(tableScan)
	require.False(t, hasJoins)

	// Test case 3: Plan with nested joins
	innerJoin := &logicalop.LogicalJoin{
		JoinType: logicalop.InnerJoin,
	}
	innerJoin.Init(sctx, 0)
	outerJoin := &logicalop.LogicalJoin{
		JoinType: logicalop.LeftOuterJoin,
	}
	outerJoin.Init(sctx, 0)
	outerJoin.SetChildren(innerJoin, tableScan)
	
	hasJoins = rule.containsReorderableJoins(outerJoin)
	require.True(t, hasJoins)
}

// TestJoinReOrderSolver_replaceJoinGroup tests the replaceJoinGroup method
func TestJoinReOrderSolver_replaceJoinGroup(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	solver := &JoinReOrderSolver{
		ctx: sctx,
	}

	// Create a simple plan
	tableScan := &logicalop.LogicalTableScan{}
	tableScan.Init(sctx, 0)

	// Create a join group result
	group := &util.JoinGroupResult{
		Group: []base.LogicalPlan{tableScan},
	}

	// Create a new join to replace
	newJoin := &logicalop.LogicalJoin{
		JoinType: logicalop.InnerJoin,
	}
	newJoin.Init(sctx, 0)
	newJoin.SetChildren(tableScan, tableScan)

	// Test replaceJoinGroup
	result := solver.replaceJoinGroup(tableScan, group, newJoin)
	require.NotNil(t, result)
}

// Benchmark tests for performance comparison
func BenchmarkJoinReorderRule_Optimize(b *testing.B) {
	rule := &JoinReorderRule{}
	ctx := context.Background()
	opt := optimizetrace.DefaultLogicalOptimizeOption()

	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create a simple join plan
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

	join := &logicalop.LogicalJoin{
		JoinType: logicalop.InnerJoin,
	}
	join.Init(sctx, 0)
	join.SetChildren(leftTable, rightTable)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := rule.Optimize(ctx, join, opt)
		if err != nil {
			b.Fatal(err)
		}
	}
} 