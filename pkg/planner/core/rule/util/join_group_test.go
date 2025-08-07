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

// TestExtractJoinGroups tests the ExtractJoinGroups function
func TestExtractJoinGroups(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Test case 1: Simple join
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

	groups := ExtractJoinGroups(join)
	require.Len(t, groups, 1)
	require.Len(t, groups[0].Group, 2)
	require.False(t, groups[0].HasOuterJoin)
}

// TestExtractJoinGroups_OuterJoin tests extraction with outer joins
func TestExtractJoinGroups_OuterJoin(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Test case: Outer join
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
		JoinType: logicalop.LeftOuterJoin,
	}
	join.Init(sctx, 0)
	join.SetChildren(leftTable, rightTable)

	groups := ExtractJoinGroups(join)
	require.Len(t, groups, 1)
	require.Len(t, groups[0].Group, 2)
	require.True(t, groups[0].HasOuterJoin)
}

// TestExtractJoinGroups_ComplexJoin tests extraction with complex join structure
func TestExtractJoinGroups_ComplexJoin(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create three tables
	table1 := &logicalop.LogicalTableScan{}
	table1.Init(sctx, 0)
	table1.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	table2 := &logicalop.LogicalTableScan{}
	table2.Init(sctx, 0)
	table2.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	table3 := &logicalop.LogicalTableScan{}
	table3.Init(sctx, 0)
	table3.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	// Create nested joins: (table1 JOIN table2) JOIN table3
	innerJoin := &logicalop.LogicalJoin{
		JoinType: logicalop.InnerJoin,
	}
	innerJoin.Init(sctx, 0)
	innerJoin.SetChildren(table1, table2)

	outerJoin := &logicalop.LogicalJoin{
		JoinType: logicalop.InnerJoin,
	}
	outerJoin.Init(sctx, 0)
	outerJoin.SetChildren(innerJoin, table3)

	groups := ExtractJoinGroups(outerJoin)
	require.Len(t, groups, 1)
	require.Len(t, groups[0].Group, 3)
	require.False(t, groups[0].HasOuterJoin)
}

// TestJoinGroupResult_CanReorder tests the CanReorder method
func TestJoinGroupResult_CanReorder(t *testing.T) {
	// Test case 1: Can reorder (inner joins, multiple tables)
	group := &JoinGroupResult{
		Group:         []base.LogicalPlan{&logicalop.LogicalTableScan{}, &logicalop.LogicalTableScan{}},
		HasOuterJoin:  false,
	}
	require.True(t, group.CanReorder())

	// Test case 2: Cannot reorder (outer joins)
	group.HasOuterJoin = true
	require.False(t, group.CanReorder())

	// Test case 3: Cannot reorder (single table)
	group.HasOuterJoin = false
	group.Group = []base.LogicalPlan{&logicalop.LogicalTableScan{}}
	require.False(t, group.CanReorder())

	// Test case 4: Cannot reorder (no tables)
	group.Group = []base.LogicalPlan{}
	require.False(t, group.CanReorder())
}

// TestExtractJoinGroups_NoJoins tests extraction when there are no joins
func TestExtractJoinGroups_NoJoins(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Test case: No joins
	tableScan := &logicalop.LogicalTableScan{}
	tableScan.Init(sctx, 0)
	tableScan.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	groups := ExtractJoinGroups(tableScan)
	require.Len(t, groups, 0)
}

// TestExtractJoinGroups_WithConditions tests extraction with join conditions
func TestExtractJoinGroups_WithConditions(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create tables with columns
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

	// Create join with conditions
	join := &logicalop.LogicalJoin{
		JoinType: logicalop.InnerJoin,
		EqualConditions: []*expression.ScalarFunction{
			expression.NewFunctionInternal(sctx.GetExprCtx(), ast.EQ, types.NewFieldType(ast.TypeLonglong), leftCol, rightCol).(*expression.ScalarFunction),
		},
		OtherConditions: []expression.Expression{
			expression.NewFunctionInternal(sctx.GetExprCtx(), ast.GT, types.NewFieldType(ast.TypeLonglong), leftCol, rightCol),
		},
	}
	join.Init(sctx, 0)
	join.SetChildren(leftTable, rightTable)

	groups := ExtractJoinGroups(join)
	require.Len(t, groups, 1)
	require.Len(t, groups[0].EqEdges, 1)
	require.Len(t, groups[0].OtherConds, 1)
	require.Len(t, groups[0].JoinTypes, 1)
	require.Equal(t, logicalop.InnerJoin, groups[0].JoinTypes[0].JoinType)
}

// Benchmark tests for performance
func BenchmarkExtractJoinGroups(b *testing.B) {
	// Create a mock session context
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &variable.StatementContext{}

	// Create a complex join structure for benchmarking
	table1 := &logicalop.LogicalTableScan{}
	table1.Init(sctx, 0)
	table1.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	table2 := &logicalop.LogicalTableScan{}
	table2.Init(sctx, 0)
	table2.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	table3 := &logicalop.LogicalTableScan{}
	table3.Init(sctx, 0)
	table3.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	// Create nested joins
	innerJoin := &logicalop.LogicalJoin{
		JoinType: logicalop.InnerJoin,
	}
	innerJoin.Init(sctx, 0)
	innerJoin.SetChildren(table1, table2)

	outerJoin := &logicalop.LogicalJoin{
		JoinType: logicalop.InnerJoin,
	}
	outerJoin.Init(sctx, 0)
	outerJoin.SetChildren(innerJoin, table3)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExtractJoinGroups(outerJoin)
	}
} 