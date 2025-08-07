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

// MockLogicalJoin is a mock implementation for testing
type MockLogicalJoin struct {
	base.LogicalPlan
	schema *expression.Schema
	children []base.LogicalPlan
	joinType int
	equalConditions []*expression.ScalarFunction
	otherConditions []expression.Expression
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

func (m *MockLogicalJoin) EqualConditions() []*expression.ScalarFunction {
	return m.equalConditions
}

func (m *MockLogicalJoin) SetEqualConditions(conditions []*expression.ScalarFunction) {
	m.equalConditions = conditions
}

func (m *MockLogicalJoin) OtherConditions() []expression.Expression {
	return m.otherConditions
}

func (m *MockLogicalJoin) SetOtherConditions(conditions []expression.Expression) {
	m.otherConditions = conditions
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

// TestExtractJoinGroups_SimpleJoin tests extracting join groups from a simple join
func TestExtractJoinGroups_SimpleJoin(t *testing.T) {
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

	// Create join
	join := &MockLogicalJoin{}
	join.SetSchema(expression.MergeSchema(leftTable.Schema(), rightTable.Schema()))
	join.SetChildren(leftTable, rightTable)
	join.SetJoinType(0) // InnerJoin

	// Extract join groups
	groups := ExtractJoinGroups(join)
	require.Len(t, groups, 1)
	require.Len(t, groups[0].Group, 2)
	require.True(t, groups[0].CanReorder())
}

// TestExtractJoinGroups_OuterJoin tests extracting join groups from an outer join
func TestExtractJoinGroups_OuterJoin(t *testing.T) {
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

	// Create outer join
	join := &MockLogicalJoin{}
	join.SetSchema(expression.MergeSchema(leftTable.Schema(), rightTable.Schema()))
	join.SetChildren(leftTable, rightTable)
	join.SetJoinType(1) // LeftOuterJoin

	// Extract join groups
	groups := ExtractJoinGroups(join)
	require.Len(t, groups, 1)
	require.Len(t, groups[0].Group, 2)
	require.False(t, groups[0].CanReorder()) // Outer joins cannot be reordered
}

// TestExtractJoinGroups_ComplexJoin tests extracting join groups from a complex join
func TestExtractJoinGroups_ComplexJoin(t *testing.T) {
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

	table3 := &MockLogicalPlan{}
	table3.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	// Create nested joins
	innerJoin := &MockLogicalJoin{}
	innerJoin.SetSchema(expression.MergeSchema(table1.Schema(), table2.Schema()))
	innerJoin.SetChildren(table1, table2)
	innerJoin.SetJoinType(0) // InnerJoin

	outerJoin := &MockLogicalJoin{}
	outerJoin.SetSchema(expression.MergeSchema(innerJoin.Schema(), table3.Schema()))
	outerJoin.SetChildren(innerJoin, table3)
	outerJoin.SetJoinType(0) // InnerJoin

	// Extract join groups
	groups := ExtractJoinGroups(outerJoin)
	require.Len(t, groups, 1)
	require.Len(t, groups[0].Group, 3)
	require.True(t, groups[0].CanReorder())
}

// TestExtractJoinGroups_NoJoins tests extracting join groups from a plan without joins
func TestExtractJoinGroups_NoJoins(t *testing.T) {
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

	// Extract join groups
	groups := ExtractJoinGroups(table)
	require.Len(t, groups, 0)
}

// TestExtractJoinGroups_WithConditions tests extracting join groups with conditions
func TestExtractJoinGroups_WithConditions(t *testing.T) {
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

	// Create equal condition
	eqCond := expression.NewFunctionInternal(sctx.GetExprCtx(), ast.EQ, types.NewFieldType(ast.TypeLonglong), 
		leftTable.Schema().Columns[0], rightTable.Schema().Columns[0]).(*expression.ScalarFunction)

	// Create join with conditions
	join := &MockLogicalJoin{}
	join.SetSchema(expression.MergeSchema(leftTable.Schema(), rightTable.Schema()))
	join.SetChildren(leftTable, rightTable)
	join.SetJoinType(0) // InnerJoin
	join.SetEqualConditions([]*expression.ScalarFunction{eqCond})

	// Extract join groups
	groups := ExtractJoinGroups(join)
	require.Len(t, groups, 1)
	require.Len(t, groups[0].Group, 2)
	require.Len(t, groups[0].EqEdges, 1)
	require.True(t, groups[0].CanReorder())
}

// TestJoinGroupResult_CanReorder tests the CanReorder method
func TestJoinGroupResult_CanReorder(t *testing.T) {
	// Test case 1: Single table - cannot reorder
	group := &JoinGroupResult{
		Group: []base.LogicalPlan{&MockLogicalPlan{}},
		HasOuterJoin: false,
	}
	require.False(t, group.CanReorder())

	// Test case 2: Multiple tables, no outer joins - can reorder
	group = &JoinGroupResult{
		Group: []base.LogicalPlan{&MockLogicalPlan{}, &MockLogicalPlan{}},
		HasOuterJoin: false,
	}
	require.True(t, group.CanReorder())

	// Test case 3: Multiple tables, with outer joins - cannot reorder
	group = &JoinGroupResult{
		Group: []base.LogicalPlan{&MockLogicalPlan{}, &MockLogicalPlan{}},
		HasOuterJoin: true,
	}
	require.False(t, group.CanReorder())
}

// TestExtractJoinGroups_Integration tests integration scenarios
func TestExtractJoinGroups_Integration(t *testing.T) {
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

	// Create join with conditions
	eqCond := expression.NewFunctionInternal(sctx.GetExprCtx(), ast.EQ, types.NewFieldType(ast.TypeLonglong), 
		leftTable.Schema().Columns[0], rightTable.Schema().Columns[0]).(*expression.ScalarFunction)

	join := &MockLogicalJoin{}
	join.SetSchema(expression.MergeSchema(leftTable.Schema(), rightTable.Schema()))
	join.SetChildren(leftTable, rightTable)
	join.SetJoinType(0) // InnerJoin
	join.SetEqualConditions([]*expression.ScalarFunction{eqCond})

	// Extract join groups
	groups := ExtractJoinGroups(join)
	require.Len(t, groups, 1)
	require.Equal(t, 0, groups[0].JoinTypes[0].JoinType) // 0 = InnerJoin
}

// TestExtractJoinGroups_ComplexScenario tests a more complex scenario
func TestExtractJoinGroups_ComplexScenario(t *testing.T) {
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

	table3 := &MockLogicalPlan{}
	table3.SetSchema(&expression.Schema{
		Columns: []*expression.Column{
			{RetType: types.NewFieldType(ast.TypeLonglong)},
		},
	})

	// Create nested joins
	innerJoin := &MockLogicalJoin{}
	innerJoin.SetSchema(expression.MergeSchema(table1.Schema(), table2.Schema()))
	innerJoin.SetChildren(table1, table2)
	innerJoin.SetJoinType(0) // InnerJoin

	outerJoin := &MockLogicalJoin{}
	outerJoin.SetSchema(expression.MergeSchema(innerJoin.Schema(), table3.Schema()))
	outerJoin.SetChildren(innerJoin, table3)
	outerJoin.SetJoinType(0) // InnerJoin

	// Extract join groups
	groups := ExtractJoinGroups(outerJoin)
	require.Len(t, groups, 1)
	require.Len(t, groups[0].Group, 3)
	require.True(t, groups[0].CanReorder())
}

// BenchmarkExtractJoinGroups benchmarks the ExtractJoinGroups function
func BenchmarkExtractJoinGroups(b *testing.B) {
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

	// Create join
	join := &MockLogicalJoin{}
	join.SetSchema(expression.MergeSchema(table1.Schema(), table2.Schema()))
	join.SetChildren(table1, table2)
	join.SetJoinType(0) // InnerJoin

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExtractJoinGroups(join)
	}
} 