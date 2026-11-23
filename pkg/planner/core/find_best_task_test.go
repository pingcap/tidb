// Copyright 2020 PingCAP, Inc.
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
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestFindBestTaskSuite(t *testing.T) {
	t.Run("TestCostOverflow", testCostOverflow)
	t.Run("TestEnforcedProperty", testEnforcedProperty)
	t.Run("TestHintCannotFitProperty", testHintCannotFitProperty)
	t.Run("MergeJoinLazyFD", testMergeJoinLazyFD)
}

// fdCountingChild is a join child that counts how often its FDs are extracted.
type fdCountingChild struct {
	*logicalop.LogicalTableDual
	extractFDCalls int
}

func (ds *fdCountingChild) ExtractFD() *funcdep.FDSet {
	ds.extractFDCalls++
	return &funcdep.FDSet{HashCodeToUniqueID: make(map[string]int)}
}

// testMergeJoinLazyFD checks that GetMergeJoin only extracts the join's FDs
// (which, for a wide DataSource, walks every column and unique index) when a
// merge join candidate needs the constant columns to match a required order.
func testMergeJoinLazyFD(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer domain.GetDomain(ctx).StatsHandle().Close()
	newCol := func() *expression.Column {
		return &expression.Column{UniqueID: ctx.GetSessionVars().AllocPlanColumnID(), RetType: types.NewFieldType(mysql.TypeLonglong)}
	}
	newChild := func(col *expression.Column) *fdCountingChild {
		dual := logicalop.LogicalTableDual{RowCount: 1}.Init(ctx.GetPlanCtx(), 0)
		dual.SetSchema(expression.NewSchema(col))
		return &fdCountingChild{LogicalTableDual: dual}
	}
	stats := &property.StatsInfo{RowCount: 100}
	for _, tc := range []struct {
		name           string
		hasProperties  bool
		sortByJoinKey  bool
		wantMergeJoins int
		wantExtractFD  int
	}{
		{"no merge join candidate", false, true, 0, 0},
		{"candidate without required order", true, false, 1, 0},
		{"candidate with required order", true, true, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := newCol(), newCol()
			left, right := newChild(a), newChild(b)
			join := logicalop.LogicalJoin{JoinType: base.InnerJoin}.Init(ctx.GetPlanCtx(), 0)
			join.SetChildren(left, right)
			join.SetSchema(expression.MergeSchema(left.Schema(), right.Schema()))
			eq, err := expression.NewFunction(ctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), a, b)
			require.NoError(t, err)
			join.EqualConditions = []*expression.ScalarFunction{eq.(*expression.ScalarFunction)}
			if tc.hasProperties {
				join.LeftProperties = [][]*expression.Column{{a}}
				join.RightProperties = [][]*expression.Column{{b}}
			}
			prop := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
			if tc.sortByJoinKey {
				prop = property.NewPhysicalProperty(property.RootTaskType, []*expression.Column{a}, false, math.MaxFloat64, false)
			}
			joins := physicalop.GetMergeJoin(join, prop, join.Schema(), stats, stats, stats)
			require.Len(t, joins, tc.wantMergeJoins)
			require.Equal(t, tc.wantExtractFD, left.extractFDCalls)
			require.Equal(t, tc.wantExtractFD, right.extractFDCalls)
		})
	}
}

func testCostOverflow(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	// Plan Tree: mockPlan -> mockDataSource
	mockPlan := mockLogicalPlan4Test{costOverflow: true}.Init(ctx.GetPlanCtx())
	mockDS := logicalop.MockDataSource{}.Init(ctx.GetPlanCtx())
	mockPlan.SetChildren(mockDS)
	// An empty property is enough for this test.
	prop := property.NewPhysicalProperty(property.RootTaskType, nil, false, 0, false)
	task, err := physicalop.FindBestTask(mockPlan, prop)
	require.NoError(t, err)
	// The cost should be overflowed, but the task shouldn't be invalid.
	require.False(t, task.Invalid())
}

func testEnforcedProperty(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	// PlanTree : mockLogicalPlan -> mockDataSource
	mockPlan := mockLogicalPlan4Test{}.Init(ctx.GetPlanCtx())
	mockDS := logicalop.MockDataSource{}.Init(ctx.GetPlanCtx())
	mockPlan.SetChildren(mockDS)

	col0 := &expression.Column{UniqueID: 1}
	col1 := &expression.Column{UniqueID: 2}
	// Use different order, so that mockLogicalPlan cannot generate any of the
	// physical plans.
	item0 := property.SortItem{Col: col0, Desc: false}
	item1 := property.SortItem{Col: col1, Desc: true}
	items := []property.SortItem{item0, item1}

	prop0 := &property.PhysicalProperty{
		SortItems:      items,
		CanAddEnforcer: false,
	}
	// should return invalid task because no physical plan can match this property.
	task, err := physicalop.FindBestTask(mockPlan, prop0)
	require.NoError(t, err)
	require.True(t, task.Invalid())

	prop1 := &property.PhysicalProperty{
		SortItems:      items,
		CanAddEnforcer: true,
	}
	// should return the valid task when the property is enforced.
	task, err = physicalop.FindBestTask(mockPlan, prop1)
	require.NoError(t, err)
	require.False(t, task.Invalid())
}

func testHintCannotFitProperty(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	// PlanTree : mockLogicalPlan -> mockDataSource
	mockPlan0 := mockLogicalPlan4Test{
		hasHintForPlan2:  true,
		canGeneratePlan2: true,
	}.Init(ctx.GetPlanCtx())
	mockDS := logicalop.MockDataSource{}.Init(ctx.GetPlanCtx())
	mockPlan0.SetChildren(mockDS)

	col0 := &expression.Column{UniqueID: 1}
	item0 := property.SortItem{Col: col0}
	items := []property.SortItem{item0}
	// case 1, The property is not empty and enforced, should enforce a sort.
	prop0 := &property.PhysicalProperty{
		SortItems:      items,
		CanAddEnforcer: true,
	}
	task, err := physicalop.FindBestTask(mockPlan0, prop0)
	require.NoError(t, err)
	require.False(t, task.Invalid())
	_, enforcedSort := task.Plan().(*physicalop.PhysicalSort)
	require.True(t, enforcedSort)
	plan2 := task.Plan().Children()[0]
	mockPhysicalPlan, ok := plan2.(*mockPhysicalPlan4Test)
	require.True(t, ok)
	require.Equal(t, 2, mockPhysicalPlan.planType)

	// case 2, The property is not empty but not enforced, still need to enforce a sort
	// to ensure the hint can work
	prop1 := &property.PhysicalProperty{
		SortItems:      items,
		CanAddEnforcer: false,
	}
	task, err = physicalop.FindBestTask(mockPlan0, prop1)
	require.NoError(t, err)
	require.False(t, task.Invalid())
	_, enforcedSort = task.Plan().(*physicalop.PhysicalSort)
	require.True(t, enforcedSort)
	plan2 = task.Plan().Children()[0]
	mockPhysicalPlan, ok = plan2.(*mockPhysicalPlan4Test)
	require.True(t, ok)
	require.Equal(t, 2, mockPhysicalPlan.planType)

	// case 3, The hint cannot work even if the property is empty, should return a warning
	// and generate physicalPlan1.
	prop2 := &property.PhysicalProperty{
		SortItems:      items,
		CanAddEnforcer: false,
	}
	mockPlan1 := mockLogicalPlan4Test{
		hasHintForPlan2:  true,
		canGeneratePlan2: false,
	}.Init(ctx)
	mockPlan1.SetChildren(mockDS)
	task, err = physicalop.FindBestTask(mockPlan1, prop2)
	require.NoError(t, err)
	require.False(t, task.Invalid())
	require.Equal(t, uint16(1), ctx.GetSessionVars().StmtCtx.WarningCount())
	// Because physicalPlan1 can match the property, so we should get it.
	mockPhysicalPlan, ok = task.Plan().(*mockPhysicalPlan4Test)
	require.True(t, ok)
	require.Equal(t, 1, mockPhysicalPlan.planType)

	// case 4, Similar to case 3, but the property is enforced now. Ths result should be
	// the same with case 3.
	ctx.GetSessionVars().StmtCtx.SetWarnings(nil)
	prop3 := &property.PhysicalProperty{
		SortItems:      items,
		CanAddEnforcer: true,
	}
	task, err = physicalop.FindBestTask(mockPlan1, prop3)
	require.NoError(t, err)
	require.False(t, task.Invalid())
	require.Equal(t, uint16(1), ctx.GetSessionVars().StmtCtx.WarningCount())
	// Because physicalPlan1 can match the property, so we don't need to enforce a sort.
	mockPhysicalPlan, ok = task.Plan().(*mockPhysicalPlan4Test)
	require.True(t, ok)
	require.Equal(t, 1, mockPhysicalPlan.planType)
}
