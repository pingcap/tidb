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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/stretchr/testify/require"
)

type mockDataSource struct {
	logicalop.BaseLogicalPlan
}

func (ds mockDataSource) Init(ctx base.PlanContext) *mockDataSource {
	ds.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, "mockDS", &ds, 0)
	return &ds
}

func (ds *mockDataSource) FindBestTask(prop *property.PhysicalProperty, planCounter *base.PlanCounterTp, opt *optimizetrace.PhysicalOptimizeOp) (base.Task, int64, error) {
	// It can satisfy any of the property!
	// Just use a TableDual for convenience.
	p := PhysicalTableDual{}.Init(ds.SCtx(), &property.StatsInfo{RowCount: 1}, 0)
	task := &RootTask{
		p: p,
	}
	planCounter.Dec(1)
	return task, 1, nil
}

// mockLogicalPlan4Test is a LogicalPlan which is used for unit test.
// The basic assumption:
//  1. mockLogicalPlan4Test can generate tow kinds of physical plan: physicalPlan1 and
//     physicalPlan2. physicalPlan1 can pass the property only when they are the same
//     order; while physicalPlan2 cannot match any of the property(in other words, we can
//     generate it only when then property is empty).
//  2. We have a hint for physicalPlan2.
//  3. If the property is empty, we still need to check `canGeneratePlan2` to decide
//     whether it can generate physicalPlan2.
type mockLogicalPlan4Test struct {
	logicalop.BaseLogicalPlan
	// hasHintForPlan2 indicates whether this mockPlan contains hint.
	// This hint is used to generate physicalPlan2. See the implementation
	// of ExhaustPhysicalPlans().
	hasHintForPlan2 bool
	// canGeneratePlan2 indicates whether this plan can generate physicalPlan2.
	canGeneratePlan2 bool
	// costOverflow indicates whether this plan will generate physical plan whose cost is overflowed.
	costOverflow bool
}

func (p mockLogicalPlan4Test) Init(ctx base.PlanContext) *mockLogicalPlan4Test {
	p.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, "mockPlan", &p, 0)
	return &p
}

func (p *mockLogicalPlan4Test) getPhysicalPlan1(prop *property.PhysicalProperty) base.PhysicalPlan {
	physicalPlan1 := mockPhysicalPlan4Test{planType: 1}.Init(p.SCtx())
	physicalPlan1.SetStats(&property.StatsInfo{RowCount: 1})
	physicalPlan1.childrenReqProps = make([]*property.PhysicalProperty, 1)
	physicalPlan1.childrenReqProps[0] = prop.CloneEssentialFields()
	return physicalPlan1
}

func (p *mockLogicalPlan4Test) getPhysicalPlan2(prop *property.PhysicalProperty) base.PhysicalPlan {
	physicalPlan2 := mockPhysicalPlan4Test{planType: 2}.Init(p.SCtx())
	physicalPlan2.SetStats(&property.StatsInfo{RowCount: 1})
	physicalPlan2.childrenReqProps = make([]*property.PhysicalProperty, 1)
	physicalPlan2.childrenReqProps[0] = property.NewPhysicalProperty(prop.TaskTp, nil, false, prop.ExpectedCnt, false)
	return physicalPlan2
}

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (p *mockLogicalPlan4Test) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	plan1 := make([]base.PhysicalPlan, 0, 1)
	plan2 := make([]base.PhysicalPlan, 0, 1)
	if prop.IsSortItemEmpty() && p.canGeneratePlan2 {
		// Generate PhysicalPlan2 when the property is empty.
		plan2 = append(plan2, p.getPhysicalPlan2(prop))
		if p.hasHintForPlan2 {
			return plan2, true, nil
		}
	}
	if all, _ := prop.AllSameOrder(); all {
		// Generate PhysicalPlan1 when properties are the same order.
		plan1 = append(plan1, p.getPhysicalPlan1(prop))
	}
	if p.hasHintForPlan2 {
		// The hint cannot work.
		if prop.IsSortItemEmpty() {
			p.SCtx().GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("the hint is inapplicable for plan2"))
		}
		return plan1, false, nil
	}
	return append(plan1, plan2...), true, nil
}

type mockPhysicalPlan4Test struct {
	basePhysicalPlan
	// 1 or 2 for physicalPlan1 or physicalPlan2.
	// See the comment of mockLogicalPlan4Test.
	planType int
}

func (p mockPhysicalPlan4Test) Init(ctx base.PlanContext) *mockPhysicalPlan4Test {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, "mockPlan", &p, 0)
	return &p
}

// Attach2Task implements the PhysicalPlan interface.
func (p *mockPhysicalPlan4Test) Attach2Task(tasks ...base.Task) base.Task {
	t := tasks[0].Copy()
	attachPlan2Task(p, t)
	return t
}

// MemoryUsage of mockPhysicalPlan4Test is only for testing
func (p *mockPhysicalPlan4Test) MemoryUsage() (sum int64) {
	return
}

func TestCostOverflow(t *testing.T) {
	ctx := MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	// Plan Tree: mockPlan -> mockDataSource
	mockPlan := mockLogicalPlan4Test{costOverflow: true}.Init(ctx.GetPlanCtx())
	mockDS := mockDataSource{}.Init(ctx.GetPlanCtx())
	mockPlan.SetChildren(mockDS)
	// An empty property is enough for this test.
	prop := property.NewPhysicalProperty(property.RootTaskType, nil, false, 0, false)
	task, _, err := mockPlan.FindBestTask(prop, &PlanCounterDisabled, optimizetrace.DefaultPhysicalOptimizeOption())
	require.NoError(t, err)
	// The cost should be overflowed, but the task shouldn't be invalid.
	require.False(t, task.Invalid())
}

func TestEnforcedProperty(t *testing.T) {
	ctx := MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	// PlanTree : mockLogicalPlan -> mockDataSource
	mockPlan := mockLogicalPlan4Test{}.Init(ctx.GetPlanCtx())
	mockDS := mockDataSource{}.Init(ctx.GetPlanCtx())
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
	task, _, err := mockPlan.FindBestTask(prop0, &PlanCounterDisabled, optimizetrace.DefaultPhysicalOptimizeOption())
	require.NoError(t, err)
	require.True(t, task.Invalid())

	prop1 := &property.PhysicalProperty{
		SortItems:      items,
		CanAddEnforcer: true,
	}
	// should return the valid task when the property is enforced.
	task, _, err = mockPlan.FindBestTask(prop1, &PlanCounterDisabled, optimizetrace.DefaultPhysicalOptimizeOption())
	require.NoError(t, err)
	require.False(t, task.Invalid())
}

func TestHintCannotFitProperty(t *testing.T) {
	ctx := MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	// PlanTree : mockLogicalPlan -> mockDataSource
	mockPlan0 := mockLogicalPlan4Test{
		hasHintForPlan2:  true,
		canGeneratePlan2: true,
	}.Init(ctx.GetPlanCtx())
	mockDS := mockDataSource{}.Init(ctx.GetPlanCtx())
	mockPlan0.SetChildren(mockDS)

	col0 := &expression.Column{UniqueID: 1}
	item0 := property.SortItem{Col: col0}
	items := []property.SortItem{item0}
	// case 1, The property is not empty and enforced, should enforce a sort.
	prop0 := &property.PhysicalProperty{
		SortItems:      items,
		CanAddEnforcer: true,
	}
	task, _, err := mockPlan0.FindBestTask(prop0, &PlanCounterDisabled, optimizetrace.DefaultPhysicalOptimizeOption())
	require.NoError(t, err)
	require.False(t, task.Invalid())
	_, enforcedSort := task.Plan().(*PhysicalSort)
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
	task, _, err = mockPlan0.FindBestTask(prop1, &PlanCounterDisabled, optimizetrace.DefaultPhysicalOptimizeOption())
	require.NoError(t, err)
	require.False(t, task.Invalid())
	_, enforcedSort = task.Plan().(*PhysicalSort)
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
	task, _, err = mockPlan1.FindBestTask(prop2, &PlanCounterDisabled, optimizetrace.DefaultPhysicalOptimizeOption())
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
	task, _, err = mockPlan1.FindBestTask(prop3, &PlanCounterDisabled, optimizetrace.DefaultPhysicalOptimizeOption())
	require.NoError(t, err)
	require.False(t, task.Invalid())
	require.Equal(t, uint16(1), ctx.GetSessionVars().StmtCtx.WarningCount())
	// Because physicalPlan1 can match the property, so we don't need to enforce a sort.
	mockPhysicalPlan, ok = task.Plan().(*mockPhysicalPlan4Test)
	require.True(t, ok)
	require.Equal(t, 1, mockPhysicalPlan.planType)
}
