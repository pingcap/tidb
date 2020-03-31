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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"fmt"
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
)

var _ = Suite(&testFindBestTaskSuite{})

type testFindBestTaskSuite struct {
	ctx sessionctx.Context
}

func (s *testFindBestTaskSuite) SetUpSuite(c *C) {
	s.ctx = MockContext()
}

type mockDataSource struct {
	baseLogicalPlan
}

// Init initili
func (ds mockDataSource) Init(ctx sessionctx.Context) *mockDataSource {
	ds.baseLogicalPlan = newBaseLogicalPlan(ctx, "mockDS", &ds, 0)
	return &ds
}

func (ds *mockDataSource) findBestTask(prop *property.PhysicalProperty) (task, error) {
	// It can satisfy any of the property!
	// Just use a TableDual for convenience.
	p := PhysicalTableDual{}.Init(ds.ctx, &property.StatsInfo{RowCount: 1}, 0)
	task := &rootTask{
		p:   p,
		cst: 10000,
	}
	return task, nil
}

// mockLogicalPlan4Test is a LogicalPlan which is used for unit test.
// The basic assumption:
// 1. mockLogicalPlan4Test can generate tow kinds of physical plan: physicalPlan1 and
//    physicalPlan2. physicalPlan1 can pass the property only when they are the same
//    order; while physicalPlan2 cannot match any of the property(in other words, we can
//    generate it only when then property is empty).
// 2. We have a hint for physicalPlan2.
// 3. If the property is empty, we still need to check `canGeneratePlan2` to decide
//    whether it can generate physicalPlan2.
type mockLogicalPlan4Test struct {
	baseLogicalPlan
	// hasHintForPlan2 indicates whether this mockPlan contains hint.
	// This hint is used to generate physicalPlan2. See the implementation
	// of exhaustPhysicalPlans().
	hasHintForPlan2 bool
	// canGeneratePlan2 indicates whether this plan can generate physicalPlan2.
	canGeneratePlan2 bool
	// costOverflow indicates whether this plan will generate physical plan whose cost is overflowed.
	costOverflow bool
}

func (p mockLogicalPlan4Test) Init(ctx sessionctx.Context) *mockLogicalPlan4Test {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, "mockPlan", &p, 0)
	return &p
}

func (p *mockLogicalPlan4Test) getPhysicalPlan1(prop *property.PhysicalProperty) PhysicalPlan {
	physicalPlan1 := mockPhysicalPlan4Test{planType: 1, costOverflow: p.costOverflow}.Init(p.ctx)
	physicalPlan1.stats = &property.StatsInfo{RowCount: 1}
	physicalPlan1.childrenReqProps = make([]*property.PhysicalProperty, 1)
	physicalPlan1.childrenReqProps[0] = prop.Clone()
	return physicalPlan1
}

func (p *mockLogicalPlan4Test) getPhysicalPlan2(prop *property.PhysicalProperty) PhysicalPlan {
	physicalPlan2 := mockPhysicalPlan4Test{planType: 2, costOverflow: p.costOverflow}.Init(p.ctx)
	physicalPlan2.stats = &property.StatsInfo{RowCount: 1}
	physicalPlan2.childrenReqProps = make([]*property.PhysicalProperty, 1)
	physicalPlan2.childrenReqProps[0] = property.NewPhysicalProperty(prop.TaskTp, nil, false, prop.ExpectedCnt, false)
	return physicalPlan2
}

func (p *mockLogicalPlan4Test) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	plan1 := make([]PhysicalPlan, 0, 1)
	plan2 := make([]PhysicalPlan, 0, 1)
	if prop.IsEmpty() && p.canGeneratePlan2 {
		// Generate PhysicalPlan2 when the property is empty.
		plan2 = append(plan2, p.getPhysicalPlan2(prop))
		if p.hasHintForPlan2 {
			return plan2, true
		}
	}
	if all, _ := prop.AllSameOrder(); all {
		// Generate PhysicalPlan1 when properties are the same order.
		plan1 = append(plan1, p.getPhysicalPlan1(prop))
	}
	if p.hasHintForPlan2 {
		// The hint cannot work.
		if prop.IsEmpty() {
			p.ctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("the hint is inapplicable for plan2"))
		}
		return plan1, false
	}
	return append(plan1, plan2...), true
}

type mockPhysicalPlan4Test struct {
	basePhysicalPlan
	// 1 or 2 for physicalPlan1 or physicalPlan2.
	// See the comment of mockLogicalPlan4Test.
	planType     int
	costOverflow bool
}

func (p mockPhysicalPlan4Test) Init(ctx sessionctx.Context) *mockPhysicalPlan4Test {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, "mockPlan", &p, 0)
	return &p
}

func (p *mockPhysicalPlan4Test) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	attachPlan2Task(p, t)
	if p.costOverflow {
		t.addCost(math.MaxFloat64)
	} else {
		t.addCost(1)
	}
	return t
}

func (s *testFindBestTaskSuite) TestCostOverflow(c *C) {
	ctx := MockContext()
	// Plan Tree: mockPlan -> mockDataSource
	mockPlan := mockLogicalPlan4Test{costOverflow: true}.Init(ctx)
	mockDS := mockDataSource{}.Init(ctx)
	mockPlan.SetChildren(mockDS)
	// An empty property is enough for this test.
	prop := property.NewPhysicalProperty(property.RootTaskType, nil, false, 0, false)
	t, err := mockPlan.findBestTask(prop)
	c.Assert(err, IsNil)
	// The cost should be overflowed, but the task shouldn't be invalid.
	c.Assert(t.invalid(), IsFalse)
	c.Assert(t.cost(), Equals, math.MaxFloat64)
}

func (s *testFindBestTaskSuite) TestEnforcedProperty(c *C) {
	ctx := MockContext()
	// PlanTree : mockLogicalPlan -> mockDataSource
	mockPlan := mockLogicalPlan4Test{}.Init(ctx)
	mockDS := mockDataSource{}.Init(ctx)
	mockPlan.SetChildren(mockDS)

	col0 := &expression.Column{UniqueID: 1}
	col1 := &expression.Column{UniqueID: 2}
	// Use different order, so that mockLogicalPlan cannot generate any of the
	// physical plans.
	item0 := property.Item{Col: col0, Desc: false}
	item1 := property.Item{Col: col1, Desc: true}
	items := []property.Item{item0, item1}

	prop0 := &property.PhysicalProperty{
		Items:    items,
		Enforced: false,
	}
	// should return invalid task because no physical plan can match this property.
	task, err := mockPlan.findBestTask(prop0)
	c.Assert(err, IsNil)
	c.Assert(task.invalid(), IsTrue)

	prop1 := &property.PhysicalProperty{
		Items:    items,
		Enforced: true,
	}
	// should return the valid task when the property is enforced.
	task, err = mockPlan.findBestTask(prop1)
	c.Assert(err, IsNil)
	c.Assert(task.invalid(), IsFalse)
}

func (s *testFindBestTaskSuite) TestHintCannotFitProperty(c *C) {
	ctx := MockContext()
	// PlanTree : mockLogicalPlan -> mockDataSource
	mockPlan0 := mockLogicalPlan4Test{
		hasHintForPlan2:  true,
		canGeneratePlan2: true,
	}.Init(ctx)
	mockDS := mockDataSource{}.Init(ctx)
	mockPlan0.SetChildren(mockDS)

	col0 := &expression.Column{UniqueID: 1}
	item0 := property.Item{Col: col0}
	items := []property.Item{item0}
	// case 1, The property is not empty and enforced, should enforce a sort.
	prop0 := &property.PhysicalProperty{
		Items:    items,
		Enforced: true,
	}
	task, err := mockPlan0.findBestTask(prop0)
	c.Assert(err, IsNil)
	c.Assert(task.invalid(), IsFalse)
	_, enforcedSort := task.plan().(*PhysicalSort)
	c.Assert(enforcedSort, IsTrue)
	plan2 := task.plan().Children()[0]
	mockPhysicalPlan, ok := plan2.(*mockPhysicalPlan4Test)
	c.Assert(ok, IsTrue)
	c.Assert(mockPhysicalPlan.planType, Equals, 2)

	// case 2, The property is not empty but not enforced, still need to enforce a sort
	// to ensure the hint can work
	prop1 := &property.PhysicalProperty{
		Items:    items,
		Enforced: false,
	}
	task, err = mockPlan0.findBestTask(prop1)
	c.Assert(err, IsNil)
	c.Assert(task.invalid(), IsFalse)
	_, enforcedSort = task.plan().(*PhysicalSort)
	c.Assert(enforcedSort, IsTrue)
	plan2 = task.plan().Children()[0]
	mockPhysicalPlan, ok = plan2.(*mockPhysicalPlan4Test)
	c.Assert(ok, IsTrue)
	c.Assert(mockPhysicalPlan.planType, Equals, 2)

	// case 3, The hint cannot work even if the property is empty, should return a warning
	// and generate physicalPlan1.
	prop2 := &property.PhysicalProperty{
		Items:    items,
		Enforced: false,
	}
	mockPlan1 := mockLogicalPlan4Test{
		hasHintForPlan2:  true,
		canGeneratePlan2: false,
	}.Init(ctx)
	mockPlan1.SetChildren(mockDS)
	task, err = mockPlan1.findBestTask(prop2)
	c.Assert(err, IsNil)
	c.Assert(task.invalid(), IsFalse)
	c.Assert(ctx.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	// Because physicalPlan1 can match the property, so we should get it.
	mockPhysicalPlan, ok = task.plan().(*mockPhysicalPlan4Test)
	c.Assert(ok, IsTrue)
	c.Assert(mockPhysicalPlan.planType, Equals, 1)

	// case 4, Similar to case 3, but the property is enforced now. Ths result should be
	// the same with case 3.
	ctx.GetSessionVars().StmtCtx.SetWarnings(nil)
	prop3 := &property.PhysicalProperty{
		Items:    items,
		Enforced: true,
	}
	task, err = mockPlan1.findBestTask(prop3)
	c.Assert(err, IsNil)
	c.Assert(task.invalid(), IsFalse)
	c.Assert(ctx.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	// Because physicalPlan1 can match the property, so we don't need to enforce a sort.
	mockPhysicalPlan, ok = task.plan().(*mockPhysicalPlan4Test)
	c.Assert(ok, IsTrue)
	c.Assert(mockPhysicalPlan.planType, Equals, 1)
}
