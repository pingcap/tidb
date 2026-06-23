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
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestFindBestTaskSuite(t *testing.T) {
	t.Run("TestCostOverflow", testCostOverflow)
	t.Run("TestEnforcedProperty", testEnforcedProperty)
	t.Run("TestHintCannotFitProperty", testHintCannotFitProperty)
	t.Run("TestPreferBoundedLimitIndexLookupForTopN", testPreferBoundedLimitIndexLookupForTopN)
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

func testPreferBoundedLimitIndexLookupForTopN(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()

	topN := logicalop.LogicalTopN{Count: 26}.Init(ctx.GetPlanCtx(), 0)
	lookupTask := buildBoundedLimitIndexLookupTask(ctx.GetPlanCtx(), 0, 26)
	tableTask := buildTopNTableReaderTask(ctx.GetPlanCtx())

	curIsBetter, decided := preferBoundedLimitIndexLookupForTopN(topN, lookupTask, tableTask)
	require.True(t, decided)
	require.True(t, curIsBetter)

	curIsBetter, decided = preferBoundedLimitIndexLookupForTopN(topN, tableTask, lookupTask)
	require.True(t, decided)
	require.False(t, curIsBetter)

	ctx.GetSessionVars().ResetRelevantOptVarsAndFixes(true)
	curIsBetter, decided = preferBoundedLimitIndexLookupForTopN(topN, lookupTask, tableTask)
	require.True(t, decided)
	require.True(t, curIsBetter)
	require.Contains(t, ctx.GetSessionVars().RelevantOptVars, vardef.TiDBOptBoundedLimitIndexLookupThreshold)
	require.Contains(t, ctx.GetSessionVars().RelevantOptVars, vardef.TiDBOptIndexLookupCostFactor)
	require.Contains(t, ctx.GetSessionVars().RelevantOptVars, vardef.TiDBOptLimitCostFactor)
	require.Contains(t, ctx.GetSessionVars().RelevantOptFixes, fixcontrol.Fix69405)
	ctx.GetSessionVars().ResetRelevantOptVarsAndFixes(false)

	topN.Offset = 474
	lookupTask = buildBoundedLimitIndexLookupTask(ctx.GetPlanCtx(), 474, 26)
	curIsBetter, decided = preferBoundedLimitIndexLookupForTopN(topN, lookupTask, tableTask)
	require.True(t, decided)
	require.True(t, curIsBetter)

	topN.Offset = 475
	lookupTask = buildBoundedLimitIndexLookupTask(ctx.GetPlanCtx(), 475, 26)
	curIsBetter, decided = preferBoundedLimitIndexLookupForTopN(topN, lookupTask, tableTask)
	require.False(t, decided)
	require.False(t, curIsBetter)

	topN.Offset = 0
	ctx.GetSessionVars().BoundedLimitIndexLookupThreshold = 0
	lookupTask = buildBoundedLimitIndexLookupTask(ctx.GetPlanCtx(), 0, 26)
	curIsBetter, decided = preferBoundedLimitIndexLookupForTopN(topN, lookupTask, tableTask)
	require.False(t, decided)
	require.False(t, curIsBetter)
	ctx.GetSessionVars().BoundedLimitIndexLookupThreshold = vardef.DefOptBoundedLimitIndexLookupThreshold

	ctx.GetSessionVars().OptimizerFixControl = map[uint64]string{fixcontrol.Fix69405: "off"}
	curIsBetter, decided = preferBoundedLimitIndexLookupForTopN(topN, lookupTask, tableTask)
	require.False(t, decided)
	require.False(t, curIsBetter)
	ctx.GetSessionVars().OptimizerFixControl = nil

	ctx.GetSessionVars().IndexLookupCostFactor = vardef.DefOptIndexLookupCostFactor + 1
	curIsBetter, decided = preferBoundedLimitIndexLookupForTopN(topN, lookupTask, tableTask)
	require.False(t, decided)
	require.False(t, curIsBetter)
	ctx.GetSessionVars().IndexLookupCostFactor = vardef.DefOptIndexLookupCostFactor

	unsafeLookupTask := buildBoundedLimitIndexLookupTask(ctx.GetPlanCtx(), 0, 26)
	reader := unsafeLookupTask.Plan().(*physicalop.PhysicalIndexLookUpReader)
	reader.TablePlan.(*physicalop.PhysicalTableScan).FilterCondition = []expression.Expression{expression.NewOne()}
	curIsBetter, decided = preferBoundedLimitIndexLookupForTopN(topN, unsafeLookupTask, tableTask)
	require.False(t, decided)
	require.False(t, curIsBetter)

	unorderedLookupTask := buildBoundedLimitIndexLookupTask(ctx.GetPlanCtx(), 0, 26)
	unorderedReader := unorderedLookupTask.Plan().(*physicalop.PhysicalIndexLookUpReader)
	unorderedReader.IndexPlan.Children()[0].(*physicalop.PhysicalIndexScan).KeepOrder = false
	curIsBetter, decided = preferBoundedLimitIndexLookupForTopN(topN, unorderedLookupTask, tableTask)
	require.False(t, decided)
	require.False(t, curIsBetter)
}

func buildBoundedLimitIndexLookupTask(ctx base.PlanContext, offset, count uint64) *physicalop.RootTask {
	stats := &property.StatsInfo{RowCount: float64(offset + count)}
	schema := expression.NewSchema(&expression.Column{UniqueID: 1, RetType: types.NewFieldType(mysql.TypeLonglong)})
	indexScan := physicalop.PhysicalIndexScan{
		KeepOrder: true,
		Prop:      &property.PhysicalProperty{},
	}.Init(ctx, 0)
	indexScan.SetSchema(schema.Clone())
	indexScan.SetStats(stats)

	indexLimit := physicalop.PhysicalLimit{Count: offset + count}.Init(ctx, stats, 0)
	indexLimit.SetChildren(indexScan)
	indexLimit.SetSchema(schema.Clone())

	tableScan := physicalop.PhysicalTableScan{StoreType: kv.TiKV}.Init(ctx, 0)
	tableScan.SetSchema(schema.Clone())
	tableScan.SetStats(stats)

	reader := physicalop.PhysicalIndexLookUpReader{
		IndexPlan:   indexLimit,
		TablePlan:   tableScan,
		PushedLimit: &physicalop.PushedDownLimit{Offset: offset, Count: count},
	}.Init(ctx, 0, plannerutil.IndexLookUpPushDownNone)

	task := &physicalop.RootTask{}
	task.SetPlan(reader)
	return task
}

func buildTopNTableReaderTask(ctx base.PlanContext) *physicalop.RootTask {
	stats := &property.StatsInfo{RowCount: 100}
	tableReader := physicalop.PhysicalTableReader{}.Init(ctx, 0)
	topN := physicalop.PhysicalTopN{Count: 26}.Init(ctx, stats, 0)
	topN.SetChildren(tableReader)
	task := &physicalop.RootTask{}
	task.SetPlan(topN)
	return task
}
