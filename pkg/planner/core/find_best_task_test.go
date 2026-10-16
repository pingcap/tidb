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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
)

func TestFindBestTaskSuite(t *testing.T) {
	t.Run("PointGetSchema", func(t *testing.T) {
		ds := &logicalop.DataSource{Columns: []*model.ColumnInfo{model.NewExtraPhysTblIDColInfo()}}
		require.False(t, isPointGetConvertableSchema(ds, false))
		require.True(t, isPointGetConvertableSchema(ds, true))
		ds.Columns = append(ds.Columns, &model.ColumnInfo{ID: 1, State: model.StateWriteOnly})
		require.False(t, isPointGetConvertableSchema(ds, true))
	})
	t.Run("TestCostOverflow", testCostOverflow)
	t.Run("TestEnforcedProperty", testEnforcedProperty)
	t.Run("TestHintCannotFitProperty", testHintCannotFitProperty)
	t.Run("RangeCountComparison", testRangeCountComparison)
	t.Run("RangeCountSkyline", testRangeCountSkyline)
}

func testRangeCountComparison(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		threshold            int64
		lhsRanges, rhsRanges int
		lhsCount, rhsCount   int
		want                 int
	}{
		{"disabled", 0, 1001, 1, 3, 2, 1},
		{"below threshold", 1000, 999, 1, 3, 2, 1},
		{"at threshold", 1000, 1000, 1, 3, 2, 1},
		{"above threshold", 1000, 1001, 1, 3, 2, 0},
		{"both above threshold", 1000, 1001, 1002, 3, 2, 0},
		{"only weaker candidate above threshold", 1000, 1, 1001, 3, 2, 1},
		{"equal predicate counts", 1000, 1001, 1, 2, 2, 0},
		{"single range", 1, 1, 1, 3, 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lhs := &candidatePath{path: &util.AccessPath{Ranges: make(ranger.Ranges, tc.lhsRanges)}, eqOrInCount: tc.lhsCount}
			rhs := &candidatePath{path: &util.AccessPath{Ranges: make(ranger.Ranges, tc.rhsRanges)}, eqOrInCount: tc.rhsCount}
			require.Equal(t, tc.want, compareEqOrIn(lhs, rhs, tc.threshold))
			require.Equal(t, -tc.want, compareEqOrIn(rhs, lhs, tc.threshold))
			// IndexMerge's predicate counts remain incomparable, even with the threshold disabled.
			lhs.path.PartialIndexPaths = []*util.AccessPath{{}}
			require.Zero(t, compareEqOrIn(lhs, rhs, tc.threshold))
			require.Zero(t, compareEqOrIn(rhs, lhs, tc.threshold))
		})
	}
}

func testRangeCountSkyline(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer domain.GetDomain(ctx).StatsHandle().Close()
	prop := property.NewPhysicalProperty(property.RootTaskType, nil, false, 0, false)
	newCandidate := func(id int64, eqCount, rangeCount int, maxCount float64) *candidatePath {
		return &candidatePath{
			path: &util.AccessPath{
				Index:               &model.IndexInfo{ID: id},
				Ranges:              make(ranger.Ranges, rangeCount),
				IsSingleScan:        true,
				CountAfterAccess:    10,
				MaxCountAfterAccess: maxCount,
			},
			accessCondsColMap: map[int64]int{id: -1},
			eqOrInCount:       eqCount,
		}
	}
	for _, pseudo := range []bool{false, true} {
		stats := &statistics.Table{
			HistColl:              statistics.HistColl{Pseudo: pseudo},
			ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMapWithoutSize(),
		}
		if !pseudo {
			stats.ColAndIdxExistenceMap.InsertIndex(1, true)
			stats.ColAndIdxExistenceMap.InsertIndex(2, true)
		}
		lhs, rhs := newCandidate(1, 3, 1001, 10), newCandidate(2, 2, 1, 20)
		for _, tc := range []struct {
			threshold int64
			want      int
		}{{0, 1}, {1001, 1}, {1000, 0}} {
			ctx.GetSessionVars().RangeMaxCount = tc.threshold
			result, _ := compareCandidates(ctx.GetPlanCtx(), stats, prop, lhs, rhs, true)
			require.Equal(t, tc.want, result, "pseudo=%v threshold=%d", pseudo, tc.threshold)
			result, _ = compareCandidates(ctx.GetPlanCtx(), stats, prop, rhs, lhs, true)
			require.Equal(t, -tc.want, result, "reversed: pseudo=%v threshold=%d", pseudo, tc.threshold)
		}
	}

	// A missing-statistics index must not retain its extra equality/IN preference above the threshold.
	stats := &statistics.Table{ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMapWithoutSize()}
	stats.ColAndIdxExistenceMap.InsertIndex(2, true)
	lhs, rhs := newCandidate(1, 3, 1001, 10), newCandidate(2, 2, 1, 10)
	for _, tc := range []struct {
		threshold int64
		want      int
	}{{0, 1}, {1001, 1}, {1000, -1}} {
		ctx.GetSessionVars().RangeMaxCount = tc.threshold
		result, _ := compareCandidates(ctx.GetPlanCtx(), stats, prop, lhs, rhs, true)
		require.Equal(t, tc.want, result)
		result, _ = compareCandidates(ctx.GetPlanCtx(), stats, prop, rhs, lhs, true)
		require.Equal(t, -tc.want, result)
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
