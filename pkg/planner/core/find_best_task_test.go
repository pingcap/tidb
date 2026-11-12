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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
)

func TestFindBestTaskSuite(t *testing.T) {
	t.Run("TestCostOverflow", testCostOverflow)
	t.Run("TestEnforcedProperty", testEnforcedProperty)
	t.Run("TestHintCannotFitProperty", testHintCannotFitProperty)
	t.Run("RangeCountComparison", testRangeCountComparison)
	t.Run("RangeCountSkyline", testRangeCountSkyline)
	t.Run("MissingStatsIndexVsFullScan", testMissingStatsIndexVsFullScan)
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
			result, _ := compareCandidates(ctx.GetPlanCtx(), stats, prop, lhs, rhs)
			require.Equal(t, tc.want, result, "pseudo=%v threshold=%d", pseudo, tc.threshold)
			result, _ = compareCandidates(ctx.GetPlanCtx(), stats, prop, rhs, lhs)
			require.Equal(t, -tc.want, result, "reversed: pseudo=%v threshold=%d", pseudo, tc.threshold)
		}
	}

	// Missing statistics and equality/IN counts cannot break the tie between
	// incomparable access conditions, regardless of the range-count threshold.
	stats := &statistics.Table{ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMapWithoutSize()}
	stats.ColAndIdxExistenceMap.InsertIndex(2, true)
	lhs, rhs := newCandidate(1, 3, 1001, 10), newCandidate(2, 2, 1, 10)
	for _, tc := range []struct {
		threshold int64
		want      int
	}{{0, 0}, {1001, 0}, {1000, 0}} {
		ctx.GetSessionVars().RangeMaxCount = tc.threshold
		result, _ := compareCandidates(ctx.GetPlanCtx(), stats, prop, lhs, rhs)
		require.Equal(t, tc.want, result)
		result, _ = compareCandidates(ctx.GetPlanCtx(), stats, prop, rhs, lhs)
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

func testMissingStatsIndexVsFullScan(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer domain.GetDomain(ctx).StatsHandle().Close()
	prop := property.NewPhysicalProperty(property.RootTaskType, nil, false, 0, false)
	// The table has statistics, but index 1 does not.
	stats := &statistics.Table{ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMapWithoutSize()}
	idx := &candidatePath{
		path:              &util.AccessPath{Index: &model.IndexInfo{ID: 1}, Ranges: make(ranger.Ranges, 1), CountAfterAccess: 10},
		accessCondsColMap: map[int64]int{1: -1, 2: -1},
		eqOrInCount:       2,
	}
	tbl := &candidatePath{
		path:              &util.AccessPath{IsIntHandlePath: true, Ranges: ranger.FullIntRange(false), IsSingleScan: true, CountAfterAccess: 1000},
		accessCondsColMap: map[int64]int{},
	}
	// The missing-statistics index with 2 equal predicates wins over a full table scan,
	// independent of tidb_opt_prefer_range_scan.
	for _, preferRange := range []bool{false, true} {
		ctx.GetSessionVars().SetAllowPreferRangeScan(preferRange)
		result, missingStats := compareCandidates(ctx.GetPlanCtx(), stats, prop, idx, tbl)
		require.Equal(t, 1, result, "preferRange=%v", preferRange)
		require.True(t, missingStats)
		result, _ = compareCandidates(ctx.GetPlanCtx(), stats, prop, tbl, idx)
		require.Equal(t, -1, result, "reversed: preferRange=%v", preferRange)
	}

	// A non-covering missing-statistics index estimated to read most of the table must not prune
	// the full table scan, see https://github.com/pingcap/tidb/issues/72561.
	broad := &candidatePath{
		path:              &util.AccessPath{Index: &model.IndexInfo{ID: 1}, Ranges: make(ranger.Ranges, 900), CountAfterAccess: 900},
		accessCondsColMap: map[int64]int{1: -1, 2: -1},
		eqOrInCount:       2,
	}
	result, missingStats := compareCandidates(ctx.GetPlanCtx(), stats, prop, broad, tbl)
	require.Equal(t, 0, result)
	require.False(t, missingStats)
	result, _ = compareCandidates(ctx.GetPlanCtx(), stats, prop, tbl, broad)
	require.Equal(t, 0, result)
	// The threshold is the default selectivity (0.8) of the table.
	broad.path.CountAfterAccess = 800
	result, _ = compareCandidates(ctx.GetPlanCtx(), stats, prop, broad, tbl)
	require.Equal(t, 0, result)
	broad.path.CountAfterAccess = 799
	result, missingStats = compareCandidates(ctx.GetPlanCtx(), stats, prop, broad, tbl)
	require.Equal(t, 1, result)
	require.True(t, missingStats)
	// A covering index has no table lookups, so it may still prune the full table scan.
	broad.path.CountAfterAccess = 900
	broad.path.IsSingleScan = true
	result, missingStats = compareCandidates(ctx.GetPlanCtx(), stats, prop, broad, tbl)
	require.Equal(t, 1, result)
	require.True(t, missingStats)

	// A non-covering index with more ranges than tidb_opt_range_max_count is a broad lookup, even
	// if its estimate is small. Zero disables the range count check.
	broad.path.IsSingleScan = false
	broad.path.CountAfterAccess = 10
	require.False(t, isBroadIndexLookup(broad, 1000, 1000))
	require.True(t, isBroadIndexLookup(broad, 1000, 899))
	require.False(t, isBroadIndexLookup(broad, 1000, 0))
	require.True(t, canPseudoWinnerPrune(broad, tbl, 1000))
	require.False(t, canPseudoWinnerPrune(broad, tbl, 899))
	// Against another index, the winner may always prune.
	require.True(t, canPseudoWinnerPrune(broad, idx, 899))
	ctx.GetSessionVars().RangeMaxCount = 899
	result, missingStats = compareCandidates(ctx.GetPlanCtx(), stats, prop, broad, tbl)
	require.Equal(t, 0, result)
	require.False(t, missingStats)
}
