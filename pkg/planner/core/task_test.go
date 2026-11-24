// Copyright 2025 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
)

func TestSkipScanCandidateProperties(t *testing.T) {
	retType := types.NewFieldType(mysql.TypeLonglong)
	groupCol := &expression.Column{UniqueID: 1, ID: 1, RetType: retType}
	secondCol := &expression.Column{UniqueID: 2, ID: 2, RetType: retType}
	thirdCol := &expression.Column{UniqueID: 3, ID: 3, RetType: retType}
	normal := &property.PhysicalProperty{
		TaskTp:    property.CopSingleReadTaskType,
		SortItems: []property.SortItem{{Col: groupCol}},
	}
	strict := normal.CloneEssentialFields()
	strict.SkipScanInfo = &property.SkipScanInfo{GroupByCols: []*expression.Column{groupCol}}
	otherGroup := normal.CloneEssentialFields()
	otherGroup.SkipScanInfo = &property.SkipScanInfo{GroupByCols: []*expression.Column{secondCol}}
	require.Equal(t, strict.HashCode(), strict.CloneEssentialFields().HashCode())
	require.NotEqual(t, strict.HashCode(), normal.HashCode())
	require.NotEqual(t, strict.HashCode(), otherGroup.HashCode())
	withTarget := strict.CloneEssentialFields()
	withTarget.SkipScanInfo = &property.SkipScanInfo{GroupByCols: strict.SkipScanInfo.GroupByCols, TargetCol: secondCol}
	otherTarget := strict.CloneEssentialFields()
	otherTarget.SkipScanInfo = &property.SkipScanInfo{GroupByCols: strict.SkipScanInfo.GroupByCols, TargetCol: thirdCol}
	require.NotEqual(t, strict.HashCode(), withTarget.HashCode())
	require.NotEqual(t, withTarget.HashCode(), otherTarget.HashCode())

	cols := []*expression.Column{groupCol, secondCol, thirdCol}
	lengths := []int{types.UnspecifiedLength, types.UnspecifiedLength, types.UnspecifiedLength}
	indexCols := []*model.IndexColumn{
		{Offset: 0, Length: types.UnspecifiedLength},
		{Offset: 1, Length: types.UnspecifiedLength},
		{Offset: 2, Length: types.UnspecifiedLength},
	}
	ds := &logicalop.DataSource{
		TableInfo:        &model.TableInfo{IsCommonHandle: true},
		CommonHandleCols: cols,
		CommonHandleLens: lengths,
	}
	secondary := &util.AccessPath{
		Index: &model.IndexInfo{
			Tp:      ast.IndexTypeBtree,
			Columns: indexCols,
		},
		IdxCols:      cols,
		IdxColLens:   lengths,
		Ranges:       ranger.FullRange(),
		IsSingleScan: true,
	}
	clustered := &util.AccessPath{
		Index: &model.IndexInfo{
			Tp:      ast.IndexTypeBtree,
			Columns: indexCols,
			Primary: true,
			Unique:  true,
		},
		Ranges:             ranger.FullRange(),
		IsCommonHandlePath: true,
	}
	require.True(t, matchesSkipScanPath(ds, secondary, strict.SkipScanInfo))
	require.True(t, matchesSkipScanPath(ds, clustered, strict.SkipScanInfo))
	require.False(t, matchesSkipScanPath(ds, secondary, otherGroup.SkipScanInfo))
	require.False(t, matchesSkipScanPath(ds, clustered, otherGroup.SkipScanInfo))
	require.True(t, matchesSkipScanPath(ds, secondary, withTarget.SkipScanInfo))
	require.True(t, matchesSkipScanPath(ds, clustered, withTarget.SkipScanInfo))
	require.False(t, matchesSkipScanPath(ds, secondary, otherTarget.SkipScanInfo))
	require.False(t, matchesSkipScanPath(ds, clustered, otherTarget.SkipScanInfo))
	prefix := *secondary
	prefix.Index = secondary.Index.Clone()
	prefix.Index.Columns[2].Length = 4
	require.False(t, matchesSkipScanPath(ds, &prefix, withTarget.SkipScanInfo))

	ctx := mock.NewContext()
	projection := (logicalop.LogicalProjection{Exprs: []expression.Expression{secondCol, thirdCol}}).Init(ctx, 0)
	projection.SetSchema(expression.NewSchema(groupCol, secondCol))
	mapped, ok := projection.TryToGetChildProp(withTarget)
	require.True(t, ok)
	require.Equal(t, secondCol, mapped.SkipScanInfo.GroupByCols[0])
	require.Equal(t, secondCol, mapped.SortItems[0].Col)
	require.Equal(t, thirdCol, mapped.SkipScanInfo.TargetCol)
	require.Equal(t, groupCol, strict.SkipScanInfo.GroupByCols[0])
	require.Equal(t, secondCol, withTarget.SkipScanInfo.TargetCol)
	_, ok = projection.TryToGetChildProp(otherTarget)
	require.False(t, ok)
	projection.Exprs[0] = &expression.Constant{RetType: retType}
	_, ok = projection.TryToGetChildProp(strict)
	require.False(t, ok)
	selection := (logicalop.LogicalSelection{}).Init(ctx, 0)
	task, err := physicalop.FindBestTask(selection, strict)
	require.NoError(t, err)
	require.True(t, task.Invalid())

	stats := &property.StatsInfo{RowCount: 100, ColNDVs: map[int64]float64{1: 5, 2: 50}}
	scan := (&physicalop.PhysicalIndexScan{}).Init(ctx, 0)
	scan.SetStats(stats)
	setSkipScanRows(scan, 5)
	require.Equal(t, float64(5), scan.StatsCount())
	require.Equal(t, map[int64]float64{1: 5, 2: 5}, scan.StatsInfo().ColNDVs)
	require.Equal(t, float64(100), stats.RowCount)
	require.Equal(t, float64(50), stats.ColNDVs[2])

	rootProp := strict.CloneEssentialFields()
	rootProp.TaskTp = property.RootTaskType
	rootScan := &physicalop.RootTask{}
	rootScan.SetPlan(scan)
	rootProjection := (physicalop.PhysicalProjection{}).Init(ctx, stats, 0, rootProp)
	rootProjection.Attach2Task(rootScan)
	require.Equal(t, scan.StatsCount(), rootProjection.StatsCount())

	t.Run("same access path competes on cost", func(t *testing.T) {
		ctx := mock.NewContext()
		ctx.Store = &mock.Store{Client: &mock.Client{}}
		ctx.GetSessionVars().CostModelVersion = modelVer1
		table := &model.TableInfo{ID: 1}
		index := &model.IndexInfo{ID: 1}
		schema := expression.NewSchema(groupCol)
		ds := (logicalop.DataSource{TableInfo: table}).Init(ctx, 0)
		groups := []expression.Expression{groupCol}
		agg := (logicalop.LogicalAggregation{GroupByItems: groups}).Init(ctx, 0)
		agg.SetChildren(ds)

		// Cache tasks for the exact same access path with controlled costs.
		// Enumerate the strict candidate first to exercise the former pruning.
		for _, skipWins := range []bool{false, true} {
			var candidates []base.PhysicalPlan
			for _, req := range []*property.PhysicalProperty{strict, normal} {
				skip := req.SkipScanInfo != nil
				scan := physicalop.PhysicalIndexScan{
					Table: table, Index: index, KeepOrder: true, SkipScan: skip,
					Ranges: ranger.FullRange(), TblColHists: &statistics.HistColl{},
				}.Init(ctx, 0)
				scan.SetSchema(schema)
				scan.SetStats(&property.StatsInfo{RowCount: 5})
				scan.PlanCostInit = true
				scan.PlanCost = 1
				if skip != skipWins {
					scan.PlanCost = 10000
				}
				ds.StoreTask(req, &physicalop.CopTask{IndexPlan: scan, KeepOrder: true, SkipScan: skip})
				candidate := physicalop.BasePhysicalAgg{GroupByItems: groups}
				candidates = append(candidates, candidate.InitForStream(ctx, scan.StatsInfo(), 0, schema, req))
			}
			root := &property.PhysicalProperty{TaskTp: property.RootTaskType, SortItems: normal.SortItems}
			best, _, err := enumeratePhysicalPlans4TaskHelper(agg, candidates, root, false)
			require.NoError(t, err)
			require.False(t, best.Invalid())
			reader := best.Plan().Children()[0].(*physicalop.PhysicalIndexReader)
			require.Equal(t, skipWins, reader.IndexPlans[0].(*physicalop.PhysicalIndexScan).SkipScan)
		}
	})

	t.Run("table skip scan cost uses operator cardinality", func(t *testing.T) {
		ctx := mock.NewContext()
		ctx.GetSessionVars().CostModelVersion = modelVer2
		ctx.GetSessionVars().StmtCtx.SetIndexForce()
		normal := physicalop.PhysicalTableScan{
			Table:       &model.TableInfo{},
			TblCols:     []*expression.Column{groupCol},
			Ranges:      ranger.FullRange(),
			TblColHists: statistics.NewHistColl(0, 5000, 0, 0, 0),
		}.Init(ctx, 0)
		normal.SetStats(&property.StatsInfo{RowCount: 5})
		skip := physicalop.PhysicalTableScan{
			Table:                     normal.Table,
			TblCols:                   normal.TblCols,
			Ranges:                    normal.Ranges,
			TblColHists:               normal.TblColHists,
			SkipScan:                  true,
			SkipScanPrefixColumnCount: 1,
		}.Init(ctx, 0)
		skip.SetStats(&property.StatsInfo{RowCount: 5000})
		setSkipScanRows(skip, normal.StatsCount())

		cost := func(scan *physicalop.PhysicalTableScan) float64 {
			t.Helper()
			result, invalid, err := getTaskPlanCost(&physicalop.CopTask{TablePlan: scan, IndexPlanFinished: true})
			require.NoError(t, err)
			require.False(t, invalid)
			return result
		}
		require.Equal(t, cost(normal), cost(skip))
	})

	t.Run("estimated skip rows do not trigger the low estimate penalty", func(t *testing.T) {
		ctx := mock.NewContext()
		ctx.GetSessionVars().SetAllowPreferRangeScan(true)
		hists := statistics.NewHistColl(0, 5000, 10, 1, 0)
		hists.SetCol(secondCol.ID, &statistics.Column{
			Histogram:         *statistics.NewHistogram(secondCol.ID, 0, 5000, 0, retType, 0, 0),
			StatsLoadedStatus: statistics.NewStatsFullLoadStatus(),
		})
		scan := physicalop.PhysicalTableScan{TblColHists: hists}.Init(ctx, 0)
		require.Positive(t, getTableScanPenalty(scan, 5, 0))
		scan.SkipScan = true
		require.Zero(t, getTableScanPenalty(scan, 5, 0))
	})
}

func TestPhysicalUnionScanAttach2Task(t *testing.T) {
	ctx := mock.NewContext()
	col, cst := &expression.Column{RetType: types.NewFieldType(mysql.TypeString)}, &expression.Constant{RetType: types.NewFieldType(mysql.TypeLonglong)}
	stats := &property.StatsInfo{RowCount: 1000}
	schema := expression.NewSchema(col)
	tblInfo := &model.TableInfo{}

	// table scan
	tableScan := &physicalop.PhysicalTableScan{
		AccessCondition: []expression.Expression{col, cst},
		Table:           tblInfo,
	}
	tableScan = tableScan.Init(ctx, 0)
	tableScan.SetSchema(schema)

	// table reader
	tableReader := &physicalop.PhysicalTableReader{
		TablePlan:  tableScan,
		TablePlans: []base.PhysicalPlan{tableScan},
		StoreType:  kv.TiKV,
	}
	tableReader = tableReader.Init(ctx, 0)
	tableReader.SetSchema(schema)

	// selection
	sel := &physicalop.PhysicalSelection{Conditions: []expression.Expression{col, cst}}
	sel = sel.Init(ctx, stats, 0)

	// projection
	proj := &physicalop.PhysicalProjection{Exprs: []expression.Expression{col}}
	proj = proj.Init(ctx, stats, 0)
	proj.SetSchema(schema)

	// wrap as pointer p
	sel.SetChildren(proj)
	proj.SetChildren(tableReader)

	task := &physicalop.RootTask{}
	task.SetPlan(sel)
	// mock a union-scan and attach to task.
	unionScan := &physicalop.PhysicalUnionScan{Conditions: []expression.Expression{col, cst}}
	unionScan.Attach2Task(task)

	// assert the child task's p is unchanged.
	require.Equal(t, task.Plan(), sel)
	require.Equal(t, task.Plan().Children()[0], proj)
	require.Equal(t, task.Plan().Children()[0].Children()[0], tableReader)
	require.Equal(t, task.Plan().Children()[0].Children()[0].(*physicalop.PhysicalTableReader).TablePlans[0], tableScan)

	task2 := &physicalop.RootTask{}
	task2.SetPlan(proj)
	// mock a union-scan and attach to task.
	unionScan2 := &physicalop.PhysicalUnionScan{Conditions: []expression.Expression{col, cst}}
	unionScan2.Self = unionScan2
	unionScan2.Attach2Task(task2)

	// assert the child task's p is unchanged.
	require.Equal(t, task2.Plan(), proj)
	require.Equal(t, task2.Plan().Children()[0], tableReader)
	require.Equal(t, task2.Plan().Children()[0].(*physicalop.PhysicalTableReader).TablePlans[0], tableScan)
}
