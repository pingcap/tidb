// Copyright 2026 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestGetPhysTopNRootVectorPropCandidateFromProjection(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().LimitPushDownThreshold = 100

	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)

	vecCol := &expression.Column{
		UniqueID: 1,
		ID:       1,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	vecColInfo := &model.ColumnInfo{
		ID:        vecCol.ID,
		FieldType: *vecCol.RetType,
	}
	vecConst := &expression.Constant{
		Value:   types.NewVectorFloat32Datum(vec),
		RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	distFn := expression.NewFunctionInternal(ctx, ast.VecCosineDistance, types.NewFieldType(mysql.TypeFloat), vecCol, vecConst)

	ds := logicalop.DataSource{}.Init(ctx, 0)
	ds.TableInfo = &model.TableInfo{
		TableCacheStatusType: model.TableCacheStatusDisable,
		Columns:              []*model.ColumnInfo{vecColInfo},
	}
	ds.PossibleAccessPaths = []*util.AccessPath{{
		StoreType: kv.TiKV,
		Index: &model.IndexInfo{
			Tp: pmodel.IndexTypeSPFresh,
			VectorInfo: &model.VectorIndexInfo{
				Kind:           model.VectorIndexKindSPFresh,
				Dimension:      3,
				DistanceMetric: model.DistanceMetricCosine,
			},
			Columns: []*model.IndexColumn{{Offset: 0}},
		},
	}}
	ds.SetSchema(expression.NewSchema(vecCol))
	ds.PushedDownConds = nil

	distCol := &expression.Column{
		UniqueID: 2,
		ID:       2,
		RetType:  types.NewFieldType(mysql.TypeFloat),
	}
	proj := logicalop.LogicalProjection{
		Exprs: []expression.Expression{distFn},
	}.Init(ctx, 0)
	proj.SetSchema(expression.NewSchema(distCol))
	proj.SetChildren(ds)

	lt := logicalop.LogicalTopN{
		ByItems: []*util.ByItems{{Expr: distCol}},
		Count:   1,
	}.Init(ctx, 0)

	lt.SetChildren(proj)

	plans := getPhysTopN(lt, &property.PhysicalProperty{})
	found := false
	for _, p := range plans {
		topN, ok := p.(*PhysicalTopN)
		if !ok {
			continue
		}
		childProp := topN.GetChildReqProps(0)
		if childProp.TaskTp == property.RootTaskType && childProp.VectorProp.VectorHelper != nil {
			require.Equal(t, uint64(1), childProp.VectorProp.TopK)
			found = true
		}
	}
	require.True(t, found, "expect RootTaskType + VectorProp candidate")
}

func TestGetPhysTopNRootVectorPropCandidateRespectsLimitToCop(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().LimitPushDownThreshold = 100

	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)

	vecCol := &expression.Column{
		UniqueID: 1,
		ID:       1,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	vecConst := &expression.Constant{
		Value:   types.NewVectorFloat32Datum(vec),
		RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	distFn := expression.NewFunctionInternal(ctx, ast.VecCosineDistance, types.NewFieldType(mysql.TypeFloat), vecCol, vecConst)

	ds := logicalop.DataSource{}.Init(ctx, 0)
	ds.TableInfo = &model.TableInfo{TableCacheStatusType: model.TableCacheStatusDisable}
	ds.PossibleAccessPaths = []*util.AccessPath{{StoreType: kv.TiKV}}
	ds.SetSchema(expression.NewSchema(vecCol))
	ds.PushedDownConds = nil

	lt := logicalop.LogicalTopN{
		ByItems:          []*util.ByItems{{Expr: distFn}},
		Count:            1,
		PreferLimitToCop: true,
	}.Init(ctx, 0)

	lt.SetChildren(ds)

	preferPushDown, meetThreshold := pushLimitOrTopNForcibly(lt, &PhysicalTopN{})
	require.True(t, preferPushDown)
	require.True(t, meetThreshold)

	plans := getPhysTopN(lt, &property.PhysicalProperty{})
	for _, p := range plans {
		topN, ok := p.(*PhysicalTopN)
		if !ok {
			continue
		}
		childProp := topN.GetChildReqProps(0)
		if childProp.TaskTp == property.RootTaskType && childProp.VectorProp.VectorHelper != nil {
			t.Fatalf("unexpected RootTaskType + VectorProp candidate when LIMIT_TO_COP is effective")
		}
	}
}

func TestGetPhysTopNRootVectorPropCandidateRejectsFiltersWithoutSPFreshPath(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().LimitPushDownThreshold = 100

	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)

	vecCol := &expression.Column{
		UniqueID: 1,
		ID:       1,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	idCol := &expression.Column{
		UniqueID: 2,
		ID:       2,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	vecConst := &expression.Constant{
		Value:   types.NewVectorFloat32Datum(vec),
		RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	idConst := &expression.Constant{
		Value:   types.NewIntDatum(1),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	distFn := expression.NewFunctionInternal(ctx, ast.VecCosineDistance, types.NewFieldType(mysql.TypeFloat), vecCol, vecConst)
	filter := expression.NewFunctionInternal(ctx, ast.GT, types.NewFieldType(mysql.TypeTiny), idCol, idConst)

	ds := logicalop.DataSource{}.Init(ctx, 0)
	ds.TableInfo = &model.TableInfo{TableCacheStatusType: model.TableCacheStatusDisable}
	ds.PossibleAccessPaths = []*util.AccessPath{{StoreType: kv.TiKV}}
	ds.SetSchema(expression.NewSchema(vecCol, idCol))
	ds.AllConds = []expression.Expression{filter}
	ds.PushedDownConds = nil

	lt := logicalop.LogicalTopN{
		ByItems: []*util.ByItems{{Expr: distFn}},
		Count:   1,
	}.Init(ctx, 0)

	lt.SetChildren(ds)

	plans := getPhysTopN(lt, &property.PhysicalProperty{})
	for _, p := range plans {
		topN, ok := p.(*PhysicalTopN)
		if !ok {
			continue
		}
		childProp := topN.GetChildReqProps(0)
		if childProp.TaskTp == property.RootTaskType && childProp.VectorProp.VectorHelper != nil {
			t.Fatalf("unexpected RootTaskType + VectorProp candidate when filters have no matching SPFresh path")
		}
	}
}

func TestGetPhysTopNFilteredSPFreshOnlyBuildsRootVectorProp(t *testing.T) {
	ctx := MockContext()
	ctx.GetSessionVars().LimitPushDownThreshold = 100

	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)

	vecCol := &expression.Column{
		UniqueID: 1,
		ID:       1,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	idCol := &expression.Column{
		UniqueID: 2,
		ID:       2,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	vecConst := &expression.Constant{
		Value:   types.NewVectorFloat32Datum(vec),
		RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	idConst := &expression.Constant{
		Value:   types.NewIntDatum(1),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	distFn := expression.NewFunctionInternal(ctx, ast.VecCosineDistance, types.NewFieldType(mysql.TypeFloat), vecCol, vecConst)
	filter := expression.NewFunctionInternal(ctx, ast.GT, types.NewFieldType(mysql.TypeTiny), idCol, idConst)

	ds := logicalop.DataSource{}.Init(ctx, 0)
	ds.TableInfo = &model.TableInfo{
		TableCacheStatusType: model.TableCacheStatusDisable,
		Columns: []*model.ColumnInfo{
			{ID: vecCol.ID, FieldType: *types.NewFieldType(mysql.TypeTiDBVectorFloat32)},
			{ID: idCol.ID, FieldType: *types.NewFieldType(mysql.TypeLonglong)},
		},
	}
	ds.PossibleAccessPaths = []*util.AccessPath{{
		StoreType: kv.TiKV,
		Index: &model.IndexInfo{
			Tp: pmodel.IndexTypeSPFresh,
			VectorInfo: &model.VectorIndexInfo{
				Kind:            model.VectorIndexKindSPFresh,
				Dimension:       3,
				DistanceMetric:  model.DistanceMetricCosine,
				StoredColumnIDs: []int64{idCol.ID},
			},
			Columns: []*model.IndexColumn{{Offset: 0}},
		},
	}}
	ds.SetSchema(expression.NewSchema(vecCol, idCol))
	ds.AllConds = []expression.Expression{filter}

	lt := logicalop.LogicalTopN{
		ByItems: []*util.ByItems{{Expr: distFn}},
		Count:   1,
	}.Init(ctx, 0)

	lt.SetChildren(ds)

	plans := getPhysTopN(lt, &property.PhysicalProperty{})
	foundRoot := false
	for _, p := range plans {
		topN, ok := p.(*PhysicalTopN)
		if !ok {
			continue
		}
		childProp := topN.GetChildReqProps(0)
		if childProp.VectorProp.VectorHelper == nil {
			continue
		}
		switch childProp.TaskTp {
		case property.RootTaskType:
			foundRoot = true
		case property.MppTaskType:
			t.Fatal("unexpected MPP VectorProp candidate for a SPFresh-only filter")
		}
	}
	require.True(t, foundRoot, "expect filtered SPFresh RootTask VectorProp candidate")
}

func TestGetPhysLimitVectorPropRequiresSPFreshPath(t *testing.T) {
	ctx := mock.NewContext()

	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)

	vecColInfo := &model.ColumnInfo{
		ID:        1,
		FieldType: *types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	vecCol := &expression.Column{
		UniqueID: 1,
		ID:       vecColInfo.ID,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	vecConst := &expression.Constant{
		Value:   types.NewVectorFloat32Datum(vec),
		RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	distFn := expression.NewFunctionInternal(ctx, ast.VecCosineDistance, types.NewFieldType(mysql.TypeFloat), vecCol, vecConst)

	ds := logicalop.DataSource{}.Init(ctx, 0)
	ds.TableInfo = &model.TableInfo{
		TableCacheStatusType: model.TableCacheStatusDisable,
		Columns:              []*model.ColumnInfo{vecColInfo},
	}
	ds.SetSchema(expression.NewSchema(vecCol))
	ds.PushedDownConds = nil

	lt := logicalop.LogicalTopN{
		ByItems: []*util.ByItems{{Expr: distFn}},
		Count:   1,
	}.Init(ctx, 0)

	lt.SetChildren(ds)

	vectorPath := &util.AccessPath{
		StoreType: kv.TiFlash,
		Index: &model.IndexInfo{
			Tp: pmodel.IndexTypeHNSW,
			VectorInfo: &model.VectorIndexInfo{
				Kind:           model.VectorIndexKindHNSW,
				Dimension:      3,
				DistanceMetric: model.DistanceMetricCosine,
			},
			Columns: []*model.IndexColumn{{Offset: 0}},
		},
	}
	ds.PossibleAccessPaths = []*util.AccessPath{vectorPath}
	require.Empty(t, getPhysLimits(lt, &property.PhysicalProperty{}))

	vectorPath.Index.Tp = pmodel.IndexTypeSPFresh
	vectorPath.Index.VectorInfo.Kind = model.VectorIndexKindSPFresh
	plans := getPhysLimits(lt, &property.PhysicalProperty{})
	require.Len(t, plans, 1)
	limit, ok := plans[0].(*PhysicalLimit)
	require.True(t, ok)
	childProp := limit.GetChildReqProps(0)
	require.Equal(t, property.RootTaskType, childProp.TaskTp)
	require.NotNil(t, childProp.VectorProp.VectorHelper)
	require.Equal(t, uint64(1), childProp.VectorProp.TopK)
}

func TestSPFreshVectorPropCandidateAdmission(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().LimitPushDownThreshold = 100

	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)
	vecColInfo := &model.ColumnInfo{ID: 1, FieldType: *types.NewFieldType(mysql.TypeTiDBVectorFloat32)}
	vecCol := &expression.Column{UniqueID: 1, ID: vecColInfo.ID, RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32)}
	vecConst := &expression.Constant{Value: types.NewVectorFloat32Datum(vec), RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32)}
	distFn := expression.NewFunctionInternal(ctx, ast.VecCosineDistance, types.NewFieldType(mysql.TypeFloat), vecCol, vecConst)
	ds := logicalop.DataSource{}.Init(ctx, 0)
	ds.TableInfo = &model.TableInfo{TableCacheStatusType: model.TableCacheStatusDisable, Columns: []*model.ColumnInfo{vecColInfo}}
	vectorPath := &util.AccessPath{
		StoreType: kv.TiKV,
		Index: &model.IndexInfo{
			Tp:         pmodel.IndexTypeSPFresh,
			VectorInfo: &model.VectorIndexInfo{Kind: model.VectorIndexKindSPFresh, Dimension: 3, DistanceMetric: model.DistanceMetricCosine},
			Columns:    []*model.IndexColumn{{Offset: 0}},
		},
	}
	ds.PossibleAccessPaths = []*util.AccessPath{vectorPath}
	ds.SetSchema(expression.NewSchema(vecCol))

	hasRootVectorProp := func(plans []base.PhysicalPlan) (bool, uint64) {
		for _, plan := range plans {
			childProp := plan.GetChildReqProps(0)
			if childProp.TaskTp == property.RootTaskType && childProp.VectorProp.VectorHelper != nil {
				return true, childProp.VectorProp.TopK
			}
		}
		return false, 0
	}

	for _, tc := range []struct {
		name           string
		count          uint64
		offset         uint64
		partitioned    bool
		withoutSPFresh bool
		want           bool
		wantTopK       uint64
	}{
		{name: "atLimit", count: maxSPFreshTopK, want: true, wantTopK: maxSPFreshTopK},
		{name: "limitWithOffsetAtLimit", count: 192, offset: 8000, want: true, wantTopK: maxSPFreshTopK},
		{name: "overLimit", count: maxSPFreshTopK + 1},
		{name: "zeroCount", count: 0},
		{name: "overflow", count: 2, offset: math.MaxUint64},
		{name: "partitioned", count: 1, partitioned: true},
		{name: "withoutSPFreshPath", count: 1, withoutSPFresh: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.withoutSPFresh {
				ds.PossibleAccessPaths = []*util.AccessPath{{StoreType: kv.TiKV}}
			} else {
				ds.PossibleAccessPaths = []*util.AccessPath{vectorPath}
			}
			lt := logicalop.LogicalTopN{ByItems: []*util.ByItems{{Expr: distFn}}, Count: tc.count, Offset: tc.offset}.Init(ctx, 0)
			if tc.partitioned {
				lt.PartitionBy = []property.SortItem{{Col: vecCol}}
			}

			lt.SetChildren(ds)

			topNFound, topNTopK := hasRootVectorProp(getPhysTopN(lt, &property.PhysicalProperty{}))
			require.Equal(t, tc.want, topNFound)
			if tc.want {
				require.Equal(t, tc.wantTopK, topNTopK)
			}

			limitFound, limitTopK := hasRootVectorProp(getPhysLimits(lt, &property.PhysicalProperty{}))
			require.Equal(t, tc.want, limitFound)
			if tc.want {
				require.Equal(t, tc.wantTopK, limitTopK)
			}
		})
	}
}

func TestSPFreshLimitWithoutParentProjectionKeepsVectorColumn(t *testing.T) {
	ctx := mock.NewContext()

	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)

	vecColInfo := &model.ColumnInfo{
		ID:        1,
		FieldType: *types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	vecCol := &expression.Column{
		UniqueID: 1,
		ID:       vecColInfo.ID,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	scan := PhysicalSPFreshVectorScan{
		Table: &model.TableInfo{
			ID:      100,
			Columns: []*model.ColumnInfo{vecColInfo},
		},
		Columns: []*model.ColumnInfo{vecColInfo},
		Index: &model.IndexInfo{
			Tp: pmodel.IndexTypeSPFresh,
			VectorInfo: &model.VectorIndexInfo{
				Kind:           model.VectorIndexKindSPFresh,
				Dimension:      3,
				DistanceMetric: model.DistanceMetricCosine,
			},
			Columns: []*model.IndexColumn{{Offset: 0}},
		},
		QueryVec: vec,
	}.Init(ctx, &property.StatsInfo{RowCount: 1, ColNDVs: map[int64]float64{}}, 0)
	scan.SetSchema(expression.NewSchema(vecCol.Clone().(*expression.Column)))

	limit := PhysicalLimit{Count: 1}.Init(ctx, &property.StatsInfo{RowCount: 1, ColNDVs: map[int64]float64{}}, 0)
	limit.SetSchema(expression.NewSchema(vecCol.Clone().(*expression.Column)))
	limit.SetChildren(scan)

	tryEnableSPFreshVectorSearchDistanceProjection(limit)

	require.Len(t, limit.Schema().Columns, 1)
	require.Equal(t, vecColInfo.ID, limit.Schema().Columns[0].ID)
	require.Len(t, scan.Columns, 1)
	require.Equal(t, vecColInfo.ID, scan.Columns[0].ID)
	require.Len(t, scan.Schema().Columns, 1)
	require.Equal(t, vecColInfo.ID, scan.Schema().Columns[0].ID)
}

func TestSPFreshLimitWithoutParentProjectionPrunesUnusedVectorColumn(t *testing.T) {
	ctx := mock.NewContext()

	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)

	idColInfo := &model.ColumnInfo{
		ID:        1,
		FieldType: *types.NewFieldType(mysql.TypeLong),
	}
	vecColInfo := &model.ColumnInfo{
		ID:        2,
		FieldType: *types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	idCol := &expression.Column{
		UniqueID: 1,
		ID:       idColInfo.ID,
		RetType:  types.NewFieldType(mysql.TypeLong),
	}
	vecCol := &expression.Column{
		UniqueID: 2,
		ID:       vecColInfo.ID,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	scan := PhysicalSPFreshVectorScan{
		Table: &model.TableInfo{
			ID:      100,
			Columns: []*model.ColumnInfo{idColInfo, vecColInfo},
		},
		Columns: []*model.ColumnInfo{idColInfo, vecColInfo},
		Index: &model.IndexInfo{
			Tp: pmodel.IndexTypeSPFresh,
			VectorInfo: &model.VectorIndexInfo{
				Kind:           model.VectorIndexKindSPFresh,
				Dimension:      3,
				DistanceMetric: model.DistanceMetricCosine,
			},
			Columns: []*model.IndexColumn{{Offset: 1}},
		},
		QueryVec: vec,
	}.Init(ctx, &property.StatsInfo{RowCount: 1, ColNDVs: map[int64]float64{}}, 0)
	scan.SetSchema(expression.NewSchema(idCol.Clone().(*expression.Column), vecCol.Clone().(*expression.Column)))

	limit := PhysicalLimit{Count: 1}.Init(ctx, &property.StatsInfo{RowCount: 1, ColNDVs: map[int64]float64{}}, 0)
	limit.SetSchema(expression.NewSchema(idCol.Clone().(*expression.Column)))
	limit.SetChildren(scan)

	tryEnableSPFreshVectorSearchDistanceProjection(limit)

	require.Len(t, limit.Schema().Columns, 1)
	require.Equal(t, idColInfo.ID, limit.Schema().Columns[0].ID)
	require.Len(t, scan.Columns, 1)
	require.Equal(t, idColInfo.ID, scan.Columns[0].ID)
	require.Len(t, scan.Schema().Columns, 1)
	require.Equal(t, idColInfo.ID, scan.Schema().Columns[0].ID)
}

func TestSPFreshLimitBottomProjectionPrunesUnusedVectorColumn(t *testing.T) {
	ctx := mock.NewContext()

	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)

	idColInfo := &model.ColumnInfo{
		ID:        1,
		FieldType: *types.NewFieldType(mysql.TypeLong),
	}
	vecColInfo := &model.ColumnInfo{
		ID:        2,
		FieldType: *types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	idCol := &expression.Column{
		UniqueID: 1,
		ID:       idColInfo.ID,
		RetType:  types.NewFieldType(mysql.TypeLong),
	}
	vecCol := &expression.Column{
		UniqueID: 2,
		ID:       vecColInfo.ID,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	vecConst := &expression.Constant{
		Value:   types.NewVectorFloat32Datum(vec),
		RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	distFn := expression.NewFunctionInternal(ctx, ast.VecCosineDistance, types.NewFieldType(mysql.TypeFloat), vecCol, vecConst)
	distCol := &expression.Column{
		UniqueID: 3,
		ID:       3,
		RetType:  types.NewFieldType(mysql.TypeFloat),
	}

	scan := PhysicalSPFreshVectorScan{
		Table: &model.TableInfo{
			ID:      100,
			Columns: []*model.ColumnInfo{idColInfo, vecColInfo},
		},
		Columns: []*model.ColumnInfo{idColInfo, vecColInfo},
		Index: &model.IndexInfo{
			Tp: pmodel.IndexTypeSPFresh,
			VectorInfo: &model.VectorIndexInfo{
				Kind:           model.VectorIndexKindSPFresh,
				Dimension:      3,
				DistanceMetric: model.DistanceMetricCosine,
			},
			Columns: []*model.IndexColumn{{Offset: 1}},
		},
		QueryVec: vec,
	}.Init(ctx, &property.StatsInfo{RowCount: 1, ColNDVs: map[int64]float64{}}, 0)
	scan.SetSchema(expression.NewSchema(idCol.Clone().(*expression.Column), vecCol.Clone().(*expression.Column)))

	bottomProj := PhysicalProjection{
		Exprs: []expression.Expression{
			idCol.Clone().(*expression.Column),
			vecCol.Clone().(*expression.Column),
			distFn,
		},
	}.Init(ctx, &property.StatsInfo{RowCount: 1, ColNDVs: map[int64]float64{}}, 0)
	bottomProj.SetSchema(expression.NewSchema(
		idCol.Clone().(*expression.Column),
		vecCol.Clone().(*expression.Column),
		distCol,
	))
	bottomProj.SetChildren(scan)

	limit := PhysicalLimit{Count: 1}.Init(ctx, &property.StatsInfo{RowCount: 1, ColNDVs: map[int64]float64{}}, 0)
	limit.SetSchema(bottomProj.Schema().Clone())
	limit.SetChildren(bottomProj)

	topProj := PhysicalProjection{
		Exprs: []expression.Expression{idCol.Clone().(*expression.Column)},
	}.Init(ctx, &property.StatsInfo{RowCount: 1, ColNDVs: map[int64]float64{}}, 0)
	topProj.SetSchema(expression.NewSchema(idCol.Clone().(*expression.Column)))
	topProj.SetChildren(limit)

	tryEnableSPFreshVectorSearchDistanceProjection(topProj)

	require.Len(t, scan.Columns, 1)
	require.Equal(t, idColInfo.ID, scan.Columns[0].ID)
	require.Len(t, scan.Schema().Columns, 1)
	require.Equal(t, idColInfo.ID, scan.Schema().Columns[0].ID)
	require.False(t, scan.EnableDistanceProj)
	require.Len(t, bottomProj.Exprs, 1)
	require.Len(t, bottomProj.Schema().Columns, 1)
	require.Len(t, limit.Schema().Columns, 1)
	require.Equal(t, idColInfo.ID, limit.Schema().Columns[0].ID)
	require.Equal(t, 0, topProj.Exprs[0].(*expression.Column).Index)
}

func TestSPFreshLimitBottomProjectionMatchesIndexedVectorColumn(t *testing.T) {
	ctx := mock.NewContext()

	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)

	v1ColInfo := &model.ColumnInfo{
		ID:        1,
		FieldType: *types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	v2ColInfo := &model.ColumnInfo{
		ID:        2,
		FieldType: *types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	v1Col := &expression.Column{
		UniqueID: 1,
		ID:       v1ColInfo.ID,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	v2Col := &expression.Column{
		UniqueID: 2,
		ID:       v2ColInfo.ID,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	vecConst := &expression.Constant{
		Value:   types.NewVectorFloat32Datum(vec),
		RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	distV2 := expression.NewFunctionInternal(ctx, ast.VecCosineDistance, types.NewFieldType(mysql.TypeFloat), v2Col, vecConst)
	distV1 := expression.NewFunctionInternal(ctx, ast.VecCosineDistance, types.NewFieldType(mysql.TypeFloat), v1Col, vecConst)
	distV2Col := &expression.Column{
		UniqueID: 3,
		ID:       3,
		RetType:  types.NewFieldType(mysql.TypeFloat),
	}
	distV1Col := &expression.Column{
		UniqueID: 4,
		ID:       4,
		RetType:  types.NewFieldType(mysql.TypeFloat),
	}

	scan := PhysicalSPFreshVectorScan{
		Table: &model.TableInfo{
			ID:      100,
			Columns: []*model.ColumnInfo{v1ColInfo, v2ColInfo},
		},
		Columns: []*model.ColumnInfo{v1ColInfo, v2ColInfo},
		Index: &model.IndexInfo{
			Tp: pmodel.IndexTypeSPFresh,
			VectorInfo: &model.VectorIndexInfo{
				Kind:           model.VectorIndexKindSPFresh,
				Dimension:      3,
				DistanceMetric: model.DistanceMetricCosine,
			},
			Columns: []*model.IndexColumn{{Offset: 0}},
		},
		QueryVec: vec,
	}.Init(ctx, &property.StatsInfo{RowCount: 1, ColNDVs: map[int64]float64{}}, 0)
	scan.SetSchema(expression.NewSchema(v1Col.Clone().(*expression.Column), v2Col.Clone().(*expression.Column)))

	bottomProj := PhysicalProjection{
		Exprs: []expression.Expression{
			distV2,
			distV1,
		},
	}.Init(ctx, &property.StatsInfo{RowCount: 1, ColNDVs: map[int64]float64{}}, 0)
	bottomProj.SetSchema(expression.NewSchema(distV2Col, distV1Col))
	bottomProj.SetChildren(scan)

	limit := PhysicalLimit{Count: 1}.Init(ctx, &property.StatsInfo{RowCount: 1, ColNDVs: map[int64]float64{}}, 0)
	limit.SetSchema(bottomProj.Schema().Clone())
	limit.SetChildren(bottomProj)

	topProj := PhysicalProjection{
		Exprs: []expression.Expression{
			limit.Schema().Columns[0].Clone().(*expression.Column),
			limit.Schema().Columns[1].Clone().(*expression.Column),
		},
	}.Init(ctx, &property.StatsInfo{RowCount: 1, ColNDVs: map[int64]float64{}}, 0)
	topProj.SetSchema(limit.Schema().Clone())
	topProj.SetChildren(limit)

	tryEnableSPFreshVectorSearchDistanceProjection(topProj)

	require.True(t, scan.EnableDistanceProj)
	helper := expression.ExtractVectorHelper(bottomProj.Exprs[0])
	require.NotNil(t, helper)
	require.Equal(t, v2ColInfo.ID, helper.Column.ID)
	distCol, ok := bottomProj.Exprs[1].(*expression.Column)
	require.True(t, ok)
	require.Equal(t, model.VirtualColVecSearchDistanceID, distCol.ID)
	require.Len(t, scan.Columns, 2)
	require.Equal(t, v2ColInfo.ID, scan.Columns[0].ID)
	require.Equal(t, model.VirtualColVecSearchDistanceID, scan.Columns[1].ID)
}
