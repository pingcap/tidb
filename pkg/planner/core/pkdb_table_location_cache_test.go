package core

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestCachedPlanLocationInfo_DetermineLocationWithParams_PointGetReadOnlyLocal(t *testing.T) {
	prev := GetLocationResolver()
	t.Cleanup(func() { SetLocationResolver(prev) })

	SetLocationResolver(&mockLocationResolver{
		local:          "local",
		tableLocations: map[int64]string{1: "remote"},
	})

	info := &CachedPlanLocationInfo{
		TableLocations: []*TableLocationInfo{{TableID: 1, IsPartitioned: false, IsPointGet: true}},
		StatementTraits: StatementTraits{
			HasPointGet: true,
			IsDML:       false,
		},
	}
	loc := info.DetermineLocationWithParams(nil, nil, nil)
	require.NotNil(t, loc)
	require.Equal(t, PhyPlanLocal, loc.PlanType)
}

func TestCachedPlanLocationInfo_DetermineLocationWithParams_PointGetDMLCanForward(t *testing.T) {
	prev := GetLocationResolver()
	t.Cleanup(func() { SetLocationResolver(prev) })

	SetLocationResolver(&mockLocationResolver{
		local:          "local",
		tableLocations: map[int64]string{1: "remote"},
	})

	info := &CachedPlanLocationInfo{
		TableLocations: []*TableLocationInfo{{TableID: 1, IsPartitioned: false, IsPointGet: true}},
		StatementTraits: StatementTraits{
			HasPointGet: true,
			IsDML:       true,
			DMLType:     DMLTypeUpdate,
		},
	}
	loc := info.DetermineLocationWithParams(nil, nil, nil)
	require.NotNil(t, loc)
	require.Equal(t, PhyPlanRemote, loc.PlanType)
	require.Equal(t, "remote", loc.TargetStore)
}

func TestCachedPlanLocationInfo_DetermineLocationWithParams_IndexLookupReadOnlyLocal(t *testing.T) {
	prev := GetLocationResolver()
	t.Cleanup(func() { SetLocationResolver(prev) })

	SetLocationResolver(&mockLocationResolver{
		local:          "local",
		tableLocations: map[int64]string{1: "remote"},
	})

	info := &CachedPlanLocationInfo{
		TableLocations: []*TableLocationInfo{{TableID: 1, IsPartitioned: false, IsIndexLookupPushDown: true}},
		StatementTraits: StatementTraits{
			HasIndexLookup: true,
			IsDML:          false,
		},
	}
	loc := info.DetermineLocationWithParams(nil, nil, nil)
	require.NotNil(t, loc)
	require.Equal(t, PhyPlanLocal, loc.PlanType)
}

func TestCachedPlanLocationInfo_DetermineLocationWithParams_PartitionedReadOnlyCanForward(t *testing.T) {
	prev := GetLocationResolver()
	t.Cleanup(func() { SetLocationResolver(prev) })

	SetLocationResolver(&mockPartitionLocationResolver{
		local:              "local",
		partitionLocations: map[int64]string{101: "remote"},
	})

	info := &CachedPlanLocationInfo{
		TableLocations: []*TableLocationInfo{{
			TableID:       1,
			IsPartitioned: true,
			PartitionIDs:  []int64{101},
		}},
		HasPartitionTable: true,
		StatementTraits: StatementTraits{
			IsDML: false,
		},
	}
	loc := info.DetermineLocationWithParams(nil, nil, nil)
	require.NotNil(t, loc)
	require.Equal(t, PhyPlanRemote, loc.PlanType)
	require.Equal(t, "remote", loc.TargetStore)
}

type mockPartitionLocationResolver struct {
	local              string
	tableLocations     map[int64]string
	partitionLocations map[int64]string
}

func (m *mockPartitionLocationResolver) ResolveTableLocation(tableID int64) string {
	return m.tableLocations[tableID]
}

func (m *mockPartitionLocationResolver) ResolvePartitionLocation(tableID int64, partitionID int64) string {
	return m.partitionLocations[partitionID]
}

func (m *mockPartitionLocationResolver) ResolveKeyLocation(tableID int64, partitionID int64, key []byte) string {
	return ""
}

func (m *mockPartitionLocationResolver) IsLocalStore(storeAddr string) bool {
	return storeAddr == m.local || storeAddr == ""
}

func (m *mockPartitionLocationResolver) GetLocalStoreAddr() string {
	return m.local
}

func buildHashPartitionedTableInfo(id int64, name string, partitionIDs []int64) *model.TableInfo {
	tbl := &model.TableInfo{
		ID:    id,
		Name:  pmodel.NewCIStr(name),
		State: model.StatePublic,
	}
	tbl.Columns = []*model.ColumnInfo{{
		State:     model.StatePublic,
		Offset:    0,
		Name:      pmodel.NewCIStr("a"),
		FieldType: *types.NewFieldType(mysql.TypeLong),
		ID:        1,
	}}
	tbl.Partition = &model.PartitionInfo{
		Type:   pmodel.PartitionTypeHash,
		Enable: true,
		Expr:   "a",
		Num:    uint64(len(partitionIDs)),
	}
	tbl.Partition.Definitions = make([]model.PartitionDefinition, 0, len(partitionIDs))
	for i, pid := range partitionIDs {
		tbl.Partition.Definitions = append(tbl.Partition.Definitions, model.PartitionDefinition{
			ID:   pid,
			Name: pmodel.NewCIStr("p" + string(rune('0'+i))),
		})
	}
	return tbl
}

func buildRangePartitionedTableInfo(id int64, name string, partitionIDs []int64) *model.TableInfo {
	tbl := &model.TableInfo{
		ID:    id,
		Name:  pmodel.NewCIStr(name),
		State: model.StatePublic,
	}
	tbl.Columns = []*model.ColumnInfo{{
		State:     model.StatePublic,
		Offset:    0,
		Name:      pmodel.NewCIStr("a"),
		FieldType: *types.NewFieldType(mysql.TypeLong),
		ID:        1,
	}, {
		State:     model.StatePublic,
		Offset:    1,
		Name:      pmodel.NewCIStr("b"),
		FieldType: *types.NewFieldType(mysql.TypeLong),
		ID:        2,
	}}
	tbl.Partition = &model.PartitionInfo{
		Type:   pmodel.PartitionTypeRange,
		Enable: true,
		Expr:   "a",
		Num:    uint64(len(partitionIDs)),
	}
	defs := make([]model.PartitionDefinition, 0, len(partitionIDs))
	if len(partitionIDs) > 0 {
		defs = append(defs, model.PartitionDefinition{
			ID:       partitionIDs[0],
			Name:     pmodel.NewCIStr("p0"),
			LessThan: []string{"10"},
		})
	}
	if len(partitionIDs) > 1 {
		defs = append(defs, model.PartitionDefinition{
			ID:       partitionIDs[1],
			Name:     pmodel.NewCIStr("p1"),
			LessThan: []string{"20"},
		})
	}
	if len(partitionIDs) > 2 {
		defs = append(defs, model.PartitionDefinition{
			ID:       partitionIDs[2],
			Name:     pmodel.NewCIStr("pMax"),
			LessThan: []string{"MAXVALUE"},
		})
	}
	tbl.Partition.Definitions = defs
	return tbl
}

func TestCachedPlanLocationInfo_DetermineLocationWithParams_PartitionedPointGetHandleDML(t *testing.T) {
	prev := GetLocationResolver()
	t.Cleanup(func() { SetLocationResolver(prev) })

	partitionIDs := []int64{101, 102, 103, 104}
	tblInfo := buildHashPartitionedTableInfo(1, "t", partitionIDs)
	is := infoschema.MockInfoSchema([]*model.TableInfo{tblInfo})

	sctx := mock.NewContext()

	tbl, ok := is.TableByID(context.Background(), 1)
	require.True(t, ok)
	pt := tbl.GetPartitionedTable()
	require.NotNil(t, pt)
	row := make([]types.Datum, len(pt.Meta().Columns))
	d1 := types.NewIntDatum(1)
	d1.Copy(&row[0])
	partIdx, err := pt.GetPartitionIdxByRow(sctx.GetExprCtx().GetEvalCtx(), row)
	require.NoError(t, err)
	partIdx, err = pt.Meta().Partition.ReplaceWithOverlappingPartitionIdx(partIdx, err)
	require.NoError(t, err)
	require.GreaterOrEqual(t, partIdx, 0)
	require.Less(t, partIdx, len(pt.Meta().Partition.Definitions))
	targetPartitionID := pt.Meta().Partition.Definitions[partIdx].ID

	partitionLocations := make(map[int64]string, len(partitionIDs))
	for _, pid := range partitionIDs {
		partitionLocations[pid] = "local"
	}
	partitionLocations[targetPartitionID] = "remote"
	SetLocationResolver(&mockPartitionLocationResolver{
		local:              "local",
		partitionLocations: partitionLocations,
	})

	locInfo := &CachedPlanLocationInfo{
		TableLocations: []*TableLocationInfo{{
			TableID:       1,
			DBName:        "test",
			TableName:     "t",
			IsPartitioned: true,
			PartitionIDs:  partitionIDs,
			IsPointGet:    true,
			PartitionByRowInfo: &PartitionByRowInfo{
				HandleColOffset:   0,
				HandleValues:      []types.Datum{types.NewIntDatum(0)},
				HandleParamOrders: []int{0},
			},
		}},
		HasPartitionTable: true,
		StatementTraits: StatementTraits{
			HasPointGet: true,
			IsDML:       true,
			DMLType:     DMLTypeUpdate,
		},
	}

	params := []expression.Expression{
		&expression.Constant{Value: types.NewIntDatum(1), RetType: types.NewFieldType(mysql.TypeLonglong)},
	}
	partIDs, allParts, err := locInfo.TableLocations[0].CalculatePartitionIDs(sctx, is, params)
	require.NoError(t, err)
	require.False(t, allParts)
	require.Equal(t, []int64{targetPartitionID}, partIDs)

	planLoc := locInfo.DetermineLocationWithParams(sctx, is, params)
	require.NotNil(t, planLoc)
	require.Equal(t, PhyPlanRemote, planLoc.PlanType)
	require.Equal(t, "remote", planLoc.TargetStore)
}

func TestCachedPlanLocationInfo_DetermineLocationWithParams_PartitionedBatchPointGetHandleDistributed(t *testing.T) {
	prev := GetLocationResolver()
	t.Cleanup(func() { SetLocationResolver(prev) })

	partitionIDs := []int64{101, 102, 103, 104}
	tblInfo := buildHashPartitionedTableInfo(1, "t", partitionIDs)
	is := infoschema.MockInfoSchema([]*model.TableInfo{tblInfo})

	sctx := mock.NewContext()

	tbl, ok := is.TableByID(context.Background(), 1)
	require.True(t, ok)
	pt := tbl.GetPartitionedTable()
	require.NotNil(t, pt)

	row1 := make([]types.Datum, len(pt.Meta().Columns))
	d1 := types.NewIntDatum(1)
	d1.Copy(&row1[0])
	pIdx1, err := pt.GetPartitionIdxByRow(sctx.GetExprCtx().GetEvalCtx(), row1)
	require.NoError(t, err)
	pIdx1, err = pt.Meta().Partition.ReplaceWithOverlappingPartitionIdx(pIdx1, err)
	require.NoError(t, err)
	pid1 := pt.Meta().Partition.Definitions[pIdx1].ID

	row2 := make([]types.Datum, len(pt.Meta().Columns))
	d2 := types.NewIntDatum(2)
	d2.Copy(&row2[0])
	pIdx2, err := pt.GetPartitionIdxByRow(sctx.GetExprCtx().GetEvalCtx(), row2)
	require.NoError(t, err)
	pIdx2, err = pt.Meta().Partition.ReplaceWithOverlappingPartitionIdx(pIdx2, err)
	require.NoError(t, err)
	pid2 := pt.Meta().Partition.Definitions[pIdx2].ID
	require.NotEqual(t, pid1, pid2)

	SetLocationResolver(&mockPartitionLocationResolver{
		local:              "local",
		partitionLocations: map[int64]string{pid1: "remote1", pid2: "remote2"},
	})

	locInfo := &CachedPlanLocationInfo{
		TableLocations: []*TableLocationInfo{{
			TableID:       1,
			DBName:        "test",
			TableName:     "t",
			IsPartitioned: true,
			PartitionIDs:  partitionIDs,
			IsPointGet:    true,
			PartitionByRowInfo: &PartitionByRowInfo{
				HandleColOffset:   0,
				HandleValues:      []types.Datum{types.NewIntDatum(0), types.NewIntDatum(0)},
				HandleParamOrders: []int{0, 1},
			},
		}},
		HasPartitionTable: true,
		StatementTraits: StatementTraits{
			HasPointGet: true,
			IsDML:       true,
			DMLType:     DMLTypeUpdate,
		},
	}

	params := []expression.Expression{
		&expression.Constant{Value: types.NewIntDatum(1), RetType: types.NewFieldType(mysql.TypeLonglong)},
		&expression.Constant{Value: types.NewIntDatum(2), RetType: types.NewFieldType(mysql.TypeLonglong)},
	}
	partIDs, allParts, err := locInfo.TableLocations[0].CalculatePartitionIDs(sctx, is, params)
	require.NoError(t, err)
	require.False(t, allParts)
	require.ElementsMatch(t, []int64{pid1, pid2}, partIDs)

	planLoc := locInfo.DetermineLocationWithParams(sctx, is, params)
	require.NotNil(t, planLoc)
	require.Equal(t, PhyPlanDistributed, planLoc.PlanType)
}

func TestBuildLocFromInsert_PartitionedValuesPruningSinglePartition(t *testing.T) {
	prev := GetLocationResolver()
	t.Cleanup(func() { SetLocationResolver(prev) })

	partitionIDs := []int64{101, 102, 103}
	tblInfo := buildRangePartitionedTableInfo(1, "t", partitionIDs)
	is := infoschema.MockInfoSchema([]*model.TableInfo{tblInfo})
	sctx := mock.NewContext()

	tbl, ok := is.TableByID(context.Background(), 1)
	require.True(t, ok)
	pt := tbl.GetPartitionedTable()
	require.NotNil(t, pt)

	row := make([]types.Datum, len(pt.Meta().Columns))
	d0 := types.NewIntDatum(1)
	d0.Copy(&row[0])
	partIdx, err := pt.GetPartitionIdxByRow(sctx.GetExprCtx().GetEvalCtx(), row)
	require.NoError(t, err)
	partIdx, err = pt.Meta().Partition.ReplaceWithOverlappingPartitionIdx(partIdx, err)
	require.NoError(t, err)
	targetPartitionID := pt.Meta().Partition.Definitions[partIdx].ID

	partitionLocations := make(map[int64]string, len(partitionIDs))
	for _, pid := range partitionIDs {
		partitionLocations[pid] = "local"
	}
	partitionLocations[targetPartitionID] = "remote"
	SetLocationResolver(&mockPartitionLocationResolver{
		local:              "local",
		partitionLocations: partitionLocations,
	})

	pa, err := expression.ParamMarkerExpression(sctx.GetExprCtx(), &driver.ParamMarkerExpr{Order: 0}, true)
	require.NoError(t, err)
	pb, err := expression.ParamMarkerExpression(sctx.GetExprCtx(), &driver.ParamMarkerExpr{Order: 1}, true)
	require.NoError(t, err)
	insPlan := &Insert{
		Table: tbl,
		Lists: [][]expression.Expression{{pa, pb}},
	}
	loc := buildLocFromInsert(insPlan)
	require.NotNil(t, loc)
	require.True(t, loc.IsPartitioned)
	require.NotNil(t, loc.PartitionByRowInfo)

	params := []expression.Expression{
		&expression.Constant{Value: types.NewIntDatum(1), RetType: types.NewFieldType(mysql.TypeLonglong)},
		&expression.Constant{Value: types.NewIntDatum(1), RetType: types.NewFieldType(mysql.TypeLonglong)},
	}
	partIDs, allParts, err := loc.CalculatePartitionIDs(sctx, is, params)
	require.NoError(t, err)
	require.False(t, allParts)
	require.Equal(t, []int64{targetPartitionID}, partIDs)

	locInfo := &CachedPlanLocationInfo{
		TableLocations:    []*TableLocationInfo{loc},
		HasPartitionTable: true,
		StatementTraits: StatementTraits{
			IsDML:   true,
			DMLType: DMLTypeInsert,
		},
	}
	planLoc := locInfo.DetermineLocationWithParams(sctx, is, params)
	require.NotNil(t, planLoc)
	require.Equal(t, PhyPlanRemote, planLoc.PlanType)
	require.Equal(t, "remote", planLoc.TargetStore)
}

func TestBuildLocFromInsert_PartitionedValuesPruningMultiPartitionDistributed(t *testing.T) {
	prev := GetLocationResolver()
	t.Cleanup(func() { SetLocationResolver(prev) })

	partitionIDs := []int64{101, 102, 103}
	tblInfo := buildRangePartitionedTableInfo(1, "t", partitionIDs)
	is := infoschema.MockInfoSchema([]*model.TableInfo{tblInfo})
	sctx := mock.NewContext()

	tbl, ok := is.TableByID(context.Background(), 1)
	require.True(t, ok)
	pt := tbl.GetPartitionedTable()
	require.NotNil(t, pt)

	row1 := make([]types.Datum, len(pt.Meta().Columns))
	d1 := types.NewIntDatum(1)
	d1.Copy(&row1[0])
	pIdx1, err := pt.GetPartitionIdxByRow(sctx.GetExprCtx().GetEvalCtx(), row1)
	require.NoError(t, err)
	pIdx1, err = pt.Meta().Partition.ReplaceWithOverlappingPartitionIdx(pIdx1, err)
	require.NoError(t, err)
	pid1 := pt.Meta().Partition.Definitions[pIdx1].ID

	row2 := make([]types.Datum, len(pt.Meta().Columns))
	d2 := types.NewIntDatum(11)
	d2.Copy(&row2[0])
	pIdx2, err := pt.GetPartitionIdxByRow(sctx.GetExprCtx().GetEvalCtx(), row2)
	require.NoError(t, err)
	pIdx2, err = pt.Meta().Partition.ReplaceWithOverlappingPartitionIdx(pIdx2, err)
	require.NoError(t, err)
	pid2 := pt.Meta().Partition.Definitions[pIdx2].ID
	require.NotEqual(t, pid1, pid2)

	SetLocationResolver(&mockPartitionLocationResolver{
		local:              "local",
		partitionLocations: map[int64]string{pid1: "remote1", pid2: "remote2"},
	})

	p0, err := expression.ParamMarkerExpression(sctx.GetExprCtx(), &driver.ParamMarkerExpr{Order: 0}, true)
	require.NoError(t, err)
	p1, err := expression.ParamMarkerExpression(sctx.GetExprCtx(), &driver.ParamMarkerExpr{Order: 1}, true)
	require.NoError(t, err)
	p2, err := expression.ParamMarkerExpression(sctx.GetExprCtx(), &driver.ParamMarkerExpr{Order: 2}, true)
	require.NoError(t, err)
	p3, err := expression.ParamMarkerExpression(sctx.GetExprCtx(), &driver.ParamMarkerExpr{Order: 3}, true)
	require.NoError(t, err)
	insPlan := &Insert{
		Table: tbl,
		Lists: [][]expression.Expression{{p0, p1}, {p2, p3}},
	}
	loc := buildLocFromInsert(insPlan)
	require.NotNil(t, loc)
	require.True(t, loc.IsPartitioned)
	require.NotNil(t, loc.PartitionByRowInfo)

	params := []expression.Expression{
		&expression.Constant{Value: types.NewIntDatum(1), RetType: types.NewFieldType(mysql.TypeLonglong)},
		&expression.Constant{Value: types.NewIntDatum(0), RetType: types.NewFieldType(mysql.TypeLonglong)},
		&expression.Constant{Value: types.NewIntDatum(11), RetType: types.NewFieldType(mysql.TypeLonglong)},
		&expression.Constant{Value: types.NewIntDatum(0), RetType: types.NewFieldType(mysql.TypeLonglong)},
	}
	partIDs, allParts, err := loc.CalculatePartitionIDs(sctx, is, params)
	require.NoError(t, err)
	require.False(t, allParts)
	require.ElementsMatch(t, []int64{pid1, pid2}, partIDs)

	locInfo := &CachedPlanLocationInfo{
		TableLocations:    []*TableLocationInfo{loc},
		HasPartitionTable: true,
		StatementTraits: StatementTraits{
			IsDML:   true,
			DMLType: DMLTypeInsert,
		},
	}
	planLoc := locInfo.DetermineLocationWithParams(sctx, is, params)
	require.NotNil(t, planLoc)
	require.Equal(t, PhyPlanDistributed, planLoc.PlanType)
}
