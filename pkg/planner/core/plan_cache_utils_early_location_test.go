package core

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/stretchr/testify/require"
)

func buildTestTableInfo(id int64, name string) *model.TableInfo {
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
	return tbl
}

func buildTestTableInfoWithPK(id int64, name string) *model.TableInfo {
	tbl := buildTestTableInfo(id, name)
	tbl.PKIsHandle = true
	tbl.Columns[0].FieldType.AddFlag(mysql.PriKeyFlag)
	return tbl
}

func buildTestTableInfoWithCommonHandlePK(id int64, name string) *model.TableInfo {
	tbl := &model.TableInfo{
		ID:             id,
		Name:           pmodel.NewCIStr(name),
		State:          model.StatePublic,
		IsCommonHandle: true,
	}
	tbl.Columns = []*model.ColumnInfo{
		{
			State:     model.StatePublic,
			Offset:    0,
			Name:      pmodel.NewCIStr("a"),
			FieldType: *types.NewFieldType(mysql.TypeLong),
			ID:        1,
		},
		{
			State:     model.StatePublic,
			Offset:    1,
			Name:      pmodel.NewCIStr("b"),
			FieldType: *types.NewFieldType(mysql.TypeLong),
			ID:        2,
		},
		{
			State:     model.StatePublic,
			Offset:    2,
			Name:      pmodel.NewCIStr("c"),
			FieldType: *types.NewFieldType(mysql.TypeLong),
			ID:        3,
		},
	}
	tbl.Indices = []*model.IndexInfo{
		{
			ID:      1,
			Name:    pmodel.NewCIStr("PRIMARY"),
			State:   model.StatePublic,
			Unique:  true,
			Primary: true,
			Columns: []*model.IndexColumn{
				{Name: pmodel.NewCIStr("a"), Offset: 0, Length: types.UnspecifiedLength},
				{Name: pmodel.NewCIStr("b"), Offset: 1, Length: types.UnspecifiedLength},
				{Name: pmodel.NewCIStr("c"), Offset: 2, Length: types.UnspecifiedLength},
			},
		},
	}
	return tbl
}

type mockLocationResolver struct {
	local          string
	tableLocations map[int64]string
}

func (m *mockLocationResolver) ResolveTableLocation(tableID int64) string {
	return m.tableLocations[tableID]
}

func (m *mockLocationResolver) ResolvePartitionLocation(tableID int64, partitionID int64) string {
	return ""
}

func (m *mockLocationResolver) ResolveKeyLocation(tableID int64, partitionID int64, key []byte) string {
	return ""
}

func (m *mockLocationResolver) IsLocalStore(storeAddr string) bool {
	return storeAddr == m.local || storeAddr == ""
}

func (m *mockLocationResolver) GetLocalStoreAddr() string {
	return m.local
}

func TestExtractEarlyLocationInfo_MultiTable(t *testing.T) {
	t1 := buildTestTableInfo(1, "t1")
	t2 := buildTestTableInfo(2, "t2")

	is := infoschema.MockInfoSchema([]*model.TableInfo{t1, t2})
	tbl1, ok := is.TableByID(context.Background(), t1.ID)
	require.True(t, ok)
	tbl2, ok := is.TableByID(context.Background(), t2.ID)
	require.True(t, ok)

	info := extractEarlyLocationInfo(context.Background(), is, &ast.SelectStmt{}, []table.Table{tbl1, tbl2, tbl1})
	require.NotNil(t, info)
	require.ElementsMatch(t, []int64{t1.ID, t2.ID}, info.TableIDs)
	require.False(t, info.HasPartitionTable)
	require.False(t, info.StatementTraits.IsDML)
	require.Equal(t, DMLTypeNone, info.StatementTraits.DMLType)
}

func TestExtractEarlyLocationInfo_PointGetSelect(t *testing.T) {
	t1 := buildTestTableInfoWithPK(1, "t1")

	is := infoschema.MockInfoSchema([]*model.TableInfo{t1})
	tbl1, ok := is.TableByID(context.Background(), t1.ID)
	require.True(t, ok)

	stmt := &ast.SelectStmt{
		From: &ast.TableRefsClause{TableRefs: &ast.Join{
			Left: &ast.TableSource{Source: &ast.TableName{Name: pmodel.NewCIStr("t1")}},
		}},
		Where: &ast.BinaryOperationExpr{
			Op: opcode.EQ,
			L:  &ast.ColumnNameExpr{Name: &ast.ColumnName{Name: pmodel.NewCIStr("a")}},
			R:  &driver.ParamMarkerExpr{Order: 0},
		},
	}

	info := extractEarlyLocationInfo(context.Background(), is, stmt, []table.Table{tbl1})
	require.NotNil(t, info)
	require.True(t, info.StatementTraits.HasPointGet)
}

func TestExtractEarlyLocationInfo_CommonHandlePointGetSelect(t *testing.T) {
	t1 := buildTestTableInfoWithCommonHandlePK(1, "t1")

	is := infoschema.MockInfoSchema([]*model.TableInfo{t1})
	tbl1, ok := is.TableByID(context.Background(), t1.ID)
	require.True(t, ok)

	stmt := &ast.SelectStmt{
		From: &ast.TableRefsClause{TableRefs: &ast.Join{
			Left: &ast.TableSource{Source: &ast.TableName{Name: pmodel.NewCIStr("t1")}},
		}},
		Where: &ast.BinaryOperationExpr{
			Op: opcode.LogicAnd,
			L: &ast.BinaryOperationExpr{
				Op: opcode.EQ,
				L:  &ast.ColumnNameExpr{Name: &ast.ColumnName{Name: pmodel.NewCIStr("a")}},
				R:  &driver.ParamMarkerExpr{Order: 0},
			},
			R: &ast.BinaryOperationExpr{
				Op: opcode.LogicAnd,
				L: &ast.BinaryOperationExpr{
					Op: opcode.EQ,
					L:  &ast.ColumnNameExpr{Name: &ast.ColumnName{Name: pmodel.NewCIStr("b")}},
					R:  &driver.ParamMarkerExpr{Order: 1},
				},
				R: &ast.BinaryOperationExpr{
					Op: opcode.EQ,
					L:  &ast.ColumnNameExpr{Name: &ast.ColumnName{Name: pmodel.NewCIStr("c")}},
					R:  &driver.ParamMarkerExpr{Order: 2},
				},
			},
		},
	}

	info := extractEarlyLocationInfo(context.Background(), is, stmt, []table.Table{tbl1})
	require.NotNil(t, info)
	require.True(t, info.StatementTraits.HasPointGet)
}

func TestExtractEarlyLocationInfo_IndexLookupSelect(t *testing.T) {
	t1 := buildTestTableInfo(1, "t1")

	is := infoschema.MockInfoSchema([]*model.TableInfo{t1})
	tbl1, ok := is.TableByID(context.Background(), t1.ID)
	require.True(t, ok)

	stmt := &ast.SelectStmt{
		From: &ast.TableRefsClause{TableRefs: &ast.Join{
			Left: &ast.TableSource{Source: &ast.TableName{Name: pmodel.NewCIStr("t1")}},
		}},
		Fields: &ast.FieldList{Fields: []*ast.SelectField{{
			Expr: &ast.ColumnNameExpr{Name: &ast.ColumnName{Name: pmodel.NewCIStr("a")}},
		}}},
		Where: &ast.BinaryOperationExpr{
			Op: opcode.EQ,
			L:  &ast.ColumnNameExpr{Name: &ast.ColumnName{Name: pmodel.NewCIStr("a")}},
			R:  &driver.ParamMarkerExpr{Order: 0},
		},
	}

	info := extractEarlyLocationInfo(context.Background(), is, stmt, []table.Table{tbl1})
	require.NotNil(t, info)
	require.False(t, info.StatementTraits.HasPointGet)
	require.True(t, info.StatementTraits.HasIndexLookup)
}

func TestEarlyLocationInfo_DetermineLocation_MultiTable(t *testing.T) {
	prev := GetLocationResolver()
	t.Cleanup(func() { SetLocationResolver(prev) })

	t.Run("remote same store", func(t *testing.T) {
		SetLocationResolver(&mockLocationResolver{
			local:          "local",
			tableLocations: map[int64]string{1: "remote", 2: "remote"},
		})
		info := &EarlyLocationInfo{TableIDs: []int64{1, 2}}
		loc := info.DetermineLocation(nil, nil, nil)
		require.NotNil(t, loc)
		require.Equal(t, PhyPlanRemote, loc.PlanType)
		require.Equal(t, "remote", loc.TargetStore)
	})

	t.Run("different stores", func(t *testing.T) {
		SetLocationResolver(&mockLocationResolver{
			local:          "local",
			tableLocations: map[int64]string{1: "remote1", 2: "remote2"},
		})
		info := &EarlyLocationInfo{TableIDs: []int64{1, 2}}
		loc := info.DetermineLocation(nil, nil, nil)
		require.NotNil(t, loc)
		require.Equal(t, PhyPlanDistributed, loc.PlanType)
	})

	t.Run("partitioned needs cached info", func(t *testing.T) {
		SetLocationResolver(&mockLocationResolver{
			local:          "local",
			tableLocations: map[int64]string{1: "remote"},
		})
		info := &EarlyLocationInfo{TableIDs: []int64{1}, HasPartitionTable: true}
		require.Nil(t, info.DetermineLocation(nil, nil, nil))
	})

	t.Run("cached info takes precedence", func(t *testing.T) {
		SetLocationResolver(&mockLocationResolver{
			local:          "local",
			tableLocations: map[int64]string{1: "remote"},
		})
		info := &EarlyLocationInfo{
			TableIDs:          []int64{1},
			HasPartitionTable: true,
			CachedLocationInfo: &CachedPlanLocationInfo{
				TableLocations: []*TableLocationInfo{{TableID: 1, IsPartitioned: false}},
			},
		}
		loc := info.DetermineLocation(nil, nil, nil)
		require.NotNil(t, loc)
		require.Equal(t, PhyPlanRemote, loc.PlanType)
		require.Equal(t, "remote", loc.TargetStore)
	})
}

func TestEarlyLocationInfo_DetermineLocation_PointGetLocal(t *testing.T) {
	prev := GetLocationResolver()
	t.Cleanup(func() { SetLocationResolver(prev) })

	SetLocationResolver(&mockLocationResolver{
		local:          "local",
		tableLocations: map[int64]string{1: "remote"},
	})
	info := &EarlyLocationInfo{
		TableIDs: []int64{1},
		StatementTraits: StatementTraits{
			HasPointGet: true,
		},
	}
	loc := info.DetermineLocation(nil, nil, nil)
	require.NotNil(t, loc)
	require.Equal(t, PhyPlanLocal, loc.PlanType)
}

func TestEarlyLocationInfo_DetermineLocation_IndexLookupLocal(t *testing.T) {
	prev := GetLocationResolver()
	t.Cleanup(func() { SetLocationResolver(prev) })

	SetLocationResolver(&mockLocationResolver{
		local:          "local",
		tableLocations: map[int64]string{1: "remote"},
	})
	info := &EarlyLocationInfo{
		TableIDs: []int64{1},
		StatementTraits: StatementTraits{
			HasIndexLookup: true,
		},
	}
	loc := info.DetermineLocation(nil, nil, nil)
	require.NotNil(t, loc)
	require.Equal(t, PhyPlanLocal, loc.PlanType)
}
