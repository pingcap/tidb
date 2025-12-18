package core

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
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
	require.False(t, info.IsDML)
	require.Equal(t, DMLTypeNone, info.DMLType)
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
