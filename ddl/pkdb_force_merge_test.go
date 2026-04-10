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

package ddl

import (
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
)

func TestBuildForceMergeKeyRanges(t *testing.T) {
	keyRanges := buildForceMergeKeyRanges([]forceMergeRange{
		{StartTableID: 8, EndTableID: 8},
		{StartTableID: 2, EndTableID: 4},
		{StartTableID: 5, EndTableID: 7},
		{StartTableID: 10, EndTableID: 10},
		{StartTableID: 0, EndTableID: 1},
		{StartTableID: 12, EndTableID: 11},
	})

	require.Len(t, keyRanges, 2)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(2)), keyRanges[0].StartKey)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(9)), keyRanges[0].EndKey)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(10)), keyRanges[1].StartKey)
	require.Equal(t, codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(11)), keyRanges[1].EndKey)
}

func TestFindForceMergeStartTableID(t *testing.T) {
	existingTableIDs := map[int64]struct{}{
		40: {},
		95: {},
	}

	require.Equal(t, int64(96), findForceMergeStartTableID(existingTableIDs, 100))
	require.Equal(t, int64(41), findForceMergeStartTableID(existingTableIDs, 50))
	require.Equal(t, int64(1), findForceMergeStartTableID(nil, 50))
}

func TestBuildForceMergeRangesUsesPartitionLookup(t *testing.T) {
	is := infoschema.MockInfoSchema([]*model.TableInfo{
		mockForceMergeTableInfo(10),
		mockForceMergeTableInfo(11, 12),
	})

	ranges := buildForceMergeRanges(is, "test", 14, mockForceMergeTableInfo(14), []int64{14})
	require.Equal(t, []forceMergeRange{{StartTableID: 13, EndTableID: 14}}, ranges)
}

func TestBuildForceMergeRangesIgnoresCurrentTableIDs(t *testing.T) {
	currentTable := mockForceMergeTableInfo(16, 17, 18)
	is := infoschema.MockInfoSchema([]*model.TableInfo{
		mockForceMergeTableInfo(15),
		currentTable,
	})

	ranges := buildForceMergeRanges(is, "test", 16, currentTable, []int64{17, 18})
	require.Equal(t, []forceMergeRange{
		{StartTableID: 16, EndTableID: 17},
		{StartTableID: 16, EndTableID: 18},
	}, ranges)
}

func TestBuildAllForceMergeRangesUsesSortedPhysicalIDs(t *testing.T) {
	is := infoschema.MockInfoSchema([]*model.TableInfo{
		mockForceMergeTableInfo(3),
		mockForceMergeTableInfo(6, 7, 10),
	})

	maxTableID, ranges := buildAllForceMergeRanges(is)
	require.Equal(t, int64(10), maxTableID)
	require.Equal(t, []forceMergeRange{
		{StartTableID: 1, EndTableID: 2},
		{StartTableID: 4, EndTableID: 6},
		{StartTableID: 8, EndTableID: 9},
	}, ranges)
}

func TestBuildAllForceMergeRangesKeepsPartitionGlobalIndexTableID(t *testing.T) {
	is := infoschema.MockInfoSchema([]*model.TableInfo{
		mockForceMergeTableInfo(8),
		mockForceMergePartitionedTableWithGlobalIndex(10, 14, 16),
	})

	maxTableID, ranges := buildAllForceMergeRanges(is)
	require.Equal(t, int64(16), maxTableID)
	require.Equal(t, []forceMergeRange{
		{StartTableID: 1, EndTableID: 7},
		{StartTableID: 9, EndTableID: 9},
		{StartTableID: 11, EndTableID: 13},
		{StartTableID: 15, EndTableID: 15},
	}, ranges)
}

func TestBuildAllForceMergeRangesTreatsSkippedSystemTablesAsOccupied(t *testing.T) {
	is := buildForceMergeInfoSchemaForTest(t,
		&model.DBInfo{
			ID:     1,
			Name:   model.NewCIStr("mysql"),
			Tables: []*model.TableInfo{mockForceMergeTableInfoWithName(5, "tidb")},
			State:  model.StatePublic,
		},
		&model.DBInfo{
			ID:     2,
			Name:   model.NewCIStr("test"),
			Tables: []*model.TableInfo{mockForceMergeTableInfo(8)},
			State:  model.StatePublic,
		},
	)

	maxTableID, ranges := buildAllForceMergeRanges(is)
	require.Equal(t, int64(8), maxTableID)
	require.Equal(t, []forceMergeRange{
		{StartTableID: 1, EndTableID: 4},
		{StartTableID: 6, EndTableID: 7},
	}, ranges)
}

func TestBuildForceMergeRangesIncludesPartitionGlobalIndexRange(t *testing.T) {
	currentTable := mockForceMergePartitionedTableWithGlobalIndex(20, 30, 31)
	is := infoschema.MockInfoSchema([]*model.TableInfo{
		mockForceMergeTableInfo(19),
		mockForceMergeTableInfo(25),
		currentTable,
	})

	ranges := buildForceMergeRanges(is, "test", 20, currentTable, []int64{30, 31})
	require.ElementsMatch(t, []forceMergeRange{
		{StartTableID: 26, EndTableID: 30},
		{StartTableID: 26, EndTableID: 31},
		{StartTableID: 20, EndTableID: 20},
	}, ranges)
	require.Equal(t, []infosync.ForceMergeKeyRange{
		{
			StartKey: codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(20)),
			EndKey:   codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(21)),
		},
		{
			StartKey: codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(26)),
			EndKey:   codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(32)),
		},
	}, buildForceMergeKeyRanges(ranges))
}

func TestForceMergeDisabledSkipsDDLLogic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	oldValue := variable.EnableDropTableForceMerge.Load()
	variable.EnableDropTableForceMerge.Store(false)
	defer variable.EnableDropTableForceMerge.Store(oldValue)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/forceMergeBuildEntered", `1*panic("force merge logic should not run when disabled")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/forceMergeBuildEntered"))
	}()

	tk.MustExec("create table t_force_merge_drop (a int)")
	tk.MustExec("drop table t_force_merge_drop")

	tk.MustExec("create table t_force_merge_truncate (a int)")
	tk.MustExec("truncate table t_force_merge_truncate")
}

func mockForceMergeTableInfo(tableID int64, partitionIDs ...int64) *model.TableInfo {
	tblInfo := &model.TableInfo{
		ID:    tableID,
		Name:  model.NewCIStr(fmt.Sprintf("t_%d", tableID)),
		State: model.StatePublic,
	}
	if len(partitionIDs) == 0 {
		return tblInfo
	}

	defs := make([]model.PartitionDefinition, 0, len(partitionIDs))
	for idx, partitionID := range partitionIDs {
		defs = append(defs, model.PartitionDefinition{
			ID:   partitionID,
			Name: model.NewCIStr(fmt.Sprintf("p_%d", idx)),
		})
	}
	tblInfo.Partition = &model.PartitionInfo{Definitions: defs}
	return tblInfo
}

func mockForceMergeTableInfoWithName(tableID int64, tableName string, partitionIDs ...int64) *model.TableInfo {
	tblInfo := mockForceMergeTableInfo(tableID, partitionIDs...)
	tblInfo.Name = model.NewCIStr(tableName)
	return tblInfo
}

func mockForceMergePartitionedTableWithGlobalIndex(tableID int64, partitionIDs ...int64) *model.TableInfo {
	tblInfo := mockForceMergeTableInfo(tableID, partitionIDs...)
	tblInfo.Indices = []*model.IndexInfo{
		{
			ID:     1,
			Name:   model.NewCIStr(fmt.Sprintf("idx_%d", tableID)),
			Global: true,
		},
	}
	return tblInfo
}

type mockForceMergeRequirement struct{}

func (mockForceMergeRequirement) Store() kv.Storage {
	return nil
}

func (mockForceMergeRequirement) AutoIDClient() *autoid.ClientDiscover {
	return nil
}

func buildForceMergeInfoSchemaForTest(t *testing.T, dbInfos ...*model.DBInfo) infoschema.InfoSchema {
	t.Helper()

	builder, err := infoschema.NewBuilder(mockForceMergeRequirement{}, nil).InitWithDBInfos(dbInfos, nil, 1)
	require.NoError(t, err)
	return builder.Build()
}

func TestForceMergeOnlyRunsAfterSuccessfulDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	oldValue := variable.EnableDropTableForceMerge.Load()
	variable.EnableDropTableForceMerge.Store(true)
	defer variable.EnableDropTableForceMerge.Store(oldValue)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/forceMergeBuildEntered", `1*panic("force merge logic should not run for failed ddl")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/forceMergeBuildEntered"))
	}()

	err := tk.ExecToErr("drop table t_force_merge_missing")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "Cancelled DDL job")

	err = tk.ExecToErr("truncate table t_force_merge_missing")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "Cancelled DDL job")
}

func TestForceMergeErrorsDoNotAffectSuccessfulDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	oldValue := variable.EnableDropTableForceMerge.Load()
	variable.EnableDropTableForceMerge.Store(true)
	defer variable.EnableDropTableForceMerge.Store(oldValue)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockForceMergeSendError", `2*return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockForceMergeSendError"))
	}()

	tk.MustExec("create table t_force_merge_drop (a int)")
	tk.MustExec("drop table t_force_merge_drop")

	tk.MustExec("create table t_force_merge_truncate (a int)")
	tk.MustExec("truncate table t_force_merge_truncate")
}
