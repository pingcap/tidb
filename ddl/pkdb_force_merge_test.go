// Copyright 2024 PingCAP, Inc.
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
	"github.com/pingcap/tidb/infoschema"
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
