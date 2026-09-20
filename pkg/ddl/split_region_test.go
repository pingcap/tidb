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
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestSplitRegionScatterConfig(t *testing.T) {
	t.Run("partitioned table keeps physical IDs and scatter group IDs separate", func(t *testing.T) {
		const (
			tableID       int64 = 100
			partition0ID  int64 = 101
			partition1ID  int64 = 102
			localIndexID  int64 = 11
			globalIndexID int64 = 22
		)

		parts := []model.PartitionDefinition{
			{ID: partition0ID, Name: ast.NewCIStr("p0")},
			{ID: partition1ID, Name: ast.NewCIStr("p1")},
		}
		tblInfo := &model.TableInfo{
			ID:              tableID,
			Name:            ast.NewCIStr("t"),
			ShardRowIDBits:  2,
			PreSplitRegions: 1,
			Partition:       &model.PartitionInfo{Enable: true, Definitions: parts},
			Indices: []*model.IndexInfo{
				{ID: localIndexID, Name: ast.NewCIStr("idx_local")},
				{ID: globalIndexID, Name: ast.NewCIStr("idx_global"), Global: true},
			},
		}

		type indexKey struct {
			tableID int64
			indexID int64
		}

		for _, test := range []struct {
			name            string
			scope           string
			expectedGroupID int64
		}{
			{name: "table", scope: vardef.ScatterTable, expectedGroupID: tableID},
			{name: "global", scope: vardef.ScatterGlobal, expectedGroupID: GlobalScatterGroupID},
		} {
			t.Run(test.name, func(t *testing.T) {
				store := &fakeAutoPreSplitStore{}

				splitPartitionTableRegion(mock.NewContext(), store, tblInfo, parts, test.scope)

				var (
					keyTableIDs []int64
					indexKeys   []indexKey
				)
				for i, call := range store.calls {
					require.Truef(t, call.scatter, "split call %d did not enable scatter", i)
					require.Truef(t, call.hasGroupID, "split call %d did not set a scatter group ID", i)
					require.Equalf(t, test.expectedGroupID, call.groupID, "split call %d used the wrong scatter group ID", i)
					for _, key := range call.keys {
						keyTableIDs = append(keyTableIDs, tablecodec.DecodeTableID(key))
						keyTableID, indexID, isRecord, err := tablecodec.DecodeKeyHead(key)
						if err == nil && !isRecord {
							indexKeys = append(indexKeys, indexKey{tableID: keyTableID, indexID: indexID})
						}
					}
				}

				require.ElementsMatch(t, []int64{
					tableID,
					partition0ID, partition0ID, partition0ID,
					partition1ID, partition1ID, partition1ID,
				}, keyTableIDs)
				require.ElementsMatch(t, []indexKey{
					{tableID: tableID, indexID: globalIndexID},
					{tableID: partition0ID, indexID: localIndexID + 1},
					{tableID: partition1ID, indexID: localIndexID + 1},
				}, indexKeys)
			})
		}
	})

	t.Run("split policies use the persisted scatter scope", func(t *testing.T) {
		tblInfo, _ := buildAutoPreSplitTestTableInfoFromSQL(t, "create table t (id bigint primary key, index idx (id))", 200)
		tblInfo.TableSplitPolicy = &model.RegionSplitPolicy{
			Lower:   []string{"0"},
			Upper:   []string{"10000"},
			Regions: 2,
		}
		workerCtx := mock.NewContext()
		// Keep the live session setting opposite to every test scope to verify the persisted scope is used.
		workerCtx.GetSessionVars().ScatterRegion = vardef.ScatterOff

		for _, test := range []struct {
			name            string
			scope           string
			expectedGroupID int64
		}{
			{name: "table", scope: vardef.ScatterTable, expectedGroupID: tblInfo.ID},
			{name: "global", scope: vardef.ScatterGlobal, expectedGroupID: GlobalScatterGroupID},
		} {
			t.Run(test.name, func(t *testing.T) {
				store := &fakeAutoPreSplitStore{}

				splitTableRegion(workerCtx, store, tblInfo, test.scope)

				require.Len(t, store.calls, 1)
				call := store.calls[0]
				if !call.scatter {
					t.Error("split policy did not enable scatter from the persisted scope")
				}
				require.True(t, call.hasGroupID)
				require.Equal(t, test.expectedGroupID, call.groupID)
				require.NotEmpty(t, call.keys)
				for _, key := range call.keys {
					require.Equal(t, tblInfo.ID, tablecodec.DecodeTableID(key))
				}
			})
		}
	})
}
