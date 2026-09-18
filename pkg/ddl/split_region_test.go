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
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type recordedSplitRegionCall struct {
	keys       [][]byte
	scatter    bool
	groupID    int64
	hasGroupID bool
}

type recordingSplitRegionStore struct {
	calls []recordedSplitRegionCall
}

func (s *recordingSplitRegionStore) SplitRegions(
	_ context.Context,
	keys [][]byte,
	scatter bool,
	groupID *int64,
) ([]uint64, error) {
	call := recordedSplitRegionCall{
		keys:       keys,
		scatter:    scatter,
		hasGroupID: groupID != nil,
	}
	if groupID != nil {
		call.groupID = *groupID
	}
	s.calls = append(s.calls, call)
	return nil, nil
}

func (*recordingSplitRegionStore) WaitScatterRegionFinish(context.Context, uint64, int) error {
	return nil
}

func (*recordingSplitRegionStore) CheckRegionInScattering(uint64) (bool, error) {
	return false, nil
}

func TestSplitRegionScatterConfig(t *testing.T) {
	t.Run("partitioned table keeps key and scatter group IDs separate", func(t *testing.T) {
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
		store := &recordingSplitRegionStore{}

		splitPartitionTableRegion(mock.NewContext(), store, tblInfo, parts, vardef.ScatterGlobal)

		type indexKey struct {
			tableID int64
			indexID int64
		}
		var indexKeys []indexKey
		for i, call := range store.calls {
			if !call.scatter {
				t.Errorf("split call %d did not enable scatter", i)
			}
			if !call.hasGroupID {
				t.Errorf("split call %d did not set a scatter group ID", i)
			}
			if call.groupID != GlobalScatterGroupID {
				t.Errorf("split call %d used scatter group ID %d, expected %d", i, call.groupID, GlobalScatterGroupID)
			}
			for _, key := range call.keys {
				decodedTableID := tablecodec.DecodeTableID(key)
				if decodedTableID == GlobalScatterGroupID {
					t.Errorf("split call %d encoded a key under scatter group ID %d", i, GlobalScatterGroupID)
				}
				tableID, indexID, isRecord, err := tablecodec.DecodeKeyHead(key)
				if err == nil && !isRecord {
					indexKeys = append(indexKeys, indexKey{tableID: tableID, indexID: indexID})
				}
			}
		}
		require.ElementsMatch(t, []indexKey{
			{tableID: tableID, indexID: globalIndexID},
			{tableID: partition0ID, indexID: localIndexID + 1},
			{tableID: partition1ID, indexID: localIndexID + 1},
		}, indexKeys)
	})

	t.Run("split policies use the persisted scatter scope", func(t *testing.T) {
		tblInfo := buildSplitPolicyTableInfo(t)
		workerCtx := mock.NewContext()
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
				store := &recordingSplitRegionStore{}

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

func buildSplitPolicyTableInfo(t *testing.T) *model.TableInfo {
	t.Helper()
	stmt, err := parser.New().ParseOneStmt("create table t (id bigint primary key)", "", "")
	require.NoError(t, err)
	tblInfo, err := BuildTableInfoFromAST(metabuild.NewContext(), stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)
	tblInfo.ID = 200
	tblInfo.TableSplitPolicy = &model.RegionSplitPolicy{
		Lower:   []string{"0"},
		Upper:   []string{"10000"},
		Regions: 2,
	}
	return tblInfo
}
